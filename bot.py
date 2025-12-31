import json
import os
import random
import re
import sys
import subprocess
import threading
from datetime import date, datetime, timezone
from pathlib import Path
import importlib.util
import logging
import time

import requests
import telebot
import telebot.apihelper as apihelper
from telebot.types import ReplyParameters
from telebot.types import (
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    BotCommand,
    BotCommandScopeDefault,
    BotCommandScopeChatMember,
    BotCommandScopeChat,
)

from holidays import HolidayService
import films
import films_day


# --- гарантированно импортируем ЛОКАЛЬНЫЙ schedule.py рядом с bot.py ---
def import_local_schedule():
    """
    Гарантирует импорт именно вашего файла schedule.py, лежащего рядом с bot.py,
    а не установленного в системе пакета schedule.
    """
    here = Path(__file__).resolve().parent
    local_path = here / "schedule.py"
    if not local_path.exists():
        raise RuntimeError(f"Не найден локальный модуль schedule.py рядом с bot.py: {local_path}")

    spec = importlib.util.spec_from_file_location("legendalf_schedule", str(local_path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Не удалось создать spec для schedule.py: {local_path}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if not hasattr(module, "register"):
        raise RuntimeError("В schedule.py не найдена функция register(...).")
    return module


schedule = import_local_schedule()
# ---------------------------------------------------------------------------

KIND_BASE = getattr(schedule, "_KIND_BASE", "base")
KIND_HOLIDAYS = getattr(schedule, "_KIND_HOLIDAYS", "holidays")

BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "legendalf.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("legendalf.bot")

telebot.logger = logging.getLogger("legendalf.telebot")
telebot.logger.setLevel(logging.INFO)

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
if not TOKEN:
    raise RuntimeError("Set TELEGRAM_BOT_TOKEN env var with your bot token")

bot = telebot.TeleBot(TOKEN)
apihelper.CONNECT_TIMEOUT = 20
apihelper.READ_TIMEOUT = 90
holiday_service = HolidayService()

DATA_FILE = Path("users.json")
QUOTES_FILE = Path("/home/skydive-dz/legendalf/quotes.txt")
MEDIA_DIR = Path("/home/skydive-dz/legendalf/image")  # сюда складываем картинки + gif + mp4

TRIGGER_PATTERNS = [
    r"^\s*легэндальф\s*,?\s*выдай\s+базу\s*\.?\s*$",
    r"^\s*легендальф\s*,?\s*выдай\s+базу\s*\.?\s*$",
    r"^\s*гэндальф\s*,?\s*выдай\s+базу\s*\.?\s*$",
    r"^\s*гендальф\s*,?\s*выдай\s+базу\s*\.?\s*$",
    r"^\s*выдай\s+базу\s*\.?\s*$",
    r"^\s*legendalf\s*,?\s*give\s+me\s+the\s+base\s*\.?\s*$",
    r"^\s*legendalf\s*,?\s*drop\s+the\s+base\s*\.?\s*$",
    r"^\s*gandalf\s*,?\s*drop\s+the\s+base\s*\.?\s*$",
]

MEDIA_TRIGGER_PATTERNS = [
    r"^\s*гэндальф\?\s*$",
    r"^\s*гендальф\?\s*$",
    r"^\s*легэндальф\?\s*$",
    r"^\s*легендальф\?\s*$",
    r"^\s*gandalf\?\s*$",
    r"^\s*gendalf\?\s*$",
    r"^\s*legendalf\?\s*$",
]

# Новое: цитата сохраняется ТОЛЬКО если админ пишет "Сохрани базу: <текст>"
SAVE_QUOTE_PREFIX_RE = re.compile(r"^\s*сохрани\s+базу\s*:\s*(.+)\s*$", re.IGNORECASE)


def now_iso_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def is_trigger(text: str) -> bool:
    if not text:
        return False
    t = text.strip().lower()
    return any(re.match(p, t, flags=re.IGNORECASE) for p in TRIGGER_PATTERNS)


def is_media_trigger(text: str) -> bool:
    if not text:
        return False
    t = text.strip().lower()
    return any(re.match(p, t, flags=re.IGNORECASE) for p in MEDIA_TRIGGER_PATTERNS)


def _build_reply_parameters(
    message_id: int | None, allow_without_reply: bool | None = None
) -> ReplyParameters | None:
    if message_id is None:
        return None
    params: dict[str, object] = {"message_id": message_id}
    if allow_without_reply is not None:
        params["allow_sending_without_reply"] = allow_without_reply
    return ReplyParameters(**params)


def default_data():
    return {"admins": [], "allowed": {}, "pending": {}, "schedules": {}}


def load_json():
    if not DATA_FILE.exists():
        DATA_FILE.write_text(json.dumps(default_data(), ensure_ascii=False, indent=2), encoding="utf-8")
        return default_data()
    data = json.loads(DATA_FILE.read_text(encoding="utf-8"))
    data.setdefault("admins", [])
    data.setdefault("allowed", {})
    data.setdefault("pending", {})
    data.setdefault("schedules", {})
    if isinstance(data.get("allowed"), list):
        data["allowed"] = {str(uid): {"added_at": now_iso_utc()} for uid in data["allowed"]}
    if isinstance(data.get("pending"), list):
        data["pending"] = {str(uid): {"requested_at": now_iso_utc()} for uid in data.get("pending", [])}
    save_json(data)
    return data


def save_json(data):
    DATA_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def user_meta(user):
    return {
        "username": user.username or None,
        "first_name": user.first_name or None,
        "last_name": user.last_name or None,
    }


def _format_username(username: str | None) -> str:
    if not username:
        return "—"
    return username if username.startswith("@") else f"@{username}"


def _display_name(meta: dict | None, uid: int) -> str:
    if not isinstance(meta, dict):
        return f"путника {uid}"
    full_name = " ".join(filter(None, [meta.get("first_name"), meta.get("last_name")])).strip()
    if full_name:
        return full_name
    username = meta.get("username")
    if username:
        return _format_username(username)
    return f"путника {uid}"


def _humanize_birthday(value: str | None) -> str:
    if not value:
        return "не задан"
    try:
        dt = datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        return value
    return dt.strftime("%d.%m.%Y")


def _describe_schedule_kind(entry: dict, kind_name: str) -> str:
    kinds = entry.get("kinds")
    if not isinstance(kinds, dict):
        return "нет"
    kind_entry = kinds.get(kind_name)
    if not isinstance(kind_entry, dict):
        return "нет"
    at_time = (kind_entry.get("at_time") or "").strip() or "—"
    enabled = bool(kind_entry.get("enabled")) and at_time not in {"", "—"}
    state = "вкл" if enabled else "выкл"
    return f"{state} ({at_time})"


def _build_user_overview_text() -> str:
    data = load_json()
    schedules = data.get("schedules", {})
    allowed = data.get("allowed", {})
    lines: list[str] = []

    if allowed:
        lines.append("Допущенные:")
        for suid in sorted(allowed.keys(), key=lambda x: int(x)):
            meta = allowed[suid]
            entry = schedules.get(suid, {})
            uname = _format_username(meta.get("username"))
            full_name = " ".join(filter(None, [meta.get("first_name"), meta.get("last_name")])).strip()
            title = f"{suid}: {uname}"
            if full_name:
                title += f" — {full_name}"
            lines.append(title)
            tz = entry.get("tz") or "не задан"
            base_desc = _describe_schedule_kind(entry, KIND_BASE)
            holidays_desc = _describe_schedule_kind(entry, KIND_HOLIDAYS)
            birthday = _humanize_birthday(meta.get("birthday"))
            lines.append(
                f"  TZ: {tz}; база: {base_desc}; праздники: {holidays_desc}; ДР: {birthday}"
            )
    else:
        lines.append("Допущенных пока нет.")

    pending = data.get("pending", {})
    if pending:
        lines.append("")
        lines.append("У врат:")
        for suid in sorted(pending.keys(), key=lambda x: int(x)):
            meta = pending[suid]
            uname = _format_username(meta.get("username"))
            requested = meta.get("requested_at", "неизвестно")
            lines.append(f"{suid}: {uname} — ждёт с {requested}")
    return "\n".join(lines)


def _find_user_record(data: dict, identifier: str):
    if not identifier:
        return None
    ident = identifier.strip()
    if ident.startswith("@"):
        ident = ident[1:]
    if not ident:
        return None

    buckets = []
    for bucket_name in ("allowed", "pending"):
        bucket = data.get(bucket_name)
        if isinstance(bucket, dict):
            buckets.append((bucket_name, bucket))

    if ident.isdigit():
        for bucket_name, bucket in buckets:
            if ident in bucket:
                return bucket_name, ident, bucket[ident]
        return None

    needle = ident.lower()
    for bucket_name, bucket in buckets:
        for suid, meta in bucket.items():
            username = meta.get("username")
            if not username:
                continue
            uname_norm = username.lstrip("@").lower()
            if uname_norm == needle:
                return bucket_name, suid, meta
    return None


def _set_user_birthday(identifier: str, born: datetime.date):
    data = load_json()
    found = _find_user_record(data, identifier)
    if not found:
        return None
    bucket_name, suid, meta = found
    if not isinstance(meta, dict):
        meta = {}
    meta["birthday"] = born.isoformat()
    bucket = data.get(bucket_name, {})
    bucket[suid] = meta
    data[bucket_name] = bucket
    save_json(data)
    return bucket_name, suid, meta


def load_quotes():
    if not QUOTES_FILE.exists():
        return ["База пока не записана: положи цитаты в quotes.txt, и они оживут."]
    lines = [line.strip() for line in QUOTES_FILE.read_text(encoding="utf-8").splitlines()]
    lines = [l for l in lines if l]
    return lines or ["База пуста: даже мудрость молчит, если её не записали."]


def random_quote() -> str:
    quotes = load_quotes()
    return random.choice(quotes)


def append_quote(text: str) -> bool:
    cleaned = (text or "").strip()
    if not cleaned:
        return False
    QUOTES_FILE.parent.mkdir(parents=True, exist_ok=True)

    if QUOTES_FILE.exists() and QUOTES_FILE.stat().st_size > 0:
        try:
            tail = QUOTES_FILE.read_bytes()[-1:]
            needs_nl = tail != b"\n"
        except Exception:
            needs_nl = True
    else:
        needs_nl = False

    with open(QUOTES_FILE, "a", encoding="utf-8") as f:
        if needs_nl:
            f.write("\n")
        f.write(cleaned)
        f.write("\n")
    return True


def list_media():
    if not MEDIA_DIR.exists() or not MEDIA_DIR.is_dir():
        return []
    exts = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".mp4"}
    return [p for p in MEDIA_DIR.iterdir() if p.is_file() and p.suffix.lower() in exts]


def send_random_media(
    chat_id: int,
    reply_to_message_id: int | None = None,
    caption: str | None = None,
):
    """
    caption defaults to a random quote.
    """
    media = list_media()
    rp = _build_reply_parameters(reply_to_message_id, allow_without_reply=True)

    if not media:
        bot.send_message(
            chat_id,
            "Я не вижу свитков с образами и видениями в моей папке.\n"
            f"Положи файлы в: {MEDIA_DIR}",
            reply_parameters=rp,
        )
        return

    path = random.choice(media)
    media_caption = caption if caption is not None else random_quote()

    try:
        with open(path, "rb") as f:
            ext = path.suffix.lower()
            if ext in {".jpg", ".jpeg", ".png", ".webp"}:
                bot.send_photo(chat_id, f, caption=media_caption, reply_parameters=rp)
            elif ext == ".gif":
                bot.send_animation(chat_id, f, caption=media_caption, reply_parameters=rp)
            elif ext == ".mp4":
                bot.send_video(chat_id, f, caption=media_caption, reply_parameters=rp)
            else:
                bot.send_document(chat_id, f, caption=media_caption, reply_parameters=rp)
    except Exception:
        bot.send_message(
            chat_id,
            "Воля была, но видение не открылось. Проверь файл и права доступа.",
            reply_parameters=rp,
        )




def _download_telegram_file_stream(file_path: str, out_path: Path) -> None:
    """
    Потоково скачивает файл Telegram на диск, без загрузки целиком в RAM.
    """
    url = f"https://api.telegram.org/file/bot{TOKEN}/{file_path}"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with requests.get(url, stream=True, timeout=(30, 600)) as r:
        r.raise_for_status()
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):  # 1 MB
                if chunk:
                    f.write(chunk)


def save_media_from_message(message) -> tuple[bool, str]:
    """
    Сохраняет присланные админом медиа в MEDIA_DIR.
    Поддерживает: photo, animation (gif), video (mp4), document(картинка/gif/mp4).
    Потоково пишет на диск.
    Возвращает (ok, info).
    """
    try:
        MEDIA_DIR.mkdir(parents=True, exist_ok=True)

        file_id = None
        ext = None

        if message.photo:
            file_id = message.photo[-1].file_id
            ext = ".jpg"

        elif getattr(message, "animation", None):
            file_id = message.animation.file_id
            ext = ".gif"
            if message.animation.file_name:
                sfx = Path(message.animation.file_name).suffix.lower()
                if sfx in {".gif", ".mp4"}:
                    ext = sfx

        elif getattr(message, "video", None):
            file_id = message.video.file_id
            ext = ".mp4"
            if message.video.file_name:
                sfx = Path(message.video.file_name).suffix.lower()
                if sfx:
                    ext = sfx

        elif message.document:
            mt = (message.document.mime_type or "").lower()
            name = message.document.file_name or ""
            suffix = Path(name).suffix.lower()

            allowed_suffixes = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".mp4"}
            if mt.startswith("image/") or mt.startswith("video/") or suffix in allowed_suffixes:
                file_id = message.document.file_id
                ext = suffix if suffix else (".mp4" if mt.startswith("video/") else ".img")
            else:
                return False, "Это не похоже ни на образ, ни на видение (jpg/png/gif/mp4)."

        if not file_id or not ext:
            return False, "Я не вижу здесь ни образа, ни видения, которое можно сохранить."

        file_size = None
        try:
            if getattr(message, "video", None) and message.video:
                file_size = message.video.file_size
            elif getattr(message, "animation", None) and message.animation:
                file_size = message.animation.file_size
            elif message.document:
                file_size = message.document.file_size
            elif message.photo:
                file_size = message.photo[-1].file_size
        except Exception:
            file_size = None

        MAX_BYTES = 150 * 1024 * 1024  # 150 MB
        if file_size and file_size > MAX_BYTES:
            return (
                False,
                f"Слишком тяжёлое видение ({file_size/1024/1024:.1f} MB). "
                f"Я приму до {MAX_BYTES/1024/1024:.0f} MB.",
            )

        f_info = bot.get_file(file_id)

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"legendalf_{ts}_{random.randint(1000, 9999)}{ext}"
        out_path = MEDIA_DIR / filename

        _download_telegram_file_stream(f_info.file_path, out_path)

        return True, str(out_path)

    except requests.exceptions.RequestException:
        return False, "Сеть дрогнула, и видение не дошло до свитка. Попробуй ещё раз."
    except Exception:
        return False, "Видение ускользнуло при сохранении. Проверь права и место на диске."


def is_admin(uid: int) -> bool:
    data = load_json()
    return uid in data.get("admins", [])


def is_allowed(uid: int) -> bool:
    data = load_json()
    suid = str(uid)
    return uid in data.get("admins", []) or suid in data.get("allowed", {})


def add_pending(user) -> bool:
    data = load_json()
    uid = str(user.id)

    if user.id in data.get("admins", []) or uid in data.get("allowed", {}):
        return False

    if uid in data.get("pending", {}):
        return False

    data["pending"][uid] = {
        **user_meta(user),
        "requested_at": now_iso_utc(),
    }
    save_json(data)
    return True


def approve_user(uid: int):
    data = load_json()
    suid = str(uid)

    meta = data["pending"].pop(suid, None) or {}
    meta.pop("requested_at", None)

    data["allowed"][suid] = {
        **meta,
        "added_at": now_iso_utc(),
    }
    save_json(data)


def deny_user(uid: int):
    data = load_json()
    suid = str(uid)
    if suid in data.get("pending", {}):
        data["pending"].pop(suid, None)
        save_json(data)


def notify_admins_new_request(user):
    data = load_json()
    admins = data.get("admins", [])
    if not admins:
        return

    uid = user.id
    uname = f"@{user.username}" if user.username else "(no username)"
    name = f"{user.first_name or ''} {user.last_name or ''}".strip()

    text = (
        "У врат появился путник и просит допустить его к мудрости Legendalf:\n"
        f"- знак: {uid}\n"
        f"- имя: {uname}\n"
        f"- как зовут в миру: {name if name else '(no name)'}\n\n"
        "Откроем ли ему путь?"
    )

    kb = InlineKeyboardMarkup()
    kb.add(
        InlineKeyboardButton("✅ Открыть врата", callback_data=f"approve:{uid}"),
        InlineKeyboardButton("❌ Оставить снаружи", callback_data=f"deny:{uid}"),
    )

    for admin_id in admins:
        try:
            bot.send_message(admin_id, text, reply_markup=kb)
        except Exception:
            pass


def format_user_line(uid: str, meta: dict) -> str:
    uname = f"@{meta.get('username')}" if meta.get("username") else "(no username)"
    name = f"{meta.get('first_name') or ''} {meta.get('last_name') or ''}".strip() or "(no name)"
    ts = meta.get("requested_at") or meta.get("added_at") or ""
    return f"- {uid} | {uname} | {name} | {ts}"


def _set_commands_with_retry(bot_instance, commands, scope, label: str, retries: int = 3, base_delay: float = 1.0) -> bool:
    for attempt in range(1, retries + 1):
        try:
            bot_instance.set_my_commands(commands, scope=scope)
            return True
        except Exception as exc:
            if attempt == retries:
                logger.warning("Failed to update %s after %d attempt(s): %s", label, retries, exc)
                return False
            delay = base_delay * attempt
            logger.warning("Retrying %s in %.1fs due to error: %s", label, delay, exc)
            time.sleep(delay)
    return False


def setup_commands():
    # Команды, которые видят ВСЕ
    common_commands = [
        BotCommand("mellon", "Молви «друг» и войди"),
        BotCommand("id", "Узнать свой знак (user_id)"),
        BotCommand("schedule", "График рассылки"),  # рекомендую показывать всем допущенным
        BotCommand("holydays", "Праздники сегодняшнего дня"),
        BotCommand("films_day", "Премьеры дня"),
        BotCommand("films_month", "Премьеры месяца (Кинопоиск)"),
    ]

    # Команды, которые видит ТОЛЬКО админ
    admin_commands = [
        BotCommand("pending", "Список путников у врат"),
        BotCommand("allow", "Открыть путь путнику"),
        BotCommand("deny", "Отказать путнику"),
        BotCommand("users", "Путники"),
        BotCommand("restart", "Перезапустить"),
    ]

    if _set_commands_with_retry(
        bot,
        common_commands,
        BotCommandScopeDefault(),
        "default commands",
    ):
        logger.info("Default commands updated (%d)", len(common_commands))

    data = load_json()
    for admin_id in data.get("admins", []):
        if _set_commands_with_retry(
            bot,
            common_commands + admin_commands,
            BotCommandScopeChat(chat_id=admin_id),
            f"admin commands for {admin_id} (chat)",
        ):
            logger.info("Admin commands updated for %s (chat)", admin_id)


def announce_startup():
    data = load_json()
    admins = data.get("admins", [])
    logger.info("Announcing startup to %d admin(s)", len(admins))
    text = "Я слуга вечного огня, повелитель пламени Анора!"
    for admin_id in admins:
        try:
            bot.send_message(admin_id, text)
        except Exception:
            pass


def _start_journalctl_forwarder(unit_name: str, logger_name: str = "legendalf.journal") -> None:
    if not unit_name or sys.platform.startswith("win"):
        return

    log = logging.getLogger(logger_name)
    line_re = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3} \w+ \[legendalf\.")
    systemd_re = re.compile(r"\b(systemd\[\d+\]|legendalf\.service:)\b", re.IGNORECASE)

    def run():
        cmd = ["journalctl", "-u", unit_name, "-f", "--no-pager", "-o", "cat"]
        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
        except Exception as exc:
            log.warning("journalctl forwarder failed to start: %s", exc)
            return

        try:
            assert proc.stdout is not None
            for line in proc.stdout:
                line = line.rstrip()
                if not line:
                    continue
                # Skip lines that already look like our own structured logs to avoid duplication.
                if line_re.match(line):
                    continue
                # Skip systemd service lifecycle noise.
                if systemd_re.search(line):
                    continue
                log.info("%s", line)
        except Exception as exc:
            log.warning("journalctl forwarder stopped: %s", exc)
        finally:
            try:
                proc.terminate()
            except Exception:
                pass

    t = threading.Thread(target=run, name="journalctl-forwarder", daemon=True)
    t.start()


def restart_self():
    os.execv(sys.executable, [sys.executable] + sys.argv)


@bot.message_handler(commands=["restart"])
def cmd_restart(message):
    uid = message.from_user.id
    if not is_admin(uid):
        logger.warning("User %s attempted /restart without permissions", uid)
        return

    logger.info("Admin %s requested /restart", uid)
    bot.reply_to(
        message,
        "Ты призвал меня начать путь заново.\n"
        "Пусть пламя Анора вновь разгорится.",
    )
    try:
        bot.stop_polling()
    except Exception:
        pass
    logger.info("Restarting process via execv")
    restart_self()

@bot.message_handler(commands=["id"])
def cmd_id(message):
    uname = f"@{message.from_user.username}" if message.from_user.username else "(no username)"
    bot.reply_to(
        message,
        "Каждому путнику дано имя и знак.\n"
        f"Твой знак: {message.from_user.id}\n"
        f"Имя, которым ты известен: {uname}",
    )


@bot.message_handler(commands=["mellon"])
def cmd_start(message):
    uid = message.from_user.id

    if is_allowed(uid):
        bot.reply_to(
            message,
            "Ты уже допущен к знаниям.\n"
            "Молви: «Легэндальф, выдай базу» — и истина откроется тебе.",
        )
        return

    created = add_pending(message.from_user)
    if created:
        notify_admins_new_request(message.from_user)

    bot.reply_to(
        message,
        "Ты не пройдёшь.\n"
        "Я передал твою просьбу хранителю врат.\n"
        "Мудрость приходит к тем, кто умеет ждать.",
    )


@bot.message_handler(commands=["pending"])
def cmd_pending(message):
    if not is_admin(message.from_user.id):
        return

    data = load_json()
    pending = data.get("pending", {})
    if not pending:
        bot.reply_to(
            message,
            "Сейчас никто не стоит у врат.\n"
            "Тишина — редкий, но добрый знак.",
        )
        return

    lines = ["У врат ждут следующие путники:"]
    for uid, meta in pending.items():
        lines.append(format_user_line(uid, meta))
    lines.append("\nОткрыть путь: /allow <id>\nОтказать: /deny <id>")

    bot.reply_to(message, "\n".join(lines))


@bot.message_handler(commands=["users"])
def cmd_users(message):
    if not is_admin(message.from_user.id):
        return

    parts = (message.text or "").split()
    if len(parts) == 1:
        bot.reply_to(message, _build_user_overview_text())
        return

    if len(parts) < 3:
        bot.reply_to(message, "Формат: /users <id|username> ДД.ММ.ГГГГ")
        return

    identifier = parts[1]
    date_token = parts[2]
    try:
        born = datetime.strptime(date_token, "%d.%m.%Y").date()
    except ValueError:
        bot.reply_to(message, "Не понял дату. Пример: 12.11.1993")
        return

    result = _set_user_birthday(identifier, born)
    if not result:
        bot.reply_to(message, "Не нашёл такого путника среди допущенных или ожидающих.")
        return

    bucket_name, suid, meta = result
    display = _display_name(meta, int(suid))
    scope = "среди допущенных" if bucket_name == "allowed" else "в очереди у врат"
    bot.reply_to(
        message,
        f"Записал день рождения {display}: {born.strftime('%d.%m.%Y')} ({scope}).",
    )


@bot.message_handler(commands=["allow"])
def cmd_allow(message):
    if not is_admin(message.from_user.id):
        return

    parts = (message.text or "").split()
    if len(parts) != 2 or not parts[1].isdigit():
        bot.reply_to(message, "Используй мудро: /allow <user_id>")
        return

    uid = int(parts[1])
    approve_user(uid)
    bot.reply_to(
        message,
        f"Решение принято.\n"
        f"Путнику со знаком {uid} открыт путь.",
    )

    try:
        bot.send_message(
            uid,
            "Врата открыты.\n"
            "Тебе дозволено спрашивать.\n"
            "Скажи: «Легэндальф, выдай базу».",
        )
    except Exception:
        pass


@bot.message_handler(commands=["deny"])
def cmd_deny(message):
    if not is_admin(message.from_user.id):
        return

    parts = (message.text or "").split()
    if len(parts) != 2 or not parts[1].isdigit():
        bot.reply_to(message, "Используй мудро: /deny <user_id>")
        return

    uid = int(parts[1])
    deny_user(uid)
    bot.reply_to(
        message,
        f"Ты отказал путнику со знаком {uid}.\n"
        "Такова воля хранителя.",
    )

    try:
        bot.send_message(
            uid,
            "Пока путь для тебя закрыт.\n"
            "Не всякий отказ — конец дороги.",
        )
    except Exception:
        pass


def _admin_gate_callback(call):
    data = call.data or ""
    return data.startswith("approve:") or data.startswith("deny:")


@bot.callback_query_handler(func=_admin_gate_callback)
def callbacks(call):
    if not is_admin(call.from_user.id):
        bot.answer_callback_query(call.id, "Эта власть тебе не дана.")
        return

    data = call.data or ""
    if data.startswith("approve:"):
        uid = int(data.split(":", 1)[1])
        approve_user(uid)
        bot.answer_callback_query(call.id, "Врата открыты.")
        try:
            bot.send_message(
                uid,
                "Врата открыты.\n"
                "Тебе дозволено спрашивать.\n"
                "Скажи: «Легэндальф, выдай базу».",
            )
        except Exception:
            pass
        try:
            bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
        except Exception:
            pass

    elif data.startswith("deny:"):
        uid = int(data.split(":", 1)[1])
        deny_user(uid)
        bot.answer_callback_query(call.id, "Решение принято.")
        try:
            bot.send_message(
                uid,
                "Пока путь для тебя закрыт.\n"
                "Не всякий отказ — конец дороги.",
            )
        except Exception:
            pass
        try:
            bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
        except Exception:
            pass


# Админ:
# - "Сохрани базу: ..." -> сохранить цитату
# - фото/гиф/видео/mp4/документ -> сохранить медиа
# - секретные фразы -> отработать как у всех
@bot.message_handler(
    func=lambda m: is_admin(m.from_user.id)
    and (m.content_type != "text" or not (m.text or "").startswith("/")),
    content_types=["text", "photo", "document", "video", "animation"],
)
def admin_ingest(message):
    if message.content_type == "text":
        text = (message.text or "").strip()

        # команды оставляем стандартным хэндлерам (/mellon, /id, /schedule, etc.)
        if text.startswith("/"):
            return

        # секретные фразы должны работать и для админа
        if is_media_trigger(text):
            send_random_media(message.chat.id, reply_to_message_id=message.message_id)
            return

        if is_trigger(text):
            bot.reply_to(message, random_quote())
            return

        # сохраняем цитату ТОЛЬКО по "Сохрани базу: ..."
        m = SAVE_QUOTE_PREFIX_RE.match(text)
        if m:
            quote = m.group(1).strip()
            if append_quote(quote):
                bot.reply_to(message, "Слова записаны в свиток. База стала богаче.")
            else:
                bot.reply_to(message, "Я слышу тишину. Пришли слова, и я запишу их.")
            return

        # иначе молчим, чтобы не мешать админу
        return

    if message.content_type in ("photo", "document", "video", "animation"):
        bot.send_message(
            message.chat.id,
            "Принял видение. Записываю его в свитки…",
            reply_parameters=_build_reply_parameters(message.message_id),
        )
        ok, info = save_media_from_message(message)
        if ok:
            bot.send_message(
                message.chat.id,
                f"Видение сохранено.\n{info}",
                reply_parameters=_build_reply_parameters(message.message_id),
            )
        else:
            bot.send_message(
                message.chat.id,
                f"Не удалось сохранить видение.\n{info}",
                reply_parameters=_build_reply_parameters(message.message_id),
            )
        return


@bot.message_handler(func=lambda m: not ((m.text or "").startswith("/")), content_types=["text"])
def on_message(message):
    uid = message.from_user.id

    if not is_allowed(uid):
        bot.reply_to(
            message,
            "Прежде чем искать ответы, нужно попросить дозволения.\n"
            "Напиши /mellon — и я передам твоё имя дальше.",
        )
        return

    # "Гэндальф?" -> случайная картинка/гиф/видео + подпись-цитата
    if is_media_trigger(message.text):
        send_random_media(
            message.chat.id,
            reply_to_message_id=message.message_id,
        )
        return

    if is_trigger(message.text):
        bot.reply_to(message, random_quote())
    else:
        bot.reply_to(
            message,
            "Слова имеют значение.\n"
            "Обратись так: «Легэндальф, выдай базу».\n"
            "Или спроси: «Гэндальф?»",
        )


if __name__ == "__main__":
    logger.info("Legendalf bot starting. Logs: %s", LOG_FILE)
    setup_commands()
    announce_startup()
    _start_journalctl_forwarder("legendalf")

    # Регистрируем модуль расписаний «База дня»
    schedule.register(
        bot,
        DATA_FILE,
        QUOTES_FILE,
        MEDIA_DIR,
        is_allowed,
        default_tz="Europe/Moscow",  # FIX: без \"...\"
        poll_interval_sec=30,
        holiday_service=holiday_service,
    )
    films.register(bot, is_allowed)
    films_day.register(bot, is_allowed)

    bot.infinity_polling(timeout=90, long_polling_timeout=60, skip_pending=True)

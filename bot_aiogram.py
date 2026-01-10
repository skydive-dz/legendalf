from __future__ import annotations

import asyncio
import logging
import os
import random
import re
import shutil
import time
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
import socket
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.exceptions import TelegramNetworkError
from aiogram.dispatcher.event.bases import SkipHandler
from aiogram.filters import Command
from aiogram.types import (
    BotCommand,
    BotCommandScopeChat,
    CallbackQuery,
    FSInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

import schedule_aiogram
from features import films as features_films
from features import holidays as features_holidays
import storage_sqlite
from retry_utils import retry_async, RETRY_DELAYS_SHORT

BASE_DIR = Path(__file__).resolve().parent
DB_FILE = BASE_DIR / "users.db"
JSON_MIGRATION_FILE = BASE_DIR / "users.json"
QUOTES_FILE = BASE_DIR / "quotes.txt"
MEDIA_DIR = BASE_DIR / "image"
LOG_DIR = BASE_DIR / "logs"
LOG_FILE = LOG_DIR / "legendalf.log"
SYSTEM_LOG_FILE = LOG_DIR / "legendalf.system.log"

KIND_BASE = "base"
KIND_HOLIDAYS = "holidays"

logger = logging.getLogger("legendalf.aiogram")
system_logger = logging.getLogger("legendalf.system")

COMMON_COMMANDS = [
    ("mellon", "Молви «друг» и войди"),
    ("id", "Узнать свой знак (user_id)"),
    ("schedule", "График рассылки"),
    ("holydays", "Праздники сегодняшнего дня"),
    ("films_day", "Премьеры дня"),
    ("films_month", "Премьеры месяца (Кинопоиск)"),
]

ADMIN_ONLY_COMMANDS = [
    ("pending", "Список путников у врат"),
    ("allow", "Открыть путь путнику"),
    ("deny", "Отказать путнику"),
    ("users", "Путники"),
    ("restart", "Перезапустить"),
]

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

SAVE_QUOTE_PREFIX_RE = re.compile(r"^\s*сохрани\s+базу\s*:\s*(.+)\s*$", re.IGNORECASE)


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


def load_quotes() -> list[str]:
    if not QUOTES_FILE.exists():
        return ["База пока не записана: положи цитаты в quotes.txt, и они оживут."]
    lines = [line.strip() for line in QUOTES_FILE.read_text(encoding="utf-8").splitlines()]
    lines = [l for l in lines if l]
    return lines or ["База пуста: даже мудрость молчит, если её не записали."]


def random_quote() -> str:
    return random.choice(load_quotes())


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


def list_media() -> list[Path]:
    if not MEDIA_DIR.exists() or not MEDIA_DIR.is_dir():
        return []
    exts = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".mp4"}
    return [p for p in MEDIA_DIR.iterdir() if p.is_file() and p.suffix.lower() in exts]


async def safe_answer(message: Message, text: str, **kwargs) -> bool:
    return await retry_async(
        lambda: message.answer(text, **kwargs),
        logger=logger,
        delays=RETRY_DELAYS_SHORT,
        retry_exceptions=(TelegramNetworkError, asyncio.TimeoutError),
    )


async def safe_media_send(task, label: str) -> bool:
    return await retry_async(
        task,
        logger=logger,
        label=label,
        retry_exceptions=(TelegramNetworkError, asyncio.TimeoutError),
    )


async def send_random_media(message: Message, caption: str | None = None) -> None:
    media = list_media()
    if not media:
        await safe_answer(
            message,
            "Я не вижу свитков с образами и видениями в моей папке.\n"
            f"Положи файлы в: {MEDIA_DIR}"
        )
        return
    path = random.choice(media)
    media_caption = caption if caption is not None else random_quote()
    ext = path.suffix.lower()
    file = FSInputFile(path)
    try:
        if ext in {".jpg", ".jpeg", ".png", ".webp"}:
            sent = await safe_media_send(lambda: message.answer_photo(file, caption=media_caption), "send photo")
        elif ext == ".gif":
            sent = await safe_media_send(lambda: message.answer_animation(file, caption=media_caption), "send animation")
        elif ext == ".mp4":
            sent = await safe_media_send(lambda: message.answer_video(file, caption=media_caption), "send video")
        else:
            sent = await safe_media_send(lambda: message.answer_document(file, caption=media_caption), "send document")
        if sent is False:
            await safe_answer(message, "Воля была, но видение не открылось. Проверь файл и права доступа.")
    except Exception:
        await safe_answer(message, "Воля была, но видение не открылось. Проверь файл и права доступа.")


async def _download_telegram_file(bot: Bot, file_path: str, out_path: Path) -> None:
    url = f"https://api.telegram.org/file/bot{bot.token}/{file_path}"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    async with aiohttp.ClientSession() as session:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=120)) as resp:
            resp.raise_for_status()
            with open(out_path, "wb") as f:
                async for chunk in resp.content.iter_chunked(1024 * 64):
                    f.write(chunk)


async def save_media_from_message(message: Message) -> tuple[bool, str]:
    file_id = None
    suffix = ""
    if message.photo:
        file_id = message.photo[-1].file_id
        suffix = ".jpg"
    elif message.animation:
        file_id = message.animation.file_id
        if message.animation.file_name:
            suffix = Path(message.animation.file_name).suffix.lower()
    elif message.video:
        file_id = message.video.file_id
        if message.video.file_name:
            suffix = Path(message.video.file_name).suffix.lower()
    elif message.document:
        file_id = message.document.file_id
        if message.document.file_name:
            suffix = Path(message.document.file_name).suffix.lower()

    if not file_id:
        return False, "Я не вижу здесь ни образа, ни видения, которое можно сохранить."
    if suffix not in {".jpg", ".jpeg", ".png", ".webp", ".gif", ".mp4"}:
        return False, "Это не похоже ни на образ, ни на видение (jpg/png/gif/mp4)."

    try:
        f_info = await message.bot.get_file(file_id)
        name = f"{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{file_id[:8]}{suffix}"
        out_path = MEDIA_DIR / name
        await _download_telegram_file(message.bot, f_info.file_path, out_path)
        return True, str(out_path)
    except aiohttp.ClientError:
        return False, "Сеть дрогнула, и видение не дошло до свитка. Попробуй ещё раз."
    except Exception as exc:
        logger.warning("Failed to save media: %s", exc)
        return False, "Видение ускользнуло при сохранении. Проверь права и место на диске."


def now_iso_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def default_data() -> dict:
    return {"admins": [], "allowed": {}, "pending": {}, "schedules": {}}


def _read_cpu_times() -> tuple[int, int]:
    line = Path("/proc/stat").read_text(encoding="utf-8").splitlines()[0]
    parts = [int(x) for x in line.split()[1:]]
    idle = parts[3] + (parts[4] if len(parts) > 4 else 0)
    total = sum(parts)
    return idle, total


def _read_meminfo() -> tuple[int, int]:
    mem_total = 0
    mem_available = 0
    for line in Path("/proc/meminfo").read_text(encoding="utf-8").splitlines():
        if line.startswith("MemTotal:"):
            mem_total = int(line.split()[1])
        elif line.startswith("MemAvailable:"):
            mem_available = int(line.split()[1])
        if mem_total and mem_available:
            break
    return mem_total, mem_available


async def system_metrics_loop(interval_sec: int = 60) -> None:
    prev_idle = None
    prev_total = None
    while True:
        try:
            idle, total = _read_cpu_times()
            cpu_pct = None
            if prev_idle is not None and prev_total is not None:
                idle_delta = idle - prev_idle
                total_delta = total - prev_total
                if total_delta > 0:
                    cpu_pct = (1 - idle_delta / total_delta) * 100
            prev_idle, prev_total = idle, total

            mem_total, mem_available = _read_meminfo()
            mem_pct = None
            if mem_total:
                mem_pct = (mem_total - mem_available) / mem_total * 100

            disk = shutil.disk_usage(BASE_DIR)
            disk_pct = disk.used / disk.total * 100 if disk.total else None

            parts = []
            if cpu_pct is not None:
                parts.append(f"cpu={cpu_pct:.1f}%")
            if mem_pct is not None:
                parts.append(f"mem={mem_pct:.1f}%")
            if disk_pct is not None:
                parts.append(f"disk={disk_pct:.1f}%")
            if parts:
                system_logger.info("metrics %s", " ".join(parts))
        except Exception as exc:
            system_logger.warning("metrics error: %s", exc)

        await asyncio.sleep(interval_sec)


def load_json() -> dict:
    return storage_sqlite.load_data(DB_FILE, JSON_MIGRATION_FILE)


def save_json(data: dict) -> None:
    storage_sqlite.save_data(DB_FILE, data)


def user_meta(user) -> dict:
    return {
        "username": getattr(user, "username", None),
        "first_name": getattr(user, "first_name", None),
        "last_name": getattr(user, "last_name", None),
    }


def is_admin(uid: int) -> bool:
    data = load_json()
    admins = data.get("admins", [])
    return uid in admins or str(uid) in admins


def is_allowed(uid: int) -> bool:
    data = load_json()
    suid = str(uid)
    return is_admin(uid) or suid in data.get("allowed", {})


def add_pending(user) -> bool:
    data = load_json()
    uid = str(user.id)
    if is_admin(user.id) or uid in data.get("allowed", {}):
        return False
    if uid in data.get("pending", {}):
        return False
    data["pending"][uid] = {**user_meta(user), "requested_at": now_iso_utc()}
    save_json(data)
    return True


def approve_user(uid: int) -> None:
    data = load_json()
    suid = str(uid)
    meta = data["pending"].pop(suid, None) or {}
    meta.pop("requested_at", None)
    data["allowed"][suid] = {**meta, "added_at": now_iso_utc()}
    save_json(data)


def deny_user(uid: int) -> None:
    data = load_json()
    suid = str(uid)
    if suid in data.get("pending", {}):
        data["pending"].pop(suid, None)
        save_json(data)


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
            lines.append(f"  TZ: {tz}; база: {base_desc}; праздники: {holidays_desc}; ДР: {birthday}")
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


def format_user_line(uid: str, meta: dict) -> str:
    uname = f"@{meta.get('username')}" if meta.get("username") else "(no username)"
    name = f"{meta.get('first_name') or ''} {meta.get('last_name') or ''}".strip() or "(no name)"
    ts = meta.get("requested_at") or meta.get("added_at") or ""
    return f"- {uid} | {uname} | {name} | {ts}"


def _commands_text(items: list[tuple[str, str]]) -> str:
    return "\n".join(f"/{cmd} — {desc}" for cmd, desc in items)


async def notify_admins_new_request(bot: Bot, user) -> None:
    data = load_json()
    admins = data.get("admins", [])
    if not admins:
        return

    uid = user.id
    uname = f"@{user.username}" if user.username else "(no username)"
    name = f"{user.first_name or ''} {user.last_name or ''}".strip() or "(no name)"
    text = (
        "У врат появился путник и просит допустить его к мудрости Legendalf:\n"
        f"- знак: {uid}\n"
        f"- имя: {uname}\n"
        f"- как зовут в миру: {name}\n\n"
        "Откроем ли ему путь?"
    )
    markup = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Открыть врата", callback_data=f"approve:{uid}")],
            [InlineKeyboardButton(text="❌ Оставить снаружи", callback_data=f"deny:{uid}")],
        ]
    )
    for admin_id in admins:
        try:
            await bot.send_message(int(admin_id), text, reply_markup=markup)
        except Exception:
            pass


async def setup_commands(bot: Bot) -> None:
    common_commands = [BotCommand(command=cmd, description=desc) for cmd, desc in COMMON_COMMANDS]
    admin_only_commands = [BotCommand(command=cmd, description=desc) for cmd, desc in ADMIN_ONLY_COMMANDS]

    try:
        await bot.set_my_commands(common_commands)
    except Exception as exc:
        logger.warning("Failed to set common commands: %s", exc)

    admins = load_json().get("admins", [])
    for admin_id in admins:
        try:
            await bot.set_my_commands(
                common_commands + admin_only_commands,
                scope=BotCommandScopeChat(chat_id=int(admin_id)),
            )
        except Exception as exc:
            logger.warning("Failed to set commands for admin %s: %s", admin_id, exc)


async def notify_admins_start(bot: Bot) -> None:
    admins = load_json().get("admins", [])
    if not admins:
        return
    text = "Я служитель вечного огня, повелитель пламени Анора!"
    for admin_id in admins:
        try:
            await bot.send_message(int(admin_id), text)
        except Exception as exc:
            logger.warning("Failed to send start notice to admin %s: %s", admin_id, exc)


async def notify_admins_ready(
    bot: Bot,
    *,
    delay_sec: int = 1,
    active_state: dict | None = None,
    ready_state: dict | None = None,
) -> None:
    await asyncio.sleep(delay_sec)
    if active_state is not None and not active_state.get("value", False):
        return
    if ready_state is not None and ready_state.get("sent"):
        return
    await notify_admins_start(bot)
    if ready_state is not None:
        ready_state["sent"] = True


def _is_timeout_exc(exc: BaseException) -> bool:
    if isinstance(exc, (asyncio.TimeoutError, aiohttp.ClientConnectionError)):
        return True
    if isinstance(exc, TelegramNetworkError):
        msg = str(exc)
        return "timeout" in msg.lower()
    msg = str(exc)
    return "timeout" in msg.lower()


async def keepalive(bot: Bot, interval_sec: int = 300) -> None:
    tick = 0
    while True:
        await asyncio.sleep(interval_sec)
        try:
            await bot.get_me()
            tick += 1
            if tick % 6 == 0:
                logger.info("Keepalive OK (last %d minutes)", tick * interval_sec // 60)
        except Exception as exc:
            logger.warning("Keepalive getMe failed: %s", exc)


router = Router()


@router.message(Command("id"))
async def cmd_id(message: Message) -> None:
    logger.info("Command /id from %s", message.from_user.id)
    uname = f"@{message.from_user.username}" if message.from_user.username else "(no username)"
    await safe_answer(message, 
        "Каждому путнику дано имя и знак.\n"
        f"Твой знак: {message.from_user.id}\n"
        f"Имя, которым ты известен: {uname}"
    )




@router.message(Command("mellon"))
async def cmd_mellon(message: Message) -> None:
    logger.info("Command /mellon from %s", message.from_user.id)
    uid = message.from_user.id

    if is_allowed(uid):
        await safe_answer(message, 
            "Ты уже допущен к знаниям.\n"
            "Молви: «Легэндальф, выдай базу» — и истина откроется тебе."
        )
        return

    created = add_pending(message.from_user)
    if created:
        await notify_admins_new_request(message.bot, message.from_user)

    await safe_answer(message, 
        "Ты не пройдёшь.\n"
        "Я передал твою просьбу хранителю врат.\n"
        "Мудрость приходит к тем, кто умеет ждать."
    )


@router.message(Command("pending"))
async def cmd_pending(message: Message) -> None:
    logger.info("Command /pending from %s", message.from_user.id)
    if not is_admin(message.from_user.id):
        return

    data = load_json()
    pending = data.get("pending", {})
    if not pending:
        await safe_answer(message, 
            "Сейчас никто не стоит у врат.\n"
            "Тишина — редкий, но добрый знак."
        )
        return

    lines = ["У врат ждут следующие путники:"]
    for uid, meta in pending.items():
        lines.append(format_user_line(uid, meta))
    lines.append("\nОткрыть путь: /allow <id>\nОтказать: /deny <id>")
    await safe_answer(message, "\n".join(lines))


@router.message(Command("users"))
async def cmd_users(message: Message) -> None:
    logger.info("Command /users from %s", message.from_user.id)
    if not is_admin(message.from_user.id):
        return

    parts = (message.text or "").split()
    if len(parts) == 1:
        await safe_answer(message, _build_user_overview_text())
        return

    if len(parts) < 3:
        await safe_answer(message, "Формат: /users <id|username> ДД.ММ.ГГГГ")
        return

    identifier = parts[1]
    date_token = parts[2]
    try:
        born = datetime.strptime(date_token, "%d.%m.%Y").date()
    except ValueError:
        await safe_answer(message, "Не понял дату. Пример: 12.11.1993")
        return

    result = _set_user_birthday(identifier, born)
    if not result:
        await safe_answer(message, "Не нашёл такого путника среди допущенных или ожидающих.")
        return

    bucket_name, suid, meta = result
    display = _display_name(meta, int(suid))
    scope = "среди допущенных" if bucket_name == "allowed" else "в очереди у врат"
    await safe_answer(message, f"Записал день рождения {display}: {born.strftime('%d.%m.%Y')} ({scope}).")


@router.message(Command("allow"))
async def cmd_allow(message: Message) -> None:
    logger.info("Command /allow from %s", message.from_user.id)
    if not is_admin(message.from_user.id):
        return
    parts = (message.text or "").split()
    if len(parts) != 2 or not parts[1].isdigit():
        await safe_answer(message, "Используй мудро: /allow <user_id>")
        return
    uid = int(parts[1])
    approve_user(uid)
    await safe_answer(message, "Решение принято.\n" f"Путнику со знаком {uid} открыт путь.")
    try:
        await message.bot.send_message(
            uid,
            "Врата открыты.\n"
            "Тебе дозволено спрашивать.\n"
            "Скажи: «Легэндальф, выдай базу».",
        )
    except Exception:
        pass


@router.message(Command("deny"))
async def cmd_deny(message: Message) -> None:
    logger.info("Command /deny from %s", message.from_user.id)
    if not is_admin(message.from_user.id):
        return
    parts = (message.text or "").split()
    if len(parts) != 2 or not parts[1].isdigit():
        await safe_answer(message, "Используй мудро: /deny <user_id>")
        return
    uid = int(parts[1])
    deny_user(uid)
    await safe_answer(message, f"Ты отказал путнику со знаком {uid}.\n" "Такова воля хранителя.")
    try:
        await message.bot.send_message(
            uid,
            "Пока путь для тебя закрыт.\n"
            "Не всякий отказ — конец дороги.",
        )
    except Exception:
        pass


@router.message(Command("restart"))
async def cmd_restart(message: Message) -> None:
    uid = message.from_user.id
    if not is_admin(uid):
        logger.warning("User %s attempted /restart without permissions", uid)
        return

    logger.info("Admin %s requested /restart", uid)
    await safe_answer(message, 
        "Ты призвал меня начать путь заново."
    )
    logger.info("Restarting process via execv")
    os.execv(os.sys.executable, [os.sys.executable] + os.sys.argv)


@router.callback_query(F.data.startswith("approve:"))
async def cb_approve(call: CallbackQuery) -> None:
    if not is_admin(call.from_user.id):
        await call.answer("Эта власть тебе не дана.")
        return
    uid = int(call.data.split(":", 1)[1])
    approve_user(uid)
    await call.answer("Врата открыты.")
    try:
        await call.message.bot.send_message(
            uid,
            "Врата открыты.",
        )
    except Exception:
        pass
    try:
        await call.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass


@router.callback_query(F.data.startswith("deny:"))
async def cb_deny(call: CallbackQuery) -> None:
    if not is_admin(call.from_user.id):
        await call.answer("Эта власть тебе не дана.")
        return
    uid = int(call.data.split(":", 1)[1])
    deny_user(uid)
    await call.answer("Решение принято.")
    try:
        await call.message.bot.send_message(
            uid,
            "Пока путь для тебя закрыт.\n"
            "Не всякий отказ — конец дороги.",
        )
    except Exception:
        pass
    try:
        await call.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass




@router.message(F.text & ~F.text.startswith("/"))
async def on_text(message: Message) -> None:
    uid = message.from_user.id
    text = (message.text or "").strip()

    if uid in schedule_aiogram._pending_add_kind or uid in schedule_aiogram._pending_del_kind:
        raise SkipHandler

    if is_admin(uid):
        if text.startswith("/"):
            return

        if is_media_trigger(text):
            await send_random_media(message)
            return
        if is_trigger(text):
            await safe_answer(message, random_quote())
            return

        m = SAVE_QUOTE_PREFIX_RE.match(text)
        if m:
            quote = m.group(1).strip()
            if append_quote(quote):
                await safe_answer(message, "Слова записаны в свиток. База стала богаче.")
            else:
                await safe_answer(message, "Я слышу тишину. Пришли слова, и я запишу их.")
            return

        return

    if not is_allowed(uid):
        await safe_answer(message, 
            "Прежде чем искать ответы, нужно попросить дозволения.\n"
            "Напиши /mellon — и я передам твоё имя дальше."
        )
        return

    if is_media_trigger(text):
        await send_random_media(message)
        return
    if is_trigger(text):
        await safe_answer(message, random_quote())
        return

    await safe_answer(message, 
        "Слова имеют значение.\n"
        "Обратись так: «Легэндальф, выдай базу».\n"
        "Или спроси: «Гэндальф?»"
    )


@router.message(F.photo | F.document | F.video | F.animation)
async def on_media(message: Message) -> None:
    uid = message.from_user.id
    if not is_admin(uid):
        return
    await safe_answer(message, "Принял видение. Записываю его в свитки…")
    ok, info = await save_media_from_message(message)
    if ok:
        await safe_answer(message, f"Видение сохранено.\n{info}")
    else:
        await safe_answer(message, f"Не удалось сохранить видение.\n{info}")


async def run_polling() -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        raise RuntimeError("Set TELEGRAM_BOT_TOKEN env var with your bot token")

    if not getattr(run_polling, "_ipv4_forced", False):
        original_getaddrinfo = socket.getaddrinfo

        def ipv4_only_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
            return original_getaddrinfo(host, port, socket.AF_INET, type, proto, flags)

        socket.getaddrinfo = ipv4_only_getaddrinfo
        setattr(run_polling, "_ipv4_forced", True)
        logger.info("IPv4-only DNS resolver enabled")

    timeout_streak = 0
    timeout_limit = 3
    use_client_timeout = True
    ready_state = {"sent": False}
    dp = Dispatcher()
    dp.include_router(router)
    dp.include_router(schedule_aiogram.router)
    dp.include_router(features_holidays.router)
    dp.include_router(features_films.router)
    allowed_updates = dp.resolve_used_update_types()
    logger.info("Dispatcher initialized. Allowed updates: %s", allowed_updates)

    while True:
        logger.info("Starting polling cycle")
        bot = None
        keepalive_task = None
        scheduler_task = None
        metrics_task = None
        ready_task = None
        polling_active = {"value": False}
        try:
            if use_client_timeout:
                timeout = aiohttp.ClientTimeout(
                    total=60,
                    connect=10,
                    sock_connect=10,
                    sock_read=40,
                )
                connector = aiohttp.TCPConnector(
                    family=socket.AF_INET,
                    ttl_dns_cache=300,
                )
                try:
                    session = AiohttpSession(timeout=timeout, connector=connector)
                except TypeError as exc:
                    logger.warning("AiohttpSession connector unsupported, fallback to timeout only: %s", exc)
                    session = AiohttpSession(timeout=timeout)
            else:
                session = AiohttpSession(timeout=30)

            bot = Bot(token, session=session)
            keepalive_task = asyncio.create_task(keepalive(bot))
            scheduler_task = asyncio.create_task(schedule_aiogram.scheduler_loop(bot))
            metrics_task = asyncio.create_task(system_metrics_loop())

            logger.info("Configuring features and schedule")
            schedule_aiogram.configure(
                data_file=DB_FILE,
                quotes_file=QUOTES_FILE,
                media_dir=MEDIA_DIR,
                default_tz="Europe/Moscow",
                poll_interval_sec=30,
                holiday_service=None,
                is_allowed_fn=is_allowed,
            )
            features_holidays.configure(
                default_tz="Europe/Moscow",
                holiday_service=None,
                is_allowed_fn=is_allowed,
            )
            features_films.configure(
                is_allowed_fn=is_allowed,
            )
            logger.info("Setting bot commands in background")
            asyncio.create_task(setup_commands(bot))
            polling_active["value"] = True
            ready_task = asyncio.create_task(
                notify_admins_ready(
                    bot,
                    delay_sec=1,
                    active_state=polling_active,
                    ready_state=ready_state,
                )
            )
            logger.info("Starting long polling")
            await dp.start_polling(
                bot,
                allowed_updates=allowed_updates,
                polling_timeout=20,
            )
            timeout_streak = 0
            logger.info("Polling stopped cleanly")
        except Exception as exc:
            msg = str(exc)
            if "ClientTimeout" in msg and "int" in msg:
                use_client_timeout = False
                logger.warning("ClientTimeout incompatible, falling back to numeric timeout")
            if _is_timeout_exc(exc):
                timeout_streak += 1
                logger.warning("Polling timeout (%d/%d): %s", timeout_streak, timeout_limit, exc)
                if timeout_streak >= timeout_limit:
                    logger.error("Restarting after %d consecutive timeouts", timeout_streak)
                    os.execv(os.sys.executable, [os.sys.executable] + os.sys.argv)
            else:
                timeout_streak = 0
                logger.exception("Polling error: %s", exc)
            await asyncio.sleep(3)
        finally:
            logger.info("Stopping background tasks and closing session")
            polling_active["value"] = False
            if keepalive_task:
                keepalive_task.cancel()
            if scheduler_task:
                scheduler_task.cancel()
            if metrics_task:
                metrics_task.cancel()
            if ready_task:
                ready_task.cancel()
            if bot:
                await bot.session.close()


def main() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_format = "%(asctime)s %(levelname)s [%(name)s] %(message)s"

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    file_handler = TimedRotatingFileHandler(
        LOG_FILE, when="midnight", backupCount=7, encoding="utf-8"
    )
    file_handler.setFormatter(logging.Formatter(log_format))

    system_handler = TimedRotatingFileHandler(
        SYSTEM_LOG_FILE, when="midnight", backupCount=7, encoding="utf-8"
    )
    system_handler.setFormatter(logging.Formatter(log_format))

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(logging.Formatter(log_format))

    root_logger.handlers.clear()
    root_logger.addHandler(file_handler)
    root_logger.addHandler(stream_handler)

    system_logger.handlers.clear()
    system_logger.propagate = False
    system_logger.setLevel(logging.INFO)
    system_logger.addHandler(system_handler)
    system_logger.addHandler(stream_handler)

    system_logger.info("Legendalf system logger initialized")
    logger.info("Legendalf bot starting")
    asyncio.run(run_polling())


if __name__ == "__main__":
    main()

import json
import random
import re
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore


# Принимаем HH:MM и HH.MM
_TIME_COLON_RE = re.compile(r"^(?:[01]\d|2[0-3]):[0-5]\d$")
_TIME_DOT_RE = re.compile(r"^(?:[01]\d|2[0-3])\.[0-5]\d$")
_INT_RE = re.compile(r"^\d{1,6}$")  # минуты (разумный верхний предел проверим отдельно)

_TZ_EXAMPLE = "Europe/Moscow"


def _now_iso_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_json(data_file: Path) -> dict:
    if not data_file.exists():
        data = {"admins": [], "allowed": {}, "pending": {}, "schedules": {}}
        data_file.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        return data

    data = json.loads(data_file.read_text(encoding="utf-8"))
    data.setdefault("admins", [])
    data.setdefault("allowed", {})
    data.setdefault("pending", {})
    data.setdefault("schedules", {})

    if isinstance(data.get("allowed"), list):
        data["allowed"] = {str(uid): {"added_at": _now_iso_utc()} for uid in data["allowed"]}
    if isinstance(data.get("pending"), list):
        data["pending"] = {str(uid): {"requested_at": _now_iso_utc()} for uid in data.get("pending", [])}

    if not isinstance(data.get("schedules"), dict):
        data["schedules"] = {}

    _save_json(data_file, data)
    return data


def _save_json(data_file: Path, data: dict) -> None:
    data_file.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _load_quotes(quotes_file: Path) -> list[str]:
    if not quotes_file.exists():
        return ["База пока молчит: заполни quotes.txt, и мудрость оживёт."]
    lines = [l.strip() for l in quotes_file.read_text(encoding="utf-8").splitlines()]
    lines = [l for l in lines if l]
    return lines or ["База пуста: даже мудрость молчит, если её не записали."]


def _random_quote(quotes_file: Path) -> str:
    return random.choice(_load_quotes(quotes_file))


def _list_media(media_dir: Path) -> list[Path]:
    if not media_dir.exists() or not media_dir.is_dir():
        return []
    exts = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".mp4"}
    return [p for p in media_dir.iterdir() if p.is_file() and p.suffix.lower() in exts]


def _send_random_media_with_caption(bot, chat_id: int, media_dir: Path, caption: str) -> None:
    media = _list_media(media_dir)
    if not media:
        bot.send_message(
            chat_id,
            "Я не вижу свитков с образами и видениями в моей папке.\n"
            f"Положи файлы в: {media_dir}"
        )
        return

    path = random.choice(media)
    try:
        with open(path, "rb") as f:
            ext = path.suffix.lower()
            if ext in {".jpg", ".jpeg", ".png", ".webp"}:
                bot.send_photo(chat_id, f, caption=caption)
            elif ext == ".gif":
                bot.send_animation(chat_id, f, caption=caption)
            elif ext == ".mp4":
                bot.send_video(chat_id, f, caption=caption)
            else:
                bot.send_document(chat_id, f, caption=caption)
    except Exception:
        bot.send_message(chat_id, "Воля была, но видение не открылось. Проверь файл и права доступа.")


def _get_tz(tz_name: str):
    if ZoneInfo is None:
        return None
    try:
        return ZoneInfo(tz_name)
    except Exception:
        return None


def _local_now(tz_name: str) -> datetime:
    tz = _get_tz(tz_name)
    if tz is None:
        return datetime.now(timezone.utc)
    return datetime.now(tz)


def _ensure_user_schedule(data: dict, uid: int, default_tz: str) -> dict:
    """
    Единственный график на пользователя.
    entry:
      enabled: bool
      tz: str
      mode: "at" | "every" | "" (нет)
      at_time: "HH:MM" (если mode="at")
      every_min: int (если mode="every")
      last_sent:
        - для "at": {"HH:MM": "YYYY-MM-DD"}
        - для "every": {"ts": <unix_seconds>}
    """
    suid = str(uid)
    schedules = data.setdefault("schedules", {})
    entry = schedules.get(suid)
    if not isinstance(entry, dict):
        entry = {}
        schedules[suid] = entry

    entry.setdefault("enabled", True)
    entry.setdefault("tz", default_tz)
    entry.setdefault("mode", "")  # "at" | "every"
    entry.setdefault("at_time", "")
    entry.setdefault("every_min", 0)
    entry.setdefault("last_sent", {})  # see docstring
    return entry


def _render_schedule(entry: dict, default_tz: str) -> str:
    enabled = "включён" if entry.get("enabled") else "выключен"
    tz = entry.get("tz", default_tz)
    mode = entry.get("mode") or ""
    if mode == "at":
        at_time = entry.get("at_time") or "не задано"
        plan = f"раз в день в {at_time}"
    elif mode == "every":
        every = int(entry.get("every_min") or 0)
        plan = f"каждые {every} мин."
    else:
        plan = "не задан"

    return (
        "График рассылки «База дня»\n"
        f"- статус: {enabled}\n"
        f"- часовой пояс: {tz}\n"
        f"- режим: {plan}\n\n"
        "Команды:\n"
        "/schedule — показать график\n"
        "/schedule_add — задать/перезаписать график\n"
        "/schedule_del — удалить график\n"
        "/schedule_off — отключить\n"
        "/schedule_on — включить\n"
        f"/schedule_tz {_TZ_EXAMPLE} — установить часовой пояс"
    )


def _parse_schedule_input(text: str) -> tuple[str, str | int]:
    """
    Возвращает:
      ("at", "HH:MM")  или  ("every", minutes)
    Бросает ValueError при неверном вводе.
    """
    raw = (text or "").strip()

    if not raw:
        raise ValueError("empty")

    # 10.00 -> 10:00
    if _TIME_DOT_RE.match(raw):
        raw = raw.replace(".", ":")

    if _TIME_COLON_RE.match(raw):
        return ("at", raw)

    if _INT_RE.match(raw):
        minutes = int(raw)
        # разумные границы, чтобы не ломать планировщик:
        # min 5 минут, max 7 дней
        if minutes < 5:
            raise ValueError("too_small")
        if minutes > 7 * 24 * 60:
            raise ValueError("too_large")
        return ("every", minutes)

    raise ValueError("bad_format")


def register(
    bot,
    data_file: Path,
    quotes_file: Path,
    media_dir: Path,
    is_allowed_fn,
    *,
    default_tz: str = "Europe/Moscow",
    poll_interval_sec: int = 30,
) -> None:
    lock = threading.RLock()

    def load_data() -> dict:
        with lock:
            return _load_json(data_file)

    def save_data(d: dict) -> None:
        with lock:
            _save_json(data_file, d)

    def reply(message, text: str):
        bot.reply_to(message, text)

    def _require_allowed(message) -> bool:
        uid = message.from_user.id
        if not is_allowed_fn(uid):
            reply(
                message,
                "Прежде чем приказывать времени, нужно получить допуск.\n"
                "Напиши /start — и я передам твоё имя хранителю врат."
            )
            return False
        return True

    @bot.message_handler(commands=["schedule"])
    def cmd_schedule(message):
        if not _require_allowed(message):
            return
        data = load_data()
        entry = _ensure_user_schedule(data, message.from_user.id, default_tz)
        reply(message, _render_schedule(entry, default_tz))
        save_data(data)

    # --- Мастер-диалог для /schedule_add ---
    @bot.message_handler(commands=["schedule_add"])
    def cmd_schedule_add(message):
        if not _require_allowed(message):
            return

        reply(
            message,
            "Укажи время или шаг.\n"
            "Примеры:\n"
            "- 10:00 — раз в день в это время\n"
            "- 60 — каждые 60 минут\n\n"
            "Одновременно может быть только один график: новая настройка сотрёт предыдущую."
        )
        bot.register_next_step_handler(message, _schedule_add_next_step)

    def _schedule_add_next_step(message):
        # если пользователь прислал команду вместо значения — не ломаемся
        if (message.text or "").strip().startswith("/"):
            reply(message, "Я жду значение, а не заклинание. Пример: 10:00 или 60.")
            return

        uid = message.from_user.id
        if not is_allowed_fn(uid):
            reply(message, "Путь закрыт. Сначала /start.")
            return

        text = (message.text or "").strip()

        try:
            mode, val = _parse_schedule_input(text)
        except ValueError as e:
            code = str(e)
            if code == "too_small":
                reply(message, "Слишком часто. Минимум — 5 минут. Пример: 15 (каждые 15 минут).")
            elif code == "too_large":
                reply(message, "Слишком редко. Максимум — 10080 минут (7 дней).")
            else:
                reply(message, "Не понял формат. Введи 10:00 / 10.00 или число минут (например 60).")
            return

        data = load_data()
        entry = _ensure_user_schedule(data, uid, default_tz)

        # ВАЖНО: новая настройка затирает предыдущую
        entry["enabled"] = True
        entry["last_sent"] = {}

        if mode == "at":
            entry["mode"] = "at"
            entry["at_time"] = str(val)
            entry["every_min"] = 0
            reply(message, "Принято. Буду присылать «Базу дня» раз в день.\n\n" + _render_schedule(entry, default_tz))

        elif mode == "every":
            entry["mode"] = "every"
            entry["every_min"] = int(val)
            entry["at_time"] = ""
            # чтобы не отправить мгновенно прямо сейчас (если не хотите) —
            # можно задать last_sent.ts = now, тогда первая отправка будет через interval
            entry["last_sent"] = {"ts": int(time.time())}
            reply(message, "Принято. Буду присылать «Базу дня» по интервалу.\n\n" + _render_schedule(entry, default_tz))

        save_data(data)

    @bot.message_handler(commands=["schedule_del"])
    def cmd_schedule_del(message):
        if not _require_allowed(message):
            return
        data = load_data()
        entry = _ensure_user_schedule(data, message.from_user.id, default_tz)

        entry["mode"] = ""
        entry["at_time"] = ""
        entry["every_min"] = 0
        entry["last_sent"] = {}
        reply(message, "График стёрт. Время снова свободно.\n\n" + _render_schedule(entry, default_tz))
        save_data(data)

    @bot.message_handler(commands=["schedule_off"])
    def cmd_schedule_off(message):
        if not _require_allowed(message):
            return
        data = load_data()
        entry = _ensure_user_schedule(data, message.from_user.id, default_tz)
        entry["enabled"] = False
        reply(message, "Отключено. Часы умолкнут.\n\n" + _render_schedule(entry, default_tz))
        save_data(data)

    @bot.message_handler(commands=["schedule_on"])
    def cmd_schedule_on(message):
        if not _require_allowed(message):
            return
        data = load_data()
        entry = _ensure_user_schedule(data, message.from_user.id, default_tz)
        entry["enabled"] = True
        reply(message, "Включено. Часы заведены.\n\n" + _render_schedule(entry, default_tz))
        save_data(data)

    @bot.message_handler(commands=["schedule_tz"])
    def cmd_schedule_tz(message):
        if not _require_allowed(message):
            return

        parts = (message.text or "").split(maxsplit=1)
        if len(parts) != 2:
            reply(message, f"Используй мудро: /schedule_tz {_TZ_EXAMPLE}")
            return

        tz_name = parts[1].strip()
        if ZoneInfo is not None and _get_tz(tz_name) is None:
            reply(message, f"Я не узнаю этот часовой пояс. Пример: {_TZ_EXAMPLE} или Europe/Berlin.")
            return

        data = load_data()
        entry = _ensure_user_schedule(data, message.from_user.id, default_tz)
        entry["tz"] = tz_name
        reply(message, f"Часовой пояс принят: {tz_name}.\n\n" + _render_schedule(entry, default_tz))
        save_data(data)

    # --- Планировщик ---
    def scheduler_loop():
        while True:
            try:
                data = load_data()
                schedules = data.get("schedules", {}) if isinstance(data, dict) else {}
                if not isinstance(schedules, dict):
                    schedules = {}

                dirty = False
                now_ts = int(time.time())

                for suid, entry in list(schedules.items()):
                    if not isinstance(entry, dict):
                        continue

                    try:
                        uid = int(suid)
                    except Exception:
                        continue

                    if not is_allowed_fn(uid):
                        continue

                    if not entry.get("enabled", True):
                        continue

                    mode = entry.get("mode") or ""
                    if mode not in {"at", "every"}:
                        continue

                    tz_name = entry.get("tz", default_tz)

                    if mode == "at":
                        at_time = (entry.get("at_time") or "").strip()
                        if not _TIME_COLON_RE.match(at_time):
                            continue

                        now_local = _local_now(tz_name)
                        hhmm = now_local.strftime("%H:%M")
                        today = now_local.strftime("%Y-%m-%d")

                        if hhmm != at_time:
                            continue

                        last_sent = entry.setdefault("last_sent", {})
                        if isinstance(last_sent, dict) and last_sent.get(at_time) == today:
                            continue

                        quote = _random_quote(quotes_file)
                        caption = f"База дня: {quote}"
                        _send_random_media_with_caption(bot, uid, media_dir, caption)

                        entry["last_sent"] = {at_time: today}
                        schedules[suid] = entry
                        dirty = True

                    elif mode == "every":
                        every_min = int(entry.get("every_min") or 0)
                        if every_min <= 0:
                            continue

                        last_sent = entry.get("last_sent", {})
                        last_ts = 0
                        if isinstance(last_sent, dict):
                            last_ts = int(last_sent.get("ts") or 0)

                        if now_ts - last_ts < every_min * 60:
                            continue

                        quote = _random_quote(quotes_file)
                        caption = f"База дня: {quote}"
                        _send_random_media_with_caption(bot, uid, media_dir, caption)

                        entry["last_sent"] = {"ts": now_ts}
                        schedules[suid] = entry
                        dirty = True

                if dirty:
                    data["schedules"] = schedules
                    save_data(data)

            except Exception:
                # не роняем бота из-за расписания
                pass

            time.sleep(max(10, int(poll_interval_sec)))

    t = threading.Thread(target=scheduler_loop, name="schedule-sender", daemon=True)
    t.start()

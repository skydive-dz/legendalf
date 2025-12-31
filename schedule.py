import json
import random
import re
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import logging
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyParameters

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore

from holidays import HolidayFetchError, HolidayService, build_holiday_caption, image_stream


# Принимаем HH:MM и HH.MM
_TIME_COLON_RE = re.compile(r"^(?:[01]\d|2[0-3]):[0-5]\d$")
_TIME_DOT_RE = re.compile(r"^(?:[01]\d|2[0-3])\.[0-5]\d$")

_TZ_EXAMPLE = "Europe/Moscow"
_KIND_BASE = "base"
_KIND_HOLIDAYS = "holidays"
_KIND_LABELS = {
    _KIND_BASE: "«База дня»",
    _KIND_HOLIDAYS: "Праздники дня",
}
_KIND_ALIASES = {
    _KIND_BASE: {"1", "база", "base", "quotes", "цитаты"},
    _KIND_HOLIDAYS: {"2", "празд", "праздники", "holidays", "holiday"},
}

logger = logging.getLogger("legendalf.schedule")


def _parse_kind_choice(text: str | None) -> str | None:
    if not text:
        return None
    t = text.strip().lower()
    if not t:
        return None
    for kind, aliases in _KIND_ALIASES.items():
        if t in aliases:
            return kind
    return None


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


def _build_reply_parameters(
    message_id: int | None, allow_without_reply: bool | None = None
) -> ReplyParameters | None:
    if message_id is None:
        return None
    params: dict[str, object] = {"message_id": message_id}
    if allow_without_reply is not None:
        params["allow_sending_without_reply"] = allow_without_reply
    return ReplyParameters(**params)


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
    Хранит один профиль пользователя с независимыми настройками по типам рассылки.
    """
    suid = str(uid)
    schedules = data.setdefault("schedules", {})
    entry = schedules.get(suid)
    if not isinstance(entry, dict):
        entry = {}
        schedules[suid] = entry

    entry.setdefault("enabled", True)
    entry.setdefault("tz", default_tz)

    # миграция старых структур
    legacy_kind = entry.pop("kind", None)
    legacy_at = entry.pop("at_time", "")
    legacy_last = entry.pop("last_sent", {})
    entry.pop("mode", None)
    entry.pop("every_min", None)

    kinds = entry.setdefault("kinds", {})
    if not isinstance(kinds, dict):
        kinds = {}
        entry["kinds"] = kinds

    def ensure_kind(kind_name: str):
        k_entry = kinds.get(kind_name)
        if not isinstance(k_entry, dict):
            k_entry = {}
        k_entry.setdefault("enabled", False)
        k_entry.setdefault("at_time", "")
        k_entry.setdefault("last_sent", {})
        kinds[kind_name] = k_entry
        return k_entry

    for kind_name in (_KIND_BASE, _KIND_HOLIDAYS):
        ensure_kind(kind_name)

    if legacy_kind in {_KIND_BASE, _KIND_HOLIDAYS}:
        k_entry = ensure_kind(legacy_kind)
        k_entry["at_time"] = legacy_at
        if isinstance(legacy_last, dict):
            k_entry["last_sent"] = legacy_last
        k_entry["enabled"] = bool(entry.get("enabled", True))

    return entry


def _render_schedule(entry: dict, default_tz: str) -> str:
    enabled = "включён" if entry.get("enabled", True) else "выключен"
    tz = entry.get("tz", default_tz)
    lines = [
        "Графики рассылок",
        f"- общий статус: {enabled}",
        f"- часовой пояс: {tz}",
    ]
    kinds = entry.get("kinds", {})
    for kind_name in (_KIND_BASE, _KIND_HOLIDAYS):
        label = _KIND_LABELS.get(kind_name, kind_name)
        k_entry = kinds.get(kind_name, {})
        at_time = k_entry.get("at_time") or "не задано"
        status = "включён" if k_entry.get("enabled") and _TIME_COLON_RE.match(k_entry.get("at_time", "")) else "выключен"
        lines.append(f"- {label}: {status}, время: {at_time}")

    lines.append("")
    lines.extend(
        [
            "Команды:",
            "/schedule — показать график",
            "/schedule_add — выбрать модуль и задать время",
            "/schedule_del — убрать время для модуля",
            "/schedule_off [1|2] — выключить всё или конкретный модуль",
            "/schedule_on [1|2] — включить всё или модуль",
            f"/schedule_tz {_TZ_EXAMPLE} — установить часовой пояс",
        ]
    )
    return "\n".join(lines)


def _parse_time_value(text: str) -> str:
    raw = (text or "").strip()
    if not raw:
        raise ValueError("empty")

    if _TIME_DOT_RE.match(raw):
        raw = raw.replace(".", ":")

    if not _TIME_COLON_RE.match(raw):
        raise ValueError("bad_format")
    return raw


def register(
    bot,
    data_file: Path,
    quotes_file: Path,
    media_dir: Path,
    is_allowed_fn,
    *,
    default_tz: str = "Europe/Moscow",
    poll_interval_sec: int = 30,
    holiday_service: HolidayService | None = None,
) -> None:
    lock = threading.RLock()
    holiday_service = holiday_service or HolidayService()

    def load_data() -> dict:
        with lock:
            return _load_json(data_file)

    def save_data(d: dict) -> None:
        with lock:
            _save_json(data_file, d)

    def reply(message, text: str, **kwargs):
        return bot.send_message(
            message.chat.id,
            text,
            reply_parameters=_build_reply_parameters(message.message_id),
            **kwargs,
        )

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

        markup = InlineKeyboardMarkup(row_width=1)
        markup.add(
            InlineKeyboardButton("База дня", callback_data=f"schedkind:{message.from_user.id}:{_KIND_BASE}"),
            InlineKeyboardButton(
                "Праздники сегодня", callback_data=f"schedkind:{message.from_user.id}:{_KIND_HOLIDAYS}"
            ),
        )
        prompt = reply(
            message,
            "Какие весточки хочешь получать?",
            reply_markup=markup,
        )
        bot.register_next_step_handler(prompt, _schedule_add_kind_step)

    def _schedule_add_kind_step(message):
        uid = message.from_user.id
        if not is_allowed_fn(uid):
            reply(message, "Путь закрыт. Сначала /start.")
            return

        kind = _parse_kind_choice(message.text or "")
        if not kind:
            prompt = bot.send_message(
                message.chat.id,
                "Ответь 1 (база) или 2 (праздники), и мы продолжим.",
            )
            bot.register_next_step_handler(prompt, _schedule_add_kind_step)
            return

        logger.info("User %s chose schedule kind %s", uid, kind)
        _request_time_input(message.chat.id, uid, kind)

    @bot.callback_query_handler(func=lambda call: call.data.startswith("schedkind:"))
    def _schedule_kind_callback(call):
        try:
            _, target_uid, kind = call.data.split(":", 2)
        except ValueError:
            bot.answer_callback_query(call.id, "Не разобрал выбор.")
            return

        if str(call.from_user.id) != target_uid:
            bot.answer_callback_query(call.id, "Эта кнопка не для тебя.")
            return

        if not is_allowed_fn(call.from_user.id):
            bot.answer_callback_query(call.id, "Сначала получи допуск.")
            return

        bot.answer_callback_query(call.id, "Выбор принят.")
        try:
            bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
        except Exception:
            pass
        logger.info("User %s chose schedule kind %s via button", call.from_user.id, kind)
        _request_time_input(call.message.chat.id, call.from_user.id, kind)

    def _request_time_input(chat_id: int, user_id: int, kind: str):
        kind_label = _KIND_LABELS.get(kind, "«База дня»")
        prompt = bot.send_message(
            chat_id,
            f"Укажи время для {kind_label}. Формат ЧЧ:ММ (например 09:00).",
        )
        bot.register_next_step_handler(prompt, _schedule_add_time_step, kind)

    def _schedule_add_time_step(message, kind: str):
        uid = message.from_user.id
        if not is_allowed_fn(uid):
            reply(message, "Путь закрыт. Сначала /start.")
            return

        text = (message.text or "").strip()
        if text.startswith("/"):
            bot.reply_to(message, "Я жду время. Пример: 10:00.")
            _request_time_input(message.chat.id, uid, kind)
            return

        try:
            at_time = _parse_time_value(text)
        except ValueError:
            bot.reply_to(message, "Не понял формат. Введи время в виде 09:30 или 9.30.")
            _request_time_input(message.chat.id, uid, kind)
            return

        data = load_data()
        entry = _ensure_user_schedule(data, uid, default_tz)
        entry["enabled"] = True
        kind_key = kind if kind in _KIND_LABELS else _KIND_BASE
        kinds = entry.get("kinds")
        if not isinstance(kinds, dict):
            kinds = {}
            entry["kinds"] = kinds
        kind_entry = kinds.get(kind_key, {})
        kind_entry["at_time"] = at_time
        kind_entry["enabled"] = True
        kind_entry["last_sent"] = {}
        logger.info("User %s set schedule %s at %s", uid, kind_key, at_time)
        kinds[kind_key] = kind_entry

        reply(
            message,
            "Принято. Рассылка обновлена.\n\n" + _render_schedule(entry, default_tz),
        )
        save_data(data)

    @bot.message_handler(commands=["schedule_del"])
    def cmd_schedule_del(message):
        if not _require_allowed(message):
            return

        prompt = reply(
            message,
            "Какой модуль очистить?\n"
            "1 — «База дня»\n"
            "2 — Праздники дня.",
        )
        bot.register_next_step_handler(prompt, _schedule_del_kind_step)

    def _schedule_del_kind_step(message):
        uid = message.from_user.id
        if not is_allowed_fn(uid):
            reply(message, "Путь закрыт. Сначала /start.")
            return

        kind = _parse_kind_choice(message.text or "")
        if not kind:
            prompt = bot.send_message(
                message.chat.id,
                "Ответь 1 или 2, чтобы выбрать модуль.",
            )
            bot.register_next_step_handler(prompt, _schedule_del_kind_step)
            return

        data = load_data()
        entry = _ensure_user_schedule(data, uid, default_tz)
        kinds = entry.get("kinds", {})
        kind_entry = kinds.get(kind, {})
        kind_entry["at_time"] = ""
        kind_entry["enabled"] = False
        kind_entry["last_sent"] = {}
        kinds[kind] = kind_entry

        logger.info("User %s cleared schedule for %s", uid, kind)
        reply(
            message,
            f"График для {_KIND_LABELS.get(kind, kind)} очищен.\n\n" + _render_schedule(entry, default_tz),
        )
        save_data(data)

    @bot.message_handler(commands=["schedule_off"])
    def cmd_schedule_off(message):
        _handle_schedule_toggle(message, enable=False)

    @bot.message_handler(commands=["schedule_on"])
    def cmd_schedule_on(message):
        _handle_schedule_toggle(message, enable=True)

    def _handle_schedule_toggle(message, *, enable: bool):
        if not _require_allowed(message):
            return

        parts = (message.text or "").split(maxsplit=1)
        target_kind = None
        if len(parts) == 2:
            target_kind = _parse_kind_choice(parts[1])
            if parts[1].strip() and target_kind is None:
                reply(
                    message,
                    "Укажи 1 (база) или 2 (праздники), чтобы переключить конкретный модуль.",
                )
                return

        data = load_data()
        entry = _ensure_user_schedule(data, message.from_user.id, default_tz)

        if target_kind is None:
            entry["enabled"] = enable
            text = "Включено." if enable else "Отключено."
            reply(message, f"{text} Общий статус обновлён.\n\n" + _render_schedule(entry, default_tz))
            logger.info("User %s set global schedule enabled=%s", message.from_user.id, enable)
        else:
            kinds = entry.get("kinds", {})
            kind_entry = kinds.get(target_kind)
            if not kind_entry or not kind_entry.get("at_time"):
                reply(message, "Сначала задай время через /schedule_add.")
                return
            kind_entry["enabled"] = enable
            kinds[target_kind] = kind_entry
            state = "включена" if enable else "выключена"
            reply(
                message,
                f"Рассылка { _KIND_LABELS.get(target_kind, target_kind) } {state}.\n\n" + _render_schedule(entry, default_tz),
            )
            logger.info(
                "User %s set schedule %s enabled=%s",
                message.from_user.id,
                target_kind,
                enable,
            )

        save_data(data)

    @bot.message_handler(commands=["holydays"])
    def cmd_holydays(message):
        if not _require_allowed(message):
            return

        logger.info("Manual /holydays requested by user %s", message.from_user.id)
        data = load_data()
        entry = _ensure_user_schedule(data, message.from_user.id, default_tz)
        tz_name = entry.get("tz", default_tz)
        now_local = _local_now(tz_name)

        try:
            daily = holiday_service.get_daily(now_local.date())
        except HolidayFetchError:
            logger.warning("Failed to fetch holidays for /holydays (user %s)", message.from_user.id)
            reply(message, "Не удалось достать праздники. Похоже, свитки calend.ru не открылись.")
            return

        caption = build_holiday_caption(daily)
        photo = image_stream(daily)
        if photo:
            bot.send_photo(
                message.chat.id,
                photo,
                reply_parameters=_build_reply_parameters(
                    message.message_id, allow_without_reply=True
                ),
            )
            logger.info(
                "Sent holidays with image to user %s (names=%d)",
                message.from_user.id,
                len(daily.name_titles),
            )
        bot.send_message(
            message.chat.id,
            caption,
            parse_mode="HTML",
            reply_parameters=_build_reply_parameters(
                message.message_id, allow_without_reply=True
            ),
        )
        if not photo:
            logger.info(
                "Sent holidays text only to user %s (names=%d)",
                message.from_user.id,
                len(daily.name_titles),
            )


    @bot.message_handler(commands=["schedule_tz"])
    def cmd_schedule_tz(message):
        if not _require_allowed(message):
            return

        parts = (message.text or "").split(maxsplit=1)
        if len(parts) != 2:
            reply(message, f"��������� �����: /schedule_tz {_TZ_EXAMPLE}")
            return

        tz_name = parts[1].strip()
        if ZoneInfo is not None and _get_tz(tz_name) is None:
            reply(message, f"� �� ����� ���� ������� ����. ������: {_TZ_EXAMPLE} ��� Europe/Berlin.")
            return

        data = load_data()
        entry = _ensure_user_schedule(data, message.from_user.id, default_tz)
        entry["tz"] = tz_name
        reply(message, f"������� ���� ������: {tz_name}.\n\n" + _render_schedule(entry, default_tz))
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

                    kinds = entry.get("kinds", {})
                    if not isinstance(kinds, dict):
                        continue

                    tz_name = entry.get("tz", default_tz)
                    now_local = _local_now(tz_name)
                    hhmm = now_local.strftime("%H:%M")
                    today = now_local.strftime("%Y-%m-%d")

                    for kind_name, kind_entry in kinds.items():
                        if not isinstance(kind_entry, dict):
                            continue
                        if not kind_entry.get("enabled", False):
                            continue

                        at_time = (kind_entry.get("at_time") or "").strip()
                        if not _TIME_COLON_RE.match(at_time):
                            continue
                        if hhmm != at_time:
                            continue

                        last_sent = kind_entry.setdefault("last_sent", {})
                        if isinstance(last_sent, dict) and last_sent.get(at_time) == today:
                            continue

                        sent = False
                        if kind_name == _KIND_HOLIDAYS:
                            try:
                                daily = holiday_service.get_daily(now_local.date())
                                caption = build_holiday_caption(daily)
                                photo = image_stream(daily)
                                if photo:
                                    bot.send_photo(uid, photo)
                                    logger.info("Sent holiday image update to %s", uid)
                                else:
                                    logger.info("Sent holiday text-only update to %s", uid)
                                bot.send_message(uid, caption, parse_mode="HTML")
                                sent = True
                            except HolidayFetchError:
                                logger.warning("Failed to fetch holidays for schedule (user=%s)", uid)
                                bot.send_message(
                                    uid,
                                    "Хотел рассказать о праздниках дня, но свитки calend.ru не раскрылись.",
                                )
                        else:
                            quote = _random_quote(quotes_file)
                            caption = f"База дня: {quote}"
                            _send_random_media_with_caption(bot, uid, media_dir, caption)
                            sent = True

                        if sent:
                            logger.info("Sent %s update to user %s at %s", kind_name, uid, at_time)
                            kind_entry["last_sent"] = {at_time: today}
                            kinds[kind_name] = kind_entry
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

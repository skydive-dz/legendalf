from __future__ import annotations

import asyncio
import logging
import random
import re
from datetime import datetime, timezone
from pathlib import Path

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message, FSInputFile
from aiogram.exceptions import TelegramNetworkError

from retry_utils import retry_async, RETRY_DELAYS_LONG, RETRY_DELAYS_SHORT

from features.holidays import (
    HolidayFetchError,
    HolidayService,
    build_holiday_caption,
    image_stream,
    send_holiday_payload,
)
from features.films import build_monthly_messages, build_daily_payloads
import storage_sqlite

logger = logging.getLogger("legendalf.schedule_aiogram")

router = Router()

_TIME_COLON_RE = re.compile(r"^(?:[01]\d|2[0-3]):[0-5]\d$")
_TIME_DOT_RE = re.compile(r"^(?:[01]\d|2[0-3])\.[0-5]\d$")

_TZ_EXAMPLE = "Europe/Moscow"
_KIND_BASE = "base"
_KIND_HOLIDAYS = "holidays"
_KIND_FILMS = "films"
_KIND_FILMS_DAY = "films_day"
_KIND_LABELS = {
    _KIND_BASE: "База дня",
    _KIND_HOLIDAYS: "Праздники дня",
    _KIND_FILMS: "Кинопремьеры месяца",
    _KIND_FILMS_DAY: "Премьеры дня",
}
_KIND_ALIASES = {
    _KIND_BASE: {"1", "база", "base", "quotes", "цитаты"},
    _KIND_HOLIDAYS: {"2", "празд", "праздники", "holidays", "holiday"},
    _KIND_FILMS: {"3", "films", "film", "кино", "фильмы", "премьеры", "кинопремьеры"},
    _KIND_FILMS_DAY: {"4", "films_day", "film_day", "премьеры дня", "кино дня", "фильмы дня"},
}

_BACK_BUTTON_TEXT = "⬅️ Назад"

_config = {
    "data_file": Path("users.db"),
    "quotes_file": Path("quotes.txt"),
    "media_dir": Path("image"),
    "default_tz": _TZ_EXAMPLE,
    "poll_interval_sec": 30,
    "holiday_service": None,
    "is_allowed_fn": None,
}

_lock = asyncio.Lock()
_pending_add_kind: dict[int, str | None] = {}
_pending_del_kind: set[int] = set()


async def _safe_answer(message: Message, text: str, **kwargs) -> bool:
    return await retry_async(
        lambda: message.bot.send_message(message.chat.id, text, **kwargs),
        logger=logger,
        delays=RETRY_DELAYS_SHORT,
        retry_exceptions=(TelegramNetworkError, asyncio.TimeoutError),
    )


async def _retry_bot_send(task, label: str) -> bool:
    return await retry_async(
        task,
        logger=logger,
        label=label,
        delays=RETRY_DELAYS_LONG,
        retry_exceptions=(TelegramNetworkError, asyncio.TimeoutError),
    )


def configure(
    *,
    data_file: Path,
    quotes_file: Path,
    media_dir: Path,
    default_tz: str,
    poll_interval_sec: int,
    holiday_service: HolidayService | None,
    is_allowed_fn,
) -> None:
    _config.update(
        {
            "data_file": data_file,
            "quotes_file": quotes_file,
            "media_dir": media_dir,
            "default_tz": default_tz,
            "poll_interval_sec": poll_interval_sec,
            "holiday_service": holiday_service or HolidayService(),
            "is_allowed_fn": is_allowed_fn,
        }
    )


def _now_iso_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


async def load_data() -> dict:
    async with _lock:
        return await asyncio.to_thread(storage_sqlite.load_data, _config["data_file"], None)


async def save_data(data: dict) -> None:
    async with _lock:
        await asyncio.to_thread(storage_sqlite.save_data, _config["data_file"], data)


def _load_quotes(quotes_file: Path) -> list[str]:
    if not quotes_file.exists():
        return ["База пока не записана: положи цитаты в quotes.txt, и они оживут."]
    lines = [l.strip() for l in quotes_file.read_text(encoding="utf-8").splitlines()]
    lines = [l for l in lines if l]
    return lines or ["База пуста: даже мудрость молчит, если её не записали."]


def _random_quote(quotes_file: Path) -> str:
    return random.choice(_load_quotes(quotes_file))


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
    suid = str(uid)
    schedules = data.setdefault("schedules", {})
    entry = schedules.get(suid)
    if not isinstance(entry, dict):
        entry = {}
        schedules[suid] = entry

    entry.setdefault("enabled", True)
    entry.setdefault("tz", default_tz)

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

    for kind_name in (_KIND_BASE, _KIND_HOLIDAYS, _KIND_FILMS, _KIND_FILMS_DAY):
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
        f"Общий статус: {enabled}",
        f"Часовой пояс: {tz}",
        "",
    ]

    kinds = entry.get("kinds", {})
    for kind_name in (_KIND_BASE, _KIND_HOLIDAYS, _KIND_FILMS, _KIND_FILMS_DAY):
        label = _KIND_LABELS.get(kind_name, kind_name)
        k_entry = kinds.get(kind_name, {})
        at_time = k_entry.get("at_time") or "—"
        is_on = k_entry.get("enabled") and _TIME_COLON_RE.match(k_entry.get("at_time", ""))
        status = "вкл" if is_on else "выкл"
        marker = "✅" if is_on else "⛔"
        lines.append(f"{marker} {label}: {status}, время {at_time}")

    lines.append("")
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


def _list_media(media_dir: Path) -> list[Path]:
    if not media_dir.exists() or not media_dir.is_dir():
        return []
    exts = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".mp4"}
    return [p for p in media_dir.iterdir() if p.is_file() and p.suffix.lower() in exts]


async def _send_random_media_with_caption(bot, chat_id: int, media_dir: Path, caption: str) -> None:
    media = _list_media(media_dir)
    if not media:
        await _retry_bot_send(
            lambda: bot.send_message(
                chat_id,
                "Я не вижу свитков с образами и видениями в моей папке.\n"
                f"Положи файлы в: {media_dir}",
            ),
            "send base media missing",
        )
        return

    path = random.choice(media)
    file = FSInputFile(path)
    try:
        ext = path.suffix.lower()
        if ext in {".jpg", ".jpeg", ".png", ".webp"}:
            await _retry_bot_send(lambda: bot.send_photo(chat_id, file, caption=caption), "send base photo")
        elif ext == ".gif":
            await _retry_bot_send(lambda: bot.send_animation(chat_id, file, caption=caption), "send base animation")
        elif ext == ".mp4":
            await _retry_bot_send(lambda: bot.send_video(chat_id, file, caption=caption), "send base video")
        else:
            await _retry_bot_send(lambda: bot.send_document(chat_id, file, caption=caption), "send base document")
    except Exception:
        await _retry_bot_send(
            lambda: bot.send_message(chat_id, "Палантир молчит, тьма окутала Средиземье."),
            "send base media failure",
        )


def _build_kind_markup(user_id: int, action: str) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(text="База дня", callback_data=f"{action}:{user_id}:{_KIND_BASE}")],
        [InlineKeyboardButton(text="Праздники сегодня", callback_data=f"{action}:{user_id}:{_KIND_HOLIDAYS}")],
        [InlineKeyboardButton(text="Кинопремьеры месяца", callback_data=f"{action}:{user_id}:{_KIND_FILMS}")],
        [InlineKeyboardButton(text="Кинопремьеры дня", callback_data=f"{action}:{user_id}:{_KIND_FILMS_DAY}")],
        [InlineKeyboardButton(text=_BACK_BUTTON_TEXT, callback_data=f"schedback:{user_id}:{action}")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def _build_back_markup(user_id: int, action: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=_BACK_BUTTON_TEXT, callback_data=f"schedback:{user_id}:{action}")]
        ]
    )


def _build_main_menu_markup(user_id: int) -> InlineKeyboardMarkup:
    row1 = [
        InlineKeyboardButton(text="Расписание", callback_data=f"schedcmd:{user_id}:schedule"),
        InlineKeyboardButton(text="Добавить", callback_data=f"schedcmd:{user_id}:add"),
        InlineKeyboardButton(text="Удалить", callback_data=f"schedcmd:{user_id}:del"),
    ]
    row2 = [
        InlineKeyboardButton(text="Выключить", callback_data=f"schedcmd:{user_id}:off"),
        InlineKeyboardButton(text="Включить", callback_data=f"schedcmd:{user_id}:on"),
        InlineKeyboardButton(text="Часовой пояс", callback_data=f"schedcmd:{user_id}:tz"),
    ]
    back = [InlineKeyboardButton(text=_BACK_BUTTON_TEXT, callback_data=f"schedmenu:{user_id}")]
    return InlineKeyboardMarkup(inline_keyboard=[row1, row2, back])


def _require_allowed_text() -> str:
    return (
        "Прежде чем приказывать времени, нужно получить допуск.\n"
        "Напиши /mellon — и я передам твоё имя хранителю врат."
    )


def _require_allowed(message: Message) -> bool:
    if not _config["is_allowed_fn"](message.from_user.id):
        return False
    return True


@router.message(Command("schedule"))
async def cmd_schedule(message: Message) -> None:
    logger.info("Command /schedule from %s", message.from_user.id)
    if not _require_allowed(message):
        await _safe_answer(message, _require_allowed_text())
        return
    data = await load_data()
    entry = _ensure_user_schedule(data, message.from_user.id, _config["default_tz"])
    await _safe_answer(
        message,
        _render_schedule(entry, _config["default_tz"]),
        reply_markup=_build_main_menu_markup(message.from_user.id),
    )
    await save_data(data)


@router.message(Command("schedule_add"))
async def cmd_schedule_add(message: Message) -> None:
    logger.info("Command /schedule_add from %s", message.from_user.id)
    if not _require_allowed(message):
        await _safe_answer(message, _require_allowed_text())
        return
    _pending_add_kind[message.from_user.id] = None
    await _safe_answer(message, "Выбери модуль рассылки.", reply_markup=_build_kind_markup(message.from_user.id, "add"))


@router.message(Command("schedule_del"))
async def cmd_schedule_del(message: Message) -> None:
    logger.info("Command /schedule_del from %s", message.from_user.id)
    if not _require_allowed(message):
        await _safe_answer(message, _require_allowed_text())
        return
    _pending_del_kind.add(message.from_user.id)
    await _safe_answer(message, "Выбери модуль, чтобы отключить.", reply_markup=_build_kind_markup(message.from_user.id, "del"))


@router.message(Command("schedule_off"))
async def cmd_schedule_off(message: Message) -> None:
    logger.info("Command /schedule_off from %s", message.from_user.id)
    await _handle_schedule_toggle(message, enable=False)


@router.message(Command("schedule_on"))
async def cmd_schedule_on(message: Message) -> None:
    logger.info("Command /schedule_on from %s", message.from_user.id)
    await _handle_schedule_toggle(message, enable=True)


async def _handle_schedule_toggle(
    message: Message,
    *,
    enable: bool,
    user_id: int | None = None,
    from_button: bool = False,
) -> None:
    uid = user_id if user_id is not None else message.from_user.id
    if not _config["is_allowed_fn"](uid):
        await _safe_answer(message, _require_allowed_text())
        return

    parts = (message.text or "").split(maxsplit=1)
    target_kind = None
    if not from_button and len(parts) == 2:
        target_kind = _parse_kind_choice(parts[1])
        if parts[1].strip() and target_kind is None:
            await _safe_answer(message, 
                "Выбери конкретный модуль."
            )
            return

    data = await load_data()
    entry = _ensure_user_schedule(data, uid, _config["default_tz"])

    if target_kind is None:
        entry["enabled"] = enable
        text = "Включено." if enable else "Отключено."
        await _safe_answer(message, f"{text} Общий статус обновлён.\n\n" + _render_schedule(entry, _config["default_tz"]))
    else:
        kinds = entry.get("kinds", {})
        kind_entry = kinds.get(target_kind)
        if not kind_entry or not kind_entry.get("at_time"):
            await _safe_answer(message, "Сначала задай время через /schedule_add.")
            return
        kind_entry["enabled"] = enable
        kinds[target_kind] = kind_entry
        state = "включена" if enable else "выключена"
        await _safe_answer(message, 
            f"Рассылка {_KIND_LABELS.get(target_kind, target_kind)} {state}.\n\n"
            + _render_schedule(entry, _config["default_tz"])
        )

    await save_data(data)


@router.message(Command("schedule_tz"))
async def cmd_schedule_tz(message: Message) -> None:
    logger.info("Command /schedule_tz from %s", message.from_user.id)
    if not _require_allowed(message):
        await _safe_answer(message, _require_allowed_text())
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) != 2:
        await _safe_answer(message, f"Использование команды: /schedule_tz {_TZ_EXAMPLE}")
        return

    tz_name = parts[1].strip()
    if ZoneInfo is not None and _get_tz(tz_name) is None:
        await _safe_answer(message, f"Я не знаю этот часовой пояс. Пример: {_TZ_EXAMPLE} или Europe/Berlin.")
        return

    data = await load_data()
    entry = _ensure_user_schedule(data, message.from_user.id, _config["default_tz"])
    entry["tz"] = tz_name
    await _safe_answer(message, f"Часовой пояс принят: {tz_name}.\n\n" + _render_schedule(entry, _config["default_tz"]))
    await save_data(data)


@router.callback_query(F.data.startswith("schedback:"))
async def cb_schedule_back(call: CallbackQuery) -> None:
    parts = (call.data or "").split(":")
    if len(parts) < 2:
        await call.answer("Некорректная команда.")
        return

    target_uid = parts[1]
    if str(call.from_user.id) != target_uid:
        await call.answer("Эта кнопка не для вас.")
        return

    if not _config["is_allowed_fn"](call.from_user.id):
        await call.answer("Доступ закрыт.")
        return

    await call.answer("Готово")
    _pending_add_kind.pop(call.from_user.id, None)
    _pending_del_kind.discard(call.from_user.id)

    try:
        await call.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

    data = await load_data()
    entry = _ensure_user_schedule(data, call.from_user.id, _config["default_tz"])
    await _safe_answer(
        call.message,
        _render_schedule(entry, _config["default_tz"]),
        reply_markup=_build_main_menu_markup(call.from_user.id),
    )
    await save_data(data)


@router.callback_query(F.data.startswith("schedmenu:"))
async def cb_schedule_menu(call: CallbackQuery) -> None:
    try:
        _, target_uid = (call.data or "").split(":", 1)
    except ValueError:
        await call.answer("Некорректная команда.")
        return

    if str(call.from_user.id) != target_uid:
        await call.answer("Эта кнопка не для тебя.")
        return

    if not _config["is_allowed_fn"](call.from_user.id):
        await call.answer("Доступ закрыт.")
        return

    await call.answer("Готово")
    try:
        await call.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

    data = await load_data()
    entry = _ensure_user_schedule(data, call.from_user.id, _config["default_tz"])
    await _safe_answer(
        call.message,
        _render_schedule(entry, _config["default_tz"]),
        reply_markup=_build_main_menu_markup(call.from_user.id),
    )
    await save_data(data)


@router.callback_query(F.data.startswith("schedcmd:"))
async def cb_schedule_command(call: CallbackQuery) -> None:
    try:
        _, target_uid, action = (call.data or "").split(":", 2)
    except ValueError:
        await call.answer("Некорректная команда.")
        return

    if str(call.from_user.id) != target_uid:
        await call.answer("Эта кнопка не для тебя.")
        return

    if not _config["is_allowed_fn"](call.from_user.id):
        await call.answer("Доступ закрыт.")
        return

    await call.answer("Принято")
    try:
        await call.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

    if action == "schedule":
        data = await load_data()
        entry = _ensure_user_schedule(data, call.from_user.id, _config["default_tz"])
        await _safe_answer(
            call.message,
            _render_schedule(entry, _config["default_tz"]),
            reply_markup=_build_main_menu_markup(call.from_user.id),
        )
        await save_data(data)
        return

    if action == "add":
        _pending_add_kind[call.from_user.id] = None
        await _safe_answer(
            call.message,
            "Выбери модуль рассылки.",
            reply_markup=_build_kind_markup(call.from_user.id, "add"),
        )
        return

    if action == "del":
        _pending_del_kind.add(call.from_user.id)
        await _safe_answer(
            call.message,
            "Выбери модуль, чтобы отключить.",
            reply_markup=_build_kind_markup(call.from_user.id, "del"),
        )
        return

    if action == "off":
        await _handle_schedule_toggle(call.message, enable=False, user_id=call.from_user.id, from_button=True)
        return

    if action == "on":
        await _handle_schedule_toggle(call.message, enable=True, user_id=call.from_user.id, from_button=True)
        return

    if action == "tz":
        await _safe_answer(call.message, f"Использование команды: /schedule_tz {_TZ_EXAMPLE}")
        return

    await _safe_answer(call.message, "Не разобрал выбор.")

@router.callback_query(F.data.startswith("add:"))
async def cb_schedule_add_kind(call: CallbackQuery) -> None:
    try:
        _, target_uid, kind = (call.data or "").split(":", 2)
    except ValueError:
        await call.answer("Не разобрал выбор.")
        return

    if str(call.from_user.id) != target_uid:
        await call.answer("Эта кнопка не для тебя.")
        return

    if not _config["is_allowed_fn"](call.from_user.id):
        await call.answer("Сначала получи допуск.")
        return

    await call.answer("Выбор принят.")
    _pending_add_kind[call.from_user.id] = kind
    try:
        await call.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

    await _request_time_input(call.message, call.from_user.id, kind)


@router.callback_query(F.data.startswith("del:"))
async def cb_schedule_del_kind(call: CallbackQuery) -> None:
    try:
        _, target_uid, kind = (call.data or "").split(":", 2)
    except ValueError:
        await call.answer("Не разобрал выбор.")
        return

    if str(call.from_user.id) != target_uid:
        await call.answer("Эта кнопка не для тебя.")
        return

    if not _config["is_allowed_fn"](call.from_user.id):
        await call.answer("Сначала получи допуск.")
        return

    await call.answer("Модуль выбран.")
    _pending_del_kind.discard(call.from_user.id)
    try:
        await call.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

    await _clear_schedule_kind(call.message, call.from_user.id, kind)


async def _request_time_input(message: Message, user_id: int, kind: str) -> None:
    kind_label = _KIND_LABELS.get(kind, "База дня")
    await _safe_answer(message, 
        f"Укажи время для {kind_label}. Формат HH:MM (например 09:00).",
        reply_markup=_build_back_markup(user_id, "add"),
    )


async def _clear_schedule_kind(message: Message, uid: int, kind: str) -> None:
    data = await load_data()
    entry = _ensure_user_schedule(data, uid, _config["default_tz"])
    kinds = entry.get("kinds", {})
    kind_entry = kinds.get(kind, {})
    kind_entry["at_time"] = ""
    kind_entry["enabled"] = False
    kind_entry["last_sent"] = {}
    kinds[kind] = kind_entry
    await _safe_answer(message, 
        f"График для {_KIND_LABELS.get(kind, kind)} очищен.\n\n"
        + _render_schedule(entry, _config["default_tz"])
    )
    await save_data(data)


@router.message(F.text & ~F.text.startswith("/"))
async def on_schedule_text(message: Message) -> None:
    uid = message.from_user.id
    text = (message.text or "").strip()

    if uid in _pending_add_kind:
        if not _config["is_allowed_fn"](uid):
            await _safe_answer(message, "Доступ закрыт. Отправьте /mellon, чтобы запросить доступ.")
            _pending_add_kind.pop(uid, None)
            return

        pending_kind = _pending_add_kind.get(uid)
        if pending_kind is None:
            kind = _parse_kind_choice(text)
            if not kind:
                await _safe_answer(message, 
                    "Выбери модуль",
                    reply_markup=_build_kind_markup(uid, "add"),
                )
                return
            _pending_add_kind[uid] = kind
            await _request_time_input(message, uid, kind)
            return

        if text.startswith("/"):
            await _safe_answer(message, "Я жду время. Пример: 10:00.")
            await _request_time_input(message, uid, pending_kind)
            return

        try:
            at_time = _parse_time_value(text)
        except ValueError:
            await _safe_answer(message, "Не понял формат. Введи время в виде 09:30 или 9.30.")
            await _request_time_input(message, uid, pending_kind)
            return

        data = await load_data()
        entry = _ensure_user_schedule(data, uid, _config["default_tz"])
        entry["enabled"] = True
        kind_key = pending_kind if pending_kind in _KIND_LABELS else _KIND_BASE
        kinds = entry.get("kinds")
        if not isinstance(kinds, dict):
            kinds = {}
            entry["kinds"] = kinds
        kind_entry = kinds.get(kind_key, {})
        kind_entry["at_time"] = at_time
        kind_entry["enabled"] = True
        kind_entry["last_sent"] = {}
        kinds[kind_key] = kind_entry
        await _safe_answer(message, 
            "Принято. Рассылка обновлена.\n\n" + _render_schedule(entry, _config["default_tz"])
        )
        await save_data(data)
        _pending_add_kind.pop(uid, None)
        return

    if uid in _pending_del_kind:
        if not _config["is_allowed_fn"](uid):
            await _safe_answer(message, "Доступ закрыт. Отправьте /mellon, чтобы запросить доступ.")
            _pending_del_kind.discard(uid)
            return

        kind = _parse_kind_choice(text)
        if not kind:
            await _safe_answer(message, 
                "Ответь 1, 2, 3 или 4, чтобы выбрать модуль.",
                reply_markup=_build_kind_markup(uid, "del"),
            )
            return
        _pending_del_kind.discard(uid)
        await _clear_schedule_kind(message, uid, kind)


async def scheduler_loop(bot) -> None:
    while True:
        try:
            data = await load_data()
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

                if not _config["is_allowed_fn"](uid):
                    continue
                if not entry.get("enabled", True):
                    continue

                kinds = entry.get("kinds", {})
                if not isinstance(kinds, dict):
                    continue

                tz_name = entry.get("tz", _config["default_tz"])
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
                            daily = await asyncio.to_thread(_config["holiday_service"].get_daily, now_local.date())
                            caption = build_holiday_caption(daily)
                            photo = daily.image_url or image_stream(daily)
                            await send_holiday_payload(bot, uid, photo, caption)
                            sent = True
                        except HolidayFetchError:
                            await _retry_bot_send(
                                lambda: bot.send_message(
                                    uid,
                                    "Не удалось заказать о праздниках: увы, но ссылки не обновились.",
                                ),
                                "send holidays error",
                            )
                            await _retry_bot_send(
                                lambda: bot.send_message(
                                    uid,
                                    "Сегодня прошел праздничный день, чтобы просто радоваться жизни.",
                                ),
                                "send holidays fallback",
                            )
                    elif kind_name == _KIND_FILMS:
                        if now_local.day != 1:
                            continue
                        try:
                            messages = await asyncio.to_thread(build_monthly_messages, now_local.date())
                            if not messages:
                                await _retry_bot_send(
                                    lambda: bot.send_message(uid, "На этот месяц премьер не найдено."),
                                    "send films month empty",
                                )
                            else:
                                for payload in messages:
                                    await _retry_bot_send(
                                        lambda payload=payload: bot.send_message(
                                            uid,
                                            payload,
                                            parse_mode="HTML",
                                            disable_web_page_preview=True,
                                        ),
                                        "send films month",
                                    )
                            sent = True
                        except Exception as exc:
                            logger.warning("Failed to fetch films for schedule (user=%s): %s", uid, exc)
                            await _retry_bot_send(
                                lambda: bot.send_message(uid, "Не получилось получить список премьер. Попробуйте позже."),
                                "send films month error",
                            )
                    elif kind_name == _KIND_FILMS_DAY:
                        try:
                            payloads = await asyncio.to_thread(build_daily_payloads, now_local.date())
                            if not payloads:
                                await _retry_bot_send(
                                    lambda: bot.send_message(uid, "Фильмов сегодня нет, Гэндальф грустит 😢"),
                                    "send films day empty",
                                )
                            else:
                                for poster_url, caption in payloads:
                                    if poster_url:
                                        sent_poster = await _retry_bot_send(
                                            lambda poster_url=poster_url, caption=caption: bot.send_photo(
                                                uid,
                                                poster_url,
                                                caption=caption,
                                                parse_mode="HTML",
                                            ),
                                            "send films day poster",
                                        )
                                        if sent_poster:
                                            continue
                                    await _retry_bot_send(
                                        lambda caption=caption: bot.send_message(
                                            uid,
                                            caption,
                                            parse_mode="HTML",
                                            disable_web_page_preview=True,
                                        ),
                                        "send films day text",
                                    )
                            sent = True
                        except Exception as exc:
                            logger.warning("Failed to fetch daily films for schedule (user=%s): %s", uid, exc)
                            await _retry_bot_send(
                                lambda: bot.send_message(
                                    uid,
                                    "Не получилось получить список премьер дня. Попробуйте позже.",
                                ),
                                "send films day error",
                            )
                    else:
                        quote = _random_quote(_config["quotes_file"])
                        caption = f"База дня: {quote}"
                        await _send_random_media_with_caption(bot, uid, _config["media_dir"], caption)
                        sent = True

                    if sent:
                        kind_entry["last_sent"] = {at_time: today}
                        kinds[kind_name] = kind_entry
                        schedules[suid] = entry
                        dirty = True

            if dirty:
                data["schedules"] = schedules
                await save_data(data)
        except Exception:
            pass

        await asyncio.sleep(max(10, int(_config["poll_interval_sec"])))

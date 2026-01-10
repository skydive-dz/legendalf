from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import date, datetime
from html import escape
from html.parser import HTMLParser
from typing import Iterable

import logging
import re
import requests
from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message
from aiogram.exceptions import TelegramNetworkError

from retry_utils import retry_async, RETRY_DELAYS_SHORT

logger = logging.getLogger("legendalf.features.films")

router = Router()

_BASE_URL = "https://www.kinopoisk.ru"
_BASE_IMG = "https://st.kp.yandex.net"
_PREMIER_URL = "https://www.kinopoisk.ru/premiere/ru/{year}/month/{month}/"
_DAILY_URL = "https://www.kinopoisk.ru/premiere/ru/date/{date}/"
_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

_RU_MONTHS = {
    1: "января",
    2: "февраля",
    3: "марта",
    4: "апреля",
    5: "мая",
    6: "июня",
    7: "июля",
    8: "августа",
    9: "сентября",
    10: "октября",
    11: "ноября",
    12: "декабря",
}

_RU_MONTH_ALIASES = {
    "январь": 1,
    "января": 1,
    "февраль": 2,
    "февраля": 2,
    "март": 3,
    "марта": 3,
    "апрель": 4,
    "апреля": 4,
    "май": 5,
    "мая": 5,
    "июнь": 6,
    "июня": 6,
    "июль": 7,
    "июля": 7,
    "август": 8,
    "августа": 8,
    "сентябрь": 9,
    "сентября": 9,
    "октябрь": 10,
    "октября": 10,
    "ноябрь": 11,
    "ноября": 11,
    "декабрь": 12,
    "декабря": 12,
}

_MONTH_YEAR_RE = re.compile(r"^(0?[1-9]|1[0-2])[./](\d{2}|\d{4})$")
_DATE_DOT_RE = re.compile(r"^(0?[1-9]|[12]\d|3[01])[./-](0?[1-9]|1[0-2])[./-](\d{2}|\d{4})$")


_config = {
    "is_allowed_fn": None,
}


def configure(*, is_allowed_fn) -> None:
    _config.update({"is_allowed_fn": is_allowed_fn})


async def _safe_answer(message: Message, text: str, **kwargs) -> bool:
    return await retry_async(
        lambda: message.answer(text, **kwargs),
        logger=logger,
        delays=RETRY_DELAYS_SHORT,
        retry_exceptions=(TelegramNetworkError, asyncio.TimeoutError),
    )


def _normalize_text(value: str) -> str:
    return " ".join(value.split()).strip()


def _full_url(href: str | None) -> str:
    if not href:
        return ""
    if href.startswith("http://") or href.startswith("https://"):
        return href
    return f"{_BASE_URL}{href}"


def _full_image_url(path: str | None) -> str:
    if not path:
        return ""
    if path.startswith("http://") or path.startswith("https://"):
        return path
    if path.startswith("/"):
        return f"{_BASE_IMG}{path}"
    return f"{_BASE_IMG}/{path}"


def _format_date(date_iso: str, *, pretty_month: bool) -> str:
    try:
        dt = date.fromisoformat(date_iso)
    except ValueError:
        return date_iso
    if pretty_month:
        month = _RU_MONTHS.get(dt.month, dt.strftime("%m"))
        return f"{dt.day} {month} {dt.year}"
    return dt.strftime("%d.%m.%Y")


def _split_country_director(text: str) -> str:
    cleaned = _normalize_text(text)
    if not cleaned:
        return ""
    if "реж." not in cleaned:
        return cleaned
    left, right = cleaned.split("реж.", 1)
    country = left.strip(" ,")
    director = right.strip(" ,")
    if country and director:
        return f"{country}, реж. {director}"
    if director:
        return f"реж. {director}"
    return country


def _find_year(text: str) -> str:
    for token in text.split():
        if token.startswith("(") and token.endswith(")"):
            token = token.strip("()")
        if token.isdigit() and len(token) == 4:
            return token
    return ""


def _poster_from_film_id(film_id: str | None) -> str:
    if not film_id or not film_id.isdigit():
        return ""
    return f"{_BASE_IMG}/images/film_big/{film_id}.jpg"


def _parse_month_year(text: str) -> date | None:
    raw = (text or "").strip().lower()
    if not raw:
        return None

    m = _MONTH_YEAR_RE.match(raw)
    if m:
        month = int(m.group(1))
        year = int(m.group(2))
        if year < 100:
            year += 2000
        return date(year, month, 1)

    parts = raw.split()
    if len(parts) == 2:
        month_name, year_text = parts
        month = _RU_MONTH_ALIASES.get(month_name)
        if month and year_text.isdigit():
            year = int(year_text)
            if 1900 <= year <= 2100:
                return date(year, month, 1)

    return None


def _parse_day_date(text: str) -> date | None:
    raw = (text or "").strip().lower()
    if not raw:
        return None

    m = _DATE_DOT_RE.match(raw)
    if m:
        day = int(m.group(1))
        month = int(m.group(2))
        year = int(m.group(3))
        if year < 100:
            year += 2000
        try:
            return date(year, month, day)
        except ValueError:
            return None

    parts = raw.split()
    if len(parts) in (2, 3) and parts[0].isdigit():
        day = int(parts[0])
        month_name = parts[1].strip(".,")
        month = _RU_MONTH_ALIASES.get(month_name)
        if not month:
            return None
        if len(parts) == 3:
            year_text = parts[2].strip(".,")
            if not year_text.isdigit():
                return None
            year = int(year_text)
            if year < 100:
                year += 2000
        else:
            year = datetime.now().year
        try:
            return date(year, month, day)
        except ValueError:
            return None

    return None


@dataclass
class PremiereItem:
    title: str
    url: str
    year: str
    date_iso: str
    country_director: str
    genres: str
    poster_url: str
    film_id: str


class _PremiereParser(HTMLParser):
    def __init__(self, *, include_image: bool) -> None:
        super().__init__()
        self.items: list[PremiereItem] = []
        self._current: dict[str, str] | None = None
        self._in_item = False
        self._div_depth = 0
        self._in_span = False
        self._span_text = ""
        self._span_class = ""
        self._span_style = ""
        self._include_image = include_image

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr = dict(attrs)
        if tag == "div" and "premier_item" in (attr.get("class") or ""):
            self._current = {
                "title": "",
                "url": "",
                "year": "",
                "date_iso": "",
                "country_director": "",
                "genres": "",
                "image": "",
                "film_id": attr.get("id") or "",
            }
            self._in_item = True
            self._div_depth = 1
            return

        if self._in_item and tag == "div":
            self._div_depth += 1

        if not self._in_item:
            return

        if tag == "meta" and attr.get("itemprop") == "startDate":
            if self._current is not None:
                self._current["date_iso"] = attr.get("content", "") or ""
            return

        if self._include_image and tag == "meta" and attr.get("itemprop") == "image":
            if self._current is not None:
                self._current["image"] = attr.get("content", "") or ""
            return

        if tag == "span":
            self._in_span = True
            self._span_text = ""
            self._span_class = attr.get("class") or ""
            self._span_style = attr.get("style") or ""
            return

        if tag == "a" and self._in_span and "name" in self._span_class:
            if self._current is not None:
                self._current["url"] = _full_url(attr.get("href"))

    def handle_data(self, data: str) -> None:
        if self._in_item and self._in_span:
            self._span_text += data

    def handle_endtag(self, tag: str) -> None:
        if self._in_item and tag == "span" and self._in_span:
            text = _normalize_text(self._span_text)
            if self._current is not None and text:
                if "name" in self._span_class and not self._current.get("title"):
                    self._current["title"] = text
                else:
                    if "margin: 0" in self._span_style or "margin:0" in self._span_style:
                        if not self._current.get("country_director"):
                            self._current["country_director"] = text
                    if text.startswith("(") and text.endswith(")") and not self._current.get("genres"):
                        self._current["genres"] = text.strip()[1:-1].strip()
                    if not self._current.get("year"):
                        year_match = _find_year(text)
                        if year_match:
                            self._current["year"] = year_match

            self._in_span = False
            self._span_text = ""
            self._span_class = ""
            self._span_style = ""
            return

        if self._in_item and tag == "div":
            self._div_depth -= 1
            if self._div_depth <= 0:
                self._flush_item()

    def _flush_item(self) -> None:
        if not self._current:
            self._reset()
            return
        image = self._current.get("image", "").strip()
        film_id = self._current.get("film_id", "").strip()
        poster_url = _poster_from_film_id(film_id) or _full_image_url(image)
        item = PremiereItem(
            title=self._current.get("title", "").strip(),
            url=self._current.get("url", "").strip(),
            year=self._current.get("year", "").strip(),
            date_iso=self._current.get("date_iso", "").strip(),
            country_director=self._current.get("country_director", "").strip(),
            genres=self._current.get("genres", "").strip(),
            poster_url=poster_url,
            film_id=film_id,
        )
        if item.title:
            self.items.append(item)
        self._reset()

    def _reset(self) -> None:
        self._current = None
        self._in_item = False
        self._div_depth = 0
        self._in_span = False
        self._span_text = ""
        self._span_class = ""
        self._span_style = ""


def _fetch_monthly_premieres(target_date: date) -> list[PremiereItem]:
    url = _PREMIER_URL.format(year=target_date.year, month=target_date.month)
    headers = {"User-Agent": _UA}
    resp = requests.get(url, headers=headers, timeout=20)
    resp.raise_for_status()
    parser = _PremiereParser(include_image=False)
    parser.feed(resp.text)
    logger.info("Parsed %d premieres from %s", len(parser.items), url)
    return parser.items


def _fetch_daily_premieres(target_date: date) -> list[PremiereItem]:
    url = _DAILY_URL.format(date=target_date.strftime("%Y-%m-%d"))
    headers = {"User-Agent": _UA}
    resp = requests.get(url, headers=headers, timeout=20)
    resp.raise_for_status()
    parser = _PremiereParser(include_image=True)
    parser.feed(resp.text)
    logger.info("Parsed %d premieres from %s", len(parser.items), url)
    if not parser.items:
        return []
    return [item for item in parser.items if item.date_iso == target_date.isoformat()] or parser.items


def _render_monthly_items(items: Iterable[PremiereItem]) -> list[str]:
    blocks: list[str] = []
    for item in items:
        title = item.title
        if item.year:
            title = f"{title} ({item.year})"
        title = escape(title)
        url = escape(item.url)
        when = _format_date(item.date_iso, pretty_month=True)
        line1 = f'<a href="{url}">{title}</a> - {escape(when)}'

        line2_parts: list[str] = []
        credits = _split_country_director(item.country_director)
        if credits:
            line2_parts.append(escape(credits))
        if item.genres:
            line2_parts.append(f"({escape(item.genres)})")
        line2 = " ".join(line2_parts)

        if line2:
            blocks.append(f"{line1}\n{line2}")
        else:
            blocks.append(line1)
    return blocks


def _chunk_messages(blocks: list[str], max_len: int = 4000) -> list[str]:
    messages: list[str] = []
    current = ""
    for block in blocks:
        if not current:
            current = block
            continue
        candidate = f"{current}\n\n{block}"
        if len(candidate) > max_len:
            messages.append(current)
            current = block
        else:
            current = candidate
    if current:
        messages.append(current)
    return messages


def build_monthly_messages(target_date: date) -> list[str]:
    items = _fetch_monthly_premieres(target_date)
    if not items:
        return []
    blocks = _render_monthly_items(items)
    return _chunk_messages(blocks)


def build_daily_payloads(target_date: date) -> list[tuple[str, str]]:
    items = _fetch_daily_premieres(target_date)
    if not items:
        return []
    payloads: list[tuple[str, str]] = []
    for item in items:
        payloads.append((item.poster_url, _format_item_caption(item)))
    return payloads


def _format_item_caption(item: PremiereItem) -> str:
    title = item.title
    if item.year:
        title = f"{title} ({item.year})"
    title = escape(title)
    url = escape(item.url)
    when = _format_date(item.date_iso, pretty_month=False)
    line1 = f'<a href="{url}">{title}</a> - {escape(when)}'

    line2_parts: list[str] = []
    credits = _split_country_director(item.country_director)
    if credits:
        line2_parts.append(escape(credits))
    if item.genres:
        line2_parts.append(f"({escape(item.genres)})")
    line2 = " ".join(line2_parts)

    if line2:
        return f"{line1}\n{line2}"
    return line1


@router.message(Command("films_month"))
async def cmd_films_month(message: Message) -> None:
    logger.info("Command /films_month from %s", message.from_user.id)
    if not _config["is_allowed_fn"](message.from_user.id):
        await _safe_answer(message, "Доступ закрыт. Отправьте /mellon, чтобы запросить доступ.")
        return

    args = (message.text or "").split(maxsplit=1)
    target = datetime.now().date()
    if len(args) == 2:
        parsed = _parse_month_year(args[1])
        if not parsed:
            await _safe_answer(
                message,
                "Не понял месяц. Примеры: /films_month февраль 2026 или /films_month 02.26",
            )
            return
        target = parsed

    try:
        messages = await asyncio.to_thread(build_monthly_messages, target)
    except Exception as exc:
        logger.warning("Failed to fetch premieres: %s", exc)
        await _safe_answer(message, "Не получилось получить список премьер. Попробуйте позже.")
        return

    if not messages:
        await _safe_answer(message, "На этот месяц премьер не найдено.")
        return

    for payload in messages:
        await _safe_answer(message, payload, parse_mode="HTML", disable_web_page_preview=True)


@router.message(Command("films_day"))
async def cmd_films_day(message: Message) -> None:
    logger.info("Command /films_day from %s", message.from_user.id)
    if not _config["is_allowed_fn"](message.from_user.id):
        await _safe_answer(message, "Доступ закрыт. Отправьте /mellon, чтобы запросить доступ.")
        return

    args = (message.text or "").split(maxsplit=1)
    target = datetime.now().date()
    if len(args) == 2:
        parsed = _parse_day_date(args[1])
        if not parsed:
            await _safe_answer(
                message,
                "Не понял дату. Примеры: /films_day 01.01.2026 или /films_day 1 января 2026",
            )
            return
        target = parsed

    try:
        payloads = await asyncio.to_thread(build_daily_payloads, target)
    except Exception as exc:
        logger.warning("Failed to fetch daily premieres: %s", exc)
        await _safe_answer(message, "Не получилось получить список премьер дня. Попробуйте позже.")
        return

    if not payloads:
        await _safe_answer(message, "Фильмов сегодня нет, Гэндальф грустит 😢")
        return

    for poster_url, caption in payloads:
        if poster_url:
            try:
                await message.answer_photo(poster_url, caption=caption, parse_mode="HTML")
                continue
            except Exception as exc:
                logger.warning("Failed to send poster: %s", exc)
        await _safe_answer(message, caption, parse_mode="HTML", disable_web_page_preview=True)


__all__ = [
    "build_monthly_messages",
    "build_daily_payloads",
    "_parse_month_year",
    "_parse_day_date",
    "configure",
    "router",
]

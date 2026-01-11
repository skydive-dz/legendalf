from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import date, datetime
from html import escape
import json
from html.parser import HTMLParser
from typing import Iterable

import logging
import re
import requests
from aiogram import Router, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.exceptions import TelegramNetworkError

from retry_utils import retry_async, RETRY_DELAYS_SHORT
from bs4 import BeautifulSoup

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
    "bot_username": None,
}

_DETAILS_CACHE: dict[str, tuple[float, dict]] = {}
_DETAILS_TTL_SEC = 6 * 60 * 60
_SSO_RE = re.compile(r"var\s+it\s*=\s*(\{.*?\});", re.DOTALL)
_BACK_DELETE: dict[tuple[int, int], list[int]] = {}


def configure(*, is_allowed_fn, bot_username: str | None = None) -> None:
    _config.update({"is_allowed_fn": is_allowed_fn})
    if bot_username:
        _config["bot_username"] = bot_username.lstrip("@")


def set_bot_username(username: str | None) -> None:
    if not username:
        return
    _config["bot_username"] = username.lstrip("@")


async def _ensure_bot_username(bot) -> None:
    if _config.get("bot_username"):
        return
    try:
        me = await retry_async(
            lambda: bot.get_me(),
            logger=logger,
            delays=RETRY_DELAYS_SHORT,
            retry_exceptions=(TelegramNetworkError, asyncio.TimeoutError),
        )
    except Exception as exc:
        logger.warning("Failed to detect bot username: %s", exc)
        return
    if getattr(me, "username", None):
        _config["bot_username"] = me.username.lstrip("@")


async def _safe_answer(message: Message, text: str, **kwargs) -> bool:
    return await retry_async(
        lambda: message.answer(text, **kwargs),
        logger=logger,
        delays=RETRY_DELAYS_SHORT,
        retry_exceptions=(TelegramNetworkError, asyncio.TimeoutError),
    )


async def _safe_delete(message: Message) -> None:
    try:
        await retry_async(
            lambda: message.delete(),
            logger=logger,
            delays=RETRY_DELAYS_SHORT,
            retry_exceptions=(TelegramNetworkError, asyncio.TimeoutError),
        )
    except Exception as exc:
        logger.debug("Failed to delete message %s: %s", getattr(message, "message_id", None), exc)


async def _answer_with_retries(fn, *, label: str):
    delays = list(RETRY_DELAYS_SHORT)
    for attempt, delay in enumerate(delays, start=1):
        try:
            return await fn()
        except (TelegramNetworkError, asyncio.TimeoutError) as exc:
            logger.warning("Failed to %s (attempt %d): %s", label, attempt, exc)
            await asyncio.sleep(delay)
        except Exception as exc:  # pragma: no cover - safety net
            logger.warning("Failed to %s: %s", label, exc)
            return None
    return None


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


def _parse_bs_premieres(html: str, *, include_image: bool) -> list[PremiereItem]:
    soup = BeautifulSoup(html, "html.parser")
    items: list[PremiereItem] = []
    for div in soup.select("div.premier_item"):
        film_id = (div.get("id") or "").strip()
        meta_date = div.find("meta", attrs={"itemprop": "startDate"})
        date_iso = (meta_date.get("content") or "").strip() if meta_date else ""
        image_url = ""
        if include_image:
            meta_img = div.find("meta", attrs={"itemprop": "image"})
            image_url = _full_image_url((meta_img.get("content") or "").strip()) if meta_img else ""
        name_span = div.find("span", class_="name")
        title = _normalize_text(name_span.get_text() if name_span else "")
        a_href = ""
        a_tag = div.find("a", href=True)
        if a_tag:
            a_href = _full_url(a_tag.get("href"))

        country_director = ""
        genres = ""
        year = ""
        for span in div.find_all("span"):
            text = _normalize_text(span.get_text())
            if not text or text == title:
                continue
            if text.startswith("(") and text.endswith(")") and not genres:
                genres = text.strip("()")
            if not country_director and "реж." in text:
                country_director = text
            if not year:
                y = _find_year(text)
                if y:
                    year = y

        poster_url = _poster_from_film_id(film_id) or image_url
        if title:
            items.append(
                PremiereItem(
                    title=title,
                    url=a_href,
                    year=year,
                    date_iso=date_iso,
                    country_director=country_director,
                    genres=genres,
                    poster_url=poster_url,
                    film_id=film_id,
                )
            )
    return items


def _format_date(date_iso: str, *, pretty_month: bool) -> str:
    try:
        dt = date.fromisoformat(date_iso)
    except ValueError:
        return date_iso
    if pretty_month:
        month = _RU_MONTHS.get(dt.month, dt.strftime("%m"))
        return f"{dt.day} {month} {dt.year}"
    return dt.strftime("%d.%m.%Y")


def _fetch_page_html(url: str, headers: dict[str, str]) -> str:
    sess = requests.Session()
    resp1 = sess.get(url, headers=headers, timeout=20)
    resp1.encoding = resp1.encoding or "utf-8"
    resp1.raise_for_status()
    text = resp1.text

    m = _SSO_RE.search(text)
    if m:
        try:
            data = json.loads(m.group(1))
            host = data.get("host")
            if host:
                sess.get(host, headers=headers, timeout=10)
                resp2 = sess.get(url, headers=headers, timeout=20)
                resp2.encoding = resp2.encoding or "utf-8"
                resp2.raise_for_status()
                text = resp2.text
        except Exception as exc:
            logger.debug("SSO bootstrap failed, fallback to original body: %s", exc)
    return text


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


def _extract_film_id(url: str | None) -> str:
    if not url:
        return ""
    match = re.search(r"/film/(\d+)/", url)
    return match.group(1) if match else ""


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


@dataclass
class FilmDetails:
    title: str
    url: str
    poster_url: str
    short_desc: str
    full_desc: str
    alt_title: str
    year: str
    country: str
    genre: str
    director: str
    duration: str
    kp_rating: str
    imdb_rating: str
    tagline: str
    premiere_ru: str
    premiere_world: str
    age_rating: str
    writers: str
    producers: str
    operators: str
    composers: str
    designers: str
    editors: str
    actors: list[str]


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
    headers = {"User-Agent": _UA, "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8"}
    html = _fetch_page_html(url, headers=headers)
    parser = _PremiereParser(include_image=False)
    parser.feed(html)
    if not parser.items:
        parser.items = _parse_bs_premieres(html, include_image=False)
    logger.info("Parsed %d premieres from %s", len(parser.items), url)
    return parser.items


def _fetch_daily_premieres(target_date: date) -> list[PremiereItem]:
    url = _DAILY_URL.format(date=target_date.strftime("%Y-%m-%d"))
    headers = {"User-Agent": _UA, "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8"}
    html = _fetch_page_html(url, headers=headers)
    parser = _PremiereParser(include_image=True)
    parser.feed(html)
    if not parser.items:
        parser.items = _parse_bs_premieres(html, include_image=True)
    logger.info("Parsed %d premieres from %s", len(parser.items), url)
    if not parser.items:
        return []
    return [item for item in parser.items if item.date_iso == target_date.isoformat()] or parser.items


def _render_monthly_items(items: Iterable[PremiereItem]) -> list[str]:
    return [_format_item_block(item, pretty_month=True) for item in items]


def _render_monthly_blocks(items: Iterable[PremiereItem]) -> list[tuple[str, str]]:
    blocks: list[tuple[str, str]] = []
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

        text = f"{line1}\n{line2}" if line2 else line1
        film_id = item.film_id or _extract_film_id(item.url)
        blocks.append((text, film_id))
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


def build_daily_items(target_date: date) -> list[PremiereItem]:
    return _fetch_daily_premieres(target_date)


def build_daily_payloads(target_date: date) -> list[tuple[str, str]]:
    items = _fetch_daily_premieres(target_date)
    if not items:
        return []
    payloads: list[tuple[str, str]] = []
    for item in items:
        payloads.append((item.poster_url, _format_item_caption(item, pretty_month=False)))
    return payloads


def _format_item_caption(item: PremiereItem, *, pretty_month: bool) -> str:
    title = item.title
    if item.year:
        title = f"{title} ({item.year})"
    title = escape(title)
    url = escape(item.url)
    when = _format_date(item.date_iso, pretty_month=pretty_month)
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


def _details_link(film_id: str) -> str:
    username = _config.get("bot_username")
    if username:
        return f"tg://resolve?domain={username}&start=film_{film_id}"
    return f"/film {film_id}"


def _format_item_block(item: PremiereItem, *, pretty_month: bool) -> str:
    caption = _format_item_caption(item, pretty_month=pretty_month)
    film_id = item.film_id or _extract_film_id(item.url)
    if film_id:
        link = _details_link(film_id)
        if link.startswith("https://") or link.startswith("tg://"):
            return f'{caption}\n<a href="{escape(link)}">Подробности</a>'
        return f"{caption}\nПодробности: {escape(link)}"
    return caption


def _parse_duration_iso(value: str) -> str:
    if not value or not value.startswith("PT"):
        return ""
    hours = 0
    minutes = 0
    match_h = re.search(r"(\d+)H", value)
    match_m = re.search(r"(\d+)M", value)
    if match_h:
        hours = int(match_h.group(1))
    if match_m:
        minutes = int(match_m.group(1))
    parts = []
    if hours:
        parts.append(f"{hours} ч")
    if minutes:
        parts.append(f"{minutes} мин")
    return " ".join(parts)


def _cleanup_rating_value(value: str) -> str:
    if not value:
        return ""
    text = _normalize_text(value)
    match = re.search(r"([0-9]+)([.,]([0-9]))?", text)
    if not match:
        return text
    integer = match.group(1)
    frac = match.group(3) or ""
    return f"{integer}.{frac}" if frac else integer


def _cleanup_description(text: str) -> str:
    if not text:
        return ""
    cleaned = text.strip()
    marker = "подробная информация о фильме"
    idx = cleaned.lower().find(marker)
    if idx != -1:
        cleaned = cleaned[:idx].rstrip(" .,;-\n\t")
    return cleaned


@router.callback_query(F.data == "filmback")
async def cb_film_back(call: CallbackQuery) -> None:
    try:
        await call.answer()
    except Exception:
        pass
    if not call.message:
        return
    chat_id = call.message.chat.id
    key = (chat_id, call.message.message_id)
    ids_to_delete = _BACK_DELETE.pop(key, [])
    # Ensure current message is deleted as well
    ids_to_delete.append(call.message.message_id)
    # remove duplicates
    seen: set[int] = set()
    unique_ids = [mid for mid in ids_to_delete if not (mid in seen or seen.add(mid))]
    for mid in unique_ids:
        try:
            await call.bot.delete_message(chat_id, mid)
        except Exception as exc:
            logger.debug("Failed to delete related film message %s: %s", mid, exc)


def _parse_ld_json(soup: BeautifulSoup) -> dict:
    for script in soup.find_all("script", type="application/ld+json"):
        text = script.string or script.get_text(strip=True)
        if not text:
            continue
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, list):
            for item in payload:
                if isinstance(item, dict) and item.get("@type") == "Movie":
                    return item
        if isinstance(payload, dict) and payload.get("@type") == "Movie":
            return payload
    return {}


def _safe_name(value) -> str:
    if isinstance(value, dict):
        return value.get("name") or ""
    return str(value) if value else ""


def _parse_fact_rows(soup: BeautifulSoup) -> dict[str, str]:
    facts: dict[str, str] = {}
    selectors = [
        "section[data-test-id='Fact'] div.styles_row__da_r3",
        "section[data-tid='Fact'] div.styles_row__da_r3",
        "section[data-test-id='Fact'] div.factItem",
    ]
    for selector in selectors:
        for row in soup.select(selector):
            title_tag = (
                row.select_one("span.styles_title__qJkyc")
                or row.select_one("div.factItem__title")
                or row.select_one("[data-tid='Title']")
            )
            value_tag = (
                row.select_one("span.styles_value__g6yP4")
                or row.select_one("div.factItem__content")
                or row.select_one("[data-tid='Value']")
            )
            title = _normalize_text(title_tag.get_text(" ", strip=True)) if title_tag else ""
            value = _normalize_text(value_tag.get_text(" ", strip=True)) if value_tag else ""
            if title and value:
                facts[title.lower()] = value
        if facts:
            return facts

    # Fallback: new layout rows with data-tid="7cda04a5"
    for row in soup.select("[data-tid='7cda04a5']"):
        title_tag = row.select_one(".styles_title__hofDs") or row.select_one("[data-tid]")
        value_tag = row.select_one(".styles_value__HhLTP") or row.select_one("[data-tid='e1e37c21']")
        title = _normalize_text(title_tag.get_text(" ", strip=True)) if title_tag else ""
        value = _normalize_text(value_tag.get_text(" ", strip=True)) if value_tag else ""
        if title and value:
            facts[title.lower()] = value

    # Fallback: title/value pairs in generic layout with title/value classes
    for title_tag in soup.select(".styles_title__hofDs"):
        value_tag = title_tag.find_next(
            "div", class_=lambda c: c and "styles_value__HhLTP" in c
        )
        title = _normalize_text(title_tag.get_text(" ", strip=True))
        value = _normalize_text(value_tag.get_text(" ", strip=True)) if value_tag else ""
        if title and value:
            facts[title.lower()] = value

    return facts


def _parse_crew_rows(soup: BeautifulSoup) -> dict[str, str]:
    crew: dict[str, str] = {}
    selectors = [
        "ul.styles_list__rfm5v li.styles_root__ti07r",
        "ul[data-test-id='Crew'] li",
    ]
    for selector in selectors:
        for row in soup.select(selector):
            title_tag = row.select_one("p.styles_title___a1P7") or row.select_one("[data-tid='Title']")
            value_tag = row.select_one("div.styles_value__g6yP4") or row.select_one("[data-tid='Value']")
            title = _normalize_text(title_tag.get_text(" ", strip=True)) if title_tag else ""
            value = _normalize_text(value_tag.get_text(" ", strip=True)) if value_tag else ""
            if title and value:
                crew[title.lower()] = value
        if crew:
            return crew
    return crew


def _extract_full_description(soup: BeautifulSoup, *, ld_full_desc: str, short_desc: str) -> str:
    candidates: list[str] = []
    selectors = [
        "[data-test-id='FilmDescription__text']",
        "[data-test-id='Synopsis']",
        "[data-tid='Synopsis']",
        "div.film-synopsys",
        "[itemprop='description']",
    ]
    for selector in selectors:
        tag = soup.select_one(selector)
        if tag:
            raw_text = tag.get_text("\n", strip=True)
            text = "\n".join(part.strip() for part in raw_text.splitlines() if part.strip())
            normalized = _normalize_text(text)
            if normalized:
                candidates.append(text if "\n" in text else normalized)
    if ld_full_desc:
        candidates.append(_normalize_text(ld_full_desc))
    if short_desc:
        candidates.append(_normalize_text(short_desc))
    if not candidates:
        return ""
    return max(candidates, key=len)


def _fetch_ratings(film_id: str) -> tuple[str, str]:
    url = f"https://rating.kinopoisk.ru/{film_id}.xml"
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
    except Exception as exc:
        logger.debug("Failed to fetch rating XML for %s: %s", film_id, exc)
        return "", ""
    kp_rating = ""
    imdb_rating = ""
    try:
        try:
            soup = BeautifulSoup(resp.text, "xml")
        except Exception:
            soup = BeautifulSoup(resp.text, "html.parser")
        kp_tag = soup.find("kp_rating")
        imdb_tag = soup.find("imdb_rating")
        if kp_tag and kp_tag.get_text():
            kp_rating = _normalize_text(kp_tag.get_text())
        if imdb_tag and imdb_tag.get_text():
            imdb_rating = _normalize_text(imdb_tag.get_text())
    except Exception as exc:
        logger.debug("Failed to parse rating XML for %s: %s", film_id, exc)
    return kp_rating, imdb_rating


def _fetch_film_details(film_id: str, url: str) -> FilmDetails:
    headers = {"User-Agent": _UA, "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8"}
    html = _fetch_page_html(url, headers=headers)
    soup = BeautifulSoup(html, "html.parser")
    ld = _parse_ld_json(soup)

    title = _safe_name(ld.get("name")) or ""
    alt_title = ""
    for key in ("alternativeHeadline", "alternateName"):
        alt_title = _safe_name(ld.get(key))
        if alt_title:
            break

    poster_url = ""
    if isinstance(ld.get("image"), str):
        poster_url = ld.get("image") or ""
    if not poster_url:
        og_image = soup.find("meta", property="og:image")
        poster_url = og_image.get("content") if og_image else ""

    short_desc = ""
    og_desc = soup.find("meta", property="og:description")
    if og_desc:
        short_desc = _cleanup_description(og_desc.get("content") or "")

    ld_full_desc = _cleanup_description(_safe_name(ld.get("description")))
    full_desc = _extract_full_description(soup, ld_full_desc=ld_full_desc, short_desc=short_desc)
    if full_desc and short_desc and _normalize_text(full_desc) == _normalize_text(short_desc):
        full_desc = ""
        ld_full_desc = ""
    full_desc = _cleanup_description(full_desc)

    year = ""
    date_published = _safe_name(ld.get("datePublished"))
    if date_published:
        year = date_published.split("-", 1)[0]

    genre = ""
    if isinstance(ld.get("genre"), list):
        genre = ", ".join(_safe_name(g) for g in ld.get("genre") if _safe_name(g))
    elif isinstance(ld.get("genre"), str):
        genre = ld.get("genre") or ""

    director = ""
    if isinstance(ld.get("director"), list):
        director = ", ".join(_safe_name(d) for d in ld.get("director") if _safe_name(d))
    elif ld.get("director"):
        director = _safe_name(ld.get("director"))

    actors: list[str] = []
    if isinstance(ld.get("actor"), list):
        actors = [_safe_name(a) for a in ld.get("actor") if _safe_name(a)]
    elif ld.get("actor"):
        actors = [_safe_name(ld.get("actor"))]

    country = ""
    origin = ld.get("countryOfOrigin")
    if isinstance(origin, list):
        country = ", ".join(_safe_name(o) for o in origin if _safe_name(o))
    elif origin:
        country = _safe_name(origin)

    duration = _parse_duration_iso(_safe_name(ld.get("duration")))

    facts = _parse_fact_rows(soup)
    kp_rating = facts.get("рейтинг кинопоиска", "")
    imdb_rating = facts.get("рейтинг imdb", "") or facts.get("рейтинг imdb.com", "")
    tagline = facts.get("слоган", "")
    premiere_ru = facts.get("премьера в россии", "") or facts.get("премьера в рф", "")
    premiere_world = facts.get("премьера в мире", "")
    age_rating = facts.get("возраст", "") or facts.get("возрастной рейтинг", "")

    crew_map = _parse_crew_rows(soup)
    if crew_map.get("режиссер") and not director:
        director = crew_map.get("режиссер", "")
    writers = crew_map.get("сценарий", "")
    producers = crew_map.get("продюсер", "")
    operators = crew_map.get("оператор", "")
    composers = crew_map.get("композитор", "")
    designers = crew_map.get("художник", "")
    editors = crew_map.get("монтаж", "")

    if not kp_rating or not imdb_rating:
        kp_xml, imdb_xml = _fetch_ratings(film_id)
        if not kp_rating:
            kp_rating = kp_xml
        if not imdb_rating:
            imdb_rating = imdb_xml

    if not duration:
        duration = facts.get("время", "")

    if not full_desc:
        full_desc = ld_full_desc or ""
    if not short_desc and full_desc:
        short_desc = full_desc

    kp_rating = _cleanup_rating_value(kp_rating)
    imdb_rating = _cleanup_rating_value(imdb_rating)

    if not title:
        og_title = soup.find("meta", property="og:title")
        title = og_title.get("content") if og_title else ""
    if not title:
        page_title = soup.title.string if soup.title else ""
        title = _normalize_text(page_title)
    if not title:
        title = url
    if not alt_title:
        og_title = soup.find("meta", property="og:title")
        if og_title and og_title.get("content"):
            alt_title = ""

    return FilmDetails(
        title=title,
        url=url,
        poster_url=poster_url,
        short_desc=_normalize_text(short_desc),
        full_desc=full_desc.strip(),
        alt_title=alt_title.strip(),
        year=year,
        country=country.strip(),
        genre=genre.strip(),
        director=director.strip(),
        duration=duration,
        kp_rating=kp_rating,
        imdb_rating=imdb_rating,
        tagline=tagline,
        premiere_ru=premiere_ru,
        premiere_world=premiere_world,
        age_rating=age_rating,
        writers=writers,
        producers=producers,
        operators=operators,
        composers=composers,
        designers=designers,
        editors=editors,
        actors=[a for a in actors if a],
    )


def _get_film_details(film_id: str, url: str) -> FilmDetails:
    now = datetime.now().timestamp()
    cached = _DETAILS_CACHE.get(film_id)
    if cached and now - cached[0] < _DETAILS_TTL_SEC:
        try:
            return FilmDetails(**cached[1])
        except TypeError:
            _DETAILS_CACHE.pop(film_id, None)
    details = _fetch_film_details(film_id, url)
    _DETAILS_CACHE[film_id] = (now, details.__dict__)
    return details


async def _send_long_text(message: Message, text: str, *, reply_markup=None) -> list[int]:
    if not text:
        return []
    sent_ids: list[int] = []
    limit = 3800
    chunks = [text[i : i + limit] for i in range(0, len(text), limit)]
    for idx, chunk in enumerate(chunks):
        rm = reply_markup if idx == len(chunks) - 1 else None
        msg = await _answer_with_retries(
            lambda: message.answer(chunk, parse_mode="HTML", reply_markup=rm),
            label="send film text",
        )
        if msg and getattr(msg, "message_id", None):
            sent_ids.append(msg.message_id)
    return sent_ids


def _build_messages(
    items: list[PremiereItem],
    *,
    pretty_month: bool,
    max_len: int = 3500,
) -> list[str]:
    blocks = [_format_item_block(item, pretty_month=pretty_month) for item in items]
    return _chunk_messages(blocks, max_len=max_len)


def _build_info_lines(details: FilmDetails) -> list[str]:
    pairs = [
        ("Рейтинг Кинопоиска", details.kp_rating),
        ("Рейтинг IMDb", details.imdb_rating),
        ("Время", details.duration),
        ("Год производства", details.year),
        ("Страна", details.country),
        ("Жанр", details.genre),
        ("Слоган", details.tagline),
        ("Режиссер", details.director),
        ("Сценарий", details.writers),
        ("Продюсер", details.producers),
        ("Оператор", details.operators),
        ("Композитор", details.composers),
        ("Художник", details.designers),
        ("Монтаж", details.editors),
        ("Премьера в России", details.premiere_ru),
        ("Премьера в мире", details.premiere_world),
        ("Возраст", details.age_rating),
    ]
    info_lines: list[str] = []
    for label, value in pairs:
        if value:
            info_lines.append(f"<b>{escape(label)}</b> — {escape(value)}")
    return info_lines


def _compose_details_text(details: FilmDetails) -> str:
    title_main = escape(details.title)
    title_alt = escape(details.alt_title) if details.alt_title else ""
    title = title_main
    if details.year and details.year not in details.title:
        title = f"{title} ({escape(details.year)})"

    blocks: list[str] = [f"<b>{title}</b>"]
    if title_alt:
        blocks.append(title_alt)
    description = details.full_desc or details.short_desc
    if description:
        blocks.append(escape(description))

    info_lines = _build_info_lines(details)
    if info_lines:
        blocks.append("<b>О фильме</b>\n" + "\n".join(info_lines))

    if details.actors:
        actors_text = "\n".join(escape(name) for name in details.actors)
        blocks.append(f"<b>В главных ролях</b>\n{actors_text}")

    return "\n\n".join(blocks).strip()


async def _send_film_details(message: Message, details: FilmDetails) -> None:
    back_markup = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="Назад", callback_data="filmback")]]
    )
    sent_messages: list[int] = []
    text = _compose_details_text(details)
    caption_limit = 1024
    caption_sent = False
    back_anchor: tuple[int, int] | None = None

    if details.poster_url:
        try:
            if text and len(text) <= caption_limit:
                msg = await _answer_with_retries(
                    lambda: message.answer_photo(
                        details.poster_url,
                        caption=text,
                        parse_mode="HTML",
                        reply_markup=back_markup,
                    ),
                    label="send film poster",
                )
                if msg:
                    sent_messages.append(msg.message_id)
                    caption_sent = True
                    back_anchor = (msg.chat.id, msg.message_id)
                else:
                    caption_sent = True
            else:
                msg = await _answer_with_retries(
                    lambda: message.answer_photo(details.poster_url),
                    label="send film poster",
                )
                if msg:
                    sent_messages.append(msg.message_id)
        except Exception as exc:
            logger.warning("Failed to send film poster %s: %s", details.url, exc)

    if text and not caption_sent:
        text_ids = await _send_long_text(message, text, reply_markup=back_markup)
        if text_ids:
            sent_messages.extend(text_ids)
            back_anchor = (message.chat.id, text_ids[-1])

    if back_anchor:
        _BACK_DELETE[back_anchor] = sent_messages


@router.message(Command("films_month"))
async def cmd_films_month(message: Message) -> None:
    logger.info("Command /films_month from %s", message.from_user.id)
    if not _config["is_allowed_fn"](message.from_user.id):
        await _safe_answer(message, "Доступ закрыт. Отправьте /mellon, чтобы запросить доступ.")
        return
    await _ensure_bot_username(message.bot)

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
        items = await asyncio.to_thread(_fetch_monthly_premieres, target)
    except Exception as exc:
        logger.warning("Failed to fetch premieres: %s", exc)
        await _safe_answer(message, "Не получилось получить список премьер. Попробуйте позже.")
        return

    if not items:
        await _safe_answer(message, "На этот месяц премьер не найдено.")
        return

    messages = _build_messages(items, pretty_month=True)
    for payload in messages:
        await _safe_answer(message, payload, parse_mode="HTML", disable_web_page_preview=True)


@router.message(Command("films_day"))
async def cmd_films_day(message: Message) -> None:
    logger.info("Command /films_day from %s", message.from_user.id)
    if not _config["is_allowed_fn"](message.from_user.id):
        await _safe_answer(message, "Доступ закрыт. Отправьте /mellon, чтобы запросить доступ.")
        return
    await _ensure_bot_username(message.bot)

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
        items = await asyncio.to_thread(build_daily_items, target)
    except Exception as exc:
        logger.warning("Failed to fetch daily premieres: %s", exc)
        await _safe_answer(message, "Не получилось получить список премьер дня. Попробуйте позже.")
        return

    if not items:
        await _safe_answer(message, "Фильмов сегодня нет, Гэндальф грустит 😢")
        return

    for item in items:
        caption = _format_item_block(item, pretty_month=False)
        if item.poster_url:
            try:
                await message.answer_photo(item.poster_url, caption=caption, parse_mode="HTML")
                continue
            except Exception as exc:
                logger.warning("Failed to send films_day poster %s: %s", item.poster_url, exc)
        await _safe_answer(message, caption, parse_mode="HTML", disable_web_page_preview=True)


@router.message(Command("start"))
async def cmd_start(message: Message) -> None:
    args = (message.text or "").split(maxsplit=1)
    if len(args) < 2:
        return
    payload = args[1].strip()
    if not payload.startswith("film_"):
        return
    film_id = payload.split("_", 1)[1]
    if not film_id.isdigit():
        return
    logger.info("Deep link film_%s from %s", film_id, message.from_user.id)
    if not _config["is_allowed_fn"](message.from_user.id):
        await _safe_answer(message, "Доступ закрыт. Отправьте /mellon, чтобы запросить доступ.")
        return
    url = f"{_BASE_URL}/film/{film_id}/"
    try:
        details = await asyncio.to_thread(_get_film_details, film_id, url)
    except Exception as exc:
        logger.warning("Failed to fetch film details %s: %s", film_id, exc)
        await _safe_answer(message, "Не удалось получить подробности о фильме.")
        return
    await _send_film_details(message, details)
    await _safe_delete(message)


@router.message(Command("film"))
async def cmd_film(message: Message) -> None:
    logger.info("Command /film from %s", message.from_user.id)
    if not _config["is_allowed_fn"](message.from_user.id):
        await _safe_answer(message, "Доступ закрыт. Отправьте /mellon, чтобы запросить доступ.")
        return

    args = (message.text or "").split(maxsplit=1)
    if len(args) < 2:
        await _safe_answer(
            message,
            "Укажи фильм. Пример: /film 26822 или /film https://www.kinopoisk.ru/film/26822/",
        )
        return

    raw = args[1].strip()
    film_id = ""
    url = ""
    if raw.isdigit():
        film_id = raw
        url = f"{_BASE_URL}/film/{film_id}/"
    else:
        url = _full_url(raw)
        film_id = _extract_film_id(url)

    if not film_id:
        await _safe_answer(
            message,
            "Не вижу id фильма. Пример: /film 26822 или /film https://www.kinopoisk.ru/film/26822/",
        )
        return

    try:
        details = await asyncio.to_thread(_get_film_details, film_id, url)
    except Exception as exc:
        logger.warning("Failed to fetch film details %s: %s", film_id, exc)
        await _safe_answer(message, "Не удалось получить подробности о фильме.")
        return

    await _send_film_details(message, details)


__all__ = [
    "build_monthly_messages",
    "build_daily_payloads",
    "build_daily_items",
    "_parse_month_year",
    "_parse_day_date",
    "configure",
    "router",
]

from __future__ import annotations

import html as html_lib
import io
import threading
from collections import OrderedDict
from dataclasses import dataclass
from datetime import date, datetime
from typing import Iterable
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup


class HolidayFetchError(Exception):
    """Raised when holidays for the requested date cannot be fetched."""


@dataclass(slots=True)
class HolidayItem:
    title: str
    description: str | None
    url: str | None
    category: str | None


@dataclass(slots=True)
class HolidayDaily:
    date_key: str
    headline: str | None
    items: list[HolidayItem]
    image_url: str | None
    image_bytes: bytes | None
    image_name: str | None
    name_titles: list[str]


MONTH_NAMES = [
    "января",
    "февраля",
    "марта",
    "апреля",
    "мая",
    "июня",
    "июля",
    "августа",
    "сентября",
    "октября",
    "ноября",
    "декабря",
]

MONTH_SLUGS = [
    "yanvarya",
    "fevralya",
    "marta",
    "aprelya",
    "maya",
    "iyunya",
    "iyulya",
    "avgusta",
    "sentyabrya",
    "oktyabrya",
    "noyabrya",
    "dekabrya",
]

WEEKDAY_SLUGS = [
    "ponedelnik",
    "vtornik",
    "sreda",
    "chetverg",
    "pyatnitsa",
    "subbota",
    "voskresene",
]


class HolidayService:
    BASE_DAY_URL = "https://www.calend.ru/day/{date}/"
    BASE_DAILY_URL = "https://www.calend.ru/calendar/daily/{slug}/"
    BASE_SITE_URL = "https://www.calend.ru"

    def __init__(self, *, max_cache: int = 3, session: requests.Session | None = None) -> None:
        self.max_cache = max_cache
        self.session = session or requests.Session()
        self._cache: OrderedDict[str, HolidayDaily] = OrderedDict()
        self._lock = threading.RLock()

    def get_daily(self, target_date: date) -> HolidayDaily:
        date_key = target_date.strftime("%Y-%m-%d")
        with self._lock:
            cached = self._cache.get(date_key)
            if cached:
                return cached

        day_html = self._fetch_day_page(date_key)

        detail_html = None
        try:
            slug = self._compose_slug(target_date)
            detail_html = self._fetch_url(self.BASE_DAILY_URL.format(slug=slug))
        except HolidayFetchError:
            detail_html = None

        daily = self._parse_daily(day_html, detail_html, date_key, target_date)

        with self._lock:
            self._cache[date_key] = daily
            while len(self._cache) > self.max_cache:
                self._cache.popitem(last=False)
        return daily

    def _fetch_url(self, url: str) -> str:
        try:
            resp = self.session.get(url, timeout=60)
            resp.raise_for_status()
            return resp.text
        except requests.RequestException as exc:
            raise HolidayFetchError(f"Не удалось получить страницу {url} ({exc}).") from exc

    def _fetch_day_page(self, date_key: str) -> str:
        return self._fetch_url(self.BASE_DAY_URL.format(date=date_key))

    def _parse_daily(
        self,
        day_html: str,
        detail_html: str | None,
        date_key: str,
        target_date: date,
    ) -> HolidayDaily:
        day_soup = BeautifulSoup(day_html, "html.parser")
        detail_soup = BeautifulSoup(detail_html, "html.parser") if detail_html else day_soup

        items = self._parse_detail_items(detail_soup)
        if not items:
            items = self._parse_day_items(day_soup)

        if not items:
            raise HolidayFetchError("Страница не вернула ни одного праздника.")

        image_url = self._extract_feature_image(detail_soup)
        if not image_url:
            first_li = day_soup.select_one(".block.holidays ul.itemsNet li")
            image_url = self._extract_image_url(first_li)

        image_bytes: bytes | None = None
        image_name: str | None = None
        if image_url:
            image_bytes, image_name = self._download_image(image_url)

        heading_tag = detail_soup.select_one("h1") or day_soup.select_one("h1")
        headline = heading_tag.get_text(strip=True) if heading_tag else None

        names = self._parse_names(day_soup)

        return HolidayDaily(
            date_key=date_key,
            headline=headline,
            items=items,
            image_url=image_url,
            image_bytes=image_bytes,
            image_name=image_name,
            name_titles=names,
        )

    def _compose_slug(self, target_date: date) -> str:
        day = target_date.day
        month_slug = MONTH_SLUGS[target_date.month - 1]
        year = target_date.year
        weekday_slug = WEEKDAY_SLUGS[target_date.weekday()]
        return f"{day}-{month_slug}-{year}-goda-{weekday_slug}"

    def _parse_names(self, soup: BeautifulSoup) -> list[str]:
        names: list[str] = []
        block = soup.select_one(".block.nameDay")
        if not block:
            return names
        for a in block.select("a.title"):
            title = a.get_text(strip=True)
            if title:
                names.append(title)
        return names

    def _parse_detail_items(self, soup: BeautifulSoup) -> list[HolidayItem]:
        target_ul = None
        for h3 in soup.find_all("h3"):
            text = h3.get_text(" ", strip=True).lower()
            if "что важного" in text:
                candidate = h3.find_next("ul")
                if candidate:
                    target_ul = candidate
                    break
        if target_ul is None:
            return []

        items: list[HolidayItem] = []
        for li in target_ul.find_all("li"):
            link = li.find("a")
            if link:
                title = link.get_text(strip=True)
                href = urljoin(self.BASE_SITE_URL, link.get("href"))
            else:
                title = li.get_text(strip=True)
                href = None
            if not title:
                continue
            items.append(
                HolidayItem(
                    title=title,
                    description=None,
                    url=href,
                    category=None,
                )
            )
        return items

    def _parse_day_items(self, soup: BeautifulSoup) -> list[HolidayItem]:
        block = soup.select_one(".block.holidays ul.itemsNet")
        if block is None:
            return []

        items: list[HolidayItem] = []
        for li in block.select("li"):
            title_tag = li.select_one(".caption .title a")
            if not title_tag:
                continue
            title = title_tag.get_text(strip=True)
            link = title_tag.get("href")
            items.append(
                HolidayItem(
                    title=title,
                    description=None,
                    url=urljoin(self.BASE_SITE_URL, link) if link else None,
                    category=None,
                )
            )
        return items

    def _extract_image_url(self, node) -> str | None:
        if node is None:
            return None
        img = node.find("img")
        if img and img.get("src"):
            return urljoin(self.BASE_SITE_URL, img["src"])
        image_div = node.select_one(".image")
        if not image_div:
            return None
        style = image_div.get("style") or ""
        marker = "url("
        if marker not in style:
            return None
        start = style.find(marker) + len(marker)
        end = style.find(")", start)
        if end == -1:
            return None
        raw = style[start:end].strip().strip("'\"")
        if not raw:
            return None
        return urljoin(self.BASE_SITE_URL, raw)

    def _extract_feature_image(self, soup: BeautifulSoup) -> str | None:
        img = soup.select_one(".wp-caption img, .single-post-thumb img")
        if img:
            src = img.get("data-lazy-src") or img.get("data-src") or img.get("src")
            if not src:
                noscript = img.find_next("noscript")
                if noscript:
                    inner = BeautifulSoup(noscript.text, "html.parser").find("img")
                    if inner and inner.get("src"):
                        src = inner["src"]
            if src:
                return urljoin(self.BASE_SITE_URL, src)
        return None

    def _download_image(self, url: str) -> tuple[bytes | None, str | None]:
        try:
            resp = self.session.get(url, timeout=60)
            resp.raise_for_status()
            name = urlparse(url).path.rsplit("/", 1)[-1] or "holidays.jpg"
            return resp.content, name
        except requests.RequestException:
            return None, None


def _escape(text: str) -> str:
    return html_lib.escape(text, quote=False)


def build_holiday_caption(daily: HolidayDaily, limit: int | None = None, include_names: bool = True) -> str:
    try:
        display_date = datetime.strptime(daily.date_key, "%Y-%m-%d").strftime("%d.%m.%Y")
    except Exception:
        display_date = daily.date_key
    lines: list[str] = []
    items = daily.items if limit is None else daily.items[:limit]
    for item in items:
        line = f"• {_escape(item.title)}"
        if item.description:
            line += f" — {_escape(item.description)}"
        elif item.category:
            line += f" ({_escape(item.category)})"
        lines.append(line)
    body = "\n".join(lines) if lines else "Сегодня подходящий день, чтобы просто радоваться жизни."
    message_lines = [f"<b>Сегодня ({display_date}) Средиземье празднует:</b>", body]

    if include_names:
        name_section = build_name_section(daily.date_key, daily.name_titles)
        if name_section:
            message_lines.append("")
            message_lines.append(name_section)

    return "\n".join(message_lines)


def build_name_section(date_key: str, names: Iterable[str]) -> str:
    names_clean = [n.strip() for n in names if n and n.strip()]
    if not names_clean:
        return ""
    try:
        dt = datetime.strptime(date_key, "%Y-%m-%d")
        month_name = MONTH_NAMES[dt.month - 1]
        date_str = f"{dt.day} {month_name}"
    except Exception:
        date_str = date_key

    names_line = ", ".join(_escape(n) for n in names_clean)
    text = (
        "<b>Кто сегодня именинник?</b>\n"
        f"{_escape(date_str)} празднуют именины {names_line}.\n"
        "Уважаемые именинники, примите поздравления от Гэндальфа!"
    )
    return text


def image_stream(daily: HolidayDaily):
    if not daily.image_bytes:
        return None
    bio = io.BytesIO(daily.image_bytes)
    bio.name = daily.image_name or "holidays.jpg"
    return bio

from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path

_LOCK = threading.RLock()


def _now_iso_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def _ensure_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS admins (
            user_id INTEGER PRIMARY KEY
        );

        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            status TEXT NOT NULL,
            added_at TEXT,
            requested_at TEXT,
            birthday TEXT
        );

        CREATE TABLE IF NOT EXISTS schedules (
            user_id INTEGER PRIMARY KEY,
            enabled INTEGER NOT NULL,
            tz TEXT,
            special_flags TEXT
        );

        CREATE TABLE IF NOT EXISTS schedule_kinds (
            user_id INTEGER NOT NULL,
            kind TEXT NOT NULL,
            enabled INTEGER NOT NULL,
            at_time TEXT,
            last_sent TEXT,
            PRIMARY KEY (user_id, kind)
        );
        """
    )


def _is_empty(conn: sqlite3.Connection) -> bool:
    cur = conn.execute("SELECT COUNT(*) FROM admins")
    if cur.fetchone()[0] != 0:
        return False
    cur = conn.execute("SELECT COUNT(*) FROM users")
    if cur.fetchone()[0] != 0:
        return False
    cur = conn.execute("SELECT COUNT(*) FROM schedules")
    if cur.fetchone()[0] != 0:
        return False
    cur = conn.execute("SELECT COUNT(*) FROM schedule_kinds")
    if cur.fetchone()[0] != 0:
        return False
    return True


def migrate_from_json(db_path: Path, json_path: Path) -> None:
    if not json_path.exists():
        return
    data = json.loads(json_path.read_text(encoding="utf-8"))
    save_data(db_path, data)


def load_data(db_path: Path, json_path: Path | None = None) -> dict:
    with _LOCK:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = _connect(db_path)
        try:
            _ensure_db(conn)
            if _is_empty(conn) and json_path is not None and json_path.exists():
                migrate_from_json(db_path, json_path)

            data = {"admins": [], "allowed": {}, "pending": {}, "schedules": {}}

            cur = conn.execute("SELECT user_id FROM admins ORDER BY user_id")
            data["admins"] = [row[0] for row in cur.fetchall()]

            cur = conn.execute(
                """
                SELECT user_id, username, first_name, last_name, status, added_at, requested_at, birthday
                FROM users
                """
            )
            for (
                user_id,
                username,
                first_name,
                last_name,
                status,
                added_at,
                requested_at,
                birthday,
            ) in cur.fetchall():
                meta = {
                    "username": username,
                    "first_name": first_name,
                    "last_name": last_name,
                }
                if status == "allowed":
                    meta["added_at"] = added_at or _now_iso_utc()
                    if birthday:
                        meta["birthday"] = birthday
                    data["allowed"][str(user_id)] = meta
                elif status == "pending":
                    meta["requested_at"] = requested_at or _now_iso_utc()
                    data["pending"][str(user_id)] = meta

            schedules: dict[str, dict] = {}
            cur = conn.execute("SELECT user_id, enabled, tz, special_flags FROM schedules")
            for user_id, enabled, tz, special_flags in cur.fetchall():
                entry = {
                    "enabled": bool(enabled),
                    "tz": tz or "",
                    "kinds": {},
                    "special_flags": json.loads(special_flags) if special_flags else {},
                }
                schedules[str(user_id)] = entry

            cur = conn.execute(
                "SELECT user_id, kind, enabled, at_time, last_sent FROM schedule_kinds"
            )
            for user_id, kind, enabled, at_time, last_sent in cur.fetchall():
                entry = schedules.setdefault(
                    str(user_id),
                    {"enabled": True, "tz": "", "kinds": {}, "special_flags": {}},
                )
                entry["kinds"][kind] = {
                    "enabled": bool(enabled),
                    "at_time": at_time or "",
                    "last_sent": json.loads(last_sent) if last_sent else {},
                }

            data["schedules"] = schedules
            return data
        finally:
            conn.close()


def save_data(db_path: Path, data: dict) -> None:
    with _LOCK:
        conn = _connect(db_path)
        try:
            _ensure_db(conn)
            conn.execute("BEGIN")
            conn.execute("DELETE FROM admins")
            conn.execute("DELETE FROM users")
            conn.execute("DELETE FROM schedules")
            conn.execute("DELETE FROM schedule_kinds")

            for admin_id in data.get("admins", []):
                try:
                    conn.execute("INSERT INTO admins (user_id) VALUES (?)", (int(admin_id),))
                except Exception:
                    continue

            allowed = data.get("allowed", {})
            if isinstance(allowed, dict):
                for suid, meta in allowed.items():
                    try:
                        uid = int(suid)
                    except Exception:
                        continue
                    conn.execute(
                        """
                        INSERT INTO users (user_id, username, first_name, last_name, status, added_at, birthday)
                        VALUES (?, ?, ?, ?, 'allowed', ?, ?)
                        """,
                        (
                            uid,
                            meta.get("username"),
                            meta.get("first_name"),
                            meta.get("last_name"),
                            meta.get("added_at") or _now_iso_utc(),
                            meta.get("birthday"),
                        ),
                    )

            pending = data.get("pending", {})
            if isinstance(pending, dict):
                for suid, meta in pending.items():
                    try:
                        uid = int(suid)
                    except Exception:
                        continue
                    conn.execute(
                        """
                        INSERT INTO users (user_id, username, first_name, last_name, status, requested_at)
                        VALUES (?, ?, ?, ?, 'pending', ?)
                        """,
                        (
                            uid,
                            meta.get("username"),
                            meta.get("first_name"),
                            meta.get("last_name"),
                            meta.get("requested_at") or _now_iso_utc(),
                        ),
                    )

            schedules = data.get("schedules", {})
            if isinstance(schedules, dict):
                for suid, entry in schedules.items():
                    try:
                        uid = int(suid)
                    except Exception:
                        continue
                    enabled = 1 if entry.get("enabled", True) else 0
                    tz = entry.get("tz") or ""
                    special_flags = json.dumps(entry.get("special_flags", {}), ensure_ascii=False)
                    conn.execute(
                        """
                        INSERT INTO schedules (user_id, enabled, tz, special_flags)
                        VALUES (?, ?, ?, ?)
                        """,
                        (uid, enabled, tz, special_flags),
                    )
                    kinds = entry.get("kinds", {})
                    if not isinstance(kinds, dict):
                        continue
                    for kind, kind_entry in kinds.items():
                        if not isinstance(kind_entry, dict):
                            continue
                        k_enabled = 1 if kind_entry.get("enabled", False) else 0
                        at_time = (kind_entry.get("at_time") or "").strip()
                        last_sent = json.dumps(kind_entry.get("last_sent", {}), ensure_ascii=False)
                        conn.execute(
                            """
                            INSERT INTO schedule_kinds (user_id, kind, enabled, at_time, last_sent)
                            VALUES (?, ?, ?, ?, ?)
                            """,
                            (uid, kind, k_enabled, at_time, last_sent),
                        )

            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

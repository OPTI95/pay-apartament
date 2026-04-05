import sqlite3
import json
import os
from pathlib import Path

DB_PATH = "data/apartments.db"


def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    Path("data").mkdir(exist_ok=True)
    with _get_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS apartments (
                id               TEXT PRIMARY KEY,
                name             TEXT NOT NULL,
                aliases          TEXT DEFAULT '[]',
                address          TEXT NOT NULL,
                price_per_sqm    INTEGER NOT NULL,
                floor_prices     TEXT DEFAULT NULL,
                description      TEXT DEFAULT '',
                main_photo       TEXT NOT NULL,
                photos_url       TEXT NOT NULL,
                layouts_url      TEXT NOT NULL,
                chess_url        TEXT NOT NULL,
                installment_text TEXT NOT NULL,
                created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS calculators (
                apt_id TEXT PRIMARY KEY,
                data   TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS admins (
                user_id INTEGER PRIMARY KEY
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS subscriptions (
                user_id    INTEGER PRIMARY KEY,
                plan       TEXT NOT NULL,
                expires_at TIMESTAMP NOT NULL,
                charge_id  TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS districts (
                id   INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL
            )
        """)
        # Migrate existing DB: add columns if missing
        for alter in [
            "ALTER TABLE apartments ADD COLUMN floor_prices TEXT DEFAULT NULL",
            "ALTER TABLE apartments ADD COLUMN district_id INTEGER DEFAULT NULL",
            "ALTER TABLE apartments ADD COLUMN photos_file_ids TEXT DEFAULT '[]'",
            "ALTER TABLE apartments ADD COLUMN installment_price_per_sqm INTEGER DEFAULT NULL",
        ]:
            try:
                conn.execute(alter)
            except Exception:
                pass
        conn.commit()
    _migrate_from_json()


def _migrate_from_json() -> None:
    json_path = "data/apartments.json"
    if not os.path.exists(json_path):
        return
    with _get_conn() as conn:
        count = conn.execute("SELECT COUNT(*) FROM apartments").fetchone()[0]
        if count > 0:
            return
    with open(json_path, "r", encoding="utf-8") as f:
        apartments = json.load(f)
    for apt in apartments:
        save_apartment(apt)


def save_apartment(apt: dict) -> None:
    floor_prices = apt.get("floor_prices")
    floor_prices_json = json.dumps(floor_prices, ensure_ascii=False) if floor_prices else None
    photos_file_ids = apt.get("photos_file_ids") or []
    with _get_conn() as conn:
        conn.execute("""
            INSERT OR REPLACE INTO apartments
            (id, name, aliases, address, price_per_sqm, floor_prices, description,
             main_photo, photos_url, layouts_url, chess_url, installment_text, district_id,
             photos_file_ids, installment_price_per_sqm)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            apt["id"],
            apt["name"],
            json.dumps(apt.get("aliases", []), ensure_ascii=False),
            apt["address"],
            apt["price_per_sqm"],
            floor_prices_json,
            apt.get("description", ""),
            apt["main_photo"],
            apt.get("photos_url", ""),
            apt["layouts_url"],
            apt["chess_url"],
            apt.get("installment_text", ""),
            apt.get("district_id"),
            json.dumps(photos_file_ids, ensure_ascii=False),
            apt.get("installment_price_per_sqm"),
        ))
        conn.commit()


def get_all_apartments() -> list[dict]:
    with _get_conn() as conn:
        rows = conn.execute("SELECT * FROM apartments ORDER BY created_at").fetchall()
    result = []
    for row in rows:
        apt = dict(row)
        apt["aliases"] = json.loads(apt["aliases"])
        apt["floor_prices"] = json.loads(apt["floor_prices"]) if apt.get("floor_prices") else None
        apt["photos_file_ids"] = json.loads(apt["photos_file_ids"]) if apt.get("photos_file_ids") else []
        result.append(apt)
    return result


_ALLOWED_FIELDS = {
    "name", "address", "price_per_sqm", "installment_price_per_sqm", "description",
    "main_photo", "photos_url", "photos_file_ids", "layouts_url", "chess_url",
    "installment_text", "district_id",
}


def delete_apartment(apt_id: str) -> None:
    with _get_conn() as conn:
        conn.execute("DELETE FROM apartments WHERE id = ?", (apt_id,))
        conn.execute("DELETE FROM calculators WHERE apt_id = ?", (apt_id,))
        conn.commit()


def update_apartment_field(apt_id: str, field: str, value) -> None:
    if field not in _ALLOWED_FIELDS:
        raise ValueError(f"Unknown field: {field}")
    with _get_conn() as conn:
        conn.execute(f"UPDATE apartments SET {field} = ? WHERE id = ?", (value, apt_id))
        conn.commit()


def update_apartment_aliases(apt_id: str, aliases: list) -> None:
    with _get_conn() as conn:
        conn.execute(
            "UPDATE apartments SET aliases = ? WHERE id = ?",
            (json.dumps(aliases, ensure_ascii=False), apt_id),
        )
        conn.commit()


def build_search_index(apartments: list[dict]) -> dict[str, dict]:
    index: dict[str, dict] = {}
    for apt in apartments:
        index[apt["name"].upper()] = apt
        for alias in apt.get("aliases", []):
            index[alias.upper()] = apt
    return index


# ---------------------------------------------------------------------------
# Calculator
# ---------------------------------------------------------------------------

def save_calculator(apt_id: str, data: dict) -> None:
    with _get_conn() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO calculators (apt_id, data) VALUES (?, ?)",
            (apt_id, json.dumps(data, ensure_ascii=False)),
        )
        conn.commit()


def get_calculator(apt_id: str) -> dict | None:
    with _get_conn() as conn:
        row = conn.execute(
            "SELECT data FROM calculators WHERE apt_id = ?", (apt_id,)
        ).fetchone()
    return json.loads(row["data"]) if row else None


def delete_calculator(apt_id: str) -> None:
    with _get_conn() as conn:
        conn.execute("DELETE FROM calculators WHERE apt_id = ?", (apt_id,))
        conn.commit()


# ---------------------------------------------------------------------------
# Admin management
# ---------------------------------------------------------------------------

def add_admin(user_id: int) -> None:
    with _get_conn() as conn:
        conn.execute("INSERT OR IGNORE INTO admins (user_id) VALUES (?)", (user_id,))
        conn.commit()


def remove_admin(user_id: int) -> None:
    with _get_conn() as conn:
        conn.execute("DELETE FROM admins WHERE user_id = ?", (user_id,))
        conn.commit()


def get_admin_ids() -> set[int]:
    with _get_conn() as conn:
        rows = conn.execute("SELECT user_id FROM admins").fetchall()
    return {row["user_id"] for row in rows}


# ---------------------------------------------------------------------------
# Districts
# ---------------------------------------------------------------------------

def add_district(name: str) -> int:
    """Add a district, return its id."""
    with _get_conn() as conn:
        cur = conn.execute(
            "INSERT OR IGNORE INTO districts (name) VALUES (?)", (name,)
        )
        conn.commit()
        if cur.lastrowid:
            return cur.lastrowid
        row = conn.execute("SELECT id FROM districts WHERE name = ?", (name,)).fetchone()
        return row["id"]


def remove_district(district_id: int) -> None:
    with _get_conn() as conn:
        conn.execute("DELETE FROM districts WHERE id = ?", (district_id,))
        conn.execute(
            "UPDATE apartments SET district_id = NULL WHERE district_id = ?", (district_id,)
        )
        conn.commit()


def get_all_districts() -> list[dict]:
    with _get_conn() as conn:
        rows = conn.execute("SELECT id, name FROM districts ORDER BY name").fetchall()
    return [dict(r) for r in rows]


def get_apartments_by_district(district_id: int) -> list[dict]:
    with _get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM apartments WHERE district_id = ? ORDER BY created_at", (district_id,)
        ).fetchall()
    result = []
    for row in rows:
        apt = dict(row)
        apt["aliases"] = json.loads(apt["aliases"])
        apt["floor_prices"] = json.loads(apt["floor_prices"]) if apt.get("floor_prices") else None
        apt["photos_file_ids"] = json.loads(apt["photos_file_ids"]) if apt.get("photos_file_ids") else []
        result.append(apt)
    return result


# ---------------------------------------------------------------------------
# Subscriptions
# ---------------------------------------------------------------------------

def save_subscription(user_id: int, plan: str, expires_at: str, charge_id: str) -> None:
    with _get_conn() as conn:
        conn.execute("""
            INSERT OR REPLACE INTO subscriptions (user_id, plan, expires_at, charge_id)
            VALUES (?, ?, ?, ?)
        """, (user_id, plan, expires_at, charge_id))
        conn.commit()


def get_active_subscription(user_id: int) -> dict | None:
    with _get_conn() as conn:
        row = conn.execute("""
            SELECT * FROM subscriptions
            WHERE user_id = ? AND expires_at > CURRENT_TIMESTAMP
        """, (user_id,)).fetchone()
    return dict(row) if row else None

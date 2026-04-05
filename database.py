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
            CREATE TABLE IF NOT EXISTS subscription_plans (
                plan_key   TEXT PRIMARY KEY,
                price_rub  INTEGER NOT NULL,
                price_stars INTEGER NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS districts (
                id   INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS commercial (
                apt_id                    TEXT PRIMARY KEY,
                price_per_sqm             INTEGER NOT NULL,
                floor_prices              TEXT DEFAULT NULL,
                installment_text          TEXT DEFAULT '',
                installment_price_per_sqm INTEGER DEFAULT NULL,
                layouts_url               TEXT DEFAULT '',
                photos_url                TEXT DEFAULT '',
                photos_file_ids           TEXT DEFAULT '[]',
                available                 INTEGER NOT NULL DEFAULT 1
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS investor_units (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                apt_id         TEXT NOT NULL,
                unit_type      TEXT NOT NULL DEFAULT 'apt',
                floor          INTEGER NOT NULL,
                layout_name    TEXT NOT NULL,
                investor_phone TEXT NOT NULL,
                available      INTEGER NOT NULL DEFAULT 1
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS view_analytics (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id   INTEGER NOT NULL,
                apt_id    TEXT NOT NULL,
                detail    TEXT DEFAULT '',
                viewed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS bot_settings (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)
        # Migrate existing DB: add columns if missing
        for alter in [
            "ALTER TABLE apartments ADD COLUMN floor_prices TEXT DEFAULT NULL",
            "ALTER TABLE apartments ADD COLUMN district_id INTEGER DEFAULT NULL",
            "ALTER TABLE apartments ADD COLUMN photos_file_ids TEXT DEFAULT '[]'",
            "ALTER TABLE apartments ADD COLUMN installment_price_per_sqm INTEGER DEFAULT NULL",
            "ALTER TABLE apartments ADD COLUMN available INTEGER NOT NULL DEFAULT 1",
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


def delete_subscription(user_id: int) -> None:
    with _get_conn() as conn:
        conn.execute("DELETE FROM subscriptions WHERE user_id = ?", (user_id,))
        conn.commit()


def extend_subscription(user_id: int, days: int, plan: str = "ручная") -> None:
    """Add days to existing subscription or create a new one from now."""
    from datetime import datetime, timedelta
    with _get_conn() as conn:
        row = conn.execute(
            "SELECT expires_at FROM subscriptions WHERE user_id = ?", (user_id,)
        ).fetchone()
        if row:
            try:
                base = datetime.fromisoformat(row["expires_at"])
                # If already expired, start from now
                if base < datetime.utcnow():
                    base = datetime.utcnow()
            except Exception:
                base = datetime.utcnow()
        else:
            base = datetime.utcnow()
        new_expiry = (base + timedelta(days=days)).isoformat()
        conn.execute("""
            INSERT OR REPLACE INTO subscriptions (user_id, plan, expires_at, charge_id)
            VALUES (?, ?, ?, NULL)
        """, (user_id, plan, new_expiry))
        conn.commit()


def get_all_active_subscriptions() -> list[dict]:
    with _get_conn() as conn:
        rows = conn.execute("""
            SELECT * FROM subscriptions
            WHERE expires_at > CURRENT_TIMESTAMP
            ORDER BY expires_at
        """).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Subscription plan prices
# ---------------------------------------------------------------------------

_DEFAULT_PRICES = {
    "1m": (299,  299),
    "3m": (807,  807),
    "6m": (1525, 1525),
}


def get_plan_prices() -> dict[str, tuple[int, int]]:
    """Returns {plan_key: (price_rub, price_stars)}."""
    with _get_conn() as conn:
        rows = conn.execute("SELECT plan_key, price_rub, price_stars FROM subscription_plans").fetchall()
    stored = {r["plan_key"]: (r["price_rub"], r["price_stars"]) for r in rows}
    return {k: stored.get(k, v) for k, v in _DEFAULT_PRICES.items()}


def update_plan_prices(plan_key: str, price_rub: int, price_stars: int) -> None:
    with _get_conn() as conn:
        conn.execute("""
            INSERT OR REPLACE INTO subscription_plans (plan_key, price_rub, price_stars)
            VALUES (?, ?, ?)
        """, (plan_key, price_rub, price_stars))
        conn.commit()


# ---------------------------------------------------------------------------
# Commercial spaces
# ---------------------------------------------------------------------------

def save_commercial(apt_id: str, data: dict) -> None:
    floor_prices = data.get("floor_prices")
    with _get_conn() as conn:
        conn.execute("""
            INSERT OR REPLACE INTO commercial
            (apt_id, price_per_sqm, floor_prices, installment_text,
             installment_price_per_sqm, layouts_url, photos_url, photos_file_ids, available)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            apt_id,
            data["price_per_sqm"],
            json.dumps(floor_prices, ensure_ascii=False) if floor_prices else None,
            data.get("installment_text", ""),
            data.get("installment_price_per_sqm"),
            data.get("layouts_url", ""),
            data.get("photos_url", ""),
            json.dumps(data.get("photos_file_ids", []), ensure_ascii=False),
            data.get("available", 1),
        ))
        conn.commit()


def get_commercial(apt_id: str) -> dict | None:
    with _get_conn() as conn:
        row = conn.execute("SELECT * FROM commercial WHERE apt_id = ?", (apt_id,)).fetchone()
    if not row:
        return None
    d = dict(row)
    d["floor_prices"] = json.loads(d["floor_prices"]) if d.get("floor_prices") else None
    d["photos_file_ids"] = json.loads(d["photos_file_ids"]) if d.get("photos_file_ids") else []
    return d


def delete_commercial(apt_id: str) -> None:
    with _get_conn() as conn:
        conn.execute("DELETE FROM commercial WHERE apt_id = ?", (apt_id,))
        conn.commit()


def set_commercial_availability(apt_id: str, available: int) -> None:
    with _get_conn() as conn:
        conn.execute("UPDATE commercial SET available = ? WHERE apt_id = ?", (available, apt_id))
        conn.commit()


def set_apartment_availability(apt_id: str, available: int) -> None:
    with _get_conn() as conn:
        conn.execute("UPDATE apartments SET available = ? WHERE id = ?", (available, apt_id))
        conn.commit()


# ---------------------------------------------------------------------------
# Investor units
# ---------------------------------------------------------------------------

def add_investor_unit(apt_id: str, unit_type: str, floor: int,
                      layout_name: str, investor_phone: str) -> int:
    with _get_conn() as conn:
        cur = conn.execute("""
            INSERT INTO investor_units (apt_id, unit_type, floor, layout_name, investor_phone)
            VALUES (?, ?, ?, ?, ?)
        """, (apt_id, unit_type, floor, layout_name, investor_phone))
        conn.commit()
        return cur.lastrowid


def get_investor_units(apt_id: str) -> list[dict]:
    with _get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM investor_units WHERE apt_id = ? ORDER BY floor, unit_type, layout_name",
            (apt_id,)
        ).fetchall()
    return [dict(r) for r in rows]


def get_investor_unit(unit_id: int) -> dict | None:
    with _get_conn() as conn:
        row = conn.execute("SELECT * FROM investor_units WHERE id = ?", (unit_id,)).fetchone()
    return dict(row) if row else None


def delete_investor_unit(unit_id: int) -> None:
    with _get_conn() as conn:
        conn.execute("DELETE FROM investor_units WHERE id = ?", (unit_id,))
        conn.commit()


def set_investor_unit_availability(unit_id: int, available: int) -> None:
    with _get_conn() as conn:
        conn.execute("UPDATE investor_units SET available = ? WHERE id = ?", (available, unit_id))
        conn.commit()


# ---------------------------------------------------------------------------
# View analytics
# ---------------------------------------------------------------------------

def log_view(user_id: int, apt_id: str, detail: str = "") -> None:
    with _get_conn() as conn:
        conn.execute(
            "INSERT INTO view_analytics (user_id, apt_id, detail) VALUES (?, ?, ?)",
            (user_id, apt_id, detail),
        )
        conn.commit()


def get_top_apts_today(n: int = 5) -> list[dict]:
    with _get_conn() as conn:
        rows = conn.execute("""
            SELECT apt_id, COUNT(*) as cnt
            FROM view_analytics
            WHERE date(viewed_at) = date('now')
            GROUP BY apt_id
            ORDER BY cnt DESC
            LIMIT ?
        """, (n,)).fetchall()
    return [dict(r) for r in rows]


def get_layout_views_today(apt_id: str) -> list[dict]:
    with _get_conn() as conn:
        rows = conn.execute("""
            SELECT detail, COUNT(*) as cnt
            FROM view_analytics
            WHERE apt_id = ? AND detail != ''
              AND date(viewed_at) = date('now')
            GROUP BY detail
            ORDER BY cnt DESC
        """, (apt_id,)).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Bot settings
# ---------------------------------------------------------------------------

def get_bot_setting(key: str, default: str = "") -> str:
    with _get_conn() as conn:
        row = conn.execute("SELECT value FROM bot_settings WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else default


def set_bot_setting(key: str, value: str) -> None:
    with _get_conn() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO bot_settings (key, value) VALUES (?, ?)",
            (key, value),
        )
        conn.commit()

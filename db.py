import os
import psycopg2
from psycopg2.extras import RealDictCursor


_LAST_DB_ERROR = ""


def _set_last_db_error(message: str):
    global _LAST_DB_ERROR
    _LAST_DB_ERROR = message


def get_last_db_error() -> str:
    return _LAST_DB_ERROR


def _ensure_app_settings_table(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS app_settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)


def get_conn():
    url = os.getenv('DATABASE_URL', '')
    if url.startswith('postgres://'):
        url = url.replace('postgres://', 'postgresql://', 1)
    return psycopg2.connect(url)


def init_db():
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS order_statuses (
                        key TEXT PRIMARY KEY,
                        status TEXT NOT NULL,
                        updated_at TIMESTAMPTZ DEFAULT NOW()
                    )
                """)
                _ensure_app_settings_table(cur)
            conn.commit()
        _set_last_db_error("")
        print("DB initialized.")
    except Exception as e:
        _set_last_db_error(str(e))
        print(f"DB init error: {e}")


def load_order_statuses() -> dict:
    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT key, status FROM order_statuses")
                _set_last_db_error("")
                return {row['key']: row['status'] for row in cur.fetchall()}
    except Exception as e:
        _set_last_db_error(str(e))
        print(f"DB load error: {e}")
        return {}


def upsert_order_status(key: str, status: str):
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO order_statuses (key, status)
                    VALUES (%s, %s)
                    ON CONFLICT (key) DO UPDATE
                        SET status = EXCLUDED.status,
                            updated_at = NOW()
                """, (key, status))
            conn.commit()
        _set_last_db_error("")
    except Exception as e:
        _set_last_db_error(str(e))
        print(f"DB upsert error: {e}")


def get_app_setting(key: str, default: str = "") -> str:
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                _ensure_app_settings_table(cur)
                cur.execute("SELECT value FROM app_settings WHERE key = %s", (key,))
                row = cur.fetchone()
                _set_last_db_error("")
                return row[0] if row and row[0] is not None else default
    except Exception as e:
        _set_last_db_error(str(e))
        print(f"DB get_app_setting error: {e}")
        return default


def set_app_setting(key: str, value: str):
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                _ensure_app_settings_table(cur)
                cur.execute("""
                    INSERT INTO app_settings (key, value)
                    VALUES (%s, %s)
                    ON CONFLICT (key) DO UPDATE
                        SET value = EXCLUDED.value,
                            updated_at = NOW()
                """, (key, value))
            conn.commit()
        _set_last_db_error("")
        return True
    except Exception as e:
        _set_last_db_error(str(e))
        print(f"DB set_app_setting error: {e}")
        return False

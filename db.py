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


def _ensure_product_costs_table(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS product_costs (
            source TEXT NOT NULL,
            variant_key TEXT NOT NULL,
            source_product_id TEXT NOT NULL DEFAULT '',
            source_variant_id TEXT NOT NULL DEFAULT '',
            sku TEXT NOT NULL DEFAULT '',
            primary_name TEXT NOT NULL,
            secondary_name TEXT NOT NULL DEFAULT '',
            product_cost NUMERIC(12, 2) NOT NULL DEFAULT 0,
            updated_at TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (source, variant_key)
        )
    """)


def get_conn():
    url = (
        os.getenv('DATABASE_URL', '')
        or os.getenv('POSTGRES_URL', '')
        or os.getenv('POSTGRESQL_URL', '')
    )
    if url.startswith('postgres://'):
        url = url.replace('postgres://', 'postgresql://', 1)
    if url:
        return psycopg2.connect(url)

    host = os.getenv('PGHOST') or os.getenv('POSTGRES_HOST')
    port = os.getenv('PGPORT') or os.getenv('POSTGRES_PORT') or '5432'
    user = os.getenv('PGUSER') or os.getenv('POSTGRES_USER')
    password = os.getenv('PGPASSWORD') or os.getenv('POSTGRES_PASSWORD')
    database = os.getenv('PGDATABASE') or os.getenv('POSTGRES_DB') or os.getenv('POSTGRES_DATABASE')

    if host and user and database:
        return psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            dbname=database,
        )

    raise RuntimeError(
        "Database configuration missing. Set DATABASE_URL (preferred) or PGHOST/PGUSER/PGPASSWORD/PGDATABASE."
    )


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
                _ensure_product_costs_table(cur)
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


def delete_order_status(key: str):
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM order_statuses WHERE key = %s", (key,))
            conn.commit()
        _set_last_db_error("")
        return True
    except Exception as e:
        _set_last_db_error(str(e))
        print(f"DB delete error: {e}")
        return False


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


def list_product_costs(source: str = "") -> list[dict]:
    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                _ensure_product_costs_table(cur)
                if source:
                    cur.execute("""
                        SELECT source, variant_key, source_product_id, source_variant_id, sku,
                               primary_name, secondary_name, product_cost, updated_at
                        FROM product_costs
                        WHERE source = %s
                        ORDER BY primary_name ASC, sku ASC, variant_key ASC
                    """, (source,))
                else:
                    cur.execute("""
                        SELECT source, variant_key, source_product_id, source_variant_id, sku,
                               primary_name, secondary_name, product_cost, updated_at
                        FROM product_costs
                        ORDER BY source ASC, primary_name ASC, sku ASC, variant_key ASC
                    """)
                rows = cur.fetchall()
                _set_last_db_error("")
                return [dict(row) for row in rows]
    except Exception as e:
        _set_last_db_error(str(e))
        print(f"DB list_product_costs error: {e}")
        return []


def get_product_cost_lookup(source: str = "") -> dict:
    rows = list_product_costs(source)
    lookup = {}
    for row in rows:
        key = row['variant_key'] if source else f"{row['source']}::{row['variant_key']}"
        lookup[key] = row
    return lookup


def upsert_product_cost(
    source: str,
    variant_key: str,
    primary_name: str,
    secondary_name: str = "",
    sku: str = "",
    product_cost=0,
    source_product_id: str = "",
    source_variant_id: str = "",
):
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                _ensure_product_costs_table(cur)
                cur.execute("""
                    INSERT INTO product_costs (
                        source,
                        variant_key,
                        source_product_id,
                        source_variant_id,
                        sku,
                        primary_name,
                        secondary_name,
                        product_cost
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (source, variant_key) DO UPDATE
                    SET source_product_id = EXCLUDED.source_product_id,
                        source_variant_id = EXCLUDED.source_variant_id,
                        sku = EXCLUDED.sku,
                        primary_name = EXCLUDED.primary_name,
                        secondary_name = EXCLUDED.secondary_name,
                        product_cost = EXCLUDED.product_cost,
                        updated_at = NOW()
                """, (
                    source,
                    variant_key,
                    source_product_id or "",
                    source_variant_id or "",
                    sku or "",
                    primary_name,
                    secondary_name or "",
                    product_cost,
                ))
            conn.commit()
        _set_last_db_error("")
        return True
    except Exception as e:
        _set_last_db_error(str(e))
        print(f"DB upsert_product_cost error: {e}")
        return False

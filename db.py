import os
import json
from datetime import date, datetime
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
            product_price NUMERIC(12, 2) NOT NULL DEFAULT 0,
            beans_kg NUMERIC(12, 2) NOT NULL DEFAULT 0,
            fabric_cost NUMERIC(12, 2) NOT NULL DEFAULT 0,
            yard_qty NUMERIC(12, 2) NOT NULL DEFAULT 0,
            fusium_cost NUMERIC(12, 2) NOT NULL DEFAULT 0,
            making_cost NUMERIC(12, 2) NOT NULL DEFAULT 0,
            overhead_cost NUMERIC(12, 2) NOT NULL DEFAULT 0,
            delivery_cost NUMERIC(12, 2) NOT NULL DEFAULT 0,
            return_cost NUMERIC(12, 2) NOT NULL DEFAULT 0,
            ads_cost NUMERIC(12, 2) NOT NULL DEFAULT 0,
            product_cost NUMERIC(12, 2) NOT NULL DEFAULT 0,
            updated_at TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (source, variant_key)
        )
    """)
    for column in (
        "product_price",
        "beans_kg",
        "fabric_cost",
        "yard_qty",
        "fusium_cost",
        "making_cost",
        "overhead_cost",
        "delivery_cost",
        "return_cost",
        "ads_cost",
    ):
        cur.execute(f"""
            ALTER TABLE product_costs
            ADD COLUMN IF NOT EXISTS {column} NUMERIC(12, 2) NOT NULL DEFAULT 0
        """)


def _ensure_exhibition_tables(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS exhibitions (
            id BIGSERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            location TEXT NOT NULL DEFAULT '',
            starts_on DATE,
            ends_on DATE,
            notes TEXT NOT NULL DEFAULT '',
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS exhibition_orders (
            id BIGSERIAL PRIMARY KEY,
            exhibition_id BIGINT REFERENCES exhibitions(id) ON DELETE SET NULL,
            order_number TEXT NOT NULL UNIQUE,
            customer_name TEXT NOT NULL DEFAULT '',
            customer_phone TEXT NOT NULL DEFAULT '',
            product_name TEXT NOT NULL,
            shopify_product_id TEXT NOT NULL DEFAULT '',
            shopify_variant_id TEXT NOT NULL DEFAULT '',
            sku TEXT NOT NULL DEFAULT '',
            items JSONB NOT NULL DEFAULT '[]'::jsonb,
            quantity INTEGER NOT NULL DEFAULT 1,
            unit_price NUMERIC(12, 2) NOT NULL DEFAULT 0,
            discount NUMERIC(12, 2) NOT NULL DEFAULT 0,
            delivery_method TEXT NOT NULL DEFAULT 'Pickup from Expo',
            delivery_address TEXT NOT NULL DEFAULT '',
            delivery_charges NUMERIC(12, 2) NOT NULL DEFAULT 0,
            payment_method TEXT NOT NULL DEFAULT 'Cash',
            payment_split TEXT NOT NULL DEFAULT '100% Paid',
            custom_paid_amount NUMERIC(12, 2) NOT NULL DEFAULT 0,
            total_amount NUMERIC(12, 2) NOT NULL DEFAULT 0,
            paid_amount NUMERIC(12, 2) NOT NULL DEFAULT 0,
            product_cost_override NUMERIC(12, 2),
            shopify_push_status TEXT NOT NULL DEFAULT 'unpushed',
            shopify_order_id TEXT NOT NULL DEFAULT '',
            shopify_order_name TEXT NOT NULL DEFAULT '',
            shopify_draft_order_id TEXT NOT NULL DEFAULT '',
            shopify_push_error TEXT NOT NULL DEFAULT '',
            shopify_pushed_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    cur.execute("""
        ALTER TABLE exhibition_orders
        ADD COLUMN IF NOT EXISTS delivery_address TEXT NOT NULL DEFAULT ''
    """)
    cur.execute("""
        ALTER TABLE exhibition_orders
        ADD COLUMN IF NOT EXISTS items JSONB NOT NULL DEFAULT '[]'::jsonb
    """)
    cur.execute("""
        ALTER TABLE exhibition_orders
        ADD COLUMN IF NOT EXISTS product_cost_override NUMERIC(12, 2)
    """)
    for column, definition in (
        ("shopify_push_status", "TEXT NOT NULL DEFAULT 'unpushed'"),
        ("shopify_order_id", "TEXT NOT NULL DEFAULT ''"),
        ("shopify_order_name", "TEXT NOT NULL DEFAULT ''"),
        ("shopify_draft_order_id", "TEXT NOT NULL DEFAULT ''"),
        ("shopify_push_error", "TEXT NOT NULL DEFAULT ''"),
        ("shopify_pushed_at", "TIMESTAMPTZ"),
    ):
        cur.execute(f"""
            ALTER TABLE exhibition_orders
            ADD COLUMN IF NOT EXISTS {column} {definition}
        """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_exhibition_orders_exhibition
        ON exhibition_orders (exhibition_id, created_at DESC)
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS exhibition_expenses (
            id BIGSERIAL PRIMARY KEY,
            exhibition_id BIGINT REFERENCES exhibitions(id) ON DELETE CASCADE,
            label TEXT NOT NULL,
            amount NUMERIC(12, 2) NOT NULL DEFAULT 0,
            payment_method TEXT NOT NULL DEFAULT 'Cash',
            expense_date DATE DEFAULT CURRENT_DATE,
            notes TEXT NOT NULL DEFAULT '',
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    cur.execute("""
        ALTER TABLE exhibition_expenses
        ADD COLUMN IF NOT EXISTS payment_method TEXT NOT NULL DEFAULT 'Cash'
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_exhibition_expenses_exhibition
        ON exhibition_expenses (exhibition_id, expense_date DESC, id DESC)
    """)


def _ensure_attendance_tables(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS attendance_employees (
            id BIGSERIAL PRIMARY KEY,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            full_name TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'employee',
            active BOOLEAN NOT NULL DEFAULT TRUE,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS attendance_locations (
            id BIGSERIAL PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            plus_code TEXT NOT NULL DEFAULT '',
            latitude NUMERIC(10, 7) NOT NULL,
            longitude NUMERIC(10, 7) NOT NULL,
            radius_meters INTEGER NOT NULL DEFAULT 150,
            active BOOLEAN NOT NULL DEFAULT TRUE,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    cur.execute("""
        INSERT INTO attendance_locations (name, plus_code, latitude, longitude, radius_meters)
        VALUES ('Tick Bags Office', '97XX+3W Lahore, Pakistan', 31.3976875, 74.2998125, 150)
        ON CONFLICT (name) DO NOTHING
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS attendance_records (
            id BIGSERIAL PRIMARY KEY,
            employee_id BIGINT NOT NULL REFERENCES attendance_employees(id) ON DELETE CASCADE,
            location_id BIGINT REFERENCES attendance_locations(id) ON DELETE SET NULL,
            work_date DATE NOT NULL DEFAULT CURRENT_DATE,
            check_in_at TIMESTAMPTZ,
            check_in_latitude NUMERIC(10, 7),
            check_in_longitude NUMERIC(10, 7),
            check_in_accuracy_meters NUMERIC(10, 2),
            check_in_distance_meters NUMERIC(10, 2),
            check_in_photo TEXT NOT NULL DEFAULT '',
            check_in_user_agent TEXT NOT NULL DEFAULT '',
            check_in_ip TEXT NOT NULL DEFAULT '',
            check_out_at TIMESTAMPTZ,
            check_out_latitude NUMERIC(10, 7),
            check_out_longitude NUMERIC(10, 7),
            check_out_accuracy_meters NUMERIC(10, 2),
            check_out_distance_meters NUMERIC(10, 2),
            check_out_photo TEXT NOT NULL DEFAULT '',
            check_out_user_agent TEXT NOT NULL DEFAULT '',
            check_out_ip TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'open',
            notes TEXT NOT NULL DEFAULT '',
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_attendance_employee_work_date
        ON attendance_records (employee_id, work_date)
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_attendance_records_date
        ON attendance_records (work_date DESC, employee_id)
    """)


def _ensure_tickbot_auto_reply_jobs_table(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS tickbot_auto_reply_jobs (
            id BIGSERIAL PRIMARY KEY,
            channel TEXT NOT NULL,
            contact_key TEXT NOT NULL,
            body TEXT NOT NULL DEFAULT '',
            customer_name TEXT NOT NULL DEFAULT '',
            display_handle TEXT NOT NULL DEFAULT '',
            contact_phone TEXT NOT NULL DEFAULT '',
            provider_message_id TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'pending',
            attempts INTEGER NOT NULL DEFAULT 0,
            last_error TEXT NOT NULL DEFAULT '',
            available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            locked_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_tickbot_auto_reply_jobs_provider
        ON tickbot_auto_reply_jobs (provider_message_id)
        WHERE provider_message_id <> ''
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_tickbot_auto_reply_jobs_pending
        ON tickbot_auto_reply_jobs (status, available_at, id)
    """)


def _ensure_whatsapp_tables(cur):
    cur.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS whatsapp_conversations (
            phone TEXT PRIMARY KEY,
            channel TEXT NOT NULL DEFAULT 'whatsapp',
            customer_name TEXT NOT NULL DEFAULT '',
            display_handle TEXT NOT NULL DEFAULT '',
            contact_phone TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'new',
            last_message TEXT NOT NULL DEFAULT '',
            last_direction TEXT NOT NULL DEFAULT '',
            last_message_at TIMESTAMPTZ DEFAULT NOW(),
            unread_count INTEGER NOT NULL DEFAULT 0,
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS whatsapp_messages (
            id BIGSERIAL PRIMARY KEY,
            phone TEXT NOT NULL,
            channel TEXT NOT NULL DEFAULT 'whatsapp',
            direction TEXT NOT NULL,
            body TEXT NOT NULL,
            provider_message_id TEXT NOT NULL DEFAULT '',
            metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_whatsapp_messages_phone_created
        ON whatsapp_messages (phone, created_at)
    """)
    cur.execute("""
        ALTER TABLE whatsapp_conversations
        ADD COLUMN IF NOT EXISTS channel TEXT NOT NULL DEFAULT 'whatsapp'
    """)
    cur.execute("""
        ALTER TABLE whatsapp_conversations
        ADD COLUMN IF NOT EXISTS display_handle TEXT NOT NULL DEFAULT ''
    """)
    cur.execute("""
        ALTER TABLE whatsapp_conversations
        ADD COLUMN IF NOT EXISTS contact_phone TEXT NOT NULL DEFAULT ''
    """)
    cur.execute("""
        ALTER TABLE whatsapp_messages
        ADD COLUMN IF NOT EXISTS channel TEXT NOT NULL DEFAULT 'whatsapp'
    """)
    cur.execute("""
        ALTER TABLE whatsapp_conversations
        ADD COLUMN IF NOT EXISTS chat_id UUID
    """)
    cur.execute("""
        ALTER TABLE whatsapp_conversations
        ADD COLUMN IF NOT EXISTS public_chat_id TEXT
    """)
    cur.execute("""
        ALTER TABLE whatsapp_conversations
        ADD COLUMN IF NOT EXISTS ai_mode TEXT NOT NULL DEFAULT 'human'
    """)
    cur.execute("""
        ALTER TABLE whatsapp_conversations
        ADD COLUMN IF NOT EXISTS ai_enabled BOOLEAN NOT NULL DEFAULT FALSE
    """)
    cur.execute("""
        ALTER TABLE whatsapp_conversations
        ADD COLUMN IF NOT EXISTS ai_paused_reason TEXT NOT NULL DEFAULT ''
    """)
    cur.execute("""
        ALTER TABLE whatsapp_conversations
        ADD COLUMN IF NOT EXISTS ai_last_decision JSONB NOT NULL DEFAULT '{}'::jsonb
    """)
    cur.execute("""
        ALTER TABLE whatsapp_conversations
        ADD COLUMN IF NOT EXISTS ai_summary TEXT NOT NULL DEFAULT ''
    """)
    cur.execute("""
        ALTER TABLE whatsapp_conversations
        ADD COLUMN IF NOT EXISTS labels JSONB NOT NULL DEFAULT '[]'::jsonb
    """)
    cur.execute("""
        ALTER TABLE whatsapp_conversations
        ADD COLUMN IF NOT EXISTS order_state TEXT NOT NULL DEFAULT 'no_order'
    """)
    cur.execute("""
        ALTER TABLE whatsapp_conversations
        ADD COLUMN IF NOT EXISTS order_candidate JSONB NOT NULL DEFAULT '{}'::jsonb
    """)
    cur.execute("""
        ALTER TABLE whatsapp_conversations
        ADD COLUMN IF NOT EXISTS assigned_to TEXT NOT NULL DEFAULT ''
    """)
    cur.execute("""
        ALTER TABLE whatsapp_conversations
        ADD COLUMN IF NOT EXISTS is_pinned BOOLEAN NOT NULL DEFAULT FALSE
    """)
    cur.execute("""
        ALTER TABLE whatsapp_conversations
        ADD COLUMN IF NOT EXISTS is_internal BOOLEAN NOT NULL DEFAULT FALSE
    """)
    cur.execute("""
        ALTER TABLE whatsapp_conversations
        ADD COLUMN IF NOT EXISTS last_customer_message_at TIMESTAMPTZ
    """)
    cur.execute("""
        ALTER TABLE whatsapp_conversations
        ADD COLUMN IF NOT EXISTS service_window_expires_at TIMESTAMPTZ
    """)
    cur.execute("""
        ALTER TABLE whatsapp_conversations
        ADD COLUMN IF NOT EXISTS manual_lock BOOLEAN NOT NULL DEFAULT FALSE
    """)
    cur.execute("""
        ALTER TABLE whatsapp_conversations
        ADD COLUMN IF NOT EXISTS updated_by TEXT NOT NULL DEFAULT ''
    """)
    cur.execute("""
        ALTER TABLE whatsapp_messages
        ADD COLUMN IF NOT EXISTS chat_id UUID
    """)
    cur.execute("""
        ALTER TABLE whatsapp_messages
        ADD COLUMN IF NOT EXISTS sender_type TEXT NOT NULL DEFAULT ''
    """)
    cur.execute("""
        ALTER TABLE whatsapp_messages
        ADD COLUMN IF NOT EXISTS message_type TEXT NOT NULL DEFAULT 'text'
    """)
    cur.execute("""
        ALTER TABLE whatsapp_messages
        ADD COLUMN IF NOT EXISTS attachments JSONB NOT NULL DEFAULT '[]'::jsonb
    """)
    cur.execute("""
        ALTER TABLE whatsapp_messages
        ADD COLUMN IF NOT EXISTS ai_metadata JSONB NOT NULL DEFAULT '{}'::jsonb
    """)
    cur.execute("""
        ALTER TABLE whatsapp_messages
        ADD COLUMN IF NOT EXISTS internal_only BOOLEAN NOT NULL DEFAULT FALSE
    """)
    cur.execute("""
        UPDATE whatsapp_conversations
        SET chat_id = gen_random_uuid()
        WHERE chat_id IS NULL
    """)
    cur.execute("""
        UPDATE whatsapp_conversations
        SET last_customer_message_at = last_message_at,
            service_window_expires_at = CASE
                WHEN channel = 'whatsapp' THEN last_message_at + INTERVAL '24 hours'
                ELSE NULL
            END
        WHERE last_direction = 'inbound' AND last_customer_message_at IS NULL
    """)
    cur.execute("""
        WITH numbered AS (
            SELECT phone,
                   CASE
                       WHEN channel = 'facebook' THEN 'TB-FB-'
                       WHEN channel = 'instagram' THEN 'TB-IG-'
                       WHEN channel = 'internal' THEN 'TB-AI-'
                       ELSE 'TB-WA-'
                   END || LPAD(ROW_NUMBER() OVER (
                       PARTITION BY CASE
                           WHEN channel = 'facebook' THEN 'TB-FB-'
                           WHEN channel = 'instagram' THEN 'TB-IG-'
                           WHEN channel = 'internal' THEN 'TB-AI-'
                           ELSE 'TB-WA-'
                       END
                       ORDER BY COALESCE(last_message_at, updated_at, NOW()), phone
                   )::text, 6, '0') AS generated_id
            FROM whatsapp_conversations
            WHERE public_chat_id IS NULL OR public_chat_id = ''
        )
        UPDATE whatsapp_conversations c
        SET public_chat_id = numbered.generated_id
        FROM numbered
        WHERE c.phone = numbered.phone
    """)
    cur.execute("""
        UPDATE whatsapp_messages m
        SET chat_id = c.chat_id,
            sender_type = CASE
                WHEN m.sender_type <> '' THEN m.sender_type
                WHEN m.direction = 'inbound' THEN 'customer'
                ELSE 'human'
            END
        FROM whatsapp_conversations c
        WHERE m.phone = c.phone
          AND (m.chat_id IS NULL OR m.sender_type = '')
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_whatsapp_conversations_channel_updated
        ON whatsapp_conversations (channel, last_message_at DESC, updated_at DESC)
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_whatsapp_conversations_chat_id ON whatsapp_conversations (chat_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_whatsapp_conversations_public_chat_id ON whatsapp_conversations (public_chat_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_whatsapp_conversations_ai_mode ON whatsapp_conversations (ai_mode)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_whatsapp_conversations_order_state ON whatsapp_conversations (order_state)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_whatsapp_conversations_last_message_at ON whatsapp_conversations (last_message_at DESC)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_whatsapp_messages_chat_id_created ON whatsapp_messages (chat_id, created_at)")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS whatsapp_rules (
            id BIGSERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            keywords TEXT NOT NULL DEFAULT '',
            response TEXT NOT NULL,
            enabled BOOLEAN NOT NULL DEFAULT TRUE,
            hold_for_review BOOLEAN NOT NULL DEFAULT FALSE,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS whatsapp_templates (
            id BIGSERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            body TEXT NOT NULL,
            category TEXT NOT NULL DEFAULT 'support',
            approved BOOLEAN NOT NULL DEFAULT FALSE,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS whatsapp_blasts (
            id BIGSERIAL PRIMARY KEY,
            template_id BIGINT,
            template_name TEXT NOT NULL DEFAULT '',
            segment TEXT NOT NULL DEFAULT '',
            total_recipients INTEGER NOT NULL DEFAULT 0,
            sent_count INTEGER NOT NULL DEFAULT 0,
            failed_count INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'draft',
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS tickbot_knowledge (
            id BIGSERIAL PRIMARY KEY,
            title TEXT NOT NULL DEFAULT '',
            content TEXT NOT NULL,
            source TEXT NOT NULL DEFAULT 'assistant',
            verified BOOLEAN NOT NULL DEFAULT TRUE,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    _ensure_tickbot_auto_reply_jobs_table(cur)


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
                _ensure_exhibition_tables(cur)
                _ensure_attendance_tables(cur)
                _ensure_tickbot_auto_reply_jobs_table(cur)
                _ensure_whatsapp_tables(cur)
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
                               primary_name, secondary_name, product_price, beans_kg,
                               fabric_cost, yard_qty, fusium_cost, making_cost, overhead_cost,
                               delivery_cost, return_cost, ads_cost, product_cost, updated_at
                        FROM product_costs
                        WHERE source = %s
                        ORDER BY primary_name ASC, sku ASC, variant_key ASC
                    """, (source,))
                else:
                    cur.execute("""
                        SELECT source, variant_key, source_product_id, source_variant_id, sku,
                               primary_name, secondary_name, product_price, beans_kg,
                               fabric_cost, yard_qty, fusium_cost, making_cost, overhead_cost,
                               delivery_cost, return_cost, ads_cost, product_cost, updated_at
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


def _date_or_none(value):
    if not value:
        return None
    if isinstance(value, date):
        return value
    try:
        return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()
    except Exception:
        return None


def list_exhibitions() -> list[dict]:
    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                _ensure_exhibition_tables(cur)
                cur.execute("""
                    SELECT id, name, location, starts_on, ends_on, notes, created_at
                    FROM exhibitions
                    ORDER BY COALESCE(starts_on, created_at::date) DESC, id DESC
                """)
                rows = [dict(row) for row in cur.fetchall()]
                _set_last_db_error("")
                return rows
    except Exception as e:
        _set_last_db_error(str(e))
        print(f"DB list_exhibitions error: {e}")
        return []


def create_exhibition(name: str, location: str = "", starts_on=None, ends_on=None, notes: str = "") -> dict | None:
    name = str(name or "").strip()
    if not name:
        return None
    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                _ensure_exhibition_tables(cur)
                cur.execute("""
                    INSERT INTO exhibitions (name, location, starts_on, ends_on, notes)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING id, name, location, starts_on, ends_on, notes, created_at
                """, (name, location or "", _date_or_none(starts_on), _date_or_none(ends_on), notes or ""))
                row = dict(cur.fetchone())
            conn.commit()
        _set_last_db_error("")
        return row
    except Exception as e:
        _set_last_db_error(str(e))
        print(f"DB create_exhibition error: {e}")
        return None


def list_exhibition_orders(exhibition_id=None, limit: int | None = None) -> list[dict]:
    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                _ensure_exhibition_tables(cur)
                params = []
                where = ""
                if exhibition_id:
                    where = "WHERE o.exhibition_id = %s"
                    params.append(exhibition_id)
                limit_sql = ""
                if limit:
                    limit_sql = "LIMIT %s"
                    params.append(limit)
                cur.execute(f"""
                    SELECT o.*, e.name AS exhibition_name
                    FROM exhibition_orders o
                    LEFT JOIN exhibitions e ON e.id = o.exhibition_id
                    {where}
                    ORDER BY o.created_at DESC, o.id DESC
                    {limit_sql}
                """, tuple(params))
                rows = [dict(row) for row in cur.fetchall()]
                _set_last_db_error("")
                return rows
    except Exception as e:
        _set_last_db_error(str(e))
        print(f"DB list_exhibition_orders error: {e}")
        return []


def get_exhibition_order(order_id) -> dict | None:
    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                _ensure_exhibition_tables(cur)
                cur.execute("""
                    SELECT o.*, e.name AS exhibition_name, e.location AS exhibition_location
                    FROM exhibition_orders o
                    LEFT JOIN exhibitions e ON e.id = o.exhibition_id
                    WHERE o.id = %s
                """, (order_id,))
                row = cur.fetchone()
                _set_last_db_error("")
                return dict(row) if row else None
    except Exception as e:
        _set_last_db_error(str(e))
        print(f"DB get_exhibition_order error: {e}")
        return None


def create_exhibition_order(
    exhibition_id,
    order_number: str,
    customer_name: str,
    customer_phone: str,
    product_name: str,
    shopify_product_id: str = "",
    shopify_variant_id: str = "",
    sku: str = "",
    items=None,
    quantity=1,
    unit_price=0,
    discount=0,
    delivery_method: str = "Pickup from Expo",
    delivery_address: str = "",
    delivery_charges=0,
    payment_method: str = "Cash",
    payment_split: str = "100% Paid",
    custom_paid_amount=0,
    total_amount=0,
    paid_amount=0,
) -> dict | None:
    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                _ensure_exhibition_tables(cur)
                cur.execute("""
                    INSERT INTO exhibition_orders (
                        exhibition_id, order_number, customer_name, customer_phone,
                        product_name, shopify_product_id, shopify_variant_id, sku,
                        items, quantity, unit_price, discount, delivery_method, delivery_address, delivery_charges,
                        payment_method, payment_split, custom_paid_amount, total_amount, paid_amount
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING *
                """, (
                    exhibition_id or None,
                    order_number,
                    customer_name or "",
                    customer_phone or "",
                    product_name,
                    shopify_product_id or "",
                    shopify_variant_id or "",
                    sku or "",
                    json.dumps(items or []),
                    int(quantity or 1),
                    unit_price,
                    discount,
                    delivery_method or "Pickup from Expo",
                    delivery_address or "",
                    delivery_charges,
                    payment_method or "Cash",
                    payment_split or "100% Paid",
                    custom_paid_amount,
                    total_amount,
                    paid_amount,
                ))
                row = dict(cur.fetchone())
            conn.commit()
        _set_last_db_error("")
        return row
    except Exception as e:
        _set_last_db_error(str(e))
        print(f"DB create_exhibition_order error: {e}")
        return None


def update_exhibition_order(
    order_id,
    exhibition_id,
    customer_name: str,
    customer_phone: str,
    product_name: str,
    shopify_product_id: str = "",
    shopify_variant_id: str = "",
    sku: str = "",
    items=None,
    quantity=1,
    unit_price=0,
    discount=0,
    delivery_method: str = "Pickup from Expo",
    delivery_address: str = "",
    delivery_charges=0,
    payment_method: str = "Cash",
    payment_split: str = "100% Paid",
    custom_paid_amount=0,
    total_amount=0,
    paid_amount=0,
) -> dict | None:
    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                _ensure_exhibition_tables(cur)
                cur.execute("""
                    UPDATE exhibition_orders
                    SET exhibition_id = %s,
                        customer_name = %s,
                        customer_phone = %s,
                        product_name = %s,
                        shopify_product_id = %s,
                        shopify_variant_id = %s,
                        sku = %s,
                        items = %s::jsonb,
                        quantity = %s,
                        unit_price = %s,
                        discount = %s,
                        delivery_method = %s,
                        delivery_address = %s,
                        delivery_charges = %s,
                        payment_method = %s,
                        payment_split = %s,
                        custom_paid_amount = %s,
                        total_amount = %s,
                        paid_amount = %s
                    WHERE id = %s
                    RETURNING *
                """, (
                    exhibition_id or None,
                    customer_name or "",
                    customer_phone or "",
                    product_name,
                    shopify_product_id or "",
                    shopify_variant_id or "",
                    sku or "",
                    json.dumps(items or []),
                    int(quantity or 1),
                    unit_price,
                    discount,
                    delivery_method or "Pickup from Expo",
                    delivery_address or "",
                    delivery_charges,
                    payment_method or "Cash",
                    payment_split or "100% Paid",
                    custom_paid_amount,
                    total_amount,
                    paid_amount,
                    order_id,
                ))
                row = cur.fetchone()
            conn.commit()
        _set_last_db_error("")
        return dict(row) if row else None
    except Exception as e:
        _set_last_db_error(str(e))
        print(f"DB update_exhibition_order error: {e}")
        return None


def delete_exhibition_order(order_id) -> bool:
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                _ensure_exhibition_tables(cur)
                cur.execute("DELETE FROM exhibition_orders WHERE id = %s", (order_id,))
                deleted = cur.rowcount > 0
            conn.commit()
        _set_last_db_error("")
        return deleted
    except Exception as e:
        _set_last_db_error(str(e))
        print(f"DB delete_exhibition_order error: {e}")
        return False


def update_exhibition_order_product_cost(order_id, product_cost_override):
    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                _ensure_exhibition_tables(cur)
                cur.execute("""
                    UPDATE exhibition_orders
                    SET product_cost_override = %s
                    WHERE id = %s
                    RETURNING *
                """, (product_cost_override, order_id))
                row = cur.fetchone()
            conn.commit()
        _set_last_db_error("")
        return dict(row) if row else None
    except Exception as e:
        _set_last_db_error(str(e))
        print(f"DB update_exhibition_order_product_cost error: {e}")
        return None


def update_exhibition_order_shopify_push(
    order_id,
    status: str,
    shopify_order_id: str = "",
    shopify_order_name: str = "",
    shopify_draft_order_id: str = "",
    error: str = "",
):
    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                _ensure_exhibition_tables(cur)
                cur.execute("""
                    UPDATE exhibition_orders
                    SET shopify_push_status = %s,
                        shopify_order_id = COALESCE(NULLIF(%s, ''), shopify_order_id),
                        shopify_order_name = COALESCE(NULLIF(%s, ''), shopify_order_name),
                        shopify_draft_order_id = COALESCE(NULLIF(%s, ''), shopify_draft_order_id),
                        shopify_push_error = %s,
                        shopify_pushed_at = CASE WHEN %s = 'pushed' THEN NOW() ELSE shopify_pushed_at END
                    WHERE id = %s
                    RETURNING *
                """, (
                    status or "unpushed",
                    shopify_order_id or "",
                    shopify_order_name or "",
                    shopify_draft_order_id or "",
                    error or "",
                    status or "unpushed",
                    order_id,
                ))
                row = cur.fetchone()
            conn.commit()
        _set_last_db_error("")
        return dict(row) if row else None
    except Exception as e:
        _set_last_db_error(str(e))
        print(f"DB update_exhibition_order_shopify_push error: {e}")
        return None


def list_exhibition_expenses(exhibition_id=None) -> list[dict]:
    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                _ensure_exhibition_tables(cur)
                params = []
                where = ""
                if exhibition_id:
                    where = "WHERE x.exhibition_id = %s"
                    params.append(exhibition_id)
                cur.execute(f"""
                    SELECT x.*, e.name AS exhibition_name
                    FROM exhibition_expenses x
                    LEFT JOIN exhibitions e ON e.id = x.exhibition_id
                    {where}
                    ORDER BY x.expense_date DESC, x.id DESC
                """, tuple(params))
                rows = [dict(row) for row in cur.fetchall()]
                _set_last_db_error("")
                return rows
    except Exception as e:
        _set_last_db_error(str(e))
        print(f"DB list_exhibition_expenses error: {e}")
        return []


def create_exhibition_expense(
    exhibition_id,
    label: str,
    amount=0,
    expense_date=None,
    notes: str = "",
    payment_method: str = "Cash",
) -> dict | None:
    label = str(label or "").strip()
    payment_method = "Bank" if payment_method == "Bank" else "Cash"
    if not exhibition_id or not label:
        return None
    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                _ensure_exhibition_tables(cur)
                cur.execute("""
                    INSERT INTO exhibition_expenses (exhibition_id, label, amount, payment_method, expense_date, notes)
                    VALUES (%s, %s, %s, %s, COALESCE(%s, CURRENT_DATE), %s)
                    RETURNING *
                """, (exhibition_id, label, amount, payment_method, _date_or_none(expense_date), notes or ""))
                row = dict(cur.fetchone())
            conn.commit()
        _set_last_db_error("")
        return row
    except Exception as e:
        _set_last_db_error(str(e))
        print(f"DB create_exhibition_expense error: {e}")
        return None


def list_attendance_employees(include_inactive: bool = False) -> list[dict]:
    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                _ensure_attendance_tables(cur)
                where = "" if include_inactive else "WHERE active = TRUE"
                cur.execute(f"""
                    SELECT id, username, full_name, role, active, created_at, updated_at
                    FROM attendance_employees
                    {where}
                    ORDER BY active DESC, full_name ASC, username ASC
                """)
                rows = [dict(row) for row in cur.fetchall()]
        _set_last_db_error("")
        return rows
    except Exception as e:
        _set_last_db_error(str(e))
        print(f"DB list_attendance_employees error: {e}")
        return []


def get_attendance_employee_by_username(username: str) -> dict | None:
    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                _ensure_attendance_tables(cur)
                cur.execute("""
                    SELECT id, username, password_hash, full_name, role, active, created_at, updated_at
                    FROM attendance_employees
                    WHERE LOWER(username) = LOWER(%s)
                """, (str(username or "").strip(),))
                row = cur.fetchone()
        _set_last_db_error("")
        return dict(row) if row else None
    except Exception as e:
        _set_last_db_error(str(e))
        print(f"DB get_attendance_employee_by_username error: {e}")
        return None


def get_attendance_employee(employee_id) -> dict | None:
    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                _ensure_attendance_tables(cur)
                cur.execute("""
                    SELECT id, username, full_name, role, active, created_at, updated_at
                    FROM attendance_employees
                    WHERE id = %s
                """, (employee_id,))
                row = cur.fetchone()
        _set_last_db_error("")
        return dict(row) if row else None
    except Exception as e:
        _set_last_db_error(str(e))
        print(f"DB get_attendance_employee error: {e}")
        return None


def create_attendance_employee(username: str, password_hash: str, full_name: str, role: str = "employee") -> dict | None:
    username = str(username or "").strip()
    full_name = str(full_name or "").strip()
    role = "admin" if role == "admin" else "employee"
    if not username or not password_hash or not full_name:
        return None
    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                _ensure_attendance_tables(cur)
                cur.execute("""
                    INSERT INTO attendance_employees (username, password_hash, full_name, role)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (username) DO UPDATE
                        SET password_hash = EXCLUDED.password_hash,
                            full_name = EXCLUDED.full_name,
                            role = EXCLUDED.role,
                            active = TRUE,
                            updated_at = NOW()
                    RETURNING id, username, full_name, role, active, created_at, updated_at
                """, (username, password_hash, full_name, role))
                row = dict(cur.fetchone())
            conn.commit()
        _set_last_db_error("")
        return row
    except Exception as e:
        _set_last_db_error(str(e))
        print(f"DB create_attendance_employee error: {e}")
        return None


def list_attendance_locations(active_only: bool = True) -> list[dict]:
    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                _ensure_attendance_tables(cur)
                where = "WHERE active = TRUE" if active_only else ""
                cur.execute(f"""
                    SELECT id, name, plus_code, latitude, longitude, radius_meters, active, created_at
                    FROM attendance_locations
                    {where}
                    ORDER BY active DESC, name ASC
                """)
                rows = [dict(row) for row in cur.fetchall()]
        _set_last_db_error("")
        return rows
    except Exception as e:
        _set_last_db_error(str(e))
        print(f"DB list_attendance_locations error: {e}")
        return []


def get_attendance_record_for_date(employee_id, work_date=None) -> dict | None:
    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                _ensure_attendance_tables(cur)
                cur.execute("""
                    SELECT r.*, e.full_name, e.username, e.role, l.name AS location_name, l.plus_code
                    FROM attendance_records r
                    JOIN attendance_employees e ON e.id = r.employee_id
                    LEFT JOIN attendance_locations l ON l.id = r.location_id
                    WHERE r.employee_id = %s
                      AND r.work_date = COALESCE(%s, CURRENT_DATE)
                """, (employee_id, _date_or_none(work_date)))
                row = cur.fetchone()
        _set_last_db_error("")
        return dict(row) if row else None
    except Exception as e:
        _set_last_db_error(str(e))
        print(f"DB get_attendance_record_for_date error: {e}")
        return None


def get_open_attendance_record_for_employee(employee_id) -> dict | None:
    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                _ensure_attendance_tables(cur)
                cur.execute("""
                    SELECT r.*, e.full_name, e.username, e.role, l.name AS location_name, l.plus_code
                    FROM attendance_records r
                    JOIN attendance_employees e ON e.id = r.employee_id
                    LEFT JOIN attendance_locations l ON l.id = r.location_id
                    WHERE r.employee_id = %s
                      AND r.check_in_at IS NOT NULL
                      AND r.check_out_at IS NULL
                    ORDER BY r.check_in_at DESC, r.id DESC
                    LIMIT 1
                """, (employee_id,))
                row = cur.fetchone()
        _set_last_db_error("")
        return dict(row) if row else None
    except Exception as e:
        _set_last_db_error(str(e))
        print(f"DB get_open_attendance_record_for_employee error: {e}")
        return None


def create_attendance_checkin(
    employee_id,
    location_id,
    latitude,
    longitude,
    accuracy_meters,
    distance_meters,
    photo: str,
    user_agent: str,
    ip_address: str,
    work_date=None,
) -> dict | None:
    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                _ensure_attendance_tables(cur)
                cur.execute("""
                    INSERT INTO attendance_records (
                        employee_id, location_id, work_date, check_in_at,
                        check_in_latitude, check_in_longitude, check_in_accuracy_meters,
                        check_in_distance_meters, check_in_photo, check_in_user_agent, check_in_ip
                    )
                    VALUES (%s, %s, COALESCE(%s, CURRENT_DATE), NOW(), %s, %s, %s, %s, %s, %s, %s)
                    RETURNING *
                """, (
                    employee_id,
                    location_id,
                    _date_or_none(work_date),
                    latitude,
                    longitude,
                    accuracy_meters,
                    distance_meters,
                    photo or "",
                    user_agent or "",
                    ip_address or "",
                ))
                row = dict(cur.fetchone())
            conn.commit()
        _set_last_db_error("")
        return row
    except Exception as e:
        _set_last_db_error(str(e))
        print(f"DB create_attendance_checkin error: {e}")
        return None


def update_attendance_checkout(
    record_id,
    latitude,
    longitude,
    accuracy_meters,
    distance_meters,
    photo: str,
    user_agent: str,
    ip_address: str,
) -> dict | None:
    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                _ensure_attendance_tables(cur)
                cur.execute("""
                    UPDATE attendance_records
                    SET check_out_at = NOW(),
                        check_out_latitude = %s,
                        check_out_longitude = %s,
                        check_out_accuracy_meters = %s,
                        check_out_distance_meters = %s,
                        check_out_photo = %s,
                        check_out_user_agent = %s,
                        check_out_ip = %s,
                        status = 'closed',
                        updated_at = NOW()
                    WHERE id = %s
                      AND check_out_at IS NULL
                    RETURNING *
                """, (
                    latitude,
                    longitude,
                    accuracy_meters,
                    distance_meters,
                    photo or "",
                    user_agent or "",
                    ip_address or "",
                    record_id,
                ))
                row = cur.fetchone()
            conn.commit()
        _set_last_db_error("")
        return dict(row) if row else None
    except Exception as e:
        _set_last_db_error(str(e))
        print(f"DB update_attendance_checkout error: {e}")
        return None


def list_attendance_records(start_date=None, end_date=None, employee_id=None) -> list[dict]:
    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                _ensure_attendance_tables(cur)
                params = []
                clauses = []
                if start_date:
                    clauses.append("r.work_date >= %s")
                    params.append(_date_or_none(start_date))
                if end_date:
                    clauses.append("r.work_date <= %s")
                    params.append(_date_or_none(end_date))
                if employee_id:
                    clauses.append("r.employee_id = %s")
                    params.append(employee_id)
                where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
                cur.execute(f"""
                    SELECT r.id, r.employee_id, r.location_id, r.work_date,
                           r.check_in_at, r.check_in_latitude, r.check_in_longitude,
                           r.check_in_accuracy_meters, r.check_in_distance_meters,
                           r.check_in_photo,
                           r.check_out_at, r.check_out_latitude, r.check_out_longitude,
                           r.check_out_accuracy_meters, r.check_out_distance_meters,
                           r.check_out_photo,
                           r.status, r.notes, r.created_at, r.updated_at,
                           e.full_name, e.username, e.role,
                           l.name AS location_name, l.plus_code
                    FROM attendance_records r
                    JOIN attendance_employees e ON e.id = r.employee_id
                    LEFT JOIN attendance_locations l ON l.id = r.location_id
                    {where}
                    ORDER BY r.work_date DESC, r.check_in_at DESC, r.id DESC
                    LIMIT 5000
                """, tuple(params))
                rows = [dict(row) for row in cur.fetchall()]
        _set_last_db_error("")
        return rows
    except Exception as e:
        _set_last_db_error(str(e))
        print(f"DB list_attendance_records error: {e}")
        return []


def get_attendance_record(record_id) -> dict | None:
    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                _ensure_attendance_tables(cur)
                cur.execute("""
                    SELECT r.*, e.full_name, e.username, e.role, l.name AS location_name, l.plus_code
                    FROM attendance_records r
                    JOIN attendance_employees e ON e.id = r.employee_id
                    LEFT JOIN attendance_locations l ON l.id = r.location_id
                    WHERE r.id = %s
                """, (record_id,))
                row = cur.fetchone()
        _set_last_db_error("")
        return dict(row) if row else None
    except Exception as e:
        _set_last_db_error(str(e))
        print(f"DB get_attendance_record error: {e}")
        return None


def upsert_product_cost(
    source: str,
    variant_key: str,
    primary_name: str,
    secondary_name: str = "",
    sku: str = "",
    product_price=0,
    beans_kg=0,
    fabric_cost=0,
    yard_qty=0,
    fusium_cost=0,
    making_cost=0,
    overhead_cost=0,
    delivery_cost=0,
    return_cost=0,
    ads_cost=0,
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
                        product_price,
                        beans_kg,
                        fabric_cost,
                        yard_qty,
                        fusium_cost,
                        making_cost,
                        overhead_cost,
                        delivery_cost,
                        return_cost,
                        ads_cost,
                        product_cost
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (source, variant_key) DO UPDATE
                    SET source_product_id = EXCLUDED.source_product_id,
                        source_variant_id = EXCLUDED.source_variant_id,
                        sku = EXCLUDED.sku,
                        primary_name = EXCLUDED.primary_name,
                        secondary_name = EXCLUDED.secondary_name,
                        product_price = EXCLUDED.product_price,
                        beans_kg = EXCLUDED.beans_kg,
                        fabric_cost = EXCLUDED.fabric_cost,
                        yard_qty = EXCLUDED.yard_qty,
                        fusium_cost = EXCLUDED.fusium_cost,
                        making_cost = EXCLUDED.making_cost,
                        overhead_cost = EXCLUDED.overhead_cost,
                        delivery_cost = EXCLUDED.delivery_cost,
                        return_cost = EXCLUDED.return_cost,
                        ads_cost = EXCLUDED.ads_cost,
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
                    product_price,
                    beans_kg,
                    fabric_cost,
                    yard_qty,
                    fusium_cost,
                    making_cost,
                    overhead_cost,
                    delivery_cost,
                    return_cost,
                    ads_cost,
                    product_cost,
                ))
            conn.commit()
        _set_last_db_error("")
        return True
    except Exception as e:
        _set_last_db_error(str(e))
        print(f"DB upsert_product_cost error: {e}")
        return False


def normalize_whatsapp_phone(phone: str) -> str:
    digits = ''.join(ch for ch in str(phone or '').strip() if ch.isdigit())
    if not digits:
        return ''
    if digits.startswith('92'):
        return f"+{digits}"
    if digits.startswith('0'):
        return f"+92{digits[1:]}"
    if digits.startswith('3') and len(digits) == 10:
        return f"+92{digits}"
    if str(phone or '').strip().startswith('+'):
        return str(phone or '').strip()
    return f"+{digits}"


def normalize_inbox_contact_key(channel: str, value: str) -> str:
    channel = str(channel or "whatsapp").strip().lower() or "whatsapp"
    raw_value = str(value or "").strip()
    if not raw_value:
        return ""
    if channel == "whatsapp":
        return normalize_whatsapp_phone(raw_value)
    if raw_value.startswith(f"{channel}:"):
        return raw_value
    return f"{channel}:{raw_value}"


def _safe_json(value, fallback):
    if value is None:
        return fallback
    if isinstance(value, (dict, list)):
        return value
    try:
        return __import__('json').loads(value)
    except Exception:
        return fallback


def _chat_id_prefix(channel: str) -> str:
    channel = str(channel or "whatsapp").lower()
    if channel == "facebook":
        return "TB-FB-"
    if channel == "instagram":
        return "TB-IG-"
    if channel == "internal":
        return "TB-AI-"
    return "TB-WA-"


def _next_public_chat_id(cur, channel: str) -> str:
    prefix = _chat_id_prefix(channel)
    cur.execute(
        "SELECT public_chat_id FROM whatsapp_conversations WHERE public_chat_id LIKE %s ORDER BY public_chat_id DESC LIMIT 1",
        (f"{prefix}%",),
    )
    row = cur.fetchone()
    current = 0
    if row:
        public_id = row["public_chat_id"] if isinstance(row, dict) else row[0]
        try:
            current = int(str(public_id).rsplit("-", 1)[-1])
        except Exception:
            current = 0
    return f"{prefix}{current + 1:06d}"


def ensure_tickbot_assistant_chat() -> dict | None:
    phone = "internal:tickbot-assistant"
    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    INSERT INTO whatsapp_conversations (
                        phone, channel, customer_name, display_handle, status, last_message,
                        last_direction, unread_count, chat_id, public_chat_id, ai_mode,
                        labels, is_pinned, is_internal
                    )
                    VALUES (
                        %s, 'internal', 'TickBot Assistant', 'Internal assistant', 'open',
                        'Ask TickBot about setup, unresolved questions, and knowledge gaps.',
                        'system', 0, gen_random_uuid(), %s, 'human',
                        '["assistant"]'::jsonb, TRUE, TRUE
                    )
                    ON CONFLICT (phone) DO UPDATE SET
                        channel = 'internal',
                        customer_name = 'TickBot Assistant',
                        display_handle = 'Internal assistant',
                        is_pinned = TRUE,
                        is_internal = TRUE,
                        public_chat_id = COALESCE(NULLIF(whatsapp_conversations.public_chat_id, ''), EXCLUDED.public_chat_id),
                        updated_at = NOW()
                    RETURNING *
                """, (phone, _next_public_chat_id(cur, "internal")))
                row = dict(cur.fetchone())
            conn.commit()
        _set_last_db_error("")
        return row
    except Exception as e:
        _set_last_db_error(str(e))
        print(f"DB ensure_tickbot_assistant_chat error: {e}")
        return None


def resolve_conversation_key(key: str) -> str:
    conversation = get_conversation_by_any_key(key)
    return conversation.get("phone") if conversation else normalize_inbox_contact_key("whatsapp", key)


def get_conversation_by_any_key(key: str) -> dict | None:
    key = str(key or "").strip()
    if not key:
        return None
    candidates = [key]
    if not key.startswith(("facebook:", "instagram:", "internal:")):
        normalized = normalize_inbox_contact_key("whatsapp", key)
        if normalized and normalized not in candidates:
            candidates.append(normalized)
    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT *
                    FROM whatsapp_conversations
                    WHERE phone = ANY(%s)
                       OR public_chat_id = %s
                       OR chat_id::text = %s
                    LIMIT 1
                """, (candidates, key, key))
                row = cur.fetchone()
                if row:
                    _set_last_db_error("")
                    return dict(row)
    except Exception as e:
        _set_last_db_error(str(e))
        print(f"DB get_conversation_by_any_key error: {e}")
    return None


def list_inbox_conversations() -> list[dict]:
    ensure_tickbot_assistant_chat()
    return list_whatsapp_conversations()


def list_inbox_messages(key: str, limit: int = 100) -> list[dict]:
    return list_whatsapp_messages(key, limit=limit)


def save_whatsapp_message(
    phone: str,
    direction: str,
    body: str,
    customer_name: str = "",
    provider_message_id: str = "",
    metadata: dict | None = None,
    channel: str = "whatsapp",
    display_handle: str = "",
    contact_phone: str = "",
    sender_type: str = "",
    message_type: str = "text",
    attachments: list | None = None,
    ai_metadata: dict | None = None,
    internal_only: bool = False,
):
    channel = str(channel or "whatsapp").strip().lower() or "whatsapp"
    phone = normalize_inbox_contact_key(channel, phone)
    direction = (direction or '').strip().lower()
    body = str(body or '').strip()
    if not phone or direction not in {'inbound', 'outbound'} or not body:
        _set_last_db_error("Phone, direction, and body are required to save a message.")
        return None
    contact_phone = normalize_whatsapp_phone(contact_phone) if contact_phone else ""
    display_handle = str(display_handle or "").strip()
    sender_type = (sender_type or ("customer" if direction == "inbound" else "human")).strip().lower()
    if sender_type not in {"customer", "human", "ai", "system", "assistant"}:
        sender_type = "customer" if direction == "inbound" else "human"
    message_type = (message_type or "text").strip().lower()

    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                if provider_message_id and provider_message_id != "dry-run":
                    try:
                        cur.execute("""
                            SELECT *
                            FROM whatsapp_messages
                            WHERE provider_message_id = %s AND channel = %s
                            ORDER BY created_at DESC
                            LIMIT 1
                        """, (provider_message_id, channel))
                        existing = cur.fetchone()
                        if existing:
                            message = dict(existing)
                            message["_duplicate"] = True
                            _set_last_db_error("")
                            return message
                    except Exception:
                        pass
                try:
                    cur.execute("""
                        INSERT INTO whatsapp_messages (
                            phone, channel, direction, body, provider_message_id, metadata,
                            sender_type, message_type, attachments, ai_metadata, internal_only
                        )
                        VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s::jsonb, %s::jsonb, %s)
                        RETURNING *
                    """, (
                        phone,
                        channel,
                        direction,
                        body,
                        provider_message_id or "",
                        __import__('json').dumps(metadata or {}),
                        sender_type,
                        message_type,
                        __import__('json').dumps(attachments or []),
                        __import__('json').dumps(ai_metadata or {}),
                        bool(internal_only),
                    ))
                    message = dict(cur.fetchone())
                    unread_increment = 1 if direction == 'inbound' else 0
                    public_chat_id = _next_public_chat_id(cur, channel)
                    cur.execute("""
                        INSERT INTO whatsapp_conversations (
                            phone, channel, customer_name, display_handle, contact_phone,
                            status, last_message, last_direction, unread_count, chat_id, public_chat_id,
                            last_customer_message_at, service_window_expires_at, is_internal
                        )
                        VALUES (
                            %s, %s, %s, %s, %s, 'new', %s, %s, %s, gen_random_uuid(), %s,
                            CASE WHEN %s = 'inbound' THEN NOW() ELSE NULL END,
                            CASE WHEN %s = 'inbound' AND %s = 'whatsapp' THEN NOW() + INTERVAL '24 hours' ELSE NULL END,
                            %s
                        )
                        ON CONFLICT (phone) DO UPDATE SET
                            channel = EXCLUDED.channel,
                            customer_name = CASE
                                WHEN EXCLUDED.customer_name <> '' THEN EXCLUDED.customer_name
                                ELSE whatsapp_conversations.customer_name
                            END,
                            display_handle = CASE
                                WHEN EXCLUDED.display_handle <> '' THEN EXCLUDED.display_handle
                                ELSE whatsapp_conversations.display_handle
                            END,
                            contact_phone = CASE
                                WHEN EXCLUDED.contact_phone <> '' THEN EXCLUDED.contact_phone
                                ELSE whatsapp_conversations.contact_phone
                            END,
                            last_message = EXCLUDED.last_message,
                            last_direction = EXCLUDED.last_direction,
                            last_message_at = NOW(),
                            chat_id = COALESCE(whatsapp_conversations.chat_id, gen_random_uuid()),
                            public_chat_id = COALESCE(NULLIF(whatsapp_conversations.public_chat_id, ''), EXCLUDED.public_chat_id),
                            unread_count = CASE
                                WHEN EXCLUDED.last_direction = 'inbound'
                                THEN whatsapp_conversations.unread_count + 1
                                ELSE whatsapp_conversations.unread_count
                            END,
                            last_customer_message_at = CASE
                                WHEN EXCLUDED.last_direction = 'inbound' THEN NOW()
                                ELSE whatsapp_conversations.last_customer_message_at
                            END,
                            service_window_expires_at = CASE
                                WHEN EXCLUDED.last_direction = 'inbound' AND EXCLUDED.channel = 'whatsapp' THEN NOW() + INTERVAL '24 hours'
                                ELSE whatsapp_conversations.service_window_expires_at
                            END,
                            updated_at = NOW()
                    """, (
                        phone,
                        channel,
                        customer_name or "",
                        display_handle,
                        contact_phone,
                        body,
                        direction,
                        unread_increment,
                        public_chat_id,
                        direction,
                        direction,
                        channel,
                        bool(internal_only),
                    ))
                    cur.execute("""
                        UPDATE whatsapp_messages m
                        SET chat_id = c.chat_id
                        FROM whatsapp_conversations c
                        WHERE m.id = %s AND c.phone = m.phone
                        RETURNING m.*
                    """, (message["id"],))
                    refreshed = cur.fetchone()
                    if refreshed:
                        message = dict(refreshed)
                except Exception:
                    cur.execute("""
                        INSERT INTO whatsapp_messages (phone, direction, body, provider_message_id, metadata)
                        VALUES (%s, %s, %s, %s, %s::jsonb)
                        RETURNING id, phone, direction, body, provider_message_id, metadata, created_at
                    """, (
                        phone,
                        direction,
                        body,
                        provider_message_id or "",
                        __import__('json').dumps(metadata or {}),
                    ))
                    message = dict(cur.fetchone())
                    message["channel"] = channel
                    message["display_handle"] = display_handle
                    message["contact_phone"] = contact_phone
                    unread_increment = 1 if direction == 'inbound' else 0
                    cur.execute("""
                        INSERT INTO whatsapp_conversations (
                            phone, customer_name, status, last_message, last_direction, unread_count
                        )
                        VALUES (%s, %s, 'new', %s, %s, %s)
                        ON CONFLICT (phone) DO UPDATE SET
                            customer_name = CASE
                                WHEN EXCLUDED.customer_name <> '' THEN EXCLUDED.customer_name
                                ELSE whatsapp_conversations.customer_name
                            END,
                            last_message = EXCLUDED.last_message,
                            last_direction = EXCLUDED.last_direction,
                            last_message_at = NOW(),
                            unread_count = CASE
                                WHEN EXCLUDED.last_direction = 'inbound'
                                THEN whatsapp_conversations.unread_count + 1
                                ELSE whatsapp_conversations.unread_count
                            END,
                            updated_at = NOW()
                    """, (
                        phone,
                        customer_name or "",
                        body,
                        direction,
                        unread_increment,
                    ))
            conn.commit()
        _set_last_db_error("")
        return message
    except Exception as e:
        _set_last_db_error(str(e))
        print(f"DB save_whatsapp_message error: {e}")
        return None


def list_whatsapp_conversations() -> list[dict]:
    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                try:
                    cur.execute("""
                        SELECT *
                        FROM whatsapp_conversations
                        ORDER BY is_pinned DESC, last_message_at DESC NULLS LAST, updated_at DESC
                    """)
                    rows = [dict(row) for row in cur.fetchall()]
                except Exception:
                    cur.execute("""
                        SELECT phone, customer_name, status, last_message, last_direction,
                               last_message_at, unread_count, updated_at
                        FROM whatsapp_conversations
                        ORDER BY last_message_at DESC NULLS LAST, updated_at DESC
                    """)
                    rows = []
                    for row in cur.fetchall():
                        item = dict(row)
                        item.setdefault('channel', 'whatsapp')
                        item.setdefault('display_handle', item.get('phone', ''))
                        item.setdefault('contact_phone', item.get('phone', ''))
                        rows.append(item)
        _set_last_db_error("")
        return rows
    except Exception as e:
        _set_last_db_error(str(e))
        print(f"DB list_whatsapp_conversations error: {e}")
        return []


def get_whatsapp_conversation(phone: str) -> dict | None:
    return get_conversation_by_any_key(phone)


def list_whatsapp_messages(phone: str, limit: int = 80) -> list[dict]:
    conversation = get_conversation_by_any_key(phone)
    phone = conversation.get('phone') if conversation else normalize_inbox_contact_key("whatsapp", phone)
    if not phone:
        return []
    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    UPDATE whatsapp_conversations
                    SET unread_count = 0, status = CASE WHEN status = 'new' THEN 'open' ELSE status END
                    WHERE phone = %s
                """, (phone,))
                try:
                    cur.execute("""
                        SELECT *
                        FROM whatsapp_messages
                        WHERE phone = %s OR (%s::uuid IS NOT NULL AND chat_id = %s::uuid)
                        ORDER BY created_at DESC
                        LIMIT %s
                    """, (phone, conversation.get("chat_id") if conversation else None, conversation.get("chat_id") if conversation else None, limit))
                    rows = [dict(row) for row in cur.fetchall()]
                except Exception:
                    cur.execute("""
                        SELECT id, phone, direction, body, provider_message_id, metadata, created_at
                        FROM whatsapp_messages
                        WHERE phone = %s
                        ORDER BY created_at DESC
                        LIMIT %s
                    """, (phone, limit))
                    rows = []
                    for row in cur.fetchall():
                        item = dict(row)
                        item.setdefault('channel', 'whatsapp')
                        rows.append(item)
            conn.commit()
        _set_last_db_error("")
        return list(reversed(rows))
    except Exception as e:
        _set_last_db_error(str(e))
        print(f"DB list_whatsapp_messages error: {e}")
        return []


def update_whatsapp_conversation_status(phone: str, status: str) -> bool:
    conversation = get_conversation_by_any_key(phone)
    phone = conversation.get('phone') if conversation else normalize_inbox_contact_key("whatsapp", phone)
    status = (status or '').strip().lower()
    if status not in {'new', 'open', 'resolved'}:
        return False
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE whatsapp_conversations
                    SET status = %s, unread_count = CASE WHEN %s <> 'new' THEN 0 ELSE unread_count END, updated_at = NOW()
                    WHERE phone = %s
                """, (status, status, phone))
            conn.commit()
        _set_last_db_error("")
        return True
    except Exception as e:
        _set_last_db_error(str(e))
        print(f"DB update_whatsapp_conversation_status error: {e}")
        return False


def update_conversation_ai_mode(key, ai_mode, reason=''):
    ai_mode = str(ai_mode or "").strip().lower()
    if ai_mode not in {"human", "suggest", "auto", "needs_human"}:
        return False
    conversation = get_conversation_by_any_key(key)
    raw_key = str(key or "").strip()
    candidates = [raw_key]
    if raw_key and not raw_key.startswith(("facebook:", "instagram:", "internal:")):
        normalized = normalize_inbox_contact_key("whatsapp", raw_key)
        if normalized and normalized not in candidates:
            candidates.append(normalized)
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE whatsapp_conversations
                    SET ai_mode = %s,
                        ai_enabled = %s,
                        ai_paused_reason = CASE WHEN %s <> '' THEN %s ELSE ai_paused_reason END,
                        manual_lock = CASE
                            WHEN %s = 'human' THEN TRUE
                            WHEN %s IN ('suggest', 'auto') THEN FALSE
                            ELSE manual_lock
                        END,
                        updated_at = NOW()
                    WHERE phone = ANY(%s)
                       OR public_chat_id = %s
                       OR chat_id::text = %s
                """, (
                    ai_mode,
                    ai_mode == "auto",
                    reason or "",
                    reason or "",
                    ai_mode,
                    ai_mode,
                    [conversation["phone"]] if conversation else candidates,
                    raw_key,
                    raw_key,
                ))
                updated = cur.rowcount
            conn.commit()
        if updated < 1:
            _set_last_db_error(f"Conversation not found for key: {raw_key}")
            return False
        _set_last_db_error("")
        return True
    except Exception as e:
        _set_last_db_error(str(e))
        print(f"DB update_conversation_ai_mode error: {e}")
        return False


def update_conversation_labels(key, add=None, remove=None):
    conversation = get_conversation_by_any_key(key)
    if not conversation:
        return None
    labels = set(_safe_json(conversation.get("labels"), []))
    for label in add or []:
        label = str(label or "").strip().lower()
        if label:
            labels.add(label)
    for label in remove or []:
        labels.discard(str(label or "").strip().lower())
    labels = sorted(labels)
    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    UPDATE whatsapp_conversations
                    SET labels = %s::jsonb, updated_at = NOW()
                    WHERE phone = %s
                    RETURNING *
                """, (__import__('json').dumps(labels), conversation["phone"]))
                row = cur.fetchone()
            conn.commit()
        _set_last_db_error("")
        return dict(row) if row else None
    except Exception as e:
        _set_last_db_error(str(e))
        print(f"DB update_conversation_labels error: {e}")
        return None


def update_order_candidate(key, order_state, order_candidate):
    conversation = get_conversation_by_any_key(key)
    if not conversation:
        return False
    order_state = str(order_state or "no_order").strip().lower()
    if order_state not in {"no_order", "collecting_details", "new_order", "pending_human_order_creation", "order_added", "cancelled"}:
        order_state = "no_order"
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE whatsapp_conversations
                    SET order_state = %s, order_candidate = %s::jsonb, updated_at = NOW()
                    WHERE phone = %s
                """, (order_state, __import__('json').dumps(order_candidate or {}), conversation["phone"]))
            conn.commit()
        _set_last_db_error("")
        return True
    except Exception as e:
        _set_last_db_error(str(e))
        print(f"DB update_order_candidate error: {e}")
        return False


def mark_conversation_needs_human(key, reason, labels=None):
    conversation = update_conversation_labels(key, add=["needs_human", *(labels or [])])
    if not conversation:
        return False
    return update_conversation_ai_mode(conversation["phone"], "needs_human", reason or "Needs human review")


def mark_new_order(key, order_candidate):
    ok = update_order_candidate(key, "new_order", order_candidate or {})
    if ok:
        update_conversation_labels(key, add=["new_order", "pending_human_order_creation"], remove=["order_added"])
    return ok


def mark_order_added(key, human_user=''):
    conversation = get_conversation_by_any_key(key)
    if not conversation:
        return False
    candidate = _safe_json(conversation.get("order_candidate"), {})
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE whatsapp_conversations
                    SET order_state = 'order_added',
                        order_candidate = %s::jsonb,
                        labels = (
                            SELECT jsonb_agg(DISTINCT value)
                            FROM jsonb_array_elements_text(
                                (labels - 'new_order' - 'pending_human_order_creation') || '["order_added"]'::jsonb
                            ) AS value
                        ),
                        ai_mode = CASE WHEN ai_mode = 'auto' THEN 'suggest' ELSE ai_mode END,
                        ai_enabled = FALSE,
                        updated_by = %s,
                        updated_at = NOW()
                    WHERE phone = %s
                """, (__import__('json').dumps(candidate), human_user or "", conversation["phone"]))
            conn.commit()
        _set_last_db_error("")
        return True
    except Exception as e:
        _set_last_db_error(str(e))
        print(f"DB mark_order_added error: {e}")
        return False


def save_internal_assistant_message(body, metadata=None):
    assistant = ensure_tickbot_assistant_chat()
    if not assistant:
        return None
    return save_whatsapp_message(
        assistant["phone"],
        "outbound",
        body,
        metadata=metadata or {},
        channel="internal",
        display_handle="Internal assistant",
        sender_type="assistant",
        internal_only=True,
    )


def save_tickbot_knowledge(title: str, content: str, source: str = "assistant", verified: bool = True):
    title = str(title or "").strip()
    content = str(content or "").strip()
    source = str(source or "assistant").strip() or "assistant"
    if not content:
        return None
    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    INSERT INTO tickbot_knowledge (title, content, source, verified, updated_at)
                    VALUES (%s, %s, %s, %s, NOW())
                    RETURNING *
                """, (title, content, source, bool(verified)))
                row = cur.fetchone()
            conn.commit()
        _set_last_db_error("")
        return dict(row) if row else None
    except Exception as e:
        _set_last_db_error(str(e))
        print(f"DB save_tickbot_knowledge error: {e}")
        return None


def list_tickbot_knowledge(limit: int = 200, verified_only: bool = True) -> list[dict]:
    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                if verified_only:
                    cur.execute("""
                        SELECT *
                        FROM tickbot_knowledge
                        WHERE verified = TRUE
                        ORDER BY updated_at DESC, id DESC
                        LIMIT %s
                    """, (max(1, int(limit or 200)),))
                else:
                    cur.execute("""
                        SELECT *
                        FROM tickbot_knowledge
                        ORDER BY updated_at DESC, id DESC
                        LIMIT %s
                    """, (max(1, int(limit or 200)),))
                rows = cur.fetchall() or []
        _set_last_db_error("")
        return [dict(row) for row in rows]
    except Exception as e:
        _set_last_db_error(str(e))
        print(f"DB list_tickbot_knowledge error: {e}")
        return []


def save_ai_decision(key, decision_json):
    conversation = get_conversation_by_any_key(key)
    if not conversation:
        return False
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE whatsapp_conversations
                    SET ai_last_decision = %s::jsonb,
                        ai_summary = %s,
                        updated_at = NOW()
                    WHERE phone = %s
                """, (
                    __import__('json').dumps(decision_json or {}),
                    str((decision_json or {}).get("internal_note") or (decision_json or {}).get("intent") or "")[:1000],
                    conversation["phone"],
                ))
            conn.commit()
        _set_last_db_error("")
        return True
    except Exception as e:
        _set_last_db_error(str(e))
        print(f"DB save_ai_decision error: {e}")
        return False


def list_whatsapp_rules() -> list[dict]:
    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT id, name, keywords, response, enabled, hold_for_review, created_at, updated_at
                    FROM whatsapp_rules
                    ORDER BY enabled DESC, name ASC
                """)
                rows = [dict(row) for row in cur.fetchall()]
        _set_last_db_error("")
        return rows
    except Exception as e:
        _set_last_db_error(str(e))
        print(f"DB list_whatsapp_rules error: {e}")
        return []


def upsert_whatsapp_rule(payload: dict) -> dict | None:
    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                rule_id = payload.get('id')
                values = (
                    (payload.get('name') or 'Keyword rule').strip(),
                    (payload.get('keywords') or '').strip(),
                    (payload.get('response') or '').strip(),
                    bool(payload.get('enabled', True)),
                    bool(payload.get('hold_for_review', False)),
                )
                if rule_id:
                    cur.execute("""
                        UPDATE whatsapp_rules
                        SET name=%s, keywords=%s, response=%s, enabled=%s, hold_for_review=%s, updated_at=NOW()
                        WHERE id=%s
                        RETURNING id, name, keywords, response, enabled, hold_for_review, created_at, updated_at
                    """, (*values, rule_id))
                else:
                    cur.execute("""
                        INSERT INTO whatsapp_rules (name, keywords, response, enabled, hold_for_review)
                        VALUES (%s, %s, %s, %s, %s)
                        RETURNING id, name, keywords, response, enabled, hold_for_review, created_at, updated_at
                    """, values)
                row = cur.fetchone()
            conn.commit()
        _set_last_db_error("")
        return dict(row) if row else None
    except Exception as e:
        _set_last_db_error(str(e))
        print(f"DB upsert_whatsapp_rule error: {e}")
        return None


def list_whatsapp_templates() -> list[dict]:
    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT id, name, body, category, approved, created_at, updated_at
                    FROM whatsapp_templates
                    ORDER BY approved DESC, name ASC
                """)
                rows = [dict(row) for row in cur.fetchall()]
        _set_last_db_error("")
        return rows
    except Exception as e:
        _set_last_db_error(str(e))
        print(f"DB list_whatsapp_templates error: {e}")
        return []


def upsert_whatsapp_template(payload: dict) -> dict | None:
    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                template_id = payload.get('id')
                values = (
                    (payload.get('name') or 'WhatsApp template').strip(),
                    (payload.get('body') or '').strip(),
                    (payload.get('category') or 'support').strip(),
                    bool(payload.get('approved', False)),
                )
                if template_id:
                    cur.execute("""
                        UPDATE whatsapp_templates
                        SET name=%s, body=%s, category=%s, approved=%s, updated_at=NOW()
                        WHERE id=%s
                        RETURNING id, name, body, category, approved, created_at, updated_at
                    """, (*values, template_id))
                else:
                    cur.execute("""
                        INSERT INTO whatsapp_templates (name, body, category, approved)
                        VALUES (%s, %s, %s, %s)
                        RETURNING id, name, body, category, approved, created_at, updated_at
                    """, values)
                row = cur.fetchone()
            conn.commit()
        _set_last_db_error("")
        return dict(row) if row else None
    except Exception as e:
        _set_last_db_error(str(e))
        print(f"DB upsert_whatsapp_template error: {e}")
        return None


def create_whatsapp_blast(template_id, template_name: str, segment: str, total: int, sent: int, failed: int):
    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    INSERT INTO whatsapp_blasts (
                        template_id, template_name, segment, total_recipients, sent_count, failed_count, status
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, 'sent')
                    RETURNING id, template_id, template_name, segment, total_recipients, sent_count, failed_count, status, created_at
                """, (template_id, template_name, segment, total, sent, failed))
                row = cur.fetchone()
            conn.commit()
        _set_last_db_error("")
        return dict(row) if row else None
    except Exception as e:
        _set_last_db_error(str(e))
        print(f"DB create_whatsapp_blast error: {e}")
        return None


def list_whatsapp_blasts() -> list[dict]:
    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT id, template_id, template_name, segment, total_recipients,
                           sent_count, failed_count, status, created_at
                    FROM whatsapp_blasts
                    ORDER BY created_at DESC
                    LIMIT 30
                """)
                rows = [dict(row) for row in cur.fetchall()]
        _set_last_db_error("")
        return rows
    except Exception as e:
        _set_last_db_error(str(e))
        print(f"DB list_whatsapp_blasts error: {e}")
        return []


def enqueue_tickbot_auto_reply_job(payload: dict) -> dict | None:
    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                values = (
                    str(payload.get("channel") or "whatsapp"),
                    str(payload.get("contact_key") or ""),
                    str(payload.get("body") or ""),
                    str(payload.get("customer_name") or ""),
                    str(payload.get("display_handle") or ""),
                    str(payload.get("contact_phone") or ""),
                    str(payload.get("provider_message_id") or ""),
                )
                cur.execute("""
                    INSERT INTO tickbot_auto_reply_jobs (
                        channel, contact_key, body, customer_name, display_handle, contact_phone, provider_message_id
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (provider_message_id) WHERE provider_message_id <> ''
                    DO UPDATE SET updated_at = tickbot_auto_reply_jobs.updated_at
                    RETURNING id, channel, contact_key, body, customer_name, display_handle,
                              contact_phone, provider_message_id, status, attempts, created_at
                """, values)
                row = cur.fetchone()
            conn.commit()
        _set_last_db_error("")
        return dict(row) if row else None
    except Exception as e:
        _set_last_db_error(str(e))
        print(f"DB enqueue_tickbot_auto_reply_job error: {e}")
        return None


def claim_tickbot_auto_reply_jobs(limit: int = 3) -> list[dict]:
    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    WITH picked AS (
                        SELECT id
                        FROM tickbot_auto_reply_jobs
                        WHERE status = 'pending'
                          AND available_at <= NOW()
                          AND attempts < 3
                        ORDER BY id
                        FOR UPDATE SKIP LOCKED
                        LIMIT %s
                    )
                    UPDATE tickbot_auto_reply_jobs j
                    SET status = 'processing',
                        attempts = attempts + 1,
                        locked_at = NOW(),
                        updated_at = NOW()
                    FROM picked
                    WHERE j.id = picked.id
                    RETURNING j.id, j.channel, j.contact_key, j.body, j.customer_name,
                              j.display_handle, j.contact_phone, j.provider_message_id, j.attempts
                """, (limit,))
                rows = [dict(row) for row in cur.fetchall()]
            conn.commit()
        _set_last_db_error("")
        return rows
    except Exception as e:
        _set_last_db_error(str(e))
        print(f"DB claim_tickbot_auto_reply_jobs error: {e}")
        return []


def finish_tickbot_auto_reply_job(job_id: int, success: bool, error: str = "") -> bool:
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                if success:
                    cur.execute("""
                        UPDATE tickbot_auto_reply_jobs
                        SET status = 'done', last_error = '', updated_at = NOW()
                        WHERE id = %s
                    """, (job_id,))
                else:
                    cur.execute("""
                        UPDATE tickbot_auto_reply_jobs
                        SET status = CASE WHEN attempts >= 3 THEN 'failed' ELSE 'pending' END,
                            last_error = %s,
                            available_at = NOW() + (INTERVAL '30 seconds' * GREATEST(attempts, 1)),
                            updated_at = NOW()
                        WHERE id = %s
                    """, (str(error or "")[:1000], job_id))
            conn.commit()
        _set_last_db_error("")
        return True
    except Exception as e:
        _set_last_db_error(str(e))
        print(f"DB finish_tickbot_auto_reply_job error: {e}")
        return False

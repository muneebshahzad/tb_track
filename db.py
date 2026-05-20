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


def _ensure_whatsapp_tables(cur):
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
        CREATE INDEX IF NOT EXISTS idx_whatsapp_conversations_channel_updated
        ON whatsapp_conversations (channel, last_message_at DESC, updated_at DESC)
    """)
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

    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                _ensure_whatsapp_tables(cur)
                try:
                    cur.execute("""
                        INSERT INTO whatsapp_messages (phone, channel, direction, body, provider_message_id, metadata)
                        VALUES (%s, %s, %s, %s, %s, %s::jsonb)
                        RETURNING id, phone, channel, direction, body, provider_message_id, metadata, created_at
                    """, (
                        phone,
                        channel,
                        direction,
                        body,
                        provider_message_id or "",
                        __import__('json').dumps(metadata or {}),
                    ))
                    message = dict(cur.fetchone())
                    unread_increment = 1 if direction == 'inbound' else 0
                    cur.execute("""
                        INSERT INTO whatsapp_conversations (
                            phone, channel, customer_name, display_handle, contact_phone,
                            status, last_message, last_direction, unread_count
                        )
                        VALUES (%s, %s, %s, %s, %s, 'new', %s, %s, %s)
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
                            unread_count = CASE
                                WHEN EXCLUDED.last_direction = 'inbound'
                                THEN whatsapp_conversations.unread_count + 1
                                ELSE whatsapp_conversations.unread_count
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
                    ))
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
                _ensure_whatsapp_tables(cur)
                try:
                    cur.execute("""
                        SELECT phone, channel, customer_name, display_handle, contact_phone,
                               status, last_message, last_direction,
                               last_message_at, unread_count, updated_at
                        FROM whatsapp_conversations
                        ORDER BY last_message_at DESC NULLS LAST, updated_at DESC
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
    for key in (phone, normalize_inbox_contact_key("whatsapp", phone)):
        if not key:
            continue
        try:
            with get_conn() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    _ensure_whatsapp_tables(cur)
                    try:
                        cur.execute("""
                            SELECT phone, channel, customer_name, display_handle, contact_phone,
                                   status, last_message, last_direction,
                                   last_message_at, unread_count, updated_at
                            FROM whatsapp_conversations
                            WHERE phone = %s
                        """, (key,))
                        row = cur.fetchone()
                    except Exception:
                        cur.execute("""
                            SELECT phone, customer_name, status, last_message, last_direction,
                                   last_message_at, unread_count, updated_at
                            FROM whatsapp_conversations
                            WHERE phone = %s
                        """, (key,))
                        row = cur.fetchone()
                        if row:
                            row = dict(row)
                            row.setdefault('channel', 'whatsapp')
                            row.setdefault('display_handle', row.get('phone', ''))
                            row.setdefault('contact_phone', row.get('phone', ''))
                    if row:
                        _set_last_db_error("")
                        return dict(row)
        except Exception as e:
            _set_last_db_error(str(e))
            print(f"DB get_whatsapp_conversation error: {e}")
            return None
    return None


def list_whatsapp_messages(phone: str, limit: int = 80) -> list[dict]:
    conversation = get_whatsapp_conversation(phone)
    phone = conversation.get('phone') if conversation else normalize_inbox_contact_key("whatsapp", phone)
    if not phone:
        return []
    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                _ensure_whatsapp_tables(cur)
                cur.execute("""
                    UPDATE whatsapp_conversations
                    SET unread_count = 0, status = CASE WHEN status = 'new' THEN 'open' ELSE status END
                    WHERE phone = %s
                """, (phone,))
                try:
                    cur.execute("""
                        SELECT id, phone, channel, direction, body, provider_message_id, metadata, created_at
                        FROM whatsapp_messages
                        WHERE phone = %s
                        ORDER BY created_at DESC
                        LIMIT %s
                    """, (phone, limit))
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
    conversation = get_whatsapp_conversation(phone)
    phone = conversation.get('phone') if conversation else normalize_inbox_contact_key("whatsapp", phone)
    status = (status or '').strip().lower()
    if status not in {'new', 'open', 'resolved'}:
        return False
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                _ensure_whatsapp_tables(cur)
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


def list_whatsapp_rules() -> list[dict]:
    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                _ensure_whatsapp_tables(cur)
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
                _ensure_whatsapp_tables(cur)
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
                _ensure_whatsapp_tables(cur)
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
                _ensure_whatsapp_tables(cur)
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
                _ensure_whatsapp_tables(cur)
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
                _ensure_whatsapp_tables(cur)
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

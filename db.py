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

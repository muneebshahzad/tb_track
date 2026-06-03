from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import re
import threading
import time
from pathlib import Path
from datetime import datetime
from typing import Any
from urllib.parse import quote

import requests
from flask import Blueprint, current_app, jsonify, redirect, render_template, request, session
from werkzeug.utils import secure_filename

from db import (
    create_whatsapp_blast,
    claim_tickbot_auto_reply_jobs,
    enqueue_tickbot_auto_reply_job,
    ensure_tickbot_assistant_chat,
    finish_tickbot_auto_reply_job,
    get_app_setting,
    get_last_db_error,
    get_conversation_by_any_key,
    get_whatsapp_conversation,
    list_tickbot_knowledge,
    list_inbox_conversations,
    list_inbox_messages,
    list_whatsapp_blasts,
    list_whatsapp_conversations,
    list_whatsapp_messages,
    list_whatsapp_rules,
    list_whatsapp_templates,
    normalize_inbox_contact_key,
    normalize_whatsapp_phone,
    save_whatsapp_message,
    save_ai_decision,
    save_internal_assistant_message,
    save_tickbot_knowledge,
    set_app_setting,
    mark_conversation_needs_human,
    mark_new_order,
    mark_order_added,
    update_conversation_ai_mode,
    update_conversation_labels,
    update_order_candidate,
    update_whatsapp_conversation_status,
    upsert_whatsapp_rule,
    upsert_whatsapp_template,
)


whatsapp_bp = Blueprint("whatsapp", __name__, template_folder="templates")
UPLOAD_DIR_NAME = "whatsapp_uploads"
SUPPORTED_IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp"}
SOCIAL_PROFILE_CACHE: dict[tuple[str, str], dict[str, str]] = {}
PRODUCT_CACHE: dict[str, Any] = {"loaded_at": 0.0, "items": []}
AUTO_REPLY_WORKER_STARTED = False
AUTO_REPLY_WORKER_LOCK = threading.Lock()
logger = logging.getLogger(__name__)


def _json_default(value):
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def jsonify_data(payload, status=200):
    return current_app.response_class(
        json.dumps(payload, default=_json_default),
        status=status,
        mimetype="application/json",
    )


def get_order_source() -> tuple[list[dict], list[dict]]:
    return (
        list(getattr(current_app, "order_details_provider", lambda: [])() or []),
        list(getattr(current_app, "daraz_orders_provider", lambda: [])() or []),
    )


def get_product_source() -> list[dict]:
    now = time.time()
    if PRODUCT_CACHE["items"] and now - float(PRODUCT_CACHE["loaded_at"] or 0) < 300:
        return list(PRODUCT_CACHE["items"] or [])
    cached_payload = get_app_setting("tickbot_product_cache", "")
    items = []
    catalog_url = os.getenv("TICKBAGS_PRODUCTS_JSON_URL", "https://tickbags.com/products.json?limit=250")
    try:
        response = requests.get(catalog_url, timeout=12)
        response.raise_for_status()
        data = response.json() if response.content else {}
        for product in data.get("products") or []:
            product_title = str(product.get("title") or "").strip()
            handle = str(product.get("handle") or "").strip()
            product_url = f"https://tickbags.com/products/{handle}" if handle else ""
            images = product.get("images") or []
            base_image = str((images[0] or {}).get("src") or "") if images else ""
            image_by_id = {str(image.get("id")): image.get("src") for image in images if image.get("id")}
            for variant in product.get("variants") or []:
                variant_title = str(variant.get("title") or "").strip()
                display_title = product_title if variant_title in {"", "Default Title"} else f"{product_title} - {variant_title}"
                featured = variant.get("featured_image") or {}
                image = str(featured.get("src") or image_by_id.get(str(featured.get("id"))) or base_image or "")
                try:
                    price = float(variant.get("price") or 0)
                except Exception:
                    price = 0
                items.append({
                    "product_id": product.get("id"),
                    "variant_id": variant.get("id"),
                    "title": display_title,
                    "product_title": product_title,
                    "variant_title": variant_title,
                    "price": price,
                    "image": image,
                    "sku": variant.get("sku") or "",
                    "handle": handle,
                    "product_url": product_url,
                    "available": bool(variant.get("available", True)),
                })
        if items:
            set_app_setting("tickbot_product_cache", json.dumps({"loaded_at": now, "items": items}, default=_json_default))
    except Exception as e:
        logger.warning("Could not fetch public product catalog for TickBot: %s", e)
        if cached_payload:
            try:
                cached = json.loads(cached_payload)
                items = list(cached.get("items") or [])
                PRODUCT_CACHE["loaded_at"] = float(cached.get("loaded_at") or now)
                PRODUCT_CACHE["items"] = items
                return list(items)
            except Exception as cache_error:
                logger.warning("Could not load stale TickBot product cache: %s", cache_error)
        if PRODUCT_CACHE["items"]:
            return list(PRODUCT_CACHE["items"] or [])
    PRODUCT_CACHE["loaded_at"] = now
    PRODUCT_CACHE["items"] = items
    return list(items)


def clean_digits(value: str) -> str:
    return "".join(ch for ch in str(value or "") if ch.isdigit())


def meta_error_message(data: dict, fallback_text: str = "") -> str:
    error = (data or {}).get("error") or {}
    message = str(error.get("message") or fallback_text or "Meta request failed.").strip()
    code = error.get("code")
    subcode = error.get("error_subcode")
    if code == 190:
        message = "Meta access token is invalid or expired. Re-save the token in Railway and redeploy."
    extras = []
    if code:
        extras.append(f"code {code}")
    if subcode:
        extras.append(f"subcode {subcode}")
    if extras:
        message = f"{message} ({', '.join(extras)})"
    return message


def get_whatsapp_access_token() -> str:
    return (
        os.getenv("WHATSAPP_ACCESS_TOKEN")
        or os.getenv("META_WHATSAPP_TOKEN")
        or os.getenv("META_WHATSAPP_ACCESS_TOKEN")
        or ""
    ).strip()


def get_whatsapp_phone_number_id() -> str:
    return (
        os.getenv("WHATSAPP_PHONE_NUMBER_ID")
        or os.getenv("META_WHATSAPP_PHONE_NUMBER_ID")
        or ""
    ).strip()


def channel_label(channel: str) -> str:
    return {
        "whatsapp": "WhatsApp",
        "facebook": "Facebook",
        "instagram": "Instagram",
    }.get(str(channel or "").lower(), "Inbox")


def fetch_social_profile(sender_id: str, channel: str) -> dict[str, str]:
    sender_id = str(sender_id or "").strip()
    channel = str(channel or "").strip().lower() or "facebook"
    if not sender_id or channel not in {"facebook", "instagram"}:
        return {}
    cache_key = (channel, sender_id)
    cached = SOCIAL_PROFILE_CACHE.get(cache_key)
    if cached:
        return dict(cached)
    setting_key = f"social_profile_cache:{channel}:{sender_id}"
    cached_payload = get_app_setting(setting_key, "")
    if cached_payload:
        try:
            cached_profile = json.loads(cached_payload)
            cached_at = float(cached_profile.get("cached_at") or 0)
            if cached_profile.get("profile") and time.time() - cached_at < 86400:
                profile = dict(cached_profile.get("profile") or {})
                SOCIAL_PROFILE_CACHE[cache_key] = profile
                return dict(profile)
        except Exception:
            pass

    token = (os.getenv("META_PAGE_ACCESS_TOKEN") or os.getenv("FACEBOOK_PAGE_ACCESS_TOKEN") or "").strip()
    api_version = os.getenv("META_GRAPH_API_VERSION", "v20.0")
    if not token:
        return {}

    fields = "name,profile_pic"
    if channel == "instagram":
        fields = "username,name,profile_pic"

    try:
        response = requests.get(
            f"https://graph.facebook.com/{api_version}/{sender_id}",
            params={"fields": fields},
            headers={"Authorization": f"Bearer {token}"},
            timeout=15,
        )
        data = response.json() if response.content else {}
        if not response.ok:
            return {}
        profile = {
            "name": str(data.get("name") or "").strip(),
            "username": str(data.get("username") or "").strip(),
            "profile_pic": str(data.get("profile_pic") or "").strip(),
        }
        SOCIAL_PROFILE_CACHE[cache_key] = profile
        set_app_setting(setting_key, json.dumps({"cached_at": time.time(), "profile": profile}, default=_json_default))
        return dict(profile)
    except Exception:
        return {}


def get_upload_dir() -> Path:
    target = Path(current_app.root_path) / "static" / UPLOAD_DIR_NAME
    target.mkdir(parents=True, exist_ok=True)
    return target


def get_public_base_url() -> str:
    return (
        os.getenv("SHOPIFY_APP_BASE_URL")
        or request.host_url.rstrip("/")
    ).rstrip("/")


def download_whatsapp_media(media_id: str) -> dict[str, str]:
    media_id = str(media_id or "").strip()
    token = get_whatsapp_access_token()
    api_version = os.getenv("META_GRAPH_API_VERSION", "v20.0")
    if not media_id or not token:
        return {}

    try:
        info_response = requests.get(
            f"https://graph.facebook.com/{api_version}/{media_id}",
            headers={"Authorization": f"Bearer {token}"},
            timeout=20,
        )
        info = info_response.json() if info_response.content else {}
        if not info_response.ok:
            return {"download_error": meta_error_message(info, info_response.text)}

        media_url = str(info.get("url") or "").strip()
        mime_type = str(info.get("mime_type") or "").lower()
        if not media_url:
            return {"download_error": "Meta media URL was empty."}

        media_response = requests.get(
            media_url,
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        if not media_response.ok:
            return {"download_error": f"Could not download media ({media_response.status_code})."}

        extension = {
            "image/jpeg": ".jpg",
            "image/jpg": ".jpg",
            "image/png": ".png",
            "image/webp": ".webp",
        }.get(mime_type)
        if not extension:
            content_type = str(media_response.headers.get("Content-Type") or "").split(";", 1)[0].lower()
            extension = {
                "image/jpeg": ".jpg",
                "image/jpg": ".jpg",
                "image/png": ".png",
                "image/webp": ".webp",
            }.get(content_type, ".jpg")

        safe_id = secure_filename(media_id)[:80] or "media"
        filename = f"inbound_{datetime.now().strftime('%Y%m%d%H%M%S')}_{safe_id}{extension}"
        target = get_upload_dir() / filename
        target.write_bytes(media_response.content)
        return {
            "attachment_url": f"{get_public_base_url()}/static/{UPLOAD_DIR_NAME}/{filename}",
            "media_mime_type": mime_type or str(media_response.headers.get("Content-Type") or ""),
            "media_filename": filename,
        }
    except Exception as e:
        return {"download_error": str(e)}


def get_whatsapp_settings() -> dict[str, Any]:
    raw = get_app_setting("whatsapp_inbox_settings", "")
    defaults = {
        "auto_handle_enabled": True,
        "ai_mode_enabled": False,
        "ai_handover_enabled": True,
        "business_facts": (
            "TickBags sells bean bags, ottomans, floor cushions, and home decor. "
            "Common customer questions are about prices, delivery time, payment method, and order tracking."
        ),
        "delivery_time_reply": "Our usual delivery time is 3 to 7 working days depending on your city and order type.",
        "payment_method_reply": "We mainly offer Cash on Delivery. Bank transfer can be arranged in special cases.",
        "price_reply": "Please share the product name or screenshot and we’ll confirm the latest price for you.",
        "human_handoff_reply": "Our team is checking this for you and will reply shortly.",
        "global_ai_mode_enabled": False,
        "default_new_chat_ai_mode": "suggest",
        "auto_reply_inside_service_window_only": True,
        "order_collection_enabled": True,
        "escalation_reply": "Let me have our team check this and reply with the correct details.",
        "ai_tone": "Warm, persuasive, professional, and brief.",
        "openai_model": os.getenv("WHATSAPP_AI_MODEL", "gpt-4.1-mini"),
    }
    if raw:
        try:
            defaults.update(json.loads(raw))
        except Exception:
            pass
    return defaults


def save_whatsapp_settings(settings: dict[str, Any]) -> bool:
    current = get_whatsapp_settings()
    current.update({
        "auto_handle_enabled": bool(settings.get("auto_handle_enabled", current.get("auto_handle_enabled", True))),
        "ai_mode_enabled": bool(settings.get("ai_mode_enabled", current.get("ai_mode_enabled", False))),
        "ai_handover_enabled": bool(settings.get("ai_handover_enabled", current.get("ai_handover_enabled", True))),
        "business_facts": str(settings.get("business_facts", current.get("business_facts", "")) or "").strip(),
        "delivery_time_reply": str(settings.get("delivery_time_reply", current.get("delivery_time_reply", "")) or "").strip(),
        "payment_method_reply": str(settings.get("payment_method_reply", current.get("payment_method_reply", "")) or "").strip(),
        "price_reply": str(settings.get("price_reply", current.get("price_reply", "")) or "").strip(),
        "human_handoff_reply": str(settings.get("human_handoff_reply", current.get("human_handoff_reply", "")) or "").strip(),
        "global_ai_mode_enabled": bool(settings.get("global_ai_mode_enabled", current.get("global_ai_mode_enabled", False))),
        "default_new_chat_ai_mode": str(settings.get("default_new_chat_ai_mode", current.get("default_new_chat_ai_mode", "suggest")) or "suggest").strip(),
        "auto_reply_inside_service_window_only": bool(settings.get("auto_reply_inside_service_window_only", current.get("auto_reply_inside_service_window_only", True))),
        "order_collection_enabled": bool(settings.get("order_collection_enabled", current.get("order_collection_enabled", True))),
        "escalation_reply": str(settings.get("escalation_reply", current.get("escalation_reply", "")) or "").strip(),
        "ai_tone": str(settings.get("ai_tone", current.get("ai_tone", "")) or "").strip(),
        "openai_model": str(settings.get("openai_model", current.get("openai_model", "gpt-4.1-mini")) or "gpt-4.1-mini").strip(),
    })
    return set_app_setting("whatsapp_inbox_settings", json.dumps(current))


KNOWLEDGE_STOP_WORDS = {
    "the", "a", "an", "and", "or", "to", "for", "of", "is", "are", "in", "on", "at", "with",
    "your", "you", "please", "can", "could", "would", "will", "this", "that", "from", "have",
    "has", "had", "our", "their", "about", "what", "when", "where", "which", "tell", "share",
    "mera", "meri", "mere", "ap", "aap", "ka", "ki", "ke", "hai", "hain", "kar", "karo", "karo",
    "dein", "do", "main", "mujhe", "mujhy", "mujh", "ho", "hona", "tha", "thi", "tk", "tak",
    "order", "tracking", "track", "status", "phone", "number", "product", "price",
}


def _knowledge_tokens(text: str) -> set[str]:
    text = str(text or "").lower()
    tokens = set(re.findall(r"[a-z0-9\u0600-\u06ff]{3,}", text))
    return {token for token in tokens if token not in KNOWLEDGE_STOP_WORDS}


def search_tickbot_knowledge(query: str, limit: int = 3) -> list[dict]:
    query_tokens = _knowledge_tokens(query)
    if not query_tokens:
        return []
    matches: list[dict] = []
    for entry in list_tickbot_knowledge(limit=250):
        haystack = " ".join([
            str(entry.get("title") or ""),
            str(entry.get("content") or ""),
        ])
        entry_tokens = _knowledge_tokens(haystack)
        if not entry_tokens:
            continue
        overlap = len(query_tokens & entry_tokens)
        if not overlap:
            continue
        score = overlap / max(len(query_tokens), 1)
        if str(query or "").lower().strip() and str(query).lower().strip() in haystack.lower():
            score += 0.5
        if score >= 0.34 or overlap >= 2:
            enriched = dict(entry)
            enriched["_score"] = round(score, 4)
            matches.append(enriched)
    matches.sort(key=lambda item: (float(item.get("_score") or 0), str(item.get("updated_at") or "")), reverse=True)
    return matches[: max(1, int(limit or 3))]


def latest_unresolved_assistant_alert(reference: str = "") -> dict | None:
    reference = str(reference or "").strip()
    messages = list_inbox_messages("internal:tickbot-assistant", limit=120)
    for msg in reversed(messages):
        metadata = _safe_json_dict((msg or {}).get("metadata"))
        if metadata.get("source") != "ai_escalation":
            continue
        conversation_ref = str(metadata.get("conversation") or "").strip()
        public_ref = str(metadata.get("public_chat_id") or "").strip()
        if reference and reference not in {conversation_ref, public_ref}:
            continue
        return {
            "message": msg,
            "metadata": metadata,
            "conversation": get_conversation_by_any_key(public_ref or conversation_ref) if (public_ref or conversation_ref) else None,
        }
    return None


def parse_assistant_teach_command(body: str) -> tuple[str, str] | None:
    text = str(body or "").strip()
    match = re.match(r"^\s*teach\s*:\s*(.+?)\s*=>\s*(.+)\s*$", text, flags=re.I | re.S)
    if not match:
        return None
    question = match.group(1).strip()
    answer = match.group(2).strip()
    if not question or not answer:
        return None
    return question, answer


def parse_assistant_answer_command(body: str) -> tuple[str, str] | None:
    text = str(body or "").strip()
    patterns = [
        r"^\s*answer\s+([A-Za-z0-9:+\-_#]+)\s*:\s*(.+)\s*$",
        r"^\s*reply\s+([A-Za-z0-9:+\-_#]+)\s*:\s*(.+)\s*$",
        r"^\s*([A-Za-z]{2}-[A-Za-z]{2}-[0-9]{6}|internal:tickbot-assistant|[+0-9][A-Za-z0-9:+\-_#]*)\s*:\s*(.+)\s*$",
    ]
    for pattern in patterns:
        match = re.match(pattern, text, flags=re.I | re.S)
        if match:
            return match.group(1).strip(), match.group(2).strip()
    return None


def _assistant_answer_success_note(reference: str, learned: bool, sent: bool, send_error: str = "") -> str:
    parts = [f"Saved answer for {reference}."]
    parts.append("TickBot will reuse it for similar questions." if learned else "Could not save reusable knowledge.")
    if sent:
        parts.append("Customer reply sent.")
    elif send_error:
        parts.append(f"Customer reply not sent: {send_error}")
    return " ".join(parts)


def _is_simple_safe_restart(text: str) -> bool:
    normalized = _normalized_customer_text(text)
    if not normalized:
        return False
    if normalized in set(AI_GREETING_PHRASES):
        return True
    if any(phrase in normalized for phrase in AI_SMALL_TALK_PHRASES):
        return True
    if any(phrase in normalized for phrase in AI_ORDER_HELP_PHRASES):
        return True
    if any(phrase in normalized for phrase in AI_AVAILABILITY_PHRASES):
        return True
    if any(phrase in normalized for phrase in AI_LOCATION_PHRASES):
        return True
    if any(phrase in normalized for phrase in AI_SUGGESTION_PHRASES):
        return True
    return normalized in {"?", "??", "???", "hi", "hello", "salam", "assalamualaikum"}


def phone_matches(left: str, right: str) -> bool:
    left_digits = clean_digits(left)
    right_digits = clean_digits(right)
    if not left_digits or not right_digits:
        return False
    return left_digits[-10:] == right_digits[-10:] or left_digits == right_digits


def order_id_matches(left: Any, right: Any) -> bool:
    left_digits = clean_digits(str(left or ""))
    right_digits = clean_digits(str(right or ""))
    return bool(left_digits and right_digits and (left_digits == right_digits or left_digits.endswith(right_digits) or right_digits.endswith(left_digits)))


def extract_order_reference(text: str) -> str:
    match = re.search(r"(?:order\s*#?|#)\s*([0-9]{5,})", str(text or ""), flags=re.I)
    if match:
        return match.group(1)
    match = re.search(r"\b([0-9]{7,})\b", str(text or ""))
    return match.group(1) if match else ""


def serialize_shopify_order(order: dict) -> dict:
    return {
        "source": "Shopify",
        "order_id": order.get("order_id"),
        "status": order.get("status"),
        "total_price": order.get("total_price"),
        "created_at": order.get("created_at"),
        "items": [
            {
                "title": item.get("product_title"),
                "quantity": item.get("quantity"),
                "tracking_number": item.get("tracking_number"),
                "tracking_url": item.get("tracking_url") or item.get("tracking_link") or "",
                "status": item.get("status"),
            }
            for item in order.get("line_items", [])
        ],
    }


def serialize_daraz_order(order: dict) -> dict:
    return {
        "source": "Daraz",
        "order_id": order.get("order_id"),
        "status": order.get("status"),
        "total_price": order.get("total_price"),
        "created_at": order.get("date"),
        "items": [
            {
                "title": item.get("name") or item.get("item_title"),
                "quantity": item.get("quantity"),
                "tracking_number": item.get("tracking_number"),
                "tracking_url": item.get("tracking_url") or item.get("tracking_link") or "",
                "status": item.get("status"),
            }
            for item in order.get("items_list", [])
        ],
    }


def customer_orders_for_phone(phone: str) -> list[dict]:
    shopify_orders, daraz_orders = get_order_source()
    matches = []

    for order in shopify_orders:
        customer = order.get("customer_details") or {}
        order_phone = customer.get("phone") or ""
        if not order_phone:
            for item in order.get("line_items", []):
                order_phone = item.get("phone") or ""
                if order_phone:
                    break
        if not phone_matches(phone, order_phone):
            continue
        matches.append(serialize_shopify_order(order))

    for order in daraz_orders:
        if not phone_matches(phone, order.get("customer_phone", "")):
            continue
        matches.append(serialize_daraz_order(order))

    matches.sort(key=lambda item: str(item.get("created_at") or ""), reverse=True)
    return matches[:12]


def find_order_by_reference(reference: str) -> dict | None:
    reference = str(reference or "").strip()
    if not reference:
        return None
    shopify_orders, daraz_orders = get_order_source()
    for order in shopify_orders:
        if (
            order_id_matches(order.get("order_id"), reference)
            or order_id_matches(order.get("id"), reference)
        ):
            return serialize_shopify_order(order)
        for item in order.get("line_items", []) or []:
            if order_id_matches(item.get("tracking_number"), reference):
                return serialize_shopify_order(order)
    for order in daraz_orders:
        if order_id_matches(order.get("order_id"), reference):
            return serialize_daraz_order(order)
    return None


def customer_orders_for_conversation(conversation: dict | None) -> list[dict]:
    if not conversation:
        return []
    channel = str(conversation.get("channel") or "whatsapp").lower()
    if channel == "whatsapp":
        return customer_orders_for_phone(conversation.get("contact_phone") or conversation.get("phone") or "")
    if conversation.get("contact_phone"):
        return customer_orders_for_phone(conversation.get("contact_phone") or "")
    return []


def latest_order_summary(phone_or_contact: str) -> str:
    orders = customer_orders_for_phone(phone_or_contact)
    if not orders:
        return ""
    latest = orders[0]
    tracking = ""
    for item in latest.get("items", []) or []:
        if item.get("tracking_number"):
            tracking = str(item.get("tracking_number"))
            break
    summary = f"Latest order {latest.get('order_id')} is {latest.get('status') or 'being processed'}."
    if tracking:
        summary += f" Tracking: {tracking}."
    return summary


def order_summary(order: dict | None) -> str:
    if not order:
        return ""
    tracking = ""
    tracking_url = ""
    item_status = ""
    for item in order.get("items", []) or []:
        if not item_status and item.get("status"):
            item_status = str(item.get("status"))
        if item.get("tracking_number"):
            tracking = str(item.get("tracking_number"))
            tracking_url = str(item.get("tracking_url") or "").strip()
            break
    status = order.get("status") or item_status or "being processed"
    status_key = str(status or "").strip().lower().replace("_", "-")
    if tracking and not tracking_url and tracking.upper() != "N/A":
        tracking_url = f"https://dashboard.tickbags.com/track/{quote(tracking)}"
    if status_key in {"un-booked", "unbooked", "manufacturing", "packed", "pending"} or not tracking or tracking.upper() == "N/A":
        return "Your order is currently being manufactured and will be forwarded for delivery as soon as possible."
    if status_key in {"booked", "consignment booked", "drop off at express center"}:
        reply = "Your order has been handed over to Leopard's Courier, they shall deliver it as soon as possible."
    elif status_key in {"out for delivery", "assigned to courier", "assigned to courier in", "assigned to courier for delivery"}:
        reply = "Your order is currently assigned to courier for delivery."
    else:
        reply = f"Your order is currently {status}."
    if tracking_url:
        reply += f"\n\nYou can track your order here: {tracking_url}"
    elif tracking:
        reply += f"\n\nTracking number: {tracking}"
    return reply


def render_template_body(body: str, phone: str) -> str:
    orders = customer_orders_for_phone(phone)
    latest = orders[0] if orders else {}
    replacements = {
        "{{phone}}": phone,
        "{{order_id}}": str(latest.get("order_id") or ""),
        "{{order_status}}": str(latest.get("status") or ""),
        "{{tracking_number}}": "",
    }
    for item in latest.get("items", []) or []:
        if item.get("tracking_number"):
            replacements["{{tracking_number}}"] = str(item.get("tracking_number"))
            break
    rendered = body or ""
    for token, value in replacements.items():
        rendered = rendered.replace(token, value)
    return rendered


def find_keyword_reply(message: str) -> tuple[str, dict | None]:
    text = (message or "").lower()
    for rule in list_whatsapp_rules():
        if not rule.get("enabled"):
            continue
        keywords = [
            keyword.strip().lower()
            for keyword in str(rule.get("keywords") or "").split(",")
            if keyword.strip()
        ]
        if keywords and any(keyword in text for keyword in keywords):
            return rule.get("response") or "", rule
    return "", None


def fallback_reply(phone: str, message: str) -> str:
    text = (message or "").lower()
    settings = get_whatsapp_settings()
    order_reference = extract_order_reference(message)
    if order_reference:
        summary = order_summary(find_order_by_reference(order_reference))
        if summary:
            return summary
    orders = customer_orders_for_phone(phone)
    if orders and any(word in text for word in ("track", "tracking", "order", "status")):
        return latest_order_summary(phone)
    if any(word in text for word in ("price", "cost", "rate", "kitnay", "kitna", "how much")):
        return settings.get("price_reply") or "Please share the product name or screenshot and we’ll confirm the latest price for you."
    if any(word in text for word in ("delivery", "deliver", "dispatch", "time", "days")):
        return settings.get("delivery_time_reply") or "Our usual delivery time is 3 to 7 working days depending on your city and order type."
    if any(word in text for word in ("payment", "cod", "cash", "bank", "advance")):
        return settings.get("payment_method_reply") or "We mainly offer Cash on Delivery. Bank transfer can be arranged in special cases."
    return settings.get("human_handoff_reply") or os.getenv(
        "WHATSAPP_FALLBACK_REPLY",
        "Thanks for messaging TickBags. Our team will review this and reply shortly.",
    )


AI_DECISION_DEFAULT = {
    "intent": "unknown",
    "confidence": 0.0,
    "should_auto_reply": False,
    "needs_human": True,
    "labels": ["needs_human"],
    "reply_text": "",
    "order_state": "no_order",
    "order_candidate": {},
    "missing_order_fields": [],
    "escalation_reason": "AI decision unavailable.",
    "knowledge_gap_question": "",
    "internal_note": "",
}

AI_BLOCKING_LABELS = {
    "needs_human", "complaint", "refund", "damaged_item", "angry_customer",
    "payment_issue", "high_value_order", "unclear_product", "outside_policy",
    "manual_lock", "order_added",
}

AI_RISK_PHRASES = (
    "refund", "exchange", "damaged", "damage", "broken", "complaint", "angry",
    "legal", "fraud", "scam", "payment issue", "paid but", "money back",
)

AI_OUT_OF_SCOPE_PHRASES = (
    "job", "relationship", "homework", "weather", "news", "politics", "medical",
    "doctor", "lawyer", "investment", "crypto",
)

AI_LOCATION_PHRASES = (
    "where are you based", "where r u based", "where in pakistan", "location",
    "pin location", "store", "shop", "address kaha", "kahan", "kidhar",
)

AI_SUGGESTION_PHRASES = (
    "suggest", "recommend", "which beanbag", "which bean bag", "beanbag",
    "bean bag", "best one", "kon sa", "konsa", "mujhy product",
    "mujhe product", "product ke bary", "product ke baray", "product ke barey",
    "product ke baare", "product k bary", "product k baray", "product k barey",
    "product k baare", "product bata", "product bta", "bary main bata",
    "baray main bata", "barey main bata", "baare main bata", "bata skty",
    "bata sakty", "bta skty", "bta sakty",
)

AI_SMALL_TALK_PHRASES = (
    "how are you", "how r u", "how are u", "how are you doing", "how r you",
    "kaise ho", "kaisay ho", "kesay ho", "kese ho", "ap kese", "aap kese",
    "kia haal hai", "kya haal hai", "kia hal hai", "kya hal hai",
    "ka haal hai", "ap ka kia haal", "aap ka kia haal", "ap ka kya haal",
    "aap ka kya haal", "apka kia haal", "apka kya haal",
)

AI_GREETING_PHRASES = (
    "hi", "hello", "hey", "salam", "assalam", "assalamualaikum",
    "asalam o aliqum", "assalam o alaikum", "asalam alaikum", "assalam alaikum",
    "salam o alaikum", "salaam", "?", "??", "???",
)

AI_FOLLOWUP_PHRASES = (
    "material", "fabric", "which fabric", "filling", "filled with", "color",
    "colors", "size", "dimensions", "price", "cost", "rate", "delivery",
    "deliver", "warranty",
)

AI_ORDER_HELP_PHRASES = (
    "order kaise", "order kaise karoon", "order kaise karun", "order kaisy",
    "order kaisy karoon", "order kaisy karun", "order kese", "order kese karoon",
    "order kese karun", "order kesay", "order kesay karoon", "order kesay karun",
    "how to order", "place order", "order process", "i want to place an order",
    "want to order", "want to buy", "order dena", "order krna", "order karna",
    "order lagana", "order laga do", "mujhe order", "mujhy order", "order chahiye",
    "order kar dein", "order kar do", "order karo",
)

AI_AVAILABILITY_PHRASES = (
    "available", "availability", "in stock", "stock", "mil jayega", "mil jaega",
    "mil jaye ga", "hai kya", "hy kya", "ye hai", "is this available",
)

AI_BROAD_CATEGORY_TERMS = (
    "bean bag", "bean bags", "beanbag", "beanbags", "ottoman", "ottomans",
    "floor cushion", "floor cushions", "cushion", "cushions", "home decor",
)


def _safe_json_dict(value: Any) -> dict:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _safe_json_list(value: Any) -> list:
    if isinstance(value, list):
        return value
    if not value:
        return []
    try:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, list) else []
    except Exception:
        return []


def _normalized_customer_text(value: str) -> str:
    text = re.sub(r"[^a-z0-9\s]", " ", str(value or "").lower())
    text = re.sub(r"\s+", " ", text).strip()
    replacements = {
        "bary": "baray",
        "barey": "baray",
        "baare": "baray",
        "baryy": "baray",
        "skty": "sakty",
        "skte": "sakty",
        "sktay": "sakty",
        "bta": "bata",
        "mujhy": "mujhe",
        "mjy": "mujhe",
        "kia": "kya",
        "hal": "haal",
    }
    words = [replacements.get(word, word) for word in text.split()]
    return " ".join(words)


def _product_match_text(value: str) -> str:
    text = _normalized_customer_text(value)
    stop_words = {
        "price", "cost", "rate", "how", "much", "kitna", "kitnay", "kya", "kia",
        "hai", "he", "please", "plz", "pls", "tell", "about", "bata", "sakty",
        "hain", "main", "ke", "k", "ka", "ki", "product", "details", "detail",
        "wrong", "answer", "track", "tracking", "order", "status", "number",
        "check", "kahan", "kidhar", "mila", "mily", "deliver",
    }
    return " ".join(word for word in text.split() if word not in stop_words)


def _product_tokens(value: str) -> set[str]:
    return {word for word in _product_match_text(value).split() if len(word) >= 3}


def normalize_ai_decision(raw: dict | None) -> dict:
    decision = dict(AI_DECISION_DEFAULT)
    if isinstance(raw, dict):
        decision.update(raw)
    decision["intent"] = str(decision.get("intent") or "unknown")
    try:
        decision["confidence"] = max(0.0, min(1.0, float(decision.get("confidence") or 0)))
    except Exception:
        decision["confidence"] = 0.0
    decision["should_auto_reply"] = bool(decision.get("should_auto_reply"))
    decision["needs_human"] = bool(decision.get("needs_human"))
    decision["labels"] = [str(label).strip().lower() for label in (decision.get("labels") or []) if str(label).strip()]
    decision["reply_text"] = str(decision.get("reply_text") or "").strip()
    if decision.get("order_state") not in {"no_order", "collecting_details", "new_order", "pending_human_order_creation", "order_added", "cancelled"}:
        decision["order_state"] = "no_order"
    decision["order_candidate"] = decision.get("order_candidate") if isinstance(decision.get("order_candidate"), dict) else {}
    decision["missing_order_fields"] = [str(item) for item in (decision.get("missing_order_fields") or []) if str(item)]
    decision["escalation_reason"] = str(decision.get("escalation_reason") or "").strip()
    decision["knowledge_gap_question"] = str(decision.get("knowledge_gap_question") or "").strip()
    decision["internal_note"] = str(decision.get("internal_note") or "").strip()
    if decision["needs_human"] and "needs_human" not in decision["labels"]:
        decision["labels"].append("needs_human")
    return decision


def service_window_open(conversation: dict | None) -> bool:
    if not conversation or str(conversation.get("channel") or "whatsapp").lower() != "whatsapp":
        return True
    expires = conversation.get("service_window_expires_at")
    if not expires:
        return False
    if isinstance(expires, str):
        try:
            expires = datetime.fromisoformat(expires.replace("Z", "+00:00"))
        except Exception:
            return False
    if getattr(expires, "tzinfo", None):
        return expires > datetime.now(expires.tzinfo)
    return expires > datetime.now()


def _parse_timestamp(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None


def _hours_since(value: Any) -> float | None:
    timestamp = _parse_timestamp(value)
    if not timestamp:
        return None
    if getattr(timestamp, "tzinfo", None):
        now = datetime.now(timestamp.tzinfo)
    else:
        now = datetime.now()
    return max(0.0, (now - timestamp).total_seconds() / 3600.0)


def _latest_previous_inbound_time(contact_key: str) -> datetime | None:
    messages = list_inbox_messages(contact_key, limit=12)
    inbound_seen = 0
    for msg in reversed(messages):
        if str((msg or {}).get("direction") or "").lower() != "inbound":
            continue
        inbound_seen += 1
        if inbound_seen == 1:
            # This is usually the message that just triggered maybe_auto_reply.
            continue
        return _parse_timestamp((msg or {}).get("created_at"))
    return None


def _should_resume_needs_human(text: str) -> bool:
    if text in set(AI_GREETING_PHRASES):
        return True
    if any(phrase in text for phrase in AI_SMALL_TALK_PHRASES):
        return True
    if any(phrase in text for phrase in AI_ORDER_HELP_PHRASES):
        return True
    return any(word in text for word in ("order", "buy", "price", "delivery", "payment", "track", "tracking"))


def maybe_resume_needs_human(conversation: dict | None, contact_key: str, body: str) -> dict | None:
    if not conversation or str(conversation.get("ai_mode") or "").lower() != "needs_human":
        return conversation
    text = _normalized_customer_text(body)
    if not _should_resume_needs_human(text):
        return conversation
    previous_inbound = _latest_previous_inbound_time(contact_key)
    cooldown_hours = _hours_since(previous_inbound)
    if cooldown_hours is None or cooldown_hours < 1.0:
        return conversation
    update_conversation_ai_mode(contact_key, "auto", "Auto-resumed after fresh customer restart")
    update_conversation_labels(contact_key, remove=["needs_human", "manual_lock"])
    return get_conversation_by_any_key(contact_key) or conversation


def build_ai_context(conversation: dict, messages: list[dict], orders: list[dict], settings: dict) -> dict:
    latest_customer_message = ""
    for msg in reversed(messages[-12:]):
        if str(msg.get("direction") or "") == "inbound":
            latest_customer_message = str(msg.get("body") or "").strip()
            if latest_customer_message:
                break
    knowledge_matches = search_tickbot_knowledge(latest_customer_message, limit=3) if latest_customer_message else []
    return {
        "conversation": {
            "channel": conversation.get("channel"),
            "customer_name": conversation.get("customer_name"),
            "display_handle": conversation.get("display_handle"),
            "public_chat_id": conversation.get("public_chat_id"),
            "ai_mode": conversation.get("ai_mode"),
            "labels": _safe_json_list(conversation.get("labels")),
            "order_state": conversation.get("order_state"),
            "order_candidate": _safe_json_dict(conversation.get("order_candidate")),
        },
        "recent_messages": [
            {
                "direction": msg.get("direction"),
                "sender_type": msg.get("sender_type"),
                "body": msg.get("body"),
                "created_at": msg.get("created_at"),
            }
            for msg in messages[-12:]
        ],
        "recent_orders": orders[:5],
        "business_facts": settings.get("business_facts", ""),
        "knowledge_matches": [
            {
                "title": item.get("title"),
                "content": item.get("content"),
                "score": item.get("_score"),
            }
            for item in knowledge_matches
        ],
        "safe_defaults": {
            "delivery_time": settings.get("delivery_time_reply"),
            "payment_method": settings.get("payment_method_reply"),
            "price": settings.get("price_reply"),
        },
        "tone": settings.get("ai_tone", ""),
    }


def _recent_product_context(messages: list[dict] | None, conversation: dict | None = None) -> str:
    candidate = ""
    order_candidate = _safe_json_dict((conversation or {}).get("order_candidate"))
    for key in ("product", "product_link", "product_image_reference"):
        if order_candidate.get(key):
            return str(order_candidate[key]).strip()[:120]
    for msg in reversed(messages or []):
        body = str((msg or {}).get("body") or "").strip()
        if not body or len(body) < 4:
            continue
        detail_match = re.search(r"details (?:i found )?for ([^:\n]+)", body, flags=re.I)
        if detail_match:
            candidate = detail_match.group(1).strip()
            return candidate[:120]
        if "tick bag" in body.lower() or "beanbag" in body.lower() or "bean bag" in body.lower():
            for sentence in re.split(r"[\n.!?]+", body):
                sentence = sentence.strip()
                if "tick bag" in sentence.lower() or "beanbag" in sentence.lower() or "bean bag" in sentence.lower():
                    candidate = sentence
                    break
        if not candidate and ("tickbags.com" in body.lower() or "http" in body.lower()):
            candidate = body
        if candidate:
            match = re.match(r"^(?:the\s+)?(.+?)\s+(?:is|with price|costs|comes|has|features)\b", candidate, flags=re.I)
            if match:
                candidate = match.group(1).strip()
            return candidate[:120]
    return ""


def _recent_image_context(messages: list[dict] | None) -> bool:
    for msg in reversed(messages or []):
        body = str((msg or {}).get("body") or "").strip().lower()
        message_type = str((msg or {}).get("message_type") or "").lower()
        metadata = _safe_json_dict((msg or {}).get("metadata"))
        if body == "[image]" or message_type == "image" or metadata.get("media_type") == "image":
            return True
        if body and body != "[image]" and str((msg or {}).get("direction") or "") == "inbound":
            return False
    return False


def _recent_availability_request(messages: list[dict] | None) -> bool:
    for msg in reversed(messages or []):
        body = _normalized_customer_text((msg or {}).get("body") or "")
        direction = str((msg or {}).get("direction") or "")
        message_type = str((msg or {}).get("message_type") or "").lower()
        metadata = _safe_json_dict((msg or {}).get("metadata"))
        if message_type == "image" or metadata.get("media_type") == "image" or body == "image":
            continue
        if direction == "inbound" and any(phrase in body for phrase in AI_AVAILABILITY_PHRASES):
            return True
        if body and direction == "inbound":
            return False
    return False


def _image_availability_reply() -> str:
    return (
        "Is photo mein product clear hai, lekin exact product/variant confirm karne ke liye "
        "please product ka name ya link bhi send kar dein. Team availability aur latest price confirm kar degi."
    )


def _availability_without_product_reply() -> str:
    return "Ji, availability confirm karne ke liye product ka name, link ya picture send kar dein. Main latest price aur stock check kar dunga."


def _broad_category_availability_reply(text: str) -> str:
    if any(word in text for word in ("salam", "assalam", "hello", "hi", "hey")):
        return "Hello! Yes, bean bags are available. Please share which color/size you want, or send the product picture/link and I will guide you."
    return "Yes, bean bags are available. Please share your preferred color/size, or send the product picture/link and I will guide you."


def _is_broad_category_availability(text: str) -> bool:
    return any(phrase in text for phrase in AI_AVAILABILITY_PHRASES) and any(term in text for term in AI_BROAD_CATEGORY_TERMS)


def _find_product_matches(query: str, messages: list[dict] | None = None, conversation: dict | None = None, limit: int = 4) -> list[dict]:
    lookup_query = _product_match_text(query)
    context = _recent_product_context(messages, conversation)
    if len(_product_tokens(lookup_query)) < 1 and context:
        lookup_query = _product_match_text(context)
    query_tokens = _product_tokens(lookup_query)
    if not query_tokens:
        return []
    matches = []
    for item in get_product_source():
        title = str(item.get("title") or item.get("product_title") or "")
        haystack = " ".join([
            title,
            str(item.get("product_title") or ""),
            str(item.get("variant_title") or ""),
            str(item.get("sku") or ""),
            str(item.get("handle") or ""),
        ])
        product_tokens = _product_tokens(haystack)
        if not product_tokens:
            continue
        overlap = len(query_tokens & product_tokens)
        if not overlap:
            continue
        score = overlap / max(len(query_tokens), 1)
        if lookup_query and lookup_query in _product_match_text(haystack):
            score += 0.75
        if score >= 0.45 or overlap >= 2:
            enriched = dict(item)
            enriched["_score"] = score
            matches.append(enriched)
    matches.sort(key=lambda item: (item.get("_score") or 0, -len(str(item.get("title") or ""))), reverse=True)
    return matches[:limit]


def _format_money(value: Any) -> str:
    try:
        amount = float(value or 0)
    except Exception:
        amount = 0
    if amount <= 0:
        return "Price not available"
    if amount.is_integer():
        return f"PKR {int(amount):,}"
    return f"PKR {amount:,.2f}".rstrip("0").rstrip(".")


def _product_url(item: dict) -> str:
    if item.get("product_url"):
        return str(item.get("product_url") or "")
    handle = str(item.get("handle") or "").strip()
    return f"https://tickbags.com/products/{handle}" if handle else ""


def _product_details_reply(matches: list[dict], query: str, asking_price: bool = False) -> str:
    if not matches:
        return ""
    primary_title = str(matches[0].get("product_title") or matches[0].get("title") or "Product")
    variants = []
    seen = set()
    for item in matches:
        title = str(item.get("title") or primary_title)
        price = _format_money(item.get("price"))
        key = (title, price)
        if key in seen:
            continue
        seen.add(key)
        variants.append(f"- {title}: {price}")
        if len(variants) >= 3:
            break
    url = _product_url(matches[0])
    image = str(matches[0].get("image") or "").strip()
    intro = f"Here are the actual details I found for {primary_title}:"
    if asking_price:
        intro = f"The current price details I found for {primary_title}:"
    lines = [intro, *variants]
    if url:
        lines.append(f"Link: {url}")
    lines.append("Order karna ho to apna name, phone number, complete address, quantity aur color/variant share kar dein.")
    return "\n".join(lines)


def _order_help_reply(product_context: str = "") -> str:
    product_line = f" ({product_context})" if product_context else ""
    return (
        f"Order place karne ke liye{product_line} ye details send kar dein:\n"
        "- Name\n"
        "- Phone number\n"
        "- Complete address\n"
        "- Quantity\n"
        "- Color/variant\n"
        "Details milte hi team order create kar degi."
    )


def _contextual_followup_reply(body: str, product_context: str, settings: dict) -> str:
    text = _normalized_customer_text(body)
    subject = f"this product ({product_context})" if product_context else "this product"
    if any(word in text for word in ("material", "fabric", "which fabric")):
        return (
            f"For {subject}, I do not have the exact fabric name saved in our system yet. "
            "It is a durable upholstery-style fabric made for regular use. I can confirm the exact fabric/color option for you."
        )
    if any(word in text for word in ("filling", "filled with")):
        return f"For {subject}, the filling detail is not saved here yet. I can have the team confirm the exact filling before you order."
    if any(word in text for word in ("color", "colors")):
        return f"For {subject}, please share the color you prefer and I will check available options."
    if any(word in text for word in ("size", "dimensions")):
        return f"For {subject}, I can check the exact size. Please confirm if you want the standard size or a larger option."
    if any(word in text for word in ("price", "cost", "rate")):
        return settings.get("price_reply") or f"Please share the exact variant for {subject}, and I will confirm the latest price."
    if any(word in text for word in ("delivery", "deliver")):
        return settings.get("delivery_time_reply") or "Our usual delivery time is 3 to 7 working days depending on your city and order type."
    return ""


def heuristic_ai_decision(channel: str, body: str, settings: dict, messages: list[dict] | None = None, conversation: dict | None = None) -> dict:
    text = _normalized_customer_text(body)
    decision = dict(AI_DECISION_DEFAULT)
    decision.update({"needs_human": False, "labels": [], "confidence": 0.72, "should_auto_reply": False})
    delivery_reply = settings.get("delivery_time_reply") or "Our usual delivery time is 3 to 7 working days depending on your city and order type."
    price_reply = settings.get("price_reply") or "Please share the product name, picture, or link and I will confirm the latest price."
    payment_reply = settings.get("payment_method_reply") or "We mainly offer Cash on Delivery. Bank transfer can be arranged in special cases."
    product_context = _recent_product_context(messages, conversation)
    contextual_reply = _contextual_followup_reply(body, product_context, settings) if any(phrase in text for phrase in AI_FOLLOWUP_PHRASES) else ""
    asking_price = any(word in text for word in ("price", "cost", "rate", "kitna", "kitnay"))
    asking_availability = any(phrase in text for phrase in AI_AVAILABILITY_PHRASES)
    has_recent_image = _recent_image_context(messages)
    order_reference = extract_order_reference(body)
    referenced_order = find_order_by_reference(order_reference) if order_reference else None
    product_matches = _find_product_matches(body, messages=messages, conversation=conversation)
    product_reply = _product_details_reply(product_matches, body, asking_price=asking_price)
    order_help = any(phrase in text for phrase in AI_ORDER_HELP_PHRASES)
    if text.strip() in AI_GREETING_PHRASES:
        decision.update({
            "intent": "greeting",
            "confidence": 0.94,
            "should_auto_reply": True,
            "needs_human": False,
            "labels": ["ai"],
            "reply_text": "Hi! How can I help you with TickBags today?",
        })
    elif any(phrase in text for phrase in AI_SMALL_TALK_PHRASES):
        decision.update({
            "intent": "greeting",
            "confidence": 0.92,
            "should_auto_reply": True,
            "needs_human": False,
            "labels": ["ai"],
            "reply_text": "I am good, thank you. How can I help you with TickBags today?",
        })
    elif any(phrase in text for phrase in ("why don't you", "why dont you", "no reply", "reply?", "respond", "where are you")):
        decision.update({
            "intent": "greeting",
            "confidence": 0.88,
            "should_auto_reply": True,
            "needs_human": False,
            "labels": ["ai"],
            "reply_text": "Sorry for the delay. I am here now. How can I help you with your TickBags order or product?",
        })
    elif "[image]" in text or text == "image":
        if _recent_availability_request(messages):
            decision.update({
                "intent": "product_question",
                "confidence": 0.80,
                "should_auto_reply": False,
                "needs_human": False,
                "labels": ["product_interest", "image_received", "unclear_product"],
                "reply_text": "",
                "order_state": "collecting_details",
                "missing_order_fields": ["product"],
                "internal_note": "Image followed a recent availability question; skipped duplicate auto reply.",
            })
            return normalize_ai_decision(decision)
        decision.update({
            "intent": "product_question",
            "labels": ["unclear_product", "image_received"],
            "reply_text": _image_availability_reply(),
            "order_state": "collecting_details",
            "missing_order_fields": ["product"],
        })
    elif any(word in text for word in AI_RISK_PHRASES):
        decision.update({
            "intent": "damaged_item" if "damage" in text or "damaged" in text else "refund_exchange",
            "needs_human": True,
            "should_auto_reply": False,
            "labels": ["refund" if "refund" in text else "complaint", "damaged_item" if "damage" in text or "damaged" in text else "needs_human"],
            "reply_text": settings.get("escalation_reply") or settings.get("human_handoff_reply") or "",
            "escalation_reason": "Complaint/refund/damaged item requires a human.",
        })
    elif any(word in text for word in AI_OUT_OF_SCOPE_PHRASES):
        decision.update({
            "intent": "out_of_scope",
            "needs_human": True,
            "should_auto_reply": False,
            "labels": ["needs_human", "outside_policy"],
            "reply_text": settings.get("escalation_reply") or settings.get("human_handoff_reply") or "",
            "escalation_reason": "Message appears outside TickBags sales/support scope.",
        })
    elif any(phrase in text for phrase in ("who are you", "what are you", "ap kon", "aap kon", "who is this")):
        decision.update({
            "intent": "greeting",
            "confidence": 0.90,
            "should_auto_reply": True,
            "needs_human": False,
            "labels": ["ai"],
            "reply_text": "I am from TickBags support. Share the product you like or your order question and I will help.",
        })
    elif referenced_order:
        decision.update({
            "intent": "order_tracking",
            "confidence": 0.95,
            "should_auto_reply": True,
            "needs_human": False,
            "labels": ["order_tracking"],
            "reply_text": order_summary(referenced_order),
        })
    elif order_reference:
        decision.update({
            "intent": "order_tracking",
            "confidence": 0.88,
            "should_auto_reply": True,
            "needs_human": False,
            "labels": ["order_tracking"],
            "reply_text": "Please share the phone number used for the order as well, and I will check the status for you.",
        })
    elif product_reply:
        decision.update({
            "intent": "price_question" if asking_price else "product_question",
            "confidence": 0.90,
            "should_auto_reply": True,
            "needs_human": False,
            "labels": ["product_interest"] + (["price_question"] if asking_price else []),
            "reply_text": product_reply,
            "order_state": "collecting_details",
            "order_candidate": {"product": str(product_matches[0].get("product_title") or product_matches[0].get("title") or "")},
            "product_attachment_url": str(product_matches[0].get("image") or ""),
        })
    elif asking_availability and has_recent_image:
        decision.update({
            "intent": "product_question",
            "confidence": 0.84,
            "should_auto_reply": True,
            "needs_human": False,
            "labels": ["product_interest", "image_received", "unclear_product"],
            "reply_text": _image_availability_reply(),
            "order_state": "collecting_details",
            "missing_order_fields": ["product"],
        })
    elif asking_availability:
        decision.update({
            "intent": "product_question",
            "confidence": 0.82,
            "should_auto_reply": True,
            "needs_human": False,
            "labels": ["product_interest"],
            "reply_text": _availability_without_product_reply(),
            "order_state": "collecting_details",
            "missing_order_fields": ["product"],
        })
    elif order_help:
        decision.update({
            "intent": "order_intent",
            "confidence": 0.88,
            "should_auto_reply": True,
            "needs_human": False,
            "labels": ["product_interest", "collecting_details"],
            "reply_text": _order_help_reply(product_context),
            "order_state": "collecting_details",
            "missing_order_fields": ["name", "phone", "address", "quantity", "variant"],
        })
    elif contextual_reply:
        decision.update({
            "intent": "product_question",
            "confidence": 0.86,
            "should_auto_reply": True,
            "needs_human": False,
            "labels": ["product_interest"],
            "reply_text": contextual_reply,
            "order_state": "collecting_details",
        })
    elif any(word in text for word in ("price", "cost", "rate", "kitna", "kitnay")):
        decision.update({"intent": "price_question", "confidence": 0.86, "labels": ["price_question"], "reply_text": price_reply, "should_auto_reply": True})
    elif any(word in text for word in ("delivery", "deliver", "kitne din", "days")):
        decision.update({"intent": "delivery_question", "confidence": 0.88, "labels": ["delivery_question"], "reply_text": delivery_reply, "should_auto_reply": True})
    elif any(word in text for word in ("payment", "cod", "cash", "bank", "advance")):
        decision.update({"intent": "payment_question", "confidence": 0.86, "labels": ["payment_question"], "reply_text": payment_reply, "should_auto_reply": True})
    elif any(phrase in text for phrase in AI_LOCATION_PHRASES):
        decision.update({
            "intent": "delivery_question",
            "confidence": 0.82,
            "should_auto_reply": True,
            "needs_human": False,
            "labels": ["delivery_question"],
            "reply_text": "We are based in Pakistan. Please share your city and the product you want, and I will guide you with the next step.",
        })
    elif any(phrase in text for phrase in AI_SUGGESTION_PHRASES):
        decision.update({
            "intent": "product_question",
            "confidence": 0.82,
            "should_auto_reply": True,
            "needs_human": False,
            "labels": ["product_interest"],
            "reply_text": "Ji bilkul. TickBags mein bean bags, ottomans, floor cushions, aur home decor options available hain. Aap kis product ke baare mein details chahte hain, ya product ka screenshot/link share kar dein.",
            "order_state": "collecting_details",
        })
    elif any(word in text for word in ("order", "buy", "want this", "address", "phone", "name")):
        candidate = {}
        if "address" in text or "lahore" in text or "karachi" in text:
            candidate["address"] = body
        decision.update({
            "intent": "order_intent",
            "labels": ["product_interest", "collecting_details"],
            "reply_text": _order_help_reply(product_context),
            "order_state": "collecting_details",
            "order_candidate": candidate,
            "missing_order_fields": ["product", "name", "phone", "address"],
            "should_auto_reply": True,
        })
    else:
        decision.update({
            "intent": "product_question",
            "confidence": 0.76,
            "should_auto_reply": True,
            "needs_human": False,
            "labels": ["ai", "product_interest"],
            "reply_text": "Please share the product name, picture, or link, and tell me what you want to know.",
            "order_state": "collecting_details",
        })
    return normalize_ai_decision(decision)


def strengthen_safe_ai_decision(body: str, decision: dict, settings: dict, messages: list[dict] | None = None, conversation: dict | None = None) -> dict:
    text = _normalized_customer_text(body)
    simple_greetings = set(AI_GREETING_PHRASES)
    product_context = _recent_product_context(messages, conversation)
    contextual_reply = _contextual_followup_reply(body, product_context, settings) if any(phrase in text for phrase in AI_FOLLOWUP_PHRASES) else ""
    asking_price = any(word in text for word in ("price", "cost", "rate", "kitna", "kitnay"))
    asking_availability = any(phrase in text for phrase in AI_AVAILABILITY_PHRASES)
    has_recent_image = _recent_image_context(messages)
    product_matches = _find_product_matches(body, messages=messages, conversation=conversation)
    product_reply = _product_details_reply(product_matches, body, asking_price=asking_price)
    order_help = any(phrase in text for phrase in AI_ORDER_HELP_PHRASES)
    default_unavailable = (
        str(decision.get("escalation_reason") or "").strip().lower() == "ai decision unavailable."
        and not decision.get("reply_text")
    )
    soft_unknown_escalation = (
        decision.get("needs_human")
        and str(decision.get("intent") or "").lower() in {"unknown", "product_question", "greeting"}
        and not any(word in text for word in AI_RISK_PHRASES + AI_OUT_OF_SCOPE_PHRASES)
        and not any(label in {"refund", "complaint", "damaged_item", "payment_issue", "outside_policy"} for label in decision.get("labels") or [])
    )
    safe_sales_question = any(
        phrase in text
        for phrase in AI_LOCATION_PHRASES + AI_SUGGESTION_PHRASES + (
            "delivery", "deliver", "kitne din", "price", "cost", "rate", "payment", "cod", "cash",
        )
    )
    if (default_unavailable or soft_unknown_escalation or (decision.get("needs_human") and safe_sales_question)) and not any(word in text for word in AI_RISK_PHRASES + AI_OUT_OF_SCOPE_PHRASES):
        decision = heuristic_ai_decision("", body, settings, messages=messages, conversation=conversation)
    if text in simple_greetings:
        decision.update({
            "intent": "greeting",
            "confidence": max(float(decision.get("confidence") or 0), 0.94),
            "should_auto_reply": True,
            "needs_human": False,
            "labels": sorted(set((decision.get("labels") or []) + ["ai"])),
            "reply_text": decision.get("reply_text") or "Hi! How can I help you with TickBags today?",
            "escalation_reason": "",
        })
    elif any(phrase in text for phrase in AI_SMALL_TALK_PHRASES):
        decision.update({
            "intent": "greeting",
            "confidence": max(float(decision.get("confidence") or 0), 0.92),
            "should_auto_reply": True,
            "needs_human": False,
            "labels": sorted(set((decision.get("labels") or []) + ["ai"])),
            "reply_text": "I am good, thank you. How can I help you with TickBags today?",
            "escalation_reason": "",
        })
    elif product_reply and (
        asking_price
        or str(decision.get("intent") or "").lower() in {"product_question", "price_question"}
        or not decision.get("reply_text")
        or "share the product name" in str(decision.get("reply_text") or "").lower()
        or "varies by size" in str(decision.get("reply_text") or "").lower()
        or decision.get("needs_human")
    ):
        decision.update({
            "intent": "price_question" if asking_price else "product_question",
            "confidence": max(float(decision.get("confidence") or 0), 0.90),
            "should_auto_reply": True,
            "needs_human": False,
            "labels": sorted(set((decision.get("labels") or []) + ["product_interest"] + (["price_question"] if asking_price else []))),
            "reply_text": product_reply,
            "order_state": decision.get("order_state") if decision.get("order_state") != "no_order" else "collecting_details",
            "order_candidate": {"product": str(product_matches[0].get("product_title") or product_matches[0].get("title") or "")},
            "product_attachment_url": str(product_matches[0].get("image") or ""),
            "escalation_reason": "",
        })
    elif asking_availability and has_recent_image and (
        not decision.get("reply_text")
        or "share the product name" in str(decision.get("reply_text") or "").lower()
        or decision.get("needs_human")
    ):
        decision.update({
            "intent": "product_question",
            "confidence": max(float(decision.get("confidence") or 0), 0.84),
            "should_auto_reply": True,
            "needs_human": False,
            "labels": sorted(set((decision.get("labels") or []) + ["product_interest", "image_received", "unclear_product"])),
            "reply_text": _image_availability_reply(),
            "order_state": "collecting_details",
            "missing_order_fields": ["product"],
            "escalation_reason": "",
        })
    elif asking_availability and (
        not decision.get("reply_text")
        or "share the product name" in str(decision.get("reply_text") or "").lower()
        or decision.get("needs_human")
    ):
        decision.update({
            "intent": "product_question",
            "confidence": max(float(decision.get("confidence") or 0), 0.82),
            "should_auto_reply": True,
            "needs_human": False,
            "labels": sorted(set((decision.get("labels") or []) + ["product_interest"])),
            "reply_text": _availability_without_product_reply(),
            "order_state": "collecting_details",
            "missing_order_fields": ["product"],
            "escalation_reason": "",
        })
    elif order_help and (
        not decision.get("reply_text")
        or "provide your full name" in str(decision.get("reply_text") or "").lower()
        or "complete delivery address" in str(decision.get("reply_text") or "").lower()
        or decision.get("needs_human")
    ):
        decision.update({
            "intent": "order_intent",
            "confidence": max(float(decision.get("confidence") or 0), 0.88),
            "should_auto_reply": True,
            "needs_human": False,
            "labels": sorted(set((decision.get("labels") or []) + ["product_interest", "collecting_details"])),
            "reply_text": _order_help_reply(product_context),
            "order_state": "collecting_details",
            "missing_order_fields": ["name", "phone", "address", "quantity", "variant"],
            "escalation_reason": "",
        })
    elif contextual_reply and (
        not decision.get("reply_text")
        or "share the product name" in str(decision.get("reply_text") or "").lower()
        or decision.get("needs_human")
    ):
        decision.update({
            "intent": "product_question",
            "confidence": max(float(decision.get("confidence") or 0), 0.86),
            "should_auto_reply": True,
            "needs_human": False,
            "labels": sorted(set((decision.get("labels") or []) + ["product_interest"])),
            "reply_text": contextual_reply,
            "order_state": decision.get("order_state") if decision.get("order_state") != "no_order" else "collecting_details",
            "escalation_reason": "",
        })
    elif any(phrase in text for phrase in ("why don't you", "why dont you", "no reply", "reply?", "respond")):
        decision.update({
            "intent": "greeting",
            "confidence": max(float(decision.get("confidence") or 0), 0.88),
            "should_auto_reply": True,
            "needs_human": False,
            "labels": sorted(set((decision.get("labels") or []) + ["ai"])),
            "reply_text": decision.get("reply_text") or "Sorry for the delay. I am here now. How can I help you with your TickBags order or product?",
            "escalation_reason": "",
        })
    elif any(phrase in text for phrase in ("who are you", "what are you", "ap kon", "aap kon", "who is this")):
        decision.update({
            "intent": "greeting",
            "confidence": max(float(decision.get("confidence") or 0), 0.90),
            "should_auto_reply": True,
            "needs_human": False,
            "labels": sorted(set((decision.get("labels") or []) + ["ai"])),
            "reply_text": decision.get("reply_text") or "I am from TickBags support. Share the product you like or your order question and I will help.",
            "escalation_reason": "",
        })
    elif decision.get("needs_human") and not decision.get("reply_text"):
        decision["reply_text"] = settings.get("escalation_reply") or settings.get("human_handoff_reply") or "Let me have our team check this and reply with the correct details."
    if str(decision.get("intent") or "") == "order_tracking":
        decision.pop("product_attachment_url", None)
    return normalize_ai_decision(decision)


def guardrail_gpt_decision(body: str, decision: dict, settings: dict, messages: list[dict] | None = None, conversation: dict | None = None) -> dict:
    """Keep GPT in charge of wording while enforcing business safety rules."""
    text = _normalized_customer_text(body)
    decision = normalize_ai_decision(decision)
    labels = set(decision.get("labels") or [])
    risk_hit = any(word in text for word in AI_RISK_PHRASES)
    outside_scope = any(word in text for word in AI_OUT_OF_SCOPE_PHRASES)
    broad_category_availability = _is_broad_category_availability(text)
    tracking_request = any(word in text for word in ("track", "tracking", "order status", "status")) or bool(extract_order_reference(body))
    if risk_hit or outside_scope:
        labels.add("needs_human")
        if risk_hit:
            labels.add("complaint")
        if outside_scope:
            labels.add("outside_policy")
        decision.update({
            "needs_human": True,
            "should_auto_reply": False,
            "labels": sorted(labels),
            "escalation_reason": decision.get("escalation_reason") or "Message requires human review.",
            "reply_text": settings.get("escalation_reply") or settings.get("human_handoff_reply") or "Let me have our team check this and reply with the correct details.",
        })
        return normalize_ai_decision(decision)

    order_reference = extract_order_reference(body)
    referenced_order = find_order_by_reference(order_reference) if order_reference else None
    if referenced_order:
        decision.update({
            "intent": "order_tracking",
            "needs_human": False,
            "should_auto_reply": True,
            "confidence": max(float(decision.get("confidence") or 0), 0.92),
            "labels": sorted(labels | {"order_tracking"}),
            "reply_text": order_summary(referenced_order),
            "escalation_reason": "",
        })
        return normalize_ai_decision(decision)

    if tracking_request and decision.get("needs_human"):
        decision.update({
            "intent": "order_tracking",
            "needs_human": False,
            "should_auto_reply": True,
            "confidence": max(float(decision.get("confidence") or 0), 0.80),
            "labels": sorted(labels | {"order_tracking"}),
            "reply_text": "Please share your order number or the phone number used for the order, and I will check the status for you.",
            "escalation_reason": "",
        })
        return normalize_ai_decision(decision)

    reply_lower = str(decision.get("reply_text") or "").lower()
    handoff_reply = (
        "team check" in reply_lower
        or "correct details" in reply_lower
        or "reply shortly" in reply_lower
        or "human" in reply_lower and "check" in reply_lower
    )
    if broad_category_availability and (decision.get("needs_human") or handoff_reply or not decision.get("reply_text")):
        decision.update({
            "intent": "product_question",
            "needs_human": False,
            "should_auto_reply": True,
            "confidence": max(float(decision.get("confidence") or 0), 0.82),
            "labels": sorted(labels | {"ai", "product_interest"}),
            "reply_text": _broad_category_availability_reply(text),
            "order_state": "collecting_details",
            "escalation_reason": "",
        })
        return normalize_ai_decision(decision)

    if decision.get("reply_text") and not decision.get("needs_human"):
        decision["should_auto_reply"] = True
        decision["confidence"] = max(float(decision.get("confidence") or 0), 0.72)

    if decision.get("needs_human") and not decision.get("reply_text"):
        decision["reply_text"] = settings.get("escalation_reply") or settings.get("human_handoff_reply") or "Let me have our team check this and reply with the correct details."

    if "order confirmed" in reply_lower and str((conversation or {}).get("order_state") or "") != "order_added":
        decision.update({
            "needs_human": True,
            "should_auto_reply": False,
            "labels": sorted(labels | {"needs_human", "pending_human_order_creation"}),
            "escalation_reason": "GPT attempted to confirm an order before human/Shopify confirmation.",
            "reply_text": settings.get("escalation_reply") or "Let me have our team check this and reply with the correct details.",
        })
        return normalize_ai_decision(decision)

    product_matches = _find_product_matches(body, messages=messages, conversation=conversation)
    if product_matches:
        labels.discard("unclear_product")
        decision["product_attachment_url"] = str(product_matches[0].get("image") or "")
        order_candidate = decision.get("order_candidate") if isinstance(decision.get("order_candidate"), dict) else {}
        order_candidate.setdefault("product", str(product_matches[0].get("product_title") or product_matches[0].get("title") or ""))
        order_candidate.setdefault("product_link", _product_url(product_matches[0]))
        decision["order_candidate"] = order_candidate
        decision["labels"] = sorted(labels | {"product_interest"})

    asking_price = any(word in text for word in ("price", "cost", "rate", "kitna", "kitnay"))
    if asking_price and "pkr" in reply_lower and not product_matches:
        decision.update({
            "needs_human": False,
            "should_auto_reply": True,
            "confidence": max(float(decision.get("confidence") or 0), 0.72),
            "labels": sorted(labels | {"price_question"}),
            "reply_text": settings.get("price_reply") or "Please share the product name, picture, or link and I will confirm the latest price.",
        })
    if str(decision.get("intent") or "") == "order_tracking":
        decision.pop("product_attachment_url", None)

    return normalize_ai_decision(decision)


def analyze_inbound_message(channel: str, contact_key: str, body: str, conversation: dict | None = None) -> dict:
    settings = get_whatsapp_settings()
    conversation = conversation or get_conversation_by_any_key(contact_key) or {}
    messages = list_inbox_messages(contact_key, limit=30) if conversation else []
    orders = customer_orders_for_conversation(conversation)
    order_reference = extract_order_reference(body)
    referenced_order = find_order_by_reference(order_reference) if order_reference else None

    if referenced_order:
        return normalize_ai_decision({
            "intent": "order_tracking",
            "confidence": 0.98,
            "should_auto_reply": True,
            "needs_human": False,
            "labels": ["order_tracking"],
            "reply_text": order_summary(referenced_order),
            "order_state": str((conversation or {}).get("order_state") or "no_order"),
            "order_candidate": {},
            "missing_order_fields": [],
            "escalation_reason": "",
            "knowledge_gap_question": "",
            "internal_note": "",
        })
    if order_reference:
        return normalize_ai_decision({
            "intent": "order_tracking",
            "confidence": 0.90,
            "should_auto_reply": True,
            "needs_human": False,
            "labels": ["order_tracking"],
            "reply_text": "Apna order wala phone number bhi share kar dein, main status check kar deta hoon.",
            "order_state": str((conversation or {}).get("order_state") or "no_order"),
            "order_candidate": {},
            "missing_order_fields": [],
            "escalation_reason": "",
            "knowledge_gap_question": "",
            "internal_note": "",
        })

    knowledge_matches = search_tickbot_knowledge(body, limit=3)
    top_knowledge = knowledge_matches[0] if knowledge_matches else None
    if top_knowledge and float(top_knowledge.get("_score") or 0) >= 0.45:
        return normalize_ai_decision({
            "intent": "product_question" if any(word in _normalized_customer_text(body) for word in ("product", "price", "available", "delivery")) else "unknown",
            "confidence": min(0.94, max(0.78, float(top_knowledge.get("_score") or 0))),
            "should_auto_reply": True,
            "needs_human": False,
            "labels": ["ai"],
            "reply_text": str(top_knowledge.get("content") or "").strip(),
            "order_state": str((conversation or {}).get("order_state") or "no_order"),
            "order_candidate": _safe_json_dict((conversation or {}).get("order_candidate")),
            "missing_order_fields": [],
            "escalation_reason": "",
            "knowledge_gap_question": "",
            "internal_note": f"Answered from TickBot knowledge: {top_knowledge.get('title') or 'assistant memory'}",
        })

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return strengthen_safe_ai_decision(
            body,
            heuristic_ai_decision(channel, body, settings, messages=messages, conversation=conversation),
            settings,
            messages=messages,
            conversation=conversation,
        )
    context = build_ai_context(conversation, messages, orders, settings)
    context["referenced_order"] = referenced_order or {}
    product_matches = _find_product_matches(body, messages=messages, conversation=conversation, limit=5)
    context["matched_products"] = [
        {
            "title": item.get("title"),
            "product_title": item.get("product_title"),
            "variant_title": item.get("variant_title"),
            "price": _format_money(item.get("price")),
            "available": item.get("available"),
            "product_url": _product_url(item),
            "image": item.get("image"),
        }
        for item in product_matches
    ]
    system_prompt = (
        "You are TickBot, a warm, human-sounding chat support and sales representative for TickBags.com. "
        "Your core purpose is to help customers choose products, place orders, track orders, and get safe support. "
        "Return only valid JSON. Never mention being AI. Reply in the customer's language when possible: English, Urdu, or Roman Urdu. "
        "Use Pakistani Urdu / Roman Urdu phrasing when replying in Urdu. Avoid Hindi-leaning wording like 'kripya' or overly formal Indian diction. "
        "Talk naturally like a good customer support rep. Basic greetings, small talk, and style/color recommendations are allowed. "
        "If conversation.ai_mode is needs_human but the customer starts a fresh greeting or simple sales/support question, it is safe to respond normally and set needs_human=false. "
        "Do not force every message into product collection. If the customer is just greeting or chatting, respond warmly and briefly. "
        "TickBags sells bean bags, ottomans, floor cushions, and home decor. For broad category questions like 'bean bags available?', it is safe to say yes and ask the customer for color/size or a product picture/link. "
        "If context.knowledge_matches contains a close reusable answer, prefer it instead of escalating. "
        "When the customer asks for product details, price, stock, image, or link, use matched_products/product context only. "
        "If matched_products is empty for exact price, exact stock, exact variant, or exact product details, ask for the product name, link, screenshot, or picture instead of inventing facts. "
        "Collect order details only when the customer shows buying intent: product, name, phone, complete address, quantity, and variant/color. "
        "For order tracking, if referenced_order is present, answer using that exact order status and tracking number. If it is missing, ask for order number or order phone. "
        "Never invent prices, availability, discounts, delivery promises, tracking, refund policy, or order status. "
        "Never say an order is confirmed unless order_state is already order_added or a real order exists in recent_orders. "
        "If complete order details are present, set order_state=new_order and label new_order, but do not tell the customer it is confirmed. "
        "Escalate angry customers, refunds, damaged items, payment issues, legal threats, sensitive issues, or anything outside TickBags/products/orders/support."
    )
    schema_hint = {
        "intent": "greeting|product_question|price_question|delivery_question|payment_question|order_tracking|order_intent|order_details_provided|complaint|refund_exchange|damaged_item|payment_issue|discount_negotiation|out_of_scope|unknown",
        "confidence": 0.0,
        "should_auto_reply": False,
        "needs_human": False,
        "labels": [],
        "reply_text": "",
        "order_state": "no_order|collecting_details|new_order|pending_human_order_creation|order_added|cancelled",
        "order_candidate": {"product": "", "product_link": "", "product_image_reference": "", "customer_name": "", "phone": "", "address": "", "quantity": "", "variant": "", "notes": ""},
        "missing_order_fields": [],
        "escalation_reason": "",
        "knowledge_gap_question": "",
        "internal_note": "",
    }
    try:
        response = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={
                "model": settings.get("openai_model", "gpt-4.1-mini"),
                "temperature": 0.45,
                "response_format": {"type": "json_object"},
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": json.dumps({
                        "schema": schema_hint,
                        "channel": channel,
                        "latest_customer_message": body,
                        "context": context,
                    }, ensure_ascii=False, default=_json_default)},
                ],
            },
            timeout=30,
        )
        data = response.json() if response.content else {}
        if response.ok:
            content = (((data.get("choices") or [{}])[0].get("message") or {}).get("content") or "{}").strip()
            return guardrail_gpt_decision(
                body,
                normalize_ai_decision(json.loads(content)),
                settings,
                messages=messages,
                conversation=conversation,
            )
    except Exception as e:
        safe = heuristic_ai_decision(channel, body, settings, messages=messages, conversation=conversation)
        safe["internal_note"] = f"AI provider failed safely: {e}"
        return strengthen_safe_ai_decision(body, safe, settings, messages=messages, conversation=conversation)
    return strengthen_safe_ai_decision(
        body,
        heuristic_ai_decision(channel, body, settings, messages=messages, conversation=conversation),
        settings,
        messages=messages,
        conversation=conversation,
    )


def should_auto_send_ai_reply(conversation: dict, decision: dict, settings: dict) -> bool:
    labels = set(_safe_json_list(conversation.get("labels"))) | set(decision.get("labels") or [])
    mode = str(conversation.get("ai_mode") or "human").lower()
    global_allowed = settings.get("global_ai_mode_enabled") and mode != "needs_human"
    if mode != "auto" and not global_allowed:
        return False
    if conversation.get("manual_lock") or labels.intersection(AI_BLOCKING_LABELS):
        return False
    if decision.get("needs_human") or not decision.get("should_auto_reply"):
        return False
    if float(decision.get("confidence") or 0) < 0.70 or not decision.get("reply_text"):
        return False
    if settings.get("auto_reply_inside_service_window_only", True) and not service_window_open(conversation):
        return False
    return True


def apply_ai_decision(conversation: dict, decision: dict) -> None:
    save_ai_decision(conversation["phone"], decision)
    labels = list(decision.get("labels") or [])
    remove_labels = []
    if decision.get("needs_human"):
        customer_message = str(decision.get("_customer_message") or "").strip()
        reason_text = str(decision.get("escalation_reason") or "").strip()
        if _is_simple_safe_restart(customer_message) and reason_text.lower() == "ai decision unavailable.":
            return
        mark_conversation_needs_human(conversation["phone"], decision.get("escalation_reason") or "Needs human review", labels=labels)
        assistant_prompt = (
            f"{conversation.get('public_chat_id') or conversation.get('phone')} needs human help.\n"
            f"Customer asked: {customer_message or 'Review latest inbound message.'}\n"
            f"Reason: {decision.get('escalation_reason') or decision.get('knowledge_gap_question') or decision.get('internal_note') or 'Needs human review.'}\n"
            f"Reply here with: Answer {conversation.get('public_chat_id') or conversation.get('phone')}: <your reply>\n"
            f"To teach a general reusable answer, use: Teach: <question> => <answer>"
        )
        save_internal_assistant_message(
            assistant_prompt,
            metadata={
                "source": "ai_escalation",
                "conversation": conversation.get("phone"),
                "public_chat_id": conversation.get("public_chat_id"),
                "customer_message": customer_message,
                "decision": decision,
            },
        )
        return
    if decision.get("order_state") == "new_order":
        mark_new_order(conversation["phone"], decision.get("order_candidate") or {})
        return
    if decision.get("order_state") in {"collecting_details", "pending_human_order_creation"}:
        update_order_candidate(conversation["phone"], decision.get("order_state"), decision.get("order_candidate") or {})
    order_candidate = decision.get("order_candidate") if isinstance(decision.get("order_candidate"), dict) else {}
    if order_candidate.get("product") or "product_interest" in labels or str(decision.get("intent") or "") in {"product_question", "order_intent", "price_question"}:
        remove_labels.append("unclear_product")
    if labels:
        update_conversation_labels(conversation["phone"], add=labels, remove=remove_labels)
    elif remove_labels:
        update_conversation_labels(conversation["phone"], remove=remove_labels)


def ai_reply_for_message(channel: str, contact_key: str, body: str, conversation: dict | None = None) -> str:
    settings = get_whatsapp_settings()
    if not settings.get("ai_mode_enabled"):
        return ""
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return ""

    contact_phone = ""
    if conversation:
        contact_phone = conversation.get("contact_phone") or ""
    phone_for_lookup = contact_phone or contact_key
    order_context = customer_orders_for_phone(phone_for_lookup)[:3]
    latest_context = latest_order_summary(phone_for_lookup)
    system_prompt = (
        "You are a warm, human-sounding customer support agent for TickBags. "
        "Reply briefly, naturally, and helpfully. Never mention being AI. "
        "Do not invent unavailable discounts, shipping promises, or order details. "
        "If specific order information is known, use it. If not known, say you'll confirm shortly. "
        f"Business facts: {settings.get('business_facts', '')}"
    )
    user_prompt = {
        "channel": channel,
        "customer_name": (conversation or {}).get("customer_name", ""),
        "contact": (conversation or {}).get("display_handle") or contact_key,
        "customer_message": body,
        "latest_order_summary": latest_context,
        "recent_orders": order_context,
        "safe_defaults": {
            "delivery_time": settings.get("delivery_time_reply"),
            "payment_method": settings.get("payment_method_reply"),
            "price": settings.get("price_reply"),
        },
    }
    try:
        response = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": settings.get("openai_model", "gpt-4.1-mini"),
                "temperature": 0.5,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": json.dumps(user_prompt, ensure_ascii=False)},
                ],
            },
            timeout=30,
        )
        data = response.json() if response.content else {}
        if response.ok:
            choices = data.get("choices") or []
            if choices:
                content = (((choices[0] or {}).get("message") or {}).get("content") or "").strip()
                return content
    except Exception:
        return ""
    return ""


def send_meta_text_message(phone: str, body: str) -> tuple[bool, str, dict]:
    token = get_whatsapp_access_token()
    phone_number_id = get_whatsapp_phone_number_id()
    api_version = os.getenv("META_GRAPH_API_VERSION", "v20.0")

    if not token or not phone_number_id:
        return False, "Meta WhatsApp credentials are not configured.", {}

    url = f"https://graph.facebook.com/{api_version}/{phone_number_id}/messages"
    payload = {
        "messaging_product": "whatsapp",
        "to": clean_digits(phone),
        "type": "text",
        "text": {"preview_url": False, "body": body},
    }
    try:
        response = requests.post(
            url,
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json=payload,
            timeout=25,
        )
        data = response.json() if response.content else {}
        if response.ok:
            message_id = ""
            messages = data.get("messages") or []
            if messages:
                message_id = messages[0].get("id") or ""
            return True, message_id, data
        return False, meta_error_message(data, response.text), data
    except Exception as e:
        return False, str(e), {}


def send_facebook_text_message(recipient_id: str, body: str, channel: str = "facebook") -> tuple[bool, str, dict]:
    token = os.getenv("META_PAGE_ACCESS_TOKEN") or os.getenv("FACEBOOK_PAGE_ACCESS_TOKEN")
    api_version = os.getenv("META_GRAPH_API_VERSION", "v20.0")
    recipient_id = str(recipient_id or "").strip()
    if not token or not recipient_id:
        return False, "Meta Page access token or recipient id is missing.", {}

    url = f"https://graph.facebook.com/{api_version}/me/messages"
    payload = {
        "recipient": {"id": recipient_id},
        "messaging_type": "RESPONSE",
        "message": {"text": body},
    }
    if channel == "instagram":
        payload["messaging_type"] = "RESPONSE"

    try:
        response = requests.post(
            url,
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json=payload,
            timeout=25,
        )
        data = response.json() if response.content else {}
        if response.ok:
            return True, data.get("message_id") or "", data
        return False, meta_error_message(data, response.text), data
    except Exception as e:
        return False, str(e), {}


def send_channel_image_message(channel: str, contact_key: str, image_url: str, caption: str = "") -> tuple[bool, str, dict]:
    channel = str(channel or "whatsapp").strip().lower() or "whatsapp"
    image_url = str(image_url or "").strip()
    caption = str(caption or "").strip()
    if not image_url:
        return False, "Image URL missing.", {}
    if channel == "whatsapp":
        token = get_whatsapp_access_token()
        phone_number_id = get_whatsapp_phone_number_id()
        api_version = os.getenv("META_GRAPH_API_VERSION", "v20.0")
        if not token or not phone_number_id:
            return False, "Meta WhatsApp credentials are not configured.", {}
        url = f"https://graph.facebook.com/{api_version}/{phone_number_id}/messages"
        payload = {
            "messaging_product": "whatsapp",
            "to": clean_digits(contact_key),
            "type": "image",
            "image": {"link": image_url},
        }
        if caption:
            payload["image"]["caption"] = caption
    else:
        token = os.getenv("META_PAGE_ACCESS_TOKEN") or os.getenv("FACEBOOK_PAGE_ACCESS_TOKEN")
        api_version = os.getenv("META_GRAPH_API_VERSION", "v20.0")
        recipient_id = str(contact_key or "")
        if ":" in recipient_id:
            recipient_id = recipient_id.split(":", 1)[1]
        if not token or not recipient_id:
            return False, "Meta Page access token or recipient id is missing.", {}
        url = f"https://graph.facebook.com/{api_version}/me/messages"
        payload = {
            "recipient": {"id": recipient_id},
            "messaging_type": "RESPONSE",
            "message": {
                "attachment": {
                    "type": "image",
                    "payload": {"url": image_url, "is_reusable": True},
                }
            },
        }

    try:
        response = requests.post(
            url,
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json=payload,
            timeout=25,
        )
        data = response.json() if response.content else {}
        if response.ok:
            message_id = data.get("message_id") or ""
            messages = data.get("messages") or []
            if messages and not message_id:
                message_id = messages[0].get("id") or ""
            return True, message_id, data
        return False, meta_error_message(data, response.text), data
    except Exception as e:
        return False, str(e), {}


def send_channel_text_message(channel: str, contact_key: str, body: str) -> tuple[bool, str, dict]:
    channel = str(channel or "whatsapp").strip().lower() or "whatsapp"
    if channel == "whatsapp":
        return send_meta_text_message(contact_key, body)
    recipient_id = str(contact_key or "")
    if ":" in recipient_id:
        recipient_id = recipient_id.split(":", 1)[1]
    return send_facebook_text_message(recipient_id, body, channel=channel)


def maybe_auto_reply(channel: str, contact_key: str, body: str, customer_name: str = "", display_handle: str = "", contact_phone: str = ""):
    settings = get_whatsapp_settings()
    conversation = get_conversation_by_any_key(contact_key)
    per_chat_mode = str((conversation or {}).get("ai_mode") or "human").lower()
    per_chat_ai_requested = per_chat_mode in {"suggest", "auto"}
    if not settings.get("auto_handle_enabled", True) and not per_chat_ai_requested:
        return
    if conversation and conversation.get("manual_lock"):
        return
    if conversation and conversation.get("ai_mode") == "needs_human":
        conversation = maybe_resume_needs_human(conversation, contact_key, body)
        if conversation and conversation.get("ai_mode") == "needs_human":
            return
        per_chat_mode = str((conversation or {}).get("ai_mode") or "human").lower()
        per_chat_ai_requested = per_chat_mode in {"suggest", "auto"}
    reply, rule = find_keyword_reply(body)
    if per_chat_ai_requested or settings.get("ai_mode_enabled") or settings.get("global_ai_mode_enabled"):
        recent_messages = list_inbox_messages(contact_key, limit=30) if conversation else []
        decision = analyze_inbound_message(channel, contact_key, body, conversation=conversation)
        decision["_customer_message"] = str(body or "").strip()
        if (
            decision.get("needs_human")
            and str(decision.get("escalation_reason") or "").strip().lower() == "ai decision unavailable."
            and _is_simple_safe_restart(body)
        ):
            decision = strengthen_safe_ai_decision(
                body,
                heuristic_ai_decision(channel, body, settings, messages=recent_messages, conversation=conversation),
                settings,
                messages=recent_messages,
                conversation=conversation,
            )
            decision["_customer_message"] = str(body or "").strip()
        conversation = get_conversation_by_any_key(contact_key) or conversation
        if conversation:
            apply_ai_decision(conversation, decision)
            conversation = get_conversation_by_any_key(contact_key) or conversation
            if should_auto_send_ai_reply(conversation, decision, settings):
                reply = decision.get("reply_text") or ""
                rule = {"id": "ai-decision", "hold_for_review": False}
            elif decision.get("needs_human") and decision.get("reply_text") and service_window_open(conversation):
                reply = decision.get("reply_text")
                rule = {"id": "ai-handoff", "hold_for_review": False}
            else:
                return
    elif not reply:
        reply = fallback_reply(contact_phone or contact_key, body)
    if reply and not (rule and rule.get("hold_for_review")):
        rendered_reply = render_template_body(reply, contact_phone or contact_key)
        product_image_url = str((locals().get("decision") or {}).get("product_attachment_url") or "").strip()
        if product_image_url and channel == "whatsapp":
            ok, provider_id, meta = send_channel_image_message(channel, contact_key, product_image_url, caption=rendered_reply)
        else:
            if product_image_url:
                image_ok, image_provider_id, image_meta = send_channel_image_message(channel, contact_key, product_image_url)
                if image_ok or os.getenv("WHATSAPP_DRY_RUN", "1") == "1":
                    save_whatsapp_message(
                        contact_key,
                        "outbound",
                        "[Image]",
                        customer_name=customer_name,
                        provider_message_id=image_provider_id if image_ok else "dry-run",
                        metadata={"source": "auto_reply_product_image", "attachment_url": product_image_url, "dry_run": not image_ok, "meta": image_meta},
                        channel=channel,
                        display_handle=display_handle,
                        contact_phone=contact_phone,
                        sender_type="ai",
                        message_type="image",
                        attachments=[{"type": "image", "url": product_image_url}],
                        ai_metadata={"decision": decision} if "decision" in locals() else {},
                    )
            ok, provider_id, meta = send_channel_text_message(channel, contact_key, rendered_reply)
        if ok or os.getenv("WHATSAPP_DRY_RUN", "1") == "1":
            save_whatsapp_message(
                contact_key,
                "outbound",
                rendered_reply,
                customer_name=customer_name,
                provider_message_id=provider_id if ok else "dry-run",
                metadata={"source": "auto_reply", "rule_id": rule.get("id") if rule else None, "dry_run": not ok, "meta": meta, "attachment_url": product_image_url},
                channel=channel,
                display_handle=display_handle,
                contact_phone=contact_phone,
                sender_type="ai" if rule and str(rule.get("id", "")).startswith("ai") else "system",
                message_type="image" if product_image_url and channel == "whatsapp" else "text",
                attachments=[{"type": "image", "url": product_image_url}] if product_image_url else [],
                ai_metadata={"decision": decision} if "decision" in locals() else {},
            )
        else:
            save_whatsapp_message(
                contact_key,
                "outbound",
                f"AI reply failed to send: {provider_id}",
                customer_name=customer_name,
                provider_message_id="send-failed",
                metadata={"source": "auto_reply_send_failed", "error": provider_id, "meta": meta},
                channel=channel,
                display_handle=display_handle,
                contact_phone=contact_phone,
                sender_type="system",
                internal_only=True,
                ai_metadata={"decision": decision} if "decision" in locals() else {},
            )
            save_internal_assistant_message(
                f"AI reply failed for {contact_key}: {provider_id}",
                metadata={"source": "auto_reply_send_failed", "conversation": contact_key, "meta": meta},
            )


def process_tickbot_auto_reply_job(job: dict) -> None:
    maybe_auto_reply(
        job.get("channel") or "whatsapp",
        job.get("contact_key") or "",
        job.get("body") or "",
        customer_name=job.get("customer_name") or "",
        display_handle=job.get("display_handle") or "",
        contact_phone=job.get("contact_phone") or "",
    )


def _auto_reply_worker_loop(app):
    with app.app_context():
        while True:
            jobs = claim_tickbot_auto_reply_jobs(limit=3)
            if not jobs:
                time.sleep(1.5)
                continue
            for job in jobs:
                try:
                    process_tickbot_auto_reply_job(job)
                    finish_tickbot_auto_reply_job(job["id"], True)
                except Exception as e:
                    logger.exception("TickBot auto-reply job failed: %s", e)
                    finish_tickbot_auto_reply_job(job["id"], False, str(e))


def ensure_auto_reply_worker_started():
    global AUTO_REPLY_WORKER_STARTED
    with AUTO_REPLY_WORKER_LOCK:
        if AUTO_REPLY_WORKER_STARTED:
            return
        app = current_app._get_current_object()
        thread = threading.Thread(target=_auto_reply_worker_loop, args=(app,), name="tickbot-auto-reply-worker", daemon=True)
        thread.start()
        AUTO_REPLY_WORKER_STARTED = True


def enqueue_auto_reply(
    channel: str,
    contact_key: str,
    body: str,
    *,
    customer_name: str = "",
    display_handle: str = "",
    contact_phone: str = "",
    provider_message_id: str = "",
) -> dict | None:
    job = enqueue_tickbot_auto_reply_job({
        "channel": channel,
        "contact_key": contact_key,
        "body": body,
        "customer_name": customer_name,
        "display_handle": display_handle,
        "contact_phone": contact_phone,
        "provider_message_id": provider_message_id,
    })
    ensure_auto_reply_worker_started()
    return job


def send_order_confirmation(order: dict[str, Any]) -> bool:
    customer = order.get("customer_details") or {}
    phone = normalize_whatsapp_phone(customer.get("phone") or "")
    if not phone:
        for item in order.get("line_items", []) or []:
            phone = normalize_whatsapp_phone(item.get("phone") or "")
            if phone:
                break
    if not phone:
        return False

    body = os.getenv(
        "WHATSAPP_ORDER_CONFIRMATION_TEMPLATE",
        "Thank you for your order {{order_id}} from TickBags. We have received your payment and will update you when it is dispatched.",
    )
    body = body.replace("{{order_id}}", str(order.get("order_id") or ""))
    ok, provider_id, meta = send_meta_text_message(phone, body)
    if ok or os.getenv("WHATSAPP_DRY_RUN", "1") == "1":
        save_whatsapp_message(
            phone,
            "outbound",
            body,
            customer_name=customer.get("name") or "",
            provider_message_id=provider_id if ok else "dry-run",
            metadata={"source": "orders_paid_webhook", "dry_run": not ok, "meta": meta},
        )
        update_whatsapp_conversation_status(phone, "open")
        return True
    return False


def verify_meta_signature(req) -> bool:
    secret = os.getenv("META_APP_SECRET") or os.getenv("WHATSAPP_APP_SECRET")
    signature = req.headers.get("X-Hub-Signature-256", "")
    if not secret or not signature.startswith("sha256="):
        return True
    expected = hmac.new(secret.encode("utf-8"), req.get_data(), hashlib.sha256).hexdigest()
    return hmac.compare_digest(signature, f"sha256={expected}")


def _extension_cors_response(payload=None, status=200):
    response = jsonify_data(payload or {}, status)
    origin = request.headers.get("Origin") or "*"
    response.headers["Access-Control-Allow-Origin"] = origin if origin.startswith("chrome-extension://") else "*"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, X-TickBot-Key"
    response.headers["Access-Control-Allow-Methods"] = "POST, OPTIONS"
    return response


def _extension_request_authorized() -> bool:
    required_key = (os.getenv("TICKBOT_EXTENSION_KEY") or os.getenv("INTERNAL_API_KEY") or "").strip()
    if not required_key:
        return True
    supplied_key = (request.headers.get("X-TickBot-Key") or "").strip()
    return hmac.compare_digest(supplied_key, required_key)


def _clean_extension_history(raw_history) -> list[dict]:
    if not isinstance(raw_history, list):
        return []
    cleaned = []
    for item in raw_history[-16:]:
        if not isinstance(item, dict):
            continue
        role = str(item.get("role") or "").strip().lower()
        if role not in {"user", "assistant"}:
            continue
        content = str(item.get("content") or item.get("text") or "").strip()
        if not content:
            continue
        cleaned.append({"role": role, "content": content[:1200]})
    return cleaned


def _extension_contextual_message(customer_message: str, history: list[dict]) -> str:
    if not history:
        return customer_message
    lines = []
    for item in history[-12:]:
        speaker = "Customer" if item.get("role") == "user" else "TickBags team"
        lines.append(f"{speaker}: {item.get('content')}")
    transcript = "\n".join(lines)
    return (
        "Recent WhatsApp conversation:\n"
        f"{transcript}\n\n"
        f"Latest customer message: {customer_message}\n"
        "Reply only to the latest customer message, using the previous messages as context."
    )


@whatsapp_bp.route("/api/tickbot/extension/draft", methods=["POST", "OPTIONS"])
def api_tickbot_extension_draft():
    if request.method == "OPTIONS":
        return _extension_cors_response({"success": True})
    if not _extension_request_authorized():
        return _extension_cors_response({"success": False, "error": "Unauthorized."}, 401)

    data = request.get_json(silent=True) or {}
    customer_message = str(data.get("customer_message") or "").strip()
    chat_name = str(data.get("chat_name") or "").strip()
    contact_key = str(data.get("contact_key") or chat_name or "").strip()
    extension_history = _clean_extension_history(data.get("conversation_history"))
    if not customer_message:
        return _extension_cors_response({"success": False, "error": "Customer message is required."}, 400)

    conversation = get_conversation_by_any_key(contact_key) if contact_key else None
    decision = analyze_inbound_message(
        "whatsapp",
        contact_key or chat_name or "whatsapp-extension",
        _extension_contextual_message(customer_message, extension_history),
        conversation=conversation,
    )
    reply = str(decision.get("reply_text") or "").strip()
    if not reply:
        settings = get_whatsapp_settings()
        reply = settings.get("human_handoff_reply") or "Thanks for messaging TickBags. Our team will check this and reply shortly."

    return _extension_cors_response({
        "success": True,
        "reply": reply,
        "decision": decision,
        "conversation": {
            "phone": (conversation or {}).get("phone", ""),
            "public_chat_id": (conversation or {}).get("public_chat_id", ""),
            "ai_mode": (conversation or {}).get("ai_mode", ""),
            "status": (conversation or {}).get("status", ""),
        },
    })


@whatsapp_bp.route("/whatsapp/inbox")
def inbox():
    return render_template("whatsapp_inbox.html", embedded_mode=True, skip_base_password_prompt=True)


@whatsapp_bp.route("/api/whatsapp/conversations")
def api_conversations():
    conversations = list_inbox_conversations()
    for item in conversations:
        if item.get("channel") in {"facebook", "instagram"}:
            item["avatar_url"] = f"/api/whatsapp/avatar/{quote(item.get('phone') or '', safe='')}"
    return jsonify_data({
        "success": True,
        "conversations": conversations,
        "db_error": get_last_db_error(),
        "fingerprint": hashlib.sha256(json.dumps(conversations, default=_json_default, sort_keys=True).encode()).hexdigest(),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    })


@whatsapp_bp.route("/api/whatsapp/conversations/<path:phone>/messages")
def api_messages(phone):
    conversation = get_conversation_by_any_key(phone)
    if conversation and conversation.get("channel") in {"facebook", "instagram"}:
        conversation["avatar_url"] = f"/api/whatsapp/avatar/{quote(conversation.get('phone') or '', safe='')}"
    return jsonify_data({
        "success": True,
        "phone": conversation.get("phone") if conversation else normalize_whatsapp_phone(phone),
        "conversation": conversation,
        "messages": list_inbox_messages(phone),
    })


@whatsapp_bp.route("/api/whatsapp/conversations/<path:phone>/orders")
def api_conversation_orders(phone):
    conversation = get_conversation_by_any_key(phone)
    return jsonify_data({
        "success": bool(conversation),
        "phone": conversation.get("phone") if conversation else normalize_whatsapp_phone(phone),
        "orders": customer_orders_for_conversation(conversation),
    }, 200 if conversation else 404)


@whatsapp_bp.route("/api/whatsapp/conversations/<path:phone>/status", methods=["POST"])
def api_status(phone):
    data = request.get_json(silent=True) or {}
    ok = update_whatsapp_conversation_status(phone, data.get("status"))
    return jsonify({"success": ok})


@whatsapp_bp.route("/api/whatsapp/conversations/<path:key>/ai-mode", methods=["POST"])
def api_ai_mode(key):
    data = request.get_json(silent=True) or {}
    ai_mode = str(data.get("ai_mode") or "").strip().lower()
    ok = update_conversation_ai_mode(key, ai_mode, data.get("reason") or "")
    if not ok:
        return jsonify_data({
            "success": False,
            "error": get_last_db_error() or "Could not update AI mode for this chat.",
            "conversation": get_conversation_by_any_key(key),
        }, 400)
    if ai_mode == "human":
        update_conversation_labels(key, add=["manual_lock"], remove=["ai"])
    elif ai_mode == "auto":
        update_conversation_labels(key, add=["ai"], remove=["manual_lock", "needs_human"])
    elif ai_mode == "suggest":
        update_conversation_labels(key, add=["ai"], remove=["manual_lock", "needs_human"])
    return jsonify_data({"success": True, "conversation": get_conversation_by_any_key(key)})


@whatsapp_bp.route("/api/whatsapp/conversations/<path:key>/labels", methods=["POST"])
def api_labels(key):
    data = request.get_json(silent=True) or {}
    conversation = update_conversation_labels(key, add=data.get("add") or [], remove=data.get("remove") or [])
    return jsonify_data({"success": bool(conversation), "conversation": conversation}, 200 if conversation else 404)


@whatsapp_bp.route("/api/whatsapp/conversations/<path:key>/manual-lock", methods=["POST"])
def api_manual_lock(key):
    data = request.get_json(silent=True) or {}
    lock = bool(data.get("manual_lock"))
    mode = "human" if lock else "suggest"
    ok = update_conversation_ai_mode(key, mode, "Manual lock" if lock else "")
    if lock:
        update_conversation_labels(key, add=["manual_lock"], remove=["ai"])
    else:
        update_conversation_labels(key, remove=["manual_lock"])
    return jsonify({"success": ok, "conversation": get_conversation_by_any_key(key)})


@whatsapp_bp.route("/api/whatsapp/conversations/<path:key>/ai-draft", methods=["POST"])
def api_ai_draft(key):
    conversation = get_conversation_by_any_key(key)
    if not conversation:
        return jsonify({"success": False, "error": "Conversation not found."}), 404
    messages = list_inbox_messages(key)
    latest = next((msg for msg in reversed(messages) if msg.get("direction") == "inbound"), {})
    decision = analyze_inbound_message(conversation.get("channel") or "whatsapp", conversation.get("phone") or key, latest.get("body") or "", conversation=conversation)
    apply_ai_decision(conversation, decision)
    return jsonify_data({"success": True, "decision": decision, "draft": decision.get("reply_text") or ""})


@whatsapp_bp.route("/api/whatsapp/conversations/<path:key>/send-ai-draft", methods=["POST"])
def api_send_ai_draft(key):
    data = request.get_json(silent=True) or {}
    body = str(data.get("body") or "").strip()
    if not body:
        return jsonify({"success": False, "error": "Draft body is required."}), 400
    conversation = get_conversation_by_any_key(key)
    if not conversation:
        return jsonify({"success": False, "error": "Conversation not found."}), 404
    if conversation.get("is_internal"):
        return jsonify({"success": False, "error": "Assistant messages are internal only."}), 400
    if conversation.get("channel") == "whatsapp" and not service_window_open(conversation):
        return jsonify({"success": False, "error": "Template required: the WhatsApp reply window is closed."}), 409
    ok, provider_id, meta = send_channel_text_message(conversation.get("channel") or "whatsapp", conversation.get("phone"), body)
    if ok or os.getenv("WHATSAPP_DRY_RUN", "1") == "1":
        save_whatsapp_message(
            conversation.get("phone"),
            "outbound",
            body,
            provider_message_id=provider_id if ok else "dry-run",
            metadata={"source": "ai_draft", "dry_run": not ok, "meta": meta},
            channel=conversation.get("channel") or "whatsapp",
            display_handle=conversation.get("display_handle") or "",
            contact_phone=conversation.get("contact_phone") or "",
            sender_type="ai",
        )
        return jsonify({"success": True, "provider_message_id": provider_id if ok else "dry-run", "dry_run": not ok})
    return jsonify({"success": False, "error": provider_id}), 502


@whatsapp_bp.route("/api/whatsapp/conversations/<path:key>/mark-new-order", methods=["POST"])
def api_mark_new_order(key):
    data = request.get_json(silent=True) or {}
    ok = mark_new_order(key, data.get("order_candidate") or {})
    return jsonify({"success": ok, "conversation": get_conversation_by_any_key(key)})


@whatsapp_bp.route("/api/whatsapp/conversations/<path:key>/order-added", methods=["POST"])
def api_order_added(key):
    data = request.get_json(silent=True) or {}
    ok = mark_order_added(key, data.get("human_user") or "team")
    if ok and (data.get("shopify_order_id") or data.get("note")):
        save_internal_assistant_message(
            f"Order added for {key}: {data.get('shopify_order_id') or ''} {data.get('note') or ''}".strip(),
            metadata={"source": "order_added", "conversation": key},
        )
    return jsonify({"success": ok, "conversation": get_conversation_by_any_key(key)})


@whatsapp_bp.route("/api/whatsapp/conversations/<path:key>/needs-human", methods=["POST"])
def api_needs_human(key):
    data = request.get_json(silent=True) or {}
    ok = mark_conversation_needs_human(key, data.get("reason") or "Needs human review", labels=data.get("labels") or [])
    return jsonify({"success": ok, "conversation": get_conversation_by_any_key(key)})


@whatsapp_bp.route("/api/tickbot-assistant/messages", methods=["GET"])
def api_assistant_messages():
    assistant = ensure_tickbot_assistant_chat()
    return jsonify_data({"success": bool(assistant), "conversation": assistant, "messages": list_inbox_messages(assistant["phone"]) if assistant else []})


@whatsapp_bp.route("/api/tickbot-assistant/message", methods=["POST"])
def api_assistant_message():
    data = request.get_json(silent=True) or {}
    body = str(data.get("body") or "").strip()
    if not body:
        return jsonify({"success": False, "error": "Message body is required."}), 400
    saved = save_whatsapp_message(
        "internal:tickbot-assistant",
        "outbound",
        body,
        metadata={"source": "assistant_chat"},
        channel="internal",
        display_handle="Internal assistant",
        sender_type="human",
        internal_only=True,
    )
    teach_command = parse_assistant_teach_command(body)
    if teach_command:
        question, answer = teach_command
        learned = save_tickbot_knowledge(question[:160], answer, source="assistant_chat", verified=True)
        if learned:
            save_internal_assistant_message(
                f"Saved reusable TickBot knowledge for: {question}",
                metadata={"source": "knowledge_saved", "question": question},
            )
        return jsonify_data({"success": bool(saved), "message": saved, "knowledge_saved": bool(learned)})

    answer_command = parse_assistant_answer_command(body)
    if answer_command:
        reference, answer_text = answer_command
        alert = latest_unresolved_assistant_alert(reference)
        conversation = get_conversation_by_any_key(reference) or (alert or {}).get("conversation")
        if not conversation:
            save_internal_assistant_message(
                f"Could not find a chat for `{reference}`. Try the public chat ID like TB-FB-000001.",
                metadata={"source": "assistant_resolution_error", "reference": reference},
            )
            return jsonify({"success": False, "error": "Conversation not found."}), 404
        metadata = (alert or {}).get("metadata") or {}
        learned = save_tickbot_knowledge(
            title=str(metadata.get("customer_message") or conversation.get("last_message") or reference)[:160],
            content=answer_text,
            source=f"assistant_resolution:{conversation.get('public_chat_id') or conversation.get('phone')}",
            verified=True,
        )
        send_error = ""
        sent = False
        if str(conversation.get("channel") or "").lower() == "whatsapp" and not service_window_open(conversation):
            send_error = "WhatsApp reply window is closed. Save a template or reply manually when the customer messages again."
        else:
            ok, provider_id, meta = send_channel_text_message(conversation.get("channel") or "whatsapp", conversation.get("phone") or reference, answer_text)
            if ok or os.getenv("WHATSAPP_DRY_RUN", "1") == "1":
                sent = True
                save_whatsapp_message(
                    conversation.get("phone") or reference,
                    "outbound",
                    answer_text,
                    customer_name=conversation.get("customer_name") or "",
                    provider_message_id=provider_id if ok else "dry-run",
                    metadata={"source": "assistant_resolution", "reference": reference, "dry_run": not ok, "meta": meta},
                    channel=conversation.get("channel") or "whatsapp",
                    display_handle=conversation.get("display_handle") or "",
                    contact_phone=conversation.get("contact_phone") or "",
                    sender_type="human",
                    ai_metadata={"assistant_resolution": True},
                )
            else:
                send_error = provider_id or "Message send failed."
        update_conversation_ai_mode(conversation.get("phone") or reference, "suggest", "Answered via TickBot Assistant")
        update_conversation_labels(conversation.get("phone") or reference, remove=["needs_human", "manual_lock"])
        save_internal_assistant_message(
            _assistant_answer_success_note(conversation.get("public_chat_id") or conversation.get("phone") or reference, bool(learned), sent, send_error),
            metadata={"source": "assistant_resolution_saved", "reference": reference, "conversation": conversation.get("phone")},
        )
        return jsonify_data({
            "success": True,
            "message": saved,
            "knowledge_saved": bool(learned),
            "customer_reply_sent": sent,
            "send_error": send_error,
            "conversation": conversation,
        })

    lowered = body.lower()
    if "what did he ask" in lowered or "what did they ask" in lowered or "latest unresolved" in lowered:
        alert = latest_unresolved_assistant_alert()
        if alert:
            metadata = alert.get("metadata") or {}
            save_internal_assistant_message(
                f"Latest unresolved: {metadata.get('public_chat_id') or metadata.get('conversation')}\nCustomer asked: {metadata.get('customer_message') or 'Review latest chat message.'}",
                metadata={"source": "assistant_summary", "reference": metadata.get("public_chat_id") or metadata.get("conversation")},
            )
        return jsonify_data({"success": bool(saved), "message": saved})

    if any(word in body.lower() for word in ("fact:", "policy:", "teach:", "remember")):
        current = get_whatsapp_settings()
        current["business_facts"] = (current.get("business_facts", "") + "\n" + body).strip()
        save_whatsapp_settings(current)
        save_internal_assistant_message("Saved this as a TickBags business fact.", metadata={"source": "knowledge_saved"})
    return jsonify_data({"success": bool(saved), "message": saved}, 200 if saved else 500)


@whatsapp_bp.route("/whatsapp/send", methods=["POST"])
@whatsapp_bp.route("/api/whatsapp/send", methods=["POST"])
def api_send():
    data = request.get_json(silent=True) or {}
    channel = str(data.get("channel") or "whatsapp").strip().lower() or "whatsapp"
    phone = normalize_inbox_contact_key(channel, data.get("phone"))
    conversation = get_conversation_by_any_key(data.get("phone")) or get_conversation_by_any_key(phone)
    if conversation:
        channel = str(conversation.get("channel") or channel).strip().lower() or channel
        phone = conversation.get("phone") or phone
    body = (data.get("body") or "").strip()
    attachment_url = (data.get("attachment_url") or "").strip()
    if not phone or (not body and not attachment_url):
        return jsonify({"success": False, "error": "Phone and message are required."}), 400
    if conversation and conversation.get("is_internal"):
        saved = save_whatsapp_message(
            phone,
            "outbound",
            body,
            metadata={"source": "assistant_manual"},
            channel="internal",
            display_handle="Internal assistant",
            sender_type="human",
            internal_only=True,
        )
        return jsonify({"success": bool(saved), "provider_message_id": "internal"})
    if conversation and channel == "whatsapp" and not attachment_url and not service_window_open(conversation):
        return jsonify({"success": False, "error": "Template required: the 24-hour reply window is closed for this WhatsApp chat."}), 409

    if attachment_url:
        ok, provider_id, meta = send_channel_image_message(channel, phone, attachment_url, caption=body)
    else:
        ok, provider_id, meta = send_channel_text_message(channel, phone, body)
    if ok:
        saved = save_whatsapp_message(
            phone,
            "outbound",
            body or "[Image]",
            provider_message_id=provider_id,
            metadata={"source": "manual", "meta": meta, "attachment_url": attachment_url},
            channel=channel,
            display_handle=(conversation or {}).get("display_handle") or data.get("display_handle") or "",
            contact_phone=(conversation or {}).get("contact_phone") or "",
        )
        if not saved:
            return jsonify({"success": False, "error": get_last_db_error() or "Message sent but could not be saved to the inbox."}), 500
        update_whatsapp_conversation_status(phone, "open")
        if attachment_url and body and channel != "whatsapp":
            extra_ok, extra_provider_id, extra_meta = send_channel_text_message(channel, phone, body)
            if extra_ok or os.getenv("WHATSAPP_DRY_RUN", "1") == "1":
                save_whatsapp_message(
                    phone,
                    "outbound",
                    body,
                    provider_message_id=extra_provider_id if extra_ok else "dry-run",
                    metadata={"source": "manual_attachment_caption", "meta": extra_meta, "dry_run": not extra_ok},
                    channel=channel,
                    display_handle=(conversation or {}).get("display_handle") or data.get("display_handle") or "",
                    contact_phone=(conversation or {}).get("contact_phone") or "",
                )
        return jsonify({"success": True, "provider_message_id": provider_id})

    if os.getenv("WHATSAPP_DRY_RUN", "1") == "1":
        save_whatsapp_message(
            phone,
            "outbound",
            body or "[Image]",
            provider_message_id="dry-run",
            metadata={"source": "manual", "dry_run": True, "error": provider_id, "attachment_url": attachment_url},
            channel=channel,
            display_handle=(conversation or {}).get("display_handle") or data.get("display_handle") or "",
            contact_phone=(conversation or {}).get("contact_phone") or "",
        )
        update_whatsapp_conversation_status(phone, "open")
        return jsonify({"success": True, "provider_message_id": "dry-run", "dry_run": True, "warning": provider_id})

    return jsonify({"success": False, "error": provider_id}), 502


@whatsapp_bp.route("/api/whatsapp/avatar/<path:phone>")
def api_avatar(phone):
    conversation = get_whatsapp_conversation(phone)
    if not conversation:
        return "", 404
    channel = str(conversation.get("channel") or "").lower()
    if channel not in {"facebook", "instagram"}:
        return "", 404
    sender_id = str(conversation.get("phone") or "")
    if ":" in sender_id:
        sender_id = sender_id.split(":", 1)[1]
    token = os.getenv("META_PAGE_ACCESS_TOKEN") or os.getenv("FACEBOOK_PAGE_ACCESS_TOKEN")
    api_version = os.getenv("META_GRAPH_API_VERSION", "v20.0")
    if not token or not sender_id:
        return "", 404
    fields = "name,profile_pic"
    if channel == "instagram":
        fields = "username,profile_pic"
    try:
        response = requests.get(
            f"https://graph.facebook.com/{api_version}/{sender_id}",
            params={"fields": fields},
            headers={"Authorization": f"Bearer {token}"},
            timeout=20,
        )
        data = response.json() if response.content else {}
        avatar_url = (data.get("profile_pic") or "").strip()
        if not avatar_url:
            return "", 404
        return redirect(avatar_url, code=302)
    except Exception:
        return "", 404


@whatsapp_bp.route("/api/whatsapp/rules", methods=["GET", "POST"])
def api_rules():
    if request.method == "POST":
        rule = upsert_whatsapp_rule(request.get_json(silent=True) or {})
        return jsonify_data({"success": bool(rule), "rule": rule}, 200 if rule else 400)
    return jsonify_data({"success": True, "rules": list_whatsapp_rules()})


@whatsapp_bp.route("/api/whatsapp/templates", methods=["GET", "POST"])
def api_templates():
    if request.method == "POST":
        template = upsert_whatsapp_template(request.get_json(silent=True) or {})
        return jsonify_data({"success": bool(template), "template": template}, 200 if template else 400)
    return jsonify_data({"success": True, "templates": list_whatsapp_templates()})


@whatsapp_bp.route("/api/whatsapp/blasts", methods=["GET", "POST"])
def api_blasts():
    if request.method == "GET":
        return jsonify_data({"success": True, "blasts": list_whatsapp_blasts()})

    data = request.get_json(silent=True) or {}
    template_id = data.get("template_id")
    segment = (data.get("segment") or "open").strip().lower()
    templates = list_whatsapp_templates()
    template = next((item for item in templates if str(item.get("id")) == str(template_id)), None)
    if not template:
        return jsonify({"success": False, "error": "Choose a saved template."}), 400
    if not template.get("approved"):
        return jsonify({"success": False, "error": "Template must be marked pre-approved before blasting."}), 400

    conversations = list_whatsapp_conversations()
    if segment != "all":
        conversations = [item for item in conversations if item.get("status") == segment]

    sent = 0
    failed = 0
    for conversation in conversations:
        phone = conversation.get("phone")
        body = render_template_body(template.get("body") or "", phone)
        ok, provider_id, meta = send_meta_text_message(phone, body)
        if ok or os.getenv("WHATSAPP_DRY_RUN", "1") == "1":
            sent += 1
            save_whatsapp_message(
                phone,
                "outbound",
                body,
                provider_message_id=provider_id if ok else "dry-run",
                metadata={"source": "blast", "template_id": template_id, "dry_run": not ok, "meta": meta},
            )
        else:
            failed += 1

    blast = create_whatsapp_blast(template_id, template.get("name") or "", segment, len(conversations), sent, failed)
    return jsonify_data({"success": True, "blast": blast})


@whatsapp_bp.route("/api/whatsapp/settings", methods=["GET", "POST"])
def api_settings():
    if request.method == "POST":
        payload = request.get_json(silent=True) or {}
        ok = save_whatsapp_settings(payload)
        return jsonify_data({"success": ok, "settings": get_whatsapp_settings()}, 200 if ok else 400)
    return jsonify_data({"success": True, "settings": get_whatsapp_settings()})


@whatsapp_bp.route("/api/whatsapp/upload", methods=["POST"])
def api_upload():
    file = request.files.get("file")
    if not file or not file.filename:
        return jsonify({"success": False, "error": "Choose an image to upload."}), 400
    extension = Path(file.filename).suffix.lower()
    if extension not in SUPPORTED_IMAGE_EXTENSIONS:
        return jsonify({"success": False, "error": "Only JPG, PNG, and WEBP images are supported."}), 400
    filename = secure_filename(file.filename)
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    target_name = f"{timestamp}_{filename}"
    target = get_upload_dir() / target_name
    file.save(target)
    public_url = f"{get_public_base_url()}/static/{UPLOAD_DIR_NAME}/{target_name}"
    return jsonify({"success": True, "url": public_url, "filename": target_name})


@whatsapp_bp.route("/webhook/whatsapp", methods=["GET", "POST"])
def whatsapp_webhook():
    if request.method == "GET":
        verify_token = os.getenv("META_WHATSAPP_VERIFY_TOKEN") or os.getenv("WHATSAPP_VERIFY_TOKEN")
        if request.args.get("hub.mode") == "subscribe" and request.args.get("hub.verify_token") == verify_token:
            return request.args.get("hub.challenge", "")
        return "Verification failed", 403

    if not verify_meta_signature(request):
        return jsonify({"success": False, "error": "Invalid webhook signature"}), 401

    payload = request.get_json(silent=True) or {}
    for entry in payload.get("entry", []):
        for change in entry.get("changes", []):
            value = change.get("value") or {}
            contacts = {
                contact.get("wa_id"): (contact.get("profile") or {}).get("name", "")
                for contact in value.get("contacts", [])
            }
            for message in value.get("messages", []):
                phone = normalize_whatsapp_phone(message.get("from"))
                message_type = message.get("type")
                body = ((message.get("text") or {}).get("body") or "").strip()
                metadata = {"webhook": True}
                attachments = []
                should_queue_reply = message_type in {"text", "image"}
                if message_type == "image":
                    image_payload = message.get("image") or {}
                    body = body or "[Image]"
                    metadata["media_type"] = "image"
                    metadata["meta_media_id"] = (image_payload.get("id") or "")
                    if image_payload.get("caption"):
                        body = str(image_payload.get("caption") or "").strip() or body
                    media_file = download_whatsapp_media(metadata["meta_media_id"])
                    metadata.update(media_file)
                    if media_file.get("attachment_url"):
                        attachments.append({
                            "type": "image",
                            "url": media_file["attachment_url"],
                            "mime_type": media_file.get("media_mime_type", ""),
                            "source": "whatsapp_media",
                        })
                elif message_type != "text":
                    friendly_type = {
                        "audio": "Voice note",
                        "voice": "Voice note",
                        "document": "Document",
                        "sticker": "Sticker",
                        "video": "Video",
                        "reaction": "Reaction",
                        "location": "Location",
                        "contacts": "Contact card",
                    }.get(str(message_type or "").lower(), str(message_type or "Unsupported message").replace("_", " ").title())
                    body = body or f"[{friendly_type}]"
                    metadata["media_type"] = str(message_type or "unsupported").lower()
                    metadata["unsupported_message"] = message
                customer_name = contacts.get(clean_digits(phone), "")
                saved = save_whatsapp_message(
                    phone,
                    "inbound",
                    body,
                    customer_name=customer_name,
                    provider_message_id=message.get("id") or "",
                    metadata=metadata,
                    channel="whatsapp",
                    display_handle=phone,
                    contact_phone=phone,
                    message_type="image" if message_type == "image" else str(message_type or "text").lower(),
                    attachments=attachments,
                )
                if should_queue_reply and not (saved or {}).get("_duplicate"):
                    enqueue_auto_reply(
                        "whatsapp",
                        phone,
                        body,
                        customer_name=customer_name,
                        display_handle=phone,
                        contact_phone=phone,
                        provider_message_id=message.get("id") or "",
                    )

    return jsonify({"success": True})


@whatsapp_bp.route("/webhook/meta-social", methods=["GET", "POST"])
def meta_social_webhook():
    if request.method == "GET":
        verify_token = os.getenv("META_SOCIAL_VERIFY_TOKEN") or os.getenv("META_VERIFY_TOKEN")
        if request.args.get("hub.mode") == "subscribe" and request.args.get("hub.verify_token") == verify_token:
            return request.args.get("hub.challenge", "")
        return "Verification failed", 403

    if not verify_meta_signature(request):
        return jsonify({"success": False, "error": "Invalid webhook signature"}), 401

    payload = request.get_json(silent=True) or {}
    default_channel = "instagram" if str(payload.get("object") or "").lower() == "instagram" else "facebook"
    for entry in payload.get("entry", []):
        for event in entry.get("messaging", []):
            message = event.get("message") or {}
            text = (message.get("text") or "").strip()
            attachments = message.get("attachments") or []
            postback = (event.get("postback") or {}).get("payload") or ""
            if not text and not attachments and not postback:
                continue
            sender = event.get("sender") or {}
            recipient = event.get("recipient") or {}
            channel = str(event.get("platform") or default_channel).strip().lower() or default_channel
            sender_id = str(sender.get("id") or "").strip()
            recipient_id = str(recipient.get("id") or "").strip()
            is_echo = bool(message.get("is_echo"))
            contact_id = recipient_id if is_echo else sender_id
            if not contact_id:
                continue
            profile = fetch_social_profile(contact_id, channel)
            customer_name = (
                profile.get("name")
                or profile.get("username")
                or ((event.get("sender") or {}).get("name"))
                or ((event.get("profile") or {}).get("name"))
                or f"{channel_label(channel)} user"
            )
            contact_key = normalize_inbox_contact_key(channel, contact_id)
            display_handle = (
                (f"@{profile.get('username')}" if profile.get("username") else "")
                or profile.get("name")
                or (f"{channel_label(channel)} · {contact_id[-6:]}" if contact_id else channel_label(channel))
            )
            body = text or str(postback or "").strip()
            metadata = {
                "webhook": True,
                "channel": channel,
                "sender_id": sender_id,
                "recipient_id": recipient_id,
                "is_echo": is_echo,
                "timestamp": event.get("timestamp"),
                "raw_message_keys": list(message.keys()) if isinstance(message, dict) else [],
            }
            if profile.get("profile_pic"):
                metadata["profile_pic"] = profile["profile_pic"]
            if attachments:
                first_attachment = attachments[0] or {}
                attachment_type = str(first_attachment.get("type") or "attachment").strip().lower() or "attachment"
                payload_data = first_attachment.get("payload") or {}
                attachment_url = str(payload_data.get("url") or payload_data.get("href") or "").strip()
                friendly_type = {
                    "image": "Image",
                    "video": "Video",
                    "audio": "Audio",
                    "file": "File",
                    "share": "Shared post",
                    "story_mention": "Story mention",
                    "ig_reel": "Instagram reel",
                    "reel": "Reel",
                }.get(attachment_type, attachment_type.replace("_", " ").title())
                body = body or f"[{friendly_type}]"
                metadata["attachment_url"] = attachment_url
                metadata["media_type"] = attachment_type
                metadata["attachment_title"] = friendly_type
                metadata["attachment_payload"] = payload_data
                metadata["unsupported_attachment"] = first_attachment
            if not body:
                continue
            stored_attachments = []
            if metadata.get("attachment_url") or metadata.get("media_type"):
                stored_attachments.append({
                    "type": metadata.get("media_type") or "attachment",
                    "url": metadata.get("attachment_url") or "",
                    "title": metadata.get("attachment_title") or "",
                    "payload": metadata.get("attachment_payload") or {},
                })
            saved = save_whatsapp_message(
                contact_key,
                "outbound" if is_echo else "inbound",
                body,
                customer_name=customer_name,
                provider_message_id=message.get("mid") or message.get("id") or "",
                metadata=metadata,
                channel=channel,
                display_handle=display_handle,
                sender_type="human" if is_echo else "customer",
                message_type=metadata.get("media_type") or "text",
                attachments=stored_attachments,
            )
            if not is_echo and not (saved or {}).get("_duplicate") and (text or metadata.get("media_type") == "image"):
                enqueue_auto_reply(
                    channel,
                    contact_key,
                    body,
                    customer_name=customer_name,
                    display_handle=display_handle,
                    provider_message_id=message.get("mid") or message.get("id") or "",
                )
    return jsonify({"success": True})


@whatsapp_bp.route("/api/whatsapp/mock-inbound", methods=["POST"])
def api_mock_inbound():
    required_key = (os.getenv("TICKBOT_MOCK_INBOUND_KEY") or os.getenv("INTERNAL_API_KEY") or "").strip()
    logged_in_staff = bool(session.get("employee_portal_authenticated") or session.get("admin_portal_authenticated"))
    if required_key:
        supplied_key = (request.headers.get("X-TickBot-Key") or request.args.get("key") or "").strip()
        if not logged_in_staff and not hmac.compare_digest(supplied_key, required_key):
            return jsonify({"success": False, "error": "Unauthorized."}), 401
    elif not logged_in_staff and not current_app.debug and os.getenv("ALLOW_UNAUTHENTICATED_MOCK_INBOUND", "0") != "1":
        return jsonify({"success": False, "error": "Mock inbound is disabled without TICKBOT_MOCK_INBOUND_KEY."}), 403
    data = request.get_json(silent=True) or {}
    channel = str(data.get("channel") or "whatsapp").strip().lower() or "whatsapp"
    phone = normalize_inbox_contact_key(channel, data.get("phone"))
    body = (data.get("body") or "").strip()
    if not phone or not body:
        return jsonify({"success": False, "error": "Phone and message are required."}), 400
    display_handle = data.get("display_handle") or (phone if channel == "whatsapp" else f"{channel_label(channel)} · test")
    saved = save_whatsapp_message(
        phone,
        "inbound",
        body,
        customer_name=data.get("customer_name") or "Test Customer",
        metadata={"mock": True},
        channel=channel,
        display_handle=display_handle,
        contact_phone=data.get("contact_phone") or (phone if channel == "whatsapp" else ""),
    )
    if not saved:
        return jsonify({"success": False, "error": get_last_db_error() or "Could not save the test message."}), 500
    enqueue_auto_reply(
        channel,
        phone,
        body,
        customer_name=data.get("customer_name") or "Test Customer",
        display_handle=display_handle,
        contact_phone=data.get("contact_phone") or (phone if channel == "whatsapp" else ""),
        provider_message_id=data.get("provider_message_id") or "",
    )
    return jsonify({"success": True})

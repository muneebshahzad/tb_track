from __future__ import annotations

import hashlib
import hmac
import json
import os
from pathlib import Path
from datetime import datetime
from typing import Any
from urllib.parse import quote

import requests
from flask import Blueprint, current_app, jsonify, redirect, render_template, request
from werkzeug.utils import secure_filename

from db import (
    create_whatsapp_blast,
    ensure_tickbot_assistant_chat,
    get_app_setting,
    get_last_db_error,
    get_conversation_by_any_key,
    get_whatsapp_conversation,
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


def phone_matches(left: str, right: str) -> bool:
    left_digits = clean_digits(left)
    right_digits = clean_digits(right)
    if not left_digits or not right_digits:
        return False
    return left_digits[-10:] == right_digits[-10:] or left_digits == right_digits


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
        matches.append({
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
                    "status": item.get("status"),
                }
                for item in order.get("line_items", [])
            ],
        })

    for order in daraz_orders:
        if not phone_matches(phone, order.get("customer_phone", "")):
            continue
        matches.append({
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
                    "status": item.get("status"),
                }
                for item in order.get("items_list", [])
            ],
        })

    matches.sort(key=lambda item: str(item.get("created_at") or ""), reverse=True)
    return matches[:12]


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


def build_ai_context(conversation: dict, messages: list[dict], orders: list[dict], settings: dict) -> dict:
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
        "safe_defaults": {
            "delivery_time": settings.get("delivery_time_reply"),
            "payment_method": settings.get("payment_method_reply"),
            "price": settings.get("price_reply"),
        },
        "tone": settings.get("ai_tone", ""),
    }


def heuristic_ai_decision(channel: str, body: str, settings: dict) -> dict:
    text = (body or "").lower()
    decision = dict(AI_DECISION_DEFAULT)
    decision.update({"needs_human": False, "labels": [], "confidence": 0.72, "should_auto_reply": False})
    if "[image]" in text:
        decision.update({
            "intent": "product_question",
            "labels": ["unclear_product", "image_received"],
            "reply_text": "Please share the product name or link too, so our team can confirm the exact details.",
            "order_state": "collecting_details",
            "missing_order_fields": ["product"],
        })
    elif any(word in text for word in ("refund", "damaged", "damage", "broken", "complaint", "angry", "legal")):
        decision.update({
            "intent": "damaged_item" if "damage" in text or "damaged" in text else "refund_exchange",
            "needs_human": True,
            "should_auto_reply": False,
            "labels": ["refund" if "refund" in text else "complaint", "damaged_item" if "damage" in text or "damaged" in text else "needs_human"],
            "reply_text": settings.get("escalation_reply") or settings.get("human_handoff_reply") or "",
            "escalation_reason": "Complaint/refund/damaged item requires a human.",
        })
    elif any(word in text for word in ("price", "cost", "rate", "kitna", "kitnay")):
        decision.update({"intent": "price_question", "labels": ["price_question"], "reply_text": settings.get("price_reply") or "", "should_auto_reply": True})
    elif any(word in text for word in ("delivery", "deliver", "kitne din", "days")):
        decision.update({"intent": "delivery_question", "labels": ["delivery_question"], "reply_text": settings.get("delivery_time_reply") or "", "should_auto_reply": True})
    elif any(word in text for word in ("order", "buy", "want this", "address", "phone", "name")):
        candidate = {}
        if "address" in text or "lahore" in text or "karachi" in text:
            candidate["address"] = body
        decision.update({
            "intent": "order_intent",
            "labels": ["product_interest", "collecting_details"],
            "reply_text": "Great. Please share product name/link, your name, phone number, complete address, and quantity.",
            "order_state": "collecting_details",
            "order_candidate": candidate,
            "missing_order_fields": ["product", "name", "phone", "address"],
            "should_auto_reply": True,
        })
    else:
        decision.update({
            "intent": "unknown",
            "needs_human": True,
            "labels": ["needs_human"],
            "reply_text": settings.get("escalation_reply") or "",
            "escalation_reason": "Unknown request needs human review.",
        })
    return normalize_ai_decision(decision)


def analyze_inbound_message(channel: str, contact_key: str, body: str, conversation: dict | None = None) -> dict:
    settings = get_whatsapp_settings()
    conversation = conversation or get_conversation_by_any_key(contact_key) or {}
    messages = list_inbox_messages(contact_key, limit=30) if conversation else []
    orders = customer_orders_for_conversation(conversation)
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return heuristic_ai_decision(channel, body, settings)
    system_prompt = (
        "You are TickBot, a warm, persuasive, professional sales/support agent for TickBags. "
        "Return only valid JSON. Never mention being AI. Reply in the customer's language when possible: English, Urdu, or Roman Urdu. "
        "Keep replies short and natural. Collect order details when the customer shows buying intent. "
        "Do not invent product availability, prices, discounts, delivery promises, tracking, refunds, or policies. "
        "If unsure or risky, set needs_human=true. Never say an order is confirmed unless order_state is already order_added or a real order exists. "
        "If complete order details are present, set order_state=new_order and label new_order. "
        "Escalate angry customers, refunds, damaged items, payment issues, unclear products, legal threats, or anything outside TickBags/products/orders/support."
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
                "temperature": 0.2,
                "response_format": {"type": "json_object"},
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": json.dumps({
                        "schema": schema_hint,
                        "channel": channel,
                        "latest_customer_message": body,
                        "context": build_ai_context(conversation, messages, orders, settings),
                    }, ensure_ascii=False, default=_json_default)},
                ],
            },
            timeout=30,
        )
        data = response.json() if response.content else {}
        if response.ok:
            content = (((data.get("choices") or [{}])[0].get("message") or {}).get("content") or "{}").strip()
            return normalize_ai_decision(json.loads(content))
    except Exception as e:
        safe = heuristic_ai_decision(channel, body, settings)
        safe["internal_note"] = f"AI provider failed safely: {e}"
        return safe
    return heuristic_ai_decision(channel, body, settings)


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
    if decision.get("needs_human"):
        mark_conversation_needs_human(conversation["phone"], decision.get("escalation_reason") or "Needs human review", labels=labels)
        save_internal_assistant_message(
            f"{conversation.get('public_chat_id') or conversation.get('phone')} needs human help: {decision.get('escalation_reason') or decision.get('knowledge_gap_question') or decision.get('internal_note') or 'Review latest message.'}",
            metadata={"source": "ai_escalation", "conversation": conversation.get("phone"), "decision": decision},
        )
        return
    if decision.get("order_state") == "new_order":
        mark_new_order(conversation["phone"], decision.get("order_candidate") or {})
        return
    if decision.get("order_state") in {"collecting_details", "pending_human_order_creation"}:
        update_order_candidate(conversation["phone"], decision.get("order_state"), decision.get("order_candidate") or {})
    if labels:
        update_conversation_labels(conversation["phone"], add=labels)


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
    if conversation and (conversation.get("manual_lock") or conversation.get("ai_mode") == "needs_human"):
        return
    reply, rule = find_keyword_reply(body)
    if per_chat_ai_requested or settings.get("ai_mode_enabled") or settings.get("global_ai_mode_enabled"):
        decision = analyze_inbound_message(channel, contact_key, body, conversation=conversation)
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
        ok, provider_id, meta = send_channel_text_message(channel, contact_key, render_template_body(reply, contact_phone or contact_key))
        if ok or os.getenv("WHATSAPP_DRY_RUN", "1") == "1":
            save_whatsapp_message(
                contact_key,
                "outbound",
                render_template_body(reply, contact_phone or contact_key),
                customer_name=customer_name,
                provider_message_id=provider_id if ok else "dry-run",
                metadata={"source": "auto_reply", "rule_id": rule.get("id") if rule else None, "dry_run": not ok, "meta": meta},
                channel=channel,
                display_handle=display_handle,
                contact_phone=contact_phone,
                sender_type="ai" if rule and str(rule.get("id", "")).startswith("ai") else "system",
                ai_metadata={"decision": decision} if "decision" in locals() else {},
            )


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
        "orders": customer_orders_for_conversation(conversation),
    })


@whatsapp_bp.route("/api/whatsapp/conversations/<path:phone>/status", methods=["POST"])
def api_status(phone):
    data = request.get_json(silent=True) or {}
    ok = update_whatsapp_conversation_status(phone, data.get("status"))
    return jsonify({"success": ok})


@whatsapp_bp.route("/api/whatsapp/conversations/<path:key>/ai-mode", methods=["POST"])
def api_ai_mode(key):
    data = request.get_json(silent=True) or {}
    ok = update_conversation_ai_mode(key, data.get("ai_mode"), data.get("reason") or "")
    if data.get("ai_mode") == "human":
        update_conversation_labels(key, add=["manual_lock"], remove=["ai"])
    elif data.get("ai_mode") == "auto":
        update_conversation_labels(key, add=["ai"], remove=["manual_lock", "needs_human"])
    return jsonify({"success": ok, "conversation": get_conversation_by_any_key(key)})


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
                    continue
                customer_name = contacts.get(clean_digits(phone), "")
                save_whatsapp_message(
                    phone,
                    "inbound",
                    body,
                    customer_name=customer_name,
                    provider_message_id=message.get("id") or "",
                    metadata=metadata,
                    channel="whatsapp",
                    display_handle=phone,
                    contact_phone=phone,
                    message_type="image" if message_type == "image" else "text",
                    attachments=attachments,
                )
                if message_type == "text":
                    maybe_auto_reply("whatsapp", phone, body, customer_name=customer_name, display_handle=phone, contact_phone=phone)

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
            if not sender_id:
                continue
            profile = fetch_social_profile(sender_id, channel)
            customer_name = (
                profile.get("name")
                or profile.get("username")
                or ((event.get("sender") or {}).get("name"))
                or ((event.get("profile") or {}).get("name"))
                or f"{channel_label(channel)} user"
            )
            contact_key = normalize_inbox_contact_key(channel, sender_id)
            display_handle = (
                (f"@{profile.get('username')}" if profile.get("username") else "")
                or profile.get("name")
                or (f"{channel_label(channel)} · {sender_id[-6:]}" if sender_id else channel_label(channel))
            )
            body = text or str(postback or "").strip()
            metadata = {
                "webhook": True,
                "channel": channel,
                "sender_id": sender_id,
                "recipient_id": recipient.get("id") or "",
                "timestamp": event.get("timestamp"),
                "raw_message_keys": list(message.keys()) if isinstance(message, dict) else [],
            }
            if profile.get("profile_pic"):
                metadata["profile_pic"] = profile["profile_pic"]
            if attachments:
                image = next((item for item in attachments if (item or {}).get("type") == "image"), None)
                if image:
                    body = body or "[Image]"
                    metadata["attachment_url"] = ((image.get("payload") or {}).get("url") or "")
                    metadata["media_type"] = "image"
                elif not body:
                    first_attachment = attachments[0] or {}
                    attachment_type = str(first_attachment.get("type") or "attachment")
                    body = f"[{attachment_type.title()}]"
                    metadata["media_type"] = attachment_type
                    metadata["unsupported_attachment"] = first_attachment
            if not body:
                continue
            save_whatsapp_message(
                contact_key,
                "inbound",
                body,
                customer_name=customer_name,
                provider_message_id=message.get("mid") or message.get("id") or "",
                metadata=metadata,
                channel=channel,
                display_handle=display_handle,
                message_type="image" if metadata.get("media_type") == "image" else "text",
                attachments=[{"type": "image", "url": metadata.get("attachment_url")}] if metadata.get("attachment_url") else [],
            )
            if text:
                maybe_auto_reply(channel, contact_key, text, customer_name=customer_name, display_handle=display_handle)
    return jsonify({"success": True})


@whatsapp_bp.route("/api/whatsapp/mock-inbound", methods=["POST"])
def api_mock_inbound():
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
    maybe_auto_reply(channel, phone, body, customer_name=data.get("customer_name") or "Test Customer", display_handle=display_handle, contact_phone=data.get("contact_phone") or (phone if channel == "whatsapp" else ""))
    return jsonify({"success": True})

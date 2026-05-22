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
    get_app_setting,
    get_last_db_error,
    get_whatsapp_conversation,
    list_whatsapp_blasts,
    list_whatsapp_conversations,
    list_whatsapp_messages,
    list_whatsapp_rules,
    list_whatsapp_templates,
    normalize_inbox_contact_key,
    normalize_whatsapp_phone,
    save_whatsapp_message,
    set_app_setting,
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
    if not settings.get("auto_handle_enabled", True):
        return
    reply, rule = find_keyword_reply(body)
    conversation = get_whatsapp_conversation(contact_key)
    if not reply:
        reply = fallback_reply(contact_phone or contact_key, body)
    if settings.get("ai_mode_enabled") and settings.get("ai_handover_enabled", True):
        ai_reply = ai_reply_for_message(channel, contact_phone or contact_key, body, conversation=conversation)
        if ai_reply:
            reply = ai_reply
            rule = {"id": "ai-mode", "hold_for_review": False}
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
    return render_template("whatsapp_inbox.html")


@whatsapp_bp.route("/api/whatsapp/conversations")
def api_conversations():
    conversations = list_whatsapp_conversations()
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
    conversation = get_whatsapp_conversation(phone)
    if conversation and conversation.get("channel") in {"facebook", "instagram"}:
        conversation["avatar_url"] = f"/api/whatsapp/avatar/{quote(conversation.get('phone') or '', safe='')}"
    return jsonify_data({
        "success": True,
        "phone": conversation.get("phone") if conversation else normalize_whatsapp_phone(phone),
        "conversation": conversation,
        "messages": list_whatsapp_messages(phone),
        "orders": customer_orders_for_conversation(conversation),
    })


@whatsapp_bp.route("/api/whatsapp/conversations/<path:phone>/status", methods=["POST"])
def api_status(phone):
    data = request.get_json(silent=True) or {}
    ok = update_whatsapp_conversation_status(phone, data.get("status"))
    return jsonify({"success": ok})


@whatsapp_bp.route("/whatsapp/send", methods=["POST"])
@whatsapp_bp.route("/api/whatsapp/send", methods=["POST"])
def api_send():
    data = request.get_json(silent=True) or {}
    channel = str(data.get("channel") or "whatsapp").strip().lower() or "whatsapp"
    phone = normalize_inbox_contact_key(channel, data.get("phone"))
    conversation = get_whatsapp_conversation(phone)
    if conversation:
        channel = str(conversation.get("channel") or channel).strip().lower() or channel
        phone = conversation.get("phone") or phone
    body = (data.get("body") or "").strip()
    attachment_url = (data.get("attachment_url") or "").strip()
    if not phone or (not body and not attachment_url):
        return jsonify({"success": False, "error": "Phone and message are required."}), 400

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
                if message_type == "image":
                    body = body or "[Image]"
                    metadata["media_type"] = "image"
                    metadata["meta_media_id"] = ((message.get("image") or {}).get("id") or "")
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
            if not text and not attachments:
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
            body = text
            metadata = {
                "webhook": True,
                "channel": channel,
                "sender_id": sender_id,
                "recipient_id": recipient.get("id") or "",
                "timestamp": event.get("timestamp"),
            }
            if profile.get("profile_pic"):
                metadata["profile_pic"] = profile["profile_pic"]
            if attachments:
                image = next((item for item in attachments if (item or {}).get("type") == "image"), None)
                if image:
                    body = body or "[Image]"
                    metadata["attachment_url"] = ((image.get("payload") or {}).get("url") or "")
                    metadata["media_type"] = "image"
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
    reply, rule = find_keyword_reply(body)
    if not reply:
        reply = fallback_reply(data.get("contact_phone") or phone, body)
    if reply and not (rule and rule.get("hold_for_review")):
        save_whatsapp_message(
            phone,
            "outbound",
            render_template_body(reply, data.get("contact_phone") or phone),
            provider_message_id="dry-run",
            metadata={"source": "mock_auto_reply", "rule_id": rule.get("id") if rule else None},
            channel=channel,
            display_handle=display_handle,
            contact_phone=data.get("contact_phone") or (phone if channel == "whatsapp" else ""),
        )
    return jsonify({"success": True})

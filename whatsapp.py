from __future__ import annotations

import hashlib
import hmac
import json
import os
from datetime import datetime
from typing import Any

import requests
from flask import Blueprint, current_app, jsonify, render_template, request

from db import (
    create_whatsapp_blast,
    get_whatsapp_conversation,
    list_whatsapp_blasts,
    list_whatsapp_conversations,
    list_whatsapp_messages,
    list_whatsapp_rules,
    list_whatsapp_templates,
    normalize_inbox_contact_key,
    normalize_whatsapp_phone,
    save_whatsapp_message,
    update_whatsapp_conversation_status,
    upsert_whatsapp_rule,
    upsert_whatsapp_template,
)


whatsapp_bp = Blueprint("whatsapp", __name__, template_folder="templates")


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


def channel_label(channel: str) -> str:
    return {
        "whatsapp": "WhatsApp",
        "facebook": "Facebook",
        "instagram": "Instagram",
    }.get(str(channel or "").lower(), "Inbox")


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
    orders = customer_orders_for_phone(phone)
    if orders and any(word in text for word in ("track", "tracking", "order", "status")):
        order = orders[0]
        tracking = ""
        for item in order.get("items", []) or []:
            if item.get("tracking_number"):
                tracking = item.get("tracking_number")
                break
        parts = [
            f"Your latest order {order.get('order_id')} is {order.get('status') or 'being processed'}."
        ]
        if tracking:
            parts.append(f"Tracking: {tracking}")
        return " ".join(parts)
    return os.getenv(
        "WHATSAPP_FALLBACK_REPLY",
        "Thanks for messaging TickBags. Our team will review this and reply shortly.",
    )


def send_meta_text_message(phone: str, body: str) -> tuple[bool, str, dict]:
    token = os.getenv("META_WHATSAPP_TOKEN") or os.getenv("WHATSAPP_ACCESS_TOKEN")
    phone_number_id = os.getenv("META_WHATSAPP_PHONE_NUMBER_ID") or os.getenv("WHATSAPP_PHONE_NUMBER_ID")
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
        return False, data.get("error", {}).get("message") or response.text, data
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
        return False, data.get("error", {}).get("message") or response.text, data
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
    return jsonify_data({
        "success": True,
        "conversations": conversations,
        "fingerprint": hashlib.sha256(json.dumps(conversations, default=_json_default, sort_keys=True).encode()).hexdigest(),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    })


@whatsapp_bp.route("/api/whatsapp/conversations/<path:phone>/messages")
def api_messages(phone):
    conversation = get_whatsapp_conversation(phone)
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
    body = (data.get("body") or "").strip()
    if not phone or not body:
        return jsonify({"success": False, "error": "Phone and message are required."}), 400

    ok, provider_id, meta = send_channel_text_message(channel, phone, body)
    if ok:
        save_whatsapp_message(
            phone,
            "outbound",
            body,
            provider_message_id=provider_id,
            metadata={"source": "manual", "meta": meta},
            channel=channel,
            display_handle=data.get("display_handle") or "",
        )
        update_whatsapp_conversation_status(phone, "open")
        return jsonify({"success": True, "provider_message_id": provider_id})

    if os.getenv("WHATSAPP_DRY_RUN", "1") == "1":
        save_whatsapp_message(
            phone,
            "outbound",
            body,
            provider_message_id="dry-run",
            metadata={"source": "manual", "dry_run": True, "error": provider_id},
            channel=channel,
            display_handle=data.get("display_handle") or "",
        )
        update_whatsapp_conversation_status(phone, "open")
        return jsonify({"success": True, "provider_message_id": "dry-run", "dry_run": True, "warning": provider_id})

    return jsonify({"success": False, "error": provider_id}), 502


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
                if message.get("type") != "text":
                    continue
                phone = normalize_whatsapp_phone(message.get("from"))
                body = ((message.get("text") or {}).get("body") or "").strip()
                customer_name = contacts.get(clean_digits(phone), "")
                save_whatsapp_message(
                    phone,
                    "inbound",
                    body,
                    customer_name=customer_name,
                    provider_message_id=message.get("id") or "",
                    metadata={"webhook": True},
                    channel="whatsapp",
                    display_handle=phone,
                    contact_phone=phone,
                )

                reply, rule = find_keyword_reply(body)
                if not reply:
                    reply = fallback_reply(phone, body)
                if reply and not (rule and rule.get("hold_for_review")):
                    rendered = render_template_body(reply, phone)
                    ok, provider_id, meta = send_channel_text_message("whatsapp", phone, rendered)
                    if ok or os.getenv("WHATSAPP_DRY_RUN", "1") == "1":
                        save_whatsapp_message(
                            phone,
                            "outbound",
                            rendered,
                            provider_message_id=provider_id if ok else "dry-run",
                            metadata={"source": "auto_reply", "rule_id": rule.get("id") if rule else None, "dry_run": not ok, "meta": meta},
                            channel="whatsapp",
                            display_handle=phone,
                            contact_phone=phone,
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
            if not text:
                continue
            sender = event.get("sender") or {}
            recipient = event.get("recipient") or {}
            channel = str(event.get("platform") or default_channel).strip().lower() or default_channel
            sender_id = str(sender.get("id") or "").strip()
            if not sender_id:
                continue
            customer_name = (
                ((event.get("sender") or {}).get("name"))
                or ((event.get("profile") or {}).get("name"))
                or f"{channel_label(channel)} user"
            )
            contact_key = normalize_inbox_contact_key(channel, sender_id)
            display_handle = f"{channel_label(channel)} · {sender_id[-6:]}" if sender_id else channel_label(channel)
            save_whatsapp_message(
                contact_key,
                "inbound",
                text,
                customer_name=customer_name,
                provider_message_id=message.get("mid") or message.get("id") or "",
                metadata={
                    "webhook": True,
                    "channel": channel,
                    "sender_id": sender_id,
                    "recipient_id": recipient.get("id") or "",
                    "timestamp": event.get("timestamp"),
                },
                channel=channel,
                display_handle=display_handle,
            )
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
    save_whatsapp_message(
        phone,
        "inbound",
        body,
        customer_name=data.get("customer_name") or "Test Customer",
        metadata={"mock": True},
        channel=channel,
        display_handle=display_handle,
        contact_phone=data.get("contact_phone") or (phone if channel == "whatsapp" else ""),
    )
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

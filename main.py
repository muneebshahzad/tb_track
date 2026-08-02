import base64
import smtplib
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal, ROUND_HALF_UP
from email.mime.text import MIMEText
from flask import render_template, request, jsonify, redirect, url_for, session, send_from_directory, Response
import datetime as dt
from datetime import datetime

import lazop
import os
import hmac
import hashlib
import base64
import asyncio
import aiohttp
from flask import Flask
import shopify
import requests
import json
from difflib import SequenceMatcher
from urllib.parse import quote, urlparse
import re
from xml.sax.saxutils import escape as xml_escape

from apscheduler.schedulers.background import BackgroundScheduler
from token_manager import get_access_token, save_tokens
from campaigns import campaigns_bp, init_campaign_dirs
from whatsapp import send_order_confirmation, whatsapp_bp
from shopify_protected_data import (
    clear_offline_token_state,
    create_oauth_state,
    exchange_oauth_code_for_token,
    fetch_protected_order_details,
    get_graphql_api_version,
    get_graphql_endpoint,
    get_graphql_token,
    get_install_url,
    get_protected_data_config_status,
    get_shop_domain,
    save_offline_token,
    verify_oauth_hmac,
)
import ssl
import certifi

from db import (
    create_exhibition,
    create_exhibition_expense,
    create_exhibition_order,
    delete_exhibition_order,
    delete_order_status,
    get_app_setting,
    get_exhibition_order,
    get_product_cost_lookup,
    init_db,
    list_exhibition_expenses,
    list_exhibition_orders,
    list_exhibitions,
    list_product_costs,
    load_order_statuses,
    set_app_setting,
    update_exhibition_order,
    update_exhibition_order_product_cost,
    upsert_order_status,
    upsert_product_cost,
)
from markupsafe import Markup

os.environ['SSL_CERT_FILE'] = certifi.where()
os.environ['REQUESTS_CA_BUNDLE'] = certifi.where()

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

app = Flask(__name__)
app.debug = False
app.secret_key = os.getenv('APP_SECRET_KEY', 'default_secret_key')
init_campaign_dirs()
app.register_blueprint(campaigns_bp)
app.register_blueprint(whatsapp_bp)

EMPLOYEE_PORTAL_PASSWORD = os.getenv('EMPLOYEE_PORTAL_PASSWORD', '@@@t')
EMPLOYEE_PORTAL_SESSION_KEY = 'employee_portal_authenticated'
ADMIN_PORTAL_PASSWORD = os.getenv('ADMIN_PORTAL_PASSWORD', 'security')
ADMIN_PORTAL_SESSION_KEY = 'admin_portal_authenticated'
SHOPIFY_OAUTH_STATE_SESSION_KEY = 'shopify_oauth_state'

# ── Jinja2 helpers ────────────────────────────────────────────────────────────

_TAG_STYLES = {
    'Leopards':           'background:#ede7f6;color:#4527a0',
    'Order Confirmed':    'background:#e8f5e9;color:#1b5e20',
    'Fulfilment Not Set': 'background:#fff8e1;color:#e65100',
    'No Throw':           'background:#fce4ec;color:#880e4f',
}

@app.template_global()
def tag_style(label):
    return _TAG_STYLES.get(label, 'background:#e8eaf6;color:#283593')

@app.template_global()
def status_badge(s):
    s = s or ''
    u = s.upper()
    if 'DELIVERED' in u:  bg, color, dot = '#d4f5e9', '#0f6848', '#1cc88a'
    elif 'RETURN' in u:   bg, color, dot = '#fce8e6', '#8b1a10', '#e74a3b'
    elif 'CANCELLED' in u: bg, color, dot = '#fce8e6', '#8b1a10', '#e74a3b'
    elif s == 'Booked':   bg, color, dot = '#dde4fb', '#2346a8', '#4e73df'
    elif s == 'Un-Booked': bg, color, dot = '#ebebed', '#4a4b55', '#858796'
    elif 'OUT FOR' in u or 'DISPATCH' in u or 'TRANSIT' in u: bg, color, dot = '#fef8e4', '#7a5c00', '#f6c23e'
    elif 'CONFIRMED' in u: bg, color, dot = '#d4f5e9', '#0f6848', '#1cc88a'
    elif 'CALL NOT' in u: bg, color, dot = '#e8f8fb', '#0a5c6e', '#36b9cc'
    else:                  bg, color, dot = '#e8f8fb', '#0a5c6e', '#36b9cc'
    return Markup(
        f'<span style="display:inline-flex;align-items:center;gap:5px;padding:3px 10px;'
        f'border-radius:99px;font-size:11px;font-weight:600;background:{bg};color:{color};white-space:nowrap;">'
        f'<span style="width:6px;height:6px;border-radius:50%;background:{dot};flex-shrink:0;"></span>'
        f'{s or "—"}</span>'
    )

@app.template_filter('format_number')
def format_number(value):
    try:
        return f'{int(value):,}'
    except (ValueError, TypeError):
        return str(value)

@app.template_filter('parse_date')
def parse_date_filter(value):
    if not value:
        return dt.datetime.now()
    for fmt in ('%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S %z', '%Y-%m-%d'):
        try:
            return dt.datetime.strptime(str(value)[:19], fmt)
        except ValueError:
            continue
    return dt.datetime.now()

@app.context_processor
def inject_now():
    is_admin_portal = bool(session.get(ADMIN_PORTAL_SESSION_KEY))
    return {
        'now': dt.datetime.now(),
        'skip_base_password_prompt': is_admin_portal,
        'embedded_mode': request.args.get('embedded') == '1',
        'daraz_oauth_url': get_daraz_authorize_url(),
    }

order_details = []
daraz_orders = []
order_details_lock = threading.RLock()
app.order_details_provider = lambda: order_details
app.daraz_orders_provider = lambda: daraz_orders


def get_app_base_url() -> str:
    explicit = (
        os.getenv('APP_BASE_URL')
        or os.getenv('PUBLIC_APP_BASE_URL')
        or os.getenv('SHOPIFY_APP_BASE_URL')
        or ''
    ).strip()
    return explicit.rstrip('/') if explicit else 'https://dashboard.tickbags.com'


def get_daraz_callback_url() -> str:
    return f"{get_app_base_url()}/daraz"


def get_daraz_authorize_url() -> str:
    callback = quote(get_daraz_callback_url(), safe='')
    return f"https://api.daraz.pk/oauth/authorize?response_type=code&redirect_uri={callback}&client_id=501554"

RATE_LIMIT = 2
LAST_REQUEST_TIME = 0
product_image_cache = {}  # (product_id, variant_id) -> (image_src, variant_name)
shopify_admin_call_lock = threading.Lock()


# ── Email ─────────────────────────────────────────────────────────────────────

@app.route('/send-email', methods=['POST'])
def send_email():
    data = request.get_json()
    to_emails = data.get('to', [])
    cc_emails = data.get('cc', [])
    subject = data.get('subject', '')
    body = data.get('body', '')

    try:
        smtp_server = 'smtp.gmail.com'
        smtp_port = 587
        smtp_user = os.getenv('SMTP_USER')
        smtp_password = os.getenv('SMTP_PASSWORD')

        msg = MIMEText(body)
        msg['From'] = smtp_user
        msg['To'] = ', '.join(to_emails)
        msg['Cc'] = ', '.join(cc_emails)
        msg['Subject'] = subject

        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.sendmail(smtp_user, to_emails + cc_emails, msg.as_string())
        server.quit()

        return jsonify({'message': 'Email sent successfully'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ── Helpers ───────────────────────────────────────────────────────────────────

def format_date(date_str):
    date_obj = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S %z")
    return date_obj.isoformat()


def setup_shopify():
    shop_url = get_shop_domain() or (os.getenv("SHOP_URL") or "").strip()
    legacy_password = (os.getenv("PASSWORD") or "").strip()
    oauth_token = get_graphql_token()
    api_key = (os.getenv("API_KEY") or "").strip()
    if not shop_url or not (legacy_password or oauth_token):
        print("SHOP_URL or Shopify token missing.")
        return

    try:
        shopify.ShopifyResource.clear_session()
    except Exception:
        pass

    if not shop_url.startswith("https://"):
        shop_url = f"https://{shop_url.lstrip('/')}"

    # TickBags' core Shopify reads still rely on the legacy private-app style token.
    if legacy_password:
        api_version = get_graphql_api_version()
        admin_api_base = f"{shop_url.rstrip('/')}/admin/api/{api_version}"
        shopify.ShopifyResource.set_site(admin_api_base)
        if api_key:
            shopify.ShopifyResource.set_user(api_key)
        shopify.ShopifyResource.set_password(legacy_password)
        return

    try:
        session_obj = shopify.Session(shop_url, get_graphql_api_version(), oauth_token)
        shopify.ShopifyResource.activate_session(session_obj)
        return
    except Exception as error:
        print(f"Could not activate Shopify session: {error}")


def _sleep_for_shopify_rate_limit(error, default_seconds: float = 2.0) -> float:
    retry_after = None
    response = getattr(error, "response", None)
    headers = getattr(response, "headers", None) if response is not None else None
    if headers:
        retry_after = headers.get("retry-after") or headers.get("Retry-After")
    try:
        return float(retry_after or default_seconds)
    except (TypeError, ValueError):
        return float(default_seconds)


def shopify_admin_find_with_retry(label, finder, *args, **kwargs):
    attempts = 0
    while attempts < 4:
        with shopify_admin_call_lock:
            try:
                return finder(*args, **kwargs)
            except Exception as error:
                attempts += 1
                message = str(error)
                if "429" in message or "Too Many Requests" in message or "Exceeded 2 calls per second" in message:
                    wait_seconds = _sleep_for_shopify_rate_limit(error, 2 + attempts)
                    print(f"Shopify rate limit while fetching {label}; sleeping {wait_seconds:.1f}s before retry.")
                    time.sleep(wait_seconds)
                    continue
                raise
    raise RuntimeError(f"Shopify rate limit persisted while fetching {label}")


# ── Leopards tracking ─────────────────────────────────────────────────────────

async def fetch_tracking_data_bulk(session, tracking_numbers):
    if not tracking_numbers:
        return {}

    api_key = os.getenv('LEOPARD_API_KEY')
    api_password = os.getenv('LEOPARD_PASSWORD')
    joined = ','.join(tracking_numbers)
    url = (
        f"https://merchantapi.leopardscourier.com/api/trackBookedPacket/"
        f"?api_key={api_key}&api_password={api_password}&track_numbers={joined}"
    )
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    try:
        async with session.get(url, ssl=ssl_context) as response:
            data = await response.json()

        result = {}
        if data.get('status') == 1 and not data.get('error'):
            for packet in data.get('packet_list', []):
                cn = packet.get('track_number')
                if cn:
                    result[cn] = packet
        return result
    except Exception as e:
        print(f"Error in fetch_tracking_data_bulk: {e}")
        return {}


def parse_leopards_status(packet, tracking_number):
    """Extract human-readable shipment status from a Leopards packet dict."""
    if not packet:
        return {
            'tracking_number': tracking_number,
            'status': 'Booked',
            'name': None, 'address': None, 'city': None, 'phone': None,
        }

    name    = packet.get('consignment_name_eng') or None
    address = packet.get('consignment_address') or None
    phone   = packet.get('consignment_phone') or None
    city    = packet.get('destination_city_name') or None
    tracking_details = packet.get('Tracking Detail', [])

    if tracking_details:
        last_tracking = tracking_details[-1]
        final_status  = last_tracking.get('Status', 'Unknown')
        reason        = last_tracking.get('Reason')
        if reason and reason != 'N/A':
            final_status += f" - {reason}"

        keywords = ["Return", "hold", "UNTRACEABLE"]
        for detail in tracking_details:
            status = detail['Status']
            reason = detail.get('Reason', 'N/A')
            if any(kw in status for kw in keywords) or any(kw in (reason or '') for kw in keywords):
                final_status = f"Being Return {reason}" if reason and reason != "N/A" else "Being Return"
                if "Returned to shipper" in packet.get('booked_packet_status', ''):
                    final_status = "RETURNED TO SHIPPER"
                break
            elif reason and reason != "N/A" and reason not in final_status:
                final_status += f" - {reason}"
            if "Returned to shipper" in packet.get('booked_packet_status', ''):
                final_status = "RETURNED TO SHIPPER"
    else:
        final_status = packet.get('booked_packet_status', 'Booked')
        if "Pickup Request not Send" in final_status:
            final_status = "Booked"

    return {
        'tracking_number': tracking_number,
        'status': final_status,
        'name': name, 'address': address, 'city': city, 'phone': phone,
    }


def process_line_item(line_item, fulfillments, tracking_cache, billing):
    """Returns tracking/status data for a line item while keeping Shopify as the customer source."""
    if line_item.fulfillment_status is None and line_item.fulfillable_quantity == 0:
        return []

    tracking_info = []

    if line_item.fulfillment_status == "fulfilled":
        for fulfillment in fulfillments:
            if fulfillment.status == "cancelled":
                continue
            for item in fulfillment.line_items:
                if item.id != line_item.id:
                    continue

                tracking_number = fulfillment.tracking_number
                tracking_urls = getattr(fulfillment, 'tracking_urls', None) or []
                tracking_url = getattr(fulfillment, 'tracking_url', None) or (tracking_urls[0] if tracking_urls else '')
                packet = tracking_cache.get(tracking_number)
                parsed = parse_leopards_status(packet, tracking_number)

                # Fall back to Shopify billing address when Leopards has no data
                tracking_info.append({
                    'tracking_number': tracking_number,
                    'tracking_url': tracking_url,
                    'status':   parsed['status'],
                    'quantity': item.quantity,
                    'name':     billing.get('name',    'N/A'),
                    'address':  billing.get('address', 'N/A'),
                    'city':     billing.get('city',    'N/A'),
                    'phone':    billing.get('phone',   'N/A'),
                })

    return tracking_info if tracking_info else [{
        'tracking_number': 'N/A',
        'tracking_url': '',
        'status':   'Un-Booked',
        'name':     billing.get('name',    'N/A'),
        'address':  billing.get('address', 'N/A'),
        'phone':    billing.get('phone',   'N/A'),
        'city':     billing.get('city',    'N/A'),
        'quantity': line_item.quantity,
    }]


def merge_customer_details(base: dict[str, str], override: dict[str, str]) -> dict[str, str]:
    base = base or {}
    override = override or {}
    return {
        "name": override.get("name") or base.get("name", ""),
        "address": override.get("address") or base.get("address", ""),
        "city": override.get("city") or base.get("city", ""),
        "phone": override.get("phone") or base.get("phone", ""),
    }


def extract_shopify_customer_details(order) -> dict[str, str]:
    def safe_attr(obj, attr):
        try:
            if isinstance(obj, dict):
                value = obj.get(attr)
                return value or ""
            value = getattr(obj, attr)
            return value or ""
        except (AttributeError, TypeError):
            return ""

    shipping = getattr(order, 'shipping_address', None)
    billing = getattr(order, 'billing_address', None)
    customer = getattr(order, 'customer', None)
    default_address = safe_attr(customer, 'default_address')

    shipping_name = safe_attr(shipping, 'name')
    billing_name = safe_attr(billing, 'name')
    default_name = safe_attr(default_address, 'name')
    first_name = (
        safe_attr(shipping, 'first_name')
        or safe_attr(billing, 'first_name')
        or safe_attr(customer, 'first_name')
    )
    last_name = (
        safe_attr(shipping, 'last_name')
        or safe_attr(billing, 'last_name')
        or safe_attr(customer, 'last_name')
    )
    composed_name = " ".join(part for part in [first_name, last_name] if part).strip()

    return {
        "name": shipping_name or billing_name or default_name or composed_name,
        "address": safe_attr(shipping, 'address1') or safe_attr(billing, 'address1') or safe_attr(default_address, 'address1'),
        "city": safe_attr(shipping, 'city') or safe_attr(billing, 'city') or safe_attr(default_address, 'city'),
        "phone": (
            safe_attr(order, 'phone')
            or safe_attr(shipping, 'phone')
            or safe_attr(billing, 'phone')
            or safe_attr(default_address, 'phone')
            or safe_attr(customer, 'phone')
        ),
    }


def enrich_orders_with_protected_customer_data(orders: list[dict[str, object]]) -> list[dict[str, object]]:
    shopify_orders = [order for order in orders if order.get("id")]
    if not shopify_orders:
        return orders

    try:
        protected_map, errors = fetch_protected_order_details([order["id"] for order in shopify_orders])
        if errors:
            print(f"Shopify protected data GraphQL warnings: {errors}")
    except Exception as e:
        print(f"Shopify protected data enrichment failed: {e}")
        return orders

    for order in shopify_orders:
        protected = protected_map.get(str(order.get("id")))
        if not protected:
            continue

        merged_details = merge_customer_details(order.get("customer_details") or {}, protected)
        order["customer_details"] = merged_details

        for item in order.get("line_items", []):
            item["name"] = protected.get("name") or item.get("name", "N/A")
            item["address"] = protected.get("address") or item.get("address", "N/A")
            item["city"] = protected.get("city") or item.get("city", "N/A")
            item["phone"] = protected.get("phone") or item.get("phone", "N/A")

    return orders


def _missing_customer_details(details: dict[str, str]) -> bool:
    details = details or {}
    return not all([
        (details.get("name") or "").strip(),
        (details.get("address") or "").strip(),
        (details.get("phone") or "").strip(),
    ])


def _details_from_shopify_rest_order(payload: dict[str, object]) -> dict[str, str]:
    def pick(*values):
        for value in values:
            text = str(value or "").strip()
            if text and text.upper() != "N/A":
                return text
        return ""

    shipping = payload.get("shipping_address") or {}
    billing = payload.get("billing_address") or {}
    customer = payload.get("customer") or {}
    default_address = customer.get("default_address") or {}

    name = pick(
        shipping.get("name"),
        billing.get("name"),
        " ".join(part for part in [customer.get("first_name"), customer.get("last_name")] if part),
        default_address.get("name"),
    )
    address = pick(
        shipping.get("address1"),
        billing.get("address1"),
        default_address.get("address1"),
    )
    city = pick(shipping.get("city"), billing.get("city"), default_address.get("city"))
    phone = pick(
        payload.get("phone"),
        shipping.get("phone"),
        billing.get("phone"),
        customer.get("phone"),
        default_address.get("phone"),
    )
    return {"name": name, "address": address, "city": city, "phone": phone}


def enrich_missing_customer_data_from_rest(orders: list[dict[str, object]]) -> list[dict[str, object]]:
    missing_orders = [
        order for order in orders
        if order.get("id") and _missing_customer_details(order.get("customer_details") or {})
    ]
    if not missing_orders:
        return orders

    try:
        base_url = get_shopify_rest_base_url()
        headers = shopify_rest_headers()
    except Exception as e:
        print(f"Shopify REST customer enrichment unavailable: {e}")
        return orders

    order_lookup = {str(order["id"]): order for order in missing_orders if order.get("id")}
    missing_ids = list(order_lookup.keys())
    enriched_count = 0
    batch_size = 20

    for index in range(0, len(missing_ids), batch_size):
        batch_ids = missing_ids[index:index + batch_size]
        params = {
            "ids": ",".join(batch_ids),
            "fields": "id,name,phone,customer,shipping_address,billing_address",
            "limit": len(batch_ids),
            "status": "any",
        }
        attempt = 0
        payload_orders: list[dict[str, object]] = []

        while attempt < 4:
            try:
                response = requests.get(
                    f"{base_url}/orders.json",
                    params=params,
                    headers=headers,
                    timeout=30,
                )
                if response.status_code == 429:
                    retry_after = response.headers.get("Retry-After")
                    wait_seconds = float(retry_after or 2 + attempt)
                    print(
                        f"Shopify REST customer enrichment rate-limited for batch "
                        f"{index // batch_size + 1}; waiting {wait_seconds:.1f}s before retry."
                    )
                    time.sleep(wait_seconds)
                    attempt += 1
                    continue

                response.raise_for_status()
                payload_orders = (response.json() or {}).get("orders") or []
                break
            except Exception as e:
                attempt += 1
                if attempt >= 4:
                    print(
                        f"Shopify REST customer enrichment failed for batch "
                        f"{index // batch_size + 1} ({','.join(batch_ids)}): {e}"
                    )
                    payload_orders = []
                    break
                time.sleep(1.5 * attempt)

        for payload_order in payload_orders:
            order_id = str(payload_order.get("id") or "").strip()
            if not order_id or order_id not in order_lookup:
                continue

            order = order_lookup[order_id]
            details = _details_from_shopify_rest_order(payload_order)
            if not any(details.values()):
                continue

            merged_details = merge_customer_details(order.get("customer_details") or {}, details)
            if merged_details != (order.get("customer_details") or {}):
                enriched_count += 1
            order["customer_details"] = merged_details
            for item in order.get("line_items", []):
                item["name"] = details.get("name") or item.get("name", "")
                item["address"] = details.get("address") or item.get("address", "")
                item["city"] = details.get("city") or item.get("city", "")
                item["phone"] = details.get("phone") or item.get("phone", "")

    print(f"Shopify REST customer enrichment filled {enriched_count} / {len(missing_orders)} missing orders.")
    return orders


async def process_order(order, tracking_cache):
    global LAST_REQUEST_TIME

    elapsed_time = time.time() - LAST_REQUEST_TIME
    if elapsed_time < 1 / RATE_LIMIT:
        await asyncio.sleep((1 / RATE_LIMIT) - elapsed_time)
    LAST_REQUEST_TIME = time.time()

    input_datetime_str = order.created_at
    parsed_datetime    = datetime.fromisoformat(input_datetime_str[:-6])
    formatted_datetime = parsed_datetime.isoformat()

    try:
        status = (order.fulfillment_status).title()
    except:
        status = "Un-fulfilled"

    customer_details = extract_shopify_customer_details(order)
    billing = customer_details

    order_info = {
        'order_link':         "https://admin.shopify.com/store/tick-bags-best-bean-bags-in-pakistan/orders/" + str(order.id),
        'order_id':           order.name,
        'tracking_id':        'N/A',
        'created_at':         formatted_datetime,
        'total_price':        order.total_price,
        'subtotal_price':     getattr(order, 'subtotal_price', 0) or 0,
        'shipping_charges':   getattr(getattr(order, 'total_shipping_price_set', None), 'shop_money', None).amount if getattr(getattr(order, 'total_shipping_price_set', None), 'shop_money', None) else 0,
        'total_discounts':    getattr(order, 'total_discounts', 0) or 0,
        'line_items':         [],
        'financial_status':   (order.financial_status).title(),
        'fulfillment_status': status,
        'customer_details':   customer_details,
        'tags':               [tag for tag in order.tags.split(", ") if tag != "Leopards Courier"],
        'id':                 order.id,
        'status':             'Un-Booked',
    }

    variant_name = ""
    image_src = "https://static.thenounproject.com/png/1578832-200.png"

    for line_item in order.line_items:
        tracking_info_list = process_line_item(line_item, order.fulfillments, tracking_cache, billing)

        if not tracking_info_list:
            continue

        if line_item.product_id is not None:
            cache_key = (line_item.product_id, line_item.variant_id)
            if cache_key in product_image_cache:
                image_src, variant_name = product_image_cache[cache_key]
            else:
                try:
                    await asyncio.sleep(0.3)
                    product = shopify_admin_find_with_retry(
                        f"product {line_item.product_id}",
                        shopify.Product.find,
                        line_item.product_id,
                    )
                    if product and product.variants:
                        for variant in product.variants:
                            if variant.id == line_item.variant_id:
                                if variant.image_id is not None:
                                    await asyncio.sleep(0.3)
                                    images = shopify_admin_find_with_retry(
                                        f"product image {variant.image_id} for product {line_item.product_id}",
                                        shopify.Image.find,
                                        image_id=variant.image_id,
                                        product_id=line_item.product_id,
                                    )
                                    variant_name = line_item.variant_title
                                    for image in images:
                                        if image.id == variant.image_id:
                                            image_src = image.src
                                else:
                                    variant_name = ""
                                    image_src = product.image.src if product.image else image_src
                    product_image_cache[cache_key] = (image_src, variant_name)
                except Exception as e:
                    print(f"Error fetching product {line_item.product_id}: {e}")
        else:
            image_src = "https://static.thenounproject.com/png/1578832-200.png"

        for info in tracking_info_list:
            unit_price = 0
            try:
                unit_price = float(line_item.price or 0)
            except Exception:
                unit_price = 0
            order_info['line_items'].append({
                'fulfillment_status': line_item.fulfillment_status,
                'image_src':          image_src,
                'product_title':      line_item.title + (" - " + variant_name if variant_name else ""),
                'quantity':           info['quantity'],
                'unit_price':         unit_price,
                'line_total':         unit_price * float(info['quantity'] or 0),
                'tracking_number':    info['tracking_number'],
                'tracking_url':       info.get('tracking_url', ''),
                'status':             info['status'],
                'name':               info.get('name', 'N/A'),
                'address':            info.get('address', 'N/A'),
                'city':               info.get('city', 'N/A'),
                'phone':              info.get('phone', 'N/A'),
            })
            order_info['status'] = info['status']

    return order_info


# ── Loadsheet ─────────────────────────────────────────────────────────────────

@app.route('/generate_loadsheet', methods=['POST'])
def generate_loadsheet():
    data = request.json
    cn_numbers = data.get("cn_numbers", [])

    if not cn_numbers:
        return jsonify({"error": "No CN numbers provided"}), 400

    api_key = os.getenv('LEOPARD_API_KEY')
    api_password = os.getenv('LEOPARD_PASSWORD')
    url = "https://merchantapi.leopardscourier.com/api/generateLoadSheet/"

    payload = {
        "api_key": api_key,
        "api_password": api_password,
        "cn_numbers": cn_numbers,
        "courier_name": "1",
        "courier_code": "1"
    }

    try:
        response = requests.post(url, json=payload)
        return jsonify(response.json())
    except requests.exceptions.RequestException as e:
        return jsonify({"error": "Failed to connect to the API"}), 500


# ── Shopify orders fetch ───────────────────────────────────────────────────────

def get_shopify_order_fetch_start_date():
    raw_date = (os.getenv('SHOPIFY_ORDER_FETCH_START_DATE') or '2024-09-01').strip()
    try:
        return datetime.fromisoformat(raw_date).isoformat()
    except ValueError:
        print(f"Invalid SHOPIFY_ORDER_FETCH_START_DATE '{raw_date}', using 2024-09-01.")
        return datetime(2024, 9, 1).isoformat()


def get_shopify_order_fetch_status():
    requested = (os.getenv('SHOPIFY_ORDER_FETCH_STATUS') or 'open').strip().lower() or 'open'
    if requested == 'any' and os.getenv('SHOPIFY_ALLOW_ALL_ORDER_FETCH', '').strip() != '1':
        print("SHOPIFY_ORDER_FETCH_STATUS=any ignored; using open. Set SHOPIFY_ALLOW_ALL_ORDER_FETCH=1 to fetch all orders.")
        return 'open'
    return requested


def fetch_all_shopify_orders(start_date, fetch_status):
    all_orders = []
    try:
        orders = shopify.Order.find(
            limit=250,
            order="created_at DESC",
            created_at_min=start_date,
            status=fetch_status,
        )
    except Exception as e:
        print(f"Error fetching orders: {e}")
        return []

    page_count = 0
    while True:
        page_orders = list(orders)
        page_count += 1
        all_orders.extend(page_orders)
        try:
            if not orders.has_next_page():
                break
            orders = orders.next_page()
        except Exception as e:
            print(f"Error fetching next page: {e}")
            break

    print(
        f"Fetched {len(all_orders)} Shopify orders across {page_count} page(s) "
        f"with status={fetch_status}, created_at_min={start_date}."
    )
    return all_orders


async def getShopifyOrders(force_status=None):
    start_date = get_shopify_order_fetch_start_date()
    fetch_status = (force_status or get_shopify_order_fetch_status() or 'any').strip().lower() or 'any'
    result = []
    total_start = time.time()

    all_orders = fetch_all_shopify_orders(start_date, fetch_status)
    if not all_orders:
        return []

    all_tracking_numbers = []
    for order in all_orders:
        for fulfillment in order.fulfillments:
            if fulfillment.status == "cancelled":
                continue
            tn = fulfillment.tracking_number
            if tn and tn not in all_tracking_numbers:
                all_tracking_numbers.append(tn)

    print(f"Found {len(all_tracking_numbers)} unique tracking numbers.")

    tracking_cache = {}
    async with aiohttp.ClientSession() as session:
        chunks = [all_tracking_numbers[i:i+50] for i in range(0, len(all_tracking_numbers), 50)]
        print(f"Fetching tracking data in {len(chunks)} bulk API calls...")
        for idx, chunk in enumerate(chunks):
            batch_result = await fetch_tracking_data_bulk(session, chunk)
            tracking_cache.update(batch_result)
            print(f"  Bulk call {idx+1}/{len(chunks)} — got {len(batch_result)} results")

    print(f"Tracking cache built: {len(tracking_cache)} CNs resolved.")

    semaphore = asyncio.Semaphore(2)

    async def process_with_semaphore(order):
        async with semaphore:
            await asyncio.sleep(0.5)
            return await process_order(order, tracking_cache)

    tasks   = [process_with_semaphore(o) for o in all_orders]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for r in results:
        if isinstance(r, Exception):
            print(f"Error processing order: {r}")
        else:
            result.append(r)

    try:
        result = enrich_orders_with_protected_customer_data(result)
    except Exception as e:
        print(f"Continuing without protected customer enrichment: {e}")

    try:
        result = enrich_missing_customer_data_from_rest(result)
    except Exception as e:
        print(f"Continuing without Shopify REST customer enrichment: {e}")

    print(f"Processed {len(result)} orders in {time.time() - total_start:.2f}s")
    return result


def refresh_daraz_cache_if_needed(force: bool = False) -> list[dict]:
    global daraz_orders
    if daraz_orders and not force:
        return daraz_orders
    try:
        statuses = ['shipped', 'pending', 'ready_to_ship', 'packed']
        refreshed = get_daraz_orders(statuses)
        if refreshed or force:
            daraz_orders = refreshed
        return daraz_orders
    except Exception as e:
        print(f"Could not refresh Daraz cache: {e}")
        return daraz_orders


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def tracking():
    global order_details, daraz_orders
    if not daraz_orders:
        refresh_daraz_cache_if_needed()
    return render_template(
        "track.html",
        order_details=order_details,
        darazOrders=daraz_orders,
        employee_approvals=build_employee_approval_items(),
    )


def build_admin_mobile_sections():
    return [
        {'id': 'dashboard', 'label': 'Dashboard', 'icon': '🏠', 'src': '/?embedded=1'},
        {'id': 'inbox', 'label': 'Inbox', 'icon': '💬', 'src': '/whatsapp/inbox?embedded=1'},
        {'id': 'payments', 'label': 'Payments', 'icon': '💰', 'src': '/payments?embedded=1'},
        {'id': 'scanner', 'label': 'Scanner', 'icon': '🔍', 'src': '/employee_portal'},
        {'id': 'employee-orders', 'label': 'Orders', 'icon': '🧾', 'src': '/employee_portal/orders'},
        {'id': 'pending', 'label': 'Pending', 'icon': '📋', 'src': '/pending?embedded=1'},
        {'id': 'undelivered', 'label': 'Undelivered', 'icon': '🚚', 'src': '/?embedded=1&preset=undelivered'},
    ]


@app.route('/admin_portal', methods=['GET', 'POST'])
def admin_portal():
    selected = (request.values.get('section') or 'dashboard').strip().lower()
    sections = build_admin_mobile_sections()
    section_ids = {section['id'] for section in sections}
    if selected not in section_ids:
        selected = 'dashboard'

    if request.method == 'POST':
        submitted_password = (request.form.get('password') or '').strip()
        if submitted_password == ADMIN_PORTAL_PASSWORD:
            session[ADMIN_PORTAL_SESSION_KEY] = True
            return redirect(url_for('admin_portal', section=selected))
        return render_template(
            'admin_portal.html',
            view='login',
            login_error='Wrong password. Try again.',
            sections=sections,
            selected_section=selected,
        ), 401

    if not admin_portal_is_authenticated():
        return render_template(
            'admin_portal.html',
            view='login',
            login_error='',
            sections=sections,
            selected_section=selected,
        )

    return render_template(
        'admin_portal.html',
        view='portal',
        sections=sections,
        selected_section=selected,
        employee_approvals=build_employee_approval_items(),
    )


@app.route('/admin_portal/logout', methods=['POST'])
def admin_portal_logout():
    session.pop(ADMIN_PORTAL_SESSION_KEY, None)
    return redirect(url_for('admin_portal'))


@app.route('/admin_portal-manifest.webmanifest')
def admin_portal_manifest():
    return send_from_directory('static', 'admin-portal.webmanifest', mimetype='application/manifest+json')


@app.route('/admin_portal-sw.js')
def admin_portal_service_worker():
    return send_from_directory('static', 'admin-portal-sw.js', mimetype='application/javascript')


@app.route('/refresh', methods=['POST'])
def refresh_data():
    global order_details, daraz_orders
    try:
        refreshed_orders = asyncio.run(getShopifyOrders())
        refresh_daraz_cache_if_needed(force=True)
        if refreshed_orders:
            order_details = refreshed_orders
            return jsonify({'message': 'Data refreshed successfully', 'count': len(order_details), 'daraz_count': len(daraz_orders)})
        return jsonify({'message': 'Refresh returned no Shopify orders; keeping existing data.', 'count': len(order_details), 'daraz_count': len(daraz_orders)}), 502
    except Exception as e:
        print(f"Error refreshing data: {e}")
        return jsonify({'message': 'Failed to refresh data'}), 500


@app.route('/shopify/protected-data/status')
def shopify_protected_data_status():
    return jsonify(get_protected_data_config_status())


@app.route('/shopify/protected-data/reset', methods=['POST'])
def shopify_protected_data_reset():
    clear_last_good = str(request.args.get('clear_last_good') or '').strip().lower() in {'1', 'true', 'yes'}
    try:
        clear_offline_token_state(clear_last_good=clear_last_good)
        session.pop(SHOPIFY_OAUTH_STATE_SESSION_KEY, None)
        return jsonify({
            "success": True,
            "message": "Shopify protected-data token state cleared.",
            "install_url": f"{request.host_url.rstrip('/')}/shopify/install",
            "cleared_last_good": clear_last_good,
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/shopify/install')
def shopify_install():
    state = create_oauth_state()
    session[SHOPIFY_OAUTH_STATE_SESSION_KEY] = state
    return redirect(get_install_url(state))


@app.route('/shopify/callback')
def shopify_callback():
    expected_state = session.get(SHOPIFY_OAUTH_STATE_SESSION_KEY)
    provided_state = request.args.get('state', '')
    shop = (request.args.get('shop') or '').strip().lower()
    code = (request.args.get('code') or '').strip()
    configured_shop = get_shop_domain()

    hmac_valid = verify_oauth_hmac(request.query_string)
    state_valid = bool(expected_state and expected_state == provided_state)
    shop_valid = bool(shop and configured_shop and shop == configured_shop)

    if not hmac_valid:
        if not (state_valid and shop_valid):
            return jsonify({"success": False, "error": "Invalid Shopify callback signature"}), 400
        print("Shopify OAuth callback proceeding with state+shop fallback after HMAC verification failed.")

    if not state_valid:
        return jsonify({"success": False, "error": "Invalid Shopify OAuth state"}), 400

    if not shop_valid:
        return jsonify({"success": False, "error": "OAuth callback shop does not match configured shop"}), 400

    if not code:
        return jsonify({"success": False, "error": "Missing Shopify OAuth code"}), 400

    try:
        payload = exchange_oauth_code_for_token(shop, code)
        print(
            "Shopify OAuth callback grant received:",
            {
                "shop": shop,
                "scopes": payload.get("scope") or payload.get("associated_user_scope") or "",
                "has_refresh_token": bool(payload.get("refresh_token")),
                "expires_in": payload.get("expires_in"),
            },
        )
        save_offline_token(shop, payload)
        session.pop(SHOPIFY_OAUTH_STATE_SESSION_KEY, None)
        return redirect('/shopify/protected-data/status?connected=1')
    except Exception as e:
        return jsonify({"success": False, "error": f"Shopify token exchange failed: {e}"}), 400


@app.route('/api/refresh-tracking', methods=['POST'])
def refresh_tracking_only():
    """Lightweight endpoint: re-fetches Leopards tracking for non-final orders only."""
    global order_details

    final_states = {"RETURNED TO SHIPPER", "Delivered", "Refused by consignee"}

    active_cns = []
    for order in order_details:
        if order.get('status') in final_states:
            continue
        for item in order.get('line_items', []):
            cn = item.get('tracking_number')
            if cn and cn != 'N/A' and cn not in active_cns:
                active_cns.append(cn)

    if not active_cns:
        return jsonify({'updated': 0})

    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    updated = 0
    api_key = os.getenv('LEOPARD_API_KEY')
    api_password = os.getenv('LEOPARD_PASSWORD')

    chunks = [active_cns[i:i+50] for i in range(0, len(active_cns), 50)]
    tracking_cache = {}

    for chunk in chunks:
        joined = ','.join(chunk)
        url = (
            f"https://merchantapi.leopardscourier.com/api/trackBookedPacket/"
            f"?api_key={api_key}&api_password={api_password}&track_numbers={joined}"
        )
        try:
            r = requests.get(url, verify=False, timeout=30)
            data = r.json()
            if data.get('status') == 1:
                for packet in data.get('packet_list', []):
                    cn = packet.get('track_number')
                    if cn:
                        tracking_cache[cn] = packet
        except Exception as e:
            print(f"Tracking refresh chunk error: {e}")

    for order in order_details:
        if order.get('status') in final_states:
            continue
        for item in order.get('line_items', []):
            cn = item.get('tracking_number')
            if cn and cn in tracking_cache:
                parsed = parse_leopards_status(tracking_cache[cn], cn)
                item['status'] = parsed['status']
                order['status'] = parsed['status']
                updated += 1

    return jsonify({'updated': updated})


@app.route('/apply_tag', methods=['POST'])
def apply_tag():
    data = request.json
    order_id = data.get('order_id')
    tag = data.get('tag')

    today_date = datetime.now().strftime('%Y-%m-%d')
    tag_with_date = f"{tag.strip()} ({today_date})"

    try:
        order = shopify.Order.find(order_id)

        if tag.strip().lower() == "returned":
            if order.cancel():
                print("Order Cancelled")
        if tag.strip().lower() == "delivered":
            if order.close():
                print("Order Closed")

        tags = [t.strip() for t in order.tags.split(", ")] if order.tags else []
        if "Leopards Courier" in tags:
            tags.remove("Leopards Courier")
        if tag_with_date not in tags:
            tags.append(tag_with_date)

        order.tags = ", ".join(tags)

        if order.save():
            return jsonify({"success": True, "message": "Tag applied successfully."})
        else:
            return jsonify({"success": False, "error": "Failed to save order changes."})
    except Exception as e:
        print(f"Error applying tag: {e}")
        return jsonify({"success": False, "error": str(e)})


async def limited_request(coroutine, semaphore):
    async with semaphore:
        await asyncio.sleep(0.5)
        return await coroutine


def normalize_scan_term(term):
    return (term or "").strip().lower().replace("#", "")


def is_lahore_city(city):
    normalized = (city or "").strip().lower()
    return "lahore" in normalized or "lhr" in normalized


def find_shopify_order_by_order_name(order_id):
    normalized_order_id = normalize_scan_term(order_id)
    for order in order_details:
        if normalize_scan_term(order.get("order_id")) == normalized_order_id:
            return order
    return None


def split_customer_name(name):
    parts = [part for part in str(name or '').strip().split() if part]
    if not parts:
        return '', 'Customer'
    if len(parts) == 1:
        return parts[0], 'Customer'
    return parts[0], " ".join(parts[1:])


def normalize_pk_phone(phone):
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


def parse_money(value, default=0.0):
    try:
        return round(float(value or default), 2)
    except (TypeError, ValueError):
        return round(float(default), 2)


def format_employee_order_note(
    payment_method,
    delivery_method,
    customer_phone,
    discount_amount,
    delivery_charges,
    advance_amount,
    custom_items,
    extra_notes='',
):
    lines = [
        f'Delivery method: {delivery_method or "Not specified"}',
        f'Phone: {customer_phone or "Not provided"}',
    ]
    if (payment_method or '').strip().lower() == 'partial':
        lines.append('Payment method: Partial')
    if discount_amount:
        lines.append(f'Discount amount: PKR {discount_amount}')
    if advance_amount:
        lines.append(f'Advance paid by customer: PKR {advance_amount}')
    if custom_items:
        lines.append('Custom items:')
        for item in custom_items:
            lines.append(
                f"- {item.get('title', 'Custom item')} | Qty {item.get('quantity', 1)} | "
                f"PKR {item.get('price', 0)} | Image {'Uploaded in portal' if item.get('image') else 'N/A'}"
            )
    if extra_notes:
        lines.append(f'Notes: {extra_notes}')
    return "\n".join(lines)


def get_active_shopify_products(limit=120):
    try:
        products = shopify_admin_find_with_retry(
            f"published products list (limit={limit})",
            shopify.Product.find,
            limit=limit,
            published_status='published',
        )
    except Exception as e:
        print(f"Could not fetch Shopify products: {e}")
        return []

    results = []
    for product in products:
        if getattr(product, 'status', 'active') != 'active':
            continue
        base_image = product.image.src if getattr(product, 'image', None) else ''
        product_images = list(getattr(product, 'images', []) or [])
        fetched_images = False
        for variant in getattr(product, 'variants', []) or []:
            variant_title = getattr(variant, 'title', '') or ''
            display_title = product.title if variant_title in {'Default Title', ''} else f"{product.title} - {variant_title}"
            variant_image = base_image
            variant_image_id = getattr(variant, 'image_id', None)

            if variant_image_id and not product_images and not fetched_images:
                try:
                    product_images = list(
                        shopify_admin_find_with_retry(
                            f"product images for product {getattr(product, 'id', None)}",
                            shopify.Image.find,
                            product_id=getattr(product, 'id', None),
                        ) or []
                    )
                except Exception as e:
                    print(f"Could not fetch images for Shopify product {getattr(product, 'id', None)}: {e}")
                    product_images = []
                fetched_images = True

            if variant_image_id and product_images:
                for image in product_images:
                    if getattr(image, 'id', None) == variant_image_id:
                        variant_image = getattr(image, 'src', '') or base_image
                        break
                    attached_variant_ids = getattr(image, 'variant_ids', None) or []
                    if getattr(variant, 'id', None) in attached_variant_ids:
                        variant_image = getattr(image, 'src', '') or base_image
                        break

            results.append({
                'product_id': getattr(product, 'id', None),
                'variant_id': getattr(variant, 'id', None),
                'title': display_title,
                'product_title': getattr(product, 'title', ''),
                'variant_title': variant_title,
                'product_description': getattr(product, 'body_html', '') or '',
                'product_vendor': getattr(product, 'vendor', '') or '',
                'product_type': getattr(product, 'product_type', '') or '',
                'product_images': [getattr(image, 'src', '') for image in product_images if getattr(image, 'src', '')],
                'price': float(getattr(variant, 'price', 0) or 0),
                'image': variant_image,
                'sku': getattr(variant, 'sku', '') or '',
                'barcode': getattr(variant, 'barcode', '') or '',
            })
    return results


def build_employee_invoice_payload(
    order_name,
    customer_name,
    phone,
    city,
    address,
    payment_method,
    delivery_method,
    catalog_items,
    custom_items,
    discount_amount,
    delivery_charges,
    advance_amount,
):
    items = []
    subtotal = 0.0

    for item in catalog_items:
        quantity = int(item.get('quantity') or 1)
        unit_price = parse_money(item.get('price'))
        line_total = round(unit_price * quantity, 2)
        subtotal += line_total
        items.append({
            'title': item.get('title') or 'Product',
            'quantity': quantity,
            'image': item.get('image') or '',
            'unit_price': unit_price,
            'line_total': line_total,
        })

    for item in custom_items:
        quantity = int(item.get('quantity') or 1)
        unit_price = parse_money(item.get('price'))
        line_total = round(unit_price * quantity, 2)
        subtotal += line_total
        items.append({
            'title': item.get('title') or 'Custom product',
            'quantity': quantity,
            'image': item.get('image') or '',
            'unit_price': unit_price,
            'line_total': line_total,
        })

    total = round(subtotal - discount_amount + delivery_charges, 2)
    balance_due = round(max(total - advance_amount, 0), 2)
    return {
        'order_id': order_name,
        'customer_name': customer_name,
        'customer_phone': phone,
        'customer_city': city,
        'customer_address': address,
        'status': 'Created',
        'summary_lines': [
            'Shopify order',
            f'Payment: {payment_method or "Not specified"}',
            f'Delivery: {delivery_method or "Not specified"}',
        ],
        'items': items,
        'totals': {
            'subtotal': round(subtotal, 2),
            'discount': round(discount_amount, 2),
            'delivery_charges': round(delivery_charges, 2),
            'total': round(total, 2),
            'advance_paid': round(advance_amount, 2),
            'balance_due': round(balance_due, 2),
        },
    }


def create_shopify_employee_order(payload):
    customer_name = (payload.get('customer_name') or '').strip()
    phone = normalize_pk_phone(payload.get('phone'))
    city = (payload.get('city') or '').strip()
    address = (payload.get('address') or '').strip()
    discount_amount = parse_money(payload.get('discount_amount'))
    delivery_charges = parse_money(payload.get('delivery_charges'))
    payment_method = (payload.get('payment_method') or 'Unpaid').strip()
    delivery_method = (payload.get('delivery_method') or '').strip()
    advance_amount = parse_money(payload.get('advance_amount'))
    catalog_items = payload.get('catalog_items') or []
    custom_items = payload.get('custom_items') or []
    extra_notes = (payload.get('notes') or '').strip()

    if not customer_name:
        raise ValueError('Customer name is required.')
    if not phone:
        raise ValueError('Phone number is required.')
    if payment_method.lower() == 'partial' and advance_amount <= 0:
        raise ValueError('Enter the advance paid amount for partial payment.')
    if payment_method.lower() != 'partial':
        advance_amount = 0

    first_name, last_name = split_customer_name(customer_name)

    line_items = []
    for item in catalog_items:
        variant_id = item.get('variant_id')
        quantity = int(item.get('quantity') or 1)
        if not variant_id or quantity < 1:
            continue
        line_item = {
            'variant_id': int(variant_id),
            'quantity': quantity,
        }
        override_price = parse_money(item.get('price'))
        if override_price > 0:
            line_item['original_unit_price'] = override_price
        line_items.append(line_item)

    normalized_custom_items = []
    for item in custom_items:
        title = (item.get('title') or '').strip()
        if not title:
            continue
        price = parse_money(item.get('price'))
        quantity = int(item.get('quantity') or 1)
        normalized_custom_items.append({
            'title': title,
            'price': price,
            'quantity': quantity,
            'image': (item.get('image') or '').strip(),
        })
        line_items.append({
            'title': title,
            'original_unit_price': price,
            'quantity': quantity,
        })

    if not line_items:
        raise ValueError('At least one product is required.')

    estimated_total = 0.0
    for item in catalog_items:
        estimated_total += parse_money(item.get('price')) * int(item.get('quantity') or 1)
    for item in normalized_custom_items:
        estimated_total += parse_money(item.get('price')) * int(item.get('quantity') or 1)
    estimated_total = round(estimated_total - discount_amount + delivery_charges, 2)
    if advance_amount > estimated_total:
        raise ValueError('Advance paid cannot be greater than the order total.')

    note = format_employee_order_note(
        payment_method=payment_method,
        delivery_method=delivery_method,
        customer_phone=phone,
        discount_amount=discount_amount,
        delivery_charges=delivery_charges,
        advance_amount=advance_amount,
        custom_items=normalized_custom_items,
        extra_notes=extra_notes,
    )

    draft_order = shopify.DraftOrder()
    draft_order.line_items = line_items
    draft_order.note = note
    draft_order.use_customer_default_address = False
    draft_order.shipping_address = {
        'first_name': first_name,
        'last_name': last_name or 'Customer',
        'phone': phone,
        'address1': address,
        'city': city,
        'country': 'Pakistan',
    }
    draft_order.billing_address = draft_order.shipping_address
    draft_order.customer = {
        'first_name': first_name,
        'last_name': last_name or 'Customer',
        'phone': phone,
    }

    if discount_amount > 0:
        draft_order.applied_discount = {
            'description': 'Employee portal discount',
            'value_type': 'fixed_amount',
            'value': discount_amount,
            'amount': discount_amount,
            'title': 'Employee portal discount',
        }

    if delivery_charges > 0:
        draft_order.shipping_line = {
            'title': 'Delivery Charges',
            'price': delivery_charges,
            'custom': True,
        }

    if not draft_order.save():
        raise RuntimeError(json.dumps(getattr(draft_order, 'errors', {}) or {'error': 'Could not save draft order'}))

    try:
        draft_order.complete()
    except Exception as e:
        errors = getattr(draft_order, 'errors', None)
        raise RuntimeError(
            f"Shopify could not complete the employee order: {e or errors or 'Unknown completion error'}"
        )

    try:
        refreshed_draft_order = shopify.DraftOrder.find(draft_order.id)
    except Exception:
        refreshed_draft_order = draft_order

    order_id = (
        getattr(refreshed_draft_order, 'order_id', None)
        or getattr(draft_order, 'order_id', None)
    )
    order_name = (
        getattr(refreshed_draft_order, 'name', '') or getattr(draft_order, 'name', '') or ''
    )
    if not order_id:
        raise RuntimeError('Shopify created the draft, but the completed order ID did not come back. Please check Draft Orders in Shopify.')

    if payment_method.lower() == 'full':
        try:
            mark_shopify_order_as_paid(order_id)
        except Exception as e:
            print(f"Could not immediately mark employee order {order_id} as paid: {e}")

    invoice_payload = build_employee_invoice_payload(
        order_name=order_name,
        customer_name=customer_name,
        phone=phone,
        city=city,
        address=address,
        payment_method=payment_method,
        delivery_method=delivery_method,
        catalog_items=catalog_items,
        custom_items=normalized_custom_items,
        discount_amount=discount_amount,
        delivery_charges=delivery_charges,
        advance_amount=advance_amount if payment_method.lower() == 'partial' else 0,
    )

    return {
        'draft_order_id': getattr(draft_order, 'id', None),
        'order_id': order_id,
        'order_name': order_name,
        'invoice': invoice_payload,
    }


def serialize_shopify_order_for_employee(order):
    customer = order.get('customer_details') or {}
    line_items = order.get('line_items') or []
    return {
        'source': 'shopify',
        'shopify_id': order.get('id'),
        'order_id': str(order.get('order_id', '')),
        'status': order.get('status', ''),
        'customer_name': customer.get('name', ''),
        'customer_phone': customer.get('phone', ''),
        'customer_city': customer.get('city', ''),
        'total_price': order.get('total_price', 0),
        'created_at': order.get('created_at', ''),
        'items': [
            {
                'title': item.get('product_title', ''),
                'quantity': item.get('quantity', 0),
                'image': item.get('image_src', ''),
                'tracking_number': item.get('tracking_number', 'N/A'),
                'status': item.get('status', ''),
            }
            for item in line_items
        ],
    }


def serialize_daraz_order_for_employee(order):
    customer = order.get('customer') or {}
    items_list = order.get('items_list') or []
    return {
        'source': 'daraz',
        'shopify_id': None,
        'order_id': str(order.get('order_id', '')),
        'status': order.get('status', ''),
        'customer_name': customer.get('name', ''),
        'customer_phone': customer.get('phone', ''),
        'customer_city': '',
        'total_price': order.get('total_price', 0),
        'created_at': order.get('date', ''),
        'items': [
            {
                'title': item.get('item_title', ''),
                'quantity': item.get('quantity', 0),
                'image': item.get('item_image', ''),
                'tracking_number': item.get('tracking_number', 'N/A'),
                'status': item.get('status', ''),
            }
            for item in items_list
        ],
    }


def build_employee_portal_orders():
    combined_orders = []
    combined_orders.extend(serialize_shopify_order_for_employee(order) for order in order_details)
    combined_orders.extend(serialize_daraz_order_for_employee(order) for order in daraz_orders)
    return combined_orders


def admin_portal_is_authenticated():
    return bool(session.get(ADMIN_PORTAL_SESSION_KEY))


def employee_portal_is_authenticated():
    return bool(session.get(EMPLOYEE_PORTAL_SESSION_KEY) or session.get(ADMIN_PORTAL_SESSION_KEY))


def employee_portal_safe_next_url(candidate):
    if candidate and str(candidate).startswith('/employee_portal'):
        return candidate
    return url_for('employee_portal')


def build_pending_orders_mobile_data():
    all_orders = []
    statuses = load_order_statuses()

    for daraz_order in daraz_orders:
        if daraz_order['status'] in ['Ready To Ship', 'Pending', 'packed', 'Packed by seller / warehouse']:
            items_with_status = []
            for item in daraz_order['items_list']:
                track_num = item.get('tracking_number', 'N/A')
                key = f"{daraz_order['order_id']}:{track_num}"
                item['applied_status'] = statuses.get(key, "")
                items_with_status.append(item)

            all_orders.append({
                'order_via':  'Daraz',
                'order_id':   daraz_order['order_id'],
                'status':     daraz_order['status'],
                'date':       daraz_order['date'],
                'customer_name': (daraz_order.get('customer') or {}).get('name', ''),
                'customer_phone': (daraz_order.get('customer') or {}).get('phone', ''),
                'customer_address': (daraz_order.get('customer') or {}).get('address', ''),
                'customer_city': '',
                'items_list': items_with_status,
                'subtotal_price': daraz_order['total_price'],
                'shipping_charges': 0,
                'total_discounts': 0,
                'total_price': daraz_order['total_price']
            })

    for shopify_order in order_details:
        if any(tag.startswith("Dispatched") for tag in shopify_order.get('tags', [])):
            continue
        if shopify_order['status'] in ['Booked', 'Un-Booked', 'Drop Off at Express Center']:
            filtered_tags = [tag.strip() for tag in shopify_order.get('tags', []) if tag and tag.strip() != 'Leopards Courier']
            customer_city = ((shopify_order.get('customer_details') or {}).get('city') or '').strip()
            shopify_items = []
            for item in shopify_order['line_items']:
                track_num = item.get('tracking_number', 'N/A')
                key = f"{shopify_order['order_id']}:{track_num}"
                shopify_items.append({
                    'item_image':     item['image_src'],
                    'item_title':     item['product_title'],
                    'quantity':       item['quantity'],
                    'unit_price':     item.get('unit_price', 0),
                    'line_total':     item.get('line_total', 0),
                    'tracking_number': track_num,
                    'status':         item['status'],
                    'applied_status': statuses.get(key, "")
                })
            all_orders.append({
                'order_via':  'Shopify',
                'shopify_id': shopify_order.get('id'),
                'order_id':   shopify_order['order_id'],
                'status':     shopify_order['status'],
                'tags':       filtered_tags,
                'customer_name': (shopify_order.get('customer_details') or {}).get('name', ''),
                'customer_phone': (shopify_order.get('customer_details') or {}).get('phone', ''),
                'customer_address': (shopify_order.get('customer_details') or {}).get('address', ''),
                'customer_city': customer_city,
                'is_lahore':  is_lahore_city(customer_city),
                'date':       shopify_order['created_at'],
                'items_list': shopify_items,
                'subtotal_price': shopify_order.get('subtotal_price', 0),
                'shipping_charges': shopify_order.get('shipping_charges', 0),
                'total_discounts': shopify_order.get('total_discounts', 0),
                'total_price': shopify_order['total_price']
            })

    return all_orders


def build_employee_approval_items():
    approvals = []
    statuses = load_order_statuses()
    approval_statuses = {"Delivered in Lahore", "Cancelled by Employee"}

    for shopify_order in order_details:
        customer = shopify_order.get('customer_details') or {}
        for item in shopify_order.get('line_items', []):
            tracking_number = item.get('tracking_number', 'N/A')
            key = f"{shopify_order['order_id']}:{tracking_number}"
            applied_status = statuses.get(key, "")
            if applied_status not in approval_statuses:
                continue

            customer_name = (
                customer.get('name')
                or item.get('name')
                or ''
            )
            customer_city = (
                customer.get('city')
                or item.get('city')
                or ''
            )
            customer_phone = (
                customer.get('phone')
                or item.get('phone')
                or ''
            )

            approvals.append({
                'shopify_id': shopify_order.get('id'),
                'order_id': shopify_order.get('order_id'),
                'tracking_number': tracking_number,
                'requested_status': applied_status,
                'item_title': item.get('product_title', ''),
                'item_image': item.get('image_src', ''),
                'quantity': item.get('quantity', 0),
                'customer_name': customer_name,
                'customer_city': customer_city,
                'customer_phone': customer_phone,
                'total_price': shopify_order.get('total_price', 0),
                'date': shopify_order.get('created_at', ''),
                'tags': shopify_order.get('tags', []),
            })

    approvals.sort(key=lambda item: item.get('date', ''), reverse=True)
    return approvals


def find_employee_portal_order(term):
    normalized = normalize_scan_term(term)
    if not normalized:
        return None

    for order in build_employee_portal_orders():
        order_number = normalize_scan_term(order.get('order_id'))
        if normalized == order_number or order_number.endswith(normalized):
            return order

        for item in order.get('items', []):
            if normalize_scan_term(item.get('tracking_number')) == normalized:
                return order

    return None


def apply_shopify_order_tag(order_id, tag, include_date=False):
    order = shopify.Order.find(order_id)
    tags = [t.strip() for t in order.tags.split(",")] if order.tags else []
    if "Leopards Courier" in tags:
        tags.remove("Leopards Courier")
    clean_tag = tag.strip()
    if include_date:
        clean_tag = f"{clean_tag} ({datetime.now().strftime('%Y-%m-%d')})"
    if clean_tag not in tags:
        tags.append(clean_tag)
    order.tags = ", ".join(tags)
    return order.save()


def get_shopify_rest_base_url():
    raw_shop_url = (os.getenv('SHOP_URL') or '').strip()
    parsed = urlparse(raw_shop_url if "://" in raw_shop_url else f"https://{raw_shop_url}")
    netloc = parsed.netloc or parsed.path
    if not netloc:
        raise RuntimeError('SHOP_URL is not configured.')
    api_version = os.getenv('SHOPIFY_ADMIN_API_VERSION', '2026-04')
    return f"https://{netloc}/admin/api/{api_version}"


def shopify_rest_headers():
    token = (os.getenv('PASSWORD') or '').strip() or get_graphql_token()
    if not token:
        raise RuntimeError('Shopify admin access token is missing.')
    return {
        'X-Shopify-Access-Token': token,
        'Content-Type': 'application/json',
        'Accept': 'application/json',
    }


def mark_shopify_order_as_paid(order_id):
    token = get_graphql_token()
    endpoint = get_graphql_endpoint()
    if not token or not endpoint:
        raise RuntimeError('Shopify GraphQL payment auth is not configured.')

    mutation = """
    mutation MarkOrderAsPaid($input: OrderMarkAsPaidInput!) {
      orderMarkAsPaid(input: $input) {
        order {
          id
          displayFinancialStatus
        }
        userErrors {
          field
          message
        }
      }
    }
    """
    variables = {
        'input': {
            'id': f'gid://shopify/Order/{int(order_id)}',
        }
    }
    response = requests.post(
        endpoint,
        headers={
            'Content-Type': 'application/json',
            'X-Shopify-Access-Token': token,
        },
        json={'query': mutation, 'variables': variables},
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    errors = payload.get('errors') or []
    if errors:
        raise RuntimeError("; ".join(error.get('message', 'Unknown Shopify GraphQL error') for error in errors))

    result = (payload.get('data') or {}).get('orderMarkAsPaid') or {}
    user_errors = result.get('userErrors') or []
    if user_errors:
        raise RuntimeError("; ".join(error.get('message', 'Unknown Shopify user error') for error in user_errors))
    return result.get('order') or {}


def capture_shopify_payment(order):
    financial_status = ((getattr(order, 'financial_status', '') or '')).lower()
    if financial_status in {'paid', 'partially_paid'}:
        return []

    base_url = get_shopify_rest_base_url()
    headers = shopify_rest_headers()
    response = requests.get(f"{base_url}/orders/{order.id}/transactions.json", headers=headers, timeout=30)
    response.raise_for_status()
    transactions = response.json().get('transactions', [])
    authorization = next(
        (
            transaction for transaction in transactions
            if transaction.get('kind') == 'authorization' and transaction.get('status') == 'success'
        ),
        None
    )

    if not authorization:
        return ['No capturable authorization transaction found.']

    payload = {
        'transaction': {
            'kind': 'capture',
            'parent_id': authorization['id'],
            'amount': str(order.total_price),
            'currency': authorization.get('currency') or getattr(order, 'currency', 'PKR') or 'PKR',
        }
    }
    capture_response = requests.post(
        f"{base_url}/orders/{order.id}/transactions.json",
        headers=headers,
        json=payload,
        timeout=30,
    )
    capture_response.raise_for_status()
    return []


def fulfill_shopify_order(order):
    fulfillment_status = ((getattr(order, 'fulfillment_status', '') or '')).lower()
    if fulfillment_status == 'fulfilled':
        return []

    base_url = get_shopify_rest_base_url()
    headers = shopify_rest_headers()
    response = requests.get(f"{base_url}/orders/{order.id}/fulfillment_orders.json", headers=headers, timeout=30)
    response.raise_for_status()
    fulfillment_orders = response.json().get('fulfillment_orders', [])
    open_fulfillment_orders = [
        {'fulfillment_order_id': fulfillment_order['id']}
        for fulfillment_order in fulfillment_orders
        if fulfillment_order.get('status') not in {'closed', 'cancelled', 'incomplete'}
    ]

    if not open_fulfillment_orders:
        return ['No open fulfillment orders found.']

    payload = {
        'fulfillment': {
            'notify_customer': False,
            'line_items_by_fulfillment_order': open_fulfillment_orders,
        }
    }
    fulfillment_response = requests.post(
        f"{base_url}/fulfillments.json",
        headers=headers,
        json=payload,
        timeout=30,
    )
    fulfillment_response.raise_for_status()
    return []


def approve_shopify_delivery(order):
    warnings = []
    try:
        mark_shopify_order_as_paid(order.id)
    except Exception as e:
        try:
            warnings.extend(capture_shopify_payment(order))
        except Exception as capture_error:
            warnings.append(f'Could not mark order as paid: {e}')
            warnings.append(f'Could not capture payment: {capture_error}')

    try:
        warnings.extend(fulfill_shopify_order(order))
    except Exception as e:
        warnings.append(f'Could not create fulfillment: {e}')

    try:
        close_result = order.close()
        if close_result is False:
            warnings.append('Shopify order close returned false.')
    except Exception as e:
        warnings.append(f'Could not close Shopify order: {e}')

    try:
        if apply_shopify_order_tag(order.id, 'Delivered in Lahore Approved', include_date=True) is False:
            warnings.append('Could not save Delivered in Lahore Approved tag.')
    except Exception as e:
        warnings.append(f'Could not save approval tag: {e}')

    return warnings


def approve_shopify_cancellation(order):
    warnings = []
    cancelled = order.cancel()
    if cancelled is False:
        warnings.append('Shopify order cancel returned false.')

    try:
        if apply_shopify_order_tag(order.id, 'Cancelled by Employee', include_date=True) is False:
            warnings.append('Could not save Cancelled by Employee tag.')
    except Exception as e:
        warnings.append(f'Could not save cancellation tag: {e}')

    return warnings


# ── Costing / profitability helpers ──────────────────────────────────────────

COST_SOURCE_DARAZ = 'daraz'
COST_SOURCE_SHOPIFY = 'shopify'

COST_SOURCE_LABELS = {
    COST_SOURCE_DARAZ: 'Daraz',
    COST_SOURCE_SHOPIFY: 'Shopify',
}

DARAZ_PROFIT_STATUSES = ['shipped', 'delivered']
DEFAULT_DARAZ_WINDOW_DAYS = 60
SHOPIFY_BEANS_PRICE_SETTING_KEY = 'shopify_beans_price_v1'
DEFAULT_SHOPIFY_BEANS_PRICE = 750.0
SHOPIFY_DEFAULT_RETURN_COST = 671.0
SHOPIFY_DEFAULT_ADS_COST = 1200.0


def money_decimal(value) -> Decimal:
    try:
        cleaned = str(value or '0').replace(',', '').strip()
        return Decimal(cleaned if cleaned else '0')
    except Exception:
        return Decimal('0')


def money_float(value) -> float:
    return float(money_decimal(value))


def money_format(value) -> str:
    amount = money_decimal(value).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
    if amount == amount.to_integral():
        return f"Rs {int(amount):,}"
    return f"Rs {amount:,.2f}"


def get_shopify_beans_price() -> float:
    return money_float(get_app_setting(SHOPIFY_BEANS_PRICE_SETTING_KEY, str(DEFAULT_SHOPIFY_BEANS_PRICE)))


def save_shopify_beans_price(value) -> bool:
    return set_app_setting(SHOPIFY_BEANS_PRICE_SETTING_KEY, str(money_float(value)))


def default_cost_calc_fields(source: str) -> dict:
    return {
        'beans_kg': 0.0,
        'fabric_cost': 0.0,
        'yard_qty': 0.0,
        'fusium_cost': 0.0,
        'making_cost': 0.0,
        'overhead_cost': 0.0,
        'delivery_cost': 0.0,
        'return_cost': SHOPIFY_DEFAULT_RETURN_COST if source == COST_SOURCE_SHOPIFY else 0.0,
        'ads_cost': SHOPIFY_DEFAULT_ADS_COST if source == COST_SOURCE_SHOPIFY else 0.0,
        'product_cost': 0.0,
    }


def calculate_shopify_net_cost(row: dict, beans_price=None) -> float:
    beans_rate = money_float(beans_price if beans_price is not None else get_shopify_beans_price())
    beans_value = money_float(row.get('beans_kg')) * beans_rate
    fabric_value = money_float(row.get('fabric_cost')) * money_float(row.get('yard_qty'))
    net_cost = (
        beans_value
        + fabric_value
        + money_float(row.get('fusium_cost'))
        + money_float(row.get('making_cost'))
        + money_float(row.get('overhead_cost'))
        + money_float(row.get('delivery_cost'))
        + money_float(row.get('return_cost'))
        + money_float(row.get('ads_cost'))
    )
    return round(net_cost, 2)


def update_shopify_variant_price(variant_id, price):
    if not variant_id:
        return
    variant = shopify.Variant.find(int(variant_id))
    variant.price = str(money_float(price))
    if not variant.save():
        raise RuntimeError('Shopify price update failed.')


def update_daraz_sku_price(item_id, sku_id, seller_sku, price):
    item_id = str(item_id or '').strip()
    sku_id = str(sku_id or '').strip()
    seller_sku = str(seller_sku or '').strip()
    if not item_id or not sku_id or not seller_sku:
        raise RuntimeError('Daraz item id, sku id, and seller sku are required to update price.')

    req = lazop.LazopRequest('/product/price_quantity/update', 'POST')
    payload = {
        'Request': {
            'Product': {
                'Skus': {
                    'Sku': {
                        'ItemId': item_id,
                        'SkuId': sku_id,
                        'SellerSku': seller_sku,
                        'Price': f"{money_float(price):.2f}",
                    }
                }
            }
        }
    }
    req.add_api_param('payload', json.dumps(payload))
    response = get_daraz_client().execute(req, get_access_token())
    body = response.body or {}
    if str(body.get('code') or response.code or '') not in {'', '0'}:
        raise RuntimeError(body.get('message') or response.message or 'Daraz price update failed.')
    return body


def daraz_api_call(path, method='POST', payload=None):
    req = lazop.LazopRequest(path, method)
    if payload is not None:
        req.add_api_param('payload', payload if isinstance(payload, str) else json.dumps(payload))
    response = get_daraz_client().execute(req, get_access_token())
    body = response.body or {}
    if str(body.get('code') or response.code or '') not in {'', '0'}:
        raise RuntimeError(body.get('message') or response.message or f'Daraz API call failed: {path}')
    return body


def migrate_daraz_image_url(url):
    url = str(url or '').strip()
    if not url:
        return ''
    host = urlparse(url).netloc.lower()
    if 'slatic.net' in host or 'daraz.pk' in host or 'lazada' in host:
        return url
    payload = f'<Request><Image><Url>{xml_escape(url)}</Url></Image></Request>'
    body = daraz_api_call('/image/migrate', 'POST', payload)
    return (((body.get('data') or {}).get('image') or {}).get('url') or '').strip()


def migrate_daraz_images(urls, limit=8):
    migrated = []
    seen = set()
    for url in urls or []:
        if not url or url in seen:
            continue
        seen.add(url)
        try:
            migrated_url = migrate_daraz_image_url(url)
            if migrated_url and migrated_url not in migrated:
                migrated.append(migrated_url)
        except Exception as e:
            print(f"Could not migrate Daraz image {url}: {e}")
        if len(migrated) >= limit:
            break
    return migrated


def daraz_sku_payload_from_shopify(row, settings, image_urls=None):
    seller_sku = str(row.get('sku') or row.get('barcode') or row.get('variant_key') or '').strip()
    variant_title = str(row.get('variant_title') or row.get('secondary_name') or '').strip()
    if variant_title in {'', 'Default Title'}:
        variant_title = 'Default'
    sku_payload = {
        'SellerSku': seller_sku,
        'quantity': str(int(money_float(settings.get('quantity') or 1000))),
        'price': f"{money_float(row.get('product_price')):.2f}",
        'package_weight': str(settings.get('package_weight') or '2'),
        'package_length': str(settings.get('package_length') or '20'),
        'package_width': str(settings.get('package_width') or '20'),
        'package_height': str(settings.get('package_height') or '20'),
        'package_content': str(settings.get('package_content') or 'Bean bag'),
        'color_family': variant_title,
        'saleProp': {'color_family': variant_title},
    }
    images = migrate_daraz_images(image_urls or [row.get('image')])
    if images:
        sku_payload['Images'] = {'Image': images[:8]}
    return sku_payload


def create_daraz_product_from_shopify(group, settings):
    category_id = str(settings.get('category_id') or '').strip()
    if not category_id:
        raise RuntimeError('Daraz category ID is required before creating new products.')
    product_images = migrate_daraz_images(group.get('images') or [])
    description = str(settings.get('description') or group.get('description') or group.get('label') or '').strip()
    short_description = str(settings.get('short_description') or group.get('label') or '').strip()
    payload = {
        'Request': {
            'Product': {
                'PrimaryCategory': category_id,
                'Images': {'Image': product_images[:8]} if product_images else {},
                'Attributes': {
                    'name': group.get('label') or 'Shopify product',
                    'name_en': group.get('label') or 'Shopify product',
                    'description': description,
                    'short_description': short_description,
                    'brand': str(settings.get('brand') or group.get('vendor') or 'No Brand'),
                    'model': group.get('label') or 'Bean bag',
                },
                'Skus': {
                    'Sku': [
                        daraz_sku_payload_from_shopify(row, settings, [row.get('image')] or product_images)
                        for row in group.get('rows') or []
                    ]
                },
            }
        }
    }
    return daraz_api_call('/product/create', 'POST', payload)


def add_daraz_variant_from_shopify(row, target_group, settings):
    if not target_group:
        raise RuntimeError('Existing Daraz product could not be found for this Shopify variant.')
    associated_sku = str((target_group.get('rows') or [{}])[0].get('seller_sku') or (target_group.get('rows') or [{}])[0].get('sku') or '').strip()
    item_id = str(target_group.get('item_id') or '').strip()
    if not item_id or not associated_sku:
        raise RuntimeError('Existing Daraz item id and AssociatedSku are required to add variants.')
    payload = {
        'Request': {
            'Product': {
                'ItemId': item_id,
                'AssociatedSku': associated_sku,
                'Attributes': {},
                'Skus': {
                    'Sku': [daraz_sku_payload_from_shopify(row, settings, [row.get('image')])]
                },
            }
        }
    }
    return daraz_api_call('/product/update', 'POST', payload)


def parse_iso_day(value: str):
    if not value:
        return None
    try:
        return datetime.strptime(value[:10], '%Y-%m-%d').date()
    except ValueError:
        return None


def daraz_created_after_iso(days_back: int = DEFAULT_DARAZ_WINDOW_DAYS) -> str:
    target = (datetime.now() - dt.timedelta(days=days_back)).date()
    return f"{target.isoformat()}T00:00:00+08:00"


def format_daraz_item_title(name: str, variation: str) -> str:
    title = f"{name or 'Unknown'} {variation or ''}".strip()
    if "Color family:" in title:
        base, color = title.split("Color family:", 1)
        title = f"{base.strip()} - {color.strip()}"
    return title


def daraz_item_key(item: dict) -> str:
    for key in ('seller_sku', 'shop_sku', 'lazada_sku', 'sku'):
        value = str(item.get(key) or '').strip()
        if value:
            return value
    return f"{(item.get('name') or '').strip()}|{(item.get('variation') or '').strip()}"


def daraz_item_price(item: dict) -> float:
    for key in ('paid_price', 'item_price', 'unit_price', 'product_price', 'price'):
        value = money_float(item.get(key))
        if value > 0:
            return value
    return 0.0


def get_daraz_client():
    return lazop.LazopClient('https://api.daraz.pk/rest', '501554', 'nrP3XFN7ChZL53cXyVED1yj4iGZZtlcD')


def fetch_daraz_order_summaries(statuses, created_after=None, limit=100, max_pages=8):
    access_token = get_access_token()
    client = get_daraz_client()
    all_orders = {}

    for status in statuses:
        offset = 0
        for _ in range(max_pages):
            req = lazop.LazopRequest('/orders/get', 'GET')
            req.add_api_param('sort_direction', 'DESC')
            req.add_api_param('offset', str(offset))
            req.add_api_param('created_after', created_after or daraz_created_after_iso())
            req.add_api_param('limit', str(limit))
            req.add_api_param('update_after', created_after or daraz_created_after_iso())
            req.add_api_param('sort_by', 'updated_at')
            req.add_api_param('status', status)
            req.add_api_param('access_token', access_token)

            response = client.execute(req)
            orders = response.body.get('data', {}).get('orders', []) or []
            if not orders:
                break

            for order in orders:
                order_copy = dict(order)
                order_copy['_requested_status'] = status
                all_orders[str(order.get('order_id'))] = order_copy

            if len(orders) < limit:
                break
            offset += limit

    return list(all_orders.values())


def fetch_daraz_order_items(order_id, access_token=None, client=None):
    access_token = access_token or get_access_token()
    client = client or get_daraz_client()
    req = lazop.LazopRequest('/order/items/get', 'GET')
    req.add_api_param('order_id', order_id)
    req.add_api_param('access_token', access_token)
    response = client.execute(req)
    return response.body.get('data', []) or []


def fetch_daraz_order_finance(order_id, order_date_str, gross_sale, access_token=None, client=None):
    access_token = access_token or get_access_token()
    client = client or get_daraz_client()

    order_date = parse_iso_day(order_date_str) or datetime.now().date()
    start_date = (order_date - dt.timedelta(days=1)).strftime('%Y-%m-%d')
    end_date = (order_date + dt.timedelta(days=120)).strftime('%Y-%m-%d')

    req = lazop.LazopRequest('/finance/transaction/details/get', 'GET')
    req.add_api_param('access_token', access_token)
    req.add_api_param('offset', '0')
    req.add_api_param('limit', '500')
    req.add_api_param('start_time', start_date)
    req.add_api_param('end_time', end_date)
    req.add_api_param('trade_order_id', str(order_id))

    response = client.execute(req)
    rows = response.body.get('data', []) or []
    gross_sale_decimal = money_decimal(gross_sale)

    if not rows:
        return {
            'gross_sale': gross_sale_decimal,
            'daraz_charges_total': Decimal('0'),
            'net_settlement': Decimal('0'),
            'paid_status': 'Not Paid',
            'statement': '',
            'breakdown': [],
            'has_finance': False,
        }

    aggregate = {}
    for row in rows:
        label = str(row.get('fee_name') or row.get('transaction_type') or 'Other').strip()
        amount = money_decimal(row.get('amount'))
        aggregate[label] = aggregate.get(label, Decimal('0')) + amount

    product_label = next((key for key in aggregate if key.strip().lower() == 'product price paid by buyer'), None)
    if product_label is None:
        aggregate['Product Price Paid by Buyer'] = gross_sale_decimal
    elif aggregate[product_label] <= 0:
        aggregate[product_label] = gross_sale_decimal

    net_settlement = sum(aggregate.values(), Decimal('0'))
    daraz_charges_total = sum(abs(value) for value in aggregate.values() if value < 0)
    paid_status = 'Paid' if any(str(row.get('paid_status', '')).lower() in ('yes', 'paid') for row in rows) else 'Not Paid'
    statement = str(rows[-1].get('statement') or '')

    ordered_items = sorted(
        aggregate.items(),
        key=lambda kv: (
            kv[0].strip().lower() != 'product price paid by buyer',
            0 if kv[1] >= 0 else 1,
            kv[0].lower(),
        )
    )

    return {
        'gross_sale': gross_sale_decimal,
        'daraz_charges_total': daraz_charges_total,
        'net_settlement': net_settlement,
        'paid_status': paid_status,
        'statement': statement,
        'breakdown': [
            {
                'label': label,
                'amount': float(amount),
                'amount_formatted': money_format(amount),
            }
            for label, amount in ordered_items
        ],
        'has_finance': True,
    }


def build_shopify_cost_catalog():
    rows = []
    for item in get_active_shopify_products(limit=250):
        variant_key = str(item.get('variant_id') or f"{item.get('product_id')}::{item.get('sku') or item.get('title')}")
        row = {
            'source': COST_SOURCE_SHOPIFY,
            'variant_key': variant_key,
            'source_product_id': str(item.get('product_id') or ''),
            'source_variant_id': str(item.get('variant_id') or ''),
            'inventory_item_id': str(item.get('inventory_item_id') or ''),
            'sku': item.get('sku') or '',
            'barcode': item.get('barcode') or '',
            'primary_name': item.get('title') or item.get('product_title') or 'Untitled variant',
            'secondary_name': '',
            'product_title': item.get('product_title') or '',
            'variant_title': item.get('variant_title') or '',
            'product_description': item.get('product_description') or '',
            'product_vendor': item.get('product_vendor') or '',
            'product_type': item.get('product_type') or '',
            'product_images': item.get('product_images') or [],
            'product_price': money_float(item.get('price')),
            'image': item.get('image') or '',
        }
        row.update(default_cost_calc_fields(COST_SOURCE_SHOPIFY))
        rows.append(row)
    rows.sort(key=lambda row: ((row.get('primary_name') or '').lower(), (row.get('sku') or '').lower()))
    return rows


def normalize_catalog_match(value: str) -> str:
    value = str(value or '').lower()
    value = re.sub(r'[^a-z0-9]+', ' ', value)
    return ' '.join(value.split())


def catalog_match_keys(value: str) -> list[str]:
    raw = str(value or '').strip()
    candidates = [
        raw,
        re.sub(r'\([^)]*\)', ' ', raw),
        raw.split(' - ')[0],
        raw.split('|')[0],
    ]
    keys = []
    for candidate in candidates:
        normalized = normalize_catalog_match(candidate)
        normalized = re.sub(r'\b(free|foot|stool|with|without|color|colour)\b', ' ', normalized)
        normalized = ' '.join(normalized.split())
        if normalized and normalized not in keys:
            keys.append(normalized)
    return keys


def catalog_identifier_keys(value: str) -> list[str]:
    raw = str(value or '').strip()
    if not raw:
        return []
    keys = []

    def add(candidate):
        normalized = normalize_catalog_match(candidate)
        if normalized and normalized not in keys:
            keys.append(normalized)

    add(raw)
    compact = re.sub(r'[^a-zA-Z0-9]+', '', raw).lower()
    if compact and compact not in keys:
        keys.append(compact)

    barcode_match = re.match(r'^(\d{8,14})[-_\s]+[a-zA-Z0-9]+$', raw)
    if barcode_match:
        add(barcode_match.group(1))

    normalized_raw = normalize_catalog_match(raw)
    parts = normalized_raw.split()
    if len(parts) > 2:
        add(' '.join(parts[:-1]))
    if len(parts) > 3:
        add(' '.join(parts[:-2]))
    return keys


def parse_daraz_special_date(value):
    text = str(value or '').strip()
    if not text:
        return None
    for fmt in ('%Y-%m-%d%H:%M', '%Y-%m-%d %H:%M', '%Y-%m-%d'):
        try:
            return datetime.strptime(text[:16], fmt)
        except ValueError:
            continue
    return None


def daraz_seller_price(sku: dict) -> tuple[float, str]:
    base_price = money_float(sku.get('price'))
    special_price = money_float(sku.get('special_price'))
    if special_price <= 0:
        return base_price, 'base'

    now = datetime.now()
    starts_at = parse_daraz_special_date(sku.get('special_from_time') or sku.get('special_from_date'))
    ends_at = parse_daraz_special_date(sku.get('special_to_time') or sku.get('special_to_date'))
    if starts_at and now < starts_at:
        return base_price, 'base'
    if ends_at and now > ends_at:
        return base_price, 'base'
    return special_price, 'seller_special'


def fetch_daraz_products_catalog(max_pages=20):
    access_token = get_access_token()
    client = get_daraz_client()
    rows = []
    seen = set()
    limit = 50

    for page in range(max_pages):
        offset = page * limit
        req = lazop.LazopRequest('/products/get', 'GET')
        req.add_api_param('filter', 'live')
        req.add_api_param('limit', str(limit))
        req.add_api_param('offset', str(offset))
        req.add_api_param('options', '1')
        response = client.execute(req, access_token)
        body = response.body or {}
        data = body.get('data') or {}
        products = data.get('products') or []
        if not products:
            break

        for product in products:
            attributes = product.get('attributes') or {}
            primary_category = str(product.get('primary_category') or product.get('PrimaryCategory') or '')
            product_name = (
                attributes.get('name_en')
                or attributes.get('Name_en')
                or attributes.get('name')
                or attributes.get('Name')
                or f"Daraz Item {product.get('item_id') or ''}".strip()
            )
            product_images = product.get('images') or []
            for sku in product.get('skus') or []:
                seller_sku = str(sku.get('SellerSku') or sku.get('seller_sku') or '').strip()
                shop_sku = str(sku.get('ShopSku') or sku.get('shop_sku') or '').strip()
                sku_id = str(sku.get('SkuId') or sku.get('sku_id') or '').strip()
                variant_key = seller_sku or shop_sku or sku_id or str(product.get('item_id') or '')
                if not variant_key or variant_key in seen:
                    continue
                seen.add(variant_key)
                seller_price, price_source = daraz_seller_price(sku)
                images = sku.get('Images') or product_images or []
                image = next((img for img in images if img), '') if isinstance(images, list) else ''
                color_family = str((sku.get('saleProp') or {}).get('color_family') or sku.get('color_family') or '').strip()
                display_name = f"{product_name} - {color_family}" if color_family and color_family.lower() not in product_name.lower() else product_name
                row = {
                    'source': COST_SOURCE_DARAZ,
                    'variant_key': variant_key,
                    'source_product_id': str(product.get('item_id') or ''),
                    'source_variant_id': sku_id,
                    'primary_category': primary_category,
                    'inventory_item_id': '',
                    'sku': seller_sku or shop_sku,
                    'shop_sku': shop_sku,
                    'seller_sku': seller_sku,
                    'primary_name': display_name,
                    'secondary_name': color_family,
                    'product_title': product_name,
                    'variant_title': color_family,
                    'product_description': attributes.get('description') or attributes.get('short_description') or '',
                    'product_images': product_images,
                    'product_price': seller_price,
                    'daraz_base_price': money_float(sku.get('price')),
                    'daraz_seller_price': seller_price,
                    'daraz_special_price': money_float(sku.get('special_price')),
                    'daraz_live_price': 0.0,
                    'daraz_live_price_source': '',
                    'daraz_price_source': price_source,
                    'daraz_special_from': str(sku.get('special_from_time') or ''),
                    'daraz_special_to': str(sku.get('special_to_time') or sku.get('special_to_date') or ''),
                    'daraz_status': str(sku.get('Status') or product.get('status') or ''),
                    'daraz_stock': int(float(sku.get('quantity') or sku.get('Available') or 0)),
                    'image': image,
                }
                row.update(default_cost_calc_fields(COST_SOURCE_DARAZ))
                rows.append(row)

        total_products = money_float(data.get('total_products'))
        if len(products) < limit or (total_products and len(rows) >= total_products):
            break

    return rows


def build_daraz_order_cost_catalog():
    orders = fetch_daraz_order_summaries(DARAZ_PROFIT_STATUSES)
    access_token = get_access_token()
    items_by_key = {}

    for order in orders:
        try:
            items = fetch_daraz_order_items(order.get('order_id'), access_token=access_token)
        except Exception as e:
            print(f"Could not fetch Daraz items for catalog order {order.get('order_id')}: {e}")
            continue
        for item in items:
            variant_key = daraz_item_key(item)
            if variant_key in items_by_key:
                continue
            row = {
                'source': COST_SOURCE_DARAZ,
                'variant_key': variant_key,
                'source_product_id': str(item.get('item_id') or item.get('product_id') or ''),
                'source_variant_id': str(item.get('order_item_id') or item.get('sku_id') or ''),
                'sku': str(item.get('seller_sku') or item.get('shop_sku') or item.get('lazada_sku') or item.get('sku') or '').strip(),
                'primary_name': format_daraz_item_title(item.get('name'), item.get('variation')),
                'secondary_name': '',
                'product_price': daraz_item_price(item),
                'image': item.get('product_main_image') or '',
            }
            row.update(default_cost_calc_fields(COST_SOURCE_DARAZ))
            items_by_key[variant_key] = row

    rows = list(items_by_key.values())
    rows.sort(key=lambda row: ((row.get('primary_name') or '').lower(), (row.get('sku') or '').lower()))
    return rows


def build_shopify_match_lookup():
    rows = with_saved_costs(COST_SOURCE_SHOPIFY, build_shopify_cost_catalog())
    by_identifier = {}
    by_name = {}
    for row in rows:
        for value in (row.get('sku'), row.get('barcode')):
            for key in catalog_identifier_keys(value):
                by_identifier.setdefault(key, row)
        for name in catalog_match_keys(row.get('primary_name')):
            by_name.setdefault(name, row)
    return by_identifier, by_name


def attach_shopify_matches_to_daraz(rows):
    by_identifier, by_name = build_shopify_match_lookup()
    for row in rows:
        matched = None
        reason = ''
        for sku_value in (row.get('seller_sku'), row.get('shop_sku'), row.get('sku')):
            for identifier_key in catalog_identifier_keys(sku_value):
                matched = by_identifier.get(identifier_key)
                if matched:
                    reason = 'SKU'
                    break
            if matched:
                break
        if not matched:
            for name_key in catalog_match_keys(row.get('primary_name')):
                matched = by_name.get(name_key)
                if matched:
                    break
            if matched:
                reason = 'Name'
        if matched:
            row['matched_shopify_key'] = matched.get('variant_key') or ''
            row['matched_shopify_name'] = matched.get('primary_name') or ''
            row['matched_shopify_sku'] = matched.get('sku') or ''
            row['matched_shopify_price'] = money_float(matched.get('product_price'))
            row['match_reason'] = reason
        else:
            row['matched_shopify_key'] = ''
            row['matched_shopify_name'] = ''
            row['matched_shopify_sku'] = ''
            row['matched_shopify_price'] = 0.0
            row['match_reason'] = ''
    return rows


def build_daraz_cost_catalog():
    try:
        rows = fetch_daraz_products_catalog()
    except Exception as e:
        print(f"Could not fetch Daraz product catalog, falling back to order items: {e}")
        rows = build_daraz_order_cost_catalog()
    rows = attach_shopify_matches_to_daraz(rows)
    rows.sort(key=lambda row: ((row.get('primary_name') or '').lower(), (row.get('sku') or '').lower()))
    return rows


def cost_product_label(row):
    name = str(row.get('primary_name') or 'Untitled product').strip()
    secondary = str(row.get('secondary_name') or '').strip()
    if secondary and name.lower().endswith(f" - {secondary.lower()}"):
        return name[:-(len(secondary) + 3)].strip()
    if ' - ' in name:
        return name.split(' - ')[0].strip()
    return name


def group_cost_rows(rows, source=''):
    groups = {}
    for row in rows:
        key = str(row.get('source_product_id') or cost_product_label(row) or row.get('variant_key') or '').strip()
        group = groups.setdefault(key, {
            'key': key,
            'item_id': str(row.get('source_product_id') or ''),
            'label': cost_product_label(row),
            'category_id': str(row.get('primary_category') or ''),
            'vendor': row.get('product_vendor') or '',
            'description': row.get('product_description') or '',
            'images': [],
            'rows': [],
            'source': source or row.get('source') or '',
        })
        for image in (row.get('product_images') or [row.get('image')]):
            if image and image not in group['images']:
                group['images'].append(image)
        if row.get('image') and row.get('image') not in group['images']:
            group['images'].append(row.get('image'))
        group['rows'].append(row)
    return groups


def build_daraz_lookup_groups(daraz_groups):
    by_identifier = {}
    by_name = {}
    for group in daraz_groups.values():
        for row in group.get('rows') or []:
            for value in (row.get('seller_sku'), row.get('shop_sku'), row.get('sku'), row.get('variant_key')):
                for key in catalog_identifier_keys(value):
                    by_identifier.setdefault(key, group)
        for key in catalog_match_keys(group.get('label')):
            by_name.setdefault(key, group)
    return by_identifier, by_name


def find_daraz_group_for_shopify_group(shopify_group, by_identifier, by_name):
    for row in shopify_group.get('rows') or []:
        for value in (row.get('sku'), row.get('barcode'), row.get('variant_key')):
            for key in catalog_identifier_keys(value):
                if key in by_identifier:
                    return by_identifier[key]
    for key in catalog_match_keys(shopify_group.get('label')):
        if key in by_name:
            return by_name[key]
    return None


def daraz_has_shopify_variant(shopify_row, daraz_group):
    if not daraz_group:
        return False
    shopify_keys = set()
    for value in (shopify_row.get('sku'), shopify_row.get('barcode'), shopify_row.get('variant_key')):
        shopify_keys.update(catalog_identifier_keys(value))
    for row in daraz_group.get('rows') or []:
        if row.get('matched_shopify_key') == shopify_row.get('variant_key'):
            return True
        for value in (row.get('seller_sku'), row.get('shop_sku'), row.get('sku'), row.get('variant_key')):
            if shopify_keys.intersection(catalog_identifier_keys(value)):
                return True
    return False


def build_daraz_sync_plan():
    shopify_rows = with_saved_costs(COST_SOURCE_SHOPIFY, build_shopify_cost_catalog())
    daraz_rows = with_saved_costs(COST_SOURCE_DARAZ, build_daraz_cost_catalog())
    shopify_groups = group_cost_rows(shopify_rows, COST_SOURCE_SHOPIFY)
    daraz_groups = group_cost_rows(daraz_rows, COST_SOURCE_DARAZ)
    by_identifier, by_name = build_daraz_lookup_groups(daraz_groups)

    price_updates = []
    for row in daraz_rows:
        matched_price = money_float(row.get('matched_shopify_price'))
        if matched_price > 0 and abs(matched_price - money_float(row.get('product_price'))) >= 0.01:
            price_updates.append({
                'id': f"price:{row.get('variant_key')}",
                'daraz_variant_key': row.get('variant_key'),
                'daraz_item_id': row.get('source_product_id'),
                'daraz_sku_id': row.get('source_variant_id'),
                'seller_sku': row.get('seller_sku') or row.get('sku'),
                'daraz_name': row.get('primary_name'),
                'daraz_price': money_float(row.get('product_price')),
                'shopify_name': row.get('matched_shopify_name'),
                'shopify_sku': row.get('matched_shopify_sku'),
                'shopify_price': matched_price,
            })

    missing_products = []
    missing_variants = []
    for group_key, group in shopify_groups.items():
        daraz_group = find_daraz_group_for_shopify_group(group, by_identifier, by_name)
        if not daraz_group:
            missing_products.append({
                'id': f"create:{group_key}",
                'shopify_group_key': group_key,
                'name': group.get('label'),
                'variant_count': len(group.get('rows') or []),
                'images': group.get('images') or [],
                'rows': [
                    {
                        'variant_key': row.get('variant_key'),
                        'name': row.get('primary_name'),
                        'variant': row.get('variant_title') or row.get('secondary_name') or '',
                        'sku': row.get('sku') or row.get('barcode'),
                        'price': money_float(row.get('product_price')),
                        'image': row.get('image') or '',
                    }
                    for row in group.get('rows') or []
                ],
            })
            continue
        for row in group.get('rows') or []:
            if not daraz_has_shopify_variant(row, daraz_group):
                missing_variants.append({
                    'id': f"variant:{row.get('variant_key')}",
                    'shopify_variant_key': row.get('variant_key'),
                    'target_daraz_item_id': daraz_group.get('item_id'),
                    'target_daraz_name': daraz_group.get('label'),
                    'name': row.get('primary_name'),
                    'variant': row.get('variant_title') or row.get('secondary_name') or '',
                    'sku': row.get('sku') or row.get('barcode'),
                    'price': money_float(row.get('product_price')),
                    'image': row.get('image') or '',
                })

    return {
        'price_updates': price_updates,
        'missing_products': missing_products,
        'missing_variants': missing_variants,
        'summary': {
            'price_updates': len(price_updates),
            'missing_products': len(missing_products),
            'missing_variants': len(missing_variants),
        },
    }


def save_daraz_matched_price(row, price):
    update_daraz_sku_price(row.get('source_product_id'), row.get('source_variant_id'), row.get('seller_sku') or row.get('sku'), price)
    calc_fields = {field: money_float(row.get(field)) for field in (
        'beans_kg',
        'fabric_cost',
        'yard_qty',
        'fusium_cost',
        'making_cost',
        'overhead_cost',
        'delivery_cost',
        'return_cost',
        'ads_cost',
    )}
    product_cost = money_float(row.get('product_cost'))
    return upsert_product_cost(
        source=COST_SOURCE_DARAZ,
        variant_key=row.get('variant_key'),
        primary_name=row.get('primary_name') or '',
        secondary_name=row.get('secondary_name') or '',
        sku=row.get('sku') or row.get('seller_sku') or '',
        product_price=str(money_float(price)),
        beans_kg=str(calc_fields['beans_kg']),
        fabric_cost=str(calc_fields['fabric_cost']),
        yard_qty=str(calc_fields['yard_qty']),
        fusium_cost=str(calc_fields['fusium_cost']),
        making_cost=str(calc_fields['making_cost']),
        overhead_cost=str(calc_fields['overhead_cost']),
        delivery_cost=str(calc_fields['delivery_cost']),
        return_cost=str(calc_fields['return_cost']),
        ads_cost=str(calc_fields['ads_cost']),
        product_cost=str(product_cost),
        source_product_id=row.get('source_product_id') or '',
        source_variant_id=row.get('source_variant_id') or '',
    )


def with_saved_costs(source: str, rows: list):
    lookup = get_product_cost_lookup(source)
    legacy_shopify_lookup = {}
    if source == COST_SOURCE_SHOPIFY:
        legacy_shopify_lookup.update(get_product_cost_lookup('shopify_lahore'))
        legacy_shopify_lookup.update(get_product_cost_lookup('shopify_leopards'))
    hydrated = []
    for row in rows:
        saved = lookup.get(row['variant_key']) or legacy_shopify_lookup.get(row['variant_key'])
        row_copy = dict(row)
        row_copy['product_cost'] = money_float(saved.get('product_cost')) if saved else 0.0
        if saved:
            row_copy['product_price'] = money_float(saved.get('product_price') or row_copy.get('product_price'))
            row_copy['secondary_name'] = saved.get('secondary_name') or row_copy.get('secondary_name') or ''
            for field in (
                'beans_kg',
                'fabric_cost',
                'yard_qty',
                'fusium_cost',
                'making_cost',
                'overhead_cost',
                'delivery_cost',
                'return_cost',
                'ads_cost',
            ):
                row_copy[field] = money_float(saved.get(field) if saved.get(field) is not None else row_copy.get(field))
            row_copy['updated_at'] = saved.get('updated_at').isoformat() if saved.get('updated_at') else ''
        hydrated.append(row_copy)
    return hydrated


def get_cost_catalog_for_source(source: str):
    if source == COST_SOURCE_DARAZ:
        return with_saved_costs(source, build_daraz_cost_catalog())
    if source == COST_SOURCE_SHOPIFY:
        return with_saved_costs(source, build_shopify_cost_catalog())
    return []


EXHIBITION_TARGET_SALE = 2_000_000
EXHIBITION_DELIVERY_PICKUPS = {'Pickup from Expo', 'Pickup from Warehouse'}
EXHIBITION_PAYMENT_SPLITS = {'100% Paid', '50% Paid', 'Custom Amount'}
EXHIBITION_PAYMENT_METHODS = {'Cash', 'Bank'}
EXHIBITION_DELIVERY_METHODS = {'Pickup from Expo', 'Home Delivery', 'Pickup from Warehouse'}


def exhibition_row_cost(row: dict, beans_price=None) -> float:
    beans_rate = money_float(beans_price if beans_price is not None else get_shopify_beans_price())
    cost = (
        money_float(row.get('beans_kg')) * beans_rate
        + money_float(row.get('fabric_cost')) * money_float(row.get('yard_qty'))
        + money_float(row.get('fusium_cost'))
        + money_float(row.get('making_cost'))
        + money_float(row.get('overhead_cost'))
    )
    return round(cost, 2)


def exhibition_serialize_date(value):
    if not value:
        return ''
    if hasattr(value, 'isoformat'):
        return value.isoformat()
    return str(value)


def exhibition_serialize_row(row: dict) -> dict:
    result = {}
    for key, value in dict(row or {}).items():
        if isinstance(value, Decimal):
            result[key] = money_float(value)
        elif hasattr(value, 'isoformat'):
            result[key] = value.isoformat()
        else:
            result[key] = value
    return result


def build_exhibition_product_catalog():
    beans_price = get_shopify_beans_price()
    rows = get_cost_catalog_for_source(COST_SOURCE_SHOPIFY)
    products = []
    for row in rows:
        cost = exhibition_row_cost(row, beans_price)
        variant_id = str(row.get('source_variant_id') or row.get('variant_key') or '')
        products.append({
            'variant_key': row.get('variant_key') or variant_id,
            'shopify_product_id': str(row.get('source_product_id') or ''),
            'shopify_variant_id': variant_id,
            'sku': row.get('sku') or '',
            'barcode': row.get('barcode') or '',
            'name': row.get('primary_name') or row.get('product_title') or 'Untitled product',
            'product_title': row.get('product_title') or row.get('primary_name') or '',
            'variant_title': row.get('variant_title') or '',
            'price': money_float(row.get('product_price')),
            'image': row.get('image') or '',
            'cost': cost,
            'cost_saved': cost > 0,
        })
    products.sort(key=lambda item: ((item.get('product_title') or item.get('name') or '').lower(), (item.get('variant_title') or '').lower()))
    return products


def build_exhibition_cost_lookup():
    beans_price = get_shopify_beans_price()
    rows = get_cost_catalog_for_source(COST_SOURCE_SHOPIFY)
    lookup = {
        'variant': {},
        'identifier': {},
        'name': {},
        'rows': [],
    }
    for row in rows:
        enriched = dict(row)
        enriched['exhibition_cost'] = exhibition_row_cost(row, beans_price)
        enriched['match_label'] = enriched.get('primary_name') or enriched.get('product_title') or ''
        lookup['rows'].append(enriched)
        for key in (
            row.get('source_variant_id'),
            row.get('variant_key'),
        ):
            key = str(key or '').strip()
            if key:
                lookup['variant'][key] = enriched
        for raw in (row.get('sku'), row.get('barcode')):
            for key in catalog_identifier_keys(raw):
                lookup['identifier'].setdefault(key, enriched)
        for raw in (row.get('primary_name'), row.get('product_title'), row.get('secondary_name')):
            for key in catalog_match_keys(raw):
                lookup['name'].setdefault(key, enriched)
    return lookup


def match_exhibition_order_cost(order: dict, lookup: dict) -> dict:
    variant_id = str(order.get('shopify_variant_id') or '').strip()
    if variant_id and variant_id in lookup['variant']:
        row = lookup['variant'][variant_id]
        return {'row': row, 'reason': 'Variant match', 'score': 1.0}

    for key in catalog_identifier_keys(order.get('sku')):
        if key in lookup['identifier']:
            row = lookup['identifier'][key]
            return {'row': row, 'reason': 'SKU match', 'score': 0.98}

    order_keys = catalog_match_keys(order.get('product_name'))
    for key in order_keys:
        if key in lookup['name']:
            row = lookup['name'][key]
            return {'row': row, 'reason': 'Name match', 'score': 0.95}

    best = None
    best_score = 0.0
    order_name = order_keys[0] if order_keys else normalize_catalog_match(order.get('product_name'))
    if order_name:
        for row in lookup['rows']:
            row_keys = catalog_match_keys(row.get('primary_name')) or [normalize_catalog_match(row.get('primary_name'))]
            for row_key in row_keys:
                if not row_key:
                    continue
                score = SequenceMatcher(None, order_name, row_key).ratio()
                if order_name in row_key or row_key in order_name:
                    score = max(score, 0.82)
                if score > best_score:
                    best = row
                    best_score = score
    if best and best_score >= 0.62:
        return {'row': best, 'reason': 'Suggested name match', 'score': round(best_score, 2)}
    return {'row': None, 'reason': 'No cost match', 'score': 0.0}


def calculate_exhibition_order_amounts(quantity, unit_price, discount, delivery_method, delivery_charges, payment_split, custom_paid_amount):
    qty = max(int(quantity or 1), 1)
    price = money_float(unit_price)
    discount_value = max(money_float(discount), 0)
    delivery = 0.0 if delivery_method in EXHIBITION_DELIVERY_PICKUPS else max(money_float(delivery_charges), 0)
    total = max((price * qty) - discount_value + delivery, 0)
    split = payment_split if payment_split in EXHIBITION_PAYMENT_SPLITS else '100% Paid'
    if split == '50% Paid':
        paid = round(total * 0.5, 2)
    elif split == 'Custom Amount':
        paid = min(max(money_float(custom_paid_amount), 0), total)
    else:
        paid = total
    return {
        'quantity': qty,
        'unit_price': price,
        'discount': discount_value,
        'delivery_charges': delivery,
        'total_amount': round(total, 2),
        'paid_amount': round(paid, 2),
        'custom_paid_amount': round(money_float(custom_paid_amount), 2) if split == 'Custom Amount' else 0.0,
    }


def normalize_exhibition_order_items(payload: dict) -> list[dict]:
    raw_items = payload.get('items')
    if not isinstance(raw_items, list) or not raw_items:
        raw_items = [{
            'product_name': payload.get('product_name'),
            'shopify_product_id': payload.get('shopify_product_id'),
            'shopify_variant_id': payload.get('shopify_variant_id'),
            'sku': payload.get('sku'),
            'quantity': payload.get('quantity') or 1,
            'unit_price': payload.get('unit_price') or payload.get('price') or 0,
        }]

    items = []
    for raw in raw_items:
        if not isinstance(raw, dict):
            continue
        name = str(raw.get('product_name') or raw.get('name') or '').strip()
        if not name:
            continue
        qty = max(int(money_float(raw.get('quantity') or 1)), 1)
        unit_price = money_float(raw.get('unit_price') or raw.get('price') or 0)
        line_total = round(qty * unit_price, 2)
        items.append({
            'product_name': name,
            'shopify_product_id': str(raw.get('shopify_product_id') or ''),
            'shopify_variant_id': str(raw.get('shopify_variant_id') or ''),
            'sku': str(raw.get('sku') or ''),
            'quantity': qty,
            'unit_price': unit_price,
            'line_total': line_total,
        })
    return items


def calculate_exhibition_multi_item_amounts(items, discount, delivery_method, delivery_charges, payment_split, custom_paid_amount):
    subtotal = sum(money_float(item.get('line_total')) for item in items)
    discount_value = max(money_float(discount), 0)
    delivery = 0.0 if delivery_method in EXHIBITION_DELIVERY_PICKUPS else max(money_float(delivery_charges), 0)
    total = max(subtotal - discount_value + delivery, 0)
    split = payment_split if payment_split in EXHIBITION_PAYMENT_SPLITS else '100% Paid'
    if split == '50% Paid':
        paid = round(total * 0.5, 2)
    elif split == 'Custom Amount':
        paid = min(max(money_float(custom_paid_amount), 0), total)
    else:
        paid = total
    return {
        'subtotal': round(subtotal, 2),
        'discount': discount_value,
        'delivery_charges': delivery,
        'total_amount': round(total, 2),
        'paid_amount': round(paid, 2),
        'custom_paid_amount': round(money_float(custom_paid_amount), 2) if split == 'Custom Amount' else 0.0,
    }


def make_exhibition_order_number():
    return f"EXH-{dt.datetime.now().strftime('%y%m%d%H%M%S')}-{os.urandom(2).hex().upper()}"


def build_exhibition_invoice_payload(order: dict) -> dict:
    created_at = order.get('created_at')
    if hasattr(created_at, 'strftime'):
        created_display = created_at.strftime('%d %b %Y %I:%M %p')
    else:
        created_display = str(created_at or '')
    total_amount = money_decimal(order.get('total_amount'))
    paid_amount = money_decimal(order.get('paid_amount'))
    items = order.get('items') if isinstance(order.get('items'), list) else []
    if not items:
        items = [{
            'product_name': order.get('product_name') or '',
            'quantity': int(order.get('quantity') or 1),
            'unit_price': money_float(order.get('unit_price')),
            'line_total': money_float(order.get('unit_price')) * int(order.get('quantity') or 1),
        }]
    invoice_items = []
    for item in items:
        qty = int(item.get('quantity') or 1)
        unit_price = money_float(item.get('unit_price'))
        line_total = money_float(item.get('line_total') or (unit_price * qty))
        invoice_items.append({
            'product_name': item.get('product_name') or '',
            'quantity': qty,
            'unit_price': money_format(unit_price),
            'line_total': money_format(line_total),
        })
    return {
        'order_number': order.get('order_number') or '',
        'exhibition_name': order.get('exhibition_name') or 'Exhibition',
        'exhibition_location': order.get('exhibition_location') or '',
        'created_display': created_display,
        'customer_name': order.get('customer_name') or '',
        'customer_phone': order.get('customer_phone') or '',
        'items': invoice_items,
        'product_name': order.get('product_name') or '',
        'quantity': int(order.get('quantity') or 1),
        'unit_price': money_format(order.get('unit_price')),
        'discount': money_format(order.get('discount')),
        'delivery_method': order.get('delivery_method') or '',
        'delivery_address': order.get('delivery_address') or '',
        'delivery_charges': money_format(order.get('delivery_charges')),
        'total_amount': money_format(total_amount),
        'paid_amount': money_format(paid_amount),
        'balance': money_format(total_amount - paid_amount),
        'payment_label': f"{order.get('payment_method') or ''} · {order.get('payment_split') or ''}".strip(' ·'),
    }


def build_exhibition_plain_receipt(invoice: dict) -> str:
    lines = [
        'TICK BAGS',
        'Exhibition Order Invoice',
        invoice.get('exhibition_name') or 'Exhibition',
    ]
    if invoice.get('exhibition_location'):
        lines.append(invoice['exhibition_location'])
    lines.extend([
        '-' * 32,
        f"Invoice: {invoice.get('order_number', '')}",
        f"Date: {invoice.get('created_display', '')}",
    ])
    if invoice.get('customer_name'):
        lines.append(f"Customer: {invoice['customer_name']}")
    if invoice.get('customer_phone'):
        lines.append(f"Phone: {invoice['customer_phone']}")
    lines.extend([
        '-' * 32,
    ])
    for item in invoice.get('items') or []:
        lines.extend([
            item.get('product_name') or '',
            f"Qty: {item.get('quantity', 1)} x {item.get('unit_price', '')}",
            f"Line Total: {item.get('line_total', '')}",
        ])
    lines.extend([
        f"Discount: {invoice.get('discount', '')}",
        f"Delivery: {invoice.get('delivery_method', '')}",
    ])
    if invoice.get('delivery_address'):
        lines.append(f"Address: {invoice['delivery_address']}")
    lines.extend([
        f"Delivery Charges: {invoice.get('delivery_charges', '')}",
        '-' * 32,
        f"Total: {invoice.get('total_amount', '')}",
        f"Paid: {invoice.get('paid_amount', '')}",
        f"Balance: {invoice.get('balance', '')}",
        f"Payment: {invoice.get('payment_label', '')}",
        '-' * 32,
        'Thank you for shopping with Tick Bags.',
        '',
    ])
    return '\n'.join(lines)


def get_exhibition_order_items(order: dict) -> list[dict]:
    items = order.get('items') if isinstance(order.get('items'), list) else []
    if items:
        return items
    return [{
        'product_name': order.get('product_name') or '',
        'shopify_product_id': order.get('shopify_product_id') or '',
        'shopify_variant_id': order.get('shopify_variant_id') or '',
        'sku': order.get('sku') or '',
        'quantity': int(order.get('quantity') or 1),
        'unit_price': money_float(order.get('unit_price')),
        'line_total': money_float(order.get('unit_price')) * int(order.get('quantity') or 1),
    }]


def normalize_exhibition_order_payload(payload: dict):
    items = normalize_exhibition_order_items(payload)
    if not items:
        return None, ('Add at least one product to the order.', 400)
    delivery_method = payload.get('delivery_method') if payload.get('delivery_method') in EXHIBITION_DELIVERY_METHODS else 'Pickup from Expo'
    delivery_address = str(payload.get('delivery_address') or '').strip()
    if delivery_method == 'Home Delivery' and not delivery_address:
        return None, ('Delivery address is required for home delivery.', 400)
    payment_method = payload.get('payment_method') if payload.get('payment_method') in EXHIBITION_PAYMENT_METHODS else 'Cash'
    payment_split = payload.get('payment_split') if payload.get('payment_split') in EXHIBITION_PAYMENT_SPLITS else '100% Paid'
    amounts = calculate_exhibition_multi_item_amounts(
        items,
        payload.get('discount') or 0,
        delivery_method,
        payload.get('delivery_charges') or 0,
        payment_split,
        payload.get('custom_paid_amount') or 0,
    )
    first_item = items[0]
    product_name = first_item.get('product_name') or ''
    if len(items) > 1:
        product_name = f"{product_name} + {len(items) - 1} more"
    total_quantity = sum(int(item.get('quantity') or 1) for item in items)
    return {
        'exhibition_id': payload.get('exhibition_id') or None,
        'customer_name': payload.get('customer_name') or '',
        'customer_phone': payload.get('customer_phone') or '',
        'product_name': product_name,
        'shopify_product_id': first_item.get('shopify_product_id') or '',
        'shopify_variant_id': first_item.get('shopify_variant_id') or '',
        'sku': first_item.get('sku') or '',
        'items': items,
        'quantity': total_quantity,
        'unit_price': first_item.get('unit_price') or 0,
        'discount': amounts['discount'],
        'delivery_method': delivery_method,
        'delivery_address': delivery_address if delivery_method == 'Home Delivery' else '',
        'delivery_charges': amounts['delivery_charges'],
        'payment_method': payment_method,
        'payment_split': payment_split,
        'custom_paid_amount': amounts['custom_paid_amount'],
        'total_amount': amounts['total_amount'],
        'paid_amount': amounts['paid_amount'],
    }, None


def build_daraz_profit_records(start_date: str = '', end_date: str = ''):
    summaries = fetch_daraz_order_summaries(DARAZ_PROFIT_STATUSES)
    start_day = parse_iso_day(start_date)
    end_day = parse_iso_day(end_date)
    filtered = []

    for order in summaries:
        created_day = parse_iso_day(str(order.get('created_at') or ''))
        if start_day and created_day and created_day < start_day:
            continue
        if end_day and created_day and created_day > end_day:
            continue
        filtered.append(order)

    if not filtered:
        return {'records': [], 'summary': {}}

    cost_lookup = get_product_cost_lookup(COST_SOURCE_DARAZ)
    access_token = get_access_token()

    def process_order(order):
        client = get_daraz_client()
        order_id = str(order.get('order_id') or '')
        items = fetch_daraz_order_items(order_id, access_token=access_token, client=client)
        finance = fetch_daraz_order_finance(
            order_id=order_id,
            order_date_str=str(order.get('created_at') or ''),
            gross_sale=order.get('price', '0'),
            access_token=access_token,
            client=client,
        )

        line_items = []
        product_cost_total = Decimal('0')
        missing_cost_count = 0
        total_quantity = 0

        for item in items:
            variant_key = daraz_item_key(item)
            saved = cost_lookup.get(variant_key)
            qty = int(item.get('quantity') or 1)
            total_quantity += qty
            unit_cost = money_decimal(saved.get('product_cost')) if saved else Decimal('0')
            line_cost = unit_cost * Decimal(qty)
            product_cost_total += line_cost
            if saved is None:
                missing_cost_count += 1

            line_items.append({
                'variant_key': variant_key,
                'primary_name': format_daraz_item_title(item.get('name'), item.get('variation')),
                'secondary_name': saved.get('secondary_name') if saved else '',
                'sku': str(item.get('seller_sku') or item.get('shop_sku') or item.get('lazada_sku') or item.get('sku') or '').strip(),
                'quantity': qty,
                'unit_cost': float(unit_cost),
                'unit_cost_formatted': money_format(unit_cost),
                'line_cost': float(line_cost),
                'line_cost_formatted': money_format(line_cost),
                'cost_missing': saved is None,
                'image': item.get('product_main_image') or '',
            })

        net_profit = finance['net_settlement'] - product_cost_total
        customer = order.get('address_shipping') or {}
        display_status = str(order.get('_requested_status') or order.get('statuses', [''])[0] or '').replace('_', ' ').title()

        return {
            'order_id': order_id,
            'created_at': str(order.get('created_at') or ''),
            'created_day': parse_iso_day(str(order.get('created_at') or '')).isoformat() if parse_iso_day(str(order.get('created_at') or '')) else '',
            'status': display_status,
            'customer_name': f"{order.get('customer_first_name', '')} {order.get('customer_last_name', '')}".strip() or 'N/A',
            'customer_phone': customer.get('phone') or 'N/A',
            'gross_sale': float(finance['gross_sale']),
            'gross_sale_formatted': money_format(finance['gross_sale']),
            'daraz_charges_total': float(finance['daraz_charges_total']),
            'daraz_charges_formatted': money_format(finance['daraz_charges_total']),
            'net_settlement': float(finance['net_settlement']),
            'net_settlement_formatted': money_format(finance['net_settlement']),
            'product_cost_total': float(product_cost_total),
            'product_cost_formatted': money_format(product_cost_total),
            'net_profit': float(net_profit),
            'net_profit_formatted': money_format(net_profit),
            'paid_status': finance['paid_status'],
            'statement': finance['statement'],
            'breakdown': finance['breakdown'],
            'has_finance': finance['has_finance'],
            'cost_missing': missing_cost_count > 0,
            'missing_cost_count': missing_cost_count,
            'items_count': len(line_items),
            'total_quantity': total_quantity,
            'line_items': line_items,
        }

    records = []
    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = [executor.submit(process_order, order) for order in filtered]
        for future in as_completed(futures):
            try:
                records.append(future.result())
            except Exception as e:
                print(f"Daraz profitability processing error: {e}")

    records.sort(key=lambda row: row.get('created_at', ''), reverse=True)

    summary = {
        'orders': len(records),
        'gross_sale_total': float(sum(money_decimal(row['gross_sale']) for row in records)),
        'daraz_charges_total': float(sum(money_decimal(row['daraz_charges_total']) for row in records)),
        'net_settlement_total': float(sum(money_decimal(row['net_settlement']) for row in records)),
        'product_cost_total': float(sum(money_decimal(row['product_cost_total']) for row in records)),
        'net_profit_total': float(sum(money_decimal(row['net_profit']) for row in records)),
        'missing_cost_orders': sum(1 for row in records if row['cost_missing']),
    }
    summary['gross_sale_total_formatted'] = money_format(summary['gross_sale_total'])
    summary['daraz_charges_total_formatted'] = money_format(summary['daraz_charges_total'])
    summary['net_settlement_total_formatted'] = money_format(summary['net_settlement_total'])
    summary['product_cost_total_formatted'] = money_format(summary['product_cost_total'])
    summary['net_profit_total_formatted'] = money_format(summary['net_profit_total'])

    return {'records': records, 'summary': summary}


# ── Daraz ─────────────────────────────────────────────────────────────────────

def get_daraz_orders(statuses):
    print("SEARCHING FOR DARAZ ORDERS")
    try:
        access_token = get_access_token()
        client = lazop.LazopClient('https://api.daraz.pk/rest', '501554', 'nrP3XFN7ChZL53cXyVED1yj4iGZZtlcD')

        all_orders = []

        for status in statuses:
            req = lazop.LazopRequest('/orders/get', 'GET')
            req.add_api_param('sort_direction', 'DESC')
            req.add_api_param('offset', '0')
            req.add_api_param('created_after', '2017-02-10T09:00:00+08:00')
            req.add_api_param('limit', '50')
            req.add_api_param('update_after', '2017-02-10T09:00:00+08:00')
            req.add_api_param('sort_by', 'updated_at')
            req.add_api_param('status', status)
            req.add_api_param('access_token', access_token)

            response = client.execute(req)
            darazOrders = response.body.get('data', {}).get('orders', [])

            for order in darazOrders:
                order_id = order.get('order_id', 'Unknown')

                item_request = lazop.LazopRequest('/order/items/get', 'GET')
                item_request.add_api_param('order_id', order_id)
                item_request.add_api_param('access_token', access_token)

                item_response = client.execute(item_request)
                items = item_response.body.get('data', [])

                item_details = []
                for item in items:
                    tracking_num = item.get('tracking_code', 'Unknown')

                    tracking_req = lazop.LazopRequest('/logistic/order/trace', 'GET')
                    tracking_req.add_api_param('order_id', order_id)
                    tracking_req.add_api_param('access_token', access_token)
                    tracking_response = client.execute(tracking_req)

                    tracking_data = tracking_response.body.get('result', {})
                    packages = tracking_data.get('data', [{}])[0].get('package_detail_info_list', [])

                    track_status = "N/A"
                    for package in packages:
                        if package.get("tracking_number") == tracking_num:
                            track_status = package.get('logistic_detail_info_list', [{}])[-1].get('title', "N/A")
                            break

                    product_title = f"{item.get('name', 'Unknown')} {item.get('variation', 'N/A')}"
                    if "Color family:" in product_title:
                        product_info, color_info = product_title.split("Color family:", 1)
                        product_title = f"{product_info.strip()} - {color_info.strip()}"

                    item_details.append({
                        'item_image':       item.get('product_main_image', 'N/A'),
                        'item_title':       product_title,
                        'quantity':         1,
                        'tracking_number':  item.get('tracking_code', 'N/A'),
                        'status':           track_status
                    })

                all_orders.append({
                    'order_id':    f"{order_id}",
                    'customer': {
                        'name':    f"{order.get('customer_first_name', '')} {order.get('customer_last_name', '')}".strip(),
                        'address': order.get('address_shipping', {}).get('address', 'N/A'),
                        'phone':   order.get('address_shipping', {}).get('phone', 'N/A')
                    },
                    'status':      status.replace('_', ' ').title(),
                    'date':        format_date(order.get('created_at', 'N/A')),
                    'total_price': order.get('price', '0.00'),
                    'items_list':  item_details,
                    'tracking_id': 'N/A',
                })

        return all_orders
    except Exception as e:
        print(f"Error fetching darazOrders: {e}")
        return []


@app.route('/daraz')
def daraz_callback():
    code = request.args.get('code')

    if code:
        client = lazop.LazopClient("https://api.daraz.pk/rest", "501554", "nrP3XFN7ChZL53cXyVED1yj4iGZZtlcD")
        req = lazop.LazopRequest('/auth/token/create')
        req.add_api_param('code', code)
        response = client.execute(req)
        body = response.body

        if "access_token" in body:
            save_tokens(body["access_token"], body["refresh_token"])
            print("Daraz tokens auto-saved via callback.")
            return redirect(url_for('daraz_orders_page'))
        else:
            return f"Auth failed: {body}", 400

    statuses = ['shipped', 'pending', 'ready_to_ship', 'packed']
    darazOrders = get_daraz_orders(statuses)
    return render_template('daraz.html', darazOrders=darazOrders)


@app.route('/daraz/orders')
def daraz_orders_page():
    statuses = ['shipped', 'pending', 'ready_to_ship', 'packed']
    darazOrders = get_daraz_orders(statuses)
    return render_template('daraz.html', darazOrders=darazOrders)


@app.route('/daraz/token-status')
def daraz_token_status():
    from token_manager import load_tokens
    tokens = load_tokens()
    if not tokens:
        return jsonify({"status": "missing"})
    expires_at = datetime.fromisoformat(tokens["expires_at"])
    days_left = (expires_at - datetime.now()).days
    return jsonify({"status": "ok", "expires_at": tokens["expires_at"], "days_left": days_left})


# ── Pending / Orders pages ────────────────────────────────────────────────────

@app.route('/pending')
def pending_orders():
    all_orders = []
    pending_items = []

    global daraz_orders, order_details

    def add_or_update_item(lst, new_item):
        for item in lst:
            if item['item_title'] == new_item['item_title']:
                item['quantity'] += new_item['quantity']
                return
        lst.append(new_item)

    for daraz_order in daraz_orders:
        if daraz_order['status'] in ['Ready To Ship', 'Pending', 'packed', 'Packed by seller / warehouse']:
            all_orders.append({
                'order_via':      'Daraz',
                'order_id':       daraz_order['order_id'],
                'status':         daraz_order['status'],
                'tracking_number': daraz_order['items_list'][0]['tracking_number'],
                'date':           daraz_order['date'],
                'items_list':     daraz_order['items_list'],
                'total_price':    daraz_order['total_price']
            })
            for item in daraz_order['items_list']:
                add_or_update_item(pending_items, {
                    'item_image': item['item_image'],
                    'item_title': item['item_title'],
                    'quantity':   item['quantity'],
                    'order_date': daraz_order['date']
                })

    for shopify_order in order_details:
        if any(tag.startswith("Dispatched") for tag in shopify_order.get('tags', [])):
            continue
        if shopify_order['status'] in ['Booked', 'Un-Booked', 'Drop Off at Express Center']:
            shopify_items = [
                {
                    'item_image':      item['image_src'],
                    'item_title':      item['product_title'],
                    'quantity':        item['quantity'],
                    'tracking_number': item['tracking_number'],
                    'status':          item['status']
                }
                for item in shopify_order['line_items']
            ]
            all_orders.append({
                'order_via':      'Shopify',
                'order_id':       shopify_order['order_id'],
                'status':         shopify_order['status'],
                'tracking_number': shopify_order['tracking_id'],
                'date':           shopify_order['created_at'],
                'items_list':     shopify_items,
                'total_price':    shopify_order['total_price']
            })
            for item in shopify_items:
                add_or_update_item(pending_items, {
                    'item_image': item['item_image'],
                    'item_title': item['item_title'],
                    'quantity':   item['quantity'],
                    'order_date': shopify_order['created_at']
                })

    half = len(pending_items) // 2
    return render_template('pending.html', all_orders=all_orders, pending_items=pending_items, half=half)


@app.route('/orders')
def pending_orders_mobile():
    return render_template('orders.html', all_orders=build_pending_orders_mobile_data(), employee_portal_mode=False)


@app.route('/undelivered')
def undelivered():
    global order_details, daraz_orders
    return render_template("undelivered.html", order_details=order_details, darazOrders=daraz_orders)


# ── Order status (Packed / Manufactured) ─────────────────────────────────────

@app.route("/update_status", methods=["POST"])
def update_status():
    data = request.get_json()
    order_id = str(data.get("order_id"))
    tracking_number = str(data.get("tracking_number", "N/A"))
    status = data.get("status")

    key = f"{order_id}:{tracking_number}"
    upsert_order_status(key, status)
    response_message = f"Status updated to {status} for {order_id} ({tracking_number})"

    if status == "Delivered in Lahore":
        matching_order = next(
            (
                order for order in order_details
                if normalize_scan_term(order.get("order_id")) == normalize_scan_term(order_id)
            ),
            None
        )
        if matching_order and matching_order.get("id"):
            try:
                if apply_shopify_order_tag(matching_order["id"], "Delivered in Lahore"):
                    local_tags = [tag for tag in (matching_order.get("tags") or []) if tag != "Leopards Courier"]
                    if "Delivered in Lahore" not in local_tags:
                        local_tags.append("Delivered in Lahore")
                    matching_order["tags"] = local_tags
                    response_message = (
                        f"Status updated to {status} for {order_id} ({tracking_number}). "
                        "Shopify tag applied: Delivered in Lahore."
                    )
            except Exception as e:
                print(f"Could not apply Shopify Lahore tag for {order_id}: {e}")

    return jsonify({"message": response_message})


@app.route("/employee_status/approve", methods=["POST"])
def approve_employee_status():
    data = request.get_json() or {}
    order_id = str(data.get("order_id") or "")
    tracking_number = str(data.get("tracking_number") or "N/A")
    requested_status = str(data.get("requested_status") or "").strip()
    key = f"{order_id}:{tracking_number}"

    if requested_status not in {"Delivered in Lahore", "Cancelled by Employee"}:
        return jsonify({"success": False, "error": "Unsupported employee approval status."}), 400

    matching_order = find_shopify_order_by_order_name(order_id)
    if not matching_order or not matching_order.get("id"):
        return jsonify({"success": False, "error": "Shopify order not found."}), 404

    try:
        order = shopify.Order.find(matching_order["id"])
        warnings = []
        if requested_status == "Delivered in Lahore":
            warnings = approve_shopify_delivery(order)
            matching_order["financial_status"] = "Paid"
            matching_order["fulfillment_status"] = "Fulfilled"
            matching_order["status"] = "Delivered"
            local_tags = [tag for tag in (matching_order.get("tags") or []) if tag != "Leopards Courier"]
            if "Delivered in Lahore Approved" not in local_tags:
                local_tags.append("Delivered in Lahore Approved")
            matching_order["tags"] = local_tags
        elif requested_status == "Cancelled by Employee":
            warnings = approve_shopify_cancellation(order)
            matching_order["status"] = "Cancelled"
            local_tags = [tag for tag in (matching_order.get("tags") or []) if tag != "Leopards Courier"]
            if "Cancelled by Employee" not in local_tags:
                local_tags.append("Cancelled by Employee")
            matching_order["tags"] = local_tags

        delete_order_status(key)

        message = f"Approved {requested_status} for {order_id}."
        if warnings:
            message = f"{message} Warnings: {' '.join(warnings)}"
        return jsonify({"success": True, "message": message, "warnings": warnings})
    except Exception as e:
        print(f"Employee approval failed for {order_id}: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/employee_portal', methods=['GET', 'POST'])
def employee_portal():
    next_url = employee_portal_safe_next_url(request.values.get('next'))

    if request.method == 'POST':
        submitted_password = (request.form.get('password') or '').strip()
        if submitted_password == EMPLOYEE_PORTAL_PASSWORD:
            session[EMPLOYEE_PORTAL_SESSION_KEY] = True
            return redirect(next_url)
        return render_template(
            'employee_portal.html',
            view='login',
            login_error='Wrong password. Try again.',
            next_url=next_url
        ), 401

    if not employee_portal_is_authenticated():
        return render_template('employee_portal.html', view='login', login_error='', next_url=next_url)

    return render_template('employee_portal.html', view='portal', employee_orders=build_employee_portal_orders())


@app.route('/employee_portal/orders')
def employee_portal_orders():
    if not employee_portal_is_authenticated():
        return redirect(url_for('employee_portal', next='/employee_portal/orders'))
    return render_template('orders.html', all_orders=build_pending_orders_mobile_data(), employee_portal_mode=True)


@app.route('/employee_portal/products')
def employee_portal_products():
    if not employee_portal_is_authenticated():
        return jsonify({'success': False, 'error': 'Unauthorized'}), 401
    return jsonify({'success': True, 'products': get_active_shopify_products()})


@app.route('/employee_portal/create-order', methods=['POST'])
def employee_portal_create_order():
    if not employee_portal_is_authenticated():
        return jsonify({'success': False, 'error': 'Unauthorized'}), 401

    data = request.get_json() or {}
    try:
        result = create_shopify_employee_order(data)
        return jsonify({
            'success': True,
            'draft_order_id': result.get('draft_order_id'),
            'order_id': result.get('order_id'),
            'order_name': result.get('order_name'),
            'invoice': result.get('invoice'),
        })
    except Exception as e:
        print(f"Employee order create failed: {e}")
        return jsonify({'success': False, 'error': str(e)}), 400


@app.route('/employee_portal/logout', methods=['POST'])
def employee_portal_logout():
    session.pop(EMPLOYEE_PORTAL_SESSION_KEY, None)
    return redirect(url_for('employee_portal'))


@app.route('/employee_portal/updates')
def employee_portal_updates():
    if not employee_portal_is_authenticated():
        return jsonify({'success': False, 'error': 'Unauthorized'}), 401

    orders = build_employee_portal_orders()
    order_summaries = [
        {
            'id': f"{order.get('source')}:{order.get('order_id')}",
            'order_id': order.get('order_id'),
            'source': order.get('source'),
            'created_at': order.get('created_at')
        }
        for order in orders
    ]
    order_summaries.sort(key=lambda entry: str(entry.get('created_at') or ''), reverse=True)
    return jsonify({
        'success': True,
        'count': len(order_summaries),
        'order_ids': [entry['id'] for entry in order_summaries],
        'latest': order_summaries[:6],
        'generated_at': datetime.now().isoformat(timespec='seconds')
    })


@app.route('/employee_portal-manifest.webmanifest')
def employee_portal_manifest():
    return send_from_directory('static', 'employee-portal.webmanifest', mimetype='application/manifest+json')


@app.route('/employee_portal-sw.js')
def employee_portal_service_worker():
    return send_from_directory('static', 'employee-portal-sw.js', mimetype='application/javascript')


@app.route('/employee_portal/report', methods=['POST'])
def employee_portal_report():
    if not employee_portal_is_authenticated():
        return jsonify({'success': False, 'error': 'Unauthorized'}), 401

    data = request.get_json() or {}
    mode = (data.get('mode') or '').strip().lower()
    scanned_orders = data.get('orders') or []

    if mode not in {'dispatch', 'return'}:
        return jsonify({'success': False, 'error': 'Invalid report mode.'}), 400

    if not scanned_orders:
        return jsonify({'success': False, 'error': 'No scanned orders provided.'}), 400

    tag_name = 'Dispatched' if mode == 'dispatch' else 'Return Received'
    tagged_count = 0
    skipped_count = 0
    seen_shopify_ids = set()

    try:
        for entry in scanned_orders:
            if entry.get('source') != 'shopify':
                skipped_count += 1
                continue

            shopify_id = entry.get('shopify_id')
            if not shopify_id or shopify_id in seen_shopify_ids:
                continue

            seen_shopify_ids.add(shopify_id)
            if apply_shopify_order_tag(shopify_id, tag_name, include_date=True):
                tagged_count += 1

        return jsonify({
            'success': True,
            'tagged_count': tagged_count,
            'skipped_count': skipped_count,
            'tag_name': tag_name,
        })
    except Exception as e:
        print(f"Employee portal report error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


# ── Webhooks ──────────────────────────────────────────────────────────────────

def get_shopify_webhook_secret():
    return (
        os.getenv('SHOPIFY_WEBHOOK_SECRET')
        or os.getenv('SHOPIFY_GRAPHQL_CLIENT_SECRET')
        or os.getenv('SHOPIFY_API_SECRET')
    )


def verify_shopify_webhook(req):
    shopify_hmac = req.headers.get('X-Shopify-Hmac-Sha256')
    data = req.get_data()
    secret = get_shopify_webhook_secret()

    if not secret:
        raise ValueError("Shopify webhook secret is not set.")
    if not shopify_hmac:
        return False

    digest = hmac.new(secret.encode('utf-8'), data, hashlib.sha256).digest()
    computed_hmac = base64.b64encode(digest).decode('utf-8')
    return hmac.compare_digest(computed_hmac, shopify_hmac)


def normalize_shopify_order_id(order_id):
    return str(order_id or '').strip()


def remove_shopify_order_from_portal(order_id):
    normalized_id = normalize_shopify_order_id(order_id)
    if not normalized_id:
        return 0

    with order_details_lock:
        before_count = len(order_details)
        order_details[:] = [
            order for order in order_details
            if normalize_shopify_order_id(order.get('id')) != normalized_id
        ]
        return before_count - len(order_details)


def shopify_payload_removes_order(order_data, topic):
    topic = (topic or '').lower()
    return (
        topic in {'orders/cancelled', 'orders/delete'}
        or bool(order_data.get('cancelled_at'))
        or bool(order_data.get('closed_at'))
    )


def build_shopify_order_state():
    with order_details_lock:
        orders = [
            {
                'id': normalize_shopify_order_id(order.get('id')),
                'order_id': order.get('order_id', ''),
                'status': order.get('status', ''),
                'tags': order.get('tags', []),
                'updated': order.get('updated_at') or order.get('created_at', ''),
            }
            for order in order_details
        ]

    state_json = json.dumps(orders, sort_keys=True, default=str)
    return {
        'success': True,
        'count': len(orders),
        'fingerprint': hashlib.sha256(state_json.encode('utf-8')).hexdigest(),
        'generated_at': datetime.now().isoformat(timespec='seconds'),
    }


@app.route('/api/shopify/order-state')
def shopify_order_state():
    return jsonify(build_shopify_order_state())


@app.route('/shopify/webhook/orders/create', methods=['POST'])
@app.route('/shopify/webhook/orders/paid', methods=['POST'])
@app.route('/shopify/webhook/orders/updated', methods=['POST'])
@app.route('/shopify/webhook/orders/cancelled', methods=['POST'])
@app.route('/shopify/webhook/orders/delete', methods=['POST'])
@app.route('/shopify/webhook/order_updated', methods=['POST'])
def shopify_order_updated():
    global order_details
    try:
        if not verify_shopify_webhook(request):
            return jsonify({'error': 'Invalid webhook signature'}), 401

        topic = request.headers.get('X-Shopify-Topic', '')
        order_data = request.get_json(silent=True) or {}
        order_id = order_data.get('id')
        if not order_id:
            return jsonify({'error': 'No order id found in payload'}), 400

        if shopify_payload_removes_order(order_data, topic):
            removed_count = remove_shopify_order_from_portal(order_id)
            return jsonify({
                'success': True,
                'message': f'Order {order_id} removed.',
                'removed_count': removed_count,
                'topic': topic,
            }), 200

        order = shopify.Order.find(order_id)
        if not order:
            return jsonify({'error': f'Order {order_id} not found'}), 404

        # Build a minimal tracking cache for this one order
        tracking_numbers = [
            f.tracking_number for f in order.fulfillments
            if f.status != "cancelled" and f.tracking_number
        ]
        tracking_cache = {}
        if tracking_numbers:
            api_key = os.getenv('LEOPARD_API_KEY')
            api_password = os.getenv('LEOPARD_PASSWORD')
            joined = ','.join(tracking_numbers)
            url = (
                f"https://merchantapi.leopardscourier.com/api/trackBookedPacket/"
                f"?api_key={api_key}&api_password={api_password}&track_numbers={joined}"
            )
            try:
                r = requests.get(url, verify=False, timeout=20)
                d = r.json()
                if d.get('status') == 1:
                    for packet in d.get('packet_list', []):
                        cn = packet.get('track_number')
                        if cn:
                            tracking_cache[cn] = packet
            except Exception as e:
                print(f"Webhook tracking fetch error: {e}")

        updated_order_info = asyncio.run(process_order(order, tracking_cache))
        updated_order_info = enrich_orders_with_protected_customer_data([updated_order_info])[0]
        if (topic or '').lower() == 'orders/paid' or str(updated_order_info.get('financial_status', '')).lower() == 'paid':
            send_order_confirmation(updated_order_info)

        updated = False
        updated_order_id = normalize_shopify_order_id(updated_order_info.get('id'))
        with order_details_lock:
            for idx, existing_order in enumerate(order_details):
                if normalize_shopify_order_id(existing_order.get('id')) == updated_order_id:
                    order_details[idx] = updated_order_info
                    updated = True
                    break
            if not updated:
                order_details.append(updated_order_info)
            order_details.sort(key=lambda item: str(item.get('created_at') or ''), reverse=True)

        return jsonify({
            'success': True,
            'message': f'Order {order_id} processed successfully',
            'created': not updated,
            'topic': topic,
        }), 200
    except Exception as e:
        print(f"Webhook processing error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/webhook/leopards', methods=['POST'])
def leopards_webhook():
    global order_details

    try:
        payload = request.get_json()
        if not payload or 'data' not in payload:
            return jsonify([{"status": 0, "errors": ["Invalid payload"]}]), 400

        updates = payload['data']
        if not isinstance(updates, list) or len(updates) == 0:
            return jsonify([{"status": 0, "errors": ["Empty data array"]}]), 400

        STATUS_MAP = {
            'RC': 'Consignment Booked', 'AC': 'Out For Delivery', 'DV': 'Delivered',
            'PN1': 'First Attempt Failed', 'PN2': 'Second Attempt Failed',
            'RO': 'Being Return', 'RN1': 'First Return Attempt', 'RN2': 'Second Return Attempt',
            'RW': 'Returned to Warehouse', 'DW': 'Delivered to Warehouse',
            'RS': 'RETURNED TO SHIPPER', 'DR': 'Delivered to Vendor',
            'AR': 'Arrived At Station', 'DP': 'Dispatched',
            'NR': 'Ready for Return', 'SP': 'Shipment Picked',
        }
        TERMINAL_STATUSES = {'DV', 'RW', 'DW', 'RS', 'DR'}
        updated_count = 0

        for update in updates:
            cn_number    = update.get('cn_number', '').strip()
            status_code  = update.get('status', '').strip()
            reason       = update.get('reason', '')

            if not cn_number or not status_code:
                continue

            human_status = STATUS_MAP.get(status_code, status_code)
            if reason and reason != 'N/A' and reason.strip():
                human_status = f"{human_status} - {reason.strip()}"

            if status_code == 'RS':
                human_status = 'RETURNED TO SHIPPER'
            elif status_code in ('RO', 'RN1', 'RN2', 'RW'):
                human_status = f"Being Return {reason}".strip() if reason and reason != 'N/A' else 'Being Return'

            for order in order_details:
                order_updated = False
                for item in order.get('line_items', []):
                    if item.get('tracking_number') == cn_number:
                        item['status'] = human_status
                        order_updated = True
                        updated_count += 1

                if order_updated:
                    all_statuses = [li.get('status', '') for li in order.get('line_items', [])]
                    if 'RETURNED TO SHIPPER' in all_statuses:
                        order['status'] = 'RETURNED TO SHIPPER'
                    elif any('Delivered' in s for s in all_statuses):
                        order['status'] = 'Delivered'
                    elif any('Being Return' in s for s in all_statuses):
                        order['status'] = next(s for s in all_statuses if 'Being Return' in s)
                    else:
                        order['status'] = human_status

                    if status_code in TERMINAL_STATUSES and order.get('id'):
                        try:
                            tag_map = {'DV': 'Delivered', 'RS': 'Returned', 'RW': 'Returned'}
                            tag = tag_map.get(status_code)
                            if tag:
                                shopify_order = shopify.Order.find(order['id'])
                                today_date = datetime.now().strftime('%Y-%m-%d')
                                tag_with_date = f"{tag} ({today_date})"
                                existing_tags = [t.strip() for t in shopify_order.tags.split(',')] if shopify_order.tags else []
                                if tag_with_date not in existing_tags:
                                    existing_tags.append(tag_with_date)
                                    shopify_order.tags = ', '.join(existing_tags)
                                    shopify_order.save()
                        except Exception as tag_err:
                            print(f"Failed to auto-tag order {order.get('order_id')}: {tag_err}")

        print(f"Leopards webhook: {len(updates)} updates, {updated_count} line items matched.")
        return jsonify([{"status": 1, "errors": []}]), 202
    except Exception as e:
        print(f"Leopards webhook error: {e}")
        return jsonify([{"status": 0, "errors": [str(e)]}]), 400


# ── Scanner ───────────────────────────────────────────────────────────────────

@app.route('/scan', methods=['GET', 'POST'])
def search():
    global order_details, daraz_orders
    if request.method == 'GET' and 'term' not in request.args:
        return render_template('scan.html', order_details=order_details)

    search_term = (request.args.get('term') or request.form.get('search_term') or "").split(',')[0].strip()
    if not search_term:
        return jsonify({"error": "No search term provided"}), 400

    order_found = None
    source = None

    for order in order_details:
        if order.get('order_id') == search_term:
            order_found = order; source = 'shopify'; break
        if any(item.get('tracking_number') == search_term for item in order.get('line_items', [])):
            order_found = order; source = 'shopify'; break

    if not order_found:
        for order in daraz_orders:
            if str(order.get('order_id')) == search_term:
                order_found = order; source = 'daraz'; break
            if any(item.get('tracking_number') == search_term for item in order.get('items_list', [])):
                order_found = order; source = 'daraz'; break

    if order_found:
        if source == 'daraz':
            formatted_order = {
                'order_id': str(order_found.get('order_id')),
                'line_items': [{
                    'product_title': item.get('item_title'),
                    'quantity':      item.get('quantity'),
                    'image_src':     item.get('item_image'),
                    'tracking_number': item.get('tracking_number', 'N/A')
                } for item in order_found.get('items_list', [])],
                'id': None, 'source': 'daraz'
            }
        else:
            formatted_order = order_found.copy()
            formatted_order['source'] = 'shopify'

        if request.method == 'POST':
            return render_template('scan.html', order_details=order_details, search_term=search_term, order_found=formatted_order)
        return jsonify(formatted_order)

    if request.method == 'POST':
        return render_template('scan.html', order_details=order_details, search_term=search_term, order_found=None)
    return jsonify({"error": "Order not found"}), 404


# ── Payments & Leopards proxy routes ─────────────────────────────────────────

import requests as _req


def build_tracking_lookup():
    lookup = {}

    for order in order_details:
        customer = order.get('customer_details') or {}
        for item in order.get('line_items', []) or []:
            cn = str(item.get('tracking_number') or '').strip()
            if not cn or cn == 'N/A' or cn in lookup:
                continue
            lookup[cn] = {
                'order_via': 'Shopify',
                'order_id': order.get('order_id', ''),
                'order_link': order.get('order_link', ''),
                'customer_name': customer.get('name') or item.get('name', ''),
                'customer_phone': customer.get('phone') or item.get('phone', ''),
                'customer_city': customer.get('city') or item.get('city', ''),
                'item_title': item.get('product_title', ''),
                'item_image': item.get('image_src', ''),
                'amount': order.get('total_price', 0),
                'status': order.get('status', ''),
            }

    for order in daraz_orders:
        customer = order.get('customer') or {}
        for item in order.get('items_list', []) or []:
            cn = str(item.get('tracking_number') or '').strip()
            if not cn or cn == 'N/A' or cn in lookup:
                continue
            lookup[cn] = {
                'order_via': 'Daraz',
                'order_id': order.get('order_id', ''),
                'order_link': '',
                'customer_name': customer.get('name', ''),
                'customer_phone': customer.get('phone', ''),
                'customer_city': '',
                'item_title': item.get('item_title', ''),
                'item_image': item.get('item_image', ''),
                'amount': order.get('total_price', 0),
                'status': order.get('status', ''),
            }

    return lookup


def normalize_shipper_advice_items(payload):
    raw_items = payload.get('data') or payload.get('packet_list') or []
    if not isinstance(raw_items, list):
        return []

    tracking_lookup = build_tracking_lookup()
    items = []
    for item in raw_items:
        cn = str(item.get('cn_number') or item.get('track_number') or '').strip()
        match = tracking_lookup.get(cn, {})
        advice_status = str(item.get('shipper_advice_status') or '').strip()
        remarks = str(item.get('shipper_remarks') or item.get('remarks') or '').strip()
        items.append({
            'id': item.get('id'),
            'cn_number': cn,
            'status': item.get('status') or item.get('booked_packet_status') or '',
            'reason': item.get('reason') or item.get('pending_reason') or '',
            'product': item.get('product') or item.get('shipment_name_eng') or '',
            'shipper_advice_status': advice_status,
            'shipper_remarks': remarks,
            'created_date': item.get('created_date') or item.get('advice_date_created') or item.get('booked_packet_date') or '',
            'consignee_name': item.get('consignee_name') or item.get('consignment_name_eng') or '',
            'consignee_address': item.get('consignee_address') or item.get('consignment_address') or '',
            'consignee_mobile': item.get('consignee_mobile') or item.get('consignment_phone') or '',
            'destination_city_name': item.get('destination_city_name') or '',
            'matched_order': match,
        })

    items.sort(key=lambda entry: str(entry.get('created_date') or ''), reverse=True)
    return items


@app.route('/payments')
def payments_page():
    return render_template('payments.html')


@app.route('/exhibition')
def exhibition_page():
    return render_template('exhibition.html')


@app.route('/exhibition/accounts')
def exhibition_accounts_page():
    return render_template('exhibition_accounts.html', target_sale=EXHIBITION_TARGET_SALE)


@app.route('/exhibition/orders')
def exhibition_orders_page():
    return render_template('exhibition_orders.html')


@app.route('/exhibition/invoice/<int:order_id>')
def exhibition_invoice_page(order_id):
    order = get_exhibition_order(order_id)
    if not order:
        return "Invoice not found", 404
    invoice = build_exhibition_invoice_payload(order)
    return render_template('exhibition_invoice.html', invoice=invoice)


@app.route('/exhibition/invoice/<int:order_id>/plain')
def exhibition_plain_invoice_page(order_id):
    order = get_exhibition_order(order_id)
    if not order:
        return "Invoice not found", 404
    invoice = build_exhibition_invoice_payload(order)
    receipt = build_exhibition_plain_receipt(invoice)
    return Response(receipt, mimetype='text/plain; charset=utf-8')


@app.route('/api/exhibition/bootstrap')
def exhibition_bootstrap_api():
    try:
        exhibitions = [exhibition_serialize_row(row) for row in list_exhibitions()]
        products = build_exhibition_product_catalog()
        recent_orders = [exhibition_serialize_row(row) for row in list_exhibition_orders(limit=25)]
        return jsonify({
            'ok': True,
            'exhibitions': exhibitions,
            'products': products,
            'recent_orders': recent_orders,
            'delivery_methods': sorted(EXHIBITION_DELIVERY_METHODS),
            'payment_methods': sorted(EXHIBITION_PAYMENT_METHODS),
            'payment_splits': ['100% Paid', '50% Paid', 'Custom Amount'],
        })
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500


@app.route('/api/exhibition/exhibitions', methods=['POST'])
def exhibition_create_exhibition_api():
    payload = request.get_json(silent=True) or request.form or {}
    row = create_exhibition(
        name=payload.get('name'),
        location=payload.get('location') or '',
        starts_on=payload.get('starts_on') or None,
        ends_on=payload.get('ends_on') or None,
        notes=payload.get('notes') or '',
    )
    if not row:
        return jsonify({'ok': False, 'error': 'Exhibition name is required.'}), 400
    return jsonify({'ok': True, 'exhibition': exhibition_serialize_row(row)})


@app.route('/api/exhibition/orders', methods=['POST'])
def exhibition_create_order_api():
    payload = request.get_json(silent=True) or {}
    normalized, error = normalize_exhibition_order_payload(payload)
    if error:
        message, status_code = error
        return jsonify({'ok': False, 'error': message}), status_code
    order = create_exhibition_order(
        order_number=make_exhibition_order_number(),
        **normalized,
    )
    if not order:
        return jsonify({'ok': False, 'error': 'Could not create exhibition order.'}), 500
    order_id = order.get('id')
    return jsonify({
        'ok': True,
        'order': exhibition_serialize_row(order),
        'invoice_url': url_for('exhibition_invoice_page', order_id=order_id),
    })


@app.route('/api/exhibition/orders/<int:order_id>', methods=['GET'])
def exhibition_get_order_api(order_id):
    order = get_exhibition_order(order_id)
    if not order:
        return jsonify({'ok': False, 'error': 'Order not found.'}), 404
    return jsonify({'ok': True, 'order': exhibition_serialize_row(order)})


@app.route('/api/exhibition/orders/<int:order_id>', methods=['PUT'])
def exhibition_update_order_api(order_id):
    if not get_exhibition_order(order_id):
        return jsonify({'ok': False, 'error': 'Order not found.'}), 404
    payload = request.get_json(silent=True) or {}
    normalized, error = normalize_exhibition_order_payload(payload)
    if error:
        message, status_code = error
        return jsonify({'ok': False, 'error': message}), status_code
    order = update_exhibition_order(order_id=order_id, **normalized)
    if not order:
        return jsonify({'ok': False, 'error': 'Could not update exhibition order.'}), 500
    return jsonify({
        'ok': True,
        'order': exhibition_serialize_row(order),
        'invoice_url': url_for('exhibition_invoice_page', order_id=order_id),
    })


@app.route('/api/exhibition/orders/<int:order_id>', methods=['DELETE'])
def exhibition_delete_order_api(order_id):
    if not delete_exhibition_order(order_id):
        return jsonify({'ok': False, 'error': 'Order not found or could not be deleted.'}), 404
    return jsonify({'ok': True})


@app.route('/api/exhibition/orders-list')
def exhibition_orders_list_api():
    exhibition_id = request.args.get('exhibition_id') or None
    search = normalize_catalog_match(request.args.get('search') or '')
    delivery_method = (request.args.get('delivery_method') or '').strip()
    rows = []
    for row in list_exhibition_orders(exhibition_id=exhibition_id):
        serialized = exhibition_serialize_row(row)
        if delivery_method and serialized.get('delivery_method') != delivery_method:
            continue
        haystack = normalize_catalog_match(
            " ".join([
                str(serialized.get('order_number') or ''),
                str(serialized.get('product_name') or ''),
                str(serialized.get('customer_name') or ''),
                str(serialized.get('customer_phone') or ''),
                str(serialized.get('exhibition_name') or ''),
            ])
        )
        if search and search not in haystack:
            continue
        rows.append(serialized)
    total_sale = sum((money_decimal(row.get('total_amount')) for row in rows), Decimal('0'))
    total_paid = sum((money_decimal(row.get('paid_amount')) for row in rows), Decimal('0'))
    return jsonify({
        'ok': True,
        'exhibitions': [exhibition_serialize_row(row) for row in list_exhibitions()],
        'delivery_methods': sorted(EXHIBITION_DELIVERY_METHODS),
        'orders': rows,
        'summary': {
            'order_count': len(rows),
            'total_sale': money_float(total_sale),
            'total_paid': money_float(total_paid),
            'balance': money_float(total_sale - total_paid),
        },
    })


@app.route('/api/exhibition/orders/<int:order_id>/product-cost', methods=['PUT'])
def exhibition_update_order_product_cost_api(order_id):
    if not get_exhibition_order(order_id):
        return jsonify({'ok': False, 'error': 'Order not found.'}), 404
    payload = request.get_json(silent=True) or {}
    raw_value = payload.get('product_cost')
    product_cost = None if raw_value in (None, '') else max(money_float(raw_value), 0)
    order = update_exhibition_order_product_cost(order_id, product_cost)
    if not order:
        return jsonify({'ok': False, 'error': 'Could not update product cost.'}), 500
    return jsonify({'ok': True, 'order': exhibition_serialize_row(order)})


@app.route('/api/exhibition/accounts')
def exhibition_accounts_api():
    exhibition_id = request.args.get('exhibition_id') or None
    orders = list_exhibition_orders(exhibition_id=exhibition_id)
    expenses = list_exhibition_expenses(exhibition_id=exhibition_id)
    lookup = build_exhibition_cost_lookup()
    order_rows = []
    total_sale = Decimal('0')
    total_paid = Decimal('0')
    total_product_cost = Decimal('0')
    for order in orders:
        item_cost_rows = []
        product_cost_total = Decimal('0')
        primary_match = None
        for item in get_exhibition_order_items(order):
            match = match_exhibition_order_cost(item, lookup)
            matched_row = match.get('row')
            if primary_match is None:
                primary_match = match
            qty = int(item.get('quantity') or 1)
            unit_cost = Decimal(str(matched_row.get('exhibition_cost') if matched_row else 0))
            line_cost = unit_cost * Decimal(qty)
            product_cost_total += line_cost
            item_cost_rows.append({
                'product_name': item.get('product_name') or '',
                'quantity': qty,
                'matched_cost_name': matched_row.get('match_label') if matched_row else '',
                'matched_cost': money_float(unit_cost),
                'line_cost': money_float(line_cost),
                'match_reason': match.get('reason'),
                'match_score': match.get('score'),
            })
        auto_product_cost_total = product_cost_total
        has_manual_cost = order.get('product_cost_override') is not None
        if has_manual_cost:
            product_cost_total = money_decimal(order.get('product_cost_override'))
        total_product_cost += product_cost_total
        total_sale += money_decimal(order.get('total_amount'))
        total_paid += money_decimal(order.get('paid_amount'))
        serialized = exhibition_serialize_row(order)
        primary_match = primary_match or {'row': None, 'reason': 'No cost match', 'score': 0}
        primary_row = primary_match.get('row')
        serialized.update({
            'item_costs': item_cost_rows,
            'matched_cost_name': primary_row.get('match_label') if primary_row else '',
            'matched_cost': item_cost_rows[0]['matched_cost'] if item_cost_rows else 0,
            'product_cost_total': money_float(product_cost_total),
            'auto_product_cost_total': money_float(auto_product_cost_total),
            'product_cost_override': money_float(order.get('product_cost_override')) if has_manual_cost else None,
            'has_manual_product_cost': has_manual_cost,
            'match_reason': primary_match.get('reason'),
            'match_score': primary_match.get('score'),
            'cost_link': f"/shopify-product-costs?search={quote(str(order.get('product_name') or ''))}",
        })
        order_rows.append(serialized)

    total_expense = sum((money_decimal(row.get('amount')) for row in expenses), Decimal('0'))
    net_profit = total_sale - total_product_cost - total_expense
    target_left = Decimal(str(EXHIBITION_TARGET_SALE)) - total_sale
    return jsonify({
        'ok': True,
        'exhibitions': [exhibition_serialize_row(row) for row in list_exhibitions()],
        'orders': order_rows,
        'expenses': [exhibition_serialize_row(row) for row in expenses],
        'summary': {
            'order_count': len(orders),
            'total_sale': money_float(total_sale),
            'total_paid': money_float(total_paid),
            'target_left': money_float(target_left),
            'total_expense': money_float(total_expense),
            'product_cost': money_float(total_product_cost),
            'net_profit': money_float(net_profit),
            'target_sale': EXHIBITION_TARGET_SALE,
        },
    })


@app.route('/api/exhibition/expenses', methods=['POST'])
def exhibition_create_expense_api():
    payload = request.get_json(silent=True) or {}
    row = create_exhibition_expense(
        exhibition_id=payload.get('exhibition_id'),
        label=payload.get('label'),
        amount=payload.get('amount') or 0,
        expense_date=payload.get('expense_date') or None,
        notes=payload.get('notes') or '',
    )
    if not row:
        return jsonify({'ok': False, 'error': 'Exhibition and expense label are required.'}), 400
    return jsonify({'ok': True, 'expense': exhibition_serialize_row(row)})


@app.route('/product-costs')
def product_costs_page():
    return redirect(url_for('shopify_product_costs_page'))


@app.route('/shopify-product-costs')
def shopify_product_costs_page():
    return render_template(
        'product_costs.html',
        page_title='Shopify Product Cost',
        page_subtitle='Beanbag cost calculator for Shopify products',
        default_source=COST_SOURCE_SHOPIFY,
        beans_price=get_shopify_beans_price(),
    )


@app.route('/daraz-product-costs')
def daraz_product_costs_page():
    return render_template(
        'product_costs.html',
        page_title='Daraz Product Cost',
        page_subtitle='Daraz product price and cost tracking',
        default_source=COST_SOURCE_DARAZ,
        beans_price=get_shopify_beans_price(),
    )


@app.route('/daraz-product-costs/sync-review')
def daraz_product_sync_review_page():
    return render_template('daraz_sync_review.html')


@app.route('/daraz-profits')
def daraz_profits_page():
    return render_template('daraz_profits.html')


@app.route('/api/costing/catalog')
def costing_catalog_api():
    source = (request.args.get('source') or COST_SOURCE_DARAZ).strip().lower()
    if source not in COST_SOURCE_LABELS:
        return jsonify({'ok': False, 'error': 'Unknown source.'}), 400
    try:
        rows = get_cost_catalog_for_source(source)
        saved = list_product_costs(source)
        return jsonify({
            'ok': True,
            'source': source,
            'label': COST_SOURCE_LABELS[source],
            'rows': rows,
            'saved_count': len(saved),
            'beans_price': get_shopify_beans_price(),
        })
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500


@app.route('/api/daraz-sync/review')
def daraz_sync_review_api():
    try:
        return jsonify({'ok': True, **build_daraz_sync_plan()})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500


@app.route('/api/daraz-sync/save-matched-prices', methods=['POST'])
def daraz_sync_save_matched_prices_api():
    try:
        rows = with_saved_costs(COST_SOURCE_DARAZ, build_daraz_cost_catalog())
        results = []
        for row in rows:
            matched_price = money_float(row.get('matched_shopify_price'))
            if matched_price <= 0:
                continue
            item = {
                'variant_key': row.get('variant_key'),
                'name': row.get('primary_name'),
                'price': matched_price,
                'ok': False,
            }
            try:
                save_daraz_matched_price(row, matched_price)
                item['ok'] = True
            except Exception as e:
                item['error'] = str(e)
            results.append(item)
        return jsonify({
            'ok': True,
            'saved': sum(1 for item in results if item.get('ok')),
            'failed': sum(1 for item in results if not item.get('ok')),
            'results': results,
        })
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500


@app.route('/api/daraz-sync/apply', methods=['POST'])
def daraz_sync_apply_api():
    payload = request.get_json(silent=True) or {}
    selected = set(str(item) for item in (payload.get('selected_ids') or []))
    settings = payload.get('settings') or {}
    if not selected:
        return jsonify({'ok': False, 'error': 'Select at least one sync item.'}), 400

    try:
        shopify_groups = group_cost_rows(with_saved_costs(COST_SOURCE_SHOPIFY, build_shopify_cost_catalog()), COST_SOURCE_SHOPIFY)
        daraz_rows = with_saved_costs(COST_SOURCE_DARAZ, build_daraz_cost_catalog())
        daraz_groups = group_cost_rows(daraz_rows, COST_SOURCE_DARAZ)
        daraz_rows_by_variant = {row.get('variant_key'): row for row in daraz_rows}
        shopify_rows_by_variant = {
            row.get('variant_key'): row
            for group in shopify_groups.values()
            for row in group.get('rows') or []
        }
        plan = build_daraz_sync_plan()
        results = []

        for action in plan.get('price_updates') or []:
            if action.get('id') not in selected:
                continue
            row = daraz_rows_by_variant.get(action.get('daraz_variant_key'))
            item = {'id': action.get('id'), 'type': 'price', 'name': action.get('daraz_name'), 'ok': False}
            try:
                save_daraz_matched_price(row, action.get('shopify_price'))
                item['ok'] = True
            except Exception as e:
                item['error'] = str(e)
            results.append(item)

        for action in plan.get('missing_products') or []:
            if action.get('id') not in selected:
                continue
            group = shopify_groups.get(action.get('shopify_group_key'))
            item = {'id': action.get('id'), 'type': 'create_product', 'name': action.get('name'), 'ok': False}
            try:
                item['response'] = create_daraz_product_from_shopify(group, settings)
                item['ok'] = True
            except Exception as e:
                item['error'] = str(e)
            results.append(item)

        for action in plan.get('missing_variants') or []:
            if action.get('id') not in selected:
                continue
            row = shopify_rows_by_variant.get(action.get('shopify_variant_key'))
            target_group = next((group for group in daraz_groups.values() if group.get('item_id') == action.get('target_daraz_item_id')), None)
            item = {'id': action.get('id'), 'type': 'add_variant', 'name': action.get('name'), 'ok': False}
            try:
                item['response'] = add_daraz_variant_from_shopify(row, target_group, settings)
                item['ok'] = True
            except Exception as e:
                item['error'] = str(e)
            results.append(item)

        return jsonify({
            'ok': True,
            'applied': sum(1 for item in results if item.get('ok')),
            'failed': sum(1 for item in results if not item.get('ok')),
            'results': results,
        })
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500


@app.route('/api/costing/save', methods=['POST'])
def costing_save_api():
    payload = request.get_json(silent=True) or {}
    source = str(payload.get('source') or '').strip().lower()
    variant_key = str(payload.get('variant_key') or '').strip()
    primary_name = str(payload.get('primary_name') or '').strip()
    secondary_name = str(payload.get('secondary_name') or '').strip()
    sku = str(payload.get('sku') or '').strip()
    source_product_id = str(payload.get('source_product_id') or '').strip()
    source_variant_id = str(payload.get('source_variant_id') or '').strip()
    product_price = money_float(payload.get('product_price'))
    calc_fields = {
        'beans_kg': money_float(payload.get('beans_kg')),
        'fabric_cost': money_float(payload.get('fabric_cost')),
        'yard_qty': money_float(payload.get('yard_qty')),
        'fusium_cost': money_float(payload.get('fusium_cost')),
        'making_cost': money_float(payload.get('making_cost')),
        'overhead_cost': money_float(payload.get('overhead_cost')),
        'delivery_cost': money_float(payload.get('delivery_cost')),
        'return_cost': money_float(payload.get('return_cost')),
        'ads_cost': money_float(payload.get('ads_cost')),
    }

    if source not in COST_SOURCE_LABELS:
        return jsonify({'ok': False, 'error': 'Unknown source.'}), 400
    if not variant_key:
        return jsonify({'ok': False, 'error': 'Variant key is required.'}), 400
    if not primary_name:
        return jsonify({'ok': False, 'error': 'Primary name is required.'}), 400

    product_cost = calculate_shopify_net_cost(calc_fields, get_shopify_beans_price()) if source == COST_SOURCE_SHOPIFY else money_float(payload.get('product_cost'))
    try:
        if source == COST_SOURCE_SHOPIFY and source_variant_id:
            update_shopify_variant_price(source_variant_id, product_price)
        if source == COST_SOURCE_DARAZ:
            update_daraz_sku_price(source_product_id, source_variant_id, sku, product_price)

        success = upsert_product_cost(
            source=source,
            variant_key=variant_key,
            primary_name=primary_name,
            secondary_name=secondary_name,
            sku=sku,
            product_price=str(product_price),
            beans_kg=str(calc_fields['beans_kg']),
            fabric_cost=str(calc_fields['fabric_cost']),
            yard_qty=str(calc_fields['yard_qty']),
            fusium_cost=str(calc_fields['fusium_cost']),
            making_cost=str(calc_fields['making_cost']),
            overhead_cost=str(calc_fields['overhead_cost']),
            delivery_cost=str(calc_fields['delivery_cost']),
            return_cost=str(calc_fields['return_cost']),
            ads_cost=str(calc_fields['ads_cost']),
            product_cost=str(product_cost),
            source_product_id=source_product_id,
            source_variant_id=source_variant_id,
        )
        if not success:
            return jsonify({'ok': False, 'error': 'Could not save product cost.'}), 500
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500

    return jsonify({
        'ok': True,
        'saved': {
            'source': source,
            'variant_key': variant_key,
            'primary_name': primary_name,
            'secondary_name': secondary_name,
            'sku': sku,
            'product_price': product_price,
            **calc_fields,
            'product_cost': float(product_cost),
            'source_product_id': source_product_id,
            'source_variant_id': source_variant_id,
        }
    })


@app.route('/api/costing/beans-price', methods=['POST'])
def costing_beans_price_api():
    payload = request.get_json(silent=True) or {}
    beans_price = money_float(payload.get('beans_price') or DEFAULT_SHOPIFY_BEANS_PRICE)
    if not save_shopify_beans_price(beans_price):
        return jsonify({'ok': False, 'error': 'Could not save beans price.'}), 500
    return jsonify({'ok': True, 'beans_price': beans_price})


@app.route('/api/daraz/profits')
def daraz_profits_api():
    start_date = (request.args.get('from_date') or '').strip()
    end_date = (request.args.get('to_date') or '').strip()
    try:
        payload = build_daraz_profit_records(start_date=start_date, end_date=end_date)
        return jsonify({'ok': True, **payload})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500


@app.route('/api/leopards/last-status')
def leopards_last_status():
    from_date = request.args.get('from_date', '')
    to_date = request.args.get('to_date', '')
    api_key = os.getenv('LEOPARD_API_KEY')
    api_password = os.getenv('LEOPARD_PASSWORD')
    url = (
        f"https://merchantapi.leopardscourier.com/api/getBookedPacketLastStatus/format/json/"
        f"?api_key={api_key}&api_password={api_password}"
        f"&from_date={from_date}&to_date={to_date}"
    )
    try:
        r = _req.get(url, verify=False, timeout=30)
        data = r.json()
        filtered = [
            p for p in data.get('packet_list', [])
            if (p.get('booked_packet_status') or '').strip().lower()
            not in ('pickup request not send', 'pickup request sent')
        ]
        data['packet_list'] = filtered
        return jsonify(data)
    except Exception as e:
        return jsonify({"status": 0, "error": str(e)}), 500


@app.route('/api/leopards/payment-details')
def leopards_payment_details():
    cn_numbers = request.args.get('cn_numbers', '')
    cn_list = [cn.strip() for cn in cn_numbers.split(',') if cn.strip()]
    api_key = os.getenv('LEOPARD_API_KEY')
    api_password = os.getenv('LEOPARD_PASSWORD')
    all_payments = []
    for i in range(0, len(cn_list), 20):
        chunk = ','.join(cn_list[i:i+20])
        url = (
            f"https://merchantapi.leopardscourier.com/api/getPaymentDetails/format/json/"
            f"?api_key={api_key}&api_password={api_password}&cn_numbers={chunk}"
        )
        try:
            r = _req.get(url, verify=False, timeout=30)
            d = r.json()
            if d.get('payment_list'):
                all_payments.extend(d['payment_list'])
        except Exception:
            pass
    return jsonify({"status": 1, "payment_list": all_payments})


@app.route('/api/leopards/shipping-charges')
def leopards_shipping_charges():
    cn_numbers = request.args.get('cn_numbers', '')
    cn_list = [cn.strip() for cn in cn_numbers.split(',') if cn.strip()]
    api_key = os.getenv('LEOPARD_API_KEY')
    api_password = os.getenv('LEOPARD_PASSWORD')
    all_data = []
    for i in range(0, len(cn_list), 20):
        chunk = ','.join(cn_list[i:i+20])
        url = (
            f"https://merchantapi.leopardscourier.com/api/getShippingCharges/format/json/"
            f"?api_key={api_key}&api_password={api_password}&cn_numbers={chunk}"
        )
        try:
            r = _req.get(url, verify=False, timeout=30)
            d = r.json()
            if d.get('data'):
                all_data.extend(d['data'])
        except Exception:
            pass
    return jsonify({"status": 1, "data": all_data})


@app.route('/api/leopards/invoices')
def leopards_invoices():
    start_date = request.args.get('start_date', '')
    end_date = request.args.get('end_date', '')
    api_key = os.getenv('LEOPARD_API_KEY')
    api_password = os.getenv('LEOPARD_PASSWORD')
    url = (
        f"https://merchantapi.leopardscourier.com/api/getInvoices/format/json/"
        f"?api_key={api_key}&api_password={api_password}"
        f"&start_date={start_date}&end_date={end_date}"
    )
    try:
        r = _req.get(url, verify=False, timeout=30)
        return jsonify(r.json())
    except Exception as e:
        return jsonify({"status": 0, "error": str(e), "data": []}), 500


@app.route('/api/debug/finance')
def debug_finance():
    cn = 'LE7523036243'
    api_key = os.getenv('LEOPARD_API_KEY')
    api_password = os.getenv('LEOPARD_PASSWORD')
    results = {}
    for name, url in [
        ('shipping_charges', f"https://merchantapi.leopardscourier.com/api/getShippingCharges/format/json/?api_key={api_key}&api_password={api_password}&cn_numbers={cn}"),
        ('payment_details',  f"https://merchantapi.leopardscourier.com/api/getPaymentDetails/format/json/?api_key={api_key}&api_password={api_password}&cn_numbers={cn}"),
    ]:
        try:
            r = _req.get(url, verify=False, timeout=30)
            results[name] = {'http_status': r.status_code, 'body': r.json()}
        except Exception as e:
            results[name] = {'error': str(e)}
    return jsonify(results)


@app.route('/api/leopards/track-packets')
def leopards_track_packets():
    track_numbers = request.args.get('track_numbers', '')
    api_key = os.getenv('LEOPARD_API_KEY')
    api_password = os.getenv('LEOPARD_PASSWORD')
    url = (
        f"https://merchantapi.leopardscourier.com/api/trackBookedPacket/format/json/"
        f"?api_key={api_key}&api_password={api_password}&track_numbers={track_numbers}"
    )
    try:
        r = _req.get(url, verify=False, timeout=30)
        return jsonify(r.json())
    except Exception as e:
        return jsonify({"status": 0, "error": str(e)}), 500


@app.route('/api/leopards/shipper-advice')
def leopards_shipper_advice():
    api_key = os.getenv('LEOPARD_API_KEY')
    api_password = os.getenv('LEOPARD_PASSWORD')
    from_date = request.args.get('from_date') or (datetime.now() - dt.timedelta(days=30)).strftime('%m/%d/%Y')
    to_date = request.args.get('to_date') or datetime.now().strftime('%m/%d/%Y')
    origin_city = request.args.get('origin_city', '')
    destination_city = request.args.get('destination_city', '')
    cn_number = request.args.get('cn_number', '')
    product = request.args.get('product', '')
    status_filter = request.args.get('status', '')
    start = int(request.args.get('start', 0) or 0)
    length = int(request.args.get('length', 100) or 100)

    payload = {
        'api_key': api_key,
        'api_password': api_password,
        'product': product,
        'status': status_filter,
        'origionID': origin_city,
        'destinationID': destination_city,
        'dateFrom': from_date,
        'toDate': to_date,
        'Cn_number': cn_number,
        'start': start,
        'length': length,
    }
    # Keep the older keys too because Leopards has published two payload variants.
    payload['from_date'] = datetime.strptime(from_date, '%m/%d/%Y').strftime('%Y-%m-%d') if '/' in from_date else from_date
    payload['to_date'] = datetime.strptime(to_date, '%m/%d/%Y').strftime('%Y-%m-%d') if '/' in to_date else to_date
    if origin_city:
        payload['origin_city'] = origin_city
    if destination_city:
        payload['destination_city'] = destination_city

    try:
        response = _req.post(
            'https://merchantapi.leopardscourier.com/api/shipperAdviceList/format/json/',
            json=payload,
            verify=False,
            timeout=45,
        )
        response.raise_for_status()
        data = response.json()
        items = normalize_shipper_advice_items(data)
        return jsonify({
            'status': data.get('status', 1),
            'error': data.get('error', '0'),
            'count': len(items),
            'items': items,
        })
    except Exception as e:
        return jsonify({'status': 0, 'error': str(e), 'items': []}), 500


@app.route('/api/leopards/shipper-advice/update', methods=['POST'])
def leopards_update_shipper_advice():
    data = request.get_json() or {}
    api_key = os.getenv('LEOPARD_API_KEY')
    api_password = os.getenv('LEOPARD_PASSWORD')
    advice_id = data.get('id')
    cn_number = str(data.get('cn_number') or '').strip()
    advice_status = str(data.get('shipper_advice_status') or '').strip().upper()
    shipper_remarks = str(data.get('shipper_remarks') or '').strip()

    if advice_status not in {'RA', 'RT'}:
        return jsonify({'success': False, 'error': 'Allowed shipper advice statuses are RA or RT.'}), 400
    if not advice_id or not cn_number:
        return jsonify({'success': False, 'error': 'Advice id and CN number are required.'}), 400

    payload = {
        'api_key': api_key,
        'api_password': api_password,
        'data': [{
            'id': advice_id,
            'cn_number': cn_number,
            'shipper_advice_status': advice_status,
            'shipper_remarks': shipper_remarks,
        }]
    }

    try:
        response = _req.post(
            'https://merchantapi.leopardscourier.com/api/updateShipperAdvice/format/json/',
            json=payload,
            verify=False,
            timeout=45,
        )
        response.raise_for_status()
        result = response.json()
        if str(result.get('status')) != '1' or str(result.get('error', '0')) not in {'0', ''}:
            return jsonify({'success': False, 'error': result.get('error') or 'Leopards rejected the shipper advice update.'}), 400
        return jsonify({'success': True, 'message': result.get('data') or 'Shipper advice updated successfully.'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/track/<tracking_number>')
def leopards_tracking_detail(tracking_number):
    tracking_number = (tracking_number or '').strip()
    if not tracking_number:
        return render_template('tracking_detail.html', tracking_number='', packet=None, parsed=None, history=[], error='Tracking number is missing.'), 400

    api_key = os.getenv('LEOPARD_API_KEY')
    api_password = os.getenv('LEOPARD_PASSWORD')
    url = (
        f"https://merchantapi.leopardscourier.com/api/trackBookedPacket/format/json/"
        f"?api_key={api_key}&api_password={api_password}&track_numbers={tracking_number}"
    )

    try:
        r = _req.get(url, verify=False, timeout=30)
        data = r.json()
        packet = next(
            (item for item in data.get('packet_list', []) if str(item.get('track_number', '')).strip() == tracking_number),
            None
        )
        if not packet:
            return render_template(
                'tracking_detail.html',
                tracking_number=tracking_number,
                packet=None,
                parsed=None,
                history=[],
                error='No Leopards tracking record was found for this CN.'
            ), 404

        parsed = parse_leopards_status(packet, tracking_number)
        history = packet.get('Tracking Detail', []) or []
        history = list(reversed(history))
        return render_template(
            'tracking_detail.html',
            tracking_number=tracking_number,
            packet=packet,
            parsed=parsed,
            history=history,
            error=''
        )
    except Exception as e:
        return render_template(
            'tracking_detail.html',
            tracking_number=tracking_number,
            packet=None,
            parsed=None,
            history=[],
            error=f'Could not fetch Leopards tracking right now: {e}'
        ), 502


@app.route('/api/leopards/active-cns')
def leopards_active_cns():
    global order_details
    cn_numbers = []
    order_map = []
    seen = set()

    for order in order_details:
        for item in order.get('line_items', []):
            cn = item.get('tracking_number', '')
            if cn and cn != 'N/A' and cn not in seen:
                seen.add(cn)
                cn_numbers.append(cn)
                order_map.append({
                    'tracking_number':            cn,
                    'order_id':                   order.get('order_id', ''),
                    'booked_packet_order_id':      order.get('order_id', ''),
                    'product_title':               item.get('product_title', ''),
                    'products':                    item.get('product_title', ''),
                    'item_image':                  item.get('image_src', ''),
                    'booked_packet_collect_amount': order.get('total_price', 0),
                    'booked_packet_weight':        '',
                    'booked_packet_status':        item.get('status', ''),
                })

    return jsonify({"cn_numbers": cn_numbers, "order_map": order_map})


@app.route('/dispatch', methods=['GET'])
def dispatch():
    return jsonify(order_details)


@app.route('/return', methods=['GET'])
def return_orders():
    return jsonify(order_details)


# ── Shopify setup ─────────────────────────────────────────────────────────────

setup_shopify()


# ── Background refresh ────────────────────────────────────────────────────────

def background_refresh():
    """Refresh all data every 120 minutes. On first run, does full Shopify fetch."""
    global daraz_orders, order_details
    print(f"BACKGROUND REFRESH: Starting at {dt.datetime.now()}")

    # Full Shopify fetch when order_details is empty (first startup)
    if not order_details:
        print("BACKGROUND REFRESH: order_details empty — doing full Shopify fetch.")
        try:
            refreshed_orders = asyncio.run(getShopifyOrders())
            if refreshed_orders:
                order_details = refreshed_orders
                print(f"BACKGROUND REFRESH: Shopify fetch complete ({len(order_details)} orders).")
            else:
                print("BACKGROUND REFRESH: Shopify fetch returned 0 orders; leaving current order cache unchanged.")
        except Exception as e:
            print(f"BACKGROUND REFRESH ERROR (Shopify full fetch): {e}")

    try:
        daraz_statuses = ['shipped', 'pending', 'ready_to_ship', 'packed']
        daraz_orders = get_daraz_orders(daraz_statuses)
        print("BACKGROUND REFRESH: Daraz orders updated.")
    except Exception as e:
        print(f"BACKGROUND REFRESH ERROR (Daraz): {e}")

    try:
        final_states = {"RETURNED TO SHIPPER", "Delivered", "Refused by consignee"}
        active_cns = [
            item['tracking_number']
            for order in order_details
            if order.get('status') not in final_states
            for item in order.get('line_items', [])
            if item.get('tracking_number') and item['tracking_number'] != 'N/A'
        ]
        active_cns = list(dict.fromkeys(active_cns))  # deduplicate preserving order

        if active_cns:
            api_key_l = os.getenv('LEOPARD_API_KEY')
            api_pass_l = os.getenv('LEOPARD_PASSWORD')
            tracking_cache = {}

            for chunk in [active_cns[i:i+50] for i in range(0, len(active_cns), 50)]:
                joined = ','.join(chunk)
                url = (
                    f"https://merchantapi.leopardscourier.com/api/trackBookedPacket/"
                    f"?api_key={api_key_l}&api_password={api_pass_l}&track_numbers={joined}"
                )
                try:
                    r = _req.get(url, verify=False, timeout=30)
                    data = r.json()
                    if data.get('status') == 1:
                        for packet in data.get('packet_list', []):
                            cn = packet.get('track_number')
                            if cn:
                                tracking_cache[cn] = packet
                except Exception as e:
                    print(f"BACKGROUND REFRESH tracking chunk error: {e}")

            for order in order_details:
                if order.get('status') in final_states:
                    continue
                for item in order.get('line_items', []):
                    cn = item.get('tracking_number')
                    if cn and cn in tracking_cache:
                        parsed = parse_leopards_status(tracking_cache[cn], cn)
                        item['status'] = parsed['status']
                        order['status'] = parsed['status']

        print("BACKGROUND REFRESH: Leopard tracking updated.")
    except Exception as e:
        print(f"BACKGROUND REFRESH ERROR (Leopard): {e}")

    print("BACKGROUND REFRESH: Done.")


# ── Startup ───────────────────────────────────────────────────────────────────

def load_initial_data():
    global order_details, daraz_orders
    print("Loading initial data...")
    statuses = ['shipped', 'pending', 'ready_to_ship', 'packed']
    daraz_orders = get_daraz_orders(statuses)
    order_details = asyncio.run(getShopifyOrders())
    print("Initial data loaded.")


# Initialize DB at module level (fast — just creates table if not exists)
with app.app_context():
    init_db()

# Start background scheduler at module level so Gunicorn picks it up
scheduler = BackgroundScheduler(daemon=True)
scheduler.add_job(background_refresh, 'interval', minutes=120)
# Also run once 30 seconds after startup (after health check passes)
scheduler.add_job(background_refresh, 'date',
                  run_date=dt.datetime.now() + dt.timedelta(seconds=30))
scheduler.start()
print("Background scheduler started. Initial data load in 30s.")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=False)

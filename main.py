import base64
import smtplib
import sys
import threading
import time
from email.mime.text import MIMEText
from flask import render_template, request, jsonify, redirect, url_for, session, send_from_directory
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

from apscheduler.schedulers.background import BackgroundScheduler
from token_manager import get_access_token, save_tokens
from campaigns import campaigns_bp, init_campaign_dirs
from shopify_protected_data import (
    create_oauth_state,
    exchange_oauth_code_for_token,
    fetch_protected_order_details,
    get_install_url,
    get_protected_data_config_status,
    get_shop_domain,
    save_offline_token,
    verify_oauth_hmac,
)
import ssl
import certifi

from db import init_db, load_order_statuses, upsert_order_status
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

EMPLOYEE_PORTAL_PASSWORD = os.getenv('EMPLOYEE_PORTAL_PASSWORD', '@@@t')
EMPLOYEE_PORTAL_SESSION_KEY = 'employee_portal_authenticated'
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
    return {'now': dt.datetime.now()}

order_details = []
daraz_orders = []

RATE_LIMIT = 2
LAST_REQUEST_TIME = 0
product_image_cache = {}  # (product_id, variant_id) -> (image_src, variant_name)


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
    """Extract human-readable status + customer info from a Leopards packet dict."""
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
    """Returns list of tracking dicts for a line item. Falls back to billing address."""
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
                packet = tracking_cache.get(tracking_number)
                parsed = parse_leopards_status(packet, tracking_number)

                # Fall back to Shopify billing address when Leopards has no data
                tracking_info.append({
                    'tracking_number': tracking_number,
                    'status':   parsed['status'],
                    'quantity': item.quantity,
                    'name':     parsed['name']    or billing.get('name',    'N/A'),
                    'address':  parsed['address'] or billing.get('address', 'N/A'),
                    'city':     parsed['city']    or billing.get('city',    'N/A'),
                    'phone':    parsed['phone']   or billing.get('phone',   'N/A'),
                })

    return tracking_info if tracking_info else [{
        'tracking_number': 'N/A',
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

    try:
        name = order.billing_address.name
    except AttributeError:
        name = ""

    try:
        address = order.billing_address.address1
    except AttributeError:
        address = ""

    try:
        city = order.billing_address.city
    except AttributeError:
        city = ""

    try:
        phone = order.billing_address.phone
    except AttributeError:
        phone = ""

    billing = {"name": name, "address": address, "city": city, "phone": phone}
    customer_details = billing

    order_info = {
        'order_link':         "https://admin.shopify.com/store/tick-bags-best-bean-bags-in-pakistan/orders/" + str(order.id),
        'order_id':           order.name,
        'tracking_id':        'N/A',
        'created_at':         formatted_datetime,
        'total_price':        order.total_price,
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
                    await asyncio.sleep(0.6)  # stay under 2 calls/sec
                    product = shopify.Product.find(line_item.product_id)
                    if product and product.variants:
                        for variant in product.variants:
                            if variant.id == line_item.variant_id:
                                if variant.image_id is not None:
                                    await asyncio.sleep(0.6)
                                    images = shopify.Image.find(image_id=variant.image_id, product_id=line_item.product_id)
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
            order_info['line_items'].append({
                'fulfillment_status': line_item.fulfillment_status,
                'image_src':          image_src,
                'product_title':      line_item.title + (" - " + variant_name if variant_name else ""),
                'quantity':           info['quantity'],
                'tracking_number':    info['tracking_number'],
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

async def getShopifyOrders():
    start_date = datetime(2024, 9, 1).isoformat()
    result = []
    total_start = time.time()

    all_orders = []
    try:
        orders = shopify.Order.find(limit=250, order="created_at DESC", created_at_min=start_date)
    except Exception as e:
        print(f"Error fetching orders: {e}")
        return []

    while True:
        all_orders.extend(orders)
        try:
            if not orders.has_next_page():
                break
            orders = orders.next_page()
        except Exception as e:
            print(f"Error fetching next page: {e}")
            break

    print(f"Fetched {len(all_orders)} orders from Shopify.")

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

    result = enrich_orders_with_protected_customer_data(result)

    print(f"Processed {len(result)} orders in {time.time() - total_start:.2f}s")
    return result


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def tracking():
    global order_details, daraz_orders
    return render_template("track.html", order_details=order_details, darazOrders=daraz_orders)


@app.route('/refresh', methods=['POST'])
def refresh_data():
    global order_details
    try:
        order_details = asyncio.run(getShopifyOrders())
        return jsonify({'message': 'Data refreshed successfully'})
    except Exception as e:
        print(f"Error refreshing data: {e}")
        return jsonify({'message': 'Failed to refresh data'}), 500


@app.route('/shopify/protected-data/status')
def shopify_protected_data_status():
    return jsonify(get_protected_data_config_status())


@app.route('/shopify/install')
def shopify_install():
    state = create_oauth_state()
    session[SHOPIFY_OAUTH_STATE_SESSION_KEY] = state
    return redirect(get_install_url(state))


@app.route('/shopify/callback')
def shopify_callback():
    params = {key: value for key, value in request.args.items()}
    if not verify_oauth_hmac(params):
        return jsonify({"success": False, "error": "Invalid Shopify callback signature"}), 400

    expected_state = session.get(SHOPIFY_OAUTH_STATE_SESSION_KEY)
    provided_state = request.args.get('state', '')
    if not expected_state or expected_state != provided_state:
        return jsonify({"success": False, "error": "Invalid Shopify OAuth state"}), 400

    shop = (request.args.get('shop') or '').strip().lower()
    code = (request.args.get('code') or '').strip()
    if not shop or shop != get_shop_domain():
        return jsonify({"success": False, "error": "OAuth callback shop does not match configured shop"}), 400
    if not code:
        return jsonify({"success": False, "error": "Missing Shopify OAuth code"}), 400

    try:
        payload = exchange_oauth_code_for_token(shop, code)
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
                if parsed['name']:
                    item['name'] = parsed['name']
                if parsed['address']:
                    item['address'] = parsed['address']
                if parsed['city']:
                    item['city'] = parsed['city']
                if parsed['phone']:
                    item['phone'] = parsed['phone']
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


def employee_portal_is_authenticated():
    return bool(session.get(EMPLOYEE_PORTAL_SESSION_KEY))


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
                'items_list': items_with_status,
                'total_price': daraz_order['total_price']
            })

    for shopify_order in order_details:
        if any(tag.startswith("Dispatched") for tag in shopify_order.get('tags', [])):
            continue
        if shopify_order['status'] in ['Booked', 'Un-Booked', 'Drop Off at Express Center']:
            filtered_tags = [tag.strip() for tag in shopify_order.get('tags', []) if tag and tag.strip() != 'Leopards Courier']
            shopify_items = []
            for item in shopify_order['line_items']:
                track_num = item.get('tracking_number', 'N/A')
                key = f"{shopify_order['order_id']}:{track_num}"
                shopify_items.append({
                    'item_image':     item['image_src'],
                    'item_title':     item['product_title'],
                    'quantity':       item['quantity'],
                    'tracking_number': track_num,
                    'status':         item['status'],
                    'applied_status': statuses.get(key, "")
                })
            all_orders.append({
                'order_via':  'Shopify',
                'order_id':   shopify_order['order_id'],
                'status':     shopify_order['status'],
                'tags':       filtered_tags,
                'is_lahore':  any('lahore' in tag.lower() for tag in filtered_tags),
                'date':       shopify_order['created_at'],
                'items_list': shopify_items,
                'total_price': shopify_order['total_price']
            })

    return all_orders


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


def apply_shopify_order_tag(order_id, tag):
    today_date = datetime.now().strftime('%Y-%m-%d')
    tag_with_date = f"{tag.strip()} ({today_date})"

    order = shopify.Order.find(order_id)
    tags = [t.strip() for t in order.tags.split(",")] if order.tags else []
    if "Leopards Courier" in tags:
        tags.remove("Leopards Courier")
    if tag_with_date not in tags:
        tags.append(tag_with_date)
    order.tags = ", ".join(tags)
    return order.save()


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

    return jsonify({"message": f"Status updated to {status} for {order_id} ({tracking_number})"})


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
            if apply_shopify_order_tag(shopify_id, tag_name):
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

def verify_shopify_webhook(req):
    shopify_hmac = req.headers.get('X-Shopify-Hmac-Sha256')
    data = req.get_data()
    secret = os.getenv('SHOPIFY_WEBHOOK_SECRET')

    if secret is None:
        raise ValueError("SHOPIFY_WEBHOOK_SECRET is not set.")

    digest = hmac.new(secret.encode('utf-8'), data, hashlib.sha256).digest()
    computed_hmac = base64.b64encode(digest).decode('utf-8')
    return hmac.compare_digest(computed_hmac, shopify_hmac)


@app.route('/shopify/webhook/order_updated', methods=['POST'])
def shopify_order_updated():
    global order_details
    try:
        if not verify_shopify_webhook(request):
            return jsonify({'error': 'Invalid webhook signature'}), 401

        order_data = request.get_json()
        order_id = order_data.get('id')
        if not order_id:
            return jsonify({'error': 'No order id found in payload'}), 400

        if order_data.get('closed_at'):
            order_details[:] = [o for o in order_details if o.get('id') != order_id]
            return jsonify({'success': True, 'message': f'Order {order_id} removed.'}), 200

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

        updated = False
        for idx, existing_order in enumerate(order_details):
            if existing_order.get('id') == updated_order_info.get('id'):
                order_details[idx] = updated_order_info
                updated = True
                break
        if not updated:
            order_details.append(updated_order_info)

        return jsonify({'success': True, 'message': f'Order {order_id} processed successfully'}), 200
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


@app.route('/payments')
def payments_page():
    return render_template('payments.html')


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

shop_url = os.getenv('SHOP_URL')
api_key  = os.getenv('API_KEY')
password = os.getenv('PASSWORD')
shopify.ShopifyResource.set_site(shop_url)
shopify.ShopifyResource.set_user(api_key)
shopify.ShopifyResource.set_password(password)


# ── Background refresh ────────────────────────────────────────────────────────

def background_refresh():
    """Refresh all data every 120 minutes. On first run, does full Shopify fetch."""
    global daraz_orders, order_details
    print(f"BACKGROUND REFRESH: Starting at {dt.datetime.now()}")

    # Full Shopify fetch when order_details is empty (first startup)
    if not order_details:
        print("BACKGROUND REFRESH: order_details empty — doing full Shopify fetch.")
        try:
            order_details = asyncio.run(getShopifyOrders())
            print(f"BACKGROUND REFRESH: Shopify fetch complete ({len(order_details)} orders).")
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
                        if parsed['name']:   item['name'] = parsed['name']
                        if parsed['city']:   item['city'] = parsed['city']
                        if parsed['phone']:  item['phone'] = parsed['phone']
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

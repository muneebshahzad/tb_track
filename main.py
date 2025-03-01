import smtplib
import time
from email.mime.text import MIMEText
from flask import render_template
import datetime
import lazop
import os
import hmac
import hashlib
import base64
import asyncio
import aiohttp
from flask import Flask
import shopify

app = Flask(__name__)
app.debug = True
app.secret_key = os.getenv('APP_SECRET_KEY', 'default_secret_key')  # Use environment variable
pre_loaded = 0
order_details = []
daraz_orders = []


@app.route('/send-email', methods=['POST'])
def send_email():
    data = request.get_json()
    to_emails = data.get('to', [])
    cc_emails = data.get('cc', [])
    subject = data.get('subject', '')
    body = data.get('body', '')

    try:
        # SMTP server configuration
        smtp_server = 'smtp.gmail.com'
        smtp_port = 587
        smtp_user = os.getenv('SMTP_USER')  # Use environment variable
        smtp_password = os.getenv('SMTP_PASSWORD')  # Use environment variable

        # Create the message
        msg = MIMEText(body)
        msg['From'] = smtp_user
        msg['To'] = ', '.join(to_emails)
        msg['Cc'] = ', '.join(cc_emails)
        msg['Subject'] = subject

        # Connect to the SMTP server and send email
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.sendmail(smtp_user, to_emails + cc_emails, msg.as_string())
        server.quit()

        return jsonify({'message': 'Email sent successfully'}), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500


def format_date(date_str):
    # Parse the date string
    date_obj = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S %z")
    # Format the date object to only show the date
    return date_obj.strftime("%Y-%m-%d")


async def fetch_tracking_data(session, tracking_number):
    api_key = os.getenv('LEOPARD_API_KEY')
    api_password = os.getenv('LEOPARD_PASSWORD')
    url = f"https://merchantapi.leopardscourier.com/api/trackBookedPacket/?api_key={api_key}&api_password={api_password}&track_numbers={tracking_number}"
    async with session.get(url) as response:
        return await response.json()


from flask import Flask, request, jsonify
import requests
import os

app = Flask(__name__)


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
        response_data = response.json()
        print("Loadsheet Response:", response_data)  # ✅ Debugging: Print response
        return jsonify(response_data)

    except requests.exceptions.RequestException as e:
        print("Error:", e)  # Log the error
        return jsonify({"error": "Failed to connect to the API"}), 500


async def process_line_item(session, line_item, fulfillments):
    if line_item.fulfillment_status is None and line_item.fulfillable_quantity == 0:
        return []

    tracking_info = []
    name = 'N/A'
    address = 'N/A'
    phone = 'N/A'
    city = 'N/A'
    if line_item.fulfillment_status == "fulfilled":
        for fulfillment in fulfillments:
            if fulfillment.status == "cancelled":
                continue
            for item in fulfillment.line_items:
                if item.id == line_item.id:
                    tracking_number = fulfillment.tracking_number
                    data = await fetch_tracking_data(session, tracking_number)
                    if data['status'] == 1 and not data['error']:
                        packet_list = data['packet_list']
                        if packet_list:
                            name = packet_list[0]['consignment_name_eng']
                            address = packet_list[0]['consignment_address']
                            phone = packet_list[0]['consignment_phone']
                            city = packet_list[0]['destination_city_name']
                            tracking_details = packet_list[0].get('Tracking Detail', [])
                            if tracking_details:
                                final_status = (packet_list[0]['Tracking Detail'][-1]['Status'] if packet_list[0].get(
                                    'Tracking Detail') else packet_list[0].get('booked_packet_status', 'Unknown'))

                                keywords = ["Return", "hold", "UNTRACEABLE"]
                                if not any(
                                        kw.lower() in final_status.lower() for kw in
                                        ["delivered", "returned to shipper"]):
                                    for detail in tracking_details:
                                        status = detail['Status']
                                        if status == 'Pending':
                                            reason = detail['Reason']
                                        else:
                                            reason = 'N/A'
                                        if any(kw in status for kw in keywords) or any(kw in reason for kw in keywords):
                                            final_status = "Being Return"
                                            break
                            else:
                                final_status = packet_list[0].get('booked_packet_status', 'Booked')
                                final_status = "Booked" if "Pickup Request Sent" in final_status or "Pickup Request not Send" in final_status else final_status

                                print("No tracking details available.")
                        else:
                            final_status = "Booked"
                            print("No packets found.")
                    else:
                        final_status = "N/A"
                        print("Error fetching data.")

                    # Track quantity for each tracking number
                    tracking_info.append({
                        'tracking_number': tracking_number,
                        'status': final_status,
                        'quantity': item.quantity,
                        'name': name,
                        'address': address,
                        'city': city,
                        "phone": phone,
                    })

    return tracking_info if tracking_info else [
        {"tracking_number": "N/A", "status": "Un-Booked", name: 'N/A', address: 'N/A', phone: 'N/A', city: 'N/A',
         "quantity": line_item.quantity}]


async def process_order(session, order):
    order_start_time = time.time()
    input_datetime_str = order.created_at
    parsed_datetime = datetime.fromisoformat(input_datetime_str[:-6])
    formatted_datetime = parsed_datetime.strftime("%b %d, %Y")

    try:
        status = (order.fulfillment_status).title()
    except:
        status = "Un-fulfilled"
    print(order)
    tags = []
    try:
        name = order.billing_address.name
    except AttributeError:
        name = " "
        print("Error retrieving name")

    try:
        address = order.billing_address.address1
    except AttributeError:
        address = " "
        print("Error retrieving address")

    try:
        city = order.billing_address.city
    except AttributeError:
        city = " "
        print("Error retrieving city")

    try:
        phone = order.billing_address.phone
    except AttributeError:
        phone = " "
        print("Error retrieving phone")

    customer_details = {
        "name": name,
        "address": address,
        "city": city,
        "phone": phone
    }
    order_info = {
        'order_link': "https://admin.shopify.com/store/tick-bags-best-bean-bags-in-pakistan/orders/" + str(order.id),
        'order_id': order.name,
        'tracking_id': 'N/A',
        'created_at': formatted_datetime,
        'total_price': order.total_price,
        'line_items': [],
        'financial_status': (order.financial_status).title(),
        'fulfillment_status': status,
        'customer_details': customer_details,
        'tags': order.tags.split(", "),
        'id': order.id
    }
    print(order.tags)

    tasks = []
    for line_item in order.line_items:
        tasks.append(process_line_item(session, line_item, order.fulfillments))

    results = await asyncio.gather(*tasks)
    variant_name = ""
    for tracking_info_list, line_item in zip(results, order.line_items):
        if tracking_info_list is None:
            continue

        if line_item.product_id is not None:
            product = shopify.Product.find(line_item.product_id)
            if product and product.variants:
                for variant in product.variants:
                    if variant.id == line_item.variant_id:
                        if variant.image_id is not None:
                            images = shopify.Image.find(image_id=variant.image_id, product_id=line_item.product_id)
                            variant_name = line_item.variant_title
                            for image in images:
                                if image.id == variant.image_id:
                                    image_src = image.src
                        else:
                            variant_name = ""
                            image_src = product.image.src
        else:
            image_src = "https://static.thenounproject.com/png/1578832-200.png"

        for info in tracking_info_list:
            order_info['line_items'].append({
                'fulfillment_status': line_item.fulfillment_status,
                'image_src': image_src,
                'product_title': line_item.title + " - " + variant_name,
                'quantity': info['quantity'],
                'tracking_number': info['tracking_number'],
                'status': info['status'],
                'name': info.get('name', 'N/A'),
                'address': info.get('address', 'N/A'),
                'city': info.get('city', 'N/A'),
                'phone': info.get('phone', 'N/A'),
            })
            order_info['status'] = info['status']

    order_end_time = time.time()
    print(f"Time taken to process order {order.order_number}: {order_end_time - order_start_time:.2f} seconds")

    return order_info


@app.route('/apply_tag', methods=['POST'])
def apply_tag():
    data = request.json
    order_id = data.get('order_id')
    tag = data.get('tag')

    # Get today's date in YYYY-MM-DD format
    today_date = datetime.now().strftime('%Y-%m-%d')
    tag_with_date = f"{tag.strip()} ({today_date})"

    try:
        # Fetch the order
        order = shopify.Order.find(order_id)

        # If the tag is "Returned", cancel the order
        if tag.strip().lower() == "returned":
            # Attempt to cancel the order
            if order.cancel():
                print("Order Cancelled")
            else:
                print("Order Cancellation Failed")
        if tag.strip().lower() == "delivered":
            if order.close():
                print("Order Cloed")
            else:
                print("Order Closing Failed")

        # Process existing tags
        if order.tags:
            tags = [t.strip() for t in order.tags.split(", ")]  # Remove excess spaces
        else:
            tags = []

        # Remove a specific tag if needed (e.g., "Leopards Courier")
        if "Leopards Courier" in tags:
            tags.remove("Leopards Courier")

        # Add new tag if it doesn't already exist
        if tag_with_date not in tags:
            tags.append(tag_with_date)

        # Update the order with the new tags
        order.tags = ", ".join(tags)

        # Save the order
        if order.save():
            return jsonify({"success": True, "message": "Tag applied successfully."})
        else:
            return jsonify({"success": False, "error": "Failed to save order changes."})

    except Exception as e:
        print(f"Error: {e}")
        return jsonify({"success": False, "error": str(e)})


async def getShopifyOrders():
    global order_details
    orders = shopify.Order.find(limit=250, order='created_at DESC')
    order_details = []
    total_start_time = time.time()

    async with aiohttp.ClientSession() as session:
        tasks = [process_order(session, order) for order in orders]
        order_details = await asyncio.gather(*tasks)

    total_end_time = time.time()
    print(f"Total time taken to process all orders: {total_end_time - total_start_time:.2f} seconds")

    return order_details


@app.route("/")
def tracking():
    global order_details, pre_loaded, daraz_orders
    return render_template("track.html", order_details=order_details, darazOrders=daraz_orders)


def get_daraz_orders(statuses):
    try:
        access_token = '50000902021Yejq5rSzhGiOp0hRUGDxjt12fb1602qewR3toNX4APiKiju3bxu'
        client = lazop.LazopClient('https://api.daraz.pk/rest', '501554', 'nrP3XFN7ChZL53cXyVED1yj4iGZZtlcD')

        all_orders = []

        for status in statuses:
            request = lazop.LazopRequest('/orders/get', 'GET')
            request.add_api_param('sort_direction', 'DESC')
            request.add_api_param('update_before', '2025-02-10T16:00:00+08:00')
            request.add_api_param('offset', '0')
            request.add_api_param('created_before', '2025-02-10T16:00:00+08:00')
            request.add_api_param('created_after', '2017-02-10T09:00:00+08:00')
            request.add_api_param('limit', '50')
            request.add_api_param('update_after', '2017-02-10T09:00:00+08:00')
            request.add_api_param('sort_by', 'updated_at')
            request.add_api_param('status', status)
            request.add_api_param('access_token', access_token)

            response = client.execute(request)
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

                    item_detail = {
                        'item_image': item.get('product_main_image', 'N/A'),
                        'item_title': product_title,
                        'quantity': 1,
                        'tracking_number': item.get('tracking_code', 'N/A'),
                        'status': track_status
                    }
                    item_details.append(item_detail)

                filtered_order = {
                    'order_id': f"{order.get('order_id', 'Unknown')}",
                    'customer': {
                        'name': f"{order.get('customer_first_name', 'Unknown')} {order.get('customer_last_name', 'Unknown')}",
                        'address': order.get('address_shipping', {}).get('address', 'N/A'),
                        'phone': order.get('address_shipping', {}).get('phone', 'N/A')
                    },
                    'status': status.replace('_', ' ').title(),
                    'date': format_date(order.get('created_at', 'N/A')),
                    'total_price': order.get('price', '0.00'),
                    'items_list': item_details,
                    'tracking_id': 'N/A',
                }
                all_orders.append(filtered_order)

        return all_orders
    except Exception as e:
        print(f"Error fetching darazOrders: {e}")
        return []


@app.route('/daraz')
def daraz():
    statuses = ['shipped', 'pending', 'ready_to_ship']
    darazOrders = get_daraz_orders(statuses)
    return render_template('daraz.html', darazOrders=darazOrders)


@app.route('/refresh', methods=['POST'])
def refresh_data():
    global order_details
    try:
        order_details = asyncio.run(getShopifyOrders())
        return render_template("track.html", order_details=order_details)
    except Exception as e:
        print(f"Error refreshing data: {e}")
        return jsonify({'message': 'Failed to refresh data'}), 500


def run_async(func, *args, **kwargs):
    return asyncio.run(func(*args, **kwargs))


@app.route('/track/<tracking_num>')
def displayTracking(tracking_num):
    print(f"Tracking Number: {tracking_num}")  # Debug line

    async def async_func():
        async with aiohttp.ClientSession() as session:
            return await fetch_tracking_data(session, tracking_num)

    data = run_async(async_func)

    return render_template('trackingdata.html', data=data)


from flask import request, jsonify
from datetime import datetime


async def fetch_order_details():
    # Run the function in the background
    global order_details
    order_details = await getShopifyOrders()


@app.route('/pending')
def pending_orders():
    all_orders = []
    pending_items_dict = {}  # Dictionary to track quantities of each unique item

    global daraz_orders, order_details

    # Process Daraz orders with the specified statuses
    for daraz_order in daraz_orders:
        if daraz_order['status'] in ['Ready To Ship', 'Pending']:
            daraz_order_data = {
                'order_via': 'Daraz',
                'order_id': daraz_order['order_id'],
                'status': daraz_order['status'],
                'tracking_number': daraz_order['items_list'][0]['tracking_number'],
                'date': daraz_order['date'],
                'items_list': daraz_order['items_list']
            }
            all_orders.append(daraz_order_data)

            for item in daraz_order['items_list']:
                product_title = item['item_title']
                quantity = item['quantity']
                item_image = item['item_image']

                if product_title in pending_items_dict:
                    pending_items_dict[product_title]['quantity'] += quantity
                else:
                    pending_items_dict[product_title] = {
                        'item_image': item_image,
                        'item_title': product_title,
                        'quantity': quantity
                    }

    # Process Shopify orders with the specified statuses
    for shopify_order in order_details:
        # Skip orders with tags starting with "Dispatched"
        if any(tag.startswith("Dispatched") for tag in shopify_order.get('tags', [])):
            continue

        if shopify_order['status'] in ['Booked', 'Un-Booked']:
            shopify_items_list = [
                {
                    'item_image': item['image_src'],
                    'item_title': item['product_title'],
                    'quantity': item['quantity'],
                    'tracking_number': item['tracking_number'],
                    'status': item['status']
                }
                for item in shopify_order['line_items']
            ]

            shopify_order_data = {
                'order_via': 'Shopify',
                'order_id': shopify_order['order_id'],
                'status': shopify_order['status'],
                'tracking_number': shopify_order['tracking_id'],
                'date': shopify_order['created_at'],
                'items_list': shopify_items_list
            }
            all_orders.append(shopify_order_data)

            # Count quantities for each item in the Shopify order
            for item in shopify_items_list:
                product_title = item['item_title']
                quantity = item['quantity']
                item_image = item['item_image']

                if product_title in pending_items_dict:
                    pending_items_dict[product_title]['quantity'] += quantity
                else:
                    pending_items_dict[product_title] = {
                        'item_image': item_image,
                        'item_title': product_title,
                        'quantity': quantity
                    }

    pending_items = list(pending_items_dict.values())
    pending_items_sorted = sorted(pending_items, key=lambda x: x['quantity'], reverse=True)

    # Split the list into two halves
    half = len(pending_items_sorted) // 2

    return render_template('pending.html', all_orders=all_orders, pending_items=pending_items_sorted, half=half)


@app.route('/undelivered')
def undelivered():
    global order_details, pre_loaded, daraz_orders
    return render_template("undelivered.html", order_details=order_details, darazOrders=daraz_orders)


def verify_shopify_webhook(request):
    """
    Verify the webhook using HMAC with the shared secret.
    """
    shopify_hmac = request.headers.get('X-Shopify-Hmac-Sha256')
    data = request.get_data()  # Raw request body (bytes)

    # Retrieve the secret from environment variables
    secret = os.getenv('SHOPIFY_WEBHOOK_SECRET')

    # Check that the secret is set. If not, raise an error.
    if secret is None:
        raise ValueError("SHOPIFY_WEBHOOK_SECRET is not set. Please configure the environment variable.")

    # Compute the HMAC digest and base64 encode it.
    digest = hmac.new(
        secret.encode('utf-8'),
        data,
        hashlib.sha256
    ).digest()
    computed_hmac = base64.b64encode(digest).decode('utf-8')

    # Compare the computed HMAC with the one sent by Shopify.
    return hmac.compare_digest(computed_hmac, shopify_hmac)


@app.route('/shopify/webhook/order_updated', methods=['POST'])
def shopify_order_updated():
    global order_details  # Ensure we're modifying the global variable
    try:
        # Verify the webhook request is from Shopify
        if not verify_shopify_webhook(request):
            return jsonify({'error': 'Invalid webhook signature'}), 401

        # Parse the JSON payload sent by Shopify
        order_data = request.get_json()
        order_id = order_data.get('id')
        if not order_id:
            return jsonify({'error': 'No order id found in payload'}), 400

        print(f"Received webhook for order ID: {order_id}")

        # Fetch the complete order from Shopify
        order = shopify.Order.find(order_id)
        if not order:
            return jsonify({'error': f'Order {order_id} not found'}), 404

        # Process the order update asynchronously.
        async def update_order():
            async with aiohttp.ClientSession() as session:
                updated_order_info = await process_order(session, order)
                return updated_order_info

        updated_order_info = asyncio.run(update_order())

        # Update the global order_details list with the new info.
        # Assuming each order has a unique 'id' field:
        updated = False
        for idx, existing_order in enumerate(order_details):
            if existing_order.get('id') == updated_order_info.get('id'):
                order_details[idx] = updated_order_info
                updated = True
                break
        # If the order wasn't in the list, you might want to add it:
        if not updated:
            order_details.append(updated_order_info)

        # Optionally, log the updated global orders
        print("Updated order_details:", order_details)

        return jsonify({
            'success': True,
            'message': f'Order {order_id} processed successfully',
            'order': updated_order_info
        }), 200

    except Exception as e:
        print(f"Webhook processing error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


from flask import request, render_template, redirect, url_for


@app.route('/scan', methods=['GET', 'POST'])
def search():
    search_term = (request.args.get('term') or request.form.get('search_term') or "").split(',')[0].strip()
    if not search_term:
        return render_template('scan.html')

    order_found = None

    for order in order_details:
        if order.get('order_id') == search_term or order.get('tracking_id') == search_term:
            order_found = order
            break
        for item in order.get('line_items', []):
            if item.get('tracking_number') == search_term:
                order_found = order
                break
        if order_found:
            break

    if request.method == 'POST':
        return render_template('scan.html', search_term=search_term, order_found=order_found)

    return jsonify(order_found if order_found else {"error": "Order not found"}), 200 if order_found else 404


@app.route('/dispatch', methods=['GET'])
def dispatch():
    # Fetch orders for dispatch
    dispatch_orders = []
    for order in order_details:
        # Add filtering logic if necessary
        dispatch_orders.append(order)
    return jsonify(dispatch_orders)


@app.route('/return', methods=['GET'])
def return_orders():
    # Fetch orders for return
    return_orders = []
    for order in order_details:
        # Add filtering logic if necessary
        return_orders.append(order)
    return jsonify(return_orders)


shop_url = os.getenv('SHOP_URL')
api_key = os.getenv('API_KEY')
password = os.getenv('PASSWORD')
shopify.ShopifyResource.set_site(shop_url)
shopify.ShopifyResource.set_user(api_key)
shopify.ShopifyResource.set_password(password)
statuses = ['shipped', 'pending', 'ready_to_ship']
daraz_orders = get_daraz_orders(statuses)

order_details = asyncio.run(getShopifyOrders())

if __name__ == "__main__":
    shop_url = os.getenv('SHOP_URL')
    api_key = os.getenv('API_KEY')
    password = os.getenv('PASSWORD')
    app.run(host="0.0.0.0", port=5001)

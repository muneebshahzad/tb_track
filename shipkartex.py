"""Read-only Shipkartex tracking and exact account shipment identification."""
import logging
import os
import re
import threading
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal, ROUND_HALF_UP

import requests

BASE_URL = 'https://portal.shipkartex.com/API/'
log = logging.getLogger(__name__)
FINAL = {'delivered', 'returned to shipper', 'cancelled', 'canceled'}


def value(obj, key, default=''):
    return obj.get(key, default) if isinstance(obj, dict) else getattr(obj, key, default)


def explicit_shipkartex(item):
    companies = {re.sub(r'[^a-z]', '', str(value(item, key)).lower())
                 for key in ('tracking_company', 'courier')}
    urls = [value(item, 'tracking_url')] + list(value(item, 'tracking_urls') or [])
    from urllib.parse import urlparse
    return bool(companies & {'shipkarte', 'shipkartex', 'shipkartexcourier', 'shipkartecourier'}) or any(
        (urlparse(str(url)).hostname or '').lower() in {'shipkartex.com', 'www.shipkartex.com', 'portal.shipkartex.com'}
        for url in urls if url
    )


def rows(payload):
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        for key in ('data', 'orders', 'order_list'):
            if isinstance(payload.get(key), list):
                return rows(payload[key])
    raise ValueError('Unexpected Shipkartex response')


class ShipkartexClient:
    def __init__(self):
        self._index = {}
        self._expires = 0
        self._lock = threading.Lock()

    def post(self, endpoint, payload):
        response = requests.post(BASE_URL + endpoint, json=payload, timeout=(5, 20))
        response.raise_for_status()
        return rows(response.json())

    def index(self):
        auth = os.getenv('SHIPKARTEX_AUTH_KEY', '').strip()
        if not auth:
            return {}
        with self._lock:
            if time.monotonic() < self._expires:
                return self._index
            try:
                records = self.post('GetOrderList.php', {
                    'auth_key': auth, 'client_code': os.getenv('SHIPKARTEX_CLIENT_CODE', '')})
                index = {}
                for row in records:
                    cn = str(row.get('tracking_no') or '').strip()
                    if not cn:
                        continue
                    for alias in (cn, row.get('thirdparty_tracking_no')):
                        if alias:
                            index[str(alias).strip()] = cn
                self._index = index
                self._expires = time.monotonic() + 300
            except (requests.RequestException, ValueError):
                # Never log payloads, credentials, or customer data.
                log.warning('Shipkartex account lookup unavailable; keeping previous identification data')
                self._expires = time.monotonic() + 60
            return self._index

    def payments(self, from_date='', to_date=''):
        """Return normalized COD/payment rows from Shipkartex's order ledger."""
        auth = os.getenv('SHIPKARTEX_AUTH_KEY', '').strip()
        if not auth:
            raise ValueError('Shipkartex credentials are not configured')
        records = self.post('GetOrderList.php', {
            'auth_key': auth,
            'client_code': os.getenv('SHIPKARTEX_CLIENT_CODE', '').strip(),
        })

        def parse_date(raw):
            try:
                return datetime.strptime(str(raw or '')[:10], '%Y-%m-%d').date()
            except ValueError:
                return None

        start = parse_date(from_date)
        end = parse_date(to_date)
        normalized = []
        for row in records:
            order_date = parse_date(row.get('order_date'))
            if start and order_date and order_date < start:
                continue
            if end and order_date and order_date > end:
                continue
            cod = float(row.get('collection_amount') or 0)
            base_charges = Decimal(str(row.get('delivery_charges') or 0))
            charges = float((base_charges * Decimal('1.1') * Decimal('1.16')).quantize(
                Decimal('1'), rounding=ROUND_HALF_UP
            ))
            payment_status = str(row.get('payment_status') or '').strip()
            shipment_status = str(row.get('status') or '').strip()
            cod_paid = payment_status.lower() == 'paid'
            receivable = cod - charges if shipment_status.lower().startswith('delivered') and not cod_paid else 0
            normalized.append({
                'order_id': str(row.get('order_id') or '—'),
                'cn': str(row.get('tracking_no') or '—'),
                'products': str(row.get('product_descriptiption') or '—'),
                'cod': cod,
                'status': shipment_status or '—',
                'ship_charges': charges,
                'ship_paid': None,
                'cod_paid': cod_paid,
                'payment_status': payment_status or 'Unknown',
                'inv_date': '—',
                'net': receivable,
                'order_date': str(row.get('order_date') or ''),
                'status_date': str(row.get('status_date') or ''),
            })
        return normalized

    def identify(self, item, index=None):
        cn = str(value(item, 'tracking_number') or '').strip()
        if not cn or cn == 'N/A':
            return None
        index = self.index() if index is None else index
        if cn in index:
            return index[cn]
        if explicit_shipkartex(item):
            return str(value(item, 'shipkartex_tracking_number') or cn)
        return None

    def history(self, cn):
        events = self.post('TrackOrder.php', {'tracking_no': cn})
        return sorted((event for event in events
                       if str(event.get('tracking_no', '')) == str(cn)
                       and isinstance(event.get('status'), str) and event['status'].strip()),
                      key=lambda event: str(event.get('created') or ''))

    def refresh(self, orders):
        index = self.index()
        shipments = {}
        for order in orders:
            for item in order.get('line_items', []):
                cn = self.identify(item, index)
                if cn:
                    item['courier'] = 'Shipkartex'
                    item['shipkartex_tracking_number'] = cn
                    if str(item.get('status', '')).lower() not in FINAL:
                        shipments.setdefault(cn, []).append((order, item))

        def fetch(cn):
            try:
                return cn, self.history(cn)
            except (requests.RequestException, ValueError):
                log.warning('Shipkartex tracking unavailable; preserving previous shipment status')
                return cn, []

        updated = 0
        with ThreadPoolExecutor(max_workers=4) as pool:
            for cn, events in pool.map(fetch, shipments):
                if not events:
                    continue
                status = events[-1]['status'].strip()
                # Retain the dashboard's existing terminal-status spelling.
                if status.lower() == 'returned to shipper':
                    status = 'RETURNED TO SHIPPER'
                for order, item in shipments[cn]:
                    item['status'] = status
                    order['status'] = status
                    updated += 1
        return updated


shipkartex = ShipkartexClient()

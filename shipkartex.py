"""Read-only Shipkartex tracking and exact account shipment identification."""
import logging
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor

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

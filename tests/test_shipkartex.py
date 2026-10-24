import ast
import os
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import requests
from shipkartex import ShipkartexClient, explicit_shipkartex, rows


class ShipkartexTests(unittest.TestCase):
    def setUp(self):
        self.client = ShipkartexClient()

    def test_metadata_and_exact_host(self):
        for company in ('Shipkartex', 'Shipkarte', 'Ship Kartex Courier'):
            self.assertTrue(explicit_shipkartex({'tracking_company': company}))
        self.assertTrue(explicit_shipkartex({'tracking_url': 'https://portal.shipkartex.com/track?cn=1'}))
        self.assertFalse(explicit_shipkartex({'tracking_url': 'https://shipkartex.com.evil.test/'}))
        self.assertFalse(explicit_shipkartex({'tracking_company': 'Leopards'}))

    def test_account_identification_and_alias(self):
        with patch.dict(os.environ, {'SHIPKARTEX_AUTH_KEY': 'test'}), patch.object(self.client, 'post', return_value=[{'tracking_no': 123, 'thirdparty_tracking_no': '456'}]) as post:
            self.assertEqual(self.client.identify({'tracking_number': '456'}), '123')
            self.assertEqual(self.client.identify({'tracking_number': '123'}), '123')
            self.assertIsNone(self.client.identify({'tracking_number': '1234'}))
            self.assertEqual(post.call_count, 1)

    def test_history_filters_and_sorts(self):
        with patch.object(self.client, 'post', return_value=[
            {'tracking_no': '1', 'status': 'Delivered', 'created': '2026-10-05 12:00:00'},
            {'tracking_no': '2', 'status': 'Cancelled', 'created': '2026-10-06'},
            {'tracking_no': '1', 'status': 'New Booked', 'created': '2026-10-01 12:00:00'},
        ]):
            self.assertEqual([e['status'] for e in self.client.history('1')], ['New Booked', 'Delivered'])

    def test_failure_preserves_status(self):
        item = {'tracking_number': '1', 'courier': 'Shipkartex', 'status': 'Out for Delivery'}
        order = {'status': 'Out for Delivery', 'line_items': [item]}
        with patch.object(self.client, 'index', return_value={}), patch.object(self.client, 'history', side_effect=requests.Timeout):
            self.assertEqual(self.client.refresh([order]), 0)
        self.assertEqual(item['status'], 'Out for Delivery')
        self.assertEqual(order['status'], 'Out for Delivery')

    def test_mixed_order_and_deduplication(self):
        a = {'tracking_number': '1', 'courier': 'Shipkartex', 'status': 'Booked'}
        b = dict(a)
        leopard = {'tracking_number': '2', 'courier': 'Leopards', 'status': 'Booked'}
        order = {'status': 'Delivered', 'line_items': [a, b, leopard]}
        with patch.object(self.client, 'index', return_value={}), patch.object(self.client, 'history', return_value=[{'status': 'Out for Delivery'}]) as history:
            self.assertEqual(self.client.refresh([order]), 2)
            history.assert_called_once_with('1')
        self.assertEqual(leopard['status'], 'Booked')
        self.assertEqual(a['status'], 'Out for Delivery')

    def test_terminal_items_are_skipped(self):
        item = {'tracking_number': '1', 'courier': 'Shipkartex', 'status': 'RETURNED TO SHIPPER'}
        with patch.object(self.client, 'index', return_value={}), patch.object(self.client, 'history') as history:
            self.client.refresh([{'line_items': [item]}])
            history.assert_not_called()

    def test_bad_payload_rejected(self):
        for payload in ({'error': 'Invalid key'}, None, 'html'):
            with self.assertRaises(ValueError):
                rows(payload)

    def test_null_leopards_packet_list_returns_not_found(self):
        tree = ast.parse(Path('main.py').read_text())
        function = next(node for node in tree.body if isinstance(node, ast.FunctionDef)
                        and node.name == 'leopards_tracking_detail')
        function.decorator_list = []
        namespace = {'shipkartex': self.client, 'order_details': [], 'os': os,
                     '_req': requests, 'requests': requests,
                     'render_template': lambda template, **kwargs: kwargs}
        exec(compile(ast.Module(body=[function], type_ignores=[]), 'main.py', 'exec'), namespace)
        with patch.object(self.client, 'index', return_value={}), patch('requests.get') as get:
            get.return_value.json.return_value = {'packet_list': None}
            response, status = namespace['leopards_tracking_detail']('LE7545162317')
        self.assertEqual(status, 404)
        self.assertEqual(response['error'], 'No Leopards tracking record was found for this CN.')

    def test_dashboard_bulk_controls_cover_status_variants(self):
        template = Path('templates/track.html').read_text()
        self.assertIn('id="selectAllDisplayedBtn"', template)
        self.assertIn('function toggleSelectAllDisplayed()', template)
        self.assertIn('id="bulkMarkDeliveredBtn"', template)
        self.assertIn("'return to sender'", template)
        self.assertIn("'returned to shipper'", template)
        self.assertIn('ORDER_DATA.filter(o => isReturnedStatus(o.status))', template)
        self.assertIn('ORDER_DATA.filter(o => isOutForDeliveryStatus(o.status))', template)

    def test_manual_refresh_does_not_send_shipkartex_to_leopards(self):
        # Load just this route to avoid application's production startup side effects.
        tree = ast.parse(Path('main.py').read_text())
        function = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == 'refresh_tracking_only')
        function.decorator_list = []
        client = self.client
        namespace = {'shipkartex': client, 'order_details': [{'status': 'Delivered', 'line_items': [
            {'tracking_number': '1', 'courier': 'Shipkartex', 'status': 'Booked'},
            {'tracking_number': '2', 'courier': 'Leopards', 'status': 'Booked'},
        ]}], 'jsonify': lambda x: x, 'ssl': __import__('ssl'), 'os': os,
                     'requests': requests, 'parse_leopards_status': lambda p, cn: {'status': 'In Transit'}}
        exec(compile(ast.Module(body=[function], type_ignores=[]), 'main.py', 'exec'), namespace)
        with patch.object(client, 'index', return_value={}), patch.object(client, 'history', return_value=[{'status': 'Delivered'}]), patch('requests.get') as get:
            get.return_value.json.return_value = {'status': 1, 'packet_list': [{'track_number': '2'}]}
            result = namespace['refresh_tracking_only']()
            self.assertEqual(result['updated'], 2)
            self.assertTrue(get.call_args.args[0].endswith('track_numbers=2'))


if __name__ == '__main__':
    unittest.main()

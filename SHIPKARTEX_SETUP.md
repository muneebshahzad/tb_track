# Shipkartex tracking

Set `SHIPKARTEX_CLIENT_CODE` and `SHIPKARTEX_AUTH_KEY` in the deployment environment, then restart the app. Client code is `1072`; use the supplied auth key. The key is never sent to the browser. No additional Python dependencies are required.

For this local checkout, credentials are saved in the ignored `.env` file (owner-only permissions). The application reads environment variables; it does not automatically load `.env`. Export them before starting the app:

```sh
set -a
source .env
set +a
```

Shipments are identified by the fulfillment's Shipkarte/Shipkartex courier name, a Shipkartex tracking URL, or an exact tracking number match against the authenticated `GetOrderList.php` response. Third-party tracking number aliases are supported when returned by the API. Account lookup is cached for five minutes. Tracking-number prefixes and customer names are not used to guess shipment ownership. Fulfillments must contain a tracking number; this integration does not create bookings or fulfill unbooked orders.

Initial Shopify import, Shopify order webhooks, manual tracking refresh, and the existing background refresh all retrieve history from `TrackOrder.php`. `/track/<tracking_number>` displays the courier's timeline. Known Shipkartex shipments are excluded from Leopards tracking refresh and its active-CN list. Tracking requests have timeouts and run with at most four workers; failed tracking refreshes preserve cached statuses. Terminal shipment items are skipped independently so other shipments in a partially completed order continue refreshing.

API reference: https://portal.shipkartex.com/downloads/Shipkartex.pdf

Run regression tests:

```sh
python -m unittest discover -s tests -v
python -m py_compile main.py shipkartex.py
```

Production deployment and its environment configuration are separate from these local changes.

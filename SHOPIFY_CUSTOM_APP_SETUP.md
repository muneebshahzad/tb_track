# Shopify Custom App Setup

This repo now supports a Shopify custom app for protected customer data.

## What this unlocks

When configured, the app will use Shopify Admin GraphQL to enrich Shopify orders with:

- customer phone
- shipping/billing address
- customer city

This is meant to fill the gaps when your current Shopify plan/API response redacts customer details.

## Shopify-side steps

1. In Shopify Admin, create a **custom app** for this store.
2. Grant Admin API access for order/customer data needed by operations.
3. Request/enable **protected customer data** for address and phone.
4. Install or release the custom app.
5. Use either the **Client ID + Secret** or a direct **Admin API access token**, depending on what Shopify shows for your app.

## Environment variables

### Option A: Dev Dashboard credentials

If Shopify shows a **Client ID** and **Secret** for the app, add these to Railway or your local environment:

```env
SHOPIFY_GRAPHQL_STORE_DOMAIN=tick-bags-best-bean-bags-in-pakistan.myshopify.com
SHOPIFY_GRAPHQL_CLIENT_ID=xxxxxxxxxxxxxxxxxxxxxxxx
SHOPIFY_GRAPHQL_CLIENT_SECRET=xxxxxxxxxxxxxxxxxxxxxxxx
SHOPIFY_GRAPHQL_API_VERSION=2026-04
```

The app will exchange these for an Admin access token automatically.

### Option B: Direct Admin API token

If Shopify gives you a direct token, use:

```env
SHOPIFY_GRAPHQL_STORE_DOMAIN=tick-bags-best-bean-bags-in-pakistan.myshopify.com
SHOPIFY_GRAPHQL_ACCESS_TOKEN=shpat_xxxxxxxxxxxxxxxxx
SHOPIFY_GRAPHQL_API_VERSION=2026-04
```

Notes:

- `SHOPIFY_GRAPHQL_STORE_DOMAIN` should be the `.myshopify.com` domain only.
- If omitted, the code will try to derive the shop domain from `SHOP_URL`.
- If the token or client credentials are missing, the app keeps working and simply skips protected-data enrichment.

## How it behaves in this repo

- Shopify orders are still fetched through the existing integration.
- After fetch, the app calls Shopify GraphQL for the same order IDs.
- If protected data is returned, it overwrites blank/redacted customer fields in `customer_details` and parcel line items.
- If Shopify denies the fields or the app is not configured, the current fallback behavior remains unchanged.

## Status endpoint

You can verify configuration at:

`/shopify/protected-data/status`

It returns whether the GraphQL bridge is configured, which auth mode it is using, and which shop/API version it will use.

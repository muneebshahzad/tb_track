# Shopify Protected Customer Data Setup

This app now uses Shopify's OAuth install flow to obtain an offline Admin API token for protected customer data.

## What the repo needs

Set these Railway variables:

```env
SHOPIFY_GRAPHQL_STORE_DOMAIN=tick-bags-best-bean-bags-in-pakistan.myshopify.com
SHOPIFY_GRAPHQL_CLIENT_ID=xxxxxxxxxxxxxxxxxxxxxxxx
SHOPIFY_GRAPHQL_CLIENT_SECRET=shpss_xxxxxxxxxxxxxxxxx
SHOPIFY_GRAPHQL_API_VERSION=2026-04
SHOPIFY_APP_BASE_URL=https://dashboard.tickbags.com
```

Notes:

- `SHOPIFY_GRAPHQL_STORE_DOMAIN` must be the `.myshopify.com` domain only.
- `SHOPIFY_APP_BASE_URL` should be the public base URL of this Flask app.
- The offline access token is stored in the app database after OAuth completes.

## Shopify app configuration

In Shopify Dev Dashboard:

1. Set **App URL** to:
   `https://dashboard.tickbags.com`
2. Add this **Redirect URL**:
   `https://dashboard.tickbags.com/shopify/callback`
3. Add scopes:
   - `read_orders`
   - `read_customers`
4. Request protected customer data access for:
   - `Address`
   - `Phone`
5. Install the app on the store.

## Connect the app

Once Railway variables are set and the app is deployed, open:

`https://dashboard.tickbags.com/shopify/install`

That starts Shopify OAuth. After approval, Shopify redirects back to:

`/shopify/callback`

The app exchanges the code for an offline token and stores it in PostgreSQL.

## Status endpoint

Check connection state at:

`https://dashboard.tickbags.com/shopify/protected-data/status`

Useful fields:

- `auth_mode`: should become `oauth_offline_token`
- `has_stored_oauth_token`: should become `true`
- `has_access_token`: should become `true`
- `oauth_scopes`: shows the scopes returned by Shopify
- `install_url`: quick link to start OAuth again if needed

## How the repo uses it

- Existing Shopify order fetch remains unchanged.
- After fetch, the app requests protected customer data through Admin GraphQL.
- Missing customer phone/address/city fields are enriched from the protected-data response.

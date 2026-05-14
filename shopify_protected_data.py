from __future__ import annotations

import hashlib
import hmac
import os
import secrets
import time
from typing import Any
from urllib.parse import urlencode, urlparse

import requests

from db import get_app_setting, get_last_db_error, set_app_setting


SHOPIFY_TOKEN_SETTING_KEY = "shopify_offline_access_token"
SHOPIFY_SCOPE_SETTING_KEY = "shopify_offline_access_scopes"
SHOPIFY_INSTALLED_SHOP_KEY = "shopify_installed_shop_domain"
SHOPIFY_INSTALLED_AT_KEY = "shopify_installed_at"

_token_cache: dict[str, Any] = {"token": "", "expires_at": 0.0}
_last_token_error: str = ""


def _clean(value: Any) -> str:
    text = str(value or "").strip()
    if text.upper() == "N/A":
        return ""
    return text


def _pick(*values: Any) -> str:
    for value in values:
        cleaned = _clean(value)
        if cleaned:
            return cleaned
    return ""


def get_shop_domain() -> str:
    explicit = _clean(os.getenv("SHOPIFY_GRAPHQL_STORE_DOMAIN"))
    if explicit:
        return explicit.replace("https://", "").replace("http://", "").strip("/")

    shop_url = _clean(os.getenv("SHOP_URL"))
    if not shop_url:
        return ""

    parsed = urlparse(shop_url if "://" in shop_url else f"https://{shop_url}")
    return (parsed.hostname or "").strip()


def get_graphql_api_version() -> str:
    return _clean(os.getenv("SHOPIFY_GRAPHQL_API_VERSION")) or "2026-04"


def get_client_id() -> str:
    return _clean(os.getenv("SHOPIFY_GRAPHQL_CLIENT_ID"))


def get_client_secret() -> str:
    return _clean(os.getenv("SHOPIFY_GRAPHQL_CLIENT_SECRET"))


def get_app_base_url() -> str:
    explicit = _clean(os.getenv("SHOPIFY_APP_BASE_URL"))
    return explicit.rstrip("/") if explicit else "https://dashboard.tickbags.com"


def get_oauth_scopes() -> list[str]:
    scopes = _clean(os.getenv("SHOPIFY_GRAPHQL_SCOPES")) or "read_orders,read_customers"
    return [scope.strip() for scope in scopes.split(",") if scope.strip()]


def get_install_url(state: str) -> str:
    params = {
        "client_id": get_client_id(),
        "scope": ",".join(get_oauth_scopes()),
        "redirect_uri": f"{get_app_base_url()}/shopify/callback",
        "state": state,
        "grant_options[]": "per-user",
        "access_mode": "offline",
    }
    return f"https://{get_shop_domain()}/admin/oauth/authorize?{urlencode(params)}"


def verify_oauth_hmac(query_string: bytes | str) -> bool:
    if not query_string or not get_client_secret():
        return False

    raw_query = query_string.decode("utf-8") if isinstance(query_string, bytes) else str(query_string)
    parts = [part for part in raw_query.split("&") if part]
    received_hmac = ""
    filtered_parts = []

    for part in parts:
        if part.startswith("hmac="):
            received_hmac = part.split("=", 1)[1]
            continue
        if part.startswith("signature="):
            continue
        filtered_parts.append(part)

    if not received_hmac:
        return False

    message = "&".join(sorted(filtered_parts))

    digest = hmac.new(
        get_client_secret().encode("utf-8"),
        message.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(digest, received_hmac)


def create_oauth_state() -> str:
    return secrets.token_urlsafe(24)


def exchange_oauth_code_for_token(shop: str, code: str) -> dict[str, Any]:
    response = requests.post(
        f"https://{shop}/admin/oauth/access_token",
        json={
            "client_id": get_client_id(),
            "client_secret": get_client_secret(),
            "code": code,
        },
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def save_offline_token(shop: str, payload: dict[str, Any]) -> None:
    token = _clean(payload.get("access_token"))
    scopes = payload.get("scope") or payload.get("associated_user_scope") or ""
    if not token:
        raise ValueError("Shopify did not return an access token")

    writes_ok = all([
        set_app_setting(SHOPIFY_TOKEN_SETTING_KEY, token),
        set_app_setting(SHOPIFY_SCOPE_SETTING_KEY, _clean(scopes)),
        set_app_setting(SHOPIFY_INSTALLED_SHOP_KEY, shop),
        set_app_setting(SHOPIFY_INSTALLED_AT_KEY, str(int(time.time()))),
    ])
    if not writes_ok:
        raise RuntimeError(f"Could not persist Shopify OAuth token to database: {get_last_db_error() or 'unknown database error'}")

    saved_token = _clean(get_app_setting(SHOPIFY_TOKEN_SETTING_KEY))
    saved_shop = _clean(get_app_setting(SHOPIFY_INSTALLED_SHOP_KEY))
    if saved_token != token or saved_shop != shop:
        raise RuntimeError(f"Shopify OAuth token did not persist correctly: {get_last_db_error() or 'verification failed'}")

    _token_cache["token"] = token
    _token_cache["expires_at"] = time.time() + 86400 * 365


def get_graphql_token() -> str:
    global _last_token_error

    static_token = _clean(os.getenv("SHOPIFY_GRAPHQL_ACCESS_TOKEN"))
    if static_token:
        _last_token_error = ""
        return static_token

    now = time.time()
    if _token_cache["token"] and now < float(_token_cache["expires_at"] or 0):
        _last_token_error = ""
        return str(_token_cache["token"])

    stored_token = _clean(get_app_setting(SHOPIFY_TOKEN_SETTING_KEY))
    if stored_token:
        _token_cache["token"] = stored_token
        _token_cache["expires_at"] = now + 3600
        _last_token_error = ""
        return stored_token

    _last_token_error = ""
    return ""


def is_graphql_configured() -> bool:
    return bool(get_shop_domain() and (get_graphql_token() or (get_client_id() and get_client_secret())))


def get_graphql_endpoint() -> str:
    shop_domain = get_shop_domain()
    api_version = get_graphql_api_version()
    if not shop_domain:
        return ""
    return f"https://{shop_domain}/admin/api/{api_version}/graphql.json"


def get_protected_data_config_status() -> dict[str, Any]:
    static_token = _clean(os.getenv("SHOPIFY_GRAPHQL_ACCESS_TOKEN"))
    stored_token = _clean(get_app_setting(SHOPIFY_TOKEN_SETTING_KEY))
    token = get_graphql_token()
    auth_mode = "unconfigured"
    if static_token:
        auth_mode = "static_token"
    elif stored_token:
        auth_mode = "oauth_offline_token"
    elif get_client_id() and get_client_secret():
        auth_mode = "oauth_ready"

    return {
        "enabled": is_graphql_configured(),
        "shop_domain": get_shop_domain(),
        "api_version": get_graphql_api_version(),
        "auth_mode": auth_mode,
        "has_static_access_token": bool(static_token),
        "has_client_id": bool(get_client_id()),
        "has_client_secret": bool(get_client_secret()),
        "has_stored_oauth_token": bool(stored_token),
        "has_access_token": bool(token),
        "token_source_ready": bool(static_token or stored_token or (get_client_id() and get_client_secret())),
        "oauth_scopes": get_app_setting(SHOPIFY_SCOPE_SETTING_KEY),
        "installed_shop": get_app_setting(SHOPIFY_INSTALLED_SHOP_KEY),
        "token_error": _last_token_error if not token else "",
        "install_url": f"{get_app_base_url()}/shopify/install",
    }


def _order_gid(order_id: int | str) -> str:
    return f"gid://shopify/Order/{int(order_id)}"


def _format_address(address: dict[str, Any] | None) -> dict[str, str]:
    address = address or {}
    return {
        "name": _pick(address.get("name")),
        "address": _pick(address.get("address1"), address.get("address2")),
        "city": _pick(address.get("city")),
        "phone": _pick(address.get("phone")),
    }


def _format_customer(customer: dict[str, Any] | None) -> dict[str, str]:
    customer = customer or {}
    full_name = " ".join(
        part for part in [_clean(customer.get("firstName")), _clean(customer.get("lastName"))] if part
    ).strip()
    default_phone = ((customer.get("defaultPhoneNumber") or {}).get("phoneNumber")) if customer else ""
    return {
        "name": full_name,
        "phone": _pick(default_phone),
    }


def _build_customer_details(order_node: dict[str, Any]) -> dict[str, str]:
    shipping = _format_address(order_node.get("shippingAddress"))
    billing = _format_address(order_node.get("billingAddress"))
    customer = _format_customer(order_node.get("customer"))
    default_address = _format_address((order_node.get("customer") or {}).get("defaultAddress"))

    return {
        "name": _pick(shipping["name"], billing["name"], customer["name"], default_address["name"]),
        "address": _pick(shipping["address"], billing["address"], default_address["address"]),
        "city": _pick(shipping["city"], billing["city"], default_address["city"]),
        "phone": _pick(order_node.get("phone"), shipping["phone"], billing["phone"], customer["phone"], default_address["phone"]),
    }


def fetch_protected_order_details(order_ids: list[int | str]) -> tuple[dict[str, dict[str, str]], list[str]]:
    if not is_graphql_configured() or not order_ids:
        return {}, []

    query = """
    query ProtectedOrderDetails($ids: [ID!]!) {
      nodes(ids: $ids) {
        ... on Order {
          id
          legacyResourceId
          phone
          shippingAddress {
            name
            address1
            address2
            city
            phone
          }
          billingAddress {
            name
            address1
            address2
            city
            phone
          }
          customer {
            firstName
            lastName
            defaultPhoneNumber {
              phoneNumber
            }
            defaultAddress {
              name
              address1
              address2
              city
              phone
            }
          }
        }
      }
    }
    """

    variables = {"ids": [_order_gid(order_id) for order_id in order_ids]}
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": get_graphql_token(),
    }

    response = requests.post(
        get_graphql_endpoint(),
        json={"query": query, "variables": variables},
        headers=headers,
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()

    errors = [error.get("message", "Unknown Shopify GraphQL error") for error in payload.get("errors") or []]
    details: dict[str, dict[str, str]] = {}

    for node in payload.get("data", {}).get("nodes") or []:
        if not node or not node.get("legacyResourceId"):
            continue
        details[str(node["legacyResourceId"])] = _build_customer_details(node)

    return details, errors

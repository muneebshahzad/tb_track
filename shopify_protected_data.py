from __future__ import annotations

import os
import time
from typing import Any
from urllib.parse import urlparse

import requests


_token_cache: dict[str, Any] = {"token": "", "expires_at": 0.0}


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


def get_graphql_token() -> str:
    static_token = _clean(os.getenv("SHOPIFY_GRAPHQL_ACCESS_TOKEN"))
    if static_token:
        return static_token

    now = time.time()
    if _token_cache["token"] and now < float(_token_cache["expires_at"] or 0):
        return str(_token_cache["token"])

    client_id = _clean(os.getenv("SHOPIFY_GRAPHQL_CLIENT_ID"))
    client_secret = _clean(os.getenv("SHOPIFY_GRAPHQL_CLIENT_SECRET"))
    shop_domain = get_shop_domain()
    if not (client_id and client_secret and shop_domain):
        return ""

    response = requests.post(
        f"https://{shop_domain}/admin/oauth/access_token",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        },
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    access_token = _clean(payload.get("access_token"))
    expires_in = int(payload.get("expires_in") or 0)
    if access_token:
        _token_cache["token"] = access_token
        _token_cache["expires_at"] = now + max(0, expires_in - 300)
    return access_token


def is_graphql_configured() -> bool:
    return bool(get_shop_domain() and get_graphql_token())


def get_graphql_endpoint() -> str:
    shop_domain = get_shop_domain()
    api_version = get_graphql_api_version()
    if not shop_domain:
        return ""
    return f"https://{shop_domain}/admin/api/{api_version}/graphql.json"


def get_protected_data_config_status() -> dict[str, Any]:
    static_token = _clean(os.getenv("SHOPIFY_GRAPHQL_ACCESS_TOKEN"))
    client_id = _clean(os.getenv("SHOPIFY_GRAPHQL_CLIENT_ID"))
    client_secret = _clean(os.getenv("SHOPIFY_GRAPHQL_CLIENT_SECRET"))
    return {
        "enabled": is_graphql_configured(),
        "shop_domain": get_shop_domain(),
        "api_version": get_graphql_api_version(),
        "auth_mode": "static_token" if static_token else ("client_credentials" if client_id and client_secret else "unconfigured"),
        "has_static_access_token": bool(static_token),
        "has_client_id": bool(client_id),
        "has_client_secret": bool(client_secret),
        "has_access_token": bool(get_graphql_token()),
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

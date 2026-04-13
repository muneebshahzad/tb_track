import json
import os
import lazop
from datetime import datetime, timedelta

TOKEN_FILE = "daraz_tokens.json"
APP_KEY = "501554"
APP_SECRET = "nrP3XFN7ChZL53cXyVED1yj4iGZZtlcD"
BASE_URL = "https://api.daraz.pk/rest"

def save_tokens(access_token, refresh_token, expires_in=604800):
    data = {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "expires_at": (datetime.now() + timedelta(seconds=expires_in)).isoformat()
    }
    with open(TOKEN_FILE, "w") as f:
        json.dump(data, f)

def load_tokens():
    if not os.path.exists(TOKEN_FILE):
        return None
    with open(TOKEN_FILE, "r") as f:
        return json.load(f)

def is_expired(tokens):
    expires_at = datetime.fromisoformat(tokens["expires_at"])
    return datetime.now() >= expires_at - timedelta(hours=1)

def refresh_access_token(refresh_token):
    client = lazop.LazopClient(BASE_URL, APP_KEY, APP_SECRET)
    request = lazop.LazopRequest('/auth/token/refresh')
    request.add_api_param('refresh_token', refresh_token)
    response = client.execute(request)
    body = response.body  # ← already a dict
    if "access_token" in body:
        save_tokens(body["access_token"], body["refresh_token"])
        print("✅ Daraz token refreshed.")
        return body["access_token"]
    else:
        raise Exception(f"Token refresh failed: {body}")


def get_access_token():
    tokens = load_tokens()
    if not tokens:
        raise Exception("❌ No Daraz tokens found. Run first_time_auth.py first.")
    if is_expired(tokens):
        print("⏰ Daraz token expired, refreshing...")
        return refresh_access_token(tokens["refresh_token"])
    return tokens["access_token"]
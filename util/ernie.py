import os
import time
from datetime import datetime, timezone

import requests


AREAS = {"Greenpoint", "Williamsburg", "East Williamsburg", "West Village"}


def _bathrooms(listing):
    return (listing.get("full_bathroom_count") or 0) + (
        (listing.get("half_bathroom_count") or 0) * 0.5
    )


def matches_ernie_search(listing):
    price = listing.get("price")
    bedrooms = listing.get("bedroom_count")
    return (
        listing.get("area_name") in AREAS
        and price is not None
        and 3000 <= price <= 6500
        and bedrooms is not None
        and 1 <= bedrooms <= 3
        and _bathrooms(listing) >= 1
    )


def send_to_ernie(listing):
    url = os.getenv("ERNIE_WEBHOOK_URL")
    secret = os.getenv("ERNIE_WEBHOOK_SECRET")
    if not url or not secret:
        return False
    if not url.startswith("https://"):
        raise ValueError("ERNIE_WEBHOOK_URL must use HTTPS")

    delivery_id = f"fm:ernie:listing:{listing['id']}"
    photo_keys = [key for key in (listing.get("photos") or "").split(",") if key]
    created_at = datetime.now(timezone.utc).isoformat()
    payload = {
        "type": "listing.created",
        "created_at": created_at,
        "listing": {
            "id": str(listing["id"]),
            "address": ", ".join(filter(None, [listing.get("street"), listing.get("unit")])),
            "neighborhood": listing.get("area_name"),
            "price": listing.get("price"),
            "bedrooms": listing.get("bedroom_count"),
            "bathrooms": _bathrooms(listing),
            "created_at": created_at,
            "streeteasy_url": f"https://streeteasy.com{listing['url_path']}",
            "photos": [
                f"https://photos.zillowstatic.com/fp/{key}-se_large_800_400.webp"
                for key in photo_keys
            ],
        },
    }
    headers = {"X-Webhook-Secret": secret, "X-Delivery-Id": delivery_id}

    for attempt in range(3):
        try:
            response = requests.post(
                url,
                json=payload,
                headers=headers,
                timeout=5,
                allow_redirects=False,
            )
            if 200 <= response.status_code < 300:
                return True
            if response.status_code < 500 and response.status_code != 429:
                return False
        except (requests.ConnectionError, requests.Timeout):
            pass
        if attempt < 2:
            time.sleep(0.5 * (2**attempt))

    return False

"""
Creative Details Fetcher
========================
Augments metric rows with creative metadata: ad format (video/image/carousel),
thumbnail URL, headline, body text, and CTA type.

Used by the analyzer to correlate creative elements with performance.
"""

import os
import logging
import requests
import time
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger("creative_fetcher")

META_TOKEN  = os.getenv("META_ACCESS_TOKEN")
API_VERSION = "v21.0"
BASE        = f"https://graph.facebook.com/{API_VERSION}"

CREATIVE_FIELDS = [
    "id",
    "name",
    "title",
    "body",
    "call_to_action_type",
    "object_type",          # VIDEO, PHOTO, etc.
    "thumbnail_url",
    "image_url",
    "video_id",
    "effective_object_story_id",
    "asset_feed_spec",      # dynamic creative info
]


def fetch_ad_creative(ad_id: str) -> dict:
    """
    Fetch the creative attached to an ad.
    Returns a dict with format, thumbnail, headline, body, cta.
    """
    # Step 1: get creative ID from the ad
    r = requests.get(
        f"{BASE}/{ad_id}",
        params={
            "fields": "creative{" + ",".join(CREATIVE_FIELDS) + "}",
            "access_token": META_TOKEN,
        },
        timeout=15,
    )
    r.raise_for_status()
    data = r.json()
    creative = data.get("creative", {})

    if not creative:
        return {"ad_id": ad_id, "format": "unknown"}

    obj_type = creative.get("object_type", "").upper()
    fmt = (
        "video"    if obj_type in ("VIDEO", "SHARE") else
        "carousel" if obj_type == "CAROUSEL" else
        "image"
    )

    return {
        "ad_id":       ad_id,
        "creative_id": creative.get("id"),
        "format":      fmt,
        "title":       creative.get("title", ""),
        "body":        creative.get("body", ""),
        "cta":         creative.get("call_to_action_type", ""),
        "thumbnail":   creative.get("thumbnail_url") or creative.get("image_url", ""),
        "video_id":    creative.get("video_id", ""),
    }


def fetch_creatives_bulk(ad_ids: list[str], sleep_between: float = 0.2) -> dict[str, dict]:
    """
    Fetch creatives for a list of ad IDs.
    Returns a dict mapping ad_id → creative metadata.
    """
    results = {}
    for i, ad_id in enumerate(ad_ids):
        try:
            results[ad_id] = fetch_ad_creative(ad_id)
            log.debug("  Fetched creative for %s (%d/%d)", ad_id, i + 1, len(ad_ids))
        except Exception as e:
            log.warning("  Failed to fetch creative for %s: %s", ad_id, e)
            results[ad_id] = {"ad_id": ad_id, "format": "unknown", "error": str(e)}
        time.sleep(sleep_between)
    return results

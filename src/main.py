"""
Module defines the main entry point for the Apify Actor.

Converted to align with Apify Actor structure without changing functionality.
"""

from __future__ import annotations

import json
import requests
import pandas as pd
from apify import Actor


def fetch_all_reviews(product_id: str) -> list[dict]:
    reviews: list[dict] = []
    offset = 0

    API_URL = "https://api.bazaarvoice.com/data/reviews.json"
    PASSKEY = "caLvvjC2zeoZVIUfUackXiwaE0qowi2Mmvb4UTm3rrn50"
    PAGE_SIZE = 100  # adjust up to your plan’s max

    while True:
        params = {
            "passkey":    PASSKEY,
            "apiversion": "5.5",
            "limit":      PAGE_SIZE,
            "offset":     offset,
            "Include":    "Products,Authors,Comments",
            "filter": [
                f"ProductId:eq:{product_id}",
                "IsRatingsOnly:eq:false",
                "ContentLocale:eq:en_US"
            ]
        }

        resp = requests.get(API_URL, params=params)
        resp.raise_for_status()
        data = resp.json()

        batch = data["Results"]
        reviews.extend(batch)

        total = data["TotalResults"]
        Actor.log.info(f"[{product_id}] Fetched {len(reviews)}/{total} reviews…")

        if len(reviews) >= total:
            break

        offset += PAGE_SIZE

    return reviews


async def main() -> None:
    """Entry point for the Apify Actor."""
    async with Actor:
        # Get list of product IDs from actor input
        actor_input = await Actor.get_input()
        product_ids = actor_input.get("product_ids") if actor_input else None
        if not product_ids:
            raise ValueError('Missing "product_ids" in input!')

        all_reviews: list[dict] = []
        for pid in product_ids:
            reviews = fetch_all_reviews(pid)
            for r in reviews:
                r["product_handle"] = pid
            all_reviews.extend(reviews)

        # Push each review to the default dataset
        for review in all_reviews:
            await Actor.push_data(review)

        # Save all reviews locally as CSV
        df = pd.json_normalize(all_reviews)
        csv_path = "all_meundies_reviews.csv"
        df.to_csv(csv_path, index=False, encoding="utf-8")
        Actor.log.info(f"Saved all reviews to → {csv_path}")


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
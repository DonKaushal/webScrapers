import requests
import json
import pandas as pd

def fetch_all_reviews(product_id):
    reviews = []
    offset  = 0

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
        print(f"[{product_id}] Fetched {len(reviews)}/{total} reviews…")

        if len(reviews) >= total:
            break

        offset += PAGE_SIZE

    return reviews

if __name__ == "__main__":
    # 1) Load product URLs
    with open("unique_bases.json", "r", encoding="utf-8") as f:
        product_ids = json.load(f)

    all_reviews = []

    # 2) Loop through each URL, extract handle, fetch & tag reviews
    for id in product_ids:
        reviews = fetch_all_reviews(id)
        for r in reviews:
            r["product_handle"] = id
        all_reviews.extend(reviews)

    # 3) Normalize and save to a single CSV
    df = pd.json_normalize(all_reviews)
    csv_path = "all_meundies_reviews.csv"
    df.to_csv(csv_path, index=False, encoding="utf-8")
    print(f"Saved all reviews to → {csv_path}")

import json
import time
from typing import List

from retail_promotions_to_shopify_metafields import DatabaseConnection, ShopifyClient, Config


def fetch_all_vendors() -> List[str]:
    db = DatabaseConnection()
    try:
        sql = """
        SELECT DISTINCT LTRIM(RTRIM(Vendor)) AS Vendor
        FROM Ecomm_DB_PROD.dbo.SM_Vendor
        WHERE Vendor IS NOT NULL AND LTRIM(RTRIM(Vendor)) <> ''
        """
        rows = db.query(sql)
        return [r["Vendor"] for r in rows if r.get("Vendor")]
    finally:
        db.close()


def main():
    print("=== All Vendors -> Shopify Quick Counts (by vendor) ===")
    shop = ShopifyClient()

    vendors = fetch_all_vendors()
    print(f"Found {len(vendors)} unique vendors in DB")

    results = []

    for vendor in vendors:
        vendor = vendor.strip()
        if not vendor:
            continue

        print(f"[Vendor] {vendor}")
        count = shop.rest_count_products_by_vendor(vendor)
        print(f"  Products found: {count}")

        results.append({
            "vendor": vendor,
            "collection_matched": False,
            "products_found": count,
            "will_write": count,
            "will_delete": 0
        })

        time.sleep(max(0.0, getattr(Config, "SLEEP_BETWEEN_CALLS", 0.0)))

    out_file = "all_vendor_product_counts.json"
    with open(out_file, "w", encoding="utf-8") as fh:
        json.dump(results, fh, ensure_ascii=False, indent=2)

    print(f"Wrote {out_file}")


if __name__ == "__main__":
    main()

import json
import time
from typing import List

from retail_promotions_to_shopify_metafields import DatabaseConnection, ShopifyClient, Config


def fetch_all_vendors() -> List[str]:
    db = DatabaseConnection()
    try:
        # First try the expected column name
        try:
            sql = """
            SELECT DISTINCT LTRIM(RTRIM(Vendor)) AS Vendor
            FROM Ecomm_DB_PROD.dbo.SM_Vendor
            WHERE Vendor IS NOT NULL AND LTRIM(RTRIM(Vendor)) <> ''
            """
            rows = db.query(sql)
            return [r["Vendor"] for r in rows if r.get("Vendor")]
        except Exception:
            # Fallback: probe the table to find a suitable column name
            probe = db.query("SELECT TOP 1 * FROM Ecomm_DB_PROD.dbo.SM_Vendor")
            if not probe:
                return []
            cols = list(probe[0].keys())
            candidates = [c for c in cols if c.lower() in ("vendor", "vendorname", "name", "vendor_name")]
            if not candidates:
                return []
            col = candidates[0]
            sql2 = f"SELECT DISTINCT LTRIM(RTRIM([{col}])) AS Vendor FROM Ecomm_DB_PROD.dbo.SM_Vendor WHERE [{col}] IS NOT NULL AND LTRIM(RTRIM([{col}])) <> ''"
            rows2 = db.query(sql2)
            return [r["Vendor"] for r in rows2 if r.get("Vendor")]
    finally:
        db.close()


def main():
    print("=== All Vendors -> Shopify Collection/Product Counts (DRY_RUN) ===")
    print(f"DRY_RUN = {Config.DRY_RUN}")

    shop = ShopifyClient()

    vendors = fetch_all_vendors()
    print(f"Found {len(vendors)} unique vendors in DB")

    results = []

    for vendor in vendors:
        vendor = vendor.strip()
        if not vendor:
            continue

        print(f"[Vendor] {vendor}")

        # First get vendor-level count (fast); skip expensive GraphQL if vendor has 0 products
        try:
            vendor_count = shop.rest_count_products_by_vendor(vendor)
        except Exception as e:
            print(f"  Error counting vendor products: {e}. Assuming 0 and continuing")
            vendor_count = 0

        if vendor_count == 0:
            print(f"  Products found: 0 (skip collection match)")
            count = 0
            collection_matched = False
        else:
            # Only attempt GraphQL collection match when vendor has products
            try:
                col = shop.find_collection_by_title_exact(vendor)
            except Exception as e:
                print(f"  Error finding collection: {e}. Fallback to product.vendor")
                col = None

            if col:
                col_id, col_title = col
                print(f"  Collection matched: {col_title}")
                try:
                    count = shop.rest_count_products_in_collection(col_id)
                except Exception as e:
                    print(f"  Error counting collection products: {e}. Falling back to vendor count")
                    count = vendor_count
                collection_matched = True
            else:
                print("  Collection not found. Fallback: product.vendor")
                count = vendor_count
                collection_matched = False

        print(f"  Products found: {count}")

        # Use same output fields as existing vendor_product_counts.json
        results.append({
            "vendor": vendor,
            "collection_matched": collection_matched,
            "products_found": count,
            "will_write": count,
            "will_delete": 0
        })

        # write incremental progress so partial runs still produce output
        out_file = "all_vendor_product_counts.json"
        try:
            with open(out_file, "w", encoding="utf-8") as fh:
                json.dump(results, fh, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"  Warning: failed to write progress file: {e}")

        # small pause to avoid hammering API (Config can be adjusted)
        time.sleep(max(0.0, getattr(Config, "SLEEP_BETWEEN_CALLS", 0.0)))

    out_file = "all_vendor_product_counts.json"
    with open(out_file, "w", encoding="utf-8") as fh:
        json.dump(results, fh, ensure_ascii=False, indent=2)

    print(f"Wrote {out_file}")


if __name__ == "__main__":
    main()

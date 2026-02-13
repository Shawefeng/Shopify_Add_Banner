import json
import time
from typing import List

import pandas as pd

from retail_promotions_to_shopify_metafields import DatabaseConnection, ShopifyClient, Config


def fetch_vendor_hub_vendors() -> List[str]:
    db = DatabaseConnection()
    try:
        # First try the expected column name
        try:
            sql = """
            SELECT DISTINCT LTRIM(RTRIM(Vendor)) AS Vendor
            FROM Ecomm_DB_PROD.dbo.VH_Vendors
            WHERE Vendor IS NOT NULL AND LTRIM(RTRIM(Vendor)) <> ''
            """
            rows = db.query(sql)
            return [r["Vendor"] for r in rows if r.get("Vendor")]
        except Exception:
            # Fallback: probe the table to find a suitable column name
            probe = db.query("SELECT TOP 1 * FROM Ecomm_DB_PROD.dbo.VH_Vendors")
            if not probe:
                return []
            cols = list(probe[0].keys())
            candidates = [c for c in cols if c.lower() in ("vendor", "vendorname", "name", "vendor_name")]
            if not candidates:
                return []
            col = candidates[0]
            sql2 = (
                "SELECT DISTINCT LTRIM(RTRIM([" + col + "])) AS Vendor "
                "FROM Ecomm_DB_PROD.dbo.VH_Vendors "
                "WHERE [" + col + "] IS NOT NULL AND LTRIM(RTRIM([" + col + "])) <> ''"
            )
            rows2 = db.query(sql2)
            return [r["Vendor"] for r in rows2 if r.get("Vendor")]
    finally:
        db.close()


def build_grouped_view(results):
    no_products = [e for e in results if int(e.get("products_found", 0)) == 0]
    no_collection_but_products = [
        e for e in results
        if not e.get("collection_matched") and int(e.get("products_found", 0)) > 0
    ]
    collection_matched = [e for e in results if e.get("collection_matched")]

    key = lambda x: (x.get("vendor") or "").lower()
    no_products.sort(key=key)
    no_collection_but_products.sort(key=key)
    collection_matched.sort(key=key)

    return {
        "no_products": no_products,
        "no_collection_but_products": no_collection_but_products,
        "collection_matched": collection_matched,
    }


def write_excel(view, out_path):
    sheets = {
        "No Products": view.get("no_products", []),
        "No Collection But Products": view.get("no_collection_but_products", []),
        "Collection Matched": view.get("collection_matched", []),
    }

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        for name, rows in sheets.items():
            if not rows:
                df = pd.DataFrame(
                    columns=["vendor", "collection_matched", "products_found", "will_write", "will_delete"]
                )
            else:
                df = pd.DataFrame(rows)
            df.to_excel(writer, sheet_name=name[:31], index=False)


def main():
    print("=== Vendor Hub -> Shopify Collection/Product Counts (DRY_RUN) ===")
    print(f"DRY_RUN = {Config.DRY_RUN}")

    shop = ShopifyClient()

    vendors = fetch_vendor_hub_vendors()
    print(f"Found {len(vendors)} unique vendors in VH_Vendors")

    results = []

    for vendor in vendors:
        vendor = vendor.strip()
        if not vendor:
            continue

        print(f"[Vendor] {vendor}")

        # First get vendor-level count; skip collection match when 0
        try:
            vendor_count = shop.rest_count_products_by_vendor(vendor)
        except Exception as e:
            print(f"  Error counting vendor products: {e}. Assuming 0 and continuing")
            vendor_count = 0

        if vendor_count == 0:
            print("  Products found: 0 (skip collection match)")
            count = 0
            collection_matched = False
        else:
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

        results.append({
            "vendor": vendor,
            "collection_matched": collection_matched,
            "products_found": count,
            "will_write": count,
            "will_delete": 0,
        })

        # Write incremental results
        out_file = "vendor_hub_product_counts.json"
        try:
            with open(out_file, "w", encoding="utf-8") as fh:
                json.dump(results, fh, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"  Warning: failed to write progress file: {e}")

        time.sleep(max(0.0, getattr(Config, "SLEEP_BETWEEN_CALLS", 0.0)))

    out_file = "vendor_hub_product_counts.json"
    with open(out_file, "w", encoding="utf-8") as fh:
        json.dump(results, fh, ensure_ascii=False, indent=2)

    view = build_grouped_view(results)
    view_file = "vendor_hub_product_counts_view.json"
    with open(view_file, "w", encoding="utf-8") as fh:
        json.dump(view, fh, ensure_ascii=False, indent=2)

    excel_file = "vendor_hub_product_counts_report.xlsx"
    write_excel(view, excel_file)

    print(f"Wrote {out_file}")
    print(f"Wrote {view_file}")
    print(f"Wrote {excel_file}")


if __name__ == "__main__":
    main()

import json
from typing import Dict, List, Optional, Tuple

from retail_promotions_to_shopify_metafields import Config, require_env, ShopifyClient, normalize

# Optional Excel output dependency
try:
    from openpyxl import Workbook
    _HAS_OPENPYXL = True
except Exception:
    _HAS_OPENPYXL = False


def fetch_vendor_counts() -> Dict[str, int]:
    require_env()
    shop = ShopifyClient()

    q = '''
    query($cursor: String) {
      products(first: 250, after: $cursor) {
        pageInfo { hasNextPage endCursor }
        nodes { id vendor }
      }
    }
    '''

    counts: Dict[str, int] = {}
    display_name: Dict[str, str] = {}

    cursor = None
    has_next = True

    print("Fetching all products from Shopify...")
    while has_next:
        data = shop.graphql(q, {"cursor": cursor})
        conn = data["data"]["products"]
        for n in conn["nodes"]:
            v = (n.get("vendor") or "").strip()
            if not v:
                continue
            key = normalize(v)
            counts[key] = counts.get(key, 0) + 1
            if key not in display_name:
                display_name[key] = v

        has_next = conn["pageInfo"]["hasNextPage"]
        cursor = conn["pageInfo"]["endCursor"]

    # build output mapping with original display names
    out = {display_name[k]: counts[k] for k in counts}
    return out


def check_collection_for_vendor(shop: ShopifyClient, vendor: str) -> Tuple[bool, Optional[str], Optional[int]]:
    """
    Check if there's a collection matching the vendor name (case-insensitive).
    Returns: (has_collection, collection_name, collection_product_count)
    """
    col = shop.find_collection_by_title_exact(vendor)
    if col:
        col_id, col_title = col
        count = shop.rest_count_products_in_collection(col_id)
        return True, col_title, count
    return False, None, None


def main():
    print("=== Shopify Vendor Counts with Collection Matching ===")
    try:
        counts = fetch_vendor_counts()
    except Exception as e:
        print("Failed to fetch vendor counts:", e)
        return

    total_vendors = len(counts)
    total_products = sum(counts.values())

    print(f"Vendors found: {total_vendors}")
    print(f"Total products counted: {total_products}")
    print("\nChecking for matching collections...")

    shop = ShopifyClient()
    results: List[dict] = []

    for idx, (vendor, product_count) in enumerate(counts.items(), 1):
        print(f"[{idx}/{total_vendors}] Checking vendor: {vendor}")
        has_collection, collection_name, collection_count = check_collection_for_vendor(shop, vendor)
        
        results.append({
            "vendor": vendor,
            "vendor_product_count": product_count,
            "has_collection": has_collection,
            "collection_name": collection_name if has_collection else "false",
            "collection_product_count": collection_count if has_collection else "false"
        })

    # Write JSON output
    with open("shopify_vendor_counts.json", "w", encoding="utf-8") as fh:
        json.dump(results, fh, ensure_ascii=False, indent=2)

    # Write CSV output
    try:
        import csv
        with open("shopify_vendor_counts.csv", "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=["vendor", "vendor_product_count", "has_collection", "collection_name", "collection_product_count"])
            writer.writeheader()
            writer.writerows(results)
    except Exception as e:
        print(f"Failed to write CSV: {e}")

    # Write Excel output
    if _HAS_OPENPYXL:
        try:
            wb = Workbook()
            ws = wb.active
            ws.title = "Vendor Counts"
            ws.append(["Vendor", "Vendor Product Count", "Has Collection", "Collection Name", "Collection Product Count"])
            for r in results:
                ws.append([
                    r["vendor"],
                    r["vendor_product_count"],
                    r["has_collection"],
                    r["collection_name"],
                    r["collection_product_count"]
                ])
            wb.save("shopify_vendor_counts.xlsx")
            print("\n✓ Wrote shopify_vendor_counts.json, shopify_vendor_counts.csv and shopify_vendor_counts.xlsx")
            return
        except Exception as e:
            print(f"Failed to write xlsx: {e}")

    print("\n✓ Wrote shopify_vendor_counts.json and shopify_vendor_counts.csv")


if __name__ == "__main__":
    main()

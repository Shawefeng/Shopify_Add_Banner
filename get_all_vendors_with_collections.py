import json
import time

from retail_promotions_to_shopify_metafields import ShopifyClient, Config

# Optional Excel output dependency
try:
    from openpyxl import Workbook
    _HAS_OPENPYXL = True
except Exception:
    _HAS_OPENPYXL = False


def fetch_all_vendors_from_shopify(shop: ShopifyClient) -> dict:
    """
    Fetch all vendors from Shopify products
    Returns: dict mapping vendor name to product count
    """
    q = '''
    query($cursor: String) {
      products(first: 250, after: $cursor) {
        pageInfo { hasNextPage endCursor }
        nodes { id vendor }
      }
    }
    '''

    vendor_counts = {}
    cursor = None
    has_next = True
    total_products = 0

    print("Fetching all products from Shopify to extract vendors...")
    while has_next:
        data = shop.graphql(q, {"cursor": cursor})
        conn = data["data"]["products"]
        
        for n in conn["nodes"]:
            total_products += 1
            v = (n.get("vendor") or "").strip()
            if v:
                vendor_counts[v] = vendor_counts.get(v, 0) + 1

        has_next = conn["pageInfo"]["hasNextPage"]
        cursor = conn["pageInfo"]["endCursor"]
        
        if total_products % 500 == 0:
            print(f"  Processed {total_products} products...")

    print(f"  Total products: {total_products}")
    print(f"  Unique vendors: {len(vendor_counts)}")
    return vendor_counts


def main():
    print("=== Shopify Vendors with Collection Matching ===\n")
    shop = ShopifyClient()

    # Fetch all vendors from Shopify products
    vendor_counts = fetch_all_vendors_from_shopify(shop)
    vendors = sorted(vendor_counts.keys())
    
    print(f"\nProcessing {len(vendors)} vendors...\n")

    results = []

    for idx, vendor in enumerate(vendors, 1):
        print(f"[{idx}/{len(vendors)}] {vendor}")
        
        # We already have the product count from the initial fetch
        vendor_product_count = vendor_counts[vendor]
        print(f"  Vendor products: {vendor_product_count}")
        
        # Check for matching collection
        col = shop.find_collection_by_title_exact(vendor)
        has_collection = col is not None
        collection_name = "false"
        collection_product_count = "false"
        
        if has_collection:
            col_id, col_title = col
            collection_name = col_title
            collection_product_count = shop.rest_count_products_in_collection(col_id)
            print(f"  Collection matched: {collection_name} ({collection_product_count} products)")
        else:
            print(f"  No matching collection")

        results.append({
            "vendor": vendor,
            "vendor_product_count": vendor_product_count,
            "has_collection": has_collection,
            "collection_name": collection_name,
            "collection_product_count": collection_product_count
        })

        time.sleep(max(0.0, getattr(Config, "SLEEP_BETWEEN_CALLS", 0.12)))
        print()

    # Write JSON
    with open("shopify_vendor_counts.json", "w", encoding="utf-8") as fh:
        json.dump(results, fh, ensure_ascii=False, indent=2)
    print("✓ Wrote shopify_vendor_counts.json")

    # Write CSV
    try:
        import csv
        with open("shopify_vendor_counts.csv", "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=["vendor", "vendor_product_count", "has_collection", "collection_name", "collection_product_count"])
            writer.writeheader()
            writer.writerows(results)
        print("✓ Wrote shopify_vendor_counts.csv")
    except Exception as e:
        print(f"Failed to write CSV: {e}")

    # Write Excel
    if _HAS_OPENPYXL:
        try:
            wb = Workbook()
            ws = wb.active
            ws.title = "Vendor Counts"
            
            # Headers
            ws.append(["Vendor", "Vendor Product Count", "Has Collection", "Collection Name", "Collection Product Count"])
            
            # Data rows
            for r in results:
                ws.append([
                    r["vendor"],
                    r["vendor_product_count"],
                    r["has_collection"],
                    r["collection_name"],
                    r["collection_product_count"]
                ])
            
            wb.save("shopify_vendor_counts.xlsx")
            print("✓ Wrote shopify_vendor_counts.xlsx")
        except Exception as e:
            print(f"Failed to write xlsx: {e}")
    else:
        print("! openpyxl not installed, skipping Excel output")

    print(f"\nTotal vendors processed: {len(results)}")


if __name__ == "__main__":
    main()

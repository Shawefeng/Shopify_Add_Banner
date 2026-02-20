import json
import os
import time
from retail_promotions_to_shopify_metafields import ShopifyClient, Config

PROGRESS_FILE = "vendor_progress.json"

QUERY = '''
query($cursor: String) {
  products(first: 250, after: $cursor) {
    pageInfo { hasNextPage endCursor }
    nodes { id vendor }
  }
}
'''


def load_progress():
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, "r", encoding="utf-8") as fh:
                return json.load(fh)
        except Exception:
            return {}
    return {}


def save_progress(data):
    try:
        with open(PROGRESS_FILE, "w", encoding="utf-8") as fh:
            json.dump(data, fh)
    except Exception:
        pass


def main():
    shop = ShopifyClient()
    progress = load_progress()
    cursor = progress.get("cursor")
    vendor_counts = progress.get("vendor_counts", {})
    total_products = progress.get("total_products", 0)

    has_next = True
    if cursor is None and progress.get("done"):
        print("Already completed. Unique vendors:", len(vendor_counts))
        return

    print("Resilient vendor count starting. Resuming cursor:", cursor)

    while has_next:
        try:
            data = shop.graphql(QUERY, {"cursor": cursor})
            conn = data["data"]["products"]

            for n in conn["nodes"]:
                total_products += 1
                v = (n.get("vendor") or "").strip()
                if v:
                    vendor_counts[v] = vendor_counts.get(v, 0) + 1

            has_next = conn["pageInfo"]["hasNextPage"]
            cursor = conn["pageInfo"]["endCursor"]

            # Save progress after each page
            save_progress({"cursor": cursor, "vendor_counts": vendor_counts, "total_products": total_products, "done": False})

            if total_products % 500 == 0:
                print(f"  Processed {total_products} products...")

            # short pause to be polite
            time.sleep(max(0.0, getattr(Config, "SLEEP_BETWEEN_CALLS", 0.12)))

        except Exception as e:
            print("Encountered error, sleeping and retrying:", str(e))
            time.sleep(10)
            continue

    # finished
    save_progress({"cursor": cursor, "vendor_counts": vendor_counts, "total_products": total_products, "done": True})

    print(f"Completed. Total products: {total_products}")
    print(f"Unique vendors: {len(vendor_counts)}")

    # write minimal output
    out = {"unique_vendors": len(vendor_counts), "total_products": total_products}
    try:
        with open("vendor_count_summary.json", "w", encoding="utf-8") as fh:
            json.dump(out, fh)
    except Exception:
        pass


if __name__ == '__main__':
    main()

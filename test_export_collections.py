import csv
import os
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

from retail_promotions_to_shopify_metafields import Config, ShopifyClient, require_env


def parse_numeric_id(gid: str) -> Optional[int]:
    if not gid:
        return None
    if gid.startswith("gid://"):
        try:
            return int(gid.rsplit("/", 1)[-1])
        except Exception:
            return None
    try:
        return int(gid)
    except Exception:
        return None


def list_collections(client: ShopifyClient, limit: int = 5) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    cursor = None

    q = """
    query($cursor: String, $first: Int!) {
      collections(first: $first, after: $cursor) {
        pageInfo { hasNextPage endCursor }
        nodes { id title handle updatedAt }
      }
    }
    """

    data = client.graphql(q, {"cursor": cursor, "first": limit})
    conn = data["data"]["collections"]
    nodes = conn.get("nodes", [])

    for n in nodes:
        gid = n.get("id", "")
        rows.append(
            {
                "collection_gid": gid,
                "collection_id": str(parse_numeric_id(gid) or ""),
                "title": n.get("title", ""),
                "handle": n.get("handle", ""),
                "updated_at": n.get("updatedAt", ""),
                "product_count": "",
                "vendors": "",
            }
        )

    return rows


def get_vendors_in_collection(client: ShopifyClient, collection_id: str) -> List[str]:
    vendors = set()
    cursor = None
    has_next = True

    q = """
    query($id: ID!, $cursor: String) {
      collection(id: $id) {
        products(first: 250, after: $cursor) {
          pageInfo { hasNextPage endCursor }
          nodes { vendor }
        }
      }
    }
    """

    while has_next:
        data = client.graphql(q, {"id": collection_id, "cursor": cursor})
        conn = data["data"]["collection"]["products"]
        for n in conn.get("nodes", []):
            v = (n.get("vendor") or "").strip()
            if v:
                vendors.add(v)
        has_next = conn["pageInfo"]["hasNextPage"]
        cursor = conn["pageInfo"]["endCursor"]

    return sorted(vendors)


def enrich_collections(rows: List[Dict[str, str]], client: ShopifyClient) -> None:
    total = len(rows)
    
    for idx, r in enumerate(rows, 1):
        gid = r.get("collection_gid")
        title = r.get("title", "")
        
        print(f"[{idx}/{total}] Processing: {title}")
        
        try:
            print(f"  - Getting product count...")
            cnt = client.rest_count_products_in_collection(gid)
            print(f"    Found {cnt} products")
        except Exception as e:
            print(f"  ! Failed to get count: {e}")
            cnt = 0
        
        try:
            print(f"  - Getting vendors...")
            vendors = get_vendors_in_collection(client, gid)
            vendors_str = ";".join(vendors)
            print(f"    Found {len(vendors)} vendors: {vendors_str[:100]}")
        except Exception as e:
            print(f"  ! Failed to get vendors: {e}")
            vendors_str = ""

        r["product_count"] = str(cnt)
        r["vendors"] = vendors_str
        
        time.sleep(0.2)


def main() -> None:
    require_env()
    client = ShopifyClient()

    print("Testing export with first 5 collections...")
    rows = list_collections(client, limit=5)
    print(f"Found {len(rows)} collections\n")
    
    enrich_collections(rows, client)
    
    print("\n=== RESULTS ===")
    for r in rows:
        print(f"\nCollection: {r['title']}")
        print(f"  Products: {r['product_count']}")
        print(f"  Vendors: {r['vendors']}")


if __name__ == "__main__":
    main()

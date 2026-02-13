import csv
import os
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

from retail_promotions_to_shopify_metafields import Config, ShopifyClient, require_env


DEFAULT_XLSX = "shopify_collections_export.xlsx"
DEFAULT_CSV = "shopify_collections_export.csv"
DEFAULT_SQL = "shopify_collections_table.sql"
DEFAULT_TABLE = "dbo.Shopify_Collections"


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


def list_collections(client: ShopifyClient) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    cursor = None
    has_next = True

    q = """
    query($cursor: String) {
      collections(first: 250, after: $cursor) {
        pageInfo { hasNextPage endCursor }
        nodes { id title handle updatedAt }
      }
    }
    """

    while has_next:
        data = client.graphql(q, {"cursor": cursor})
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

        has_next = conn["pageInfo"]["hasNextPage"]
        cursor = conn["pageInfo"]["endCursor"]

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
    start_time = time.time()
    
    for idx, r in enumerate(rows, 1):
        gid = r.get("collection_gid")
        title = r.get("title", "")[:50]
        
        if idx % 10 == 0 or idx == total:
            elapsed = time.time() - start_time
            avg_time = elapsed / idx
            remaining = (total - idx) * avg_time
            print(f"  [{idx}/{total} - {idx*100//total}%] ETA: {int(remaining//60)}m{int(remaining%60)}s - Last: {title}", flush=True)
        
        try:
            cnt = client.rest_count_products_in_collection(gid)
        except Exception as e:
            print(f"  ! Count error for '{title}': {str(e)[:80]}", flush=True)
            cnt = 0
            time.sleep(2)
        
        try:
            vendors = get_vendors_in_collection(client, gid)
            vendors_str = ";".join(vendors)
        except KeyboardInterrupt:
            raise
        except Exception as e:
            print(f"  ! Vendor error for '{title}': {str(e)[:80]}", flush=True)
            vendors_str = ""
            time.sleep(2)

        r["product_count"] = str(cnt)
        r["vendors"] = vendors_str
        
        time.sleep(0.15)  # Rate limiting


def write_csv(path: str, rows: List[Dict[str, str]], extra: Dict[str, str]) -> None:
    if not rows:
        return
    fieldnames = list(rows[0].keys()) + list(extra.keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            row = dict(r)
            row.update(extra)
            writer.writerow(row)


def write_xlsx(path: str, rows: List[Dict[str, str]], extra: Dict[str, str]) -> bool:
    try:
        from openpyxl import Workbook
    except Exception:
        return False

    if not rows:
        return True

    fieldnames = list(rows[0].keys()) + list(extra.keys())
    wb = Workbook()
    ws = wb.active
    ws.title = "collections"

    ws.append(fieldnames)
    for r in rows:
        row = dict(r)
        row.update(extra)
        ws.append([row.get(k, "") for k in fieldnames])

    try:
        wb.save(path)
        return True
    except PermissionError:
        return False
    except Exception:
        return False


def write_create_table_sql(path: str, table_name: str) -> None:
    sql = f"""
IF OBJECT_ID('{table_name}', 'U') IS NULL
BEGIN
    CREATE TABLE {table_name} (
        CollectionId BIGINT NULL,
        CollectionGid NVARCHAR(128) NOT NULL,
        Title NVARCHAR(255) NOT NULL,
        Handle NVARCHAR(255) NULL,
        UpdatedAt DATETIME2 NULL,
        ProductCount INT NULL,
        Vendors NVARCHAR(MAX) NULL,
        Shop NVARCHAR(255) NOT NULL,
        ExportedAt DATETIME2 NOT NULL
    );
END;
""".lstrip()

    with open(path, "w", encoding="utf-8") as f:
        f.write(sql)


def main() -> None:
    require_env()
    client = ShopifyClient()

    exported_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    extra = {
        "shop": Config.SHOPIFY_SHOP,
        "exported_at": exported_at,
    }

    rows = list_collections(client)
    
    print(f"Enriching {len(rows)} collections with product counts and vendors...")
    print(f"This may take 30-60 minutes. Progress will be saved every 50 collections.")
    sys.stdout.flush()
    
    try:
        enrich_collections(rows, client)
    except KeyboardInterrupt:
        print("\n\nInterrupted! Saving partial results...")
    except Exception as e:
        print(f"\n\nError occurred: {e}")
        print("Saving partial results...")

    # Sort by product count descending (numeric sort)
    print("Sorting collections by product count (descending)...")
    rows.sort(key=lambda r: int(r.get("product_count", "0") or "0"), reverse=True)

    xlsx_path = os.getenv("COLLECTIONS_XLSX", DEFAULT_XLSX)
    csv_path = os.getenv("COLLECTIONS_CSV", DEFAULT_CSV)
    sql_path = os.getenv("COLLECTIONS_SQL", DEFAULT_SQL)
    table_name = os.getenv("COLLECTIONS_TABLE", DEFAULT_TABLE)

    wrote_xlsx = write_xlsx(xlsx_path, rows, extra)
    if not wrote_xlsx:
        write_csv(csv_path, rows, extra)
    write_create_table_sql(sql_path, table_name)

    print(f"\nShop: {Config.SHOPIFY_SHOP}")
    print(f"API: {Config.SHOPIFY_API_VERSION}")
    print(f"Rows: {len(rows)}")
    if wrote_xlsx:
        print(f"Excel: {xlsx_path}")
    else:
        print(f"Excel failed (openpyxl missing). CSV created: {csv_path}")
    print(f"SQL: {sql_path}")


if __name__ == "__main__":
    main()

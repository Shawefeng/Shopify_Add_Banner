import os
import time
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from typing import Optional, Dict, List, Tuple, Set

import pyodbc
import requests
import json
from urllib.parse import quote_plus

# Optional: load .env automatically if python-dotenv installed
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass


"""
retail_promotions_to_shopify_metafields.py

Goal (per latest requirements):
- Banner must ALWAYS display REAL start/end dates from database
- But banner must APPEAR early:
    Sale: appear X days before real start
    Price Increase: appear Y days before real start
- And DISAPPEAR:
    Sale: disappear after real end
    Price Increase:
        if EndDate exists -> disappear after real end
        if EndDate missing -> disappear Z days after real start

Important constraints:
- Shopify stores only ONE set of dates: the REAL dates.
- We do NOT store "display window" dates in Shopify.
- Liquid controls display timing and formatting.
- Python controls data existence:
    - Write metafields when today is inside display window
    - Delete metafields when today is outside display window
"""


# =========================
# Config (ENV)
# =========================
class Config:
    # Shopify
    SHOPIFY_SHOP = os.getenv("SHOPIFY_SHOP", "").strip()
    SHOPIFY_TOKEN = os.getenv("SHOPIFY_TOKEN", "").strip()
    SHOPIFY_API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2024-01").strip()
    REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))

    # DB
    DB_SERVER = os.getenv("DB_SERVER", r"sql01-union\sql2012").strip()
    DB_NAME = os.getenv("DB_NAME", "Ecomm_DB_PROD").strip()
    DB_USER = os.getenv("DB_USER", "ssis").strip()
    DB_PASSWORD = os.getenv("DB_PASSWORD", "ssis").strip()

    # X Y Z
    # Default behavior (if env is missing): X/Y/Z = 5/15/5
    # - X (X_DAYS_BEFORE_SALE_START): number of days before a Sale start to begin writing/keeping the metafield
    # - Y (Y_DAYS_BEFORE_PI_START): number of days before a Price Increase start to begin writing/keeping the metafield
    # - Z (Z_DAYS_AFTER_PI_START): when a Price Increase has no end date, the metafield is retained until Z days after the start
    # Note: these settings only control whether the script writes/deletes Shopify metafields.
    # The front-end display/formatting of dates is handled in Shopify Liquid templates.
    SALE_PRE_DAYS = int(os.getenv("X_DAYS_BEFORE_SALE_START", os.getenv("SALE_PRE_DAYS", "5")))
    PI_PRE_DAYS = int(os.getenv("Y_DAYS_BEFORE_PI_START", os.getenv("PI_PRE_DAYS", "15")))
    PI_POST_DAYS = int(os.getenv("Z_DAYS_AFTER_PI_START", os.getenv("PI_POST_DAYS", "5")))

    # Behavior
    DRY_RUN = os.getenv("DRY_RUN", "1").strip().lower() in ("1", "true", "yes")
    DB_ONLY = os.getenv("DB_ONLY", "0").strip().lower() in ("1", "true", "yes")
    SLEEP_BETWEEN_CALLS = float(os.getenv("SLEEP_BETWEEN_CALLS", "0.12"))

    # Metafields
    MF_NAMESPACE = "custom"
    MF_SALE_START = "promo_sale_start_date"
    MF_SALE_END = "promo_sale_end_date"
    MF_PI_START = "promo_pi_start_date"
    MF_PI_END = "promo_pi_end_date"


def require_env():
    if not Config.SHOPIFY_SHOP or not Config.SHOPIFY_TOKEN:
        raise ValueError("Missing SHOPIFY_SHOP or SHOPIFY_TOKEN. Put them in .env or environment variables.")


def normalize(s: str) -> str:
    return " ".join((s or "").strip().lower().split())


def to_date_only(v) -> Optional[date]:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    if isinstance(v, str):
        s = v.strip()
        for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
            try:
                return datetime.strptime(s, fmt).date()
            except ValueError:
                pass
        try:
            return datetime.fromisoformat(s.replace("Z", "")).date()
        except Exception:
            return None
    return None


# =========================
# Data Models
# =========================
@dataclass
class RetailPromoRow:
    id: int
    vendor: str
    collection_id: Optional[str]
    entry_type: str
    start_date: date
    end_date: Optional[date]


@dataclass
class VendorPlan:
    vendor: str
    collection_ids: List[str] = None

    # Display windows (NOT written to Shopify)
    sale_display_start: Optional[date] = None
    sale_display_end: Optional[date] = None
    pi_display_start: Optional[date] = None
    pi_display_end: Optional[date] = None

    # Real dates (written to Shopify)
    sale_real_start: Optional[date] = None
    sale_real_end: Optional[date] = None
    pi_real_start: Optional[date] = None
    pi_real_end: Optional[date] = None   # Keep None if DB end is missing

    def __post_init__(self):
        if self.collection_ids is None:
            self.collection_ids = []


# =========================
# DB Access
# =========================
class DatabaseConnection:
    def __init__(self):
        self.conn = pyodbc.connect(
            f"DRIVER={{SQL Server}};"
            f"SERVER={Config.DB_SERVER};"
            f"DATABASE={Config.DB_NAME};"
            f"UID={Config.DB_USER};"
            f"PWD={Config.DB_PASSWORD}"
        )
        self.cursor = self.conn.cursor()

    def query(self, sql: str) -> List[Dict]:
        self.cursor.execute(sql)
        cols = [c[0] for c in self.cursor.description]
        return [dict(zip(cols, row)) for row in self.cursor.fetchall()]

    def close(self):
        try:
            self.cursor.close()
        finally:
            self.conn.close()


class RetailPromotionsReader:
    """
    Reads promotions that should exist today based on display windows.

    Sale window:
      show_from = StartD - X
      show_to   = EndD

    Price Increase window:
      show_from = StartD - Y
      show_to   = EndD if exists else StartD + Z
    """
    def __init__(self, db: DatabaseConnection):
        self.db = db

    def fetch_active_today(self, x: int, y: int, z: int) -> List[RetailPromoRow]:
        sql = f"""
        DECLARE @X INT = {x};
        DECLARE @Y INT = {y};
        DECLARE @Z INT = {z};

        WITH t AS (
            SELECT
                ID,
                Vendor,
                CollectionID,
                EntryType,
                TRY_CONVERT(date, Date_of_Start) AS StartD,
                TRY_CONVERT(date, Date_of_End)   AS EndD
            FROM Ecomm_DB_PROD.dbo.SM_Retail_Sales
        )
        SELECT
            ID,
            Vendor,
            CollectionID,
            EntryType,
            StartD AS Date_of_Start,
            EndD   AS Date_of_End
        FROM t
        WHERE
        (
            LTRIM(RTRIM(EntryType)) = 'Sale'
            AND StartD IS NOT NULL
            AND EndD IS NOT NULL
            AND DATEADD(day, -@X, StartD) <= CAST(GETDATE() AS date)
            AND EndD >= CAST(GETDATE() AS date)
        )
        OR
        (
            LTRIM(RTRIM(EntryType)) = 'Price Increase'
            AND StartD IS NOT NULL
            AND DATEADD(day, -@Y, StartD) <= CAST(GETDATE() AS date)
            AND COALESCE(EndD, DATEADD(day, @Z, StartD)) >= CAST(GETDATE() AS date)
        );
        """
        raw = self.db.query(sql)
        print("DEBUG fetch_active_today raw rows =", len(raw))

        rows: List[RetailPromoRow] = []
        for r in raw:
            vendor = (r.get("Vendor") or "").strip()
            raw_collection_id = r.get("CollectionID")
            collection_id = None
            if raw_collection_id is not None:
                cid = str(raw_collection_id).strip()
                if cid:
                    collection_id = cid
            entry_type = (r.get("EntryType") or "").strip()
            s = to_date_only(r.get("Date_of_Start"))
            e = to_date_only(r.get("Date_of_End"))

            if not vendor or not entry_type or not s:
                continue

            rows.append(RetailPromoRow(
                id=int(r["ID"]),
                vendor=vendor,
                collection_id=collection_id,
                entry_type=entry_type,
                start_date=s,
                end_date=e
            ))
        return rows


# =========================
# Aggregation
# =========================
def compute_display_window(row: RetailPromoRow, x: int, y: int, z: int) -> Tuple[date, date]:
    t = normalize(row.entry_type)

    if t == "sale":
        # appear X days early, disappear at real end
        return (row.start_date - timedelta(days=x)), (row.end_date or row.start_date)

    if t == "price increase":
        # appear Y days early, disappear at real end, else start + Z
        end_display = row.end_date if row.end_date else (row.start_date + timedelta(days=z))
        return (row.start_date - timedelta(days=y)), end_display

    return row.start_date, row.start_date


def aggregate_by_vendor(rows: List[RetailPromoRow], x: int, y: int, z: int) -> List[VendorPlan]:
    by_vendor: Dict[str, VendorPlan] = {}

    for r in rows:
        v = r.vendor
        w = by_vendor.get(v) or VendorPlan(vendor=v)

        if r.collection_id and r.collection_id not in w.collection_ids:
            w.collection_ids.append(r.collection_id)

        t = normalize(r.entry_type)

        d_start, d_end = compute_display_window(r, x, y, z)

        if t == "sale":
            w.sale_display_start = d_start if w.sale_display_start is None else min(w.sale_display_start, d_start)
            w.sale_display_end = d_end if w.sale_display_end is None else max(w.sale_display_end, d_end)

            w.sale_real_start = r.start_date if w.sale_real_start is None else min(w.sale_real_start, r.start_date)
            real_end = r.end_date or r.start_date
            w.sale_real_end = real_end if w.sale_real_end is None else max(w.sale_real_end, real_end)

        elif t == "price increase":
            w.pi_display_start = d_start if w.pi_display_start is None else min(w.pi_display_start, d_start)
            w.pi_display_end = d_end if w.pi_display_end is None else max(w.pi_display_end, d_end)

            w.pi_real_start = r.start_date if w.pi_real_start is None else min(w.pi_real_start, r.start_date)

            # IMPORTANT: do NOT force an end date into Shopify if DB end is missing
            # Liquid can display "Starts on" when pi_end is missing.
            if r.end_date is not None:
                w.pi_real_end = r.end_date if w.pi_real_end is None else max(w.pi_real_end, r.end_date)

        by_vendor[v] = w

    return list(by_vendor.values())


# =========================
# Shopify GraphQL Client
# =========================
class ShopifyClient:
    @staticmethod
    def to_collection_gid(collection_id: str) -> str:
        cid = (collection_id or "").strip()
        if cid.startswith("gid://"):
            return cid
        return f"gid://shopify/Collection/{cid}"

    def __init__(self):
        self.endpoint = f"https://{Config.SHOPIFY_SHOP}/admin/api/{Config.SHOPIFY_API_VERSION}/graphql.json"

    def graphql(self, query: str, variables: Optional[dict] = None, retries: int = 4) -> dict:
        headers = {
            "X-Shopify-Access-Token": Config.SHOPIFY_TOKEN,
            "Content-Type": "application/json",
        }
        payload = {"query": query, "variables": variables or {}}

        last_err = None
        for attempt in range(retries):
            try:
                resp = requests.post(self.endpoint, headers=headers, json=payload, timeout=Config.REQUEST_TIMEOUT)

                if resp.status_code in (429, 500, 502, 503, 504):
                    last_err = RuntimeError(f"Temporary Shopify error {resp.status_code}: {resp.text}")
                    time.sleep(1.2 + attempt * 1.0)
                    continue

                resp.raise_for_status()
                data = resp.json()

                if data.get("errors"):
                    raise RuntimeError(f"GraphQL errors: {data['errors']}")

                return data
            except Exception as e:
                last_err = e
                time.sleep(1.0 + attempt * 1.0)

        raise RuntimeError(f"Shopify GraphQL failed after retries: {last_err}")

    def find_collection_by_title_exact(self, title: str) -> Optional[Tuple[str, str]]:
        q = """
        query($q: String!) {
          collections(first: 20, query: $q) {
            nodes { id title }
          }
        }
        """
        target = normalize(title)

        data = self.graphql(q, {"q": f'title:"{title}"'})
        for n in data["data"]["collections"]["nodes"]:
            if normalize(n.get("title", "")) == target:
                return n["id"], n["title"]

        data2 = self.graphql(q, {"q": f"title:{title}"})
        for n in data2["data"]["collections"]["nodes"]:
            if normalize(n.get("title", "")) == target:
                return n["id"], n["title"]

        return None

    def list_product_ids_in_collection(self, collection_id: str) -> List[str]:
        ids: List[str] = []
        cursor = None
        has_next = True
        gql_collection_id = self.to_collection_gid(collection_id)

        q = """
        query($id: ID!, $cursor: String) {
          collection(id: $id) {
            products(first: 250, after: $cursor) {
              pageInfo { hasNextPage endCursor }
              nodes { id }
            }
          }
        }
        """
        while has_next:
            data = self.graphql(q, {"id": gql_collection_id, "cursor": cursor})
            collection_data = data.get("data", {}).get("collection")
            if not collection_data:
                break
            conn = collection_data["products"]
            ids.extend([n["id"] for n in conn["nodes"]])
            has_next = conn["pageInfo"]["hasNextPage"]
            cursor = conn["pageInfo"]["endCursor"]

        return ids

    def rest_count_products_in_collection(self, collection_id: str) -> int:
        # collection_id may be a GraphQL gid like 'gid://shopify/Collection/12345'
        # REST count endpoint expects the numeric id.
        numeric_id = collection_id
        try:
            if collection_id.startswith("gid://"):
                numeric_id = collection_id.rsplit("/", 1)[-1]
        except Exception:
            numeric_id = collection_id

        url = f"https://{Config.SHOPIFY_SHOP}/admin/api/{Config.SHOPIFY_API_VERSION}/products/count.json?collection_id={quote_plus(numeric_id)}"
        headers = {"X-Shopify-Access-Token": Config.SHOPIFY_TOKEN}
        try:
            resp = requests.get(url, headers=headers, timeout=Config.REQUEST_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            return int(data.get("count", 0))
        except Exception:
            return 0

    def rest_count_products_by_vendor(self, vendor: str) -> int:
        url = f"https://{Config.SHOPIFY_SHOP}/admin/api/{Config.SHOPIFY_API_VERSION}/products/count.json?vendor={quote_plus(vendor)}"
        headers = {"X-Shopify-Access-Token": Config.SHOPIFY_TOKEN}
        try:
            resp = requests.get(url, headers=headers, timeout=Config.REQUEST_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            return int(data.get("count", 0))
        except Exception:
            return 0

    def list_product_ids_by_vendor(self, vendor: str) -> List[str]:
        ids: List[str] = []
        cursor = None
        has_next = True
        target = normalize(vendor)

        q = """
        query($q: String!, $cursor: String) {
          products(first: 250, after: $cursor, query: $q) {
            pageInfo { hasNextPage endCursor }
            nodes { id vendor }
          }
        }
        """
        qstr = f'vendor:"{vendor}"'

        while has_next:
            data = self.graphql(q, {"q": qstr, "cursor": cursor})
            conn = data["data"]["products"]

            for n in conn["nodes"]:
                if normalize(n.get("vendor", "")) == target:
                    ids.append(n["id"])

            has_next = conn["pageInfo"]["hasNextPage"]
            cursor = conn["pageInfo"]["endCursor"]

        return ids

    def metafields_set(self, metafields: List[dict]) -> None:
        m = """
        mutation($m: [MetafieldsSetInput!]!) {
          metafieldsSet(metafields: $m) {
            metafields { id namespace key }
            userErrors { field message }
          }
        }
        """
        data = self.graphql(m, {"m": metafields})
        errs = data["data"]["metafieldsSet"]["userErrors"]
        if errs:
            raise RuntimeError(f"metafieldsSet userErrors: {errs}")

    def get_metafield_ids(self, product_id: str, namespace: str, keys: List[str]) -> Dict[str, Optional[str]]:
        q = """
        query($id: ID!, $namespace: String!) {
          product(id: $id) {
            metafields(first: 100, namespace: $namespace) {
              edges {
                node {
                  id
                  key
                  namespace
                }
              }
            }
          }
        }
        """
        data = self.graphql(q, {"id": product_id, "namespace": namespace})
        edges = data.get("data", {}).get("product", {}).get("metafields", {}).get("edges", []) or []

        out = {k: None for k in keys}
        for edge in edges:
            node = edge.get("node") if edge else None
            if node and node.get("key") in out:
                out[node["key"]] = node.get("id")
        return out

    def metafield_delete(self, metafield_id: str) -> None:
        m = """
        mutation($id: ID!) {
          metafieldDelete(input: {id: $id}) {
            deletedId
            userErrors { field message }
          }
        }
        """
        data = self.graphql(m, {"id": metafield_id})
        errs = data["data"]["metafieldDelete"]["userErrors"]
        if errs:
            raise RuntimeError(f"metafieldDelete userErrors: {errs}")


def build_date_metafield(owner_id: str, namespace: str, key: str, d: date) -> dict:
    return {
        "ownerId": owner_id,
        "namespace": namespace,
        "key": key,
        "type": "date",
        "value": d.isoformat(),
    }


# =========================
# Main
# =========================
def main():
    print("DB_ONLY =", Config.DB_ONLY)

    if not Config.DB_ONLY:
        require_env()

    print("=== Retail Promotions -> Shopify Metafields (GraphQL) ===")
    today = datetime.now().date()
    print(f"Today: {today}")
    print(f"SALE_PRE_DAYS (X) = {Config.SALE_PRE_DAYS}")
    print(f"PI_PRE_DAYS   (Y) = {Config.PI_PRE_DAYS}")
    print(f"PI_POST_DAYS  (Z) = {Config.PI_POST_DAYS}")
    print(f"DRY_RUN = {Config.DRY_RUN}")
    print(f"DB_NAME = {Config.DB_NAME}")
    print("")

    db = DatabaseConnection()
    try:
        reader = RetailPromotionsReader(db)
        rows = reader.fetch_active_today(Config.SALE_PRE_DAYS, Config.PI_PRE_DAYS, Config.PI_POST_DAYS)
    finally:
        db.close()

    if not rows:
        print("No active retail promotions today. Nothing to write.")
        return

    vendor_plans = aggregate_by_vendor(rows, Config.SALE_PRE_DAYS, Config.PI_PRE_DAYS, Config.PI_POST_DAYS)
    print(f"Vendors to process: {len(vendor_plans)}")
    print("")

    if Config.DB_ONLY:
        print("DB_ONLY=1 so Shopify steps are skipped.")
        for w in vendor_plans:
            print(
                f"{w.vendor} | "
                f"Sale display: {w.sale_display_start}->{w.sale_display_end} real: {w.sale_real_start}->{w.sale_real_end} | "
                f"PI display: {w.pi_display_start}->{w.pi_display_end} real: {w.pi_real_start}->{w.pi_real_end}"
            )
        return

    shop = ShopifyClient()

    vendor_results = []

    product_cache: Dict[str, int] = {}

    updated_products = 0
    deleted_metafields = 0

    for w in vendor_plans:
        vendor = w.vendor
        print(f"[Vendor] {vendor}")
        if w.collection_ids:
            print(f"  CollectionID from DB: {', '.join(w.collection_ids)}")
        else:
            print("  CollectionID from DB: (empty)")

        print(f"  Sale display: {w.sale_display_start} -> {w.sale_display_end}")
        print(f"  Sale REAL:    {w.sale_real_start} -> {w.sale_real_end}")
        print(f"  PI display:   {w.pi_display_start} -> {w.pi_display_end}")
        print(f"  PI REAL:      {w.pi_real_start} -> {w.pi_real_end}")

        sale_should_exist = (
            w.sale_display_start is not None and w.sale_display_end is not None and
            w.sale_display_start <= today <= w.sale_display_end
        )
        pi_should_exist = (
            w.pi_display_start is not None and w.pi_display_end is not None and
            w.pi_display_start <= today <= w.pi_display_end
        )

        # Product targeting priority:
        # 1) if DB has CollectionID -> use it directly
        # 2) otherwise fallback to all products by vendor
        has_collection_id = len(w.collection_ids) > 0
        cache_key = f"{vendor}::{'collection_id' if has_collection_id else 'vendor'}::{','.join(w.collection_ids)}"

        if cache_key in product_cache:
            product_count = product_cache[cache_key]
        else:
            if has_collection_id:
                if Config.DRY_RUN:
                    product_count = sum(shop.rest_count_products_in_collection(cid) for cid in w.collection_ids)
                else:
                    product_ids: Set[str] = set()
                    for cid in w.collection_ids:
                        product_ids.update(shop.list_product_ids_in_collection(cid))
                    product_count = len(product_ids)
                print("  Product scope source: CollectionID")
            else:
                print("  Product scope source: Vendor fallback (no CollectionID)")
                product_count = shop.rest_count_products_by_vendor(vendor) if Config.DRY_RUN else len(shop.list_product_ids_by_vendor(vendor))

            product_cache[cache_key] = product_count

        print(f"  Products found: {product_count}")

        # If DRY_RUN we compute per-vendor write/delete counts using product_count (no per-product requests)
        if Config.DRY_RUN:
            will_write = 0
            will_delete = 0

            # payload exists for a product if sale_should_exist with real dates OR pi_should_exist with pi_real_start
            payload_will_exist = (sale_should_exist and w.sale_real_start and w.sale_real_end) or (pi_should_exist and w.pi_real_start)
            if payload_will_exist:
                will_write = product_count

            # keys_to_delete exist for a product if not sale_should_exist OR not pi_should_exist
            keys_will_delete = (not sale_should_exist) or (not pi_should_exist)
            if keys_will_delete:
                will_delete = product_count

            print(f"  DRY_RUN SUMMARY for {vendor}: products found={product_count}, will WRITE metafields on {will_write} products, will DELETE metafields on {will_delete} products")
            vendor_results.append({
                "vendor": vendor,
                "used_collection_id": has_collection_id,
                "collection_ids": w.collection_ids,
                "products_found": product_count,
                "will_write": will_write,
                "will_delete": will_delete
            })
            # skip per-product processing in dry-run
            continue

        # Non-dry-run: perform per-product reads and safe writes/deletes using real DB dates
        print("")

        # collect product ids for this vendor (respecting CollectionID priority)
        product_ids: List[str] = []
        if has_collection_id:
            seen: Set[str] = set()
            for cid in w.collection_ids:
                ids = shop.list_product_ids_in_collection(cid)
                for pid in ids:
                    if pid not in seen:
                        seen.add(pid)
                        product_ids.append(pid)
        else:
            product_ids = shop.list_product_ids_by_vendor(vendor)

        print(f"  Processing {len(product_ids)} products for writes/deletes")

        # per-product operations
        for pid in product_ids:
            # build set payloads using REAL dates (not display window)
            to_set = []
            if sale_should_exist and w.sale_real_start and w.sale_real_end:
                to_set.append(build_date_metafield(pid, Config.MF_NAMESPACE, Config.MF_SALE_START, w.sale_real_start))
                to_set.append(build_date_metafield(pid, Config.MF_NAMESPACE, Config.MF_SALE_END, w.sale_real_end))

            if pi_should_exist and w.pi_real_start:
                to_set.append(build_date_metafield(pid, Config.MF_NAMESPACE, Config.MF_PI_START, w.pi_real_start))
                if w.pi_real_end is not None:
                    to_set.append(build_date_metafield(pid, Config.MF_NAMESPACE, Config.MF_PI_END, w.pi_real_end))

            # set metafields if any
            if to_set:
                try:
                    shop.metafields_set(to_set)
                    updated_products += 1
                except Exception as e:
                    print(f"  Failed to set metafields for {pid}: {e}")

            # determine deletions: if sale shouldn't exist -> delete sale keys; if pi shouldn't exist -> delete pi keys
            keys_to_check = []
            if not sale_should_exist:
                keys_to_check.extend([Config.MF_SALE_START, Config.MF_SALE_END])
            if not pi_should_exist:
                keys_to_check.extend([Config.MF_PI_START, Config.MF_PI_END])

            if keys_to_check:
                try:
                    existing = shop.get_metafield_ids(pid, Config.MF_NAMESPACE, keys_to_check)
                    for k, mid in existing.items():
                        if mid:
                            try:
                                shop.metafield_delete(mid)
                                deleted_metafields += 1
                            except Exception as e:
                                print(f"  Failed to delete metafield {k} ({mid}) for {pid}: {e}")
                except Exception as e:
                    print(f"  Failed to fetch metafields for {pid}: {e}")

            time.sleep(Config.SLEEP_BETWEEN_CALLS)

    print("=== Done ===")
    if Config.DRY_RUN:
        print("Dry run mode. No changes written.")
        try:
            with open("vendor_product_counts.json", "w", encoding="utf-8") as fh:
                json.dump(vendor_results, fh, ensure_ascii=False, indent=2)
            print("Wrote vendor_product_counts.json")
        except Exception as e:
            print(f"Failed to write vendor_product_counts.json: {e}")
    else:
        print(f"Total products updated: {updated_products}")
        print(f"Total metafields deleted: {deleted_metafields}")


if __name__ == "__main__":
    main()

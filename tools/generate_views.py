import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

pairs = [
    (ROOT / 'vendor_product_counts.json', ROOT / 'vendor_product_counts_view.json'),
    (ROOT / 'all_vendor_product_counts.json', ROOT / 'all_vendor_product_counts_view.json'),
]

for src, dst in pairs:
    if not src.exists():
        print(f"Source not found: {src}")
        continue

    data = json.loads(src.read_text(encoding='utf-8'))

    no_products = [e for e in data if int(e.get('products_found', 0)) == 0]
    no_collection_but_products = [e for e in data if not e.get('collection_matched') and int(e.get('products_found', 0)) > 0]
    collection_matched = [e for e in data if e.get('collection_matched')]

    key = lambda x: (x.get('vendor') or '').lower()
    no_products.sort(key=key)
    no_collection_but_products.sort(key=key)
    collection_matched.sort(key=key)

    view = {
        'no_products': no_products,
        'no_collection_but_products': no_collection_but_products,
        'collection_matched': collection_matched,
    }

    dst.write_text(json.dumps(view, ensure_ascii=False, indent=2), encoding='utf-8')
    print(f"Wrote view: {dst}")

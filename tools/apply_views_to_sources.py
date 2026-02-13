import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

mapping = [
    (ROOT / 'vendor_product_counts_view.json', ROOT / 'vendor_product_counts.json'),
    (ROOT / 'all_vendor_product_counts_view.json', ROOT / 'all_vendor_product_counts.json'),
]

for view_path, target_path in mapping:
    if not view_path.exists():
        print(f"View file not found: {view_path}")
        continue

    view = json.loads(view_path.read_text(encoding='utf-8'))

    # Concatenate groups in requested order
    concatenated = []
    for key in ('no_products', 'no_collection_but_products', 'collection_matched'):
        group = view.get(key) or []
        concatenated.extend(group)

    # Backup original target
    if target_path.exists():
        bak = target_path.with_suffix(target_path.suffix + '.bak')
        target_path.replace(bak)
        print(f"Backed up {target_path} -> {bak}")

    target_path.write_text(json.dumps(concatenated, ensure_ascii=False, indent=2), encoding='utf-8')
    print(f"Wrote grouped file: {target_path}")

import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
VIEW = ROOT / 'all_vendor_product_counts_view.json'
OUT = ROOT / 'all_vendor_product_counts_report.xlsx'

if not VIEW.exists():
    print(f"View file not found: {VIEW}")
    sys.exit(1)

with VIEW.open('r', encoding='utf-8') as f:
    view = json.load(f)

try:
    import pandas as pd
except ImportError:
    print('pandas not installed. Please run: python -m pip install pandas openpyxl')
    sys.exit(2)

sheets = {
    'No Products': view.get('no_products', []),
    'No Collection But Products': view.get('no_collection_but_products', []),
    'Collection Matched': view.get('collection_matched', []),
}

with pd.ExcelWriter(OUT, engine='openpyxl') as writer:
    for name, rows in sheets.items():
        if not rows:
            # write an empty sheet with header
            df = pd.DataFrame(columns=['vendor', 'collection_matched', 'products_found', 'will_write', 'will_delete'])
        else:
            df = pd.DataFrame(rows)
        df.to_excel(writer, sheet_name=name[:31], index=False)

print(f'Wrote Excel: {OUT}')

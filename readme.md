# SSMS -> Shopify Metafields (Banner Dates)

Purpose: write REAL promo dates from SSMS into Shopify product metafields. Banner timing is controlled by X/Y/Z.

X/Y/Z behavior (defaults to 5/15/5 if env keys are not set):
- X (X_DAYS_BEFORE_SALE_START): Sale banner appears X days before REAL sale start and disappears after REAL sale end.
- Y (Y_DAYS_BEFORE_PI_START): Price Increase banner appears Y days before REAL PI start.
- Z (Z_DAYS_AFTER_PI_START): If PI has no end date, banner disappears Z days after REAL PI start.

Shopify metafields created on products (namespace `custom`, type `date`):
- custom.promo_sale_start_date
- custom.promo_sale_end_date
- custom.promo_pi_start_date
- custom.promo_pi_end_date

Install (Windows):
python -m pip install requests pyodbc python-dotenv

Run:
python retail_promotions_to_shopify_metafields.py

.env (same folder as the script):
SHOPIFY_SHOP=xxx.myshopify.com
SHOPIFY_TOKEN=shpat_xxx
SHOPIFY_API_VERSION=2024-01
DB_SERVER=sql01-union\sql2012
DB_NAME=Ecomm_DB_PROD
DB_USER=ssis
DB_PASSWORD=ssis
X_DAYS_BEFORE_SALE_START=0
Y_DAYS_BEFORE_PI_START=0
Z_DAYS_AFTER_PI_START=0
DRY_RUN=1
DB_ONLY=0

Modes:
- DB_ONLY=1 means read SSMS only, no Shopify calls.
- DRY_RUN=1 means read and print only. DRY_RUN=0 means write and delete.

Liquid alignment (recommended):
- SALE_LEAD_DAYS = X_DAYS_BEFORE_SALE_START
- PI_LEAD_DAYS = Y_DAYS_BEFORE_PI_START
- PI_FALLBACK_DAYS = Z_DAYS_AFTER_PI_START

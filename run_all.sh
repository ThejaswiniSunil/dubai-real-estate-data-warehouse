#!/usr/bin/env bash
# Full demo: generate data -> historical load -> incremental load (+SCD2)
set -euo pipefail
cd "$(dirname "$0")"
python etl/generate_data.py
rm -f output/dubai_dw.db
python etl/etl_pipeline.py --transactions data/raw/transactions_batch1.csv --area-master data/raw/area_master_v1.csv
python etl/etl_pipeline.py --transactions data/raw/transactions_batch2.csv --area-master data/raw/area_master_v2.csv
echo "Done. Import output/powerbi_export/*.csv into Power BI (see powerbi/POWERBI_BUILD_GUIDE.md)."

from pathlib import Path
import logging
from bootcamp_data.etl import ETLConfig, run_etl


ROOT = Path(__file__).resolve().parents[1]

cfg = ETLConfig(
    root=ROOT,
    raw_orders=ROOT / "data" / "raw" / "orders.csv",
    raw_users=ROOT / "data" / "raw" / "users.csv",
    out_orders_clean=ROOT / "data" / "processed" / "orders_clean.parquet",
    out_users=ROOT / "data" / "processed" / "users.parquet",
    out_analytics=ROOT / "data" / "processed" / "analytics_table.parquet",
    run_meta=ROOT / "data" / "processed" / "_run_meta.json",
)

# ---- logging to file + console ----
log_file = cfg.root / "data" / "processed" / "etl.log"
log_file.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)

run_etl(cfg)
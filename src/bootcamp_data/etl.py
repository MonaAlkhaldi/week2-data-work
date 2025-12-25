from __future__ import annotations
import json
import logging
from dataclasses import dataclass, asdict
from pathlib import Path
import pandas as pd
from bootcamp_data.io import read_orders_csv, read_users_csv, write_parquet
from bootcamp_data.quality import require_columns, assert_non_empty, assert_unique_key
from bootcamp_data.transforms import enforce_schema, add_missing_flags,normalize_text,apply_mapping,parse_datetime,add_time_parts, winsorize, iqr_bounds
from bootcamp_data.joins import safe_left_join



log = logging.getLogger(__name__)



@dataclass(frozen=True)
class ETLConfig:
    root: Path
    raw_orders: Path
    raw_users: Path
    out_orders_clean: Path
    out_users: Path
    out_analytics: Path
    run_meta: Path



def load_inputs(cfg: ETLConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    orders = read_orders_csv(cfg.raw_orders)
    users = read_users_csv(cfg.raw_users)
    return orders, users



def transform(orders_raw: pd.DataFrame, users_raw: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
   
    require_columns(orders_raw, ["order_id", "user_id", "amount", "quantity", "created_at", "status"])
    require_columns(users_raw, ["user_id", "country", "signup_date"])
    assert_non_empty(orders_raw)
    assert_non_empty(users_raw)

  
    assert_unique_key(users_raw, "user_id")

   
    orders = enforce_schema(orders_raw)

   
    status_norm = normalize_text(orders["status"])
    mapping = {
        "paid": "paid",
        "refund": "refund",
        "refunded": "refund",
    }
    orders = orders.assign(status_clean=apply_mapping(status_norm, mapping))

   
    orders = add_missing_flags(orders, ["amount", "quantity"])

    
    orders = (
        orders
        .pipe(parse_datetime, col="created_at", utc=True)
        .pipe(add_time_parts, ts_col="created_at")
    )

    
    users = users_raw.copy()
    users["signup_date"] = pd.to_datetime(users["signup_date"], errors="coerce")

   
    n_before = len(orders)
    joined = safe_left_join(
        left=orders,
        right=users,
        on="user_id",
        validate="many_to_one",
        suffixes=("", "_user"),
    )
    assert len(joined) == n_before, "Row count changed (join explosion?)"

    
    joined = joined.assign(amount_winsor=winsorize(joined["amount"]))

    lo, hi = iqr_bounds(joined["amount"], k=1.5)
    joined = joined.assign(
        amount_is_outlier=(joined["amount"] < lo) | (joined["amount"] > hi)
    )

    
    orders_clean = orders.copy()

    return joined, orders_clean



def load_outputs(analytics: pd.DataFrame, users: pd.DataFrame, orders_clean: pd.DataFrame, cfg: ETLConfig) -> None:
    write_parquet(orders_clean, cfg.out_orders_clean)
    write_parquet(users, cfg.out_users)
    write_parquet(analytics, cfg.out_analytics)


def write_run_meta(cfg: ETLConfig, *, analytics: pd.DataFrame, orders_clean: pd.DataFrame, users: pd.DataFrame) -> None:
    missing_created_at = int(analytics["created_at"].isna().sum()) if "created_at" in analytics.columns else None
    country_match_rate = 1.0 - float(analytics["country"].isna().mean()) if "country" in analytics.columns else None

    meta = {
        "rows_orders_clean": int(len(orders_clean)),
        "rows_users": int(len(users)),
        "rows_analytics": int(len(analytics)),
        "missing_created_at": missing_created_at,
        "country_match_rate": country_match_rate,
        "config": {k: str(v) for k, v in asdict(cfg).items()},
    }

    cfg.run_meta.parent.mkdir(parents=True, exist_ok=True)
    cfg.run_meta.write_text(json.dumps(meta, indent=2), encoding="utf-8")


def run_etl(cfg: ETLConfig) -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    log.info("Loading inputs...")
    orders_raw, users_raw = load_inputs(cfg)

    log.info("Transforming (orders=%s, users=%s)...", len(orders_raw), len(users_raw))
    analytics, orders_clean = transform(orders_raw, users_raw)

    users_out = users_raw.copy()
    users_out["signup_date"] = pd.to_datetime(users_out["signup_date"], errors="coerce")

    log.info("Writing output")
    load_outputs(analytics, users_out, orders_clean, cfg)

    log.info("Writing run metadata.")
    write_run_meta(cfg, analytics=analytics, orders_clean=orders_clean, users=users_out)

    log.info("Done")

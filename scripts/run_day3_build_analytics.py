from bootcamp_data.config import make_paths
from bootcamp_data.io import read_parquet
from bootcamp_data.transforms import parse_datetime ,add_time_parts , iqr_bounds , winsorize
from bootcamp_data.quality import require_columns,assert_non_empty ,assert_in_range,assert_unique_key
from bootcamp_data.joins import safe_left_join
from pathlib import Path
import pandas as pd



paths = make_paths()

#read the files

order_c=read_parquet(paths.processed / "orders_clean.parquet")
users_pq=read_parquet(paths.processed / "users.parquet")

print(order_c.columns)
print("-----------------------------------------------------------------------------")
print(users_pq.columns)
#cheack requeird colunns 
require_columns(order_c, ["order_id","user_id","amount","quantity","created_at","status_clean" , "amount__isna" , "quantity__isna"])
require_columns(users_pq, ["user_id","country","signup_date"])
#Aseert not empty 
assert_non_empty(order_c)
assert_non_empty(users_pq)
#assert unige key 
assert_unique_key(users_pq,"user_id")



#Converting to datetime 
dt_order=parse_datetime(order_c , col = "created_at")
#print(dt_order.head())

#Add time part 
#print(add_time_parts(dt_order,"created_at").head())



# Finding the outliers
Lower_bound, Upper_bound = iqr_bounds(dt_order["amount"], 1.5)

print("IQR lower bound:", Lower_bound)
print("IQR upper bound:", Upper_bound)

# Flagging outliers
outli_order = dt_order.assign(
    is_outlier = (dt_order["amount"] < Lower_bound) |
                 (dt_order["amount"] > Upper_bound)
)
#print(outli_order.head())


winsorize(dt_order["amount"])

wiz_order=dt_order.assign(
    amount_wiz = winsorize(dt_order["amount"])
)

#missing created_at after parsing
n_missing_ts = int(dt_order["created_at"].isna().sum())
print("missing created_at after parse:", n_missing_ts, "/", len(dt_order))

print("rows before join:", len(dt_order))
joined = safe_left_join(
    left=dt_order,
    right=users_pq,
    on="user_id",
    validate="many_to_one",
    suffixes=("", "_user"),
)

assert len(joined) == len(dt_order), "Row count changed (join explosion?)"
print("joined rows:", len(joined))
print("joined columns:", joined.columns.tolist())

joined = joined.assign(amount_winsor=winsorize(joined["amount"]))

#WRITING THE FILE 
out_path = paths.processed / "analytics_table.parquet"
out_path.parent.mkdir(parents=True, exist_ok=True)
joined.to_parquet(out_path, index=False)
print("wrote:", out_path)

#task 7
summary = (
    joined.groupby("country", dropna=False)
          .agg(n=("order_id","size"), revenue=("amount","sum"))
          .reset_index().sort_values("revenue", ascending=False)
)
print(summary)



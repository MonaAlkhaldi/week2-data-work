from bootcamp_data.config import make_paths
from bootcamp_data.io import read_orders_csv, read_users_csv, write_parquet
from bootcamp_data.transforms import enforce_schema,missingness_report,add_missing_flags,normalize_text, apply_mapping
from bootcamp_data.quality import require_columns,assert_non_empty ,assert_in_range
from pathlib import Path
import pandas as pd
    

ROOT=Path(__file__).resolve().parents[1]
paths=make_paths(ROOT)

#read the files
r_order = read_orders_csv(paths.raw / "orders.csv")
r_users = read_users_csv(paths.raw /"users.csv")

#-----------check requiermint-----------------
require_columns(r_order , ["order_id" , "user_id" ,"amount" , "quantity" , "status"])
require_columns(r_users , [ "user_id" ,"country" , "signup_date"])
#-----------make sure the dataset is not empty
assert_non_empty(r_order)
assert_non_empty(r_order)

#--------------enforce-------------
order_df_enforce = enforce_schema(r_order)
#print(order_df_enforce.head())

  

#-----------------misiing repo[order]
report_order = missingness_report(order_df_enforce)#this will return a df so I will convert it to csv for the report 
# create reports 
paths.reports.mkdir(parents=True, exist_ok=True)

rep_path = paths.reports / "missingness2_orders.csv"
report_order.to_csv(rep_path, index=True)

#------------Normlize-----
#the function takes a series so when we do this order_df_enforce["status"] we are giveing a serise from the df a series is a one dim list
order_df_enforce["status_clean"]=normalize_text(order_df_enforce["status"]) #Herer i overwrite the data with the normlized one so now its normailzed 
#print(order_df_enforce.head())


#------------------adding a coulmn flag 
order_df_enforce=add_missing_flags(order_df_enforce , ["amount" , "quantity"])
print(order_df_enforce.head())


#-------Writing the orders_clean.parquet
write_parquet(order_df_enforce , paths.processed / "orders_clean.parquet")

#-----Adding assert--------------------

assert_in_range(order_df_enforce["amount"],lo=0,hi=10000)










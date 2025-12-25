from pathlib import Path 
from bootcamp_data.config import make_paths  
from bootcamp_data.io import read_users_csv ,read_orders_csv , write_parquet , read_parquet
from bootcamp_data.transforms import  enforce_schema


#First we need tp find the file root 
#resolve --- > give us the absoulote path
#parent --> retrun the parent of the file and we said 1 becouse in my structure [0] --script [1] ---root [week2-data-work]


paths = make_paths()


order_df=read_orders_csv(paths.raw / "orders.csv")
print(order_df.head(10))
user_df=read_users_csv(paths.raw / "users.csv")
order_df_enforce= enforce_schema(order_df)
write_parquet(order_df_enforce ,paths.processed / "orders.parquet")
write_parquet(user_df ,paths.processed / "users.parquet")




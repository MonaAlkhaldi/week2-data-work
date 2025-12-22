import pandas as pd 
from pandas import DataFrame

def enforce_schema(df) -> DataFrame:
    #we put assign to overwrite 
    return df.assign(
       quantity= pd.to_numeric(df["quantity"] , errors="coerce").astype("int64"),
       amount= pd.to_numeric(df["amount"] , errors="coerce").astype("Float64 "),
       order_id = df["order_id"].astype("string"),
       user_id=df["user_id"].astype("string"),

        )

     

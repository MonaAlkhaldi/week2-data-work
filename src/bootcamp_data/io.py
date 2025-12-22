import pandas as pd
from pathlib import Path
from pandas import DataFrame


#reading the csv with some enforcing 

def read_orders_csv(path)-> DataFrame :
    df =pd.read_csv(
        path,
        na_values=["","na","N/A", "null", "None"],
        dtype={"order_id" : "string" , "user_id" : "string"},
    )
    return df 

def read_users_csv(path) -> DataFrame:
    df = pd.read_csv(
        path,
        dtype={"user_id" : "string" , "country" : "string"},
        na_values=["","na","N/A", "null", "None"],

    )
    return df 

#tis functon dose not return anything beacuse it is a sideefict function it only write a file in desk
def write_parquet(df, path) -> None:
    #we want to transform the dataframe(df) into parquet 
    #Note : Before we write any file it is good prictice to cheack if the file exxist or not 
    path.parent.mkdir(parents=True , exist_ok=True)
    df.to_parquet(path ,index=False) #we use index = false to make sure that pandes do not write index instade of the columns

def read_parquet(path) -> DataFrame:
    pq=pd.read_parquet(path)
    return pq



     



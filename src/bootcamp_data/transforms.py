import pandas as pd 
from pandas import DataFrame 
from pandas import Series

def enforce_schema(df) -> DataFrame:
    #we put assign to overwrite 
    return df.assign(
       quantity= pd.to_numeric(df["quantity"] , errors="coerce").astype("Int64"),
       amount= pd.to_numeric(df["amount"] , errors="coerce").astype("Float64"),
       order_id = df["order_id"].astype("string"),
       user_id=df["user_id"].astype("string"),

        )

     
def missingness_report(df) -> DataFrame:
    num_row=len(df)
    num_miss=df.isna().sum() #this will check each row in the df and every null will be true then sum will take this true as 1 and sum the number
    prec_miss=num_miss/num_row

    report = pd.DataFrame({
    "n_missing": num_row,
    "p_missing": num_miss
})

    report = report.sort_values("p_missing", ascending=False)

def add_missing_flags(df, cols) -> DataFrame:
    d_copy=df.copy() #its best practice to make a copy the orginal data so we make sure it will not chsnge
    for c in cols:
       d_copy[f"{c}__isna"] = d_copy[c].isna()
    return d_copy


def normalize_text(s: Series) -> Series:
    return(
        s.astype("String").str.strip().str.casefold()
    )

def apply_mapping(s: Series, mapping) -> Series:
    return s.map(lambda x : mapping.get(x,x)) 






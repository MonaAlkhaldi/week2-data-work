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
    num_rows = len(df)

    n_missing = df.isna().sum()          # missing count per column
    p_missing = n_missing / num_rows     # percentage per column

    report = pd.DataFrame({
        "n_missing": n_missing,
        "p_missing": p_missing,
    })

    return report.sort_values("p_missing", ascending=False)




def add_missing_flags(df, cols) -> DataFrame:
    d_copy=df.copy() #its best practice to make a copy the orginal data so we make sure it will not chsnge
    for c in cols:
       d_copy[f"{c}__isna"] = d_copy[c].isna()
    return d_copy


def normalize_text(s: Series) -> Series:
    return (
        s.astype("string")#this function transform any type the column[s] have into StringDtype : “astype("string") forces the column into a safe, consistent text type that pandas understands.”
         .str.strip()     #even the null will be converted to <NA> Whitch is importent when cleaning
         .str.casefold()
    )

def apply_mapping(s: Series, mapping) -> Series:
    return s.map(lambda x : mapping.get(x,x)) 



def dedupe_keep_latest(df: DataFrame, key_cols: list[str], ts_col: str) -> DataFrame:
    return (
        df.sort_values(ts_col)#this sort the values based on time {dec}
          .drop_duplicates(subset=key_cols, keep="last")#this deelete the dublicat in aa spesific row and keeps only the last dup
          .reset_index(drop=True)#this mean after we droped the dup rows reset the index make it 12345... agine 
    )






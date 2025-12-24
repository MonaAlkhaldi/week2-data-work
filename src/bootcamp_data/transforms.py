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


#---------------------------------D3Task---------------------------------------------------------------
#converting a column to datetime type 
def parse_datetime(df, col, utc=True):
    
    dt = pd.to_datetime(df[col] , errors="coerce" , utc=True)
    return df.assign(**{col : dt}) #** mean that we expect a keyword pass  --> col = "name"

def add_time_parts(df, ts_col):
    ts = df[ts_col]
#the dt allows pandas to accses the datetime function similer to when we put str befoer the string function when we norm
    return df.assign(
        data = ts.dt.date,
        year=ts.dt.year,
        month = ts.dt.to_period("M").astype("string"),#astype : becoude to_perioud return a Period type and the files we use want string
        day=ts.dt.day_name(),
        hour=ts.dt.hour,

    )


#---------------------------------------Outilers Helper T2 ------------------------------------
#Finding the data bounds 
def iqr_bounds(s, k=1.5):
    s = pd.to_numeric(s, errors="coerce").dropna()
    Q1 = s.quantile(0.25)
    Q3 = s.quantile(0.75)
    IQR=Q3-Q1 
    Lower_limt=float(Q1-k*IQR)
    Upper_limt=float(Q3+k*IQR)
    return Lower_limt , Upper_limt

#Flaging outliers

def winsorize(s, lo=0.01, hi=0.99):
    s = pd.to_numeric(s, errors="coerce")
    L_limt=s.quantile(lo)
    U_Limit=s.quantile(hi)
    return s.clip(lower = L_limt , upper = U_Limit)#every value that is less the L_Limt will be changed to L_Limt
    

    










import pandas as pd

#cheack 1 : most importent 
def require_columns(df, cols): #this is a function that will cheack a list of columns that i will provide and make sure it is their 
    missing=[]
    for c in cols:
        if c not in df.columns:#columns is a built in function in pandas that return a list of coulmns 
          missing.append(c)
    assert not missing , f"Missing columns: {missing}"

#Here we want to cheack if the dataset is NOT empty
def assert_non_empty(df):#the idea is to make sure that the number of rows is > 0
   df_nRows=len(df)
   assert df_nRows > 0 , "The dataset has no rows"
#Here we check that the keys are uniqe and not null
#isna() finds missing values return True → value is missing
#notna() finds non-missing values return True → value is NOT missing

def assert_unique_key(df, key, allow_na=False):
   #cheacking if the keys are null or not 
   if not allow_na:
      assert not df[key].isna().any() , f"{key} contains NA"
    #cheacking if their is any duplicate AND makeing sure that their is no null values in the key
    #Becouse Pandas by defult consider null as duplict
   dup=df[key].duplicated(keep=False) & df[key].notna()
   assert not dup.any() , f"{key} not unige! ; {dup.sum()} rows are duplicate "

def assert_in_range(s: pd.Series, lo=None, hi=None, name: str = "value") -> None:
    x = s.dropna()
    if lo is not None:
        assert (x >= lo).all(), f"{name} below {lo}"
    if hi is not None:
        assert (x <= hi).all(), f"{name} above {hi}"


   

   

            

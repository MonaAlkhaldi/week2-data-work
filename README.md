## Setup
### After downloading the repo you should:
- python -m venv .venv

### Activate venv
- .venv\Scripts\activate
- pip install -r requirements.txt

  **IMPORTANT** Before you run anything
- Before you run, make sure you are in **week2-data-work**
- And make sure you put **$env:PYTHONPATH="src"** in PowerShell

---

## USE THIS ONLY IF YOU FACED THIS PROBLEM:
- **If the Jupyter Notebook did not find the venv you created, just do these steps:**
1. dir .venv  
2. dir .venv\Scripts\python.exe : we want to make sure that python.exe is there  
3. **If it is there**, copy the path using:  
   Resolve-Path .venv\Scripts\python.exe  
4. Now go to Jupyter, click **Ctrl + Shift + P**, then click **Enter interpreter path**, then paste the path

---

## Run ETL
**IMPORTANT**
- Before you run, make sure you are in **week2-data-work**
- And make sure you put **$env:PYTHONPATH="src"** in PowerShell
- Then run this:  
  uv run python scripts/run_elt.py

---

## Outputs
- data/processed/analytics_table.parquet
- data/processed/_run_meta.json
- reports/figures/*.png

---

## EDA
Open notebooks/eda.ipynb and run all cells.


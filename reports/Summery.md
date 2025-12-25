# Week 2 Summary — ETL + EDA

## Key findings
### For Q1 : 
- **Daily revenue increased steadily** from approximately **20.5 SAR on December 1st to 50 SAR by December 6th**, followed by a **sharp spike to 189 SAR on December 7th**, likely driven by a large or outlier order. Revenue then dropped to **0 SAR on December 8th**, suggesting missing or incomplete data.

![Daily Revenue Over Time](reports/figures/total_amount_change_daily.png)

---------------------------------------------------------------------------------------------------------------------------------------------------

### For Q2
- **Average order value varies significantly by country**.  The **UK** shows the highest average order amount (~**135 SAR**), while **Saudi Arabia (SA)** has the highest number of orders (**7**) but a lower average spend (~**42 SAR**), indicating more frequent but lower-value purchases.
- **Problematic orders (refunded or missing data) differ by country**, with high rates in **AE (~67%)** and **UK (~50%)**, a moderate rate in **SA (~43%)**, and zero reported issues in **US and EG**, likely due to small sample sizes.


![Daily Revenue Over Time](reports/figures/revenue_by_country.png)

---------------------------------------------------------------------------------------------------------------------------------------------------

### For Q3 
- **Problem order rates vary by country** with AE and UK showing the highest levels, SA at a moderate level, and US and EG showing zero issues likely due to small sample sizes. These data quality issues may affect the reliability of downstream analysis and should be addressed.

![Daily Revenue Over Time](reports/figures/problem_order_rate_heatmap.png)

---------------------------------------------------------------------------------------------------------------------------------------------------

## Definitions
- **Daily revenue** [Q1] is calculated by grouping orders by calendar day (Day) and summing the winsorized order amount (amount_winsor) for each day.
- ***Daily* revenue** [Q2] is derived by aggregating orders at the day level, where revenue is defined as the sum of winsorized order amounts and  order volume is measured by the count of order IDs.
- **Revenue** = sum of `amount_winsor` across orders.
- **Refund rate** (problem_rate) [Q3] = refunded orders divided by total orders, where a refund is defined as `status_clean == "refund"`.
- **Time window** = daily aggregation based on the `created_at` timestamp.
- 
## Descriptive statistics
### For the entier data
- The **average order amount** is approximately **62.2**, with a **maximum value of 200**, indicating the presence of large or extreme transactions.
- After winsorization, the **average order amount remains stable at 61.3**, while the maximum value is reduced to **189**, confirming effective outlier mitigation.
- The **average order quantity** is approximately **1.58**, and the low standard deviation suggests relatively stable purchasing behavior.
- All users **signed up in November 2025**, which limits cohort-based analysis to a single signup month.

## Data quality caveats
- **Missingness**: Several orders contain missing values in `amount`, `quantity`, or `created_at`..
- **Duplicates**: No duplicate user IDs were found after enforcing uniqueness on `user_id`.
- **Join coverage**: The join between orders and users preserved row counts, with a high country match rate reported in `_run_meta.json`.
- **Outliers**: Extreme order amounts are present; winsorization was applied to reduce their impact while preserving overall trends.

## Next questions
- How do **refund and missing-data rates evolve over time** 
- Are problematic orders associated with **order size, country, or user signup behavior**?

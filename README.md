WITH mx AS (
  SELECT max(business_date) AS dt 
  FROM dm_ib_dev.wealth_Insights_Customer
)
SELECT 
  count(*)                    AS total_rows,
  count(DISTINCT RCIF_NUMBER) AS distinct_rcif,
  count(*) - count(DISTINCT RCIF_NUMBER) AS duplicate_rows
FROM dm_ib_dev.wealth_Insights_Customer w
JOIN mx ON w.business_date = mx.dt

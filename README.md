WITH mx AS (
  SELECT max(business_date) dt
  FROM dm_ib_dev.wic2_wealth_fact
),
wealth_latest AS (
  SELECT distinct rcif_number
  FROM dm_ib_dev.wic2_wealth_fact w
  JOIN mx ON w.business_date = mx.dt
),
rcif_ibn AS (
  SELECT distinct
    cast(rcif_cust_nbr as string) as rcif_number,
    cust_internet_banking_nbr as ibn
  FROM eil.d_involved_party_h
  WHERE business_date = date('2025-12-31')   -- use your cust_dt from spark log if different
    AND source_system_code='CF'
    AND nvl(deceased_ind,'N')='N'
    AND cust_internet_banking_nbr is not null
),
digital_active_ibn AS (
  SELECT distinct relt_ibn as ibn
  FROM dm_ib.digital_banking_master
  WHERE ods_business_dt >= date('2025-08-01')
    AND ods_business_dt <= date('2026-01-31')
    AND (
      datediff(ods_business_dt, mob_last_login_date) <= 90
      OR datediff(ods_business_dt, olb_last_login_date) <= 90
    )
)
SELECT count(distinct w.rcif_number) as wealth_digital_active_rcif_any_ibn
FROM wealth_latest w
JOIN rcif_ibn r ON w.rcif_number = r.rcif_number
JOIN digital_active_ibn d ON r.ibn = d.ibn;

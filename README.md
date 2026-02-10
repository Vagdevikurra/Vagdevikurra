WITH max_dt AS (
  SELECT max(ods_business_dt) as ods_dt
  FROM dm_ib.digital_banking_master
  WHERE ods_business_dt >= date('2025-08-01')
    AND ods_business_dt <= date('2026-01-31')
),
digital_active_ibn AS (
  SELECT distinct dbm.relt_ibn as ibn
  FROM dm_ib.digital_banking_master dbm
  JOIN max_dt m ON dbm.ods_business_dt = m.ods_dt
  WHERE dbm.relt_ibn is not null
    AND (
      datediff(m.ods_dt, dbm.mob_last_login_date) <= 90
      OR datediff(m.ods_dt, dbm.olb_last_login_date) <= 90
    )
),
mx AS (SELECT max(business_date) dt FROM dm_ib_dev.wic2_wealth_fact),
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
  WHERE business_date = date('2025-12-31')
    AND source_system_code='CF'
    AND nvl(deceased_ind,'N')='N'
    AND cust_internet_banking_nbr is not null
)
SELECT count(distinct w.rcif_number) as wealth_digital_active_rcif_maxdt
FROM wealth_latest w
JOIN rcif_ibn r ON w.rcif_number=r.rcif_number
JOIN digital_active_ibn d ON r.ibn=d.ibn;

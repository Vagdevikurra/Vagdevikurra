# wealth_insights_2tables_simple_targets.py

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("wealth_insights_2tables_simple_targets")
    .enableHiveSupport()
    .getOrCreate()
)

START_DATE = "2025-07-01"

CUSTOMER_DIM = "dm_ib_dev.wealth_insights_customer_dim_6m1"
ACCOUNTS_FACT = "dm_ib_dev.wealth_insights_accounts_fact_6m1"

# ------------------------------------------------------------
# CUSTOMER DIM: 1 row per RCIF (target ~270k)
# ------------------------------------------------------------
customer_sql = f"""
WITH
-- 1) RCIF spine (this is your customer population driver)
rcif_spine AS (
  SELECT DISTINCT CAST(rcif_cust_nbr AS STRING) AS rcif_number
  FROM eil.m_involved_party_h
  WHERE CAST(business_date AS DATE) >= DATE '{START_DATE}'

  UNION

  SELECT DISTINCT CAST(rcif_customer_nbr AS STRING) AS rcif_number
  FROM dm_ib.digital_banking_master
  WHERE CAST(ods_business_dt AS DATE) >= DATE '{START_DATE}'
),

-- 2) Latest involved_party per RCIF (gives ip_id + cib)
ip_latest AS (
  SELECT
      CAST(ip.rcif_cust_nbr AS STRING)              AS rcif_number,
      CAST(ip.business_date AS DATE)                AS ods_business_dt,
      ip.involved_party_id                          AS ip_id,
      CAST(ip.cust_internet_banking_nbr AS STRING)  AS customer_internet_banking_number,
      CAST(ip.cust_internet_banking_nbr AS STRING)  AS rcif_customer_number,
      MAX(ip.private_client_code)                   AS private_client_code,
      MAX(ip.private_client_trust_code)             AS private_client_trust_code,
      ROW_NUMBER() OVER (
        PARTITION BY ip.rcif_cust_nbr
        ORDER BY ip.business_date DESC
      ) AS rn
  FROM eil.d_involved_party_h ip
  WHERE ip.source_system_code = 'CF'
    AND NVL(ip.deceased_ind,'N') = 'N'
    AND CAST(ip.business_date AS DATE) >= DATE '{START_DATE}'
),
ip1 AS (
  SELECT
      rcif_number, ods_business_dt, ip_id,
      customer_internet_banking_number, rcif_customer_number,
      private_client_code, private_client_trust_code
  FROM ip_latest
  WHERE rn = 1
),

-- 3) City (latest per RCIF)
addr_latest AS (
  SELECT
    CAST(ip.rcif_cust_nbr AS STRING) AS rcif_number,
    addr.city_name                   AS city_name,
    ROW_NUMBER() OVER(
      PARTITION BY ip.rcif_cust_nbr
      ORDER BY ip.business_date DESC
    ) AS rn
  FROM eil.d_involved_party_h ip
  JOIN eil.d_involved_party_address_h addr
    ON ip.involved_party_id = addr.involved_party_id
   AND ip.business_date     = addr.business_date
  WHERE ip.source_system_code = 'CF'
    AND NVL(ip.deceased_ind,'N') = 'N'
    AND CAST(ip.business_date AS DATE) >= DATE '{START_DATE}'
),
addr1 AS (
  SELECT rcif_number, city_name
  FROM addr_latest
  WHERE rn = 1
),

-- 4) Digital aggregated per reltibn (this keeps the digital universe large)
dig AS (
  SELECT
      CAST(relt_ibn AS STRING)           AS reltibn,
      MAX(olb_last_login_date)           AS lst_login_olb,
      MAX(mob_last_login_date)           AS lst_login_mob,
      MAX(CAST(ods_business_dt AS DATE)) AS dig_ods_business_dt
  FROM dm_ib.digital_banking_master
  WHERE CAST(ods_business_dt AS DATE) >= DATE '{START_DATE}'
  GROUP BY CAST(relt_ibn AS STRING)
),

-- 5) Wealth counts per RCIF (just enough for business_group/division)
wealth_counts AS (
  SELECT
      CAST(ind.rcif_cust_nbr AS STRING) AS rcif_number,
      COUNT(DISTINCT CASE WHEN ar.business_service_segment_type_code = 'IS_CT' THEN ar.arrangement_id END) AS Corporate_Trust_Count,
      COUNT(DISTINCT CASE WHEN ar.business_service_segment_type_code = 'IS_IT' THEN ar.arrangement_id END) AS Institutional_Trust_Count,
      COUNT(DISTINCT CASE WHEN ar.business_service_segment_type_code = 'REGIS_FC' THEN ar.arrangement_id END) AS Investment_Count,
      COUNT(DISTINCT CASE WHEN ar.business_service_segment_type_code = 'REGIS' THEN ar.arrangement_id END) AS Insurance_Count,
      COUNT(DISTINCT CASE WHEN ar.business_service_segment_type_code = 'PWM' THEN ar.arrangement_id END) AS PWM_Count,
      COUNT(DISTINCT CASE WHEN ar.source_system_code = 'TR' THEN ar.arrangement_id END) AS Trust_Count,
      COUNT(DISTINCT CASE WHEN ar.source_system_code IN ('DA','SV','CC','MG','LS','IM','PC','LO','BW','CH','CS','EL','IC','MA','PF','PR','SD')
                          THEN ar.arrangement_id END) AS Banking_Count
  FROM eil.d_involved_party_h ind
  JOIN eil.d_arrangement_to_involved_party_relationship_h a2i
    ON ind.involved_party_id  = a2i.involved_party_id
   AND ind.business_date      = a2i.business_date
   AND ind.source_system_code = a2i.source_system_code
  JOIN eil.d_arrangement_h ar
    ON a2i.arrangement_id                 = ar.arrangement_id
   AND a2i.arrangement_source_system_code = ar.source_system_code
   AND a2i.business_date                  = ar.business_date
  WHERE ind.source_system_code = 'CF'
    AND NVL(ind.deceased_ind,'N') = 'N'
    AND ar.closed_ind = 'N'
    AND CAST(ind.business_date AS DATE) >= DATE '{START_DATE}'
  GROUP BY CAST(ind.rcif_cust_nbr AS STRING)
),

wealth_group AS (
  SELECT
    w.rcif_number,
    CASE
      WHEN i.private_client_code IN ('039','539','339') THEN 'Private Wealth'
      WHEN i.private_client_trust_code IN ('239','739') THEN 'Private Wealth'
      WHEN (w.Corporate_Trust_Count > 0 OR w.Institutional_Trust_Count > 0) THEN 'Institutional Services'
      WHEN (w.Investment_Count > 0 OR w.Insurance_Count > 0) THEN 'Investment Services'
      ELSE 'Banking'
    END AS business_group,

    CASE
      WHEN (i.private_client_code IN ('039','539','339') OR i.private_client_trust_code IN ('239','739'))
      THEN
        CASE
          WHEN w.Trust_Count > 0 AND w.Banking_Count > 0 THEN 'Banking & TMT'
          WHEN (w.Investment_Count + w.Trust_Count) > 0 AND w.Banking_Count = 0 THEN 'Investments Only'
          ELSE 'Banking Only'
        END
      WHEN (w.Investment_Count > 0 OR w.Insurance_Count > 0)
      THEN
        CASE
          WHEN w.Investment_Count > 0 AND w.Insurance_Count = 0 THEN 'Investment'
          WHEN w.Investment_Count = 0 AND w.Insurance_Count > 0 THEN 'Insurance'
          ELSE 'Insurance & Investment'
        END
      ELSE
        CASE
          WHEN w.Corporate_Trust_Count > 0 AND w.Institutional_Trust_Count = 0 THEN 'Corporate Trust'
          WHEN w.Corporate_Trust_Count = 0 AND w.Institutional_Trust_Count > 0 THEN 'Institutional Trust'
          WHEN w.PWM_Count > 0 THEN 'Banking Only'
          ELSE 'Corporate & Institutional Trust'
        END
    END AS division
  FROM wealth_counts w
  LEFT JOIN ip1 i
    ON w.rcif_number = i.rcif_number
)

SELECT
  i.ods_business_dt AS ods_business_dt,
  d.reltibn         AS reltibn,
  i.rcif_customer_number AS rcif_customer_number,

  CASE WHEN i.ods_business_dt IS NOT NULL AND d.lst_login_mob IS NOT NULL
            AND DATEDIFF(i.ods_business_dt, d.lst_login_mob) <= 90
       THEN 'Mobile Active' ELSE 'Non Mobile Active' END AS mobile_activity_flag,
  CASE WHEN d.lst_login_mob IS NULL THEN 'Non Mobile User' ELSE 'Mobile User' END AS mobile_flag,

  CASE WHEN i.ods_business_dt IS NOT NULL AND d.lst_login_olb IS NOT NULL
            AND DATEDIFF(i.ods_business_dt, d.lst_login_olb) <= 90
       THEN 'OLB Active' ELSE 'Non OLB Active' END AS olb_active_flag,
  CASE WHEN d.lst_login_olb IS NULL THEN 'Non OLB User' ELSE 'OLB User' END AS olb_flag,

  CASE WHEN i.ods_business_dt IS NOT NULL AND (
            (d.lst_login_mob IS NOT NULL AND DATEDIFF(i.ods_business_dt, d.lst_login_mob) <= 90)
         OR (d.lst_login_olb IS NOT NULL AND DATEDIFF(i.ods_business_dt, d.lst_login_olb) <= 90)
       )
       THEN 'Digital Active' ELSE 'Non Digital Active' END AS digitally_active_flag,

  s.rcif_number AS rcif_number,
  i.ip_id       AS ip_id,
  i.customer_internet_banking_number AS customer_internet_banking_number,
  a.city_name   AS city_name,
  g.business_group AS business_group,
  g.division       AS division

FROM rcif_spine s
LEFT JOIN ip1 i
  ON s.rcif_number = i.rcif_number
LEFT JOIN dig d
  ON i.customer_internet_banking_number = d.reltibn
LEFT JOIN addr1 a
  ON s.rcif_number = a.rcif_number
LEFT JOIN wealth_group g
  ON s.rcif_number = g.rcif_number
"""

spark.sql(customer_sql).write.mode("overwrite").saveAsTable(CUSTOMER_DIM)
print(f"Saved: {CUSTOMER_DIM}")

# ------------------------------------------------------------
# ACCOUNTS FACT: keep only RCIFs in customer dim (1-*)
# Columns: rcif_number, arrangement_id, open_date, balance
# ------------------------------------------------------------
accounts_sql = f"""
WITH cust_rcifs AS (
  SELECT DISTINCT rcif_number
  FROM {CUSTOMER_DIM}
),
scoped AS (
  SELECT
      CAST(ind.rcif_cust_nbr AS STRING) AS rcif_number,
      ar.arrangement_id                 AS arrangement_id,
      CAST(ar.open_date AS DATE)        AS open_date,
      ar.current_balance_amt            AS balance,
      CAST(ind.business_date AS DATE)   AS business_date
  FROM eil.d_involved_party_h ind
  JOIN eil.d_arrangement_to_involved_party_relationship_h a2i
    ON ind.involved_party_id  = a2i.involved_party_id
   AND ind.business_date      = a2i.business_date
   AND ind.source_system_code = a2i.source_system_code
  JOIN eil.d_arrangement_h ar
    ON a2i.arrangement_id                 = ar.arrangement_id
   AND a2i.arrangement_source_system_code = ar.source_system_code
   AND a2i.business_date                  = ar.business_date
  WHERE ind.source_system_code = 'CF'
    AND NVL(ind.deceased_ind,'N') = 'N'
    AND ar.closed_ind = 'N'
    AND CAST(ind.business_date AS DATE) >= DATE '{START_DATE}'
),
latest_per_arr AS (
  SELECT
    s.*,
    ROW_NUMBER() OVER (
      PARTITION BY s.rcif_number, s.arrangement_id
      ORDER BY s.business_date DESC
    ) AS rn
  FROM scoped s
),
dedup AS (
  SELECT rcif_number, arrangement_id, open_date, balance
  FROM latest_per_arr
  WHERE rn = 1
)
SELECT d.*
FROM dedup d
JOIN cust_rcifs c
  ON d.rcif_number = c.rcif_number
"""

spark.sql(accounts_sql).write.mode("overwrite").saveAsTable(ACCOUNTS_FACT)
print(f"Saved: {ACCOUNTS_FACT}")

# ------------------------------------------------------------
# OPTIONAL: sanity checks to match your expected ranges
# (prints only, no extra tables)
# ------------------------------------------------------------
checks = [
    ("RCIF customers (~270k)", f"SELECT COUNT(DISTINCT rcif_number) AS v FROM {CUSTOMER_DIM}"),
    ("Digital reltibn distinct (~3.4m)", f"SELECT COUNT(DISTINCT relt_ibn) AS v FROM dm_ib.digital_banking_master WHERE CAST(ods_business_dt AS DATE) >= DATE '{START_DATE}'"),
    ("Digital users by RCIF (~121k)", f"SELECT COUNT(DISTINCT rcif_number) AS v FROM {CUSTOMER_DIM} WHERE customer_internet_banking_number IS NOT NULL"),
    ("Accounts distinct arrangement (~303k)", f"SELECT COUNT(DISTINCT arrangement_id) AS v FROM {ACCOUNTS_FACT}"),
    ("Invest accounts count (if you add account_type_code later)", "SELECT 'N/A' AS v")
]

for label, q in checks:
    try:
        v = spark.sql(q).collect()[0][0]
        print(f"{label}: {v}")
    except Exception as e:
        print(f"{label}: check failed -> {e}")

print("DONE.")

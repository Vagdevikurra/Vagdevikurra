#!/usr/bin/env python3
# ============================================================
# Wealth Insights ETL – FINAL CORRECT VERSION
# Builds ONLY 2 Hive tables:
#   1) wealth_insights_customer_dim
#   2) wealth_insights_accounts_fact
# ============================================================

from pyspark.sql import SparkSession

APP_NAME = "wealth_insights_etl_final"
DB = "dm_ib_dev"
DIM = f"{DB}.wealth_insights_customer_dim"
FACT = f"{DB}.wealth_insights_accounts_fact"

# ------------------------------------------------------------
# Spark
# ------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName(APP_NAME)
    .enableHiveSupport()
    .config("spark.sql.shuffle.partitions", "128")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)

spark.sql(f"USE {DB}")
spark.sparkContext.setLogLevel("WARN")

spark.sql("SET hive.exec.dynamic.partition=true")
spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict")

# ------------------------------------------------------------
# 0) Date Window – last 6 COMPLETE months
# ------------------------------------------------------------
spark.sql("""
CREATE OR REPLACE TEMP VIEW date_window AS
SELECT
  add_months(trunc(current_date(),'MM'), -6) AS start_month,
  trunc(current_date(),'MM') AS end_month
""")

# ------------------------------------------------------------
# 1) RCIF Universe
# ------------------------------------------------------------
spark.sql("""
CREATE OR REPLACE TEMP VIEW rcif_universe AS
SELECT DISTINCT CAST(rcif_number AS STRING) rcif_number
FROM (
    SELECT CAST(rcif_cust_nbr AS STRING) rcif_number
    FROM eil.m_involved_party_h
    UNION
    SELECT CAST(rcif_customer_nbr AS STRING) rcif_number
    FROM dm_ib.digital_banking_master
) x
WHERE rcif_number IS NOT NULL AND TRIM(rcif_number) <> ''
""")

# ------------------------------------------------------------
# 2) CUSTOMER DIM (latest snapshot)
# ------------------------------------------------------------
spark.sql("""
CREATE OR REPLACE TEMP VIEW rcif_latest AS
SELECT MAX(CAST(business_date AS DATE)) last_dt
FROM eil.d_involved_party_h
""")

spark.sql("""
CREATE OR REPLACE TEMP VIEW customer_dim AS
SELECT
  CAST(ip.rcif_cust_nbr AS STRING) rcif_number,
  CAST(ip.involved_party_id AS STRING) involved_party_id,
  CAST(ip.cust_internet_banking_nbr AS STRING) cust_internet_banking_nbr,
  CAST(ip.involved_party_name AS STRING) involved_party_name,
  CAST(ip.birth_date AS DATE) birth_date,
  CASE
    WHEN ip.birth_date BETWEEN DATE '1946-01-01' AND DATE '1964-12-31' THEN 'Baby Boomer'
    WHEN ip.birth_date BETWEEN DATE '1965-01-01' AND DATE '1980-12-31' THEN 'Gen X'
    WHEN ip.birth_date BETWEEN DATE '1981-01-01' AND DATE '1996-12-31' THEN 'Millennial'
    WHEN ip.birth_date >= DATE '1997-01-01' THEN 'Gen Z'
    ELSE 'Unknown'
  END AS customer_generation,
  addr.city_name,
  addr.state_name,
  addr.country_name,
  ld.last_dt AS as_of_date
FROM eil.d_involved_party_h ip
JOIN rcif_latest ld
  ON CAST(ip.business_date AS DATE) = ld.last_dt
LEFT JOIN eil.d_involved_party_address_h addr
  ON ip.involved_party_id = addr.involved_party_id
 AND ip.business_date = addr.business_date
WHERE ip.source_system_code = 'CF'
  AND NVL(ip.deceased_ind,'N') = 'N'
""")

spark.sql(f"DROP TABLE IF EXISTS {DIM}")
spark.sql(f"""
CREATE TABLE {DIM}
STORED AS PARQUET
AS
SELECT d.*
FROM customer_dim d
JOIN rcif_universe u
  ON d.rcif_number = u.rcif_number
""")

# ------------------------------------------------------------
# 3) DIGITAL (correct enrollments + activity)
# ------------------------------------------------------------
spark.sql("""
CREATE OR REPLACE TEMP VIEW digital_base AS
SELECT
  TRUNC(CAST(ods_business_dt AS DATE),'MM') month_dt,
  CAST(rcif_customer_nbr AS STRING) rcif_number,
  CAST(ibn AS STRING) ibn,
  CAST(mob_last_login_date AS DATE) mob_login,
  CAST(olb_last_login_date AS DATE) olb_login
FROM dm_ib.digital_banking_master
WHERE ods_business_dt >= add_months(trunc(current_date(),'MM'), -6)
  AND ods_business_dt < trunc(current_date(),'MM')
""")

spark.sql("""
CREATE OR REPLACE TEMP VIEW digital_customer_month AS
SELECT
  month_dt,
  rcif_number,
  COUNT(DISTINCT ibn) digital_enrollments_cnt,
  MAX(CASE WHEN datediff(month_dt, mob_login) <= 90 THEN 1 ELSE 0 END) mobile_active_ind,
  MAX(CASE WHEN datediff(month_dt, olb_login) <= 90 THEN 1 ELSE 0 END) olb_active_ind,
  MAX(
    CASE
      WHEN datediff(month_dt, mob_login) <= 90
        OR datediff(month_dt, olb_login) <= 90
      THEN 1 ELSE 0
    END
  ) digital_active_ind,
  MAX(CASE WHEN mob_login IS NOT NULL THEN 1 ELSE 0 END) mobile_user_ind,
  MAX(CASE WHEN olb_login IS NOT NULL THEN 1 ELSE 0 END) olb_user_ind
FROM digital_base
GROUP BY month_dt, rcif_number
""")

# ------------------------------------------------------------
# 4) WEALTH (business group + division)
# ------------------------------------------------------------
spark.sql("""
CREATE OR REPLACE TEMP VIEW wealth_customer_month AS
SELECT
  TRUNC(CAST(ind.business_date AS DATE),'MM') month_dt,
  CAST(ind.rcif_cust_nbr AS STRING) rcif_number,
  CASE
    WHEN ind.private_client_code IS NOT NULL THEN 'Private Wealth'
    WHEN ar.business_service_segment_type_code IN ('IS_CT','IS_IT') THEN 'Institutional Services'
    ELSE 'Investment Services'
  END AS business_group,
  COUNT(DISTINCT ar.arrangement_id) wealth_accounts_cnt,
  COUNT(DISTINCT CASE WHEN ar.business_service_segment_type_code='IS_CT' THEN ar.arrangement_id END) corporate_trust_count,
  COUNT(DISTINCT CASE WHEN ar.business_service_segment_type_code='IS_IT' THEN ar.arrangement_id END) institutional_trust_count,
  COUNT(DISTINCT CASE WHEN ar.business_service_segment_type_code='REGIS_FC' THEN ar.arrangement_id END) investment_count,
  COUNT(DISTINCT CASE WHEN ar.business_service_segment_type_code='REGIS' THEN ar.arrangement_id END) insurance_count,
  COUNT(DISTINCT CASE WHEN ar.business_service_segment_type_code='PWM' THEN ar.arrangement_id END) pwm_count,
  COUNT(DISTINCT CASE WHEN ar.source_system_code='TR' THEN ar.arrangement_id END) trust_count,
  COUNT(DISTINCT ar.arrangement_id) banking_count
FROM eil.m_involved_party_h ind
JOIN eil.m_arrangement_to_involved_party_relationship_h a2i
  ON ind.involved_party_id = a2i.involved_party_id
JOIN eil.m_arrangement_h ar
  ON a2i.arrangement_id = ar.arrangement_id
WHERE ind.source_system_code='CF'
  AND ar.closed_ind='N'
GROUP BY
  TRUNC(CAST(ind.business_date AS DATE),'MM'),
  CAST(ind.rcif_cust_nbr AS STRING),
  CASE
    WHEN ind.private_client_code IS NOT NULL THEN 'Private Wealth'
    WHEN ar.business_service_segment_type_code IN ('IS_CT','IS_IT') THEN 'Institutional Services'
    ELSE 'Investment Services'
  END
""")

# ------------------------------------------------------------
# 5) InvestPath snapshot (latest)
# ------------------------------------------------------------
spark.sql("""
CREATE OR REPLACE TEMP VIEW investpath_customer AS
SELECT
  CAST(ind.rcif_cust_nbr AS STRING) rcif_number,
  COUNT(DISTINCT ind.involved_party_id) investpath_customers_cnt,
  COUNT(DISTINCT ar.arrangement_id) investpath_accounts_cnt,
  SUM(ar.current_balance_amt) investpath_balance_amt,
  SUM(CASE WHEN ar.current_balance_amt > 0 THEN 1 ELSE 0 END) investpath_accounts_funded_cnt,
  MIN(ar.open_date) investpath_first_open_date
FROM eil.d_involved_party_h ind
JOIN eil.d_arrangement_to_involved_party_relationship_h a2i
  ON ind.involved_party_id = a2i.involved_party_id
JOIN eil.d_arrangement_h ar
  ON a2i.arrangement_id = ar.arrangement_id
WHERE ar.account_type_code='IP'
  AND ar.source_system_code='RN'
  AND ar.closed_ind='N'
GROUP BY CAST(ind.rcif_cust_nbr AS STRING)
""")

# ------------------------------------------------------------
# 6) FACT (RCIF + MONTH grain)
# ------------------------------------------------------------
spark.sql("""
CREATE OR REPLACE TEMP VIEW fact_stg AS
SELECT
  COALESCE(d.month_dt, w.month_dt) month_dt,
  COALESCE(d.rcif_number, w.rcif_number) rcif_number,

  d.digital_enrollments_cnt,
  d.digital_active_ind,
  d.mobile_active_ind,
  d.olb_active_ind,
  d.mobile_user_ind,
  d.olb_user_ind,

  w.business_group,
  w.wealth_accounts_cnt,
  w.corporate_trust_count,
  w.institutional_trust_count,
  w.investment_count,
  w.insurance_count,
  w.pwm_count,
  w.trust_count,
  w.banking_count,

  i.investpath_customers_cnt,
  i.investpath_accounts_cnt,
  i.investpath_balance_amt,
  i.investpath_accounts_funded_cnt,
  i.investpath_first_open_date
FROM digital_customer_month d
FULL OUTER JOIN wealth_customer_month w
  ON d.month_dt = w.month_dt AND d.rcif_number = w.rcif_number
LEFT JOIN investpath_customer i
  ON COALESCE(d.rcif_number, w.rcif_number) = i.rcif_number
""")

spark.sql(f"DROP TABLE IF EXISTS {FACT}")
spark.sql(f"""
CREATE TABLE {FACT}
STORED AS PARQUET
AS
SELECT *
FROM fact_stg
WHERE month_dt IS NOT NULL
""")

spark.stop()

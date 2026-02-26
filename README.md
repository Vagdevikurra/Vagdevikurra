
from pyspark.sql import SparkSession, functions as F
from pyspark.sql import Window as W
from pyspark import StorageLevel

# -----------------------
# CONFIG
# -----------------------
DEFAULT_DB = "dm_ib_dev"
EIL_DB = "eil"
DMIB_DB = "dm_ib"

START_DT = "2025-08-01"
END_DT   = "2026-01-31"

WEALTH_SRC_LIST = [
    "BI","TR","DA","SV","CC","MG","LS","TM","LO",
    "CS","IC","MA","PF","PR","SD","CM","EL","RN"
]
wealth_src_csv = ",".join([f"'{x}'" for x in WEALTH_SRC_LIST])

APPLY_PRIMARY_OWNER_FILTER = False
primary_owner_pred = "1=1"
if APPLY_PRIMARY_OWNER_FILTER:
    primary_owner_pred = "COALESCE(a2i.relationship_role,'') = 'PRIMARY'"

IP_FUNDED_BALANCE_THRESHOLD = 0.0

TARGET_WRITE_PARTITIONS = 32
MAX_RECORDS_PER_FILE = 1_000_000

# -----------------------
# SPARK
# -----------------------
spark = (
    SparkSession.builder
    .appName("Wealth_Insights_2tables_cols")
    .enableHiveSupport()
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Stability (no business logic change)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# -----------------------
# 0) ACTIVE RCIFs (from PowerQuery)
# -----------------------
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW active_rcifs AS
SELECT DISTINCT CAST(rcif_cust_nbr AS STRING) AS rcif_number
FROM {EIL_DB}.d_involved_party_h
WHERE CAST(business_date AS DATE) >= add_months(current_date(), -6)

UNION

SELECT DISTINCT CAST(rcif_customer_nbr AS STRING) AS rcif_number
FROM {DMIB_DB}.digital_banking_master
WHERE CAST(ods_business_dt AS DATE) >= add_months(current_date(), -6)
""")

# -----------------------
# 1) MONTH ENDS
# -----------------------
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW month_ends AS
WITH cal AS (
  SELECT add_months(date('{START_DT}'), n) AS month_start,
         add_months(date('{START_DT}'), n+1) AS next_month_start
  FROM (
    SELECT posexplode(
      sequence(0, CAST(months_between(date('{END_DT}'), date('{START_DT}')) AS INT))
    ) AS (n, _)
  ) t
),
all_dates AS (
  SELECT DISTINCT CAST(business_date AS DATE) AS bd FROM {EIL_DB}.d_involved_party_h
  UNION SELECT DISTINCT CAST(business_date AS DATE) FROM {EIL_DB}.d_arrangement_to_involved_party_relationship_h
  UNION SELECT DISTINCT CAST(business_date AS DATE) FROM {EIL_DB}.d_arrangement_h
)
SELECT
  m.month_start,
  MAX(a.bd) AS business_date
FROM cal m
LEFT JOIN all_dates a
  ON a.bd >= m.month_start AND a.bd < m.next_month_start
GROUP BY m.month_start
ORDER BY m.month_start
""")
spark.sql("CACHE TABLE month_ends")
spark.sql("SELECT COUNT(*) FROM month_ends").collect()

# -----------------------
# 2) WEALTH ARR -> WEALTH AGG (RCIF grain)
# -----------------------
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW wealth_arr AS
SELECT
  CAST(ind.business_date AS DATE) AS business_date,
  CAST(ind.rcif_cust_nbr AS STRING) AS rcif_number,

  ind.private_client_code,
  ind.private_client_trust_code,

  ar.arrangement_id,
  ar.source_system_code,
  ar.business_service_segment_type_code,

  concat_ws('|', ar.source_system_code, CAST(ar.arrangement_id AS STRING)) AS acct_key

FROM {EIL_DB}.d_involved_party_h ind
JOIN active_rcifs r
  ON CAST(ind.rcif_cust_nbr AS STRING) = r.rcif_number
JOIN month_ends d
  ON CAST(ind.business_date AS DATE) = d.business_date
JOIN {EIL_DB}.d_arrangement_to_involved_party_relationship_h a2i
  ON ind.involved_party_id = a2i.involved_party_id
 AND ind.business_date     = a2i.business_date
 AND ind.source_system_code = a2i.source_system_code
JOIN {EIL_DB}.d_arrangement_h ar
  ON a2i.arrangement_id = ar.arrangement_id
 AND a2i.arrangement_source_system_code = ar.source_system_code
 AND a2i.business_date = ar.business_date

WHERE ind.source_system_code = 'CF'
  AND nvl(ind.deceased_ind,'N') = 'N'
  AND ar.closed_ind = 'N'
  AND {primary_owner_pred}
  AND ar.source_system_code IN ({wealth_src_csv})
""")

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW wealth_agg AS
WITH by_rcif AS (
  SELECT
    business_date,
    rcif_number,

    COUNT(DISTINCT acct_key) AS accts_cnt,

    COUNT(DISTINCT CASE WHEN business_service_segment_type_code = 'IS_CT' THEN acct_key END) AS corporate_trust_count,
    COUNT(DISTINCT CASE WHEN business_service_segment_type_code = 'IS_IT' THEN acct_key END) AS institutional_trust_count,
    COUNT(DISTINCT CASE WHEN business_service_segment_type_code IN ('REGIS_FC','REGIS') THEN acct_key END) AS investment_count,
    COUNT(DISTINCT CASE WHEN business_service_segment_type_code = 'PWI' THEN acct_key END) AS insurance_count,
    COUNT(DISTINCT CASE WHEN business_service_segment_type_code = 'PWM' THEN acct_key END) AS pwm_count,

    MAX(CASE WHEN private_client_code IN ('039','539','339')
              OR private_client_trust_code IN ('239','739')
             THEN 1 ELSE 0 END) AS private_flag

  FROM wealth_arr
  GROUP BY business_date, rcif_number
)
SELECT
  business_date,
  rcif_number,

  CASE
    WHEN private_flag = 1 THEN 'Private Wealth'
    WHEN (corporate_trust_count + institutional_trust_count) > 0 THEN 'Institutional Services'
    WHEN (investment_count + insurance_count) > 0 THEN 'Investment Services'
    WHEN pwm_count > 0 THEN 'Private Wealth'
    ELSE 'Other'
  END AS business_group,

  CASE
    WHEN private_flag = 1 THEN 'Private Wealth'
    WHEN (corporate_trust_count > 0 AND institutional_trust_count = 0) THEN 'Corporate Trust'
    WHEN (corporate_trust_count = 0 AND institutional_trust_count > 0) THEN 'Institutional Trust'
    WHEN pwm_count > 0 THEN 'Banking Only'
    ELSE 'Corporate & Institutional Trust'
  END AS division,

  accts_cnt
FROM by_rcif
""")

# -----------------------
# 3) INVESTPATH monthly as-of -> RCIF grain (as columns)
# -----------------------
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW investpath_agg AS
WITH anchors AS (
  SELECT business_date AS anchor_dt, TRUNC(business_date,'MM') AS month_start
  FROM month_ends
),

ind_snap AS (
  SELECT a.anchor_dt, MAX(CAST(ind.business_date AS DATE)) AS ind_bd
  FROM anchors a
  JOIN {EIL_DB}.d_involved_party_h ind
    ON CAST(ind.business_date AS DATE) >= a.month_start
   AND CAST(ind.business_date AS DATE) <= a.anchor_dt
  WHERE ind.source_system_code = 'CF'
  GROUP BY a.anchor_dt
),

a2i_snap AS (
  SELECT a.anchor_dt, MAX(CAST(a2i.business_date AS DATE)) AS a2i_bd
  FROM anchors a
  JOIN {EIL_DB}.d_arrangement_to_involved_party_relationship_h a2i
    ON CAST(a2i.business_date AS DATE) >= a.month_start
   AND CAST(a2i.business_date AS DATE) <= a.anchor_dt
  GROUP BY a.anchor_dt
),

ar_snap AS (
  SELECT a.anchor_dt, MAX(CAST(ar.business_date AS DATE)) AS ar_bd
  FROM anchors a
  JOIN {EIL_DB}.d_arrangement_h ar
    ON CAST(ar.business_date AS DATE) >= a.month_start
   AND CAST(ar.business_date AS DATE) <= a.anchor_dt
  WHERE ar.source_system_code = 'RN'
    AND ar.account_type_code = 'IP'
    AND ar.closed_ind = 'N'
  GROUP BY a.anchor_dt
),

ind_at AS (
  SELECT
    s.anchor_dt AS business_date,
    ind.involved_party_id AS ip_id,
    CAST(ind.rcif_cust_nbr AS STRING) AS rcif_number
  FROM ind_snap s
  JOIN {EIL_DB}.d_involved_party_h ind
    ON CAST(ind.business_date AS DATE) = s.ind_bd
  JOIN active_rcifs r
    ON CAST(ind.rcif_cust_nbr AS STRING) = r.rcif_number
  WHERE ind.source_system_code = 'CF'
    AND nvl(ind.deceased_ind,'N') = 'N'
),

a2i_at AS (
  SELECT
    s.anchor_dt AS business_date,
    a2i.involved_party_id AS ip_id,
    a2i.arrangement_id,
    a2i.arrangement_source_system_code AS arr_src
  FROM a2i_snap s
  JOIN {EIL_DB}.d_arrangement_to_involved_party_relationship_h a2i
    ON CAST(a2i.business_date AS DATE) = s.a2i_bd
),

ar_at AS (
  SELECT
    s.anchor_dt AS business_date,
    ar.arrangement_id,
    ar.source_system_code,
    CAST(COALESCE(ar.current_balance_amt,0.0) AS DOUBLE) AS ip_balance
  FROM ar_snap s
  JOIN {EIL_DB}.d_arrangement_h ar
    ON CAST(ar.business_date AS DATE) = s.ar_bd
  WHERE ar.source_system_code = 'RN'
    AND ar.account_type_code = 'IP'
    AND ar.closed_ind = 'N'
),

joined AS (
  SELECT
    i.business_date,
    i.rcif_number,
    concat_ws('|', a.arr_src, CAST(a.arrangement_id AS STRING)) AS ip_account_id,
    ar.ip_balance
  FROM ind_at i
  JOIN a2i_at a
    ON a.business_date = i.business_date AND a.ip_id = i.ip_id
  JOIN ar_at ar
    ON ar.business_date = i.business_date
   AND ar.arrangement_id = a.arrangement_id
   AND ar.source_system_code = a.arr_src
  WHERE {primary_owner_pred}
)

SELECT
  business_date,
  rcif_number,
  COUNT(DISTINCT ip_account_id) AS ip_accounts_cnt,
  SUM(ip_balance) AS ip_aum,
  COUNT(DISTINCT CASE WHEN ip_balance > {IP_FUNDED_BALANCE_THRESHOLD} THEN ip_account_id END) AS ip_funded_accounts_cnt,
  CASE WHEN COUNT(DISTINCT ip_account_id) > 0 THEN 1 ELSE 0 END AS ip_customers_flag
FROM joined
GROUP BY business_date, rcif_number
""")

# -----------------------
# 4) FINAL FACT (one row per RCIF per month)
# -----------------------
fact_df = (
    spark.table("wealth_agg").alias("w")
    .join(
        spark.table("investpath_agg").alias("ip"),
        on=[F.col("w.business_date") == F.col("ip.business_date"),
            F.col("w.rcif_number") == F.col("ip.rcif_number")],
        how="left"
    )
    .select(
        F.col("w.business_date").alias("business_date"),
        F.col("w.rcif_number").alias("rcif_number"),
        F.col("w.business_group").alias("business_group"),
        F.col("w.division").alias("division"),
        F.col("w.accts_cnt").alias("accts_cnt"),

        F.coalesce(F.col("ip.ip_accounts_cnt"), F.lit(0)).cast("long").alias("ip_accounts_cnt"),
        F.coalesce(F.col("ip.ip_aum"), F.lit(0.0)).cast("double").alias("ip_aum"),
        F.coalesce(F.col("ip.ip_funded_accounts_cnt"), F.lit(0)).cast("long").alias("ip_funded_accounts_cnt"),
        F.coalesce(F.col("ip.ip_customers_flag"), F.lit(0)).cast("int").alias("ip_customers_flag"),
    )
)

# HARD SAFETY: enforce grain uniqueness (fix duplicates forever)
wdu = W.partitionBy("business_date", "rcif_number").orderBy(
    F.col("accts_cnt").desc_nulls_last(),
    F.col("ip_accounts_cnt").desc_nulls_last()
)
fact_df = (fact_df.withColumn("_rn", F.row_number().over(wdu))
                  .filter(F.col("_rn") == 1)
                  .drop("_rn"))

# -----------------------
# 5) CUSTOMER DIM (RCIF grain, latest)
# -----------------------
cust_dt = spark.sql(f"""
SELECT MAX(CAST(business_date AS DATE)) AS dt
FROM {EIL_DB}.d_involved_party_h
WHERE source_system_code = 'CF'
""").collect()[0]["dt"]

addr_dt = spark.sql(f"""
SELECT MAX(CAST(business_date AS DATE)) AS dt
FROM {EIL_DB}.d_involved_party_address_h
""").collect()[0]["dt"]

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW wia2_customer_new AS
WITH ip AS (
  SELECT
    involved_party_id AS ip_id,
    CAST(rcif_cust_nbr AS STRING) AS rcif_number,
    cust_internet_banking_nbr AS ibn,
    birth_date
  FROM {EIL_DB}.d_involved_party_h
  WHERE CAST(business_date AS DATE) = date('{cust_dt}')
    AND source_system_code = 'CF'
    AND nvl(deceased_ind,'N')='N'
),
addr_ranked AS (
  SELECT
    involved_party_id AS ip_id,
    state_name,
    ROW_NUMBER() OVER(PARTITION BY involved_party_id ORDER BY nvl(state_name,'') DESC) AS rn
  FROM {EIL_DB}.d_involved_party_address_h
  WHERE CAST(business_date AS DATE) = date('{addr_dt}')
)
SELECT
  ip.rcif_number,
  MAX(ip.ibn) AS primary_ibn,
  MAX(a.state_name) AS state_name,
  MAX(ip.birth_date) AS birth_date
FROM ip
LEFT JOIN addr_ranked a
  ON ip.ip_id = a.ip_id AND a.rn = 1
GROUP BY ip.rcif_number
""")

cust_df = spark.table("wia2_customer_new")

# -----------------------
# 6) WRITE ONLY TWO TABLES
# -----------------------
# FACT
out_fact = fact_df.persist(StorageLevel.MEMORY_AND_DISK)
_ = out_fact.count()  # materialize

out_fact = out_fact.repartition(600, "business_date", "rcif_number").coalesce(TARGET_WRITE_PARTITIONS)

spark.sql(f"DROP TABLE IF EXISTS {DEFAULT_DB}.wic2_wealth_fact")
(out_fact.write
 .mode("overwrite")
 .option("maxRecordsPerFile", MAX_RECORDS_PER_FILE)
 .saveAsTable(f"{DEFAULT_DB}.wic2_wealth_fact")
)

# DIM
out_cust = cust_df.persist(StorageLevel.MEMORY_AND_DISK)
_ = out_cust.count()

out_cust = out_cust.coalesce(TARGET_WRITE_PARTITIONS)

spark.sql(f"DROP TABLE IF EXISTS {DEFAULT_DB}.wia2_customer")
(out_cust.write
 .mode("overwrite")
 .option("maxRecordsPerFile", MAX_RECORDS_PER_FILE)
 .saveAsTable(f"{DEFAULT_DB}.wia2_customer")
)

print(f"Saved {DEFAULT_DB}.wic2_wealth_fact")
print(f"Saved {DEFAULT_DB}.wia2_customer")
print("DONE.")

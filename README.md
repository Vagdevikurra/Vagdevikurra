from pyspark.sql import SparkSession, functions as F

# =========================
# CONFIG
# =========================
DEFAULT_DB = "dm_ib_dev"
EIL_DB = "eil"
DMIB_DB = "dm_ib"

START_DT = "2025-08-01"
END_DT   = "2026-01-31"   # just for digital window filtering

WEALTH_SRC_LIST = (
    "'BI','BN','BW','TR','DA','SV','CC','MG','LS','TM','PC','LO','BM','CS','IC','MA','PF','PR','SD','CN','EL','RN'"
)

spark = (
    SparkSession.builder
    .appName("wealth_digital_rebuild_FINAL")
    .enableHiveSupport()
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("="*120)
print("REBUILD FINAL: Wealth + Digital + Customer (with validations)")
print(f"Window: {START_DT} to {END_DT}")
print("="*120)

# -------------------------------------------------------------------------------------
# 0) Pick latest available CF snapshot for customer (this fixes empty customer build)
# -------------------------------------------------------------------------------------
cust_dt = spark.sql(f"""
SELECT max(business_date) as dt
FROM {EIL_DB}.d_involved_party_h
WHERE source_system_code='CF'
""").collect()[0]["dt"]

addr_dt = spark.sql(f"""
SELECT max(business_date) as dt
FROM {EIL_DB}.d_involved_party_address_h
""").collect()[0]["dt"]

print(f"[INFO] customer snapshot date (d_involved_party_h CF): {cust_dt}")
print(f"[INFO] address snapshot date (d_involved_party_address_h): {addr_dt}")

# -------------------------------------------------------------------------------------
# 1) Month-ends (robust last date per month from m_involved_party_h)
# -------------------------------------------------------------------------------------
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW month_ends AS
WITH m AS (
  SELECT business_date, year(business_date) yy, month(business_date) mm
  FROM {EIL_DB}.m_involved_party_h
  WHERE cast(business_date as date) >= date('{START_DT}')
    AND cast(business_date as date) <= date('{END_DT}')
)
SELECT max(business_date) as business_date
FROM m
GROUP BY yy, mm
""")

print("\n[0] month_ends:")
spark.sql("SELECT * FROM month_ends ORDER BY business_date").show(truncate=False)

# -------------------------------------------------------------------------------------
# 2) WEALTH base join
# -------------------------------------------------------------------------------------
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW wealth_base AS
SELECT
  cast(ind.business_date as date)           as business_date,
  ind.involved_party_id                     as ip_id,
  cast(ind.rcif_cust_nbr as string)         as rcif_number,
  ind.cust_internet_banking_nbr             as ibn,

  CASE
    WHEN ind.private_client_code in ('039','539','339') THEN 'Private Wealth'
    WHEN ind.private_client_trust_code in ('239','739') THEN 'Private Wealth'
    ELSE CASE
      WHEN ar.business_service_segment_type_code in ('IS_CT','IS_IT') THEN 'Institutional Services'
      WHEN ar.business_service_segment_type_code in ('REGIS_FC','REGIS') THEN 'Investment Services'
      WHEN ar.business_service_segment_type_code = 'PWM' THEN 'Private Wealth'
      ELSE concat(ar.business_service_segment_type_code, 'Category2??')
    END
  END as business_group,

  ar.arrangement_id,
  ar.source_system_code,
  ar.business_service_segment_type_code,

  ind.private_client_code,
  ind.private_client_trust_code

FROM {EIL_DB}.m_involved_party_h ind
JOIN month_ends d
  ON ind.business_date = d.business_date

JOIN {EIL_DB}.d_arrangement_to_involved_party_relationship_h a2i
  ON ind.involved_party_id = a2i.involved_party_id
 AND ind.business_date = a2i.business_date
 AND ind.source_system_code = a2i.source_system_code

JOIN {EIL_DB}.d_arrangement_h ar
  ON a2i.arrangement_id = ar.arrangement_id
 AND a2i.arrangement_source_system_code = ar.source_system_code
 AND a2i.business_date = ar.business_date

WHERE ind.source_system_code = 'CF'
  AND nvl(ind.deceased_ind,'N') = 'N'
  AND ar.closed_ind = 'N'
  AND ar.source_system_code in ({WEALTH_SRC_LIST})

  AND (
    CASE
      WHEN ind.private_client_code in ('039','539','339') THEN 1
      WHEN ind.private_client_trust_code in ('239','739') THEN 1
      ELSE CASE
        WHEN ar.business_service_segment_type_code in ('IS_CT','IS_IT','REGIS_FC','REGIS','PWM') THEN 1
        ELSE 0
      END
    END
  ) = 1
""")

# -------------------------------------------------------------------------------------
# 3) WEALTH aggregate (FIX accounts inflation using composite key)
#    accts_cnt = count distinct concat(source_system_code, arrangement_id)
# -------------------------------------------------------------------------------------
spark.sql("""
CREATE OR REPLACE TEMP VIEW wealth_agg AS
SELECT
  business_date,
  rcif_number,
  business_group,

  count(distinct case when business_service_segment_type_code='IS_CT' then concat_ws('|',source_system_code,arrangement_id) end) as corporate_trust_count,
  count(distinct case when business_service_segment_type_code='IS_IT' then concat_ws('|',source_system_code,arrangement_id) end) as institutional_trust_count,
  count(distinct case when business_service_segment_type_code='REGIS_FC' then concat_ws('|',source_system_code,arrangement_id) end) as investment_count,
  count(distinct case when business_service_segment_type_code='REGIS' then concat_ws('|',source_system_code,arrangement_id) end) as insurance_count,
  count(distinct case when business_service_segment_type_code='PWM' then concat_ws('|',source_system_code,arrangement_id) end) as pwm_count,

  count(distinct case when source_system_code='TR' then concat_ws('|',source_system_code,arrangement_id) end) as trust_count,
  count(distinct case when source_system_code in ('DA','SV','CC','MG','LS','TM','PC','LO','BM','CS','IC','MA','PF','PR','SD') then concat_ws('|',source_system_code,arrangement_id) end) as banking_count,

  count(distinct concat_ws('|',source_system_code,arrangement_id)) as accts_cnt
FROM wealth_base
GROUP BY business_date, rcif_number, business_group
""")

wealth_fact_df = (
    spark.table("wealth_agg")
    .withColumn(
        "division",
        F.when(F.col("business_group") == "Private Wealth",
            F.when((F.col("trust_count") > 0) & (F.col("banking_count") > 0), "Banking & IMAT")
             .when(((F.col("investment_count") + F.col("trust_count")) > 0) & (F.col("banking_count") == 0), "Investments Only")
             .otherwise("Banking only")
        )
        .when(F.col("business_group") == "Investment Services",
            F.when((F.col("investment_count") > 0) & (F.col("insurance_count") == 0), "Investment")
             .when((F.col("investment_count") > 0) & (F.col("insurance_count") > 0), "Insurance")
             .otherwise("Insurance & Investment")
        )
        .when((F.col("corporate_trust_count") > 0) & (F.col("institutional_trust_count") == 0), "Corporate Trust")
        .when((F.col("corporate_trust_count") == 0) & (F.col("institutional_trust_count") > 0), "Institutional Trust")
        .when(F.col("pwm_count") > 0, "Banking only")
        .otherwise("Corporate & Institutional Trust")
    )
    .select("business_date","rcif_number","business_group","division","accts_cnt")
)

wealth_fact_df.createOrReplaceTempView("wic2_wealth_fact")

# -------------------------------------------------------------------------------------
# 4) DIGITAL (monthly grain like your screenshot)
# -------------------------------------------------------------------------------------
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW digital_monthly AS
WITH dig_customer AS (
  SELECT
    trunc(ods_business_dt, 'MM') as month_dt,
    relt_ibn as ibn,
    cast(rcif_customer_nbr as string) as rcif_number,
    max(olb_last_login_date) as last_olb,
    max(mob_last_login_date) as last_mob,
    max(ods_business_dt) as ods_dt
  FROM {DMIB_DB}.digital_banking_master
  WHERE ods_business_dt >= date('{START_DT}')
    AND ods_business_dt <= date('{END_DT}')
  GROUP BY trunc(ods_business_dt,'MM'), relt_ibn, cast(rcif_customer_nbr as string)
)
SELECT
  month_dt,
  ibn,
  rcif_number,
  ods_dt,
  case when last_mob is null then 'Non Mobile User' else 'Mobile User' end as mobile_flag,
  case when last_olb is null then 'Non OLB User' else 'OLB User' end as olb_flag,
  case when datediff(ods_dt,last_mob) <= 90 then 'Mobile Active' else 'Non Mobile Active' end as mobile_active_flag,
  case when datediff(ods_dt,last_olb) <= 90 then 'OLB Active' else 'Non OLB Active' end as olb_active_flag,
  case
    when (datediff(ods_dt,last_mob) <= 90) OR (datediff(ods_dt,last_olb) <= 90)
      then 'Digital Active'
    else 'Non Digital Active'
  end as digitally_active_flag,
  case when ibn is null then 'Non Digital User' else 'Digital User' end as digital_flag
FROM dig_customer
WHERE ibn is not null
""")

# -------------------------------------------------------------------------------------
# 5) CUSTOMER snapshot (this was empty before — now uses cust_dt and addr_dt)
# -------------------------------------------------------------------------------------
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW rcif_customer AS
WITH ip AS (
  SELECT
    involved_party_id as ip_id,
    cast(rcif_cust_nbr as string) as rcif_number,
    cust_internet_banking_nbr as ibn,
    birth_date
  FROM {EIL_DB}.d_involved_party_h
  WHERE business_date = date('{cust_dt}')
    AND source_system_code = 'CF'
    AND nvl(deceased_ind,'N') = 'N'
    AND birth_date is not null
),
addr_ranked AS (
  SELECT
    involved_party_id as ip_id,
    state_name,
    row_number() over (PARTITION BY involved_party_id ORDER BY nvl(state_name,'') desc) as rn
  FROM {EIL_DB}.d_involved_party_address_h
  WHERE business_date = date('{addr_dt}')
)
SELECT
  ip.rcif_number,
  ip.ibn,
  a.state_name
FROM ip
LEFT JOIN addr_ranked a
  ON ip.ip_id = a.ip_id AND a.rn = 1
""")

# -------------------------------------------------------------------------------------
# 6) Build FINAL customer table at RCIF grain (non-zero guaranteed)
# -------------------------------------------------------------------------------------
spark.sql("""
CREATE OR REPLACE TEMP VIEW wia2_customer AS
WITH joined AS (
  SELECT
    c.rcif_number,
    max(c.state_name) as state_name,
    max(c.ibn)        as primary_ibn,

    max(case when d.digital_flag='Digital User' then 1 else 0 end) as digital_user_bin,
    max(case when d.digitally_active_flag='Digital Active' then 1 else 0 end) as digital_active_bin,

    max(case when d.mobile_flag='Mobile User' then 1 else 0 end) as mobile_user_bin,
    max(case when d.mobile_active_flag='Mobile Active' then 1 else 0 end) as mobile_active_bin,

    max(case when d.olb_flag='OLB User' then 1 else 0 end) as olb_user_bin,
    max(case when d.olb_active_flag='OLB Active' then 1 else 0 end) as olb_active_bin
  FROM rcif_customer c
  LEFT JOIN digital_monthly d
    ON c.ibn = d.ibn
  GROUP BY c.rcif_number
)
SELECT
  rcif_number,
  state_name,
  primary_ibn,
  case when digital_user_bin=1 then 'Digital User' else 'Non Digital User' end as digital_flag,
  case when digital_active_bin=1 then 'Digital Active' else 'Non Digital Active' end as digitally_active_flag,
  case when mobile_user_bin=1 then 'Mobile User' else 'Non Mobile User' end as mobile_flag,
  case when mobile_active_bin=1 then 'Mobile Active' else 'Non Mobile Active' end as mobile_active_flag,
  case when olb_user_bin=1 then 'OLB User' else 'Non OLB User' end as olb_flag,
  case when olb_active_bin=1 then 'OLB Active' else 'Non OLB Active' end as olb_active_flag
FROM joined
""")

# -------------------------------------------------------------------------------------
# 7) VALIDATIONS (fail fast if customer table is empty)
# -------------------------------------------------------------------------------------
print("\n" + "="*120)
print("VALIDATION")
print("="*120)

wealth_latest = spark.sql("""
WITH mx AS (SELECT max(business_date) as dt FROM wic2_wealth_fact)
SELECT
  (SELECT max(business_date) FROM wic2_wealth_fact) as latest_dt,
  count(distinct w.rcif_number) as wealth_rcifs_latest,
  sum(w.accts_cnt) as wealth_accounts_latest
FROM wic2_wealth_fact w
JOIN mx ON w.business_date = mx.dt
""")
wealth_latest.show(truncate=False)

cust_cnt = spark.sql("""
SELECT
  count(*) as rows,
  count(distinct rcif_number) as rcifs,
  count(distinct primary_ibn) as ibns
FROM wia2_customer
""").collect()[0]

print(f"[CHECK] wia2_customer rows={cust_cnt['rows']}, rcifs={cust_cnt['rcifs']}, ibns={cust_cnt['ibns']}")
if cust_cnt["rows"] == 0:
    raise RuntimeError("wia2_customer is EMPTY. Stopping. Customer snapshot/join failed.")

spark.sql("""
SELECT digitally_active_flag, count(*) as cnt
FROM wia2_customer
GROUP BY digitally_active_flag
ORDER BY cnt DESC
""").show(truncate=False)

spark.sql("""
SELECT count(distinct ibn) as digital_active_ibn_window
FROM digital_monthly
WHERE digitally_active_flag='Digital Active'
""").show(truncate=False)

spark.sql("""
WITH mx AS (SELECT max(business_date) as dt FROM wic2_wealth_fact),
wealth_latest AS (
  SELECT distinct rcif_number
  FROM wic2_wealth_fact w
  JOIN mx ON w.business_date = mx.dt
)
SELECT count(*) as wealth_digital_active_rcif
FROM wealth_latest w
JOIN wia2_customer c
  ON w.rcif_number = c.rcif_number
WHERE c.digitally_active_flag='Digital Active'
""").show(truncate=False)

# -------------------------------------------------------------------------------------
# 8) WRITE TABLES (overwrite)
# -------------------------------------------------------------------------------------
print("\nWriting tables...")

spark.table("wic2_wealth_fact").write.mode("overwrite").saveAsTable(f"{DEFAULT_DB}.wic2_wealth_fact")
spark.table("wia2_customer").write.mode("overwrite").saveAsTable(f"{DEFAULT_DB}.wia2_customer")

print(f"✓ Saved {DEFAULT_DB}.wic2_wealth_fact")
print(f"✓ Saved {DEFAULT_DB}.wia2_customer")
print("DONE.")
print("="*120)

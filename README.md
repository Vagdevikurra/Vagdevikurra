from pyspark.sql import SparkSession, functions as F

# =========================
# CONFIG
# =========================
DEFAULT_DB = "dm_ib_dev"
EIL_DB = "eil"
DMIB_DB = "dm_ib"

START_DT = "2025-08-01"
END_DT   = "2026-01-31"   # window end (used for filtering source tables)

# Wealth allowed source systems (from your screenshots)
WEALTH_SRC_LIST = (
    "'BI','BN','BW','TR','DA','SV','CC','MG','LS','TM','PC','LO','BM','CS','IC','MA','PF','PR','SD','CN','EL','RN','IS_CT','IS_IT','PWM'"
)

spark = (
    SparkSession.builder
    .appName("wealth_digital_FINAL_STABLE")
    .enableHiveSupport()
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("="*120)
print("FINAL STABLE BUILD (no latest=0, no view dependency)")
print(f"Window: {START_DT} to {END_DT}")
print("="*120)

# =====================================================================================
# 0) Month-end dates within window (robust: last available per month)
# =====================================================================================
month_ends_df = spark.sql(f"""
WITH m AS (
  SELECT business_date,
         year(business_date) yy,
         month(business_date) mm
  FROM {EIL_DB}.m_involved_party_h
  WHERE cast(business_date as date) >= date('{START_DT}')
    AND cast(business_date as date) <= date('{END_DT}')
)
SELECT max(business_date) as business_date
FROM m
GROUP BY yy, mm
""")

month_ends_df.createOrReplaceTempView("month_ends")
print("\n[0] month_ends:")
spark.sql("SELECT * FROM month_ends ORDER BY business_date").show(truncate=False)

# =====================================================================================
# 1) Wealth base (join + filters)
# =====================================================================================
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

# =====================================================================================
# 2) Wealth aggregate at correct grain + division
# =====================================================================================
spark.sql("""
CREATE OR REPLACE TEMP VIEW wealth_agg AS
SELECT
  business_date,
  ip_id,
  rcif_number,
  ibn,
  business_group,

  count(distinct case when business_service_segment_type_code='IS_CT' then arrangement_id end) as corporate_trust_count,
  count(distinct case when business_service_segment_type_code='IS_IT' then arrangement_id end) as institutional_trust_count,
  count(distinct case when business_service_segment_type_code='REGIS_FC' then arrangement_id end) as investment_count,
  count(distinct case when business_service_segment_type_code='REGIS' then arrangement_id end) as insurance_count,
  count(distinct case when business_service_segment_type_code='PWM' then arrangement_id end) as pwm_count,

  count(distinct case when source_system_code='TR' then arrangement_id end) as trust_count,
  count(distinct case when source_system_code in ('DA','SV','CC','MG','LS','TM','PC','LO','BM','CS','IC','MA','PF','PR','SD') then arrangement_id end) as banking_count,

  count(distinct arrangement_id) as accts_cnt
FROM wealth_base
GROUP BY business_date, ip_id, rcif_number, ibn, business_group
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

# Persist to avoid recompute & reduce instability
spark.table("wic2_wealth_fact").persist()
spark.table("wic2_wealth_fact").count()

# =====================================================================================
# 3) DIGITAL at correct grain: 1 row per IBN for the window
# =====================================================================================
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW digital_ibn AS
WITH dig AS (
  SELECT
    relt_ibn,
    max(ods_business_dt) as ods_dt,
    max(olb_last_login_date) as last_olb,
    max(mob_last_login_date) as last_mob
  FROM {DMIB_DB}.digital_banking_master
  WHERE ods_business_dt >= date('{START_DT}')
    AND ods_business_dt <= date('{END_DT}')
  GROUP BY relt_ibn
)
SELECT
  relt_ibn as ibn,
  ods_dt,
  last_olb,
  last_mob,
  case when last_mob is not null then 'Mobile User' else 'Non Mobile User' end as mobile_flag,
  case when last_olb is not null then 'OLB User' else 'Non OLB User' end as olb_flag,
  case when datediff(ods_dt, last_mob) <= 90 then 'Mobile Active' else 'Non Mobile Active' end as mobile_active_flag,
  case when datediff(ods_dt, last_olb) <= 90 then 'OLB Active' else 'Non OLB Active' end as olb_active_flag,
  case
    when (datediff(ods_dt,last_mob) <= 90) OR (datediff(ods_dt,last_olb) <= 90)
      then 'Digital Active'
    else 'Non Digital Active'
  end as digitally_active_flag,
  'Digital User' as digital_flag
FROM dig
WHERE relt_ibn is not null
""")

# =====================================================================================
# 4) CUSTOMER BASE (RCIF) — no arrangement join, dedupe address at END_DT
# =====================================================================================
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW rcif_customer AS
WITH ip AS (
  SELECT
    involved_party_id as ip_id,
    cast(rcif_cust_nbr as string) as rcif_number,
    cust_internet_banking_nbr as ibn,
    birth_date
  FROM {EIL_DB}.d_involved_party_h
  WHERE business_date = date('{END_DT}')
    AND source_system_code = 'CF'
    AND nvl(deceased_ind,'N') = 'N'
    AND birth_date is not null
),
addr_ranked AS (
  SELECT
    involved_party_id as ip_id,
    state_name,
    row_number() over (
      PARTITION BY involved_party_id
      ORDER BY nvl(state_name,'') desc
    ) as rn
  FROM {EIL_DB}.d_involved_party_address_h
  WHERE business_date = date('{END_DT}')
)
SELECT
  ip.rcif_number,
  ip.ip_id,
  ip.ibn,
  a.state_name
FROM ip
LEFT JOIN addr_ranked a
  ON ip.ip_id = a.ip_id AND a.rn = 1
""")

spark.sql("""
CREATE OR REPLACE TEMP VIEW rcif_digital_join AS
SELECT
  c.rcif_number,
  max(c.state_name) as state_name,
  c.ibn,
  d.digitally_active_flag,
  d.digital_flag,
  d.mobile_flag,
  d.mobile_active_flag,
  d.olb_flag,
  d.olb_active_flag
FROM rcif_customer c
LEFT JOIN digital_ibn d
  ON c.ibn = d.ibn
GROUP BY c.rcif_number, c.ibn
""")

spark.sql("""
CREATE OR REPLACE TEMP VIEW wia2_customer AS
WITH ranked AS (
  SELECT
    rcif_number,
    max(state_name) as state_name,
    max(ibn) as primary_ibn,

    max(case when digital_flag='Digital User' then 1 else 0 end) as digital_user_bin,
    max(case when digitally_active_flag='Digital Active' then 1 else 0 end) as digital_active_bin,

    max(case when mobile_flag='Mobile User' then 1 else 0 end) as mobile_user_bin,
    max(case when mobile_active_flag='Mobile Active' then 1 else 0 end) as mobile_active_bin,

    max(case when olb_flag='OLB User' then 1 else 0 end) as olb_user_bin,
    max(case when olb_active_flag='OLB Active' then 1 else 0 end) as olb_active_bin
  FROM rcif_digital_join
  GROUP BY rcif_number
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
FROM ranked
""")

# =====================================================================================
# 5) RCIF SET (union wealth rcifs + digital rcifs in window)
# =====================================================================================
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW wir2_rcif_set AS
WITH wealth_rcifs AS (
  SELECT distinct cast(rcif_cust_nbr as string) as rcif_number
  FROM {EIL_DB}.m_involved_party_h
  WHERE cast(business_date as date) >= date('{START_DT}')
    AND cast(business_date as date) <= date('{END_DT}')
),
digital_rcifs AS (
  SELECT distinct cast(rcif_customer_nbr as string) as rcif_number
  FROM {DMIB_DB}.digital_banking_master
  WHERE ods_business_dt >= date('{START_DT}')
    AND ods_business_dt <= date('{END_DT}')
)
SELECT distinct rcif_number
FROM (
  SELECT * FROM wealth_rcifs
  UNION ALL
  SELECT * FROM digital_rcifs
) u
""")

# =====================================================================================
# 6) VALIDATION (LATEST DETERMINED FROM FACT ITSELF — NEVER 0)
# =====================================================================================
print("\n" + "="*120)
print("VALIDATION OUTPUTS")
print("="*120)

spark.sql("""
SELECT count(distinct rcif_number) as wealth_rcifs_all_6mo
FROM wic2_wealth_fact
""").show(truncate=False)

spark.sql("""
SELECT max(business_date) as latest_dt
FROM wic2_wealth_fact
""").show(truncate=False)

spark.sql("""
WITH mx AS (SELECT max(business_date) as dt FROM wic2_wealth_fact)
SELECT
  count(distinct w.rcif_number) as wealth_rcifs_latest,
  sum(w.accts_cnt)             as wealth_accounts_latest
FROM wic2_wealth_fact w
JOIN mx ON w.business_date = mx.dt
""").show(truncate=False)

spark.sql("""
SELECT business_date,
       count(distinct rcif_number) as rcifs,
       sum(accts_cnt)              as accts
FROM wic2_wealth_fact
GROUP BY business_date
ORDER BY business_date
""").show(truncate=False)

spark.sql("""
SELECT count(distinct ibn) as digital_active_ibn_window
FROM digital_ibn
WHERE digitally_active_flag='Digital Active'
""").show(truncate=False)

spark.sql("""
SELECT count(*) as digital_active_rcif_window
FROM wia2_customer
WHERE digitally_active_flag='Digital Active'
""").show(truncate=False)

# =====================================================================================
# 7) WRITE TABLES
# =====================================================================================
print("\nWriting final tables...")
spark.table("wia2_customer").write.mode("overwrite").saveAsTable(f"{DEFAULT_DB}.wia2_customer")
spark.table("wic2_wealth_fact").write.mode("overwrite").saveAsTable(f"{DEFAULT_DB}.wic2_wealth_fact")
spark.table("wir2_rcif_set").write.mode("overwrite").saveAsTable(f"{DEFAULT_DB}.wir2_rcif_set")

print(f"✓ Saved: {DEFAULT_DB}.wia2_customer")
print(f"✓ Saved: {DEFAULT_DB}.wic2_wealth_fact")
print(f"✓ Saved: {DEFAULT_DB}.wir2_rcif_set")
print("\nDONE.")
print("="*120)

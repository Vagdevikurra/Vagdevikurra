from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark import SparkConf, StorageLevel

# ── Configuration ────────────────────────────────────────────────────────────────
DEFAULT_DB = "dm_ib_dev"
EIL_DB     = "eil"
DMIB_DB    = "dm_ib"
START_DT   = "2025-09-01"
END_DT     = "2026-02-28"

# Wealth source systems — PC and BW excluded as requested
WEALTH_SRC_LIST = [
    "BI", "TR", "DA", "SV", "CC", "MG", "LS", "TM", "LO",
    "CS", "IC", "MA", "PF", "PR", "SD", "CM", "EL", "RN"
]

APPLY_PRIMARY_OWNER_FILTER = False
IP_FUNDED_BALANCE_THRESHOLD = 0.0
DEBUG = True

conf = (
    SparkConf()
    .setAppName("wealth_insights")
    .set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .set("spark.sql.autoBroadcastJoinThreshold", "209715200")   # 200MB
    .set("spark.sql.shuffle.partitions", "600")
    .set("spark.sql.broadcastTimeout", "1200")
    .set("spark.executor.heartbeatInterval", "10s")
    .set("spark.network.timeout", "1200s")
    .set("spark.rpc.askTimeout", "300s")
    .set("spark.storage.blockManagerSlaveTimeoutMs", "900000")
    .set("spark.task.maxFailures", "16")
    .set("spark.stage.maxConsecutiveAttempts", "10")
    .set("spark.yarn.max.executor.failures", "64")
    .set("spark.speculation", "false")
    .set("spark.blacklist.enabled", "true")
    .set("spark.blacklist.task.maxTaskAttemptsPerExecutor", "2")
    .set("spark.blacklist.task.maxTaskAttemptsPerNode", "2")
    .set("spark.blacklist.stage.maxFailedTasksPerExecutor", "2")
    .set("spark.blacklist.stage.maxFailedExecutorsPerNode", "2")
)

spark = (
    SparkSession.builder
    .config(conf=conf)
    .enableHiveSupport()
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ── Source Tables ────────────────────────────────────────────────────────────────

# 0) Latest snapshot dates — customer & address
cust_dt = spark.sql(f"""
    SELECT MAX(CAST(business_date AS date)) AS dt
    FROM {EIL_DB}.d_involved_party_h
    WHERE source_system_code = 'CF'
""").collect()[0]["dt"]

addr_dt = spark.sql(f"""
    SELECT MAX(CAST(business_date AS date)) AS dt
    FROM {EIL_DB}.d_involved_party_address_h
""").collect()[0]["dt"]

print(f"[INFO] d_involved_party_h  (CF) snapshot : {cust_dt}")
print(f"[INFO] d_involved_party_address_h snapshot : {addr_dt}")
print(f"[INFO] Window : {START_DT} .. {END_DT}")


# ── 1) Month-end business dates ─────────────────────────────────────────────────
# d_ tables only carry weekday data.  For months whose calendar last day falls on
# a weekend / holiday (e.g. Aug 31 Sun, Nov 30 Sun, Jan 31 Sat) we pick the
# latest available business day inside that calendar month.

wealth_src_csv = ",".join([f"'{s}'" for s in WEALTH_SRC_LIST])

spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW month_ends AS
    WITH cal AS (
        SELECT add_months(date('{START_DT}'), n) AS month_start,
               add_months(date('{START_DT}'), n + 1) AS next_month_start
        FROM (
            SELECT sequence(
                0,
                CAST(months_between(date('{END_DT}'), date('{START_DT}')) AS INT)
            ) AS s
        ) t
        LATERAL VIEW posexplode(s) pe AS n, _
    ),
    all_dates AS (
        SELECT DISTINCT bd FROM (
            SELECT CAST(business_date AS date) AS bd FROM {EIL_DB}.d_involved_party_h
            UNION ALL
            SELECT CAST(business_date AS date) AS bd FROM {EIL_DB}.d_arrangement_to_involved_party_relationship_h
            UNION ALL
            SELECT CAST(business_date AS date) AS bd FROM {EIL_DB}.d_arrangement_h
        ) raw_dates
    )
    SELECT MAX(a.bd) AS business_date
      FROM cal m
      LEFT JOIN all_dates a
        ON a.bd >= m.month_start AND a.bd < m.next_month_start
     GROUP BY m.month_start
     ORDER BY m.month_start
""")

spark.sql("CACHE TABLE month_ends")
_ = spark.sql("SELECT COUNT(*) AS cnt FROM month_ends").collect()

if DEBUG:
    print("\n[1] month_ends (6 month-end business dates):")
    spark.sql("SELECT * FROM month_ends ORDER BY business_date").show(truncate=False)


# ── 2) WEALTH base — arrangement grain ──────────────────────────────────────────
primary_owner_pred = "1=1"
if APPLY_PRIMARY_OWNER_FILTER:
    primary_owner_pred = "COALESCE(a2i.relationship_role,'') = 'PRIMARY'"

spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW wealth_arr AS
    SELECT
        CAST(ind.business_date AS date)        AS business_date,
        CAST(ind.rcif_cust_nbr AS string)      AS rcif_number,
        ind.involved_party_id                  AS ip_id,
        ind.cust_internet_banking_nbr          AS cust_internet_banking_nbr,
        ind.private_client_code,
        ind.private_client_trust_code,
        ar.arrangement_id,
        ar.source_system_code,
        ar.business_service_segment_type_code
    FROM {EIL_DB}.d_involved_party_h ind
    JOIN month_ends d
        ON CAST(ind.business_date AS date) = d.business_date
    JOIN {EIL_DB}.d_arrangement_to_involved_party_relationship_h a2i
        ON  ind.involved_party_id   = a2i.involved_party_id
        AND ind.business_date       = a2i.business_date
        AND ind.source_system_code  = a2i.source_system_code
    JOIN {EIL_DB}.d_arrangement_h ar
        ON  a2i.arrangement_id                 = ar.arrangement_id
        AND a2i.arrangement_source_system_code = ar.source_system_code
        AND a2i.business_date                  = ar.business_date
    WHERE ind.source_system_code = 'CF'
      AND NVL(ind.deceased_ind, 'N') = 'N'
      AND ar.closed_ind = 'N'
      AND {primary_owner_pred}
      AND ar.source_system_code IN ({wealth_src_csv})
      AND (
            CASE
                WHEN ind.private_client_code      IN ('039','539','339') THEN 1
                WHEN ind.private_client_trust_code IN ('239','739')      THEN 1
                ELSE CASE
                    WHEN ar.business_service_segment_type_code
                         IN ('IS_CT','IS_IT','REGIS_FC','REGIS','PWM') THEN 1
                    ELSE 0
                END
            END
          ) = 1
""")

if DEBUG:
    print("\n[2] wealth_arr sample:")
    spark.sql("SELECT * FROM wealth_arr LIMIT 10").show(truncate=False)


# ── 3) WEALTH aggregate — RCIF grain ────────────────────────────────────────────
spark.sql("""
    CREATE OR REPLACE TEMP VIEW wealth_agg AS
    WITH base AS (
        SELECT
            business_date,
            rcif_number,
            ip_id,
            cust_internet_banking_nbr,
            private_client_code,
            private_client_trust_code,
            source_system_code,
            business_service_segment_type_code,
            concat_ws('|', source_system_code, CAST(arrangement_id AS string)) AS acct_key
        FROM wealth_arr
    ),
    by_rcif AS (
        SELECT
            business_date,
            rcif_number,
            MAX(ip_id)                       AS ip_id,
            MAX(cust_internet_banking_nbr)   AS cust_internet_banking_nbr,
            COUNT(DISTINCT acct_key)         AS wealth_accts_cnt,

            -- segment / system counts for business_group & division
            COUNT(DISTINCT CASE WHEN business_service_segment_type_code = 'IS_CT'    THEN acct_key END) AS corporate_trust_count,
            COUNT(DISTINCT CASE WHEN business_service_segment_type_code = 'IS_IT'    THEN acct_key END) AS institutional_trust_count,
            COUNT(DISTINCT CASE WHEN business_service_segment_type_code = 'REGIS_FC' THEN acct_key END) AS investment_count,
            COUNT(DISTINCT CASE WHEN business_service_segment_type_code = 'REGIS'    THEN acct_key END) AS insurance_count,
            COUNT(DISTINCT CASE WHEN business_service_segment_type_code = 'PWM'      THEN acct_key END) AS pwm_count,

            COUNT(DISTINCT CASE WHEN source_system_code = 'TR' THEN acct_key END) AS trust_count,
            COUNT(DISTINCT CASE WHEN source_system_code IN
                  ('DA','SV','CC','MG','LS','TM','LO','CM','CS','EL',
                   'IC','MA','PF','PR','SD','BI','RN')
                  THEN acct_key END) AS banking_count,

            MAX(CASE WHEN private_client_code      IN ('039','539','339')
                       OR private_client_trust_code IN ('239','739')
                     THEN 1 ELSE 0 END) AS private_flag
        FROM base
        GROUP BY business_date, rcif_number
    )
    SELECT
        business_date,
        rcif_number,
        ip_id,
        cust_internet_banking_nbr,
        wealth_accts_cnt,

        -- Business group at RCIF grain
        CASE
            WHEN private_flag = 1 THEN 'Private Wealth'
            WHEN (corporate_trust_count + institutional_trust_count) > 0 THEN 'Institutional Services'
            WHEN (investment_count + insurance_count) > 0               THEN 'Investment Services'
            WHEN pwm_count > 0                                          THEN 'Private Wealth'
            ELSE 'Other'
        END AS business_group,

        -- Division at RCIF grain
        CASE
            -- ── Private Wealth sub-divisions ──
            WHEN CASE
                     WHEN private_flag = 1 THEN 'Private Wealth'
                     WHEN (corporate_trust_count + institutional_trust_count) > 0 THEN 'Institutional Services'
                     WHEN (investment_count + insurance_count) > 0               THEN 'Investment Services'
                     WHEN pwm_count > 0                                          THEN 'Private Wealth'
                     ELSE 'Other'
                 END = 'Private Wealth'
            THEN CASE
                     WHEN trust_count > 0 AND banking_count > 0                        THEN 'Banking & IMAT'
                     WHEN (investment_count + trust_count) > 0 AND banking_count = 0   THEN 'Investments Only'
                     ELSE 'Banking only'
                 END
            -- ── Investment Services sub-divisions ──
            WHEN CASE
                     WHEN private_flag = 1 THEN 'Private Wealth'
                     WHEN (corporate_trust_count + institutional_trust_count) > 0 THEN 'Institutional Services'
                     WHEN (investment_count + insurance_count) > 0               THEN 'Investment Services'
                     WHEN pwm_count > 0                                          THEN 'Private Wealth'
                     ELSE 'Other'
                 END = 'Investment Services'
            THEN CASE
                     WHEN investment_count > 0 AND insurance_count = 0 THEN 'Investment'
                     WHEN investment_count > 0 AND insurance_count > 0 THEN 'Insurance'
                     ELSE 'Insurance & Investment'
                 END
            -- ── Institutional Services sub-divisions ──
            WHEN (corporate_trust_count > 0 AND institutional_trust_count = 0) THEN 'Corporate Trust'
            WHEN (corporate_trust_count = 0 AND institutional_trust_count > 0) THEN 'Institutional Trust'
            WHEN pwm_count > 0                                                 THEN 'Banking only'
            ELSE 'Corporate & Institutional Trust'
        END AS division

    FROM by_rcif
""")

# Cache wealth_agg — it feeds the customer table and is reused
spark.sql("CACHE TABLE wealth_agg")
_ = spark.sql("SELECT COUNT(*) FROM wealth_agg").collect()

if DEBUG:
    print("\n[3] wealth_agg sample:")
    spark.sql("SELECT * FROM wealth_agg LIMIT 10").show(truncate=False)


# ── 4) INVESTPATH (RN + IP) — arrangement grain, monthly as-of ──────────────────
# Simplified: join directly on month_ends dates (same approach as wealth_arr).
# Persist to DISK_ONLY — too large for memory cache but we need it for both
# the RCIF agg and the account-level output table.

investpath_df = spark.sql(f"""
    SELECT
        CAST(ind.business_date AS date)                            AS business_date,
        CAST(ind.rcif_cust_nbr AS STRING)                          AS rcif_number,
        ind.cust_internet_banking_nbr,
        ind.involved_party_id                                      AS ip_id,
        CONCAT_WS('|', ar.source_system_code,
                   CAST(ar.arrangement_id AS STRING))               AS ip_account_id,
        CAST(COALESCE(ar.current_balance_amt, 0.0) AS DOUBLE)     AS ip_balance,
        CAST(ar.open_date AS DATE)                                 AS ip_open_date
    FROM {EIL_DB}.d_involved_party_h ind
    JOIN month_ends d
        ON CAST(ind.business_date AS date) = d.business_date
    JOIN {EIL_DB}.d_arrangement_to_involved_party_relationship_h a2i
        ON  ind.involved_party_id   = a2i.involved_party_id
        AND ind.business_date       = a2i.business_date
        AND ind.source_system_code  = a2i.source_system_code
    JOIN {EIL_DB}.d_arrangement_h ar
        ON  a2i.arrangement_id                 = ar.arrangement_id
        AND a2i.arrangement_source_system_code = ar.source_system_code
        AND a2i.business_date                  = ar.business_date
    WHERE ind.source_system_code     = 'CF'
      AND NVL(ind.deceased_ind, 'N') = 'N'
      AND ar.source_system_code      = 'RN'
      AND ar.account_type_code       = 'IP'
      AND ar.closed_ind              = 'N'
      AND {primary_owner_pred}
""")
investpath_df.persist(StorageLevel.DISK_ONLY)
investpath_cnt = investpath_df.count()
investpath_df.createOrReplaceTempView("investpath_arr")
print(f"[INFO] investpath_arr persisted to disk: {investpath_cnt:,} rows")

# RCIF-level aggregate for the customer-table join
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW investpath_agg AS
    SELECT
        business_date,
        rcif_number,
        COUNT(DISTINCT ip_account_id)  AS ip_accounts_cnt,
        SUM(COALESCE(ip_balance, 0.0)) AS ip_sum,
        COUNT(DISTINCT CASE WHEN ip_balance > {IP_FUNDED_BALANCE_THRESHOLD}
                            THEN ip_account_id END) AS ip_funded_accounts_cnt,
        CASE WHEN COUNT(DISTINCT ip_account_id) > 0 THEN 1 ELSE 0 END AS ip_customers_flag
    FROM investpath_arr
    GROUP BY business_date, rcif_number
""")

if DEBUG:
    print("\n[4] investpath_agg sample:")
    spark.sql("SELECT * FROM investpath_agg LIMIT 5").show(truncate=False)


# ── 5) DIGITAL monthly ──────────────────────────────────────────────────────────
# NOTE: digital_banking_master uses column `ibn` (not reltibn).
# We aggregate per (month, rcif, ibn) so we can later join to wealth on
# cust_internet_banking_nbr = ibn — only the wealth-linked ibn drives flags.
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW digital_monthly AS
    SELECT
        TRUNC(ods_business_dt, 'MM')            AS month_dt,
        CAST(rcif_customer_nbr AS string)       AS rcif_number,
        ibn,
        MAX(olb_last_login_date)                AS last_olb,
        MAX(mob_last_login_date)                AS last_mob,
        MAX(ods_business_dt)                    AS ods_dt
    FROM {DMIB_DB}.digital_banking_master
    WHERE ods_business_dt >= date('{START_DT}')
      AND ods_business_dt <= date('{END_DT}')
    GROUP BY TRUNC(ods_business_dt, 'MM'),
             CAST(rcif_customer_nbr AS string),
             ibn
""")

# Cache digital_monthly — feeds the customer table join
spark.sql("CACHE TABLE digital_monthly")

if DEBUG:
    print("\n[5] digital_monthly sample:")
    spark.sql("SELECT * FROM digital_monthly LIMIT 10").show(truncate=False)


# ── 6) Address snapshot ─────────────────────────────────────────────────────────
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW rcif_address AS
    WITH addr_ranked AS (
        SELECT
            involved_party_id                 AS ip_id,
            state_name,
            ROW_NUMBER() OVER (
                PARTITION BY involved_party_id
                ORDER BY NVL(state_name, '') DESC
            ) AS rn
        FROM {EIL_DB}.d_involved_party_address_h
        WHERE CAST(business_date AS date) = date('{addr_dt}')
    )
    SELECT ip_id, state_name
      FROM addr_ranked
     WHERE rn = 1
""")


# ── 7) Customer Table — wealth_insights_cust ────────────────────────────────────
# Grain: (business_date, rcif_number) — one row per customer per month-end.
# business_date holds the 6 month-end dates (Sep–Feb).
# Digital flags are derived by joining on cust_internet_banking_nbr = ibn,
# so only the wealth-linked ibn drives the flags (not all ibn per RCIF).

spark.sql("""
    CREATE OR REPLACE TEMP VIEW wealth_insights_cust AS
    SELECT
        w.business_date,
        w.rcif_number,
        w.cust_internet_banking_nbr,
        w.ip_id,
        w.business_group,
        w.division,
        w.wealth_accts_cnt,

        -- Mobile flags
        CASE WHEN d.last_mob IS NOT NULL THEN 'Mobile User'
             ELSE 'Non Mobile User'
        END AS mobile_flag,

        CASE WHEN d.last_mob IS NOT NULL
              AND datediff(d.ods_dt, d.last_mob) <= 90
             THEN 'Mobile Active'
             ELSE 'Non Mobile Active'
        END AS mobile_active_flag,

        -- OLB flags
        CASE WHEN d.last_olb IS NOT NULL THEN 'OLB User'
             ELSE 'Non OLB User'
        END AS olb_flag,

        CASE WHEN d.last_olb IS NOT NULL
              AND datediff(d.ods_dt, d.last_olb) <= 90
             THEN 'OLB Active'
             ELSE 'Non OLB Active'
        END AS olb_active_flag,

        -- Digital enrollment (ibn match exists = enrolled)
        CASE WHEN d.ibn IS NOT NULL THEN 'Digital User'
             ELSE 'Non Digital User'
        END AS digital_flag,

        -- Digital active = either channel active within 90 days
        CASE WHEN (d.last_mob IS NOT NULL AND datediff(d.ods_dt, d.last_mob) <= 90)
               OR (d.last_olb IS NOT NULL AND datediff(d.ods_dt, d.last_olb) <= 90)
             THEN 'Digital Active'
             ELSE 'Non Digital Active'
        END AS digital_active_flag,

        -- fact_type
        CASE WHEN d.ibn IS NOT NULL THEN 'WEALTH & DIGITAL'
             ELSE 'WEALTH'
        END AS fact_type

    FROM wealth_agg w
    LEFT JOIN digital_monthly d
        ON  w.rcif_number               = d.rcif_number
        AND w.cust_internet_banking_nbr = d.ibn
        AND TRUNC(w.business_date, 'MM') = d.month_dt
""")

if DEBUG:
    try:
        print("\n[7] wealth_insights_cust — flag distribution:")
        spark.sql("""
            SELECT business_date,
                   COUNT(DISTINCT rcif_number) AS wealth_customers,
                   COUNT(DISTINCT CASE WHEN digital_flag        = 'Digital User'    THEN rcif_number END) AS digital_enrolled,
                   COUNT(DISTINCT CASE WHEN digital_active_flag = 'Digital Active'  THEN rcif_number END) AS digital_active,
                   COUNT(DISTINCT CASE WHEN olb_active_flag     = 'OLB Active'      THEN rcif_number END) AS olb_active,
                   COUNT(DISTINCT CASE WHEN mobile_active_flag  = 'Mobile Active'   THEN rcif_number END) AS mobile_active
              FROM wealth_insights_cust
             GROUP BY business_date
             ORDER BY business_date
        """).show(truncate=False)

        # Delta check vs Power BI reference
        print("\n[7b] Delta vs Power BI reference (positive = we're over):")
        ref_data = [
            ("2025-10-31", 64385, 59359, 89061),
            ("2025-11-28", 64489, 59812, 89507),
            ("2025-12-31", 64598, 60220, 89928),
            ("2026-01-30", 65088, 60577, 90451),
            ("2026-02-27", 65767, 60799, 91034),
        ]
        our = spark.sql("""
            SELECT business_date,
                   COUNT(DISTINCT CASE WHEN olb_active_flag     = 'OLB Active'     THEN rcif_number END) AS olb,
                   COUNT(DISTINCT CASE WHEN mobile_active_flag  = 'Mobile Active'  THEN rcif_number END) AS mob,
                   COUNT(DISTINCT CASE WHEN digital_active_flag = 'Digital Active' THEN rcif_number END) AS dig
              FROM wealth_insights_cust
             WHERE business_date >= '2025-10-01'
             GROUP BY business_date ORDER BY business_date
        """).collect()
        our_map = {str(r["business_date"]): r for r in our}
        print(f"{'month':>12}  {'olb_delta':>10}  {'mob_delta':>10}  {'dig_delta':>10}")
        for dt, ref_olb, ref_mob, ref_dig in ref_data:
            r = our_map.get(dt)
            if r:
                print(f"{dt:>12}  {r['olb']-ref_olb:>+10,}  {r['mob']-ref_mob:>+10,}  {r['dig']-ref_dig:>+10,}")

        # ── Diagnostic: check if wealth_agg has duplicate RCIFs ──
        print("\n[7c] Duplicate check — wealth_agg rows vs distinct RCIFs:")
        spark.sql("""
            SELECT business_date,
                   COUNT(*)                    AS total_rows,
                   COUNT(DISTINCT rcif_number) AS distinct_rcifs
              FROM wealth_agg
             GROUP BY business_date ORDER BY business_date
        """).show(truncate=False)

        # ── Diagnostic: Power BI-style INTERSECT approach (rcif-only, no ibn) ──
        print("\n[7d] Power BI-style count (RCIF-only intersect, no ibn constraint):")
        spark.sql(f"""
            SELECT w.business_date,
                   COUNT(DISTINCT CASE WHEN d.olb_active_flag     = 'OLB Active'    THEN w.rcif_number END) AS olb_active_rcif,
                   COUNT(DISTINCT CASE WHEN d.mob_active_flag     = 'Mobile Active'  THEN w.rcif_number END) AS mob_active_rcif,
                   COUNT(DISTINCT CASE WHEN d.digital_active_flag = 'Digital Active' THEN w.rcif_number END) AS dig_active_rcif
              FROM wealth_agg w
              INNER JOIN (
                  SELECT rcif_number, month_dt,
                         MAX(CASE WHEN last_olb IS NOT NULL AND datediff(ods_dt, last_olb) <= 90
                                  THEN 'OLB Active' ELSE 'Non OLB Active' END) AS olb_active_flag,
                         MAX(CASE WHEN last_mob IS NOT NULL AND datediff(ods_dt, last_mob) <= 90
                                  THEN 'Mobile Active' ELSE 'Non Mobile Active' END) AS mob_active_flag,
                         MAX(CASE WHEN (last_mob IS NOT NULL AND datediff(ods_dt, last_mob) <= 90)
                                    OR (last_olb IS NOT NULL AND datediff(ods_dt, last_olb) <= 90)
                                  THEN 'Digital Active' ELSE 'Non Digital Active' END) AS digital_active_flag
                    FROM digital_monthly
                   GROUP BY rcif_number, month_dt
              ) d
                ON  w.rcif_number = d.rcif_number
                AND TRUNC(w.business_date, 'MM') = d.month_dt
             GROUP BY w.business_date
             ORDER BY w.business_date
        """).show(truncate=False)

    except Exception as e:
        print(f"[WARN] Debug queries failed: {e}")


# ══════════════════════════════════════════════════════════════════════════════════
# WRITE TABLES — customer first (small), then account (large)
# ══════════════════════════════════════════════════════════════════════════════════
TARGET_WRITE_PARTITIONS = 32
MAX_RECORDS_PER_FILE    = 1_000_000

# -- 9a) Customer table (write immediately — small, fast)
print("\n[9a] Writing customer table ...")
cust_df = spark.table("wealth_insights_cust")
_ = cust_df.cache().count()
print(f"[INFO] wealth_insights_cust rows: {_:,}")
cust_df = cust_df.coalesce(TARGET_WRITE_PARTITIONS)

(cust_df.write
    .mode("overwrite")
    .option("maxRecordsPerFile", MAX_RECORDS_PER_FILE)
    .saveAsTable(f"{DEFAULT_DB}.wealth_insights_cust")
)
print(f"[OK] Saved {DEFAULT_DB}.wealth_insights_cust")


# -- 9b) Account table (larger — uses investpath_df which is DISK_ONLY persisted)
print("\n[9b] Writing account table ...")
try:
    # Build account DF directly from the persisted investpath_df to avoid
    # re-reading the lazy view through Spark SQL (which can lose the persist).
    addr_df = spark.table("rcif_address")

    acct_df = (
        investpath_df
        .join(addr_df, on="ip_id", how="left")
        .withColumn("ip_accounts_cnt",
                     F.count("ip_account_id").over(
                         Window.partitionBy("business_date", "rcif_number")))
    )
    acct_cnt = acct_df.count()
    print(f"[INFO] wealth_insights_acct rows: {acct_cnt:,}")
    acct_df = acct_df.coalesce(TARGET_WRITE_PARTITIONS)

    (acct_df.write
        .mode("overwrite")
        .option("maxRecordsPerFile", MAX_RECORDS_PER_FILE)
        .saveAsTable(f"{DEFAULT_DB}.wealth_insights_acct")
    )
    print(f"[OK] Saved {DEFAULT_DB}.wealth_insights_acct")
except Exception as e:
    print(f"[ERROR] Account table write failed: {e}")
    print("[INFO] Customer table was already saved successfully.")


# Cleanup
for t in ["month_ends", "wealth_agg", "digital_monthly"]:
    try:
        spark.sql(f"UNCACHE TABLE IF EXISTS {t}")
    except Exception:
        pass
try:
    investpath_df.unpersist()
    cust_df.unpersist()
except Exception:
    pass

print("DONE.")

from pyspark.sql import SparkSession, functions as F, types as T
from pyspark import StorageLevel

# ----------------------------
# CONFIG / CONSTANTS
# ----------------------------
DEFAULT_DB = "dm_ib_dev"
EIL_DB     = "eil"
DMIB_DB    = "dm_ib"

START_DT = "2025-09-01"
END_DT   = "2026-02-28"

# Wealth source systems — PC and BW excluded as requested
WEALTH_SRC_LIST = [
    "BI", "TR", "DA", "SV", "CC", "MG", "LS", "TM", "LO",
    "CS", "IC", "MA", "PF", "PR", "SD", "CM", "EL", "RN"
]

APPLY_PRIMARY_OWNER_FILTER  = False
IP_FUNDED_BALANCE_THRESHOLD = 0.0
DEBUG                       = False

# ----------------------------
# SPARK SESSION
# ----------------------------
spark = (
    SparkSession.builder
        .appName("Wealth_Insights")
        .enableHiveSupport()
        .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ----------------------------
# OPERATIONAL / STABILITY SETTINGS
# ----------------------------
spark.conf.set("spark.sql.adaptive.enabled",               "false")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold",     209715200)  # 200 MB
spark.conf.set("spark.sql.broadcastTimeout",               "1200")
spark.conf.set("spark.sql.shuffle.partitions",             "600")

# Heartbeats & timeouts
spark.conf.set("spark.executor.heartbeatInterval",         "10s")
spark.conf.set("spark.network.timeout",                    "1200s")
spark.conf.set("spark.rpc.askTimeout",                     "300s")
spark.conf.set("spark.storage.blockManagerSlaveTimeoutMs", "900000")

# Retry tolerance
spark.conf.set("spark.task.maxFailures",                   "16")
spark.conf.set("spark.stage.maxConsecutiveAttempts",       "10")
spark.conf.set("spark.yarn.max.executor.failures",         "64")

# File committer tuning
spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version",          "2")
spark.conf.set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")

# Avoid duplicate speculative work
spark.conf.set("spark.speculation", "false")

# Blacklist flaky nodes
spark.conf.set("spark.blacklist.enabled",                          "true")
spark.conf.set("spark.blacklist.task.maxTaskAttemptsPerExecutor",  "2")
spark.conf.set("spark.blacklist.task.maxTaskAttemptsPerNode",      "2")
spark.conf.set("spark.blacklist.stage.maxFailedTasksPerExecutor",  "2")
spark.conf.set("spark.blacklist.stage.maxFailedExecutorsPerNode",  "2")

print(f"Window: {START_DT} .. {END_DT}")

# ============================================================
# 0) Latest snapshot dates
#
#    d_ tables  → daily loads, SKIP weekends & bank holidays.
#                 MAX(business_date) gives the last business day.
#    m_ tables  → monthly, load EVERY calendar day including
#                 weekends & holidays (e.g. digital_banking_master).
#                 No special handling needed; MAX() already correct.
# ============================================================
cust_dt = spark.sql(f"""
    SELECT MAX(CAST(business_date AS date)) AS dt
    FROM {EIL_DB}.d_involved_party_h
    WHERE source_system_code = 'CF'
""").collect()[0]["dt"]

addr_dt = spark.sql(f"""
    SELECT MAX(CAST(business_date AS date)) AS dt
    FROM {EIL_DB}.d_involved_party_address_h
""").collect()[0]["dt"]

print(f"[INFO] d_involved_party_h snapshot date        : {cust_dt}")
print(f"[INFO] d_involved_party_address_h snapshot date: {addr_dt}")

# ============================================================
# 1) month_ends: MAX available d_ business_date per calendar
#    month.  Because d_ tables skip weekends/holidays, MAX()
#    resolves to the last actual business day loaded that month.
# ============================================================
wealth_src_csv = ",".join(f"'{s}'" for s in WEALTH_SRC_LIST)

spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW month_ends AS
    WITH cal AS (
        SELECT
            add_months(date('{START_DT}'), n)     AS month_start,
            add_months(date('{START_DT}'), n + 1) AS next_month_start
        FROM (
            SELECT sequence(
                0,
                CAST(months_between(date('{END_DT}'), date('{START_DT}')) AS INT)
            ) AS s
        ) g
        LATERAL VIEW posexplode(s) pe AS n, _
    ),
    all_dates AS (
        -- d_ tables: weekday-only snapshots
        SELECT DISTINCT CAST(business_date AS date) AS bd
        FROM {EIL_DB}.d_involved_party_h
        UNION
        SELECT DISTINCT CAST(business_date AS date)
        FROM {EIL_DB}.d_arrangement_to_involved_party_relationship_h
        UNION
        SELECT DISTINCT CAST(business_date AS date)
        FROM {EIL_DB}.d_arrangement_h
    )
    SELECT
        m.month_start,
        MAX(a.bd) AS business_date
    FROM cal m
    LEFT JOIN all_dates a
           ON a.bd >= m.month_start
          AND a.bd <  m.next_month_start
    GROUP BY m.month_start
    ORDER BY m.month_start
""")

# Cache to avoid recomputation across subsequent joins
spark.sql("CACHE TABLE month_ends")
spark.sql("SELECT COUNT(*) FROM month_ends").collect()

if DEBUG:
    print("\n[1] month_ends (max d_ business date per month):")
    spark.sql("SELECT * FROM month_ends ORDER BY business_date").show(200, truncate=False)

# ============================================================
# 2) WEALTH base at arrangement grain
# ============================================================
primary_owner_pred = "1=1"
if APPLY_PRIMARY_OWNER_FILTER:
    primary_owner_pred = "COALESCE(a2i.relationship_role,'') = 'PRIMARY'"

spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW wealth_arr AS
    SELECT
        CAST(ind.business_date AS date)           AS business_date,
        CAST(ind.rcif_cust_nbr AS string)         AS rcif_number,
        ind.involved_party_id                     AS ip_id,
        ind.cust_internet_banking_nbr             AS ibn,
        ind.private_client_code,
        ind.private_client_trust_code,
        ar.arrangement_id,
        ar.source_system_code,
        ar.business_service_segment_type_code,
        ar.closed_ind
    FROM {EIL_DB}.d_involved_party_h ind
    JOIN month_ends d
      ON CAST(ind.business_date AS date) = d.business_date
    JOIN {EIL_DB}.d_arrangement_to_involved_party_relationship_h a2i
      ON  ind.involved_party_id  = a2i.involved_party_id
      AND ind.business_date      = a2i.business_date
      AND ind.source_system_code = a2i.source_system_code
    JOIN {EIL_DB}.d_arrangement_h ar
      ON  a2i.arrangement_id                 = ar.arrangement_id
      AND a2i.arrangement_source_system_code = ar.source_system_code
      AND a2i.business_date                  = ar.business_date
    WHERE ind.source_system_code = 'CF'
      AND nvl(ind.deceased_ind, 'N') = 'N'
      AND ar.closed_ind = 'N'
      AND ({primary_owner_pred})
      AND ar.source_system_code IN ({wealth_src_csv})
      AND (
            CASE
                WHEN ind.private_client_code       IN ('039','539','339') THEN 1
                WHEN ind.private_client_trust_code IN ('239','739')       THEN 1
                ELSE CASE
                    WHEN ar.business_service_segment_type_code
                         IN ('IS_CT','IS_IT','REGIS_FC','REGIS','PWM')   THEN 1
                    ELSE 0
                END
            END
          ) = 1
""")

if DEBUG:
    print("\n[2] wealth_arr sample:")
    spark.sql("SELECT * FROM wealth_arr LIMIT 10").show(truncate=False)

# ============================================================
# 3) WEALTH aggregate (RCIF grain)
# ============================================================
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW wealth_agg AS
    WITH base AS (
        SELECT
            business_date,
            rcif_number,
            private_client_code,
            private_client_trust_code,
            source_system_code,
            business_service_segment_type_code,
            concat_ws('|', source_system_code, cast(arrangement_id AS string)) AS acct_key
        FROM wealth_arr
    ),
    by_rcif AS (
        SELECT
            business_date,
            rcif_number,
            COUNT(DISTINCT acct_key) AS accts_cnt,
            -- segment/system counts for classification
            COUNT(DISTINCT CASE WHEN business_service_segment_type_code = 'IS_CT'    THEN acct_key END) AS corporate_trust_count,
            COUNT(DISTINCT CASE WHEN business_service_segment_type_code = 'IS_IT'    THEN acct_key END) AS institutional_trust_count,
            COUNT(DISTINCT CASE WHEN business_service_segment_type_code = 'REGIS_FC' THEN acct_key END) AS investment_count,
            COUNT(DISTINCT CASE WHEN business_service_segment_type_code = 'REGIS'    THEN acct_key END) AS insurance_count,
            COUNT(DISTINCT CASE WHEN business_service_segment_type_code = 'PWM'      THEN acct_key END) AS pwm_count,
            COUNT(DISTINCT CASE WHEN source_system_code = 'TR' THEN acct_key END) AS trust_count,
            COUNT(DISTINCT CASE WHEN source_system_code IN (
                'DA','SV','CC','MG','LS','TM','LO','CS','EL','IC','MA','PF','PR','SD','BI','RN','IS_CT','IS_IT','PWM'
            ) THEN acct_key END) AS banking_count,
            MAX(CASE
                    WHEN private_client_code       IN ('039','539','339')
                      OR private_client_trust_code IN ('239','739')
                    THEN 1 ELSE 0
                END) AS private_flag
        FROM base
        GROUP BY business_date, rcif_number
    )
    SELECT
        business_date,
        rcif_number,
        -- Business group at RCIF grain
        CASE
            WHEN private_flag = 1 THEN 'Private Wealth'
            ELSE CASE
                WHEN (corporate_trust_count + institutional_trust_count) > 0 THEN 'Institutional Services'
                WHEN (investment_count + insurance_count) > 0               THEN 'Investment Services'
                WHEN pwm_count > 0                                           THEN 'Private Wealth'
                ELSE 'Other'
            END
        END AS business_group,
        -- Division at RCIF grain
        CASE
            WHEN CASE
                     WHEN private_flag = 1 THEN 'Private Wealth'
                     ELSE CASE
                         WHEN (corporate_trust_count + institutional_trust_count) > 0 THEN 'Institutional Services'
                         WHEN (investment_count + insurance_count) > 0               THEN 'Investment Services'
                         WHEN pwm_count > 0                                           THEN 'Private Wealth'
                         ELSE 'Other'
                     END
                 END = 'Private Wealth'
            THEN CASE
                     WHEN trust_count > 0 AND banking_count > 0       THEN 'Banking & IMAT'
                     WHEN (investment_count + trust_count) > 0
                          AND banking_count = 0                        THEN 'Investments Only'
                     ELSE 'Banking only'
                 END
            WHEN CASE
                     WHEN private_flag = 1 THEN 'Private Wealth'
                     ELSE CASE
                         WHEN (corporate_trust_count + institutional_trust_count) > 0 THEN 'Institutional Services'
                         WHEN (investment_count + insurance_count) > 0               THEN 'Investment Services'
                         WHEN pwm_count > 0                                           THEN 'Private Wealth'
                         ELSE 'Other'
                     END
                 END = 'Investment Services'
            THEN CASE
                     WHEN investment_count > 0 AND insurance_count = 0 THEN 'Investment'
                     WHEN investment_count > 0 AND insurance_count > 0 THEN 'Insurance & Investment'
                     ELSE 'Insurance & Investment'
                 END
            WHEN (corporate_trust_count > 0 AND institutional_trust_count = 0) THEN 'Corporate Trust'
            WHEN (corporate_trust_count = 0 AND institutional_trust_count > 0) THEN 'Institutional Trust'
            WHEN pwm_count > 0                                                  THEN 'Banking only'
            ELSE 'Corporate & Institutional Trust'
        END AS division,
        accts_cnt
    FROM by_rcif
""")

# ============================================================
# 4) INVESTPATH (RN + IP) — arrangement grain, monthly as-of
#    All underlying tables are d_ → skip weekends/holidays.
#    Anchor dates from month_ends already account for this.
# ============================================================
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW investpath_arr AS
    WITH anchors AS (
        SELECT
            m.business_date              AS anchor_dt,
            TRUNC(m.business_date, 'MM') AS month_start
        FROM month_ends m
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
          AND ar.account_type_code  = 'IP'
          AND ar.closed_ind         = 'N'
        GROUP BY a.anchor_dt
    ),
    ind_at AS (
        SELECT
            s.anchor_dt                       AS business_date,
            ind.involved_party_id             AS ip_id,
            CAST(ind.rcif_cust_nbr AS STRING) AS rcif_number
        FROM ind_snap s
        JOIN {EIL_DB}.d_involved_party_h ind
          ON CAST(ind.business_date AS DATE) = s.ind_bd
        WHERE ind.source_system_code = 'CF'
          AND NVL(ind.deceased_ind,'N') = 'N'
    ),
    a2i_at AS (
        SELECT
            s.anchor_dt                                AS business_date,
            a2i.involved_party_id                      AS ip_id,
            a2i.arrangement_id,
            a2i.arrangement_source_system_code         AS arr_src
        FROM a2i_snap s
        JOIN {EIL_DB}.d_arrangement_to_involved_party_relationship_h a2i
          ON CAST(a2i.business_date AS DATE) = s.a2i_bd
    ),
    ar_at AS (
        SELECT
            s.anchor_dt                                          AS business_date,
            ar.arrangement_id,
            ar.source_system_code,
            CAST(COALESCE(ar.current_balance_amt, 0.0) AS DOUBLE) AS ip_balance,
            CAST(ar.open_date AS DATE)                           AS ip_open_date
        FROM ar_snap s
        JOIN {EIL_DB}.d_arrangement_h ar
          ON CAST(ar.business_date AS DATE) = s.ar_bd
        WHERE ar.source_system_code = 'RN'
          AND ar.account_type_code  = 'IP'
          AND ar.closed_ind         = 'N'
    ),
    ip_joined AS (
        SELECT
            i.business_date,
            i.rcif_number,
            CONCAT_WS('|', ar.source_system_code,
                      CAST(ar.arrangement_id AS STRING))         AS ip_account_id,
            ar.ip_balance,
            ar.ip_open_date,
            i.ip_id
        FROM ind_at i
        JOIN a2i_at a2i
          ON a2i.ip_id          = i.ip_id
         AND a2i.business_date  = i.business_date
        JOIN ar_at ar
          ON ar.arrangement_id      = a2i.arrangement_id
         AND ar.source_system_code  = a2i.arr_src
         AND ar.business_date       = a2i.business_date
        WHERE ({primary_owner_pred})
    )
    SELECT
        business_date,
        rcif_number,
        ip_id,
        ip_account_id,
        MAX(ip_balance)   AS ip_balance,
        MIN(ip_open_date) AS ip_open_date
    FROM ip_joined
    GROUP BY business_date, rcif_number, ip_id, ip_account_id
""")

spark.sql("""
    CREATE OR REPLACE TEMP VIEW investpath_arr_dedup AS
    SELECT * FROM investpath_arr
""")

spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW investpath_agg AS
    SELECT
        business_date,
        rcif_number,
        COUNT(DISTINCT ip_account_id)                                              AS ip_accounts_cnt,
        SUM(COALESCE(ip_balance, 0.0))                                             AS ip_sum,
        COUNT(DISTINCT CASE WHEN ip_balance > {IP_FUNDED_BALANCE_THRESHOLD}
                             THEN ip_account_id END)                               AS ip_funded_accounts_cnt,
        CASE WHEN COUNT(DISTINCT ip_account_id) > 0 THEN 1 ELSE 0 END             AS ip_customers_flag
    FROM investpath_arr_dedup
    GROUP BY business_date, rcif_number
""")

if DEBUG:
    print("\n[3-4] Wealth + InvestPath aggregation samples:")
    spark.sql("SELECT * FROM wealth_agg     LIMIT 5").show(truncate=False)
    spark.sql("SELECT * FROM investpath_agg LIMIT 5").show(truncate=False)

# ============================================================
# 5) BUILD: Wealth_Insights_Customer1
# ============================================================
wealth_fact_df = (
    spark.table("wealth_agg")
        .select("business_date", "rcif_number", "business_group", "division", "accts_cnt")
        .join(spark.table("investpath_agg"), ["business_date", "rcif_number"], "left")
        .withColumn("ip_accounts_cnt",
                    F.coalesce(F.col("ip_accounts_cnt"), F.lit(0)).cast("long"))
        .withColumn("ip_sum",
                    F.coalesce(F.col("ip_sum"), F.lit(0.0)).cast("double"))
        .withColumn("ip_funded_accounts_cnt",
                    F.coalesce(F.col("ip_funded_accounts_cnt"), F.lit(0)).cast("long"))
        .withColumn("ip_customers_flag",
                    F.coalesce(F.col("ip_customers_flag"), F.lit(0)).cast("int"))
        .withColumn("fact_type",    F.lit("WEALTH"))
        .withColumn("ip_id",        F.lit(None).cast("string"))
        .withColumn("ip_account_id",F.lit(None).cast("string"))
        .withColumn("ip_balance",   F.lit(None).cast("double"))
        .withColumn("ip_open_date", F.lit(None).cast("date"))
)

wealth_fact_df.createOrReplaceTempView("Wealth_Insights_Customer1")

if DEBUG:
    print("\n[5] wealth fact sample (RCIF + InvestPath):")
    spark.sql("""
        SELECT business_date, rcif_number, business_group, division,
               accts_cnt, ip_accounts_cnt, ip_funded_accounts_cnt,
               ip_sum, ip_customers_flag, fact_type
        FROM Wealth_Insights_Customer1
        ORDER BY business_date, rcif_number
        LIMIT 20
    """).show(truncate=False)

# ============================================================
# 6) DIGITAL monthly
#    digital_banking_master is an m_ table →
#    loads EVERY calendar day including weekends & holidays.
#    TRUNC to month + GROUP BY produces one row per RCIF/IBN/month.
# ============================================================
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW digital_monthly AS
    WITH dig_customer AS (
        SELECT
            TRUNC(ods_business_dt, 'MM')         AS month_dt,
            CAST(rcif_customer_nbr AS string)     AS rcif_number,
            ibn,
            MAX(olb_last_login_date)              AS last_olb,
            MAX(mob_last_login_date)              AS last_mob,
            MAX(ods_business_dt)                  AS ods_dt
        FROM {DMIB_DB}.digital_banking_master
        WHERE ods_business_dt >= date('{START_DT}')
          AND ods_business_dt <= date('{END_DT}')
        GROUP BY TRUNC(ods_business_dt, 'MM'),
                 CAST(rcif_customer_nbr AS string),
                 ibn
    )
    SELECT
        month_dt,
        rcif_number,
        ibn,
        ods_dt,
        CASE WHEN last_mob IS NULL
             THEN 'Non Mobile User'  ELSE 'Mobile User'  END AS mobile_flag,
        CASE WHEN last_olb IS NULL
             THEN 'Non OLB User'     ELSE 'OLB User'     END AS olb_flag,
        CASE WHEN last_mob IS NOT NULL
              AND datediff(ods_dt, last_mob) <= 90
             THEN 'Mobile Active'    ELSE 'Non Mobile Active' END AS mobile_active_flag,
        CASE
            WHEN (last_olb IS NOT NULL AND datediff(ods_dt, last_olb) <= 90)
              OR (last_mob IS NOT NULL AND datediff(ods_dt, last_olb) <= 90)
            THEN 'Digital Active'
            ELSE 'Non Digital Active'
        END AS digitally_active_flag,
        CASE WHEN ibn IS NULL
             THEN 'Non Digital User' ELSE 'Digital User'  END AS digital_flag
    FROM dig_customer
""")

if DEBUG:
    print("\n[6] digital_monthly sample:")
    spark.sql("SELECT * FROM digital_monthly LIMIT 10").show(truncate=False)

# ============================================================
# 7) CUSTOMER snapshot + BUILD: Wealth_Insights_Account1
#    cust_dt / addr_dt already reflect last d_ load date.
# ============================================================
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW rcif_customer AS
    WITH ip AS (
        SELECT
            involved_party_id              AS ip_id,
            CAST(rcif_cust_nbr AS string)  AS rcif_number,
            cust_internet_banking_nbr      AS ibn,
            birth_date
        FROM {EIL_DB}.d_involved_party_h
        WHERE CAST(business_date AS date) = date('{cust_dt}')
          AND source_system_code = 'CF'
          AND nvl(deceased_ind,'N') = 'N'
          AND birth_date IS NOT NULL
    ),
    addr_ranked AS (
        SELECT
            involved_party_id AS ip_id,
            state_name,
            row_number() OVER (
                PARTITION BY involved_party_id
                ORDER BY nvl(state_name,'') DESC
            ) AS rn
        FROM {EIL_DB}.d_involved_party_address_h
        WHERE CAST(business_date AS date) = date('{addr_dt}')
    )
    SELECT
        ip.rcif_number,
        ip.ibn,
        a.state_name
    FROM ip
    LEFT JOIN addr_ranked a
           ON ip.ip_id = a.ip_id
          AND a.rn = 1
""")

spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW Wealth_Insights_Account1 AS
    WITH joined AS (
        SELECT
            c.rcif_number,
            MAX(c.state_name) AS state_name,
            MAX(c.ibn)        AS primary_ibn,
            MAX(CASE WHEN d.digital_flag          = 'Digital User'    THEN 1 ELSE 0 END) AS digital_user_bin,
            MAX(CASE WHEN d.digitally_active_flag = 'Digital Active'  THEN 1 ELSE 0 END) AS digitally_active_bin,
            MAX(CASE WHEN d.mobile_flag           = 'Mobile User'     THEN 1 ELSE 0 END) AS mobile_user_bin,
            MAX(CASE WHEN d.mobile_active_flag    = 'Mobile Active'   THEN 1 ELSE 0 END) AS mobile_active_bin,
            MAX(CASE WHEN d.olb_flag              = 'OLB User'        THEN 1 ELSE 0 END) AS olb_user_bin,
            MAX(CASE WHEN d.olb_flag              = 'OLB Active'      THEN 1 ELSE 0 END) AS olb_active_bin
        FROM rcif_customer c
        LEFT JOIN digital_monthly d
               ON c.ibn = d.ibn
        GROUP BY c.rcif_number
    )
    SELECT
        rcif_number,
        state_name,
        primary_ibn,
        CASE WHEN digital_user_bin     = 1 THEN 'Digital User'    ELSE 'Non Digital User'    END AS digital_flag,
        CASE WHEN digitally_active_bin = 1 THEN 'Digital Active'  ELSE 'Non Digital Active'  END AS digitally_active_flag,
        CASE WHEN mobile_user_bin      = 1 THEN 'Mobile User'     ELSE 'Non Mobile User'     END AS mobile_flag,
        CASE WHEN mobile_active_bin    = 1 THEN 'Mobile Active'   ELSE 'Non Mobile Active'   END AS mobile_active_flag,
        CASE WHEN olb_user_bin         = 1 THEN 'OLB User'        ELSE 'Non OLB User'        END AS olb_flag,
        CASE WHEN olb_active_bin       = 1 THEN 'OLB Active'      ELSE 'Non OLB Active'      END AS olb_active_flag
    FROM joined
""")

if DEBUG:
    print("\n[7] Wealth_Insights_Account1 distribution (digital activity):")
    spark.sql("""
        SELECT digitally_active_flag, COUNT(*) AS cnt
        FROM Wealth_Insights_Account1
        GROUP BY digitally_active_flag
        ORDER BY cnt DESC
    """).show(truncate=False)

# ============================================================
# 8) WRITE TABLES (robust write; _1 suffix)
#    Keep tail small so YARN can place tasks;
#    lower TARGET_WRITE_PARTITIONS to 24/16 if queue is tight.
# ============================================================
TARGET_WRITE_PARTITIONS = 32
MAX_RECORDS_PER_FILE    = 1_000_000

# --- Wealth_Insights_Customer1 ---
wealth_out_df = (
    spark.table("Wealth_Insights_Customer1")
         .persist(StorageLevel.DISK_ONLY)
)
_ = wealth_out_df.count()   # materialise to cut lineage for retries
wealth_out_df = wealth_out_df.coalesce(TARGET_WRITE_PARTITIONS)

(
    wealth_out_df.write
        .mode("overwrite")
        .option("maxRecordsPerFile", MAX_RECORDS_PER_FILE)
        .saveAsTable(f"{DEFAULT_DB}.Wealth_Insights_Customer1")
)

# --- Wealth_Insights_Account1 ---
account_out_df = (
    spark.table("Wealth_Insights_Account1")
         .persist(StorageLevel.DISK_ONLY)
)
_ = account_out_df.count()
account_out_df = account_out_df.coalesce(TARGET_WRITE_PARTITIONS)

(
    account_out_df.write
        .mode("overwrite")
        .option("maxRecordsPerFile", MAX_RECORDS_PER_FILE)
        .saveAsTable(f"{DEFAULT_DB}.Wealth_Insights_Account1")
)

print(f"Saved {DEFAULT_DB}.Wealth_Insights_Customer1")
print(f"Saved {DEFAULT_DB}.Wealth_Insights_Account1")
print("DONE.")

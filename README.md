"""
Wealth Insights Pipeline  —  v3  (memory-safe, no OOM)
=======================================================
Fix for Py4JNetworkError / executor killed by YARN OOM.

Root cause of all previous errors:
  - checkpoint(eager=True) and persist()+count() both force a
    full DAG replay in one executor pass on a 1.6 M-row join
    spanning 5 large tables → YARN kills the container → JVM dies
    → "Answer from Java side is empty"

Solution: materialise EVERY intermediate stage as a physical
Hive table and read it back fresh before the next step.
Each stage then has a ONE-HOP lineage. Nothing blows up.

Stage order
-----------
  S0  snapshot dates     (collect only — tiny)
  S1  wi_stage_month_ends
  S2  wi_stage_wealth_arr
  S3  wi_stage_wealth_agg
  S4  wi_stage_investpath_agg
  S5  wi_stage_digital_monthly
  S6  wi_stage_rcif_customer
  S7  wealth_insights_account   → written once
  S8  wealth_insights_customer  → written one month at a time
"""

from pyspark.sql import SparkSession, functions as F

# ──────────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────────
DEFAULT_DB  = "dm_ib_dev"
STAGE_DB    = "dm_ib_dev"   # staging tables land here; change if needed
EIL_DB      = "eil"
DMIB_DB     = "dm_ib"

START_DT    = "2025-09-01"
END_DT      = "2026-02-28"

WEALTH_SRC_LIST = [
    "BI", "TR", "DA", "SV", "CC", "MG", "LS", "TM", "LO",
    "CS", "IC", "MA", "PF", "PR", "SD", "CM", "EL", "RN"
]

APPLY_PRIMARY_OWNER_FILTER  = False
IP_FUNDED_BALANCE_THRESHOLD = 0.0

# ──────────────────────────────────────────────────────────────
# SPARK SESSION
# NOTE: executor.memory / memoryOverhead MUST be set here.
# spark.conf.set() after getOrCreate() is ignored by YARN for
# memory — YARN has already allocated the containers by then.
# ──────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
        .appName("Wealth_Insights_v3")
        .enableHiveSupport()
        .config("spark.executor.memory",                    "10g")
        .config("spark.executor.memoryOverhead",            "3g")
        .config("spark.driver.memory",                      "4g")
        .config("spark.driver.memoryOverhead",              "1g")
        .config("spark.memory.fraction",                    "0.8")
        .config("spark.memory.storageFraction",             "0.2")
        .config("spark.sql.shuffle.partitions",             "800")
        .config("spark.sql.adaptive.enabled",               "false")
        .config("spark.sql.autoBroadcastJoinThreshold",     str(200 * 1024 * 1024))
        .config("spark.sql.broadcastTimeout",               "1200")
        .config("spark.executor.heartbeatInterval",         "20s")
        .config("spark.network.timeout",                    "1200s")
        .config("spark.rpc.askTimeout",                     "600s")
        .config("spark.storage.blockManagerSlaveTimeoutMs", "900000")
        .config("spark.task.maxFailures",                   "8")
        .config("spark.stage.maxConsecutiveAttempts",       "10")
        .config("spark.yarn.max.executor.failures",         "32")
        .config("spark.speculation",                        "false")
        .config("spark.blacklist.enabled",                  "true")
        .config("spark.blacklist.task.maxTaskAttemptsPerExecutor",  "2")
        .config("spark.blacklist.task.maxTaskAttemptsPerNode",      "2")
        .config("spark.blacklist.stage.maxFailedTasksPerExecutor",  "2")
        .config("spark.blacklist.stage.maxFailedExecutorsPerNode",  "2")
        .config("mapreduce.fileoutputcommitter.algorithm.version",  "2")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ──────────────────────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────────────────────
WRITE_PARTS      = 64
MAX_REC_PER_FILE = 500_000

wealth_src_csv     = ",".join(f"'{s}'" for s in WEALTH_SRC_LIST)
primary_owner_pred = (
    "COALESCE(a2i.relationship_role,'') = 'PRIMARY'"
    if APPLY_PRIMARY_OWNER_FILTER else "1=1"
)


def materialise(sql, stage_table, partitions=WRITE_PARTS):
    """
    Run sql → write to STAGE_DB.stage_table → register as temp view.
    Short lineage guaranteed on every subsequent read.
    """
    print(f"  → {STAGE_DB}.{stage_table} …", flush=True)
    (spark.sql(sql)
          .repartition(partitions)
          .write
          .mode("overwrite")
          .saveAsTable(f"{STAGE_DB}.{stage_table}"))
    spark.table(f"{STAGE_DB}.{stage_table}").createOrReplaceTempView(stage_table)
    print(f"  ✓ {stage_table}", flush=True)


# ══════════════════════════════════════════════════════════════
# S0  Snapshot dates
# ══════════════════════════════════════════════════════════════
print("\n[S0] snapshot dates")
cust_dt = spark.sql(f"""
    SELECT MAX(CAST(business_date AS date)) AS dt
    FROM {EIL_DB}.d_involved_party_h
    WHERE source_system_code = 'CF'
""").collect()[0]["dt"]

addr_dt = spark.sql(f"""
    SELECT MAX(CAST(business_date AS date)) AS dt
    FROM {EIL_DB}.d_involved_party_address_h
""").collect()[0]["dt"]

print(f"  cust_dt={cust_dt}  addr_dt={addr_dt}")

# ══════════════════════════════════════════════════════════════
# S1  month_ends
# ══════════════════════════════════════════════════════════════
print("\n[S1] month_ends")
materialise(f"""
    WITH cal AS (
        SELECT add_months(date('{START_DT}'), n) AS month_start,
               add_months(date('{START_DT}'), n + 1) AS next_month_start
        FROM (
            SELECT sequence(
                0,
                CAST(months_between(date('{END_DT}'), date('{START_DT}')) AS INT)
            ) AS s
        ) g LATERAL VIEW posexplode(s) pe AS n, _
    ),
    all_dates AS (
        SELECT DISTINCT CAST(business_date AS date) AS bd
        FROM {EIL_DB}.d_involved_party_h
        UNION
        SELECT DISTINCT CAST(business_date AS date)
        FROM {EIL_DB}.d_arrangement_to_involved_party_relationship_h
        UNION
        SELECT DISTINCT CAST(business_date AS date)
        FROM {EIL_DB}.d_arrangement_h
    )
    SELECT MAX(a.bd) AS business_date
    FROM cal m
    LEFT JOIN all_dates a
        ON a.bd >= m.month_start AND a.bd < m.next_month_start
    GROUP BY m.month_start
    ORDER BY m.month_start
""", "wi_stage_month_ends", partitions=1)

month_list = [
    r["business_date"]
    for r in spark.sql(
        "SELECT business_date FROM wi_stage_month_ends ORDER BY business_date"
    ).collect()
]
print(f"  months: {month_list}")

# ══════════════════════════════════════════════════════════════
# S2  wealth_arr  (arrangement grain)
# ══════════════════════════════════════════════════════════════
print("\n[S2] wealth_arr")
materialise(f"""
    SELECT
        CAST(ind.business_date AS date)                AS business_date,
        CAST(ind.rcif_cust_nbr AS string)              AS rcif_number,
        ind.involved_party_id                          AS ip_id,
        ind.cust_internet_banking_nbr                  AS ibn,
        ind.private_client_code,
        ind.private_client_trust_code,
        ar.arrangement_id,
        ar.source_system_code,
        ar.business_service_segment_type_code,
        ar.closed_ind
    FROM {EIL_DB}.d_involved_party_h ind
    JOIN wi_stage_month_ends d
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
                WHEN ind.private_client_code IN ('039','539','339')        THEN 1
                WHEN ind.private_client_trust_code IN ('239','739')        THEN 1
                WHEN ar.business_service_segment_type_code
                     IN ('IS_CT','IS_IT','REGIS_FC','REGIS','PWM')         THEN 1
                ELSE 0
            END
          ) = 1
""", "wi_stage_wealth_arr")

# ══════════════════════════════════════════════════════════════
# S3  wealth_agg  (RCIF grain)
# ══════════════════════════════════════════════════════════════
print("\n[S3] wealth_agg")
materialise(f"""
    WITH base AS (
        SELECT
            business_date,
            rcif_number,
            private_client_code,
            private_client_trust_code,
            source_system_code,
            business_service_segment_type_code,
            concat_ws('|', source_system_code,
                      cast(arrangement_id as string)) AS acct_key
        FROM wi_stage_wealth_arr
    ),
    by_rcif AS (
        SELECT
            business_date,
            rcif_number,
            COUNT(DISTINCT acct_key)                                                                     AS accts_cnt,
            COUNT(DISTINCT CASE WHEN business_service_segment_type_code = 'IS_CT'
                                THEN acct_key END)                                                       AS corporate_trust_count,
            COUNT(DISTINCT CASE WHEN business_service_segment_type_code = 'IS_IT'
                                THEN acct_key END)                                                       AS institutional_trust_count,
            COUNT(DISTINCT CASE WHEN business_service_segment_type_code = 'REGIS_FC'
                                THEN acct_key END)                                                       AS investment_count,
            COUNT(DISTINCT CASE WHEN business_service_segment_type_code = 'REGIS'
                                THEN acct_key END)                                                       AS insurance_count,
            COUNT(DISTINCT CASE WHEN business_service_segment_type_code = 'PWM'
                                THEN acct_key END)                                                       AS pwm_count,
            COUNT(DISTINCT CASE WHEN source_system_code = 'TR'
                                THEN acct_key END)                                                       AS trust_count,
            COUNT(DISTINCT CASE WHEN source_system_code IN (
                'DA','SV','CC','MG','LS','TM','LO','CM','CS','EL','IC','MA',
                'PF','PR','SD','BI','RN','IS_CT','IS_IT','PWM')
                                THEN acct_key END)                                                       AS banking_count,
            MAX(CASE WHEN private_client_code       IN ('039','539','339')
                       OR private_client_trust_code IN ('239','739')
                     THEN 1 ELSE 0 END)                                                                  AS private_flag
        FROM base
        GROUP BY business_date, rcif_number
    )
    SELECT
        business_date,
        rcif_number,
        CASE
            WHEN private_flag = 1 THEN 'Private Wealth'
            WHEN (corporate_trust_count + institutional_trust_count) > 0 THEN 'Institutional Services'
            WHEN (investment_count + insurance_count) > 0                THEN 'Investment Services'
            WHEN pwm_count > 0                                           THEN 'Private Wealth'
            ELSE 'Other'
        END AS business_group,
        CASE
            WHEN private_flag = 1
              OR (pwm_count > 0
                  AND (corporate_trust_count + institutional_trust_count) = 0
                  AND (investment_count + insurance_count) = 0)
            THEN CASE
                WHEN trust_count > 0 AND banking_count > 0                      THEN 'Banking & IMAT'
                WHEN (investment_count + trust_count) > 0 AND banking_count = 0 THEN 'Investments Only'
                ELSE 'Banking only'
            END
            WHEN (investment_count + insurance_count) > 0
             AND (corporate_trust_count + institutional_trust_count) = 0
             AND private_flag = 0
            THEN CASE
                WHEN investment_count > 0 AND insurance_count = 0 THEN 'Investment'
                WHEN investment_count > 0 AND insurance_count > 0 THEN 'Insurance'
                ELSE 'Insurance & Investment'
            END
            WHEN corporate_trust_count > 0 AND institutional_trust_count = 0 THEN 'Corporate Trust'
            WHEN corporate_trust_count = 0 AND institutional_trust_count > 0 THEN 'Institutional Trust'
            WHEN corporate_trust_count > 0 AND institutional_trust_count > 0 THEN 'Corporate & Institutional Trust'
            ELSE 'Banking only'
        END AS division,
        accts_cnt
    FROM by_rcif
""", "wi_stage_wealth_agg")

# ══════════════════════════════════════════════════════════════
# S4  investpath_agg
# ══════════════════════════════════════════════════════════════
print("\n[S4] investpath_agg")
materialise(f"""
    WITH anchors AS (
        SELECT business_date AS anchor_dt,
               TRUNC(business_date, 'MM') AS month_start
        FROM wi_stage_month_ends
    ),
    ind_snap AS (
        SELECT a.anchor_dt,
               MAX(CAST(ind.business_date AS DATE)) AS ind_bd
        FROM anchors a
        JOIN {EIL_DB}.d_involved_party_h ind
            ON CAST(ind.business_date AS DATE) >= a.month_start
           AND CAST(ind.business_date AS DATE) <= a.anchor_dt
        WHERE ind.source_system_code = 'CF'
        GROUP BY a.anchor_dt
    ),
    a2i_snap AS (
        SELECT a.anchor_dt,
               MAX(CAST(a2i.business_date AS DATE)) AS a2i_bd
        FROM anchors a
        JOIN {EIL_DB}.d_arrangement_to_involved_party_relationship_h a2i
            ON CAST(a2i.business_date AS DATE) >= a.month_start
           AND CAST(a2i.business_date AS DATE) <= a.anchor_dt
        GROUP BY a.anchor_dt
    ),
    ar_snap AS (
        SELECT a.anchor_dt,
               MAX(CAST(ar.business_date AS DATE)) AS ar_bd
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
        SELECT s.anchor_dt AS business_date,
               ind.involved_party_id             AS ip_id,
               CAST(ind.rcif_cust_nbr AS STRING) AS rcif_number
        FROM ind_snap s
        JOIN {EIL_DB}.d_involved_party_h ind
            ON CAST(ind.business_date AS DATE) = s.ind_bd
        WHERE ind.source_system_code = 'CF'
          AND NVL(ind.deceased_ind,'N') = 'N'
    ),
    a2i_at AS (
        SELECT s.anchor_dt AS business_date,
               a2i.involved_party_id              AS ip_id,
               a2i.arrangement_id,
               a2i.arrangement_source_system_code AS arr_src
        FROM a2i_snap s
        JOIN {EIL_DB}.d_arrangement_to_involved_party_relationship_h a2i
            ON CAST(a2i.business_date AS DATE) = s.a2i_bd
    ),
    ar_at AS (
        SELECT s.anchor_dt AS business_date,
               ar.arrangement_id,
               ar.source_system_code,
               CAST(COALESCE(ar.current_balance_amt, 0.0) AS DOUBLE) AS ip_balance,
               CAST(ar.open_date AS DATE)                             AS ip_open_date
        FROM ar_snap s
        JOIN {EIL_DB}.d_arrangement_h ar
            ON CAST(ar.business_date AS DATE) = s.ar_bd
        WHERE ar.source_system_code = 'RN'
          AND ar.account_type_code  = 'IP'
          AND ar.closed_ind         = 'N'
    ),
    ip_raw AS (
        SELECT i.business_date,
               i.rcif_number,
               CONCAT_WS('|', ar.source_system_code,
                         CAST(ar.arrangement_id AS STRING)) AS ip_account_id,
               ar.ip_balance,
               ar.ip_open_date
        FROM ind_at i
        JOIN a2i_at a2i
            ON a2i.ip_id         = i.ip_id
           AND a2i.business_date = i.business_date
        JOIN ar_at ar
            ON ar.arrangement_id     = a2i.arrangement_id
           AND ar.source_system_code = a2i.arr_src
           AND ar.business_date      = i.business_date
    ),
    ip_dedup AS (
        SELECT business_date, rcif_number, ip_account_id,
               MAX(ip_balance)   AS ip_balance,
               MIN(ip_open_date) AS ip_open_date
        FROM ip_raw
        GROUP BY business_date, rcif_number, ip_account_id
    )
    SELECT
        business_date,
        rcif_number,
        COUNT(DISTINCT ip_account_id)                                                              AS ip_accounts_cnt,
        SUM(COALESCE(ip_balance, 0.0))                                                             AS ip_sum,
        COUNT(DISTINCT CASE WHEN ip_balance > {IP_FUNDED_BALANCE_THRESHOLD}
                            THEN ip_account_id END)                                                AS ip_funded_accounts_cnt,
        CASE WHEN COUNT(DISTINCT ip_account_id) > 0 THEN 1 ELSE 0 END                             AS ip_customers_flag
    FROM ip_dedup
    GROUP BY business_date, rcif_number
""", "wi_stage_investpath_agg")

# ══════════════════════════════════════════════════════════════
# S5  digital_monthly
# ══════════════════════════════════════════════════════════════
print("\n[S5] digital_monthly")
materialise(f"""
    WITH raw AS (
        SELECT
            TRUNC(ods_business_dt, 'MM')       AS month_dt,
            CAST(rcif_customer_nbr AS string)  AS rcif_number,
            ibn,
            MAX(olb_last_login_date)           AS last_olb,
            MAX(mob_last_login_date)           AS last_mob,
            MAX(ods_business_dt)               AS ods_dt
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
        CASE WHEN last_olb IS NULL
             THEN 'Non OLB User'    ELSE 'OLB User'    END AS olb_flag,
        CASE WHEN last_mob IS NULL
             THEN 'Non Mobile User' ELSE 'Mobile User' END AS mob_flag,
        CASE WHEN last_olb IS NOT NULL AND datediff(ods_dt, last_olb) <= 90
             THEN 'OLB Active'    ELSE 'Non OLB Active'    END AS olb_active_flag,
        CASE WHEN last_mob IS NOT NULL AND datediff(ods_dt, last_mob) <= 90
             THEN 'Mobile Active' ELSE 'Non Mobile Active' END AS mob_active_flag,
        CASE WHEN (last_olb IS NOT NULL AND datediff(ods_dt, last_olb) <= 90)
               OR (last_mob IS NOT NULL AND datediff(ods_dt, last_mob) <= 90)
             THEN 'Digital Active' ELSE 'Non Digital Active' END AS digitally_active_flag,
        CASE WHEN ibn IS NULL
             THEN 'Non Digital User' ELSE 'Digital User' END AS digital_flag
    FROM raw
""", "wi_stage_digital_monthly")

# ══════════════════════════════════════════════════════════════
# S6  rcif_customer   (ibn in SELECT — critical fix from v2)
# ══════════════════════════════════════════════════════════════
print("\n[S6] rcif_customer")
materialise(f"""
    WITH ip AS (
        SELECT
            involved_party_id               AS ip_id,
            CAST(rcif_cust_nbr AS string)   AS rcif_number,
            cust_internet_banking_nbr       AS ibn,
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
    LEFT JOIN addr_ranked a ON ip.ip_id = a.ip_id AND a.rn = 1
""", "wi_stage_rcif_customer", partitions=32)

# ══════════════════════════════════════════════════════════════
# S7  wealth_insights_account  (one row per RCIF, no date dim)
# ══════════════════════════════════════════════════════════════
print("\n[S7] wealth_insights_account")
(spark.sql(f"""
    WITH joined AS (
        SELECT
            c.rcif_number,
            MAX(c.state_name) AS state_name,
            MAX(c.ibn)        AS ibn,
            MAX(CASE WHEN d.digital_flag          = 'Digital User'   THEN 1 ELSE 0 END) AS digital_user_bin,
            MAX(CASE WHEN d.digitally_active_flag = 'Digital Active' THEN 1 ELSE 0 END) AS digital_active_bin,
            MAX(CASE WHEN d.mob_flag              = 'Mobile User'    THEN 1 ELSE 0 END) AS mobile_user_bin,
            MAX(CASE WHEN d.mob_active_flag       = 'Mobile Active'  THEN 1 ELSE 0 END) AS mobile_active_bin,
            MAX(CASE WHEN d.olb_flag              = 'OLB User'       THEN 1 ELSE 0 END) AS olb_user_bin,
            MAX(CASE WHEN d.olb_active_flag       = 'OLB Active'     THEN 1 ELSE 0 END) AS olb_active_bin
        FROM wi_stage_rcif_customer c
        LEFT JOIN wi_stage_digital_monthly d ON c.ibn = d.ibn
        GROUP BY c.rcif_number
    )
    SELECT
        rcif_number,
        state_name,
        ibn,
        CASE WHEN digital_user_bin   = 1 THEN 'Digital User'      ELSE 'Non Digital User'   END AS digital_flag,
        CASE WHEN digital_active_bin = 1 THEN 'Digital Active'    ELSE 'Non Digital Active' END AS digitally_active_flag,
        CASE WHEN mobile_user_bin    = 1 THEN 'Mobile User'       ELSE 'Non Mobile User'    END AS mobile_flag,
        CASE WHEN mobile_active_bin  = 1 THEN 'Mobile Active'     ELSE 'Non Mobile Active'  END AS mobile_active_flag,
        CASE WHEN olb_user_bin       = 1 THEN 'OLB User'          ELSE 'Non OLB User'       END AS olb_flag,
        CASE WHEN olb_active_bin     = 1 THEN 'OLB Active'        ELSE 'Non OLB Active'     END AS olb_active_flag
    FROM joined
""")
    .repartition(32)
    .write
    .mode("overwrite")
    .saveAsTable(f"{DEFAULT_DB}.wealth_insights_account"))

print(f"  ✓ {DEFAULT_DB}.wealth_insights_account")

# ══════════════════════════════════════════════════════════════
# S8  wealth_insights_customer  — one month per Spark job
#     Reads 3 already-materialised tables → trivial lineage
# ══════════════════════════════════════════════════════════════
print("\n[S8] wealth_insights_customer (month-by-month)")
for i, biz_dt in enumerate(month_list):
    print(f"  [{i+1}/{len(month_list)}] {biz_dt} …", flush=True)

    month_df = spark.sql(f"""
        SELECT
            w.business_date,
            w.rcif_number,
            w.business_group,
            w.division,
            w.accts_cnt,
            CAST(COALESCE(ip.ip_accounts_cnt,        0)   AS bigint) AS ip_accounts_cnt,
            CAST(COALESCE(ip.ip_funded_accounts_cnt, 0)   AS bigint) AS ip_funded_accounts_cnt,
            CAST(COALESCE(ip.ip_sum,                 0.0) AS double) AS ip_sum,
            CAST(COALESCE(ip.ip_customers_flag,      0)   AS int)    AS ip_customers_flag,
            'WEALTH'                                                  AS fact_type,
            COALESCE(a.digital_flag,          'Non Digital User')    AS digital_flag,
            COALESCE(a.digitally_active_flag, 'Non Digital Active')  AS digitally_active_flag,
            COALESCE(a.mobile_flag,           'Non Mobile User')     AS mobile_flag,
            COALESCE(a.mobile_active_flag,    'Non Mobile Active')   AS mobile_active_flag,
            COALESCE(a.olb_flag,              'Non OLB User')        AS olb_flag,
            COALESCE(a.olb_active_flag,       'Non OLB Active')      AS olb_active_flag,
            COALESCE(a.state_name,            'Unknown')             AS state_name
        FROM wi_stage_wealth_agg w
        LEFT JOIN wi_stage_investpath_agg ip
            ON w.business_date = ip.business_date
           AND w.rcif_number   = ip.rcif_number
        LEFT JOIN {DEFAULT_DB}.wealth_insights_account a
            ON w.rcif_number = a.rcif_number
        WHERE w.business_date = date('{biz_dt}')
    """)

    write_mode = "overwrite" if i == 0 else "append"
    (month_df
        .repartition(WRITE_PARTS)
        .write
        .mode(write_mode)
        .option("maxRecordsPerFile", MAX_REC_PER_FILE)
        .saveAsTable(f"{DEFAULT_DB}.wealth_insights_customer"))

    print(f"    ✓ {biz_dt} → {write_mode}", flush=True)

# ══════════════════════════════════════════════════════════════
print(f"\n✅  {DEFAULT_DB}.wealth_insights_customer")
print(f"✅  {DEFAULT_DB}.wealth_insights_account")
print("DONE.")

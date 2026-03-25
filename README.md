from pyspark.sql import SparkSession
from pyspark import SparkConf

DEFAULT_DB = "dm_ib_dev"
EIL_DB     = "eil"
START_DT   = "2025-09-01"
END_DT     = "2026-02-28"
APPLY_PRIMARY_OWNER_FILTER = False

conf = (
    SparkConf()
    .setAppName("wealth_insights_account")
    .set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .set("spark.sql.autoBroadcastJoinThreshold", "209715200")
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
    .set("mapreduce.fileoutputcommitter.algorithm.version", "2")
)

spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
spark.sparkContext.setLogLevel("WARN")

primary_owner_pred = "1=1"
if APPLY_PRIMARY_OWNER_FILTER:
    primary_owner_pred = "COALESCE(a2i.relationship_role,'') = 'PRIMARY'"

cust_dt = spark.sql(f"""
    SELECT MAX(CAST(business_date AS date)) AS dt
    FROM {EIL_DB}.d_involved_party_h WHERE source_system_code = 'CF'
""").collect()[0]["dt"]

addr_dt = spark.sql(f"""
    SELECT MAX(CAST(business_date AS date)) AS dt
    FROM {EIL_DB}.d_involved_party_address_h
""").collect()[0]["dt"]

print(f"[INFO] cust_dt : {cust_dt}")
print(f"[INFO] addr_dt : {addr_dt}")
print(f"[INFO] Window  : {START_DT} .. {END_DT}")

# ── Month-end business dates ─────────────────────────────────────────────────────
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW month_ends AS
    WITH cal AS (
        SELECT add_months(date('{START_DT}'), n) AS month_start,
               add_months(date('{START_DT}'), n + 1) AS next_month_start
        FROM (SELECT sequence(0, CAST(months_between(date('{END_DT}'), date('{START_DT}')) AS INT)) AS s) t
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
    FROM cal m LEFT JOIN all_dates a ON a.bd >= m.month_start AND a.bd < m.next_month_start
    GROUP BY m.month_start
""")
print("[OK] month_ends")

# ── RCIF bridge (same as Step 1) ─────────────────────────────────────────────────
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW rcif_bridge AS
    SELECT DISTINCT
        CAST(rcif_cust_nbr AS string) AS rcif_number,
        involved_party_id             AS ip_id
    FROM {EIL_DB}.d_involved_party_h
    WHERE CAST(business_date AS date) = date('{cust_dt}')
      AND source_system_code = 'CF'
      AND NVL(deceased_ind, 'N') = 'N'
      AND birth_date IS NOT NULL
""")
print("[OK] rcif_bridge")

# ══════════════════════════════════════════════════════════════════════════════════
# WRITE: Account table — filtered through rcif_bridge
# ══════════════════════════════════════════════════════════════════════════════════
print("\n[WRITE] Account table ...")
spark.sql(f"DROP TABLE IF EXISTS {DEFAULT_DB}.wealth_insights_acct")
spark.sql(f"""
    CREATE TABLE {DEFAULT_DB}.wealth_insights_acct AS
    WITH ip_raw AS (
        SELECT
            CAST(ind.business_date AS date)                        AS business_date,
            CAST(ind.rcif_cust_nbr AS STRING)                      AS rcif_number,
            ind.cust_internet_banking_nbr,
            ind.involved_party_id                                  AS ip_id,
            CAST(COALESCE(ar.current_balance_amt, 0.0) AS DOUBLE) AS ip_balance,
            CAST(ar.open_date AS DATE)                             AS ip_open_date
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
        INNER JOIN rcif_bridge rb
            ON ind.involved_party_id = rb.ip_id
        WHERE ind.source_system_code     = 'CF'
          AND NVL(ind.deceased_ind, 'N') = 'N'
          AND ar.source_system_code      = 'RN'
          AND ar.account_type_code       = 'IP'
          AND ar.closed_ind              = 'N'
          AND {primary_owner_pred}
    ),
    ip_counts AS (
        SELECT business_date, rcif_number, COUNT(*) AS ip_accounts_cnt
        FROM ip_raw
        GROUP BY business_date, rcif_number
    )
    SELECT
        r.business_date,
        r.rcif_number,
        r.cust_internet_banking_nbr,
        r.ip_id,
        r.ip_balance,
        r.ip_open_date,
        c.ip_accounts_cnt,
        addr.state_name
    FROM ip_raw r
    JOIN ip_counts c
        ON  r.business_date = c.business_date
        AND r.rcif_number   = c.rcif_number
    LEFT JOIN (
        SELECT involved_party_id AS ip_id, state_name
        FROM (
            SELECT involved_party_id, state_name,
                   ROW_NUMBER() OVER (PARTITION BY involved_party_id ORDER BY NVL(state_name, '') DESC) AS rn
            FROM {EIL_DB}.d_involved_party_address_h
            WHERE CAST(business_date AS date) = date('{addr_dt}')
        ) ranked WHERE rn = 1
    ) addr
        ON r.ip_id = addr.ip_id
""")

print(f"[OK] Saved {DEFAULT_DB}.wealth_insights_acct")
spark.stop()
print("DONE.")

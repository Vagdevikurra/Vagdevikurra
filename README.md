from pyspark.sql import SparkSession
from pyspark import SparkConf

DEFAULT_DB = "dm_ib_dev"
EIL_DB     = "eil"
START_DT   = "2025-09-01"
END_DT     = "2026-02-28"
APPLY_PRIMARY_OWNER_FILTER = False

conf = (
    SparkConf()
    .setAppName("wealth_insights_acct_raw")
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

print("[WRITE] Raw IP rows (3-way join) ...")
spark.sql(f"DROP TABLE IF EXISTS {DEFAULT_DB}.wealth_insights_acct_raw")
spark.sql(f"""
    CREATE TABLE {DEFAULT_DB}.wealth_insights_acct_raw AS
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
    WHERE ind.source_system_code     = 'CF'
      AND NVL(ind.deceased_ind, 'N') = 'N'
      AND ar.source_system_code      = 'RN'
      AND ar.account_type_code       = 'IP'
      AND ar.closed_ind              = 'N'
      AND {primary_owner_pred}
""")

print(f"[OK] Saved {DEFAULT_DB}.wealth_insights_acct_raw")
spark.stop()
print("DONE — Now run Step 3.")

from pyspark.sql import SparkSession
from pyspark import SparkConf

DEFAULT_DB = "dm_ib_dev"
EIL_DB     = "eil"

conf = (
    SparkConf()
    .setAppName("wealth_insights_acct_final")
    .set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .set("spark.sql.autoBroadcastJoinThreshold", "209715200")
    .set("spark.sql.shuffle.partitions", "200")
    .set("spark.sql.broadcastTimeout", "1200")
    .set("spark.executor.heartbeatInterval", "10s")
    .set("spark.network.timeout", "1200s")
    .set("spark.rpc.askTimeout", "300s")
    .set("spark.storage.blockManagerSlaveTimeoutMs", "900000")
    .set("spark.task.maxFailures", "16")
    .set("mapreduce.fileoutputcommitter.algorithm.version", "2")
)

spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
spark.sparkContext.setLogLevel("WARN")

addr_dt = spark.sql(f"""
    SELECT MAX(CAST(business_date AS date)) AS dt
    FROM {EIL_DB}.d_involved_party_address_h
""").collect()[0]["dt"]

print(f"[INFO] address snapshot : {addr_dt}")
print("[WRITE] Final account table ...")

spark.sql(f"DROP TABLE IF EXISTS {DEFAULT_DB}.wealth_insights_acct")
spark.sql(f"""
    CREATE TABLE {DEFAULT_DB}.wealth_insights_acct AS
    SELECT
        r.business_date,
        r.rcif_number,
        r.cust_internet_banking_nbr,
        r.ip_id,
        r.ip_balance,
        r.ip_open_date,
        c.ip_accounts_cnt,
        addr.state_name
    FROM {DEFAULT_DB}.wealth_insights_acct_raw r
    JOIN (
        SELECT business_date, rcif_number, COUNT(*) AS ip_accounts_cnt
        FROM {DEFAULT_DB}.wealth_insights_acct_raw
        GROUP BY business_date, rcif_number
    ) c
        ON  r.business_date = c.business_date
        AND r.rcif_number   = c.rcif_number
    LEFT JOIN (
        SELECT ip_id, state_name
        FROM (
            SELECT involved_party_id AS ip_id, state_name,
                   ROW_NUMBER() OVER (PARTITION BY involved_party_id ORDER BY NVL(state_name, '') DESC) AS rn
            FROM {EIL_DB}.d_involved_party_address_h
            WHERE CAST(business_date AS date) = date('{addr_dt}')
        ) ranked WHERE rn = 1
    ) addr
        ON r.ip_id = addr.ip_id
""")

print(f"[OK] Saved {DEFAULT_DB}.wealth_insights_acct")

# Cleanup raw table
spark.sql(f"DROP TABLE IF EXISTS {DEFAULT_DB}.wealth_insights_acct_raw")
print("[OK] Dropped raw table")

spark.stop()
print("DONE — All 3 steps complete.")

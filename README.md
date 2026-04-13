from pyspark.sql import SparkSession
from pyspark import SparkConf

DEFAULT_DB = "dm_ib_dev"
EIL_DB     = "eil"
DMIB_DB    = "dm_ib"
START_DT   = "2025-09-01"
END_DT     = "2026-02-28"

WEALTH_SRC_LIST = [
    "BI", "TR", "DA", "SV", "CC", "LS", "MG", "TM",
    "LO", "CS", "IC", "MA", "PF", "PR", "SD", "CM", "EL", "RN"
]

conf = (
    SparkConf()
    .setAppName("wealth_insights_customer")
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

wealth_src_csv = ",".join([f"'{s}'" for s in WEALTH_SRC_LIST])

cust_dt = spark.sql(f"SELECT MAX(CAST(business_date AS date)) AS dt FROM {EIL_DB}.d_involved_party_h WHERE source_system_code='CF'").collect()[0]["dt"]
max_dig_dt = spark.sql(f"SELECT MAX(ods_business_dt) AS dt FROM {DMIB_DB}.digital_banking_master").collect()[0]["dt"]

print(f"[INFO] cust_dt    : {cust_dt}")
print(f"[INFO] max_dig_dt : {max_dig_dt}")
print(f"[INFO] Window     : {START_DT} .. {END_DT}")

# ── Quick data check ─────────────────────────────────────────────────────────────
dig_check = spark.sql(f"""
    SELECT COUNT(*) AS cnt FROM {DMIB_DB}.digital_banking_master
    WHERE ods_business_dt >= date('{START_DT}') AND ods_business_dt <= date('{END_DT}')
""").collect()[0]["cnt"]
print(f"[CHECK] digital_banking_master rows in date range: {dig_check:,}")

# ── 1) Month-end business dates ──────────────────────────────────────────────────
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW month_ends AS
    WITH cal AS (
        SELECT date('{START_DT}') AS ms, add_months(date('{START_DT}'),1) AS ns
        UNION ALL SELECT add_months(date('{START_DT}'),1), add_months(date('{START_DT}'),2)
        UNION ALL SELECT add_months(date('{START_DT}'),2), add_months(date('{START_DT}'),3)
        UNION ALL SELECT add_months(date('{START_DT}'),3), add_months(date('{START_DT}'),4)
        UNION ALL SELECT add_months(date('{START_DT}'),4), add_months(date('{START_DT}'),5)
        UNION ALL SELECT add_months(date('{START_DT}'),5), add_months(date('{START_DT}'),6)
    ),
    all_biz AS (
        SELECT DISTINCT bd FROM (
            SELECT CAST(business_date AS date) bd FROM {EIL_DB}.d_involved_party_h
            UNION ALL SELECT CAST(business_date AS date) FROM {EIL_DB}.d_arrangement_to_involved_party_relationship_h
            UNION ALL SELECT CAST(business_date AS date) FROM {EIL_DB}.d_arrangement_h
        ) x
    )
    SELECT MAX(a.bd) AS business_date
    FROM cal m LEFT JOIN all_biz a ON a.bd >= m.ms AND a.bd < m.ns
    GROUP BY m.ms
""")
spark.sql("CACHE TABLE month_ends")
print(f"[OK] month_ends: {spark.sql('SELECT COUNT(*) FROM month_ends').collect()[0][0]} months")
spark.sql("SELECT * FROM month_ends ORDER BY business_date").show(truncate=False)

# ── 2) Wealth base ───────────────────────────────────────────────────────────────
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW wealth_arr AS
    SELECT CAST(ind.business_date AS date) AS business_date,
        CAST(ind.rcif_cust_nbr AS string) AS rcif_number,
        ind.involved_party_id AS ip_id, ind.cust_internet_banking_nbr,
        ind.private_client_code, ind.private_client_trust_code,
        ar.arrangement_id, ar.source_system_code, ar.business_service_segment_type_code
    FROM {EIL_DB}.d_involved_party_h ind
    JOIN month_ends d ON CAST(ind.business_date AS date) = d.business_date
    JOIN {EIL_DB}.d_arrangement_to_involved_party_relationship_h a2i
        ON ind.involved_party_id=a2i.involved_party_id AND ind.business_date=a2i.business_date AND ind.source_system_code=a2i.source_system_code
    JOIN {EIL_DB}.d_arrangement_h ar
        ON a2i.arrangement_id=ar.arrangement_id AND a2i.arrangement_source_system_code=ar.source_system_code AND a2i.business_date=ar.business_date
        AND ar.source_system_code IN ({wealth_src_csv}) AND ar.closed_ind='N'
    WHERE ind.source_system_code='CF' AND NVL(ind.deceased_ind,'N')='N'
      AND (CASE WHEN ind.private_client_code IN ('039','539','339') THEN 1 WHEN ind.private_client_trust_code IN ('239','739') THEN 1
           ELSE CASE WHEN ar.business_service_segment_type_code IN ('IS_CT','IS_IT','REGIS_FC','REGIS','PWM') THEN 1 ELSE 0 END END)=1
""")
print("[OK] wealth_arr")

# ── 3) Wealth agg ────────────────────────────────────────────────────────────────
spark.sql("""
    CREATE OR REPLACE TEMP VIEW wealth_agg AS
    WITH base AS (SELECT *, concat_ws('|',source_system_code,CAST(arrangement_id AS string)) AS ak FROM wealth_arr),
    r AS (SELECT business_date, rcif_number, MAX(ip_id) ip_id, MAX(cust_internet_banking_nbr) ibn,
        COUNT(DISTINCT ak) accts,
        COUNT(DISTINCT CASE WHEN business_service_segment_type_code='IS_CT' THEN ak END) ct,
        COUNT(DISTINCT CASE WHEN business_service_segment_type_code='IS_IT' THEN ak END) it,
        COUNT(DISTINCT CASE WHEN business_service_segment_type_code='REGIS_FC' THEN ak END) inv,
        COUNT(DISTINCT CASE WHEN business_service_segment_type_code='REGIS' THEN ak END) ins,
        COUNT(DISTINCT CASE WHEN business_service_segment_type_code='PWM' THEN ak END) pwm,
        COUNT(DISTINCT CASE WHEN source_system_code='TR' THEN ak END) tr,
        COUNT(DISTINCT CASE WHEN source_system_code IN ('DA','SV','CC','MG','LS','TM','LO','CM','CS','EL','IC','MA','PF','PR','SD','BI','RN') THEN ak END) bk,
        MAX(CASE WHEN private_client_code IN ('039','539','339') OR private_client_trust_code IN ('239','739') THEN 1 ELSE 0 END) prv
    FROM base GROUP BY business_date, rcif_number)
    SELECT business_date, rcif_number, ip_id, ibn, accts,
        CASE WHEN prv=1 THEN 'Private Wealth' WHEN (ct+it)>0 THEN 'Institutional Services' WHEN (inv+ins)>0 THEN 'Investment Services' WHEN pwm>0 THEN 'Private Wealth' ELSE 'Other' END bg,
        CASE WHEN CASE WHEN prv=1 THEN 'PW' WHEN (ct+it)>0 THEN 'IS' WHEN (inv+ins)>0 THEN 'IV' WHEN pwm>0 THEN 'PW' ELSE 'O' END='PW'
             THEN CASE WHEN tr>0 AND bk>0 THEN 'Banking & IMAT' WHEN (inv+tr)>0 AND bk=0 THEN 'Investments Only' ELSE 'Banking only' END
             WHEN CASE WHEN prv=1 THEN 'PW' WHEN (ct+it)>0 THEN 'IS' WHEN (inv+ins)>0 THEN 'IV' WHEN pwm>0 THEN 'PW' ELSE 'O' END='IV'
             THEN CASE WHEN inv>0 AND ins=0 THEN 'Investment' WHEN inv=0 AND ins>0 THEN 'Insurance' ELSE 'Insurance & Investment' END
             WHEN ct>0 AND it=0 THEN 'Corporate Trust' WHEN ct=0 AND it>0 THEN 'Institutional Trust'
             WHEN pwm>0 THEN 'Banking only' ELSE 'Corporate & Institutional Trust' END div
    FROM r
""")
spark.sql("CACHE TABLE wealth_agg")
print(f"[OK] wealth_agg: {spark.sql('SELECT COUNT(*) FROM wealth_agg').collect()[0][0]:,} rows")

# ── 4) RCIF bridge + digital flags ───────────────────────────────────────────────
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW rcif_bridge AS
    SELECT DISTINCT CAST(rcif_cust_nbr AS string) AS rcif_number, cust_internet_banking_nbr AS ibn
    FROM {EIL_DB}.d_involved_party_h
    WHERE CAST(business_date AS date)=date('{cust_dt}') AND source_system_code='CF' AND NVL(deceased_ind,'N')='N' AND birth_date IS NOT NULL
""")

spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW dig_latest AS
    SELECT ibn, MAX(olb_last_login_date) last_olb, MAX(mob_last_login_date) last_mob, MAX(ods_business_dt) ods_dt
    FROM {DMIB_DB}.digital_banking_master WHERE ods_business_dt=date('{max_dig_dt}') GROUP BY ibn
""")

spark.sql("""
    CREATE OR REPLACE TEMP VIEW rcif_flags AS
    SELECT b.rcif_number,
        MAX(CASE WHEN d.last_mob IS NOT NULL THEN 1 ELSE 0 END) mu,
        MAX(CASE WHEN d.last_mob IS NOT NULL AND datediff(d.ods_dt,d.last_mob)<=90 THEN 1 ELSE 0 END) ma,
        MAX(CASE WHEN d.last_olb IS NOT NULL THEN 1 ELSE 0 END) ou,
        MAX(CASE WHEN d.last_olb IS NOT NULL AND datediff(d.ods_dt,d.last_olb)<=90 THEN 1 ELSE 0 END) oa,
        MAX(CASE WHEN d.ibn IS NOT NULL THEN 1 ELSE 0 END) du,
        MAX(CASE WHEN (d.last_mob IS NOT NULL AND datediff(d.ods_dt,d.last_mob)<=90) OR (d.last_olb IS NOT NULL AND datediff(d.ods_dt,d.last_olb)<=90) THEN 1 ELSE 0 END) da
    FROM rcif_bridge b LEFT JOIN dig_latest d ON b.ibn=d.ibn GROUP BY b.rcif_number
""")
spark.sql("CACHE TABLE rcif_flags")
print(f"[OK] rcif_flags: {spark.sql('SELECT COUNT(*) FROM rcif_flags').collect()[0][0]:,} rows")

# ══════════════════════════════════════════════════════════════════════════════════
# 5) SINGLE CREATE TABLE — wealth dedup + flags UNION ALL digital from source
# ══════════════════════════════════════════════════════════════════════════════════
print("\n[WRITE] Creating Wealth_Insights_Customer ...")
spark.sql(f"DROP TABLE IF EXISTS {DEFAULT_DB}.Wealth_Insights_Customer")
spark.sql(f"""
    CREATE TABLE {DEFAULT_DB}.Wealth_Insights_Customer AS

    SELECT w.business_date, w.rcif_number, w.ibn AS cust_internet_banking_nbr,
           CAST(w.ip_id AS string) AS ip_id, w.bg AS business_group, w.div AS division, w.accts AS wealth_accts_cnt,
           CASE WHEN f.mu=1 THEN 'Mobile User' ELSE 'Non Mobile User' END AS mobile_flag,
           CASE WHEN f.ma=1 THEN 'Mobile Active' ELSE 'Non Mobile Active' END AS mobile_active_flag,
           CASE WHEN f.ou=1 THEN 'OLB User' ELSE 'Non OLB User' END AS olb_flag,
           CASE WHEN f.oa=1 THEN 'OLB Active' ELSE 'Non OLB Active' END AS olb_active_flag,
           CASE WHEN f.du=1 THEN 'Digital User' ELSE 'Non Digital User' END AS digital_flag,
           CASE WHEN f.da=1 THEN 'Digital Active' ELSE 'Non Digital Active' END AS digitally_active_flag,
           'WEALTH' AS fact_type
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY business_date, rcif_number ORDER BY accts DESC) rn
        FROM wealth_agg
    ) w
    LEFT JOIN rcif_flags f ON w.rcif_number = f.rcif_number
    WHERE w.rn = 1

    UNION ALL

    SELECT
        CAST(TRUNC(dbm.ods_business_dt, 'MM') AS date) AS business_date,
        CAST(dbm.rcif_customer_nbr AS string) AS rcif_number,
        dbm.ibn AS cust_internet_banking_nbr,
        CAST(NULL AS string) AS ip_id,
        CAST(NULL AS string) AS business_group,
        CAST(NULL AS string) AS division,
        CAST(NULL AS bigint) AS wealth_accts_cnt,
        CASE WHEN MAX(dbm.mob_last_login_date) IS NOT NULL THEN 'Mobile User' ELSE 'Non Mobile User' END AS mobile_flag,
        CASE WHEN MAX(dbm.mob_last_login_date) IS NOT NULL AND datediff(MAX(dbm.ods_business_dt), MAX(dbm.mob_last_login_date)) <= 90
             THEN 'Mobile Active' ELSE 'Non Mobile Active' END AS mobile_active_flag,
        CASE WHEN MAX(dbm.olb_last_login_date) IS NOT NULL THEN 'OLB User' ELSE 'Non OLB User' END AS olb_flag,
        CASE WHEN MAX(dbm.olb_last_login_date) IS NOT NULL AND datediff(MAX(dbm.ods_business_dt), MAX(dbm.olb_last_login_date)) <= 90
             THEN 'OLB Active' ELSE 'Non OLB Active' END AS olb_active_flag,
        'Digital User' AS digital_flag,
        CASE WHEN (MAX(dbm.mob_last_login_date) IS NOT NULL AND datediff(MAX(dbm.ods_business_dt), MAX(dbm.mob_last_login_date)) <= 90)
               OR (MAX(dbm.olb_last_login_date) IS NOT NULL AND datediff(MAX(dbm.ods_business_dt), MAX(dbm.olb_last_login_date)) <= 90)
             THEN 'Digital Active' ELSE 'Non Digital Active' END AS digitally_active_flag,
        'DIGITAL' AS fact_type
    FROM {DMIB_DB}.digital_banking_master dbm
    WHERE dbm.ods_business_dt >= date('{START_DT}') AND dbm.ods_business_dt <= date('{END_DT}')
    GROUP BY TRUNC(dbm.ods_business_dt, 'MM'), CAST(dbm.rcif_customer_nbr AS string), dbm.ibn
""")

# Verify
total = spark.sql(f"SELECT COUNT(*) FROM {DEFAULT_DB}.Wealth_Insights_Customer").collect()[0][0]
wcnt = spark.sql(f"SELECT COUNT(*) FROM {DEFAULT_DB}.Wealth_Insights_Customer WHERE fact_type='WEALTH'").collect()[0][0]
dcnt = spark.sql(f"SELECT COUNT(*) FROM {DEFAULT_DB}.Wealth_Insights_Customer WHERE fact_type='DIGITAL'").collect()[0][0]
da = spark.sql(f"SELECT COUNT(DISTINCT cust_internet_banking_nbr) FROM {DEFAULT_DB}.Wealth_Insights_Customer WHERE fact_type='DIGITAL' AND digitally_active_flag='Digital Active'").collect()[0][0]
print(f"\n[RESULT] Total={total:,}, WEALTH={wcnt:,}, DIGITAL={dcnt:,}")
print(f"[RESULT] Top of Company Digital Active: {da:,}")

spark.stop()
print("DONE.")

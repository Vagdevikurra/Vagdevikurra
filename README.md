from pyspark.sql import SparkSession
from pyspark import SparkConf

# ── Configuration ────────────────────────────────────────────────────────────────
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

max_dig_dt = spark.sql(f"SELECT MAX(ods_business_dt) AS dt FROM {DMIB_DB}.digital_banking_master").collect()[0]["dt"]
cust_dt = spark.sql(f"SELECT MAX(CAST(business_date AS date)) AS dt FROM {EIL_DB}.d_involved_party_h WHERE source_system_code = 'CF'").collect()[0]["dt"]

print(f"[INFO] cust_dt    : {cust_dt}")
print(f"[INFO] max_dig_dt : {max_dig_dt}")
print(f"[INFO] Window     : {START_DT} .. {END_DT}")

# ══════════════════════════════════════════════════════════════════════════════════
# Single CREATE TABLE with all logic inlined as CTEs
# ══════════════════════════════════════════════════════════════════════════════════
print("\n[WRITE] Wealth_Insights_Customer ...")
spark.sql(f"DROP TABLE IF EXISTS {DEFAULT_DB}.Wealth_Insights_Customer")
spark.sql(f"""
    CREATE TABLE {DEFAULT_DB}.Wealth_Insights_Customer AS

    WITH month_ends AS (
        SELECT date('{START_DT}') AS month_start, add_months(date('{START_DT}'), 1) AS next_month_start
        UNION ALL SELECT add_months(date('{START_DT}'), 1), add_months(date('{START_DT}'), 2)
        UNION ALL SELECT add_months(date('{START_DT}'), 2), add_months(date('{START_DT}'), 3)
        UNION ALL SELECT add_months(date('{START_DT}'), 3), add_months(date('{START_DT}'), 4)
        UNION ALL SELECT add_months(date('{START_DT}'), 4), add_months(date('{START_DT}'), 5)
        UNION ALL SELECT add_months(date('{START_DT}'), 5), add_months(date('{START_DT}'), 6)
    ),

    all_biz_dates AS (
        SELECT DISTINCT bd FROM (
            SELECT CAST(business_date AS date) AS bd FROM {EIL_DB}.d_involved_party_h
            UNION ALL
            SELECT CAST(business_date AS date) AS bd FROM {EIL_DB}.d_arrangement_to_involved_party_relationship_h
            UNION ALL
            SELECT CAST(business_date AS date) AS bd FROM {EIL_DB}.d_arrangement_h
        ) x
    ),

    month_end_dates AS (
        SELECT MAX(a.bd) AS business_date
        FROM month_ends m
        LEFT JOIN all_biz_dates a ON a.bd >= m.month_start AND a.bd < m.next_month_start
        GROUP BY m.month_start
    ),

    -- Wealth base: arrangement grain
    wealth_arr AS (
        SELECT
            CAST(ind.business_date AS date) AS business_date,
            CAST(ind.rcif_cust_nbr AS string) AS rcif_number,
            ind.involved_party_id AS ip_id,
            ind.cust_internet_banking_nbr,
            ind.private_client_code,
            ind.private_client_trust_code,
            ar.arrangement_id,
            ar.source_system_code,
            ar.business_service_segment_type_code
        FROM {EIL_DB}.d_involved_party_h ind
        JOIN month_end_dates d ON CAST(ind.business_date AS date) = d.business_date
        JOIN {EIL_DB}.d_arrangement_to_involved_party_relationship_h a2i
            ON ind.involved_party_id = a2i.involved_party_id
            AND ind.business_date = a2i.business_date
            AND ind.source_system_code = a2i.source_system_code
        JOIN {EIL_DB}.d_arrangement_h ar
            ON a2i.arrangement_id = ar.arrangement_id
            AND a2i.arrangement_source_system_code = ar.source_system_code
            AND a2i.business_date = ar.business_date
            AND ar.source_system_code IN ({wealth_src_csv})
            AND ar.closed_ind = 'N'
        WHERE ind.source_system_code = 'CF'
          AND NVL(ind.deceased_ind, 'N') = 'N'
          AND (CASE
                  WHEN ind.private_client_code IN ('039','539','339') THEN 1
                  WHEN ind.private_client_trust_code IN ('239','739') THEN 1
                  ELSE CASE WHEN ar.business_service_segment_type_code
                       IN ('IS_CT','IS_IT','REGIS_FC','REGIS','PWM') THEN 1 ELSE 0 END
               END) = 1
    ),

    -- Wealth aggregate: RCIF grain
    wealth_agg AS (
        SELECT
            business_date, rcif_number,
            MAX(ip_id) AS ip_id,
            MAX(cust_internet_banking_nbr) AS cust_internet_banking_nbr,
            COUNT(DISTINCT concat_ws('|', source_system_code, CAST(arrangement_id AS string))) AS wealth_accts_cnt,
            COUNT(DISTINCT CASE WHEN business_service_segment_type_code = 'IS_CT' THEN arrangement_id END) AS corporate_trust_count,
            COUNT(DISTINCT CASE WHEN business_service_segment_type_code = 'IS_IT' THEN arrangement_id END) AS institutional_trust_count,
            COUNT(DISTINCT CASE WHEN business_service_segment_type_code = 'REGIS_FC' THEN arrangement_id END) AS investment_count,
            COUNT(DISTINCT CASE WHEN business_service_segment_type_code = 'REGIS' THEN arrangement_id END) AS insurance_count,
            COUNT(DISTINCT CASE WHEN business_service_segment_type_code = 'PWM' THEN arrangement_id END) AS pwm_count,
            COUNT(DISTINCT CASE WHEN source_system_code = 'TR' THEN arrangement_id END) AS trust_count,
            COUNT(DISTINCT CASE WHEN source_system_code IN ('DA','SV','CC','MG','LS','TM','LO','CM','CS','EL','IC','MA','PF','PR','SD','BI','RN')
                  THEN arrangement_id END) AS banking_count,
            MAX(CASE WHEN private_client_code IN ('039','539','339')
                       OR private_client_trust_code IN ('239','739') THEN 1 ELSE 0 END) AS private_flag
        FROM wealth_arr
        GROUP BY business_date, rcif_number
    ),

    -- Business group + division
    wealth_final AS (
        SELECT business_date, rcif_number, ip_id, cust_internet_banking_nbr, wealth_accts_cnt,
            CASE
                WHEN private_flag = 1 THEN 'Private Wealth'
                WHEN (corporate_trust_count + institutional_trust_count) > 0 THEN 'Institutional Services'
                WHEN (investment_count + insurance_count) > 0 THEN 'Investment Services'
                WHEN pwm_count > 0 THEN 'Private Wealth'
                ELSE 'Other'
            END AS business_group,
            CASE
                WHEN CASE WHEN private_flag = 1 THEN 'Private Wealth'
                          WHEN (corporate_trust_count + institutional_trust_count) > 0 THEN 'Institutional Services'
                          WHEN (investment_count + insurance_count) > 0 THEN 'Investment Services'
                          WHEN pwm_count > 0 THEN 'Private Wealth' ELSE 'Other' END = 'Private Wealth'
                THEN CASE WHEN trust_count > 0 AND banking_count > 0 THEN 'Banking & IMAT'
                          WHEN (investment_count + trust_count) > 0 AND banking_count = 0 THEN 'Investments Only'
                          ELSE 'Banking only' END
                WHEN CASE WHEN private_flag = 1 THEN 'Private Wealth'
                          WHEN (corporate_trust_count + institutional_trust_count) > 0 THEN 'Institutional Services'
                          WHEN (investment_count + insurance_count) > 0 THEN 'Investment Services'
                          WHEN pwm_count > 0 THEN 'Private Wealth' ELSE 'Other' END = 'Investment Services'
                THEN CASE WHEN investment_count > 0 AND insurance_count = 0 THEN 'Investment'
                          WHEN investment_count = 0 AND insurance_count > 0 THEN 'Insurance'
                          ELSE 'Insurance & Investment' END
                WHEN (corporate_trust_count > 0 AND institutional_trust_count = 0) THEN 'Corporate Trust'
                WHEN (corporate_trust_count = 0 AND institutional_trust_count > 0) THEN 'Institutional Trust'
                WHEN pwm_count > 0 THEN 'Banking only'
                ELSE 'Corporate & Institutional Trust'
            END AS division
        FROM wealth_agg
    ),

    -- RCIF bridge: birth_date NOT NULL, latest snapshot
    rcif_bridge AS (
        SELECT DISTINCT
            CAST(rcif_cust_nbr AS string) AS rcif_number,
            cust_internet_banking_nbr AS ibn
        FROM {EIL_DB}.d_involved_party_h
        WHERE CAST(business_date AS date) = date('{cust_dt}')
          AND source_system_code = 'CF'
          AND NVL(deceased_ind, 'N') = 'N'
          AND birth_date IS NOT NULL
    ),

    -- Digital: latest day ONLY (matching Power BI)
    dig_latest AS (
        SELECT
            ibn,
            MAX(olb_last_login_date) AS last_olb,
            MAX(mob_last_login_date) AS last_mob,
            MAX(ods_business_dt) AS ods_dt
        FROM {DMIB_DB}.digital_banking_master
        WHERE ods_business_dt = date('{max_dig_dt}')
        GROUP BY ibn
    ),

    -- Static digital flags per RCIF (through bridge)
    rcif_dig_flags AS (
        SELECT
            b.rcif_number,
            MAX(CASE WHEN d.last_mob IS NOT NULL THEN 1 ELSE 0 END) AS mob_user,
            MAX(CASE WHEN d.last_mob IS NOT NULL AND datediff(d.ods_dt, d.last_mob) <= 90 THEN 1 ELSE 0 END) AS mob_active,
            MAX(CASE WHEN d.last_olb IS NOT NULL THEN 1 ELSE 0 END) AS olb_user,
            MAX(CASE WHEN d.last_olb IS NOT NULL AND datediff(d.ods_dt, d.last_olb) <= 90 THEN 1 ELSE 0 END) AS olb_active,
            MAX(CASE WHEN d.ibn IS NOT NULL THEN 1 ELSE 0 END) AS digital_user,
            MAX(CASE WHEN (d.last_mob IS NOT NULL AND datediff(d.ods_dt, d.last_mob) <= 90)
                       OR (d.last_olb IS NOT NULL AND datediff(d.ods_dt, d.last_olb) <= 90) THEN 1 ELSE 0 END) AS digital_active
        FROM rcif_bridge b
        LEFT JOIN dig_latest d ON b.ibn = d.ibn
        GROUP BY b.rcif_number
    ),

    -- Digital monthly for DIGITAL fact_type rows
    digital_monthly AS (
        SELECT
            TRUNC(ods_business_dt, 'MM') AS month_dt,
            CAST(rcif_customer_nbr AS string) AS rcif_number,
            ibn,
            MAX(olb_last_login_date) AS last_olb,
            MAX(mob_last_login_date) AS last_mob,
            MAX(ods_business_dt) AS ods_dt
        FROM {DMIB_DB}.digital_banking_master
        WHERE ods_business_dt >= date('{START_DT}') AND ods_business_dt <= date('{END_DT}')
        GROUP BY TRUNC(ods_business_dt, 'MM'), CAST(rcif_customer_nbr AS string), ibn
    )

    -- WEALTH rows
    SELECT
        w.business_date,
        w.rcif_number,
        w.cust_internet_banking_nbr,
        w.ip_id,
        w.business_group,
        w.division,
        w.wealth_accts_cnt,
        CASE WHEN f.mob_user = 1 THEN 'Mobile User' ELSE 'Non Mobile User' END AS mobile_flag,
        CASE WHEN f.mob_active = 1 THEN 'Mobile Active' ELSE 'Non Mobile Active' END AS mobile_active_flag,
        CASE WHEN f.olb_user = 1 THEN 'OLB User' ELSE 'Non OLB User' END AS olb_flag,
        CASE WHEN f.olb_active = 1 THEN 'OLB Active' ELSE 'Non OLB Active' END AS olb_active_flag,
        CASE WHEN f.digital_user = 1 THEN 'Digital User' ELSE 'Non Digital User' END AS digital_flag,
        CASE WHEN f.digital_active = 1 THEN 'Digital Active' ELSE 'Non Digital Active' END AS digitally_active_flag,
        'WEALTH' AS fact_type
    FROM wealth_final w
    LEFT JOIN rcif_dig_flags f ON w.rcif_number = f.rcif_number

    UNION ALL

    -- DIGITAL rows
    SELECT
        CAST(month_dt AS date) AS business_date,
        rcif_number,
        ibn AS cust_internet_banking_nbr,
        CAST(NULL AS string) AS ip_id,
        CAST(NULL AS string) AS business_group,
        CAST(NULL AS string) AS division,
        CAST(NULL AS bigint) AS wealth_accts_cnt,
        CASE WHEN last_mob IS NOT NULL THEN 'Mobile User' ELSE 'Non Mobile User' END AS mobile_flag,
        CASE WHEN last_mob IS NOT NULL AND datediff(ods_dt, last_mob) <= 90
             THEN 'Mobile Active' ELSE 'Non Mobile Active' END AS mobile_active_flag,
        CASE WHEN last_olb IS NOT NULL THEN 'OLB User' ELSE 'Non OLB User' END AS olb_flag,
        CASE WHEN last_olb IS NOT NULL AND datediff(ods_dt, last_olb) <= 90
             THEN 'OLB Active' ELSE 'Non OLB Active' END AS olb_active_flag,
        'Digital User' AS digital_flag,
        CASE WHEN (last_mob IS NOT NULL AND datediff(ods_dt, last_mob) <= 90)
               OR (last_olb IS NOT NULL AND datediff(ods_dt, last_olb) <= 90)
             THEN 'Digital Active' ELSE 'Non Digital Active' END AS digitally_active_flag,
        'DIGITAL' AS fact_type
    FROM digital_monthly
""")

print(f"[OK] Saved {DEFAULT_DB}.Wealth_Insights_Customer")
spark.stop()
print("DONE.")

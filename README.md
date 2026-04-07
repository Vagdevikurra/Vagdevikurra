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

print("\n[WRITE] Wealth_Insights_Customer ...")
spark.sql(f"DROP TABLE IF EXISTS {DEFAULT_DB}.Wealth_Insights_Customer")
spark.sql(f"""
    CREATE TABLE {DEFAULT_DB}.Wealth_Insights_Customer AS

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
    ),
    month_ends AS (
        SELECT MAX(a.bd) AS business_date
        FROM cal m LEFT JOIN all_biz a ON a.bd >= m.ms AND a.bd < m.ns
        GROUP BY m.ms
    ),

    wealth_arr AS (
        SELECT
            CAST(ind.business_date AS date) AS business_date,
            CAST(ind.rcif_cust_nbr AS string) AS rcif_number,
            ind.involved_party_id AS ip_id,
            ind.cust_internet_banking_nbr,
            ind.private_client_code, ind.private_client_trust_code,
            ar.arrangement_id, ar.source_system_code,
            ar.business_service_segment_type_code
        FROM {EIL_DB}.d_involved_party_h ind
        JOIN month_ends d ON CAST(ind.business_date AS date) = d.business_date
        JOIN {EIL_DB}.d_arrangement_to_involved_party_relationship_h a2i
            ON ind.involved_party_id=a2i.involved_party_id AND ind.business_date=a2i.business_date AND ind.source_system_code=a2i.source_system_code
        JOIN {EIL_DB}.d_arrangement_h ar
            ON a2i.arrangement_id=ar.arrangement_id AND a2i.arrangement_source_system_code=ar.source_system_code AND a2i.business_date=ar.business_date
            AND ar.source_system_code IN ({wealth_src_csv}) AND ar.closed_ind='N'
        WHERE ind.source_system_code='CF' AND NVL(ind.deceased_ind,'N')='N'
          AND (CASE WHEN ind.private_client_code IN ('039','539','339') THEN 1
                    WHEN ind.private_client_trust_code IN ('239','739') THEN 1
                    ELSE CASE WHEN ar.business_service_segment_type_code IN ('IS_CT','IS_IT','REGIS_FC','REGIS','PWM') THEN 1 ELSE 0 END END)=1
    ),

    wealth_agg AS (
        SELECT business_date, rcif_number, MAX(ip_id) AS ip_id, MAX(cust_internet_banking_nbr) AS cust_internet_banking_nbr,
            COUNT(DISTINCT concat_ws('|',source_system_code,CAST(arrangement_id AS string))) AS wealth_accts_cnt,
            COUNT(DISTINCT CASE WHEN business_service_segment_type_code='IS_CT' THEN arrangement_id END) AS ct_cnt,
            COUNT(DISTINCT CASE WHEN business_service_segment_type_code='IS_IT' THEN arrangement_id END) AS it_cnt,
            COUNT(DISTINCT CASE WHEN business_service_segment_type_code='REGIS_FC' THEN arrangement_id END) AS inv_cnt,
            COUNT(DISTINCT CASE WHEN business_service_segment_type_code='REGIS' THEN arrangement_id END) AS ins_cnt,
            COUNT(DISTINCT CASE WHEN business_service_segment_type_code='PWM' THEN arrangement_id END) AS pwm_cnt,
            COUNT(DISTINCT CASE WHEN source_system_code='TR' THEN arrangement_id END) AS tr_cnt,
            COUNT(DISTINCT CASE WHEN source_system_code IN ('DA','SV','CC','MG','LS','TM','LO','CM','CS','EL','IC','MA','PF','PR','SD','BI','RN') THEN arrangement_id END) AS bk_cnt,
            MAX(CASE WHEN private_client_code IN ('039','539','339') OR private_client_trust_code IN ('239','739') THEN 1 ELSE 0 END) AS prv
        FROM wealth_arr GROUP BY business_date, rcif_number
    ),

    wealth_pop AS (
        SELECT business_date, rcif_number, ip_id, cust_internet_banking_nbr, wealth_accts_cnt,
            CASE WHEN prv=1 THEN 'Private Wealth'
                 WHEN (ct_cnt+it_cnt)>0 THEN 'Institutional Services'
                 WHEN (inv_cnt+ins_cnt)>0 THEN 'Investment Services'
                 WHEN pwm_cnt>0 THEN 'Private Wealth' ELSE 'Other' END AS business_group,
            CASE
                WHEN CASE WHEN prv=1 THEN 'Private Wealth' WHEN (ct_cnt+it_cnt)>0 THEN 'Institutional Services'
                     WHEN (inv_cnt+ins_cnt)>0 THEN 'Investment Services' WHEN pwm_cnt>0 THEN 'Private Wealth' ELSE 'Other' END = 'Private Wealth'
                THEN CASE WHEN tr_cnt>0 AND bk_cnt>0 THEN 'Banking & IMAT' WHEN (inv_cnt+tr_cnt)>0 AND bk_cnt=0 THEN 'Investments Only' ELSE 'Banking only' END
                WHEN CASE WHEN prv=1 THEN 'Private Wealth' WHEN (ct_cnt+it_cnt)>0 THEN 'Institutional Services'
                     WHEN (inv_cnt+ins_cnt)>0 THEN 'Investment Services' WHEN pwm_cnt>0 THEN 'Private Wealth' ELSE 'Other' END = 'Investment Services'
                THEN CASE WHEN inv_cnt>0 AND ins_cnt=0 THEN 'Investment' WHEN inv_cnt=0 AND ins_cnt>0 THEN 'Insurance' ELSE 'Insurance & Investment' END
                WHEN ct_cnt>0 AND it_cnt=0 THEN 'Corporate Trust'
                WHEN ct_cnt=0 AND it_cnt>0 THEN 'Institutional Trust'
                WHEN pwm_cnt>0 THEN 'Banking only'
                ELSE 'Corporate & Institutional Trust'
            END AS division
        FROM wealth_agg
    ),

    wealth_dedup AS (
        SELECT business_date, rcif_number, ip_id, cust_internet_banking_nbr,
               business_group, division, wealth_accts_cnt
        FROM (
            SELECT p.*, ROW_NUMBER() OVER (PARTITION BY business_date, rcif_number ORDER BY wealth_accts_cnt DESC) AS rn
            FROM wealth_pop p
        ) x WHERE rn=1
    ),

    rcif_bridge AS (
        SELECT DISTINCT CAST(rcif_cust_nbr AS string) AS rcif_number, cust_internet_banking_nbr AS ibn
        FROM {EIL_DB}.d_involved_party_h
        WHERE CAST(business_date AS date)=date('{cust_dt}') AND source_system_code='CF'
          AND NVL(deceased_ind,'N')='N' AND birth_date IS NOT NULL
    ),

    dig_latest AS (
        SELECT ibn, MAX(olb_last_login_date) AS last_olb, MAX(mob_last_login_date) AS last_mob, MAX(ods_business_dt) AS ods_dt
        FROM {DMIB_DB}.digital_banking_master WHERE ods_business_dt=date('{max_dig_dt}')
        GROUP BY ibn
    ),

    rcif_dig_flags AS (
        SELECT b.rcif_number,
            MAX(CASE WHEN d.last_mob IS NOT NULL THEN 1 ELSE 0 END) AS mob_user,
            MAX(CASE WHEN d.last_mob IS NOT NULL AND datediff(d.ods_dt,d.last_mob)<=90 THEN 1 ELSE 0 END) AS mob_active,
            MAX(CASE WHEN d.last_olb IS NOT NULL THEN 1 ELSE 0 END) AS olb_user,
            MAX(CASE WHEN d.last_olb IS NOT NULL AND datediff(d.ods_dt,d.last_olb)<=90 THEN 1 ELSE 0 END) AS olb_active,
            MAX(CASE WHEN d.ibn IS NOT NULL THEN 1 ELSE 0 END) AS digital_user,
            MAX(CASE WHEN (d.last_mob IS NOT NULL AND datediff(d.ods_dt,d.last_mob)<=90)
                       OR (d.last_olb IS NOT NULL AND datediff(d.ods_dt,d.last_olb)<=90) THEN 1 ELSE 0 END) AS digital_active
        FROM rcif_bridge b LEFT JOIN dig_latest d ON b.ibn=d.ibn
        GROUP BY b.rcif_number
    ),

    digital_monthly AS (
        SELECT TRUNC(ods_business_dt,'MM') AS month_dt,
               CAST(rcif_customer_nbr AS string) AS rcif_number, ibn,
               MAX(olb_last_login_date) AS last_olb, MAX(mob_last_login_date) AS last_mob, MAX(ods_business_dt) AS ods_dt
        FROM {DMIB_DB}.digital_banking_master
        WHERE ods_business_dt>=date('{START_DT}') AND ods_business_dt<=date('{END_DT}')
        GROUP BY TRUNC(ods_business_dt,'MM'), CAST(rcif_customer_nbr AS string), ibn
    )

    SELECT w.business_date, w.rcif_number, w.cust_internet_banking_nbr,
           CAST(w.ip_id AS string) AS ip_id, w.business_group, w.division, w.wealth_accts_cnt,
           CASE WHEN f.mob_user=1 THEN 'Mobile User' ELSE 'Non Mobile User' END AS mobile_flag,
           CASE WHEN f.mob_active=1 THEN 'Mobile Active' ELSE 'Non Mobile Active' END AS mobile_active_flag,
           CASE WHEN f.olb_user=1 THEN 'OLB User' ELSE 'Non OLB User' END AS olb_flag,
           CASE WHEN f.olb_active=1 THEN 'OLB Active' ELSE 'Non OLB Active' END AS olb_active_flag,
           CASE WHEN f.digital_user=1 THEN 'Digital User' ELSE 'Non Digital User' END AS digital_flag,
           CASE WHEN f.digital_active=1 THEN 'Digital Active' ELSE 'Non Digital Active' END AS digitally_active_flag,
           'WEALTH' AS fact_type
    FROM wealth_dedup w
    LEFT JOIN rcif_dig_flags f ON w.rcif_number=f.rcif_number

    UNION ALL

    SELECT CAST(month_dt AS date), rcif_number, ibn, CAST(NULL AS string), CAST(NULL AS string),
           CAST(NULL AS string), CAST(NULL AS bigint),
           CASE WHEN last_mob IS NOT NULL THEN 'Mobile User' ELSE 'Non Mobile User' END,
           CASE WHEN last_mob IS NOT NULL AND datediff(ods_dt,last_mob)<=90 THEN 'Mobile Active' ELSE 'Non Mobile Active' END,
           CASE WHEN last_olb IS NOT NULL THEN 'OLB User' ELSE 'Non OLB User' END,
           CASE WHEN last_olb IS NOT NULL AND datediff(ods_dt,last_olb)<=90 THEN 'OLB Active' ELSE 'Non OLB Active' END,
           'Digital User',
           CASE WHEN (last_mob IS NOT NULL AND datediff(ods_dt,last_mob)<=90)
                  OR (last_olb IS NOT NULL AND datediff(ods_dt,last_olb)<=90) THEN 'Digital Active' ELSE 'Non Digital Active' END,
           'DIGITAL'
    FROM digital_monthly
""")

print(f"[OK] Saved {DEFAULT_DB}.Wealth_Insights_Customer")
spark.stop()
print("DONE.")

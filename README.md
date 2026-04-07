from pyspark.sql import SparkSession
from pyspark import SparkConf

DEFAULT_DB = "dm_ib_dev"
EIL_DB     = "eil"
DMIB_DB    = "dm_ib"
START_DT   = "2025-09-01"
END_DT     = "2026-02-28"

WEALTH_SRC_LIST = [
    "BI", "RN", "TR", "DA", "SV", "CC", "LS", "MG", "TM",
    "LO", "CS", "IC", "MA", "PF", "PR", "SD", "CM", "EL"
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
print(f"[INFO] Window : {START_DT} .. {END_DT}")

print("\n[WRITE] Wealth_Insights_Customer ...")
spark.sql(f"DROP TABLE IF EXISTS {DEFAULT_DB}.Wealth_Insights_Customer")
spark.sql(f"""
    CREATE TABLE {DEFAULT_DB}.Wealth_Insights_Customer AS

    WITH cal AS (
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
    month_ends AS (
        SELECT MAX(a.bd) AS business_date
        FROM cal m LEFT JOIN all_biz_dates a ON a.bd >= m.month_start AND a.bd < m.next_month_start
        GROUP BY m.month_start
    ),

    Dig_Customer AS (
        SELECT
            DATE_ADD(ADD_MONTHS(TRUNC(dbm.ods_business_dt, 'MM'), 1), -1) AS ods_business_dt,
            dbm.ibn AS reltibn,
            CAST(dbm.rcif_customer_nbr AS string) AS rcif_customer_nbr,
            MAX(dbm.olb_last_login_date) AS lst_login_olb,
            MAX(dbm.mob_last_login_date) AS lst_login_mob
        FROM {DMIB_DB}.digital_banking_master dbm
        WHERE dbm.ods_business_dt >= date('{START_DT}')
          AND dbm.ods_business_dt <= date('{END_DT}')
        GROUP BY
            DATE_ADD(ADD_MONTHS(TRUNC(dbm.ods_business_dt, 'MM'), 1), -1),
            dbm.ibn, CAST(dbm.rcif_customer_nbr AS string)
    ),

    Digital_Agg AS (
        SELECT c.ods_business_dt, c.reltibn, c.rcif_customer_nbr,
            CASE WHEN datediff(c.ods_business_dt, c.lst_login_mob) <= 90 THEN 1 ELSE 0 END AS Mobile_Active_Flag,
            CASE WHEN c.lst_login_mob IS NULL THEN 0 ELSE 1 END AS Mobile_Flag,
            CASE WHEN datediff(c.ods_business_dt, c.lst_login_olb) <= 90 THEN 1 ELSE 0 END AS OLB_Active_Flag,
            CASE WHEN c.lst_login_olb IS NULL THEN 0 ELSE 1 END AS OLB_Flag,
            CASE WHEN datediff(c.ods_business_dt, c.lst_login_mob) <= 90
                   OR datediff(c.ods_business_dt, c.lst_login_olb) <= 90 THEN 1 ELSE 0 END AS Digital_Active_Flag,
            CASE WHEN c.lst_login_mob IS NOT NULL OR c.lst_login_olb IS NOT NULL THEN 1 ELSE 0 END AS Digital_User
        FROM Dig_Customer c
    ),

    pwl AS (
        SELECT
            CAST(ind.business_date AS date) AS business_date,
            CAST(ind.rcif_cust_nbr AS string) AS rcif_number,
            ind.cust_internet_banking_nbr, ind.involved_party_id AS ip_id,
            CASE
                WHEN ind.private_client_code IN ('039','539','339') THEN 'Private Wealth'
                WHEN ind.private_client_trust_code IN ('239','739') THEN 'Private Wealth'
                ELSE CASE
                    WHEN ar.business_service_segment_type_code IN ('IS_CT','IS_IT') THEN 'Institutional Services'
                    WHEN ar.business_service_segment_type_code IN ('REGIS_FC','REGIS') THEN 'Investment Services'
                    WHEN ar.business_service_segment_type_code = 'PWM' THEN 'Private Wealth'
                    ELSE concat(ar.business_service_segment_type_code, '-Category2/?')
                END
            END AS business_group,
            COUNT(DISTINCT CASE WHEN ar.business_service_segment_type_code = 'IS_CT' THEN ar.arrangement_id END) AS corporate_trust_count,
            COUNT(DISTINCT CASE WHEN ar.business_service_segment_type_code = 'IS_IT' THEN ar.arrangement_id END) AS institutional_trust_count,
            COUNT(DISTINCT CASE WHEN ar.business_service_segment_type_code = 'REGIS_FC' THEN ar.arrangement_id END) AS investment_count,
            COUNT(DISTINCT CASE WHEN ar.business_service_segment_type_code = 'REGIS' THEN ar.arrangement_id END) AS insurance_count,
            COUNT(DISTINCT CASE WHEN ar.business_service_segment_type_code = 'PWM' THEN ar.arrangement_id END) AS pwm_count,
            COUNT(DISTINCT CASE WHEN ar.source_system_code = 'TR' THEN ar.arrangement_id END) AS trust_count,
            COUNT(DISTINCT CASE WHEN ar.source_system_code IN ('DA','SV','CC','MG','LS','TM','LO','CM','CS','EL','IC','MA','PF','PR','SD') THEN ar.arrangement_id END) AS banking_count,
            COUNT(ar.arrangement_id) AS accts_cnt
        FROM {EIL_DB}.d_involved_party_h ind
        JOIN month_ends d ON CAST(ind.business_date AS date) = d.business_date
        JOIN {EIL_DB}.d_arrangement_to_involved_party_relationship_h a2i
            ON ind.involved_party_id = a2i.involved_party_id
            AND ind.business_date = a2i.business_date AND ind.source_system_code = a2i.source_system_code
        JOIN {EIL_DB}.d_arrangement_h ar
            ON a2i.arrangement_id = ar.arrangement_id
            AND a2i.arrangement_source_system_code = ar.source_system_code
            AND a2i.business_date = ar.business_date
            AND ar.source_system_code IN ({wealth_src_csv}) AND ar.closed_ind = 'N'
        WHERE ind.source_system_code = 'CF' AND NVL(ind.deceased_ind, 'N') = 'N'
          AND (CASE
                  WHEN ind.private_client_code IN ('039','539','339') THEN 1
                  WHEN ind.private_client_trust_code IN ('239','739') THEN 1
                  ELSE CASE WHEN ar.business_service_segment_type_code IN ('IS_CT','IS_IT','REGIS_FC','REGIS','PWM') THEN 1 ELSE 0 END
               END) = 1
        GROUP BY ind.business_date, ind.involved_party_id, ind.rcif_cust_nbr,
                 ind.cust_internet_banking_nbr, ar.business_service_segment_type_code,
                 ind.private_client_code, ind.private_client_trust_code
    ),

    Wealth_Pop AS (
        SELECT business_date, rcif_number, cust_internet_banking_nbr, ip_id,
               business_group, accts_cnt,
            CASE
                WHEN business_group = 'Private Wealth' THEN CASE
                    WHEN trust_count > 0 AND banking_count > 0 THEN 'Banking & IMAT'
                    WHEN investment_count + trust_count > 0 AND banking_count = 0 THEN 'Investments Only'
                    ELSE 'Banking only' END
                WHEN business_group = 'Investment Services' THEN CASE
                    WHEN investment_count > 0 AND insurance_count = 0 THEN 'Investment'
                    WHEN investment_count = 0 AND insurance_count > 0 THEN 'Insurance'
                    ELSE 'Insurance & Investment' END
                ELSE CASE
                    WHEN corporate_trust_count > 0 AND institutional_trust_count = 0 THEN 'Corporate Trust'
                    WHEN corporate_trust_count = 0 AND institutional_trust_count > 0 THEN 'Institutional Trust'
                    WHEN pwm_count > 0 THEN 'Banking only'
                    ELSE 'Corporate & Institutional Trust' END
            END AS division
        FROM pwl
    ),

    wealth_dedup AS (
        SELECT business_date, rcif_number, cust_internet_banking_nbr, ip_id,
               business_group, division, accts_cnt
        FROM (
            SELECT p.*, ROW_NUMBER() OVER (PARTITION BY business_date, rcif_number ORDER BY accts_cnt DESC) AS rn
            FROM Wealth_Pop p
        ) x WHERE rn = 1
    )

    SELECT w.business_date, w.rcif_number, w.cust_internet_banking_nbr,
           CAST(w.ip_id AS string) AS ip_id, w.business_group, w.division,
           CAST(w.accts_cnt AS bigint) AS wealth_accts_cnt,
           CASE WHEN d.Mobile_Flag = 1 THEN 'Mobile User' ELSE 'Non Mobile User' END AS mobile_flag,
           CASE WHEN d.Mobile_Active_Flag = 1 THEN 'Mobile Active' ELSE 'Non Mobile Active' END AS mobile_active_flag,
           CASE WHEN d.OLB_Flag = 1 THEN 'OLB User' ELSE 'Non OLB User' END AS olb_flag,
           CASE WHEN d.OLB_Active_Flag = 1 THEN 'OLB Active' ELSE 'Non OLB Active' END AS olb_active_flag,
           CASE WHEN d.Digital_User = 1 THEN 'Digital User' ELSE 'Non Digital User' END AS digital_flag,
           CASE WHEN d.Digital_Active_Flag = 1 THEN 'Digital Active' ELSE 'Non Digital Active' END AS digitally_active_flag,
           'WEALTH' AS fact_type
    FROM wealth_dedup w
    LEFT JOIN Digital_Agg d
        ON TRUNC(w.business_date, 'MM') = TRUNC(d.ods_business_dt, 'MM')
        AND w.rcif_number = d.rcif_customer_nbr

    UNION ALL

    SELECT CAST(ods_business_dt AS date), rcif_customer_nbr, reltibn,
           CAST(NULL AS string), CAST(NULL AS string), CAST(NULL AS string), CAST(NULL AS bigint),
           CASE WHEN Mobile_Flag = 1 THEN 'Mobile User' ELSE 'Non Mobile User' END,
           CASE WHEN Mobile_Active_Flag = 1 THEN 'Mobile Active' ELSE 'Non Mobile Active' END,
           CASE WHEN OLB_Flag = 1 THEN 'OLB User' ELSE 'Non OLB User' END,
           CASE WHEN OLB_Active_Flag = 1 THEN 'OLB Active' ELSE 'Non OLB Active' END,
           CASE WHEN Digital_User = 1 THEN 'Digital User' ELSE 'Non Digital User' END,
           CASE WHEN Digital_Active_Flag = 1 THEN 'Digital Active' ELSE 'Non Digital Active' END,
           'DIGITAL'
    FROM Digital_Agg
""")

print(f"[OK] Saved {DEFAULT_DB}.Wealth_Insights_Customer")
spark.stop()
print("DONE.")

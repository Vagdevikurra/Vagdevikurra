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

# ══════════════════════════════════════════════════════════════════════════════════
# Single CREATE TABLE — matching colleague's working code exactly
# ══════════════════════════════════════════════════════════════════════════════════
print("\n[WRITE] Wealth_Insights_Customer ...")
spark.sql(f"DROP TABLE IF EXISTS {DEFAULT_DB}.Wealth_Insights_Customer")
spark.sql(f"""
    CREATE TABLE {DEFAULT_DB}.Wealth_Insights_Customer AS

    -- ── Dig_Customer: 6-month rolling, per-month aggregation ────────────────────
    WITH Dig_Customer AS (
        SELECT
            DATE_ADD(ADD_MONTHS(TRUNC(dbm.ods_business_dt, 'MM'), 1), -1) AS ods_business_dt,
            dbm.ibn AS reltibn,
            dbm.rcif_customer_nbr,
            MAX(dbm.olb_last_login_date) AS lst_login_olb,
            MAX(dbm.mob_last_login_date) AS lst_login_mob
        FROM {DMIB_DB}.digital_banking_master dbm
        WHERE dbm.ods_business_dt >= date_sub(TRUNC(CAST('{END_DT}' AS date), 'MM'), 180)
          AND dbm.ods_business_dt < TRUNC(CAST('{END_DT}' AS date), 'MM')
        GROUP BY
            DATE_ADD(ADD_MONTHS(TRUNC(dbm.ods_business_dt, 'MM'), 1), -1),
            dbm.ibn,
            dbm.rcif_customer_nbr
    ),

    -- ── Digital_Agg: compute 0/1 flags per month per customer ───────────────────
    Digital_Agg AS (
        SELECT
            c.ods_business_dt,
            c.reltibn,
            c.rcif_customer_nbr,
            CASE WHEN datediff(c.ods_business_dt, c.lst_login_mob) <= 90 THEN 1 ELSE 0 END AS Mobile_Active_Flag,
            CASE WHEN c.lst_login_mob IS NULL THEN 0 ELSE 1 END AS Mobile_Flag,
            CASE WHEN datediff(c.ods_business_dt, c.lst_login_olb) <= 90 THEN 1 ELSE 0 END AS OLB_Active_Flag,
            CASE WHEN c.lst_login_olb IS NULL THEN 0 ELSE 1 END AS OLB_Flag,
            CASE WHEN datediff(c.ods_business_dt, c.lst_login_mob) <= 90
                   OR datediff(c.ods_business_dt, c.lst_login_olb) <= 90 THEN 1 ELSE 0 END AS Digital_Active_Flag,
            CASE WHEN c.lst_login_mob IS NOT NULL OR c.lst_login_olb IS NOT NULL THEN 1 ELSE 0 END AS Digital_User
        FROM Dig_Customer c
    ),

    -- ── last_ip_date: all business dates in last 6 months ───────────────────────
    last_ip_date AS (
        SELECT DISTINCT business_date AS last_dt
        FROM {EIL_DB}.d_involved_party_h
        WHERE CAST(business_date AS date) >= add_months(CAST('{END_DT}' AS date), -6)
    ),

    -- ── pwl: wealth base ────────────────────────────────────────────────────────
    pwl AS (
        SELECT
            CAST(ind.business_date AS date) AS business_date,
            ind.rcif_cust_nbr AS RCIF_NUMBER,
            ind.cust_internet_banking_nbr,
            ind.involved_party_id AS ip_id,
            CASE
                WHEN ind.private_client_code IN ('039','539','339') THEN 'Private Wealth'
                WHEN ind.private_client_trust_code IN ('239','739') THEN 'Private Wealth'
                ELSE CASE
                    WHEN ar.business_service_segment_type_code IN ('IS_CT','IS_IT') THEN 'Institutional Services'
                    WHEN ar.business_service_segment_type_code IN ('REGIS_FC','REGIS') THEN 'Investment Services'
                    WHEN ar.business_service_segment_type_code = 'PWM' THEN 'Private Wealth'
                    ELSE concat(ar.business_service_segment_type_code, '-Category2/?')
                END
            END AS Business_Group,
            COUNT(DISTINCT CASE WHEN ar.business_service_segment_type_code = 'IS_CT' THEN ar.arrangement_id END) AS Corporate_Trust_Count,
            COUNT(DISTINCT CASE WHEN ar.business_service_segment_type_code = 'IS_IT' THEN ar.arrangement_id END) AS Institutional_Trust_Count,
            COUNT(DISTINCT CASE WHEN ar.business_service_segment_type_code = 'REGIS_FC' THEN ar.arrangement_id END) AS Investment_Count,
            COUNT(DISTINCT CASE WHEN ar.business_service_segment_type_code = 'REGIS' THEN ar.arrangement_id END) AS Insurance_Count,
            COUNT(DISTINCT CASE WHEN ar.business_service_segment_type_code = 'PWM' THEN ar.arrangement_id END) AS PWM_Count,
            COUNT(DISTINCT CASE WHEN ar.source_system_code = 'TR' THEN ar.arrangement_id END) AS Trust_Count,
            COUNT(DISTINCT CASE WHEN ar.source_system_code IN ('DA','SV','CC','MG','LS','TM','LO','CM','CS','EL','IC','MA','PF','PR','SD') THEN ar.arrangement_id END) AS Banking_Count,
            COUNT(ar.arrangement_id) AS accts_Cnt
        FROM {EIL_DB}.d_involved_party_h ind
        INNER JOIN last_ip_date ON ind.business_date = last_ip_date.last_dt
        INNER JOIN {EIL_DB}.d_arrangement_to_involved_party_relationship_h a2i
            ON ind.involved_party_id = a2i.involved_party_id
            AND ind.business_date = a2i.business_date
            AND ind.source_system_code = a2i.source_system_code
        INNER JOIN {EIL_DB}.d_arrangement_h ar
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
                  ELSE CASE WHEN ar.business_service_segment_type_code IN ('IS_CT','IS_IT','REGIS_FC','REGIS','PWM') THEN 1 ELSE 0 END
               END) = 1
        GROUP BY ind.business_date, ind.involved_party_id, ind.rcif_cust_nbr,
                 ind.cust_internet_banking_nbr, ar.business_service_segment_type_code,
                 ind.private_client_code, ind.private_client_trust_code
    ),

    -- ── Wealth_Pop: apply division logic ────────────────────────────────────────
    Wealth_Pop AS (
        SELECT
            PWl.business_date,
            PWl.ip_id,
            PWl.cust_internet_banking_nbr,
            CAST(PWl.RCIF_NUMBER AS string) AS rcif_number,
            PWl.Business_Group,
            CASE
                WHEN PWl.Business_Group = 'Private Wealth' THEN CASE
                    WHEN PWl.Trust_Count > 0 AND PWl.Banking_Count > 0 THEN 'Banking & IMAT'
                    WHEN PWl.Investment_Count + PWl.Trust_Count > 0 AND PWl.Banking_Count = 0 THEN 'Investments Only'
                    ELSE 'Banking only' END
                WHEN PWl.Business_Group = 'Investment Services' THEN CASE
                    WHEN PWl.Investment_Count > 0 AND PWl.Insurance_Count = 0 THEN 'Investment'
                    WHEN PWl.Investment_Count = 0 AND PWl.Insurance_Count > 0 THEN 'Insurance'
                    ELSE 'Insurance & Investment' END
                ELSE CASE
                    WHEN PWl.Corporate_Trust_Count > 0 AND PWl.Institutional_Trust_Count = 0 THEN 'Corporate Trust'
                    WHEN PWl.Corporate_Trust_Count = 0 AND PWl.Institutional_Trust_Count > 0 THEN 'Institutional Trust'
                    WHEN PWl.PWM_Count > 0 THEN 'Banking only'
                    ELSE 'Corporate & Institutional Trust' END
            END AS division,
            PWl.accts_Cnt
        FROM pwl
    ),

    -- ── wealth_dedup: one row per (business_date, rcif_number) ──────────────────
    -- Keeps highest accts_cnt when customer appears in multiple LOBs
    wealth_dedup AS (
        SELECT *
        FROM (
            SELECT p.*,
                   ROW_NUMBER() OVER (PARTITION BY business_date, rcif_number ORDER BY accts_cnt DESC) AS rn
            FROM Wealth_Pop p
        ) x
        WHERE rn = 1
    )

    -- ── WEALTH rows: join deduped wealth to per-month digital flags ─────────────
    SELECT
        w.business_date,
        w.rcif_number,
        w.cust_internet_banking_nbr,
        CAST(w.ip_id AS string) AS ip_id,
        w.Business_Group AS business_group,
        w.division,
        CAST(w.accts_Cnt AS bigint) AS wealth_accts_cnt,
        CASE WHEN d.Mobile_Flag = 1 THEN 'Mobile User' ELSE 'Non Mobile User' END AS mobile_flag,
        CASE WHEN d.Mobile_Active_Flag = 1 THEN 'Mobile Active' ELSE 'Non Mobile Active' END AS mobile_active_flag,
        CASE WHEN d.OLB_Flag = 1 THEN 'OLB User' ELSE 'Non OLB User' END AS olb_flag,
        CASE WHEN d.OLB_Active_Flag = 1 THEN 'OLB Active' ELSE 'Non OLB Active' END AS olb_active_flag,
        CASE WHEN d.Digital_User = 1 THEN 'Digital User' ELSE 'Non Digital User' END AS digital_flag,
        CASE WHEN d.Digital_Active_Flag = 1 THEN 'Digital Active' ELSE 'Non Digital Active' END AS digitally_active_flag,
        'WEALTH' AS fact_type
    FROM wealth_dedup w
    LEFT JOIN Digital_Agg d
        ON w.business_date = d.ods_business_dt
        AND w.rcif_number = d.rcif_customer_nbr

    UNION ALL

    -- ── DIGITAL rows ────────────────────────────────────────────────────────────
    SELECT
        CAST(ods_business_dt AS date) AS business_date,
        rcif_customer_nbr AS rcif_number,
        reltibn AS cust_internet_banking_nbr,
        CAST(NULL AS string) AS ip_id,
        CAST(NULL AS string) AS business_group,
        CAST(NULL AS string) AS division,
        CAST(NULL AS bigint) AS wealth_accts_cnt,
        CASE WHEN Mobile_Flag = 1 THEN 'Mobile User' ELSE 'Non Mobile User' END AS mobile_flag,
        CASE WHEN Mobile_Active_Flag = 1 THEN 'Mobile Active' ELSE 'Non Mobile Active' END AS mobile_active_flag,
        CASE WHEN OLB_Flag = 1 THEN 'OLB User' ELSE 'Non OLB User' END AS olb_flag,
        CASE WHEN OLB_Active_Flag = 1 THEN 'OLB Active' ELSE 'Non OLB Active' END AS olb_active_flag,
        CASE WHEN Digital_User = 1 THEN 'Digital User' ELSE 'Non Digital User' END AS digital_flag,
        CASE WHEN Digital_Active_Flag = 1 THEN 'Digital Active' ELSE 'Non Digital Active' END AS digitally_active_flag,
        'DIGITAL' AS fact_type
    FROM Digital_Agg
""")

print(f"[OK] Saved {DEFAULT_DB}.Wealth_Insights_Customer")
spark.stop()
print("DONE.")


from pyspark.sql import SparkSession
from pyspark import SparkConf

# ── Configuration ────────────────────────────────────────────────────────────────
DEFAULT_DB = "dm_ib_dev"
EIL_DB     = "eil"
DMIB_DB    = "dm_ib"
START_DT   = "2025-09-01"
END_DT     = "2026-02-28"

WEALTH_SRC_LIST = [
    "BI", "TR", "DA", "SV", "CC", "MG", "LS", "TM", "LO",
    "CS", "IC", "MA", "PF", "PR", "SD", "CM", "EL", "RN"
]
APPLY_PRIMARY_OWNER_FILTER = False

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
primary_owner_pred = "1=1"
if APPLY_PRIMARY_OWNER_FILTER:
    primary_owner_pred = "COALESCE(a2i.relationship_role,'') = 'PRIMARY'"


# ── Snapshot dates ───────────────────────────────────────────────────────────────
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


# ── 1) Month-end business dates ─────────────────────────────────────────────────
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW month_ends AS
    SELECT MAX(CAST(business_date AS date)) AS business_date
    FROM {EIL_DB}.d_involved_party_h
    WHERE CAST(business_date AS date) >= date('{START_DT}')
      AND CAST(business_date AS date) <= date('{END_DT}')
    GROUP BY TRUNC(CAST(business_date AS date), 'MM')
    ORDER BY 1
""")
print("[OK] month_ends")


# ── 2) WEALTH base — arrangement grain ──────────────────────────────────────────
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW wealth_arr AS
    SELECT
        CAST(ind.business_date AS date)        AS business_date,
        CAST(ind.rcif_cust_nbr AS string)      AS rcif_number,
        ind.involved_party_id                  AS ip_id,
        ind.cust_internet_banking_nbr,
        ind.private_client_code,
        ind.private_client_trust_code,
        ar.arrangement_id,
        ar.source_system_code,
        ar.business_service_segment_type_code
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
    WHERE ind.source_system_code = 'CF'
      AND NVL(ind.deceased_ind, 'N') = 'N'
      AND ar.closed_ind = 'N'
      AND {primary_owner_pred}
      AND ar.source_system_code IN ({wealth_src_csv})
      AND (
            CASE
                WHEN ind.private_client_code      IN ('039','539','339') THEN 1
                WHEN ind.private_client_trust_code IN ('239','739')      THEN 1
                ELSE CASE
                    WHEN ar.business_service_segment_type_code
                         IN ('IS_CT','IS_IT','REGIS_FC','REGIS','PWM') THEN 1
                    ELSE 0
                END
            END
          ) = 1
""")
print("[OK] wealth_arr")


# ── 3) WEALTH aggregate — RCIF grain ────────────────────────────────────────────
spark.sql("""
    CREATE OR REPLACE TEMP VIEW wealth_agg AS
    WITH base AS (
        SELECT
            business_date,
            rcif_number,
            ip_id,
            cust_internet_banking_nbr,
            private_client_code,
            private_client_trust_code,
            source_system_code,
            business_service_segment_type_code,
            concat_ws('|', source_system_code, CAST(arrangement_id AS string)) AS acct_key
        FROM wealth_arr
    ),
    by_rcif AS (
        SELECT
            business_date,
            rcif_number,
            MAX(ip_id)                       AS ip_id,
            MAX(cust_internet_banking_nbr)   AS cust_internet_banking_nbr,
            COUNT(DISTINCT acct_key)         AS wealth_accts_cnt,

            COUNT(DISTINCT CASE WHEN business_service_segment_type_code = 'IS_CT'    THEN acct_key END) AS corporate_trust_count,
            COUNT(DISTINCT CASE WHEN business_service_segment_type_code = 'IS_IT'    THEN acct_key END) AS institutional_trust_count,
            COUNT(DISTINCT CASE WHEN business_service_segment_type_code = 'REGIS_FC' THEN acct_key END) AS investment_count,
            COUNT(DISTINCT CASE WHEN business_service_segment_type_code = 'REGIS'    THEN acct_key END) AS insurance_count,
            COUNT(DISTINCT CASE WHEN business_service_segment_type_code = 'PWM'      THEN acct_key END) AS pwm_count,
            COUNT(DISTINCT CASE WHEN source_system_code = 'TR'                       THEN acct_key END) AS trust_count,
            COUNT(DISTINCT CASE WHEN source_system_code IN (
                'DA','SV','CC','MG','LS','TM','LO','CM','CS','IC','MA','PF','PR','SD'
            ) THEN acct_key END) AS banking_count,

            MAX(CASE WHEN private_client_code      IN ('039','539','339') THEN 1
                     WHEN private_client_trust_code IN ('239','739')      THEN 1
                     ELSE 0 END) AS private_flag
        FROM base GROUP BY business_date, rcif_number
    )
    SELECT
        business_date,
        rcif_number,
        ip_id,
        cust_internet_banking_nbr,
        wealth_accts_cnt,
        corporate_trust_count,
        institutional_trust_count,
        investment_count,
        insurance_count,
        pwm_count,
        trust_count,
        banking_count,
        private_flag,

        CASE
            WHEN private_flag = 1 THEN 'Private Wealth'
            WHEN business_service_segment_type_code_group = 'IS' THEN 'Institutional Services'
            WHEN business_service_segment_type_code_group = 'INV' THEN 'Investment Services'
            WHEN pwm_count > 0 THEN 'Private Wealth'
            ELSE 'Other'
        END AS business_group,

        CASE
            WHEN private_flag = 1 THEN
                CASE
                    WHEN trust_count > 0 AND banking_count > 0 THEN 'Banking & IM&T'
                    WHEN investment_count + trust_count > 0 AND banking_count = 0 THEN 'Investments Only'
                    ELSE 'Banking only'
                END
            WHEN corporate_trust_count > 0 OR institutional_trust_count > 0
                 OR investment_count > 0 OR insurance_count > 0 THEN
                CASE
                    WHEN investment_count > 0 AND insurance_count = 0 THEN 'Investment'
                    WHEN investment_count = 0 AND insurance_count > 0 THEN 'Insurance'
                    WHEN investment_count > 0 AND insurance_count > 0 THEN 'Insurance & Investment'
                    WHEN corporate_trust_count > 0 AND institutional_trust_count = 0 THEN 'Corporate Trust'
                    WHEN corporate_trust_count = 0 AND institutional_trust_count > 0 THEN 'Institutional Trust'
                    WHEN pwm_count > 0 THEN 'Banking only'
                    ELSE 'Corporate & Institutional Trust'
                END
            ELSE NULL
        END AS division

    FROM (
        SELECT by_rcif.*,
            CASE
                WHEN corporate_trust_count > 0 OR institutional_trust_count > 0 THEN 'IS'
                WHEN investment_count > 0 OR insurance_count > 0 THEN 'INV'
                ELSE NULL
            END AS business_service_segment_type_code_group
        FROM by_rcif
    ) enriched
""")
print("[OK] wealth_agg")


# ── 3b) WEALTH DEDUP — one row per rcif per business_date ────────────────────────
# Matches colleague's wealth_dedup CTE: keep the row with the highest accts_cnt.
# This eliminates duplicate counting when a customer appears in multiple segments.
spark.sql("""
    CREATE OR REPLACE TEMP VIEW wealth_dedup AS
    SELECT *
    FROM (
        SELECT w.*,
               ROW_NUMBER() OVER (
                   PARTITION BY business_date, rcif_number
                   ORDER BY wealth_accts_cnt DESC
               ) AS rn
        FROM wealth_agg w
    ) ranked
    WHERE rn = 1
""")
print("[OK] wealth_dedup (1 row per rcif per month)")


# ── 4) Digital monthly ───────────────────────────────────────────────────────────
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW digital_monthly AS
    SELECT
        TRUNC(ods_business_dt, 'MM')            AS month_dt,
        CAST(rcif_customer_nbr AS string)       AS rcif_number,
        MAX(olb_last_login_date)                AS last_olb,
        MAX(mob_last_login_date)                AS last_mob,
        MAX(ods_business_dt)                    AS ods_dt
    FROM {DMIB_DB}.digital_banking_master
    WHERE ods_business_dt >= date('{START_DT}')
      AND ods_business_dt <= date('{END_DT}')
    GROUP BY TRUNC(ods_business_dt, 'MM'),
             CAST(rcif_customer_nbr AS string)
""")
print("[OK] digital_monthly")


# ── 5) Address snapshot ──────────────────────────────────────────────────────────
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW rcif_address AS
    WITH addr_ranked AS (
        SELECT
            involved_party_id AS ip_id, state_name,
            ROW_NUMBER() OVER (
                PARTITION BY involved_party_id
                ORDER BY NVL(state_name, '') DESC
            ) AS rn
        FROM {EIL_DB}.d_involved_party_address_h
        WHERE CAST(business_date AS date) = date('{addr_dt}')
    )
    SELECT ip_id, state_name FROM addr_ranked WHERE rn = 1
""")
print("[OK] rcif_address")


# ══════════════════════════════════════════════════════════════════════════════════
# WRITE: Customer table — uses wealth_dedup (not wealth_agg) to avoid duplicates
# ══════════════════════════════════════════════════════════════════════════════════
print("\n[WRITE] Customer table ...")
spark.sql(f"DROP TABLE IF EXISTS {DEFAULT_DB}.wealth_insights_customer")
spark.sql(f"""
    CREATE TABLE {DEFAULT_DB}.wealth_insights_customer AS

    -- WEALTH rows: join deduped wealth to digital on rcif + month
    SELECT
        w.business_date,
        w.rcif_number,
        w.cust_internet_banking_nbr,
        w.ip_id,
        w.business_group,
        w.division,
        w.wealth_accts_cnt,
        CASE WHEN d.last_mob IS NOT NULL THEN 'Mobile User'
             ELSE 'Non Mobile User' END AS mobile_flag,
        CASE WHEN d.last_mob IS NOT NULL AND datediff(d.ods_dt, d.last_mob) <= 90
             THEN 'Mobile Active' ELSE 'Non Mobile Active' END AS mobile_active_flag,
        CASE WHEN d.last_olb IS NOT NULL THEN 'OLB User'
             ELSE 'Non OLB User' END AS olb_flag,
        CASE WHEN d.last_olb IS NOT NULL AND datediff(d.ods_dt, d.last_olb) <= 90
             THEN 'OLB Active' ELSE 'Non OLB Active' END AS olb_active_flag,
        CASE WHEN d.rcif_number IS NOT NULL THEN 'Digital User'
             ELSE 'Non Digital User' END AS digital_flag,
        CASE WHEN (d.last_mob IS NOT NULL AND datediff(d.ods_dt, d.last_mob) <= 90)
               OR (d.last_olb IS NOT NULL AND datediff(d.ods_dt, d.last_olb) <= 90)
             THEN 'Digital Active' ELSE 'Non Digital Active' END AS digitally_active_flag,
        CASE WHEN d.rcif_number IS NOT NULL THEN 'WEALTH & DIGITAL'
             ELSE 'WEALTH' END AS fact_type

    FROM wealth_dedup w
    LEFT JOIN digital_monthly d
        ON  w.rcif_number = d.rcif_number
        AND TRUNC(w.business_date, 'MM') = d.month_dt

    UNION ALL

    -- DIGITAL rows (full company digital population, per month)
    SELECT
        CAST(month_dt AS date)   AS business_date,
        rcif_number,
        CAST(NULL AS string)     AS cust_internet_banking_nbr,
        CAST(NULL AS string)     AS ip_id,
        CAST(NULL AS string)     AS business_group,
        CAST(NULL AS string)     AS division,
        CAST(NULL AS bigint)     AS wealth_accts_cnt,
        CASE WHEN last_mob IS NOT NULL THEN 'Mobile User'
             ELSE 'Non Mobile User' END AS mobile_flag,
        CASE WHEN last_mob IS NOT NULL AND datediff(ods_dt, last_mob) <= 90
             THEN 'Mobile Active' ELSE 'Non Mobile Active' END AS mobile_active_flag,
        CASE WHEN last_olb IS NOT NULL THEN 'OLB User'
             ELSE 'Non OLB User' END AS olb_flag,
        CASE WHEN last_olb IS NOT NULL AND datediff(ods_dt, last_olb) <= 90
             THEN 'OLB Active' ELSE 'Non OLB Active' END AS olb_active_flag,
        'Digital User' AS digital_flag,
        CASE WHEN (last_mob IS NOT NULL AND datediff(ods_dt, last_mob) <= 90)
               OR (last_olb IS NOT NULL AND datediff(ods_dt, last_olb) <= 90)
             THEN 'Digital Active' ELSE 'Non Digital Active' END AS digitally_active_flag,
        'DIGITAL' AS fact_type
    FROM digital_monthly
""")

print(f"[OK] Saved {DEFAULT_DB}.wealth_insights_customer")
spark.stop()
print("DONE — Customer table complete. Now run Step 2 (Account table).")

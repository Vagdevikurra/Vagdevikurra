from pyspark.sql import SparkSession
from pyspark import SparkConf

DEFAULT_DB = "dm_ib_dev"
EIL_DB     = "eil"
DMIB_DB    = "dm_ib"
START_DT   = "2025-09-01"
END_DT     = "2026-02-28"

WEALTH_SRC_LIST = [
    "BI", "TR", "DA", "SV", "CC", "MG", "LS", "TM", "LO",
    "CS", "IC", "MA", "PF", "PR", "SD", "CM", "EL", "RN"
]
RCIF_SRC_LIST = [
    "DA", "SV", "CC", "MG", "LS", "TM", "PC", "LO", "BW",
    "CM", "CS", "EL", "IC", "MA", "PF", "PR", "SD", "TR",
    "BI", "RN", "IS_CT", "IS_IT", "PWM"
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
rcif_src_csv   = ",".join([f"'{s}'" for s in RCIF_SRC_LIST])
primary_owner_pred = "1=1"
if APPLY_PRIMARY_OWNER_FILTER:
    primary_owner_pred = "COALESCE(a2i.relationship_role,'') = 'PRIMARY'"

cust_dt = spark.sql(f"""
    SELECT MAX(CAST(business_date AS date)) AS dt
    FROM {EIL_DB}.d_involved_party_h
""").collect()[0]["dt"]

max_digital_dt = spark.sql(f"""
    SELECT MAX(ods_business_dt) AS dt
    FROM {DMIB_DB}.digital_banking_master
""").collect()[0]["dt"]

print(f"[INFO] cust_dt        : {cust_dt}")
print(f"[INFO] max_digital_dt : {max_digital_dt}")
print(f"[INFO] Window         : {START_DT} .. {END_DT}")

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

# ── WEALTH base ──────────────────────────────────────────────────────────────────
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW wealth_arr AS
    SELECT
        CAST(ind.business_date AS date)   AS business_date,
        CAST(ind.rcif_cust_nbr AS string) AS rcif_number,
        ind.involved_party_id             AS ip_id,
        ind.cust_internet_banking_nbr,
        ind.private_client_code,
        ind.private_client_trust_code,
        ar.arrangement_id, ar.source_system_code,
        ar.business_service_segment_type_code
    FROM {EIL_DB}.d_involved_party_h ind
    JOIN month_ends d ON CAST(ind.business_date AS date) = d.business_date
    JOIN {EIL_DB}.d_arrangement_to_involved_party_relationship_h a2i
        ON  ind.involved_party_id  = a2i.involved_party_id
        AND ind.business_date      = a2i.business_date
        AND ind.source_system_code = a2i.source_system_code
    JOIN {EIL_DB}.d_arrangement_h ar
        ON  a2i.arrangement_id                 = ar.arrangement_id
        AND a2i.arrangement_source_system_code = ar.source_system_code
        AND a2i.business_date                  = ar.business_date
    WHERE ind.source_system_code = 'CF'
      AND NVL(ind.deceased_ind, 'N') = 'N'
      AND ar.closed_ind = 'N'
      AND {primary_owner_pred}
      AND ar.source_system_code IN ({wealth_src_csv})
      AND (CASE
              WHEN ind.private_client_code      IN ('039','539','339') THEN 1
              WHEN ind.private_client_trust_code IN ('239','739')      THEN 1
              ELSE CASE WHEN ar.business_service_segment_type_code
                             IN ('IS_CT','IS_IT','REGIS_FC','REGIS','PWM') THEN 1 ELSE 0 END
           END) = 1
""")
print("[OK] wealth_arr")

# ── WEALTH aggregate ─────────────────────────────────────────────────────────────
spark.sql("""
    CREATE OR REPLACE TEMP VIEW wealth_agg AS
    WITH base AS (
        SELECT *, concat_ws('|', source_system_code, CAST(arrangement_id AS string)) AS acct_key
        FROM wealth_arr
    ),
    by_rcif AS (
        SELECT
            business_date, rcif_number,
            MAX(ip_id) AS ip_id, MAX(cust_internet_banking_nbr) AS cust_internet_banking_nbr,
            COUNT(DISTINCT acct_key) AS wealth_accts_cnt,
            COUNT(DISTINCT CASE WHEN business_service_segment_type_code = 'IS_CT'    THEN acct_key END) AS corporate_trust_count,
            COUNT(DISTINCT CASE WHEN business_service_segment_type_code = 'IS_IT'    THEN acct_key END) AS institutional_trust_count,
            COUNT(DISTINCT CASE WHEN business_service_segment_type_code = 'REGIS_FC' THEN acct_key END) AS investment_count,
            COUNT(DISTINCT CASE WHEN business_service_segment_type_code = 'REGIS'    THEN acct_key END) AS insurance_count,
            COUNT(DISTINCT CASE WHEN business_service_segment_type_code = 'PWM'      THEN acct_key END) AS pwm_count,
            COUNT(DISTINCT CASE WHEN source_system_code = 'TR' THEN acct_key END) AS trust_count,
            COUNT(DISTINCT CASE WHEN source_system_code IN
                  ('DA','SV','CC','MG','LS','TM','LO','CM','CS','EL','IC','MA','PF','PR','SD','BI','RN')
                  THEN acct_key END) AS banking_count,
            MAX(CASE WHEN private_client_code IN ('039','539','339')
                       OR private_client_trust_code IN ('239','739') THEN 1 ELSE 0 END) AS private_flag
        FROM base GROUP BY business_date, rcif_number
    )
    SELECT business_date, rcif_number, ip_id, cust_internet_banking_nbr, wealth_accts_cnt,
        CASE WHEN private_flag = 1 THEN 'Private Wealth'
             WHEN (corporate_trust_count + institutional_trust_count) > 0 THEN 'Institutional Services'
             WHEN (investment_count + insurance_count) > 0 THEN 'Investment Services'
             WHEN pwm_count > 0 THEN 'Private Wealth'
             ELSE 'Other' END AS business_group,
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
                      WHEN investment_count > 0 AND insurance_count > 0 THEN 'Insurance'
                      ELSE 'Insurance & Investment' END
            WHEN (corporate_trust_count > 0 AND institutional_trust_count = 0) THEN 'Corporate Trust'
            WHEN (corporate_trust_count = 0 AND institutional_trust_count > 0) THEN 'Institutional Trust'
            WHEN pwm_count > 0 THEN 'Banking only'
            ELSE 'Corporate & Institutional Trust'
        END AS division
    FROM by_rcif
""")
print("[OK] wealth_agg")

# ══════════════════════════════════════════════════════════════════════════════════
# RCIF_Dig — EXACTLY matching Power BI GitHub query
# Dig_Customer: ONLY max(ods_business_dt) single day
# rc: INNER JOIN address, broader source list, NO birth_date filter
# RCIF_Dig: rc LEFT JOIN Dig_Customer ON cust_internet_banking_nbr = ibn
# ══════════════════════════════════════════════════════════════════════════════════
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW rcif_dig AS
    WITH dig_customer AS (
        SELECT
            ibn                        AS reltibn,
            MAX(olb_last_login_date)   AS lst_login_olb,
            MAX(mob_last_login_date)   AS lst_login_mob,
            ods_business_dt
        FROM {DMIB_DB}.digital_banking_master
        WHERE ods_business_dt = date('{max_digital_dt}')
        GROUP BY ibn, ods_business_dt
    ),
    rc AS (
        SELECT
            MAX(CAST(ip.rcif_cust_nbr AS string)) AS rcif_number,
            ip.involved_party_id,
            ip.cust_internet_banking_nbr
        FROM {EIL_DB}.d_involved_party_h ip
        INNER JOIN {EIL_DB}.d_arrangement_to_involved_party_relationship_h a2i
            ON  ip.business_date       = a2i.business_date
            AND ip.source_system_code  = a2i.source_system_code
            AND ip.involved_party_id   = a2i.involved_party_id
        INNER JOIN {EIL_DB}.d_arrangement_h ar
            ON  a2i.business_date                  = ar.business_date
            AND a2i.arrangement_source_system_code = ar.source_system_code
            AND a2i.arrangement_id                 = ar.arrangement_id
            AND ar.source_system_code IN ({rcif_src_csv})
        INNER JOIN {EIL_DB}.d_involved_party_address_h addr
            ON  ip.involved_party_id = addr.involved_party_id
            AND ip.business_date     = addr.business_date
        WHERE ip.business_date = (SELECT MAX(business_date) FROM {EIL_DB}.d_involved_party_h)
          AND ip.source_system_code = 'CF'
          AND NVL(ip.deceased_ind, 'N') = 'N'
        GROUP BY ip.involved_party_id, ip.cust_internet_banking_nbr
    )
    SELECT
        rc.rcif_number,
        rc.cust_internet_banking_nbr,
        c.reltibn,
        CASE WHEN c.lst_login_mob IS NOT NULL AND datediff(c.ods_business_dt, c.lst_login_mob) <= 90
             THEN 'Mobile Active' ELSE 'Non Mobile Active' END AS mobile_active_flag,
        CASE WHEN c.lst_login_mob IS NULL THEN 'Non Mobile User'
             ELSE 'Mobile User' END AS mobile_flag,
        CASE WHEN c.lst_login_olb IS NOT NULL AND datediff(c.ods_business_dt, c.lst_login_olb) <= 90
             THEN 'OLB Active' ELSE 'Non OLB Active' END AS olb_active_flag,
        CASE WHEN c.lst_login_olb IS NULL THEN 'Non OLB User'
             ELSE 'OLB User' END AS olb_flag,
        CASE WHEN (c.lst_login_mob IS NOT NULL AND datediff(c.ods_business_dt, c.lst_login_mob) <= 90)
               OR (c.lst_login_olb IS NOT NULL AND datediff(c.ods_business_dt, c.lst_login_olb) <= 90)
             THEN 'Digital Active' ELSE 'Non Digital Active' END AS digitally_active_flag,
        CASE WHEN c.reltibn IS NULL THEN 'Non Digital User'
             ELSE 'Digital User' END AS digital_flag
    FROM rc
    LEFT JOIN dig_customer c
        ON rc.cust_internet_banking_nbr = c.reltibn
""")
print(f"[OK] rcif_dig (digital snapshot: {max_digital_dt})")

# ── Digital monthly for DIGITAL rows ─────────────────────────────────────────────
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW digital_monthly AS
    SELECT
        TRUNC(ods_business_dt, 'MM') AS month_dt,
        CAST(rcif_customer_nbr AS string) AS rcif_number,
        ibn,
        MAX(olb_last_login_date)  AS last_olb,
        MAX(mob_last_login_date)  AS last_mob,
        MAX(ods_business_dt)      AS ods_dt
    FROM {DMIB_DB}.digital_banking_master
    WHERE ods_business_dt >= date('{START_DT}') AND ods_business_dt <= date('{END_DT}')
    GROUP BY TRUNC(ods_business_dt, 'MM'), CAST(rcif_customer_nbr AS string), ibn
""")
print("[OK] digital_monthly")

# ══════════════════════════════════════════════════════════════════════════════════
# WRITE: Customer table
# ══════════════════════════════════════════════════════════════════════════════════
print("\n[WRITE] Customer table ...")
spark.sql(f"DROP TABLE IF EXISTS {DEFAULT_DB}.wealth_insights_cust7")
spark.sql(f"""
    CREATE TABLE {DEFAULT_DB}.wealth_insights_cust7 AS
    SELECT
        w.business_date,
        w.rcif_number,
        w.cust_internet_banking_nbr,
        w.ip_id,
        w.business_group,
        w.division,
        w.wealth_accts_cnt,
        COALESCE(r.mobile_flag,          'Non Mobile User')   AS mobile_flag,
        COALESCE(r.mobile_active_flag,   'Non Mobile Active') AS mobile_active_flag,
        COALESCE(r.olb_flag,             'Non OLB User')      AS olb_flag,
        COALESCE(r.olb_active_flag,      'Non OLB Active')    AS olb_active_flag,
        COALESCE(r.digital_flag,         'Non Digital User')  AS digital_flag,
        COALESCE(r.digitally_active_flag,'Non Digital Active') AS digitally_active_flag,
        'WEALTH' AS fact_type
    FROM wealth_agg w
    LEFT JOIN rcif_dig r
        ON w.rcif_number = r.rcif_number

    UNION ALL

    SELECT
        CAST(d.month_dt AS date)   AS business_date,
        d.rcif_number,
        d.ibn                      AS cust_internet_banking_nbr,
        CAST(NULL AS string)       AS ip_id,
        CAST(NULL AS string)       AS business_group,
        CAST(NULL AS string)       AS division,
        CAST(NULL AS bigint)       AS wealth_accts_cnt,
        CASE WHEN d.last_mob IS NOT NULL THEN 'Mobile User' ELSE 'Non Mobile User' END AS mobile_flag,
        CASE WHEN d.last_mob IS NOT NULL AND datediff(d.ods_dt, d.last_mob) <= 90
             THEN 'Mobile Active' ELSE 'Non Mobile Active' END AS mobile_active_flag,
        CASE WHEN d.last_olb IS NOT NULL THEN 'OLB User' ELSE 'Non OLB User' END AS olb_flag,
        CASE WHEN d.last_olb IS NOT NULL AND datediff(d.ods_dt, d.last_olb) <= 90
             THEN 'OLB Active' ELSE 'Non OLB Active' END AS olb_active_flag,
        'Digital User' AS digital_flag,
        CASE WHEN (d.last_mob IS NOT NULL AND datediff(d.ods_dt, d.last_mob) <= 90)
               OR (d.last_olb IS NOT NULL AND datediff(d.ods_dt, d.last_olb) <= 90)
             THEN 'Digital Active' ELSE 'Non Digital Active' END AS digitally_active_flag,
        'DIGITAL' AS fact_type
    FROM digital_monthly d
    WHERE EXISTS (SELECT 1 FROM rcif_dig r WHERE r.cust_internet_banking_nbr = d.ibn)
""")

print(f"[OK] Saved {DEFAULT_DB}.wealth_insights_cust7")
spark.stop()
print("DONE.")


from pyspark.sql import SparkSession
from pyspark import SparkConf

DEFAULT_DB = "dm_ib_dev"
EIL_DB     = "eil"
START_DT   = "2025-09-01"
END_DT     = "2026-02-28"
APPLY_PRIMARY_OWNER_FILTER = False

RCIF_SRC_LIST = [
    "DA", "SV", "CC", "MG", "LS", "TM", "PC", "LO", "BW",
    "CM", "CS", "EL", "IC", "MA", "PF", "PR", "SD", "TR",
    "BI", "RN", "IS_CT", "IS_IT", "PWM"
]

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

rcif_src_csv = ",".join([f"'{s}'" for s in RCIF_SRC_LIST])
primary_owner_pred = "1=1"
if APPLY_PRIMARY_OWNER_FILTER:
    primary_owner_pred = "COALESCE(a2i.relationship_role,'') = 'PRIMARY'"

addr_dt = spark.sql(f"""
    SELECT MAX(CAST(business_date AS date)) AS dt
    FROM {EIL_DB}.d_involved_party_address_h
""").collect()[0]["dt"]

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

# ── RCIF bridge (same as Power BI: INNER JOIN address, no birth_date filter) ─────
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW rcif_bridge AS
    SELECT DISTINCT
        ip.involved_party_id AS ip_id
    FROM {EIL_DB}.d_involved_party_h ip
    INNER JOIN {EIL_DB}.d_arrangement_to_involved_party_relationship_h a2i
        ON  ip.business_date       = a2i.business_date
        AND ip.source_system_code  = a2i.source_system_code
        AND ip.involved_party_id   = a2i.involved_party_id
    INNER JOIN {EIL_DB}.d_arrangement_h ar
        ON  a2i.business_date                  = ar.business_date
        AND a2i.arrangement_source_system_code = ar.source_system_code
        AND a2i.arrangement_id                 = ar.arrangement_id
        AND ar.source_system_code IN ({rcif_src_csv})
    INNER JOIN {EIL_DB}.d_involved_party_address_h addr
        ON  ip.involved_party_id = addr.involved_party_id
        AND ip.business_date     = addr.business_date
    WHERE ip.business_date = (SELECT MAX(business_date) FROM {EIL_DB}.d_involved_party_h)
      AND ip.source_system_code = 'CF'
      AND NVL(ip.deceased_ind, 'N') = 'N'
""")
print("[OK] rcif_bridge")

# ══════════════════════════════════════════════════════════════════════════════════
# WRITE: Account table — filtered through rcif_bridge
# ══════════════════════════════════════════════════════════════════════════════════
print("\n[WRITE] Account table ...")
spark.sql(f"DROP TABLE IF EXISTS {DEFAULT_DB}.wealth_insights_acct7")
spark.sql(f"""
    CREATE TABLE {DEFAULT_DB}.wealth_insights_acct7 AS
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

print(f"[OK] Saved {DEFAULT_DB}.wealth_insights_acct7")
spark.stop()
print("DONE.")





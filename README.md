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

cust_dt = spark.sql(f"""
    SELECT MAX(CAST(business_date AS date)) AS dt
    FROM {EIL_DB}.d_involved_party_h WHERE source_system_code = 'CF'
""").collect()[0]["dt"]

max_dig_dt = spark.sql(f"""
    SELECT MAX(ods_business_dt) AS dt FROM {DMIB_DB}.digital_banking_master
""").collect()[0]["dt"]

print(f"[INFO] cust_dt    : {cust_dt}")
print(f"[INFO] max_dig_dt : {max_dig_dt}")
print(f"[INFO] Window     : {START_DT} .. {END_DT}")

# ── Month-end business dates ─────────────────────────────────────────────────────
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW month_ends AS
    WITH cal AS (
        SELECT date('{START_DT}') AS month_start, add_months(date('{START_DT}'), 1) AS next_month_start
        UNION ALL SELECT add_months(date('{START_DT}'), 1), add_months(date('{START_DT}'), 2)
        UNION ALL SELECT add_months(date('{START_DT}'), 2), add_months(date('{START_DT}'), 3)
        UNION ALL SELECT add_months(date('{START_DT}'), 3), add_months(date('{START_DT}'), 4)
        UNION ALL SELECT add_months(date('{START_DT}'), 4), add_months(date('{START_DT}'), 5)
        UNION ALL SELECT add_months(date('{START_DT}'), 5), add_months(date('{START_DT}'), 6)
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

# ── RCIF bridge (birth_date NOT NULL, NO address join) ───────────────────────────
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW rcif_bridge AS
    SELECT DISTINCT
        CAST(rcif_cust_nbr AS string)      AS rcif_number,
        cust_internet_banking_nbr          AS ibn
    FROM {EIL_DB}.d_involved_party_h
    WHERE CAST(business_date AS date) = date('{cust_dt}')
      AND source_system_code = 'CF'
      AND NVL(deceased_ind, 'N') = 'N'
      AND birth_date IS NOT NULL
""")
print("[OK] rcif_bridge")

# ── Digital flags — LATEST DAY ONLY ──────────────────────────────────────────────
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW dig_latest AS
    SELECT
        ibn,
        MAX(olb_last_login_date)  AS last_olb,
        MAX(mob_last_login_date)  AS last_mob,
        MAX(ods_business_dt)      AS ods_dt
    FROM {DMIB_DB}.digital_banking_master
    WHERE ods_business_dt = date('{max_dig_dt}')
    GROUP BY ibn
""")
print(f"[OK] dig_latest ({max_dig_dt})")

# ── Static flags per RCIF ────────────────────────────────────────────────────────
spark.sql("""
    CREATE OR REPLACE TEMP VIEW rcif_dig_flags AS
    SELECT
        b.rcif_number,
        MAX(CASE WHEN d.last_mob IS NOT NULL THEN 1 ELSE 0 END) AS mob_user,
        MAX(CASE WHEN d.last_mob IS NOT NULL AND datediff(d.ods_dt, d.last_mob) <= 90
                 THEN 1 ELSE 0 END) AS mob_active,
        MAX(CASE WHEN d.last_olb IS NOT NULL THEN 1 ELSE 0 END) AS olb_user,
        MAX(CASE WHEN d.last_olb IS NOT NULL AND datediff(d.ods_dt, d.last_olb) <= 90
                 THEN 1 ELSE 0 END) AS olb_active,
        MAX(CASE WHEN d.ibn IS NOT NULL THEN 1 ELSE 0 END) AS digital_user,
        MAX(CASE WHEN (d.last_mob IS NOT NULL AND datediff(d.ods_dt, d.last_mob) <= 90)
                   OR (d.last_olb IS NOT NULL AND datediff(d.ods_dt, d.last_olb) <= 90)
                 THEN 1 ELSE 0 END) AS digital_active
    FROM rcif_bridge b
    LEFT JOIN dig_latest d ON b.ibn = d.ibn
    GROUP BY b.rcif_number
""")
print("[OK] rcif_dig_flags")

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
# WRITE
# ══════════════════════════════════════════════════════════════════════════════════
print("\n[WRITE] Customer table ...")
spark.sql(f"DROP TABLE IF EXISTS {DEFAULT_DB}.wealth_insights_cust")
spark.sql(f"""
    CREATE TABLE {DEFAULT_DB}.wealth_insights_cust AS

    SELECT
        w.business_date, w.rcif_number, w.cust_internet_banking_nbr,
        w.ip_id, w.business_group, w.division, w.wealth_accts_cnt,
        CASE WHEN f.mob_user     = 1 THEN 'Mobile User'    ELSE 'Non Mobile User'    END AS mobile_flag,
        CASE WHEN f.mob_active   = 1 THEN 'Mobile Active'  ELSE 'Non Mobile Active'  END AS mobile_active_flag,
        CASE WHEN f.olb_user     = 1 THEN 'OLB User'       ELSE 'Non OLB User'       END AS olb_flag,
        CASE WHEN f.olb_active   = 1 THEN 'OLB Active'     ELSE 'Non OLB Active'     END AS olb_active_flag,
        CASE WHEN f.digital_user = 1 THEN 'Digital User'   ELSE 'Non Digital User'   END AS digital_flag,
        CASE WHEN f.digital_active=1 THEN 'Digital Active'  ELSE 'Non Digital Active'  END AS digitally_active_flag,
        'WEALTH' AS fact_type
    FROM wealth_agg w
    LEFT JOIN rcif_dig_flags f ON w.rcif_number = f.rcif_number

    UNION ALL

    SELECT
        CAST(month_dt AS date) AS business_date,
        rcif_number, ibn AS cust_internet_banking_nbr,
        CAST(NULL AS string) AS ip_id, CAST(NULL AS string) AS business_group,
        CAST(NULL AS string) AS division, CAST(NULL AS bigint) AS wealth_accts_cnt,
        CASE WHEN last_mob IS NOT NULL THEN 'Mobile User' ELSE 'Non Mobile User' END,
        CASE WHEN last_mob IS NOT NULL AND datediff(ods_dt, last_mob) <= 90
             THEN 'Mobile Active' ELSE 'Non Mobile Active' END,
        CASE WHEN last_olb IS NOT NULL THEN 'OLB User' ELSE 'Non OLB User' END,
        CASE WHEN last_olb IS NOT NULL AND datediff(ods_dt, last_olb) <= 90
             THEN 'OLB Active' ELSE 'Non OLB Active' END,
        'Digital User',
        CASE WHEN (last_mob IS NOT NULL AND datediff(ods_dt, last_mob) <= 90)
               OR (last_olb IS NOT NULL AND datediff(ods_dt, last_olb) <= 90)
             THEN 'Digital Active' ELSE 'Non Digital Active' END,
        'DIGITAL' AS fact_type
    FROM digital_monthly
""")

print(f"[OK] Saved {DEFAULT_DB}.wealth_insights_cust")
spark.stop()
print("DONE.")

from pyspark.sql import SparkSession
from pyspark import SparkConf

DEFAULT_DB = "dm_ib_dev"
EIL_DB     = "eil"

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

addr_dt = spark.sql(f"""
    SELECT MAX(CAST(business_date AS date)) AS dt
    FROM {EIL_DB}.d_involved_party_address_h
""").collect()[0]["dt"]

print(f"[INFO] addr_dt : {addr_dt}")

# ══════════════════════════════════════════════════════════════════════════════════
# InvestPath — EXACTLY matching Power BI:
#   Lst_Date = MAX(business_date), latest snapshot only
#   No birth_date, no bridge, no address filter
#   CF, not deceased, RN, IP, closed_ind = 'N'
# ══════════════════════════════════════════════════════════════════════════════════
print("\n[WRITE] Account table ...")
spark.sql(f"DROP TABLE IF EXISTS {DEFAULT_DB}.wealth_insights_acct")
spark.sql(f"""
    CREATE TABLE {DEFAULT_DB}.wealth_insights_acct AS
    WITH inv AS (
        SELECT
            CAST(ind.rcif_cust_nbr AS STRING)                      AS rcif_number,
            ind.involved_party_id                                  AS ip_id,
            CAST(COALESCE(ar.current_balance_amt, 0.0) AS DOUBLE) AS ip_balance,
            CAST(ar.open_date AS DATE)                             AS ip_open_date,
            CAST(ar.arrangement_id AS STRING)                      AS accounts
        FROM {EIL_DB}.d_involved_party_h ind
        INNER JOIN (
            SELECT MAX(business_date) AS last_date FROM {EIL_DB}.d_involved_party_h
        ) lst ON ind.business_date = lst.last_date
        INNER JOIN {EIL_DB}.d_arrangement_to_involved_party_relationship_h a2i
            ON  ind.involved_party_id   = a2i.involved_party_id
            AND ind.business_date       = a2i.business_date
            AND ind.source_system_code  = a2i.source_system_code
        INNER JOIN {EIL_DB}.d_arrangement_h ar
            ON  a2i.arrangement_id                 = ar.arrangement_id
            AND a2i.arrangement_source_system_code = ar.source_system_code
            AND a2i.business_date                  = ar.business_date
            AND ar.closed_ind              = 'N'
            AND ar.account_type_code       = 'IP'
            AND ar.source_system_code      = 'RN'
        WHERE ind.source_system_code     = 'CF'
          AND NVL(ind.deceased_ind, 'N') = 'N'
    )
    SELECT
        inv.rcif_number,
        inv.ip_id,
        inv.ip_balance,
        inv.ip_open_date,
        inv.accounts,
        addr.state_name
    FROM inv
    LEFT JOIN (
        SELECT involved_party_id AS ip_id, state_name
        FROM (
            SELECT involved_party_id, state_name,
                   ROW_NUMBER() OVER (PARTITION BY involved_party_id ORDER BY NVL(state_name, '') DESC) AS rn
            FROM {EIL_DB}.d_involved_party_address_h
            WHERE CAST(business_date AS date) = date('{addr_dt}')
        ) ranked WHERE rn = 1
    ) addr ON inv.ip_id = addr.ip_id
""")

print(f"[OK] Saved {DEFAULT_DB}.wealth_insights_acct")
spark.stop()
print("DONE.")

from pyspark.sql import SparkSession

DEFAULT_DB = "dm_ib_dev"

spark = (
    SparkSession.builder
    .appName("wealth_insights_validation")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .enableHiveSupport()
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("=" * 80)
print("  WEALTH INSIGHTS — VALIDATION")
print("=" * 80)

# ── 1) Row counts ────────────────────────────────────────────────────────────────
print("\n[1] Row counts:")
cust_total  = spark.sql(f"SELECT COUNT(*) AS c FROM {DEFAULT_DB}.wealth_insights_cust").collect()[0]["c"]
wealth_rows = spark.sql(f"SELECT COUNT(*) AS c FROM {DEFAULT_DB}.wealth_insights_cust WHERE fact_type = 'WEALTH'").collect()[0]["c"]
digital_rows = spark.sql(f"SELECT COUNT(*) AS c FROM {DEFAULT_DB}.wealth_insights_cust WHERE fact_type = 'DIGITAL'").collect()[0]["c"]
acct_rows   = spark.sql(f"SELECT COUNT(*) AS c FROM {DEFAULT_DB}.wealth_insights_acct").collect()[0]["c"]
print(f"  Customer total  : {cust_total:>12,}")
print(f"  WEALTH rows     : {wealth_rows:>12,}")
print(f"  DIGITAL rows    : {digital_rows:>12,}")
print(f"  Account rows    : {acct_rows:>12,}")

# ── 2) Wealth customers per month ────────────────────────────────────────────────
print("\n[2] Wealth customers per month (expect ~265-267k):")
spark.sql(f"""
    SELECT business_date, COUNT(DISTINCT rcif_number) AS wealth_customers
    FROM {DEFAULT_DB}.wealth_insights_cust WHERE fact_type = 'WEALTH'
    GROUP BY business_date ORDER BY business_date
""").show(truncate=False)

# ── 3) Top of Company Digital Active ─────────────────────────────────────────────
print("[3] Top of Company Digital Active (expect ~3.4M):")
spark.sql(f"""
    SELECT COUNT(DISTINCT cust_internet_banking_nbr) AS total_digital_active
    FROM {DEFAULT_DB}.wealth_insights_cust
    WHERE fact_type = 'DIGITAL' AND digitally_active_flag = 'Digital Active'
""").show(truncate=False)

# ── 4) Digital Enrollment Wealth ─────────────────────────────────────────────────
print("[4] Digital Enrollment Wealth (expect ~123k):")
spark.sql(f"""
    SELECT business_date, COUNT(DISTINCT CASE WHEN digital_flag = 'Digital User' THEN rcif_number END) AS digital_enrolled
    FROM {DEFAULT_DB}.wealth_insights_cust WHERE fact_type = 'WEALTH'
    GROUP BY business_date ORDER BY business_date
""").show(truncate=False)

# ── 5) Wealth active flags vs Power BI reference ────────────────────────────────
print("[5] Wealth active flags vs Power BI reference:")
ref = {
    "2025-10-31": (64385, 59359, 89061),
    "2025-11-28": (64489, 59812, 89507),
    "2025-12-31": (64598, 60220, 89928),
    "2026-01-30": (65088, 60577, 90451),
    "2026-02-27": (65767, 60799, 91034),
}
rows = spark.sql(f"""
    SELECT business_date,
           COUNT(DISTINCT CASE WHEN olb_active_flag = 'OLB Active' THEN rcif_number END) AS olb,
           COUNT(DISTINCT CASE WHEN mobile_active_flag = 'Mobile Active' THEN rcif_number END) AS mob,
           COUNT(DISTINCT CASE WHEN digitally_active_flag = 'Digital Active' THEN rcif_number END) AS dig
    FROM {DEFAULT_DB}.wealth_insights_cust WHERE fact_type = 'WEALTH'
    GROUP BY business_date ORDER BY business_date
""").collect()
print(f"  {'month':>12}  {'olb_ours':>9}  {'olb_ref':>9}  {'olb_d':>7}  {'mob_ours':>9}  {'mob_ref':>9}  {'mob_d':>7}  {'dig_ours':>9}  {'dig_ref':>9}  {'dig_d':>7}")
for r in rows:
    dt = str(r["business_date"])
    ro, rm, rd = ref.get(dt, (None, None, None))
    olb_d = "{:>+,}".format(r['olb']-ro) if ro else "n/a"
    mob_d = "{:>+,}".format(r['mob']-rm) if rm else "n/a"
    dig_d = "{:>+,}".format(r['dig']-rd) if rd else "n/a"
    ro_s = "{:>,}".format(ro) if ro else "n/a"
    rm_s = "{:>,}".format(rm) if rm else "n/a"
    rd_s = "{:>,}".format(rd) if rd else "n/a"
    print(f"  {dt:>12}  {r['olb']:>9,}  {ro_s:>9}  {olb_d:>7}  {r['mob']:>9,}  {rm_s:>9}  {mob_d:>7}  {r['dig']:>9,}  {rd_s:>9}  {dig_d:>7}")

# ── 6) Penetration ───────────────────────────────────────────────────────────────
print("\n[6] Wealth Digital Active Penetration (expect ~34.87%):")
spark.sql(f"""
    SELECT business_date,
           COUNT(DISTINCT rcif_number) AS wealth_total,
           COUNT(DISTINCT CASE WHEN digitally_active_flag = 'Digital Active' THEN rcif_number END) AS dig_active,
           ROUND(100.0 * COUNT(DISTINCT CASE WHEN digitally_active_flag = 'Digital Active' THEN rcif_number END)
                       / COUNT(DISTINCT rcif_number), 2) AS penetration_pct
    FROM {DEFAULT_DB}.wealth_insights_cust WHERE fact_type = 'WEALTH'
    GROUP BY business_date ORDER BY business_date
""").show(truncate=False)

# ── 7) InvestPath (expect: 119 customers, 115 accounts, $1.75M AUM) ─────────────
print("[7] InvestPath summary:")
spark.sql(f"""
    SELECT
        COUNT(DISTINCT ip_id) AS investpath_customers,
        COUNT(*) AS investpath_accounts,
        ROUND(SUM(ip_balance), 2) AS aum,
        ROUND(SUM(ip_balance) / NULLIF(COUNT(*), 0), 2) AS avg_balance,
        SUM(CASE WHEN ip_balance > 0 THEN 1 ELSE 0 END) AS funded_accounts
    FROM {DEFAULT_DB}.wealth_insights_acct
""").show(truncate=False)

# ── 8) InvestPath by state ───────────────────────────────────────────────────────
print("[8] InvestPath by state:")
spark.sql(f"""
    SELECT state_name,
           COUNT(DISTINCT ip_id) AS customers,
           COUNT(*) AS accounts
    FROM {DEFAULT_DB}.wealth_insights_acct
    WHERE state_name IS NOT NULL
    GROUP BY state_name ORDER BY customers DESC
    LIMIT 10
""").show(truncate=False)

print("\n" + "=" * 80)
print("  VALIDATION COMPLETE")
print("=" * 80)
spark.stop()

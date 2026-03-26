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
RCIF_SRC_LIST = [
    "DA", "SV", "CC", "MG", "LS", "TM", "PC", "LO", "BW",
    "CM", "CS", "EL", "IC", "MA", "PF", "PR", "SD", "TR",
    "BI", "RN", "IS_CT", "IS_IT", "PWM"
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
rcif_src_csv   = ",".join([f"'{s}'" for s in RCIF_SRC_LIST])

max_dig_dt = spark.sql(f"SELECT MAX(ods_business_dt) AS dt FROM {DMIB_DB}.digital_banking_master").collect()[0]["dt"]
print(f"[INFO] max_dig_dt : {max_dig_dt}")
print(f"[INFO] Window     : {START_DT} .. {END_DT}")


# ══════════════════════════════════════════════════════════════════════════════════
# All logic inlined into a single CREATE TABLE to avoid temp view session issues
# ══════════════════════════════════════════════════════════════════════════════════
print("\n[WRITE] Customer table ...")
spark.sql(f"DROP TABLE IF EXISTS {DEFAULT_DB}.Wealth_Insights_Customer")
spark.sql(f"""
    CREATE TABLE {DEFAULT_DB}.Wealth_Insights_Customer AS

    WITH last_ip_date AS (
        SELECT DISTINCT business_date AS last_dt
        FROM {EIL_DB}.d_involved_party_h
        WHERE CAST(business_date AS date) >= add_months(CAST('{END_DT}' AS date), -6)
    ),

    pwl_raw AS (
        SELECT
            CAST(ind.business_date AS date) AS business_date,
            CAST(ind.rcif_cust_nbr AS string) AS rcif_number,
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
            END AS business_group,
            COUNT(DISTINCT CASE WHEN ar.business_service_segment_type_code = 'IS_CT' THEN ar.arrangement_id END) AS corporate_trust_count,
            COUNT(DISTINCT CASE WHEN ar.business_service_segment_type_code = 'IS_IT' THEN ar.arrangement_id END) AS institutional_trust_count,
            COUNT(DISTINCT CASE WHEN ar.business_service_segment_type_code = 'REGIS_FC' THEN ar.arrangement_id END) AS investment_count,
            COUNT(DISTINCT CASE WHEN ar.business_service_segment_type_code = 'REGIS' THEN ar.arrangement_id END) AS insurance_count,
            COUNT(DISTINCT CASE WHEN ar.business_service_segment_type_code = 'PWM' THEN ar.arrangement_id END) AS pwm_count,
            COUNT(DISTINCT CASE WHEN ar.source_system_code = 'TR' THEN ar.arrangement_id END) AS trust_count,
            COUNT(DISTINCT CASE WHEN ar.source_system_code IN
                  ('DA','SV','CC','MG','LS','TM','LO','CM','CS','EL','IC','MA','PF','PR','SD')
                  THEN ar.arrangement_id END) AS banking_count,
            COUNT(ar.arrangement_id) AS accts_cnt
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

    wealth_agg AS (
        SELECT business_date, rcif_number, MAX(ip_id) AS ip_id,
               MAX(cust_internet_banking_nbr) AS cust_internet_banking_nbr,
               MAX(business_group) AS business_group,
               SUM(accts_cnt) AS wealth_accts_cnt,
               SUM(corporate_trust_count) AS corporate_trust_count,
               SUM(institutional_trust_count) AS institutional_trust_count,
               SUM(investment_count) AS investment_count,
               SUM(insurance_count) AS insurance_count,
               SUM(pwm_count) AS pwm_count,
               SUM(trust_count) AS trust_count,
               SUM(banking_count) AS banking_count
        FROM pwl_raw GROUP BY business_date, rcif_number
    ),

    wealth_final AS (
        SELECT business_date, rcif_number, ip_id, cust_internet_banking_nbr,
               business_group, wealth_accts_cnt,
               CASE
                   WHEN business_group = 'Private Wealth' THEN CASE
                       WHEN trust_count > 0 AND banking_count > 0 THEN 'Banking & IMAT'
                       WHEN investment_count + trust_count > 0 AND banking_count = 0 THEN 'Investments Only'
                       ELSE 'Banking only' END
                   WHEN business_group = 'Investment Services' THEN CASE
                       WHEN investment_count > 0 AND insurance_count = 0 THEN 'Investment'
                       WHEN investment_count = 0 AND insurance_count > 0 THEN 'Insurance'
                       ELSE 'Insurance & Investment' END
                   WHEN corporate_trust_count > 0 AND institutional_trust_count = 0 THEN 'Corporate Trust'
                   WHEN corporate_trust_count = 0 AND institutional_trust_count > 0 THEN 'Institutional Trust'
                   WHEN pwm_count > 0 THEN 'Banking only'
                   ELSE 'Corporate & Institutional Trust'
               END AS division
        FROM wealth_agg
    ),

    dig_customer AS (
        SELECT ibn AS reltibn,
               MAX(olb_last_login_date) AS lst_login_olb,
               MAX(mob_last_login_date) AS lst_login_mob,
               ods_business_dt
        FROM {DMIB_DB}.digital_banking_master
        WHERE ods_business_dt = date('{max_dig_dt}')
        GROUP BY ibn, ods_business_dt
    ),

    rc AS (
        SELECT MAX(CAST(ip.rcif_cust_nbr AS string)) AS rcif_number,
               ip.involved_party_id, ip.cust_internet_banking_nbr
        FROM {EIL_DB}.d_involved_party_h ip
        INNER JOIN {EIL_DB}.d_arrangement_to_involved_party_relationship_h a2i
            ON ip.business_date = a2i.business_date AND ip.source_system_code = a2i.source_system_code
            AND ip.involved_party_id = a2i.involved_party_id
        INNER JOIN {EIL_DB}.d_arrangement_h ar
            ON a2i.business_date = ar.business_date AND a2i.arrangement_source_system_code = ar.source_system_code
            AND a2i.arrangement_id = ar.arrangement_id AND ar.source_system_code IN ({rcif_src_csv})
        INNER JOIN {EIL_DB}.d_involved_party_address_h addr
            ON ip.involved_party_id = addr.involved_party_id AND ip.business_date = addr.business_date
        WHERE ip.business_date = (SELECT MAX(business_date) FROM {EIL_DB}.d_involved_party_h)
          AND ip.source_system_code = 'CF' AND NVL(ip.deceased_ind, 'N') = 'N'
        GROUP BY ip.involved_party_id, ip.cust_internet_banking_nbr
    ),

    rcif_dig AS (
        SELECT rc.rcif_number,
            CASE WHEN datediff(c.ods_business_dt, c.lst_login_mob) <= 90 THEN 'Mobile Active' ELSE 'Non Mobile Active' END AS mobile_active_flag,
            CASE WHEN c.lst_login_mob IS NULL THEN 'Non Mobile User' ELSE 'Mobile User' END AS mobile_flag,
            CASE WHEN datediff(c.ods_business_dt, c.lst_login_olb) <= 90 THEN 'OLB Active' ELSE 'Non OLB Active' END AS olb_active_flag,
            CASE WHEN c.lst_login_olb IS NULL THEN 'Non OLB User' ELSE 'OLB User' END AS olb_flag,
            CASE WHEN datediff(c.ods_business_dt, c.lst_login_mob) <= 90 OR datediff(c.ods_business_dt, c.lst_login_olb) <= 90
                 THEN 'Digital Active' ELSE 'Non Digital Active' END AS digitally_active_flag,
            CASE WHEN c.reltibn IS NULL THEN 'Non Digital User' ELSE 'Digital User' END AS digital_flag
        FROM rc LEFT JOIN dig_customer c ON rc.cust_internet_banking_nbr = c.reltibn
    ),

    digital_monthly AS (
        SELECT TRUNC(ods_business_dt, 'MM') AS month_dt,
               CAST(rcif_customer_nbr AS string) AS rcif_number, ibn,
               MAX(olb_last_login_date) AS last_olb, MAX(mob_last_login_date) AS last_mob,
               MAX(ods_business_dt) AS ods_dt
        FROM {DMIB_DB}.digital_banking_master
        WHERE ods_business_dt >= date('{START_DT}') AND ods_business_dt <= date('{END_DT}')
        GROUP BY TRUNC(ods_business_dt, 'MM'), CAST(rcif_customer_nbr AS string), ibn
    )

    SELECT w.business_date, w.rcif_number, w.cust_internet_banking_nbr,
           w.ip_id, w.business_group, w.division, w.wealth_accts_cnt,
           COALESCE(f.mobile_flag, 'Non Mobile User') AS mobile_flag,
           COALESCE(f.mobile_active_flag, 'Non Mobile Active') AS mobile_active_flag,
           COALESCE(f.olb_flag, 'Non OLB User') AS olb_flag,
           COALESCE(f.olb_active_flag, 'Non OLB Active') AS olb_active_flag,
           COALESCE(f.digital_flag, 'Non Digital User') AS digital_flag,
           COALESCE(f.digitally_active_flag, 'Non Digital Active') AS digitally_active_flag,
           'WEALTH' AS fact_type
    FROM wealth_final w
    LEFT JOIN rcif_dig f ON w.rcif_number = f.rcif_number

    UNION ALL

    SELECT CAST(month_dt AS date), rcif_number, ibn, CAST(NULL AS string),
           CAST(NULL AS string), CAST(NULL AS string), CAST(NULL AS bigint),
           CASE WHEN last_mob IS NOT NULL THEN 'Mobile User' ELSE 'Non Mobile User' END,
           CASE WHEN last_mob IS NOT NULL AND datediff(ods_dt, last_mob) <= 90 THEN 'Mobile Active' ELSE 'Non Mobile Active' END,
           CASE WHEN last_olb IS NOT NULL THEN 'OLB User' ELSE 'Non OLB User' END,
           CASE WHEN last_olb IS NOT NULL AND datediff(ods_dt, last_olb) <= 90 THEN 'OLB Active' ELSE 'Non OLB Active' END,
           'Digital User',
           CASE WHEN (last_mob IS NOT NULL AND datediff(ods_dt, last_mob) <= 90)
                  OR (last_olb IS NOT NULL AND datediff(ods_dt, last_olb) <= 90)
                THEN 'Digital Active' ELSE 'Non Digital Active' END,
           'DIGITAL'
    FROM digital_monthly
""")

print(f"[OK] Saved {DEFAULT_DB}.Wealth_Insights_Customer")
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
# InvestPath — matching Power BI exactly
# Lst_Date = MAX(business_date), latest snapshot only
# CF, not deceased, RN, IP, closed_ind = 'N'
# ══════════════════════════════════════════════════════════════════════════════════
print("\n[WRITE] Account table ...")
spark.sql(f"DROP TABLE IF EXISTS {DEFAULT_DB}.Wealth_Insights_Account")
spark.sql(f"""
    CREATE TABLE {DEFAULT_DB}.Wealth_Insights_Account AS
    WITH Lst_Date AS (
        SELECT MAX(business_date) AS last_date FROM {EIL_DB}.d_involved_party_h
    ),
    INV AS (
        SELECT
            ind.rcif_cust_nbr          AS rcif_nbr,
            ind.involved_party_id      AS ip_id,
            ar.current_balance_amt     AS balance,
            ar.open_date,
            ar.arrangement_id          AS act_cnt
        FROM {EIL_DB}.d_involved_party_h ind
        INNER JOIN Lst_Date ON ind.business_date = Lst_Date.last_date
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
        CAST(INV.rcif_nbr AS STRING) AS rcif_number,
        INV.ip_id,
        CAST(COALESCE(INV.balance, 0.0) AS DOUBLE) AS balance,
        CAST(INV.open_date AS DATE) AS open_date,
        CAST(INV.act_cnt AS STRING) AS accounts,
        addr.state_name
    FROM INV
    LEFT JOIN (
        SELECT involved_party_id AS ip_id, state_name
        FROM (
            SELECT involved_party_id, state_name,
                   ROW_NUMBER() OVER (PARTITION BY involved_party_id ORDER BY NVL(state_name, '') DESC) AS rn
            FROM {EIL_DB}.d_involved_party_address_h
            WHERE CAST(business_date AS date) = date('{addr_dt}')
        ) ranked WHERE rn = 1
    ) addr ON INV.ip_id = addr.ip_id
""")

print(f"[OK] Saved {DEFAULT_DB}.Wealth_Insights_Account")
spark.stop()
print("DONE.")
from pyspark.sql import SparkSession

DEFAULT_DB = "dm_ib_dev"
spark = SparkSession.builder.appName("validate").config("spark.sql.legacy.timeParserPolicy","LEGACY").enableHiveSupport().getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("=" * 80)
print("  VALIDATION")
print("=" * 80)

ct = spark.sql(f"SELECT COUNT(*) c FROM {DEFAULT_DB}.Wealth_Insights_Customer").collect()[0]["c"]
wt = spark.sql(f"SELECT COUNT(*) c FROM {DEFAULT_DB}.Wealth_Insights_Customer WHERE fact_type='WEALTH'").collect()[0]["c"]
dt = spark.sql(f"SELECT COUNT(*) c FROM {DEFAULT_DB}.Wealth_Insights_Customer WHERE fact_type='DIGITAL'").collect()[0]["c"]
at = spark.sql(f"SELECT COUNT(*) c FROM {DEFAULT_DB}.Wealth_Insights_Account").collect()[0]["c"]
print(f"\n[1] Rows: Customer={ct:,} (WEALTH={wt:,}, DIGITAL={dt:,}), Account={at:,}")

print("\n[2] Wealth customers per month:")
spark.sql(f"SELECT business_date, COUNT(DISTINCT rcif_number) AS n FROM {DEFAULT_DB}.Wealth_Insights_Customer WHERE fact_type='WEALTH' GROUP BY business_date ORDER BY business_date").show(truncate=False)

print("[3] Top of Company Digital Active:")
spark.sql(f"SELECT COUNT(DISTINCT cust_internet_banking_nbr) AS n FROM {DEFAULT_DB}.Wealth_Insights_Customer WHERE fact_type='DIGITAL' AND digitally_active_flag='Digital Active'").show(truncate=False)

print("[4] Digital Enrollment:")
spark.sql(f"SELECT business_date, COUNT(DISTINCT CASE WHEN digital_flag='Digital User' THEN rcif_number END) AS enrolled FROM {DEFAULT_DB}.Wealth_Insights_Customer WHERE fact_type='WEALTH' GROUP BY business_date ORDER BY business_date").show(truncate=False)

print("[5] Flags vs reference:")
ref = {"2025-10-31":(64385,59359,89061),"2025-11-28":(64489,59812,89507),"2025-12-31":(64598,60220,89928),"2026-01-30":(65088,60577,90451),"2026-02-27":(65767,60799,91034)}
rows = spark.sql(f"""
    SELECT business_date,
           COUNT(DISTINCT CASE WHEN olb_active_flag='OLB Active' THEN rcif_number END) AS olb,
           COUNT(DISTINCT CASE WHEN mobile_active_flag='Mobile Active' THEN rcif_number END) AS mob,
           COUNT(DISTINCT CASE WHEN digitally_active_flag='Digital Active' THEN rcif_number END) AS dig
    FROM {DEFAULT_DB}.Wealth_Insights_Customer WHERE fact_type='WEALTH'
    GROUP BY business_date ORDER BY business_date
""").collect()
print(f"  {'month':>12} {'olb':>8} {'ref':>8} {'d':>7} {'mob':>8} {'ref':>8} {'d':>7} {'dig':>8} {'ref':>8} {'d':>7}")
for r in rows:
    d = str(r["business_date"])
    ro,rm,rd = ref.get(d,(None,None,None))
    od = "{:+,}".format(r['olb']-ro) if ro else "-"
    md = "{:+,}".format(r['mob']-rm) if rm else "-"
    dd = "{:+,}".format(r['dig']-rd) if rd else "-"
    print(f"  {d:>12} {r['olb']:>8,} {str(ro or '-'):>8} {od:>7} {r['mob']:>8,} {str(rm or '-'):>8} {md:>7} {r['dig']:>8,} {str(rd or '-'):>8} {dd:>7}")

print("\n[6] Penetration (expect ~34.87%):")
spark.sql(f"""
    SELECT business_date, COUNT(DISTINCT rcif_number) AS total,
           COUNT(DISTINCT CASE WHEN digitally_active_flag='Digital Active' THEN rcif_number END) AS active,
           ROUND(100.0*COUNT(DISTINCT CASE WHEN digitally_active_flag='Digital Active' THEN rcif_number END)/COUNT(DISTINCT rcif_number),2) AS pct
    FROM {DEFAULT_DB}.Wealth_Insights_Customer WHERE fact_type='WEALTH'
    GROUP BY business_date ORDER BY business_date
""").show(truncate=False)

print("[7] InvestPath (expect: 119 customers, 115 accounts, $1.75M):")
spark.sql(f"""
    SELECT COUNT(DISTINCT ip_id) AS customers, COUNT(*) AS accounts,
           ROUND(SUM(balance),2) AS aum, ROUND(SUM(balance)/NULLIF(COUNT(*),0),2) AS avg_bal,
           SUM(CASE WHEN balance>0 THEN 1 ELSE 0 END) AS funded
    FROM {DEFAULT_DB}.Wealth_Insights_Account
""").show(truncate=False)

print("[8] InvestPath by state:")
spark.sql(f"SELECT state_name, COUNT(DISTINCT ip_id) AS cust, COUNT(*) AS accts FROM {DEFAULT_DB}.Wealth_Insights_Account WHERE state_name IS NOT NULL GROUP BY state_name ORDER BY cust DESC LIMIT 10").show(truncate=False)

print("=" * 80)
spark.stop()

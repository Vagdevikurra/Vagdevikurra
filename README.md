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

print("\n[WRITE] Wealth_Insights_Account ...")
spark.sql(f"DROP TABLE IF EXISTS {DEFAULT_DB}.Wealth_Insights_Account")
spark.sql(f"""
    CREATE TABLE {DEFAULT_DB}.Wealth_Insights_Account AS
    WITH Lst_Date AS (
        SELECT MAX(business_date) AS last_date FROM {EIL_DB}.d_involved_party_h
    ),
    INV AS (
        SELECT
            ind.rcif_cust_nbr AS rcif_nbr,
            ind.involved_party_id AS ip_id,
            ar.current_balance_amt AS balance,
            ar.open_date,
            ar.arrangement_id AS act_cnt
        FROM {EIL_DB}.d_involved_party_h ind
        INNER JOIN Lst_Date ON ind.business_date = Lst_Date.last_date
        INNER JOIN {EIL_DB}.d_arrangement_to_involved_party_relationship_h a2i
            ON ind.involved_party_id = a2i.involved_party_id
            AND ind.business_date = a2i.business_date
            AND ind.source_system_code = a2i.source_system_code
        INNER JOIN {EIL_DB}.d_arrangement_h ar
            ON a2i.arrangement_id = ar.arrangement_id
            AND a2i.arrangement_source_system_code = ar.source_system_code
            AND a2i.business_date = ar.business_date
            AND ar.closed_ind = 'N'
            AND ar.account_type_code = 'IP'
            AND ar.source_system_code = 'RN'
        WHERE ind.source_system_code = 'CF'
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
                   ROW_NUMBER() OVER (PARTITION BY involved_party_id ORDER BY NVL(state_name,'') DESC) AS rn
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
print("  WEALTH INSIGHTS — VALIDATION")
print("=" * 80)

ct = spark.sql(f"SELECT COUNT(*) c FROM {DEFAULT_DB}.Wealth_Insights_Customer").collect()[0]["c"]
wt = spark.sql(f"SELECT COUNT(*) c FROM {DEFAULT_DB}.Wealth_Insights_Customer WHERE fact_type='WEALTH'").collect()[0]["c"]
dt = spark.sql(f"SELECT COUNT(*) c FROM {DEFAULT_DB}.Wealth_Insights_Customer WHERE fact_type='DIGITAL'").collect()[0]["c"]
at = spark.sql(f"SELECT COUNT(*) c FROM {DEFAULT_DB}.Wealth_Insights_Account").collect()[0]["c"]
print(f"\n[1] Rows: Customer={ct:,} (WEALTH={wt:,}, DIGITAL={dt:,}), Account={at:,}")

print("\n[2] Wealth customers per month (expect ~265-267k):")
spark.sql(f"SELECT business_date, COUNT(DISTINCT rcif_number) AS n FROM {DEFAULT_DB}.Wealth_Insights_Customer WHERE fact_type='WEALTH' GROUP BY business_date ORDER BY business_date").show(truncate=False)

print("[3] Top of Company Digital Active (expect ~3.4M):")
spark.sql(f"SELECT COUNT(DISTINCT cust_internet_banking_nbr) AS n FROM {DEFAULT_DB}.Wealth_Insights_Customer WHERE fact_type='DIGITAL' AND digitally_active_flag='Digital Active'").show(truncate=False)

print("[4] Digital Enrollment Wealth (expect ~123k):")
spark.sql(f"SELECT business_date, COUNT(DISTINCT CASE WHEN digital_flag='Digital User' THEN rcif_number END) AS enrolled FROM {DEFAULT_DB}.Wealth_Insights_Customer WHERE fact_type='WEALTH' GROUP BY business_date ORDER BY business_date").show(truncate=False)

print("[5] Flags vs Power BI reference:")
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
    ro_s = "{:>,}".format(ro) if ro else "-"
    rm_s = "{:>,}".format(rm) if rm else "-"
    rd_s = "{:>,}".format(rd) if rd else "-"
    print(f"  {d:>12} {r['olb']:>8,} {ro_s:>8} {od:>7} {r['mob']:>8,} {rm_s:>8} {md:>7} {r['dig']:>8,} {rd_s:>8} {dd:>7}")

print("\n[6] Penetration (expect ~34.87%):")
spark.sql(f"""
    SELECT business_date, COUNT(DISTINCT rcif_number) AS total,
           COUNT(DISTINCT CASE WHEN digitally_active_flag='Digital Active' THEN rcif_number END) AS active,
           ROUND(100.0*COUNT(DISTINCT CASE WHEN digitally_active_flag='Digital Active' THEN rcif_number END)/COUNT(DISTINCT rcif_number),2) AS pct
    FROM {DEFAULT_DB}.Wealth_Insights_Customer WHERE fact_type='WEALTH'
    GROUP BY business_date ORDER BY business_date
""").show(truncate=False)

print("[7] Business group breakdown (latest month):")
spark.sql(f"""
    SELECT business_group, COUNT(DISTINCT rcif_number) AS customers
    FROM {DEFAULT_DB}.Wealth_Insights_Customer
    WHERE fact_type='WEALTH' AND business_date = (SELECT MAX(business_date) FROM {DEFAULT_DB}.Wealth_Insights_Customer WHERE fact_type='WEALTH')
    GROUP BY business_group ORDER BY customers DESC
""").show(truncate=False)

print("[8] InvestPath (expect: 119 cust, 115 accts, $1.75M AUM):")
spark.sql(f"""
    SELECT COUNT(DISTINCT ip_id) AS customers, COUNT(*) AS accounts,
           ROUND(SUM(balance),2) AS aum, ROUND(SUM(balance)/NULLIF(COUNT(*),0),2) AS avg_bal,
           SUM(CASE WHEN balance>0 THEN 1 ELSE 0 END) AS funded
    FROM {DEFAULT_DB}.Wealth_Insights_Account
""").show(truncate=False)

print("[9] InvestPath by state:")
spark.sql(f"SELECT state_name, COUNT(DISTINCT ip_id) AS cust, COUNT(*) AS accts FROM {DEFAULT_DB}.Wealth_Insights_Account WHERE state_name IS NOT NULL GROUP BY state_name ORDER BY cust DESC LIMIT 10").show(truncate=False)

print("\n" + "=" * 80)
print("  DONE")
print("=" * 80)
spark.stop()


// ══════════════════════════════════════════════════════════════════════════════
// WEALTH INSIGHTS — DAX MEASURES (FINAL)
// ══════════════════════════════════════════════════════════════════════════════
// Tables: Wealth_Insights_Customer (WEALTH + DIGITAL rows)
//         Wealth_Insights_Account (InvestPath)
// Setup: business_date = Date type, date slicer on page
// ══════════════════════════════════════════════════════════════════════════════


// ═══ HEADLINE CARDS (pinned to selected month) ═══

Wealth Users = 
VAR _dt = MAX(Wealth_Insights_Customer[business_date])
RETURN CALCULATE(
    DISTINCTCOUNT(Wealth_Insights_Customer[rcif_number]),
    Wealth_Insights_Customer[fact_type] = "WEALTH",
    Wealth_Insights_Customer[business_date] = _dt
)

Top of Company Digital Active = 
CALCULATE(
    DISTINCTCOUNT(Wealth_Insights_Customer[cust_internet_banking_nbr]),
    Wealth_Insights_Customer[fact_type] = "DIGITAL",
    Wealth_Insights_Customer[digitally_active_flag] = "Digital Active"
)

Digital Enrollments Wealth = 
VAR _dt = MAX(Wealth_Insights_Customer[business_date])
RETURN CALCULATE(
    DISTINCTCOUNT(Wealth_Insights_Customer[rcif_number]),
    Wealth_Insights_Customer[fact_type] = "WEALTH",
    Wealth_Insights_Customer[digital_flag] = "Digital User",
    Wealth_Insights_Customer[business_date] = _dt
)

Accounts = 
VAR _dt = MAX(Wealth_Insights_Customer[business_date])
RETURN CALCULATE(
    SUM(Wealth_Insights_Customer[wealth_accts_cnt]),
    Wealth_Insights_Customer[fact_type] = "WEALTH",
    Wealth_Insights_Customer[business_date] = _dt
)

Accounts per User = DIVIDE([Accounts], [Wealth Users], 0)

Wealth Active User Adoption % = 
VAR _dt = MAX(Wealth_Insights_Customer[business_date])
VAR _active = CALCULATE(
    DISTINCTCOUNT(Wealth_Insights_Customer[rcif_number]),
    Wealth_Insights_Customer[fact_type] = "WEALTH",
    Wealth_Insights_Customer[digitally_active_flag] = "Digital Active",
    Wealth_Insights_Customer[business_date] = _dt)
VAR _total = CALCULATE(
    DISTINCTCOUNT(Wealth_Insights_Customer[rcif_number]),
    Wealth_Insights_Customer[fact_type] = "WEALTH",
    Wealth_Insights_Customer[business_date] = _dt)
RETURN DIVIDE(_active, _total, 0)

% of Digital Population = 
VAR _dt = MAX(Wealth_Insights_Customer[business_date])
VAR _wd = CALCULATE(
    DISTINCTCOUNT(Wealth_Insights_Customer[rcif_number]),
    Wealth_Insights_Customer[fact_type] = "WEALTH",
    Wealth_Insights_Customer[digital_flag] = "Digital User",
    Wealth_Insights_Customer[business_date] = _dt)
RETURN DIVIDE(_wd, [Top of Company Digital Active], 0)


// ═══ BAR CHARTS (axis = business_date, no date pin) ═══

Wealth OLB Active = 
CALCULATE(
    DISTINCTCOUNT(Wealth_Insights_Customer[rcif_number]),
    Wealth_Insights_Customer[fact_type] = "WEALTH",
    Wealth_Insights_Customer[olb_active_flag] = "OLB Active"
)

Wealth OLB Active % = 
DIVIDE(
    [Wealth OLB Active],
    CALCULATE(DISTINCTCOUNT(Wealth_Insights_Customer[rcif_number]),
              Wealth_Insights_Customer[fact_type] = "WEALTH"), 0
)

Wealth MOB Active = 
CALCULATE(
    DISTINCTCOUNT(Wealth_Insights_Customer[rcif_number]),
    Wealth_Insights_Customer[fact_type] = "WEALTH",
    Wealth_Insights_Customer[mobile_active_flag] = "Mobile Active"
)

Wealth MOB Active % = 
DIVIDE(
    [Wealth MOB Active],
    CALCULATE(DISTINCTCOUNT(Wealth_Insights_Customer[rcif_number]),
              Wealth_Insights_Customer[fact_type] = "WEALTH"), 0
)

Wealth Digital Active = 
CALCULATE(
    DISTINCTCOUNT(Wealth_Insights_Customer[rcif_number]),
    Wealth_Insights_Customer[fact_type] = "WEALTH",
    Wealth_Insights_Customer[digitally_active_flag] = "Digital Active"
)


// ═══ TREND LINE ═══

Wealth Users Trend = 
CALCULATE(
    DISTINCTCOUNT(Wealth_Insights_Customer[rcif_number]),
    Wealth_Insights_Customer[fact_type] = "WEALTH"
)

Wealth Digital Active Penetration = DIVIDE([Wealth Digital Active], [Wealth Users Trend], 0)


// ═══ INSIGHTS PAGE — OLB/MOB MATRIX ═══

OLB Active Users = 
CALCULATE(
    DISTINCTCOUNT(Wealth_Insights_Customer[rcif_number]),
    Wealth_Insights_Customer[fact_type] = "WEALTH",
    Wealth_Insights_Customer[olb_active_flag] = "OLB Active"
)

Wealth OLB Enrolled = 
CALCULATE(
    DISTINCTCOUNT(Wealth_Insights_Customer[rcif_number]),
    Wealth_Insights_Customer[fact_type] = "WEALTH",
    Wealth_Insights_Customer[olb_flag] = "OLB User"
)

Wealth OLB Enrolled % = 
DIVIDE([Wealth OLB Enrolled],
    CALCULATE(DISTINCTCOUNT(Wealth_Insights_Customer[rcif_number]),
              Wealth_Insights_Customer[fact_type] = "WEALTH"), 0)

OLB MoM % = 
VAR _c = [Wealth OLB Active]
VAR _p = CALCULATE([Wealth OLB Active],
    DATEADD(Wealth_Insights_Customer[business_date], -1, MONTH))
RETURN DIVIDE(_c - _p, _p, 0)

OLB MoM Delta = 
VAR _c = [Wealth OLB Active]
VAR _p = CALCULATE([Wealth OLB Active],
    DATEADD(Wealth_Insights_Customer[business_date], -1, MONTH))
RETURN _c - _p

MOB Active Users = 
CALCULATE(
    DISTINCTCOUNT(Wealth_Insights_Customer[rcif_number]),
    Wealth_Insights_Customer[fact_type] = "WEALTH",
    Wealth_Insights_Customer[mobile_active_flag] = "Mobile Active"
)

Wealth MOB Enrolled = 
CALCULATE(
    DISTINCTCOUNT(Wealth_Insights_Customer[rcif_number]),
    Wealth_Insights_Customer[fact_type] = "WEALTH",
    Wealth_Insights_Customer[mobile_flag] = "Mobile User"
)

Wealth MOB Enrolled % = 
DIVIDE([Wealth MOB Enrolled],
    CALCULATE(DISTINCTCOUNT(Wealth_Insights_Customer[rcif_number]),
              Wealth_Insights_Customer[fact_type] = "WEALTH"), 0)

MOB MoM % = 
VAR _c = [Wealth MOB Active]
VAR _p = CALCULATE([Wealth MOB Active],
    DATEADD(Wealth_Insights_Customer[business_date], -1, MONTH))
RETURN DIVIDE(_c - _p, _p, 0)

MOB MoM Delta = 
VAR _c = [Wealth MOB Active]
VAR _p = CALCULATE([Wealth MOB Active],
    DATEADD(Wealth_Insights_Customer[business_date], -1, MONTH))
RETURN _c - _p


// ═══ DECOMPOSITION TREE ═══
// Analyze: [Wealth Users Trend]
// Explain By: business_group, then division


// ═══ RCIF DONUT ═══

Total RCIF Records = 
CALCULATE(COUNTROWS(Wealth_Insights_Customer),
    Wealth_Insights_Customer[fact_type] = "DIGITAL")

Digital User Records = 
CALCULATE(COUNTROWS(Wealth_Insights_Customer),
    Wealth_Insights_Customer[fact_type] = "DIGITAL",
    Wealth_Insights_Customer[digital_flag] = "Digital User")

Non Digital User Records = 
CALCULATE(COUNTROWS(Wealth_Insights_Customer),
    Wealth_Insights_Customer[fact_type] = "DIGITAL",
    Wealth_Insights_Customer[digital_flag] = "Non Digital User")


// ═══ INVESTPATH ═══

InvestPath Customers = DISTINCTCOUNT(Wealth_Insights_Account[ip_id])

InvestPath Accounts = COUNTROWS(Wealth_Insights_Account)

AUM = SUM(Wealth_Insights_Account[balance])

$Balance per IP Account = DIVIDE([AUM], [InvestPath Accounts], 0)

InvestPath Account Funded = 
CALCULATE(COUNTROWS(Wealth_Insights_Account),
    Wealth_Insights_Account[balance] > 0)


// ═══ FORMATTING ═══
// % measures → format 0.00%
// Currency → $#,##0
// Counts → #,##0
// Date slicer → business_date from Wealth_Insights_Customer
// Cards → single month selected
// Trends/charts → all months or no slicer
// DATEADD needs business_date marked as Date type in model



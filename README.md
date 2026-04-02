from pyspark.sql import SparkSession

DEFAULT_DB = "dm_ib_dev"
spark = SparkSession.builder.appName("dup_check").config("spark.sql.legacy.timeParserPolicy","LEGACY").enableHiveSupport().getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("  DUPLICATE RCIF CHECK")
print("=" * 60)

# 1) Are there duplicate rcif_numbers per business_date in WEALTH rows?
print("\n[1] Duplicate RCIFs per month in WEALTH rows:")
spark.sql(f"""
    SELECT business_date,
           COUNT(*) AS total_rows,
           COUNT(DISTINCT rcif_number) AS distinct_rcifs,
           COUNT(*) - COUNT(DISTINCT rcif_number) AS duplicates
    FROM {DEFAULT_DB}.Wealth_Insights_Customer
    WHERE fact_type = 'WEALTH'
    GROUP BY business_date ORDER BY business_date
""").show(truncate=False)

# 2) Which RCIFs appear more than once per month?
print("[2] Sample duplicate RCIFs (if any):")
spark.sql(f"""
    SELECT rcif_number, business_date, COUNT(*) AS cnt, 
           COLLECT_SET(business_group) AS business_groups
    FROM {DEFAULT_DB}.Wealth_Insights_Customer
    WHERE fact_type = 'WEALTH'
    GROUP BY rcif_number, business_date
    HAVING COUNT(*) > 1
    ORDER BY cnt DESC
    LIMIT 20
""").show(truncate=False)

# 3) How many RCIFs have multiple business_groups?
print("[3] RCIFs with multiple business_groups in latest month:")
spark.sql(f"""
    WITH latest AS (
        SELECT rcif_number, business_group
        FROM {DEFAULT_DB}.Wealth_Insights_Customer
        WHERE fact_type = 'WEALTH'
          AND business_date = (SELECT MAX(business_date) FROM {DEFAULT_DB}.Wealth_Insights_Customer WHERE fact_type='WEALTH')
    )
    SELECT COUNT(DISTINCT rcif_number) AS multi_lob_customers
    FROM (
        SELECT rcif_number, COUNT(DISTINCT business_group) AS n_groups
        FROM latest
        GROUP BY rcif_number
        HAVING COUNT(DISTINCT business_group) > 1
    ) x
""").show(truncate=False)

# 4) Impact on digital flags — do duplicates inflate the count?
print("[4] Digital active count: with vs without duplicates:")
spark.sql(f"""
    SELECT business_date,
           COUNT(CASE WHEN digitally_active_flag = 'Digital Active' THEN 1 END) AS raw_count,
           COUNT(DISTINCT CASE WHEN digitally_active_flag = 'Digital Active' THEN rcif_number END) AS distinct_count,
           COUNT(CASE WHEN digitally_active_flag = 'Digital Active' THEN 1 END) 
           - COUNT(DISTINCT CASE WHEN digitally_active_flag = 'Digital Active' THEN rcif_number END) AS inflation
    FROM {DEFAULT_DB}.Wealth_Insights_Customer
    WHERE fact_type = 'WEALTH'
    GROUP BY business_date ORDER BY business_date
""").show(truncate=False)

print("=" * 60)
spark.stop()

// ══════════════════════════════════════════════════════════════════════════════
// WEALTH INSIGHTS — DAX MEASURES (FINAL v2)
// ══════════════════════════════════════════════════════════════════════════════
// Tables: Wealth_Insights_Customer (WEALTH + DIGITAL rows)
//         Wealth_Insights_Account (InvestPath)
//
// IMPORTANT: All measures use DISTINCTCOUNT(rcif_number) to handle 
// customers who appear in multiple lines of business (business_group).
// Top cards always show the MOST RECENT month.
// ══════════════════════════════════════════════════════════════════════════════


// ═══ HEADLINE CARDS (always most recent month) ═══

Wealth Users = 
VAR _dt = CALCULATE(MAX(Wealth_Insights_Customer[business_date]), Wealth_Insights_Customer[fact_type]="WEALTH")
RETURN CALCULATE(
    DISTINCTCOUNT(Wealth_Insights_Customer[rcif_number]),
    Wealth_Insights_Customer[fact_type] = "WEALTH",
    Wealth_Insights_Customer[business_date] = _dt
)

Top of Company Digital Active = 
VAR _dt = CALCULATE(MAX(Wealth_Insights_Customer[business_date]), Wealth_Insights_Customer[fact_type]="DIGITAL")
RETURN CALCULATE(
    DISTINCTCOUNT(Wealth_Insights_Customer[cust_internet_banking_nbr]),
    Wealth_Insights_Customer[fact_type] = "DIGITAL",
    Wealth_Insights_Customer[digitally_active_flag] = "Digital Active",
    Wealth_Insights_Customer[business_date] = _dt
)

Digital Enrollments Wealth = 
VAR _dt = CALCULATE(MAX(Wealth_Insights_Customer[business_date]), Wealth_Insights_Customer[fact_type]="WEALTH")
RETURN CALCULATE(
    DISTINCTCOUNT(Wealth_Insights_Customer[rcif_number]),
    Wealth_Insights_Customer[fact_type] = "WEALTH",
    Wealth_Insights_Customer[digital_flag] = "Digital User",
    Wealth_Insights_Customer[business_date] = _dt
)

Accounts = 
VAR _dt = CALCULATE(MAX(Wealth_Insights_Customer[business_date]), Wealth_Insights_Customer[fact_type]="WEALTH")
RETURN CALCULATE(
    SUM(Wealth_Insights_Customer[wealth_accts_cnt]),
    Wealth_Insights_Customer[fact_type] = "WEALTH",
    Wealth_Insights_Customer[business_date] = _dt
)

Accounts per User = DIVIDE([Accounts], [Wealth Users], 0)

Wealth Active User Adoption % = 
VAR _dt = CALCULATE(MAX(Wealth_Insights_Customer[business_date]), Wealth_Insights_Customer[fact_type]="WEALTH")
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
DIVIDE([Digital Enrollments Wealth], [Top of Company Digital Active], 0)


// ═══ BAR CHARTS (axis = business_date) ═══
// These auto-slice by month when business_date is on the axis.
// DISTINCTCOUNT handles multi-LOB customers correctly.

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


// ═══ INSIGHTS — OLB/MOB MATRIX ═══

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
// Explain By: business_group → division


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


// ═══ SETUP NOTES ═══
// 1. Set business_date to Date type in model
// 2. Cards always show most recent month (no slicer needed)
// 3. Bar charts: put business_date on axis
// 4. DISTINCTCOUNT handles multi-LOB customers — no double counting
// 5. DATEADD for MoM needs business_date marked as Date
// 6. Format: % → 0.00%, currency → $#,##0, counts → #,##0




from pyspark.sql import SparkSession
from pyspark import SparkConf

DEFAULT_DB = "dm_ib_dev"
EIL_DB     = "eil"
DMIB_DB    = "dm_ib"

spark = (
    SparkSession.builder
    .appName("digital_flag_diagnostic")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .enableHiveSupport()
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("=" * 80)
print("  DIGITAL FLAG GAP DIAGNOSTIC")
print("  Goal: find why we're ~5-7% over Power BI for OLB/MOB/Digital Active")
print("=" * 80)


# ── 1) What date range does the OLD Power BI source table use? ───────────────────
print("\n[1] Check if old wealth table exists and its date range:")
for tbl in ["Wealth_Insights_Customer1", "wealth_insights_customer1",
            "Wealth_Insights_Customerl", "wealth_insights_cust1",
            "wic2_wealth_fact"]:
    try:
        r = spark.sql(f"SELECT COUNT(*) AS c FROM {DEFAULT_DB}.{tbl}").collect()[0]["c"]
        print(f"  Found {DEFAULT_DB}.{tbl} with {r:,} rows")
        spark.sql(f"""
            SELECT MIN(business_date) AS min_dt, MAX(business_date) AS max_dt
            FROM {DEFAULT_DB}.{tbl}
        """).show(truncate=False)
    except:
        pass


# ── 2) Check digital_banking_master date coverage ────────────────────────────────
print("\n[2] digital_banking_master date coverage:")
spark.sql(f"""
    SELECT MIN(ods_business_dt) AS earliest,
           MAX(ods_business_dt) AS latest,
           COUNT(DISTINCT TRUNC(ods_business_dt, 'MM')) AS months_available
    FROM {DMIB_DB}.digital_banking_master
""").show(truncate=False)


# ── 3) Compare 90-day window: ods_dt vs month-end date ──────────────────────────
print("\n[3] ods_dt vs month-end — does it matter?")
spark.sql(f"""
    WITH dig AS (
        SELECT
            TRUNC(ods_business_dt, 'MM') AS month_dt,
            CAST(rcif_customer_nbr AS string) AS rcif_number,
            MAX(olb_last_login_date)  AS last_olb,
            MAX(mob_last_login_date)  AS last_mob,
            MAX(ods_business_dt)      AS ods_dt
        FROM {DMIB_DB}.digital_banking_master
        WHERE ods_business_dt >= '2025-09-01' AND ods_business_dt <= '2026-02-28'
        GROUP BY TRUNC(ods_business_dt, 'MM'), CAST(rcif_customer_nbr AS string)
    )
    SELECT month_dt,
           COUNT(DISTINCT CASE WHEN datediff(ods_dt, last_olb) <= 90
                               THEN rcif_number END) AS olb_by_ods_dt,
           COUNT(DISTINCT CASE WHEN datediff(last_day(month_dt), last_olb) <= 90
                               THEN rcif_number END) AS olb_by_month_end,
           COUNT(DISTINCT CASE WHEN datediff(ods_dt, last_mob) <= 90
                               THEN rcif_number END) AS mob_by_ods_dt,
           COUNT(DISTINCT CASE WHEN datediff(last_day(month_dt), last_mob) <= 90
                               THEN rcif_number END) AS mob_by_month_end
    FROM dig
    GROUP BY month_dt ORDER BY month_dt
""").show(truncate=False)


# ── 4) Does grouping by ibn vs RCIF-only change flag counts? ─────────────────────
print("\n[4] Digital active at RCIF level — ibn-grain vs rcif-grain:")
spark.sql(f"""
    WITH by_ibn AS (
        SELECT TRUNC(ods_business_dt, 'MM') AS month_dt,
               CAST(rcif_customer_nbr AS string) AS rcif_number,
               ibn,
               MAX(olb_last_login_date) AS last_olb,
               MAX(ods_business_dt) AS ods_dt
        FROM {DMIB_DB}.digital_banking_master
        WHERE ods_business_dt >= '2025-09-01' AND ods_business_dt <= '2026-02-28'
        GROUP BY TRUNC(ods_business_dt, 'MM'), CAST(rcif_customer_nbr AS string), ibn
    ),
    by_rcif AS (
        SELECT TRUNC(ods_business_dt, 'MM') AS month_dt,
               CAST(rcif_customer_nbr AS string) AS rcif_number,
               MAX(olb_last_login_date) AS last_olb,
               MAX(ods_business_dt) AS ods_dt
        FROM {DMIB_DB}.digital_banking_master
        WHERE ods_business_dt >= '2025-09-01' AND ods_business_dt <= '2026-02-28'
        GROUP BY TRUNC(ods_business_dt, 'MM'), CAST(rcif_customer_nbr AS string)
    ),
    ibn_flags AS (
        SELECT month_dt,
               COUNT(DISTINCT CASE WHEN datediff(ods_dt, last_olb) <= 90 THEN rcif_number END) AS olb_active
        FROM by_ibn GROUP BY month_dt
    ),
    rcif_flags AS (
        SELECT month_dt,
               COUNT(DISTINCT CASE WHEN datediff(ods_dt, last_olb) <= 90 THEN rcif_number END) AS olb_active
        FROM by_rcif GROUP BY month_dt
    )
    SELECT i.month_dt, i.olb_active AS ibn_grain, r.olb_active AS rcif_grain,
           i.olb_active - r.olb_active AS diff
    FROM ibn_flags i JOIN rcif_flags r ON i.month_dt = r.month_dt
    ORDER BY i.month_dt
""").show(truncate=False)


# ── 5) INTERSECT check — wealth RCIFs in digital with active flags ───────────────
print("\n[5] INTERSECT — wealth RCIFs found in digital with active flags:")
spark.sql(f"""
    WITH wealth_rcifs AS (
        SELECT DISTINCT rcif_number, business_date
        FROM {DEFAULT_DB}.wealth_insights_cust7
        WHERE fact_type = 'WEALTH'
    ),
    dig AS (
        SELECT TRUNC(ods_business_dt, 'MM') AS month_dt,
               CAST(rcif_customer_nbr AS string) AS rcif_number,
               MAX(olb_last_login_date) AS last_olb,
               MAX(mob_last_login_date) AS last_mob,
               MAX(ods_business_dt) AS ods_dt
        FROM {DMIB_DB}.digital_banking_master
        WHERE ods_business_dt >= '2025-09-01' AND ods_business_dt <= '2026-02-28'
        GROUP BY TRUNC(ods_business_dt, 'MM'), CAST(rcif_customer_nbr AS string)
    )
    SELECT w.business_date,
           COUNT(DISTINCT w.rcif_number) AS wealth_total,
           COUNT(DISTINCT CASE WHEN d.last_olb IS NOT NULL AND datediff(d.ods_dt, d.last_olb) <= 90
                               THEN w.rcif_number END) AS olb_active,
           COUNT(DISTINCT CASE WHEN d.last_mob IS NOT NULL AND datediff(d.ods_dt, d.last_mob) <= 90
                               THEN w.rcif_number END) AS mob_active,
           COUNT(DISTINCT CASE WHEN (d.last_olb IS NOT NULL AND datediff(d.ods_dt, d.last_olb) <= 90)
                                 OR (d.last_mob IS NOT NULL AND datediff(d.ods_dt, d.last_mob) <= 90)
                               THEN w.rcif_number END) AS dig_active
    FROM wealth_rcifs w
    INNER JOIN dig d
        ON  w.rcif_number = d.rcif_number
        AND TRUNC(w.business_date, 'MM') = d.month_dt
    GROUP BY w.business_date ORDER BY w.business_date
""").show(truncate=False)


# ── 6) Day threshold sensitivity (Feb only) ─────────────────────────────────────
print("\n[6] Sensitivity: flag counts at different day thresholds (Feb only):")
spark.sql(f"""
    WITH wealth_rcifs AS (
        SELECT DISTINCT rcif_number
        FROM {DEFAULT_DB}.wealth_insights_cust7
        WHERE fact_type = 'WEALTH' AND business_date = '2026-02-27'
    ),
    dig AS (
        SELECT CAST(rcif_customer_nbr AS string) AS rcif_number,
               MAX(olb_last_login_date) AS last_olb,
               MAX(mob_last_login_date) AS last_mob,
               MAX(ods_business_dt) AS ods_dt
        FROM {DMIB_DB}.digital_banking_master
        WHERE ods_business_dt >= '2026-02-01' AND ods_business_dt <= '2026-02-28'
        GROUP BY CAST(rcif_customer_nbr AS string)
    )
    SELECT
        COUNT(DISTINCT CASE WHEN datediff(d.ods_dt, d.last_olb) <= 60  THEN w.rcif_number END) AS olb_60d,
        COUNT(DISTINCT CASE WHEN datediff(d.ods_dt, d.last_olb) <= 90  THEN w.rcif_number END) AS olb_90d,
        COUNT(DISTINCT CASE WHEN datediff(d.ods_dt, d.last_olb) <= 120 THEN w.rcif_number END) AS olb_120d,
        COUNT(DISTINCT CASE WHEN datediff(d.ods_dt, d.last_mob) <= 60  THEN w.rcif_number END) AS mob_60d,
        COUNT(DISTINCT CASE WHEN datediff(d.ods_dt, d.last_mob) <= 90  THEN w.rcif_number END) AS mob_90d,
        COUNT(DISTINCT CASE WHEN datediff(d.ods_dt, d.last_mob) <= 120 THEN w.rcif_number END) AS mob_120d
    FROM wealth_rcifs w
    INNER JOIN dig d ON w.rcif_number = d.rcif_number
""").show(truncate=False)
print("    Power BI ref for Feb: OLB=65,767  MOB=60,799")


# ── 7) Check old tables that Power BI might read from ────────────────────────────
print("\n[7] Looking for old tables Power BI might read:")
for tbl in ["digital_monthly", "Wealth_Insights_Account1", "wealth_insights_account1",
            "wealth_insights_acct1", "wealth_insights_acct7"]:
    try:
        r = spark.sql(f"SELECT COUNT(*) AS c FROM {DEFAULT_DB}.{tbl}").collect()[0]["c"]
        print(f"  {DEFAULT_DB}.{tbl} ({r:,} rows)")
    except:
        pass


# ── 8) Column check on old customer tables ────────────────────────────────────────
print("\n[8] Column check on old customer tables:")
for tbl in ["wealth_insights_customer1", "Wealth_Insights_Customer1",
            "wealth_insights_cust1", "wic2_wealth_fact"]:
    try:
        cols = [c.name for c in spark.sql(f"SELECT * FROM {DEFAULT_DB}.{tbl} LIMIT 0").schema]
        print(f"  {DEFAULT_DB}.{tbl}: {cols}")
    except:
        pass


print("\n" + "=" * 80)
print("  DIAGNOSTIC COMPLETE")
print("=" * 80)

spark.stop()

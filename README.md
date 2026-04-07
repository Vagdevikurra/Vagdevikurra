from pyspark.sql import SparkSession

DEFAULT_DB = "dm_ib_dev"
CUST_TABLE = f"{DEFAULT_DB}.Wealth_Insights_Customer"
ACCT_TABLE = f"{DEFAULT_DB}.Wealth_Insights_Account"

spark = SparkSession.builder.appName("validate").config("spark.sql.legacy.timeParserPolicy","LEGACY").enableHiveSupport().getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("=" * 80)
print("  WEALTH INSIGHTS — VALIDATION")
print("=" * 80)

# Schema check
print("\n[0] Customer table schema:")
spark.sql(f"SELECT * FROM {CUST_TABLE} LIMIT 0").printSchema()

# Row counts
ct = spark.sql(f"SELECT COUNT(*) c FROM {CUST_TABLE}").collect()[0]["c"]
wt = spark.sql(f"SELECT COUNT(*) c FROM {CUST_TABLE} WHERE fact_type='WEALTH'").collect()[0]["c"]
dt = spark.sql(f"SELECT COUNT(*) c FROM {CUST_TABLE} WHERE fact_type='DIGITAL'").collect()[0]["c"]
at = spark.sql(f"SELECT COUNT(*) c FROM {ACCT_TABLE}").collect()[0]["c"]
print(f"\n[1] Rows: Customer={ct:,} (WEALTH={wt:,}, DIGITAL={dt:,}), Account={at:,}")

# Sample WEALTH row
print("\n[1b] Sample WEALTH row:")
spark.sql(f"SELECT * FROM {CUST_TABLE} WHERE fact_type='WEALTH' LIMIT 3").show(truncate=False, vertical=True)

# Sample DIGITAL row
print("[1c] Sample DIGITAL row:")
spark.sql(f"SELECT * FROM {CUST_TABLE} WHERE fact_type='DIGITAL' LIMIT 3").show(truncate=False, vertical=True)

# Wealth customers per month
print("[2] Wealth customers per month (expect ~265-267k):")
spark.sql(f"""
    SELECT business_date, COUNT(DISTINCT rcif_number) AS n
    FROM {CUST_TABLE} WHERE fact_type='WEALTH'
    GROUP BY business_date ORDER BY business_date
""").show(truncate=False)

# Top of Company Digital Active
print("[3] Top of Company Digital Active (expect ~3.4M):")
spark.sql(f"""
    SELECT COUNT(DISTINCT cust_internet_banking_nbr) AS total_digital_active
    FROM {CUST_TABLE}
    WHERE fact_type='DIGITAL' AND digitally_active_flag='Digital Active'
""").show(truncate=False)

# Check DIGITAL flag distribution
print("[3b] DIGITAL rows flag distribution:")
spark.sql(f"""
    SELECT digitally_active_flag, COUNT(*) AS cnt
    FROM {CUST_TABLE} WHERE fact_type='DIGITAL'
    GROUP BY digitally_active_flag
""").show(truncate=False)

# Digital Enrollment
print("[4] Digital Enrollment Wealth (expect ~123k):")
spark.sql(f"""
    SELECT business_date,
           COUNT(DISTINCT CASE WHEN digital_flag='Digital User' THEN rcif_number END) AS enrolled
    FROM {CUST_TABLE} WHERE fact_type='WEALTH'
    GROUP BY business_date ORDER BY business_date
""").show(truncate=False)

# Check WEALTH flag distribution
print("[4b] WEALTH digital_flag distribution (latest month):")
spark.sql(f"""
    SELECT digital_flag, COUNT(*) AS cnt
    FROM {CUST_TABLE}
    WHERE fact_type='WEALTH'
      AND business_date = (SELECT MAX(business_date) FROM {CUST_TABLE} WHERE fact_type='WEALTH')
    GROUP BY digital_flag
""").show(truncate=False)

# Flags vs reference
print("[5] Flags vs Power BI reference:")
ref = {
    "2025-10-31": (64385, 59359, 89061),
    "2025-11-28": (64489, 59812, 89507),
    "2025-12-31": (64598, 60220, 89928),
    "2026-01-30": (65088, 60577, 90451),
    "2026-02-27": (65767, 60799, 91034),
}
rows = spark.sql(f"""
    SELECT business_date,
           COUNT(DISTINCT CASE WHEN olb_active_flag='OLB Active' THEN rcif_number END) AS olb,
           COUNT(DISTINCT CASE WHEN mobile_active_flag='Mobile Active' THEN rcif_number END) AS mob,
           COUNT(DISTINCT CASE WHEN digitally_active_flag='Digital Active' THEN rcif_number END) AS dig
    FROM {CUST_TABLE} WHERE fact_type='WEALTH'
    GROUP BY business_date ORDER BY business_date
""").collect()
print(f"  {'month':>12} {'olb':>8} {'ref':>8} {'d':>7} {'mob':>8} {'ref':>8} {'d':>7} {'dig':>8} {'ref':>8} {'d':>7}")
for r in rows:
    d = str(r["business_date"])
    ro, rm, rd = ref.get(d, (None, None, None))
    od = "{:+,}".format(r['olb']-ro) if ro else "-"
    md = "{:+,}".format(r['mob']-rm) if rm else "-"
    dd = "{:+,}".format(r['dig']-rd) if rd else "-"
    ro_s = "{:>,}".format(ro) if ro else "-"
    rm_s = "{:>,}".format(rm) if rm else "-"
    rd_s = "{:>,}".format(rd) if rd else "-"
    print(f"  {d:>12} {r['olb']:>8,} {ro_s:>8} {od:>7} {r['mob']:>8,} {rm_s:>8} {md:>7} {r['dig']:>8,} {rd_s:>8} {dd:>7}")

# Penetration
print("\n[6] Penetration (expect ~34.87%):")
spark.sql(f"""
    SELECT business_date,
           COUNT(DISTINCT rcif_number) AS total,
           COUNT(DISTINCT CASE WHEN digitally_active_flag='Digital Active' THEN rcif_number END) AS active,
           ROUND(100.0 * COUNT(DISTINCT CASE WHEN digitally_active_flag='Digital Active' THEN rcif_number END)
                       / COUNT(DISTINCT rcif_number), 2) AS pct
    FROM {CUST_TABLE} WHERE fact_type='WEALTH'
    GROUP BY business_date ORDER BY business_date
""").show(truncate=False)

# Business group
print("[7] Business group breakdown (latest month):")
spark.sql(f"""
    SELECT business_group, COUNT(DISTINCT rcif_number) AS customers
    FROM {CUST_TABLE}
    WHERE fact_type='WEALTH'
      AND business_date = (SELECT MAX(business_date) FROM {CUST_TABLE} WHERE fact_type='WEALTH')
    GROUP BY business_group ORDER BY customers DESC
""").show(truncate=False)

# InvestPath
print("[8] InvestPath (expect: 119 cust, 115 accts, $1.75M):")
spark.sql(f"""
    SELECT COUNT(DISTINCT ip_id) AS customers, COUNT(*) AS accounts,
           ROUND(SUM(balance),2) AS aum,
           ROUND(SUM(balance)/NULLIF(COUNT(*),0),2) AS avg_bal,
           SUM(CASE WHEN balance > 0 THEN 1 ELSE 0 END) AS funded
    FROM {ACCT_TABLE}
""").show(truncate=False)

print("[9] InvestPath by state:")
spark.sql(f"""
    SELECT state_name, COUNT(DISTINCT ip_id) AS cust, COUNT(*) AS accts
    FROM {ACCT_TABLE} WHERE state_name IS NOT NULL
    GROUP BY state_name ORDER BY cust DESC LIMIT 10
""").show(truncate=False)

print("\n" + "=" * 80)
print("  DONE")
print("=" * 80)
spark.stop()

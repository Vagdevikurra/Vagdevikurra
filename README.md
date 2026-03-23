from pyspark.sql import SparkSession
from pyspark import SparkConf

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

print(f"  wealth_insights_cust total     : {cust_total:>12,}")
print(f"    fact_type = WEALTH           : {wealth_rows:>12,}")
print(f"    fact_type = DIGITAL          : {digital_rows:>12,}")
print(f"  wealth_insights_acct           : {acct_rows:>12,}")


# ── 2) Wealth customers per month ────────────────────────────────────────────────
print("\n[2] Wealth customers per month (expect ~265-267k):")
spark.sql(f"""
    SELECT business_date,
           COUNT(DISTINCT rcif_number) AS wealth_customers
    FROM {DEFAULT_DB}.wealth_insights_cust
    WHERE fact_type = 'WEALTH'
    GROUP BY business_date ORDER BY business_date
""").show(truncate=False)


# ── 3) Top of Company Digital Active (expect ~3.4M total) ────────────────────────
print("[3] Top of Company Digital Active (from DIGITAL rows):")
spark.sql(f"""
    SELECT COUNT(DISTINCT cust_internet_banking_nbr) AS total_digital_active
    FROM {DEFAULT_DB}.wealth_insights_cust
    WHERE fact_type = 'DIGITAL'
      AND digitally_active_flag = 'Digital Active'
""").show(truncate=False)


# ── 4) Digital Enrollment Wealth (expect ~123k) ─────────────────────────────────
print("[4] Digital Enrollment Wealth (wealth customers who are digital users):")
spark.sql(f"""
    SELECT business_date,
           COUNT(DISTINCT CASE WHEN digital_flag = 'Digital User' THEN rcif_number END) AS digital_enrolled
    FROM {DEFAULT_DB}.wealth_insights_cust
    WHERE fact_type = 'WEALTH'
    GROUP BY business_date ORDER BY business_date
""").show(truncate=False)


# ── 5) Wealth Active flags vs Power BI reference ────────────────────────────────
print("[5] Wealth active flags vs Power BI reference:")
print("    (Power BI reference → OLB / MOB / Digital Active)")

ref = {
    "2025-09-30": (None,  None,  None),
    "2025-10-31": (64385, 59359, 89061),
    "2025-11-28": (64489, 59812, 89507),
    "2025-12-31": (64598, 60220, 89928),
    "2026-01-30": (65088, 60577, 90451),
    "2026-02-27": (65767, 60799, 91034),
}

rows = spark.sql(f"""
    SELECT business_date,
           COUNT(DISTINCT CASE WHEN olb_active_flag      = 'OLB Active'     THEN rcif_number END) AS olb,
           COUNT(DISTINCT CASE WHEN mobile_active_flag   = 'Mobile Active'  THEN rcif_number END) AS mob,
           COUNT(DISTINCT CASE WHEN digitally_active_flag = 'Digital Active' THEN rcif_number END) AS dig
    FROM {DEFAULT_DB}.wealth_insights_cust
    WHERE fact_type = 'WEALTH'
    GROUP BY business_date ORDER BY business_date
""").collect()

print(f"  {'month':>12}  {'olb_ours':>9}  {'olb_ref':>9}  {'olb_Δ':>8}  {'mob_ours':>9}  {'mob_ref':>9}  {'mob_Δ':>8}  {'dig_ours':>9}  {'dig_ref':>9}  {'dig_Δ':>8}")
print("  " + "-" * 105)
for r in rows:
    dt = str(r["business_date"])
    ref_olb, ref_mob, ref_dig = ref.get(dt, (None, None, None))
    olb_d = f"{r['olb']-ref_olb:>+,}" if ref_olb else "n/a"
    mob_d = f"{r['mob']-ref_mob:>+,}" if ref_mob else "n/a"
    dig_d = f"{r['dig']-ref_dig:>+,}" if ref_dig else "n/a"
    ref_olb_s = f"{ref_olb:>,}" if ref_olb else "n/a"
    ref_mob_s = f"{ref_mob:>,}" if ref_mob else "n/a"
    ref_dig_s = f"{ref_dig:>,}" if ref_dig else "n/a"
    print(f"  {dt:>12}  {r['olb']:>9,}  {ref_olb_s:>9}  {olb_d:>8}  {r['mob']:>9,}  {ref_mob_s:>9}  {mob_d:>8}  {r['dig']:>9,}  {ref_dig_s:>9}  {dig_d:>8}")


# ── 6) Business group breakdown (expect ~177k Investment Svc, ~84k Private Wealth)
print("\n[6] Business group breakdown (latest month):")
spark.sql(f"""
    SELECT business_group, COUNT(DISTINCT rcif_number) AS customers
    FROM {DEFAULT_DB}.wealth_insights_cust
    WHERE fact_type = 'WEALTH'
      AND business_date = (SELECT MAX(business_date) FROM {DEFAULT_DB}.wealth_insights_cust WHERE fact_type = 'WEALTH')
    GROUP BY business_group ORDER BY customers DESC
""").show(truncate=False)


# ── 7) Account table summary ────────────────────────────────────────────────────
print("[7] Account table summary:")
spark.sql(f"""
    SELECT business_date,
           COUNT(DISTINCT ip_id) AS ip_customers,
           COUNT(*)              AS ip_rows,
           ROUND(SUM(ip_balance), 2) AS total_aum,
           ROUND(SUM(ip_balance) / NULLIF(COUNT(*), 0), 2) AS avg_balance
    FROM {DEFAULT_DB}.wealth_insights_acct
    GROUP BY business_date ORDER BY business_date
""").show(truncate=False)


# ── 8) Accounts per user (expect ~6.48) ──────────────────────────────────────────
print("[8] Accounts per user (expect ~6.48):")
spark.sql(f"""
    SELECT ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT rcif_number), 2) AS accounts_per_user
    FROM {DEFAULT_DB}.wealth_insights_acct
""").show(truncate=False)


# ── 9) Wealth Digital Active Penetration (expect ~34.87%) ────────────────────────
print("[9] Wealth Digital Active Penetration (expect ~34.87%):")
spark.sql(f"""
    SELECT business_date,
           COUNT(DISTINCT rcif_number) AS wealth_total,
           COUNT(DISTINCT CASE WHEN digitally_active_flag = 'Digital Active' THEN rcif_number END) AS dig_active,
           ROUND(100.0 * COUNT(DISTINCT CASE WHEN digitally_active_flag = 'Digital Active' THEN rcif_number END)
                       / COUNT(DISTINCT rcif_number), 2) AS penetration_pct
    FROM {DEFAULT_DB}.wealth_insights_cust
    WHERE fact_type = 'WEALTH'
    GROUP BY business_date ORDER BY business_date
""").show(truncate=False)


print("\n" + "=" * 80)
print("  VALIDATION COMPLETE")
print("=" * 80)

spark.stop()

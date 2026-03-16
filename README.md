from pyspark.sql import SparkSession, functions as F
from pyspark import SparkConf

# ── Config ────────────────────────────────────────────────────────────────────
DEFAULT_DB = "dm_ib_dev"

conf = SparkConf().setAppName("wealth_insights_validation")
spark = (
    SparkSession.builder
    .config(conf=conf)
    .enableHiveSupport()
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ── Load output tables ────────────────────────────────────────────────────────
cust = spark.table(f"{DEFAULT_DB}.wealth_insights_customer")
acct = spark.table(f"{DEFAULT_DB}.wealth_insights_account")

# =============================================================================
# VALIDATION CHECKS vs DAX MEASURES
# =============================================================================
print("=" * 65)
print("  WEALTH INSIGHTS — VALIDATION vs DAX MEASURES")
print("=" * 65)

# [1] Wealth Customers — expect ~267,664
wealth_cust_count = (
    cust
    .filter(F.col("Business_Group").isNotNull())
    .select(F.countDistinct("RCIF_NUMBER").alias("n"))
    .collect()[0]["n"]
)
print(f"\n[1]  Wealth Customers        (expect ~267,664): {wealth_cust_count:>12,}")

# [2] Digital Enrollment Wealth — expect ~123,379
digital_enroll = (
    cust
    .filter(
        F.col("Business_Group").isNotNull() &
        (F.col("Digital_flag") == "Digital User")
    )
    .select(F.countDistinct("RCIF_NUMBER").alias("n"))
    .collect()[0]["n"]
)
print(f"[2]  Digital Enrollment      (expect ~123,379): {digital_enroll:>12,}")

# [3] Wealth Digital Active — expect 88k-91k
wealth_dig_active = (
    cust
    .filter(
        F.col("Business_Group").isNotNull() &
        (F.col("Digitally_Active_Flag") == "Digital Active")
    )
    .select(F.countDistinct("RCIF_NUMBER").alias("n"))
    .collect()[0]["n"]
)
print(f"[3]  Wealth Digital Active   (expect 88k-91k):  {wealth_dig_active:>12,}")

# [4] Wealth OLB Active — expect 63k-65k
wealth_olb = (
    cust
    .filter(
        F.col("Business_Group").isNotNull() &
        (F.col("OLB_Active_Flag") == "OLB Active")
    )
    .select(F.countDistinct("RCIF_NUMBER").alias("n"))
    .collect()[0]["n"]
)
print(f"[4]  Wealth OLB Active       (expect 63k-65k):  {wealth_olb:>12,}")

# [5] Wealth Mobile Active — expect 59k-61k
wealth_mob = (
    cust
    .filter(
        F.col("Business_Group").isNotNull() &
        (F.col("Mobile_Active_Flag") == "Mobile Active")
    )
    .select(F.countDistinct("RCIF_NUMBER").alias("n"))
    .collect()[0]["n"]
)
print(f"[5]  Wealth Mobile Active    (expect 59k-61k):  {wealth_mob:>12,}")

# [6] Digital Penetration % — expect ~35%
pen_pct = round((wealth_dig_active / wealth_cust_count) * 100, 2) if wealth_cust_count else 0
print(f"[6]  Digital Penetration %   (expect ~35%):     {pen_pct:>11.2f}%")

# [7] Total Accounts (accts_cnt sum) — expect ~600k
total_accts = (
    cust
    .filter(F.col("Business_Group").isNotNull())
    .join(
        acct.select("RCIF_NUMBER", "accts_cnt").distinct(),
        "RCIF_NUMBER", "left"
    )
    .agg(F.sum("accts_cnt").alias("total"))
    .collect()[0]["total"]
)
print(f"[7]  Total Accounts          (expect ~600k):    {total_accts:>12,}")

# [8] Accounts per user — expect ~6.5
accts_per_user = round(total_accts / wealth_cust_count, 2) if wealth_cust_count else 0
print(f"[8]  Accounts per User       (expect ~6.5):     {accts_per_user:>12.2f}")

# [9] InvestPath Customers — expect ~123
ip_cust = (
    acct
    .select(F.countDistinct("ip_id").alias("n"))
    .collect()[0]["n"]
)
print(f"\n[9]  InvestPath Customers    (expect ~123):     {ip_cust:>12,}")

# [10] InvestPath Accounts — expect ~118
ip_accts = (
    acct
    .select(F.countDistinct("arrangement_id").alias("n"))
    .collect()[0]["n"]
)
print(f"[10] InvestPath Accounts     (expect ~118):     {ip_accts:>12,}")

# [11] InvestPath Funded Accounts — expect ~108
ip_funded = (
    acct
    .filter(F.col("balance") > 0)
    .select(F.countDistinct("arrangement_id").alias("n"))
    .collect()[0]["n"]
)
print(f"[11] IP Funded Accounts      (expect ~108):     {ip_funded:>12,}")

# [12] AUM — expect ~$1.83M
aum = acct.agg(F.sum("balance").alias("t")).collect()[0]["t"] or 0
aum_m = round(aum / 1_000_000, 2)
print(f"[12] AUM                     (expect ~$1.83M):  ${aum_m:>10.2f}M")

# [13] Avg Balance per IP Account — expect $15k-$18k
avg_bal = round(aum / ip_accts, 2) if ip_accts else 0
print(f"[13] Avg Balance/IP Acct     (expect $15k-18k): ${avg_bal:>10,.2f}")

# ── Business Group & Division breakdown ───────────────────────────────────────
print("\n" + "=" * 65)
print("  BUSINESS GROUP BREAKDOWN  (expect: PW~177k, IS~64k, Inv Svc~26k)")
print("=" * 65)
(
    cust
    .filter(F.col("Business_Group").isNotNull())
    .groupBy("Business_Group")
    .agg(F.countDistinct("RCIF_NUMBER").alias("Customers"))
    .orderBy(F.desc("Customers"))
    .show(truncate=False)
)

print("=" * 65)
print("  DIVISION BREAKDOWN")
print("=" * 65)
(
    cust
    .filter(F.col("division").isNotNull())
    .groupBy("Business_Group", "division")
    .agg(F.countDistinct("RCIF_NUMBER").alias("Customers"))
    .orderBy("Business_Group", F.desc("Customers"))
    .show(truncate=False)
)

print("=" * 65)
print("  DIGITAL FLAGS (Wealth customers only)")
print("=" * 65)
(
    cust
    .filter(F.col("Business_Group").isNotNull())
    .groupBy("Digitally_Active_Flag", "OLB_Active_Flag", "Mobile_Active_Flag")
    .agg(F.countDistinct("RCIF_NUMBER").alias("Customers"))
    .orderBy(F.desc("Customers"))
    .show(truncate=False)
)

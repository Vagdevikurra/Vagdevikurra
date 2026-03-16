from pyspark.sql import SparkSession, functions as F
from pyspark import SparkConf

DEFAULT_DB = "dm_ib_dev"
conf = SparkConf().setAppName("wealth_validation")
spark = (
    SparkSession.builder
    .config(conf=conf)
    .enableHiveSupport()
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

cust = spark.table(f"{DEFAULT_DB}.wealth_insights_customer")
acct = spark.table(f"{DEFAULT_DB}.wealth_insights_account")

print("=" * 65)
print("  WEALTH INSIGHTS — VALIDATION vs DAX")
print("=" * 65)

# NOTE: cust has multiple rows per customer (one per segment/Business_Group)
# DISTINCTCOUNT is used for all customer counts — same as Power BI

# [1] Wealth Customers — expect ~267,664
n1 = cust.filter(F.col("Business_Group").isNotNull()) \
         .select(F.countDistinct("RCIF_NUMBER")).collect()[0][0]
print(f"\n[1]  Wealth Customers        (expect ~267,664):  {n1:>10,}")

# [2] Digital Enrollment Wealth — expect ~123,379
n2 = cust.filter(F.col("Business_Group").isNotNull()) \
         .filter(F.col("Digital_flag") == "Digital User") \
         .select(F.countDistinct("RCIF_NUMBER")).collect()[0][0]
print(f"[2]  Digital Enrollment      (expect ~123,379):  {n2:>10,}")

# [3] Wealth Digital Active — expect 88k-91k
n3 = cust.filter(F.col("Business_Group").isNotNull()) \
         .filter(F.col("Digitally_Active_Flag") == "Digital Active") \
         .select(F.countDistinct("RCIF_NUMBER")).collect()[0][0]
print(f"[3]  Wealth Digital Active   (expect 88k-91k):   {n3:>10,}")

# [4] Wealth OLB Active — expect 63k-65k
n4 = cust.filter(F.col("Business_Group").isNotNull()) \
         .filter(F.col("OLB_Active_Flag") == "OLB Active") \
         .select(F.countDistinct("RCIF_NUMBER")).collect()[0][0]
print(f"[4]  Wealth OLB Active       (expect 63k-65k):   {n4:>10,}")

# [5] Wealth Mobile Active — expect 59k-61k
n5 = cust.filter(F.col("Business_Group").isNotNull()) \
         .filter(F.col("Mobile_Active_Flag") == "Mobile Active") \
         .select(F.countDistinct("RCIF_NUMBER")).collect()[0][0]
print(f"[5]  Wealth Mobile Active    (expect 59k-61k):   {n5:>10,}")

# [6] Digital Penetration — expect ~35%
pct = round(n3 / n1 * 100, 2) if n1 else 0
print(f"[6]  Digital Penetration %   (expect ~35%):      {pct:>9.2f}%")

# [7] Total Accounts (COUNT of rows where accts_cnt not null) — expect ~600k
n7 = cust.filter(F.col("Business_Group").isNotNull()) \
         .filter(F.col("accts_cnt").isNotNull()) \
         .count()
print(f"[7]  Total Accounts          (expect ~600k):     {n7:>10,}")

# [8] Accounts per user — expect ~6.5
apu = round(n7 / n1, 2) if n1 else 0
print(f"[8]  Accounts per User       (expect ~6.5):      {apu:>10.2f}")

# [9-13] InvestPath
n9  = acct.select(F.countDistinct("ip_id")).collect()[0][0]
n10 = acct.select(F.countDistinct("arrangement_id")).collect()[0][0]
n11 = acct.filter(F.col("balance") > 0).select(F.countDistinct("arrangement_id")).collect()[0][0]
aum = float(acct.agg(F.sum("balance")).collect()[0][0] or 0)
avg = round(aum / n10, 2) if n10 else 0

print(f"\n[9]  InvestPath Customers    (expect ~123):      {n9:>10,}")
print(f"[10] InvestPath Accounts     (expect ~118):      {n10:>10,}")
print(f"[11] IP Funded Accounts      (expect ~108):      {n11:>10,}")
print(f"[12] AUM                     (expect ~$1.83M):   ${aum/1e6:>9.2f}M")
print(f"[13] Avg Balance/IP Acct     (expect $15k-18k):  ${avg:>9,.2f}")

print("\n" + "=" * 65)
print("  BUSINESS GROUP  (expect: PW~177k, IS~64k, InvSvc~26k)")
print("  NOTE: DISTINCTCOUNT per group — customers may span multiple groups")
print("=" * 65)
cust.filter(F.col("Business_Group").isNotNull()) \
    .groupBy("Business_Group") \
    .agg(F.countDistinct("RCIF_NUMBER").alias("Customers")) \
    .orderBy(F.desc("Customers")).show(truncate=False)

print("=" * 65)
print("  DIVISION BREAKDOWN")
print("=" * 65)
cust.filter(F.col("division").isNotNull()) \
    .groupBy("Business_Group", "division") \
    .agg(F.countDistinct("RCIF_NUMBER").alias("Customers")) \
    .orderBy("Business_Group", F.desc("Customers")).show(truncate=False)

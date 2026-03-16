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

# [1] Wealth Customers ~267,664
n = cust.filter(F.col("Business_Group").isNotNull()) \
        .select(F.countDistinct("RCIF_NUMBER")).collect()[0][0]
print(f"\n[1]  Wealth Customers        (expect ~267,664):  {n:>10,}")

# [2] Digital Enrollment ~123,379
# DAX: Wealth customers WHERE digital_flag = "Digital User"
n2 = cust.filter(F.col("Business_Group").isNotNull()) \
         .filter(F.col("Digital_flag") == "Digital User") \
         .select(F.countDistinct("RCIF_NUMBER")).collect()[0][0]
print(f"[2]  Digital Enrollment      (expect ~123,379):  {n2:>10,}")

# [3] Wealth Digital Active 88k-91k
n3 = cust.filter(F.col("Business_Group").isNotNull()) \
         .filter(F.col("Digitally_Active_Flag") == "Digital Active") \
         .select(F.countDistinct("RCIF_NUMBER")).collect()[0][0]
print(f"[3]  Wealth Digital Active   (expect 88k-91k):   {n3:>10,}")

# [4] Wealth OLB Active 63k-65k
n4 = cust.filter(F.col("Business_Group").isNotNull()) \
         .filter(F.col("OLB_Active_Flag") == "OLB Active") \
         .select(F.countDistinct("RCIF_NUMBER")).collect()[0][0]
print(f"[4]  Wealth OLB Active       (expect 63k-65k):   {n4:>10,}")

# [5] Wealth Mobile Active 59k-61k
n5 = cust.filter(F.col("Business_Group").isNotNull()) \
         .filter(F.col("Mobile_Active_Flag") == "Mobile Active") \
         .select(F.countDistinct("RCIF_NUMBER")).collect()[0][0]
print(f"[5]  Wealth Mobile Active    (expect 59k-61k):   {n5:>10,}")

# [6] Digital Penetration ~35%
pct = round(n3 / n * 100, 2) if n else 0
print(f"[6]  Digital Penetration %   (expect ~35%):      {pct:>9.2f}%")

# [7] Total Accounts ~600k
# DAX: COUNT(Wealth[pw1.accts_cnt]) - accts_cnt now in customer table
total_accts = cust.filter(F.col("Business_Group").isNotNull()) \
                  .agg(F.sum("accts_cnt")).collect()[0][0] or 0
print(f"[7]  Total Accounts          (expect ~600k):     {total_accts:>10,}")

# [8] Accounts per user ~6.5
apu = round(total_accts / n, 2) if n else 0
print(f"[8]  Accounts per User       (expect ~6.5):      {apu:>10.2f}")

# [9] InvestPath Customers ~123
n9 = acct.select(F.countDistinct("ip_id")).collect()[0][0]
print(f"\n[9]  InvestPath Customers    (expect ~123):      {n9:>10,}")

# [10] InvestPath Accounts ~118
n10 = acct.select(F.countDistinct("arrangement_id")).collect()[0][0]
print(f"[10] InvestPath Accounts     (expect ~118):      {n10:>10,}")

# [11] InvestPath Funded ~108
n11 = acct.filter(F.col("balance") > 0) \
          .select(F.countDistinct("arrangement_id")).collect()[0][0]
print(f"[11] IP Funded Accounts      (expect ~108):      {n11:>10,}")

# [12] AUM ~$1.83M
aum = acct.agg(F.sum("balance")).collect()[0][0] or 0
print(f"[12] AUM                     (expect ~$1.83M):   ${aum/1e6:>9.2f}M")

# [13] Avg Balance $15k-18k
avg = round(aum / n10, 2) if n10 else 0
print(f"[13] Avg Balance/IP Acct     (expect $15k-18k):  ${avg:>9,.2f}")

# Business Group breakdown
print("\n" + "=" * 65)
print("  BUSINESS GROUP  (expect: PW~177k, IS~64k, InvSvc~26k)")
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

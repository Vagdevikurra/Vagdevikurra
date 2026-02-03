from pyspark.sql import SparkSession, functions as F

# ==========================================================
# CONFIG
# ==========================================================
DB = "dm_ib_dev"
START_DATE = "2025-08-01"
END_DATE   = "2026-01-31"

# ==========================================================
# Spark
# ==========================================================
def get_spark(app_name="wid2_use_source_totally_active_flag"):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .enableHiveSupport()
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "300")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

spark = get_spark()
spark.sql(f"USE {DB}")

# ==========================================================
# Tables
# ==========================================================
DBM = spark.table(f"{DB}.digital_banking_master")  # dm_ib_dev.digital_banking_master
IP  = spark.table("eil.d_involved_party_h")

# ==========================================================
# Snapshot date within window (for comparison)
# ==========================================================
snapshot_dt = (
    DBM
    .where((F.to_date("ods_business_dt") >= F.lit(START_DATE)) &
           (F.to_date("ods_business_dt") <= F.lit(END_DATE)))
    .select(F.max(F.to_date("ods_business_dt")).alias("dt"))
    .first()["dt"]
)

print(f"Window: {START_DATE} -> {END_DATE} | Snapshot date used: {snapshot_dt}")

# ==========================================================
# DIGITAL (DBM-only) using SOURCE FLAG (matches dashboard)
#   - ibn column is 'ibn'
#   - activity flag column assumed 'totally_active_flag' (your wording)
# ==========================================================
dig = (
    DBM
    .where(F.to_date("ods_business_dt") == F.lit(snapshot_dt))
    .select(
        F.to_date("ods_business_dt").alias("ods_business_dt"),
        F.upper(F.trim(F.col("ibn").cast("string"))).alias("reltibn"),
        F.col("totally_active_flag").cast("string").alias("digitally_active_flag")
    )
    .where(F.col("reltibn").isNotNull() & (F.length("reltibn") > 0))
)

print("✅ KPI (dashboard): DISTINCTCOUNT(ibn) where totally_active_flag = 'Digital Active'")
dig.where(F.col("digitally_active_flag") == "Digital Active") \
   .selectExpr("count(distinct reltibn) as digital_active_ibn") \
   .show(truncate=False)

# ==========================================================
# RCIF mapping (for relationships in Power BI)
# ==========================================================
ip_snapshot_dt = (
    IP.select(F.max(F.to_date("business_date")).alias("dt"))
      .first()["dt"]
)

rcif_map = (
    IP
    .where(F.to_date("business_date") == F.lit(ip_snapshot_dt))
    .where(F.col("source_system_code") == "CF")
    .where(F.coalesce(F.col("deceased_ind"), F.lit("N")) == "N")
    .select(
        F.upper(F.trim(F.col("cust_internet_banking_nbr").cast("string"))).alias("reltibn"),
        F.col("rcif_cust_nbr").cast("string").alias("rcif_number"),
        F.col("involved_party_id").alias("involved_party_id"),
        F.col("cust_internet_banking_nbr").alias("cust_internet_banking_nbr")
    )
    .where(F.col("reltibn").isNotNull() & (F.length("reltibn") > 0))
)

# ==========================================================
# wid2 (what you load to Power BI: includes RCIF)
# ==========================================================
wid2 = (
    dig
    .join(rcif_map, on="reltibn", how="left")   # LEFT so you don't lose IBNs in digital table
    .select(
        "ods_business_dt",
        "reltibn",
        "digitally_active_flag",
        "rcif_number",
        "involved_party_id",
        "cust_internet_banking_nbr"
    )
)

print("ℹ️ Coverage check: IBNs in digital that have RCIF mapping")
wid2.where(F.col("rcif_number").isNotNull()) \
    .selectExpr("count(distinct reltibn) as ibn_with_rcif") \
    .show(truncate=False)

# Save if needed:
# wid2.write.mode("overwrite").saveAsTable(f"{DB}.wid2")

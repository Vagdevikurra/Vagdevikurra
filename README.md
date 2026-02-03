from pyspark.sql import SparkSession, functions as F

# ==========================================================
# CONFIG
# ==========================================================
DB = "dm_ib_dev"

START_DATE = "2025-08-01"
END_DATE   = "2026-01-31"

# ==========================================================
# Spark (same style as your other code)
# ==========================================================
def get_spark(app_name="wid2_windowed_snapshot"):
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
# Source tables (NO guard rails)
# ==========================================================
DBM = spark.table(f"{DB}.digital_banking_master")  # << dm_ib_dev.digital_banking_master
IP  = spark.table("eil.d_involved_party_h")        # use d_ as per your screenshots

# ==========================================================
# 1) Find snapshot date WITHIN window
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
# 2) Dig_Customer (snapshot only) — GROUP BY ibn + ods_business_dt
#    NOTE: ibn column name is 'ibn' (per your message)
# ==========================================================
dig_customer = (
    DBM
    .where(F.to_date("ods_business_dt") == F.lit(snapshot_dt))
    .select(
        F.upper(F.trim(F.col("ibn").cast("string"))).alias("reltibn"),
        F.to_date("ods_business_dt").alias("ods_business_dt"),
        F.to_date(F.col("olb_last_login_date")).alias("lst_login_olb"),
        F.to_date(F.col("mob_last_login_date")).alias("lst_login_mob")
    )
    .where(F.col("reltibn").isNotNull() & (F.length("reltibn") > 0))
    .groupBy("reltibn", "ods_business_dt")
    .agg(
        F.max("lst_login_olb").alias("lst_login_olb"),
        F.max("lst_login_mob").alias("lst_login_mob")
    )
)

# ==========================================================
# 3) RCIF mapping (snapshot on IP side) — JOIN via cust_internet_banking_nbr = ibn
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
        F.col("cust_internet_banking_nbr").alias("cust_internet_banking_nbr"),
        F.col("involved_party_id").alias("involved_party_id")
    )
    .where(F.col("reltibn").isNotNull() & (F.length("reltibn") > 0))
)

# ==========================================================
# 4) wid2 FINAL (same flag logic as your SQL)
# ==========================================================
wid2 = (
    dig_customer
    .join(rcif_map, on="reltibn", how="inner")
    .withColumn(
        "mobile_active_flag",
        F.when(F.datediff(F.col("ods_business_dt"), F.col("lst_login_mob")) <= 90, F.lit("Mobile Active"))
         .otherwise(F.lit("Non Mobile Active"))
    )
    .withColumn(
        "mobile_flag",
        F.when(F.col("lst_login_mob").isNull(), F.lit("Non Mobile User")).otherwise(F.lit("Mobile User"))
    )
    .withColumn(
        "olb_active_flag",
        F.when(F.datediff(F.col("ods_business_dt"), F.col("lst_login_olb")) <= 90, F.lit("OLB Active"))
         .otherwise(F.lit("Non OLB Active"))
    )
    .withColumn(
        "olb_flag",
        F.when(F.col("lst_login_olb").isNull(), F.lit("Non OLB User")).otherwise(F.lit("OLB User"))
    )
    .withColumn(
        "digitally_active_flag",
        F.when(
            (F.datediff(F.col("ods_business_dt"), F.col("lst_login_mob")) <= 90) |
            (F.datediff(F.col("ods_business_dt"), F.col("lst_login_olb")) <= 90),
            F.lit("Digital Active")
        ).otherwise(F.lit("Non Digital Active"))
    )
    .withColumn(
        "digital_flag",
        F.when(F.col("rcif_number").isNull(), F.lit("Non Digital User")).otherwise(F.lit("Digital User"))
    )
    .select(
        "ods_business_dt",
        "reltibn",
        "rcif_number",
        "involved_party_id",
        "cust_internet_banking_nbr",
        "lst_login_olb",
        "lst_login_mob",
        "mobile_active_flag",
        "mobile_flag",
        "olb_active_flag",
        "olb_flag",
        "digitally_active_flag",
        "digital_flag"
    )
)

# ==========================================================
# SANITY
# ==========================================================
print("Digital Active IBN on snapshot day:")
wid2.where(F.col("digitally_active_flag") == "Digital Active") \
    .selectExpr("count(distinct reltibn) as digital_active_ibn") \
    .show(truncate=False)

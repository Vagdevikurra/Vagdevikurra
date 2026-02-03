from pyspark.sql import SparkSession, functions as F

DB = "dm_ib_dev"

def get_spark(app_name="wid2_force_34m_global_snapshot"):
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

DBM = spark.table(f"{DB}.digital_banking_master")
IP  = spark.table("eil.d_involved_party_h")

# ==========================================================
# 1) FORCE the same snapshot as source SQL: GLOBAL MAX
# ==========================================================
snapshot_dt = DBM.select(F.max(F.to_date("ods_business_dt")).alias("dt")).first()["dt"]
print("✅ GLOBAL snapshot_dt used (drives ~3.4M):", snapshot_dt)

# ==========================================================
# 2) Digital-only (exact) + 90-day flags
# ==========================================================
dig_customer = (
    DBM
    .where(F.to_date("ods_business_dt") == F.lit(snapshot_dt))
    .select(
        F.upper(F.trim(F.col("ibn").cast("string"))).alias("reltibn"),
        F.to_date("ods_business_dt").alias("ods_business_dt"),
        F.to_date(F.col("olb_last_login_date")).alias("lst_login_olb"),
        F.to_date(F.col("mob_last_login_date")).alias("lst_login_mob"),
    )
    .where(F.col("reltibn").isNotNull() & (F.length("reltibn") > 0))
    .groupBy("reltibn", "ods_business_dt")
    .agg(
        F.max("lst_login_olb").alias("lst_login_olb"),
        F.max("lst_login_mob").alias("lst_login_mob")
    )
)

wid2_digital = (
    dig_customer
    .withColumn(
        "digitally_active_flag",
        F.when(
            (F.datediff(F.col("ods_business_dt"), F.col("lst_login_mob")) <= 90) |
            (F.datediff(F.col("ods_business_dt"), F.col("lst_login_olb")) <= 90),
            F.lit("Digital Active")
        ).otherwise(F.lit("Non Digital Active"))
    )
    .select("ods_business_dt", "reltibn", "digitally_active_flag")
)

# ==========================================================
# 3) THIS is the 3.4M KPI (digital-only, no RCIF dependency)
# ==========================================================
print("✅ KPI: Top Digital Active IBN (should be ~3.4M)")
wid2_digital.where(F.col("digitally_active_flag") == "Digital Active") \
    .selectExpr("count(distinct reltibn) as top_digital_active_ibn") \
    .show(truncate=False)

# ==========================================================
# 4) OPTIONAL: add RCIF number ONLY for wic2 relationship (LEFT JOIN)
# ==========================================================
ip_snapshot_dt = IP.select(F.max(F.to_date("business_date")).alias("dt")).first()["dt"]

rcif_map = (
    IP
    .where(F.to_date("business_date") == F.lit(ip_snapshot_dt))
    .where(F.col("source_system_code") == "CF")
    .where(F.coalesce(F.col("deceased_ind"), F.lit("N")) == "N")
    .select(
        F.upper(F.trim(F.col("cust_internet_banking_nbr").cast("string"))).alias("reltibn"),
        F.col("rcif_cust_nbr").cast("string").alias("rcif_number")
    )
    .groupBy("reltibn")
    .agg(F.max("rcif_number").alias("rcif_number"))
)

wid2 = (
    wid2_digital
    .join(rcif_map, on="reltibn", how="left")
    .select("ods_business_dt", "reltibn", "rcif_number", "digitally_active_flag")
)

# Save if you want:
# wid2.write.mode("overwrite").saveAsTable(f"{DB}.wid2")

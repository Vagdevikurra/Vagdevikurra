from pyspark.sql import SparkSession, functions as F

# ==========================================================
# Spark
# ==========================================================
spark = (
    SparkSession.builder
    .appName("digital_active_monthly_window_0801_0131")
    .enableHiveSupport()
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.shuffle.partitions", "300")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

spark.sql("USE dm_ib_dev")

DBM = spark.table("dm_ib_dev.digital_banking_master")

START_DATE = "2025-08-01"
END_DATE   = "2026-01-31"

# ==========================================================
# 1) Pull rows in window + create month_end bucket (Hive TRUNC/MM + last_day equivalent)
# ==========================================================
base = (
    DBM
    .where(
        (F.to_date("ods_business_dt") >= F.lit(START_DATE)) &
        (F.to_date("ods_business_dt") <= F.lit(END_DATE))
    )
    .select(
        F.last_day(F.to_date("ods_business_dt")).alias("month_dt"),
        F.to_date("ods_business_dt").alias("ods_business_dt"),
        F.upper(F.trim(F.col("ibn").cast("string"))).alias("reltibn"),
        F.to_date(F.col("olb_last_login_date")).alias("lst_login_olb"),
        F.to_date(F.col("mob_last_login_date")).alias("lst_login_mob"),
    )
    .where(F.col("reltibn").isNotNull() & (F.length("reltibn") > 0))
)

# ==========================================================
# 2) Monthly grain per IBN (month_dt + reltibn)
#    - take max ods_business_dt inside the month for that IBN
#    - take max login dates seen in that month
# ==========================================================
monthly = (
    base
    .groupBy("month_dt", "reltibn")
    .agg(
        F.max("ods_business_dt").alias("ods_business_dt"),
        F.max("lst_login_olb").alias("lst_login_olb"),
        F.max("lst_login_mob").alias("lst_login_mob"),
    )
)

# ==========================================================
# 3) Flags (same 90-day logic)
# ==========================================================
monthly_flagged = (
    monthly
    .withColumn(
        "digitally_active_flag",
        F.when(
            (F.col("lst_login_mob").isNotNull() & (F.datediff(F.col("ods_business_dt"), F.col("lst_login_mob")) <= 90)) |
            (F.col("lst_login_olb").isNotNull() & (F.datediff(F.col("ods_business_dt"), F.col("lst_login_olb")) <= 90)),
            F.lit("Digital Active")
        ).otherwise(F.lit("Non Digital Active"))
    )
)

# ==========================================================
# 4) FINAL KPI for the whole window:
#    Distinct IBNs that were "Digital Active" in ANY month inside 08/01–01/31
# ==========================================================
print("✅ Digital Active IBN (08/01–01/31) — monthly logic, distinct across window:")
monthly_flagged.where(F.col("digitally_active_flag") == "Digital Active") \
    .selectExpr("count(distinct reltibn) as digital_active_ibn_0801_0131") \
    .show(truncate=False)

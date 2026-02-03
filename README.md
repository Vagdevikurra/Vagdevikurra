from pyspark.sql import SparkSession, functions as F

# ==========================================================
# Spark
# ==========================================================
spark = (
    SparkSession.builder
    .appName("digital_active_0801_0131")
    .enableHiveSupport()
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

spark.sql("USE dm_ib_dev")

DBM = spark.table("dm_ib_dev.digital_banking_master")

START_DATE = "2025-08-01"
END_DATE   = "2026-01-31"

# ==========================================================
# 1) Filter DIGITAL data to 08/01–01/31 (same as Hive WHERE)
# ==========================================================
digital_window = (
    DBM
    .where(
        (F.to_date("ods_business_dt") >= F.lit(START_DATE)) &
        (F.to_date("ods_business_dt") <= F.lit(END_DATE))
    )
    .select(
        F.upper(F.trim(F.col("ibn").cast("string"))).alias("reltibn"),
        F.to_date("ods_business_dt").alias("ods_business_dt"),
        F.to_date(F.col("olb_last_login_date")).alias("lst_login_olb"),
        F.to_date(F.col("mob_last_login_date")).alias("lst_login_mob"),
    )
    .where(F.col("reltibn").isNotNull() & (F.length("reltibn") > 0))
)

# ==========================================================
# 2) Digitally Active flag (EXACT rule)
# ==========================================================
digital_active = (
    digital_window
    .withColumn(
        "digitally_active_flag",
        F.when(
            (F.datediff(F.col("ods_business_dt"), F.col("lst_login_mob")) <= 90) |
            (F.datediff(F.col("ods_business_dt"), F.col("lst_login_olb")) <= 90),
            F.lit("Digital Active")
        ).otherwise(F.lit("Non Digital Active"))
    )
)

# ==========================================================
# 3) FINAL KPI — EXACT Hive equivalent
# ==========================================================
print("✅ Digital Active IBN (08/01–01/31):")
digital_active.where(F.col("digitally_active_flag") == "Digital Active") \
    .selectExpr("count(distinct reltibn) as digital_active_ibn_0801_0131") \
    .show(truncate=False)

from pyspark.sql import SparkSession, functions as F

# ==========================================================
# Spark
# ==========================================================
spark = (
    SparkSession.builder
    .appName("wia2_wealth_0801_0131")
    .enableHiveSupport()
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.shuffle.partitions", "300")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

spark.sql("USE dm_ib_dev")

# ==========================================================
# CONFIG
# ==========================================================
START_DATE = "2025-08-01"
END_DATE   = "2026-01-31"

AR_SOURCE_SYSTEM_LIST = [
    'BI','RN','TR','DA','SV','CC','LS','MG','TM','PC','LO',
    'BW','CS','IC','MA','PF','PR','SD','CM','EL'
]

WEALTH_SEG_KEEP = ["IS_CT","IS_IT","REGIS_FC","REGIS","PWM"]
WEALTH_BG_KEEP  = ["Private Wealth","Institutional Services","Investment Services"]

# ==========================================================
# TABLES
# ==========================================================
IP  = spark.table("eil.d_involved_party_h")
A2I = spark.table("eil.d_arrangement_to_involved_party_relationship_h")
AR  = spark.table("eil.d_arrangement_h")

# ==========================================================
# 1) Wealth snapshot date INSIDE window
# ==========================================================
snapshot_dt = (
    IP
    .where(
        (F.to_date("business_date") >= F.lit(START_DATE)) &
        (F.to_date("business_date") <= F.lit(END_DATE))
    )
    .select(F.max(F.to_date("business_date")).alias("dt"))
    .first()["dt"]
)

print("Wealth snapshot date used:", snapshot_dt)

# ==========================================================
# 2) Involved Party (CF, alive)
# ==========================================================
ind = (
    IP
    .where(F.to_date("business_date") == F.lit(snapshot_dt))
    .where(F.col("source_system_code") == "CF")
    .where(F.coalesce(F.col("deceased_ind"), F.lit("N")) == "N")
    .select(
        F.col("involved_party_id").alias("ip_id"),
        F.col("rcif_cust_nbr").cast("string").alias("rcif_number"),
        F.col("cust_internet_banking_nbr").alias("cust_internet_banking_nbr"),
        F.col("private_client_code"),
        F.col("private_client_trust_code"),
        F.col("business_date")
    )
)

# ==========================================================
# 3) Arrangement to IP relationship
# ==========================================================
a2i = (
    A2I
    .where(F.to_date("business_date") == F.lit(snapshot_dt))
    .select(
        "business_date",
        "involved_party_id",
        "arrangement_id",
        "arrangement_source_system_code",
        "source_system_code"
    )
)

# ==========================================================
# 4) Arrangement (open + allowed source systems)
# ==========================================================
ar = (
    AR
    .where(F.to_date("business_date") == F.lit(snapshot_dt))
    .where(F.col("source_system_code").isin(AR_SOURCE_SYSTEM_LIST))
    .where(F.col("closed_ind") == "N")
    .select(
        "business_date",
        "arrangement_id",
        "source_system_code",
        "business_service_segment_type_code",
        "open_date",
        "current_balance_amt"
    )
)

# ==========================================================
# 5) Join chain (exact wealth logic)
# ==========================================================
joined = (
    ind.alias("ind")
    .join(
        a2i.alias("a2i"),
        (F.col("ind.ip_id") == F.col("a2i.involved_party_id")) &
        (F.col("ind.business_date") == F.col("a2i.business_date")) &
        (F.col("a2i.source_system_code") == F.col("ind.source_system_code")),
        "inner"
    )
    .join(
        ar.alias("ar"),
        (F.col("a2i.arrangement_id") == F.col("ar.arrangement_id")) &
        (F.col("a2i.arrangement_source_system_code") == F.col("ar.source_system_code")) &
        (F.col("a2i.business_date") == F.col("ar.business_date")),
        "inner"
    )
)

# ==========================================================
# 6) Business group (same logic as before)
# ==========================================================
business_group = (
    F.when(F.col("ind.private_client_code").isin("039","539","339"), F.lit("Private Wealth"))
     .when(F.col("ind.private_client_trust_code").isin("239","739"), F.lit("Private Wealth"))
     .otherwise(
        F.when(F.col("ar.business_service_segment_type_code").isin("IS_CT","IS_IT"), F.lit("Institutional Services"))
         .when(F.col("ar.business_service_segment_type_code").isin("REGIS_FC","REGIS"), F.lit("Investment Services"))
         .when(F.col("ar.business_service_segment_type_code") == "PWM", F.lit("Private Wealth"))
         .otherwise(F.lit("Other"))
     )
)

wia2_raw = (
    joined
    .withColumn("business_group", business_group)
    .select(
        F.col("ind.business_date").alias("business_date"),
        F.col("ind.rcif_number").alias("rcif_number"),
        F.col("ind.ip_id").alias("ip_id"),
        F.col("ind.cust_internet_banking_nbr").alias("customer_internet_banking_nbr"),
        F.col("ar.arrangement_id").alias("arrangement_id"),
        F.col("ar.open_date").alias("open_date"),
        F.col("ar.current_balance_amt").alias("balance"),
        F.col("ar.business_service_segment_type_code").alias("seg_code"),
        F.col("ar.source_system_code").alias("ar_source_system_code"),
        F.col("business_group")
    )
)

# ==========================================================
# 7) Wealth population filter
# ==========================================================
wia2 = (
    wia2_raw
    .where(
        (F.col("seg_code").isin(WEALTH_SEG_KEEP)) |
        (F.col("business_group").isin(WEALTH_BG_KEEP))
    )
    .dropDuplicates(["arrangement_id", "rcif_number"])
)

# ==========================================================
# SANITY
# ==========================================================
print("Accounts total (08/01–01/31 snapshot):")
wia2.selectExpr("count(distinct arrangement_id) as accounts_total").show(truncate=False)

# Optional save:
# wia2.write.mode("overwrite").saveAsTable("dm_ib_dev.wia2")

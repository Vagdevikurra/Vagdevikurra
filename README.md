from pyspark.sql import SparkSession, functions as F

# =========================
# CONFIG / PARAMS
# =========================
DB = "dm_ib_dev"

WEALTH_FQN    = f"{DB}.wealth_tbl"
DIGITAL_FQN   = f"{DB}.digital_tbl"
INVEST_FQN    = f"{DB}.investpath_tbl"

DROP_AND_RECREATE = True

# If you want a fixed range you can use these later, but Wealth is usually a single snapshot
START_DATE = "2025-07-01"
END_DATE   = "2025-12-31"

def get_spark():
    spark = (
        SparkSession.builder
        .appName("wealth_digital_investpath_3tables")
        .enableHiveSupport()
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "300")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

spark = get_spark()
spark.sql(f"USE {DB}")

# =========================
# 0) COMMON SNAPSHOT DATES
# =========================
wealth_snapshot_dt = spark.sql("select max(business_date) as dt from eil.d_involved_party_h").first()["dt"]
digital_snapshot_dt = spark.sql("select max(ods_business_dt) as dt from dm_ib.digital_banking_master").first()["dt"]

# =========================
# 1) WEALTH TABLE (Customer grain: 1 row per RCIF)
# =========================
# Minimal stable wealth customer table:
# - One row per RCIF
# - Includes ods_business_dt, accts_cnt, business_group, division, ip_id, cust_internet_banking_number
wealth_base = (
    spark.table("eil.d_involved_party_h").alias("ip")
    .join(
        spark.table("eil.d_arrangement_to_involved_party_relationship_h").alias("a2i"),
        (F.col("ip.involved_party_id") == F.col("a2i.involved_party_id")) &
        (F.col("ip.business_date") == F.col("a2i.business_date")) &
        (F.col("ip.source_system_code") == F.col("a2i.source_system_code")),
        "inner"
    )
    .join(
        spark.table("eil.d_arrangement_h").alias("ar"),
        (F.col("a2i.arrangement_id") == F.col("ar.arrangement_id")) &
        (F.col("a2i.business_date") == F.col("ar.business_date")) &
        (F.col("a2i.source_system_code") == F.col("ar.source_system_code")),
        "inner"
    )
    .where(
        (F.col("ip.business_date") == F.lit(wealth_snapshot_dt)) &
        (F.col("ip.source_system_code") == F.lit("CF")) &
        (F.coalesce(F.col("ip.deceased_ind"), F.lit("N")) == F.lit("N"))
    )
    # Business Group logic (adapt as needed)
    .withColumn(
        "business_group",
        F.when(F.col("ip.private_client_code").isin("039", "539", "339"), F.lit("Private Wealth"))
         .when(F.col("ip.private_client_trust_code").isin("239", "739"), F.lit("Private Wealth"))
         .otherwise(
            F.when(F.col("ar.business_service_segment_type_code").isin("IS_CT", "IS_IT"), F.lit("Institutional Services"))
             .when(F.col("ar.business_service_segment_type_code").isin("REGIS_FC", "REGIS"), F.lit("Investment Services"))
             .when(F.col("ar.business_service_segment_type_code") == F.lit("PWM"), F.lit("Private Wealth"))
             .otherwise(F.lit("Other"))
         )
    )
)

wealth_tbl = (
    wealth_base
    .groupBy(
        F.lit(wealth_snapshot_dt).alias("ods_business_dt"),
        F.col("ip.rcif_cust_nbr").cast("string").alias("rcif_number"),
    )
    .agg(
        F.max(F.col("ip.involved_party_id")).alias("ip_id"),
        F.max(F.col("ip.cust_internet_banking_nbr")).alias("customer_internet_banking_number"),
        F.max(F.col("business_group")).alias("business_group"),
        # If you have division logic elsewhere, replace this:
        F.max(F.col("business_group")).alias("division"),
        F.countDistinct(F.col("ar.arrangement_id")).alias("accts_cnt"),
    )
    .dropDuplicates(["rcif_number"])
)

# sanity check
wealth_tbl.selectExpr("count(distinct rcif_number) as wealth_rcif").show(truncate=False)

# =========================
# 2) DIGITAL TABLE (Digital grain: 1 row per ods_business_dt + reltibn)
# =========================
# Build flags + active flags:
# - mobile_flag: Mobile User / Non Mobile User (based on last login null)
# - olb_flag: OLB User / Non OLB User
# - mobile_active_flag / olb_active_flag: based on 90 day recency
# - digitally_active_flag: if either active
# - digital_flag: Digital User / Non Digital User (based on reltibn null - typically reltibn won't be null here)
digital_tbl = (
    spark.table("dm_ib.digital_banking_master")
    .where(
        (F.col("ods_business_dt") == F.lit(digital_snapshot_dt))  # keep a stable snapshot like your dashboard
        # If you truly want a range, replace with: between START_DATE and END_DATE + then decide how to aggregate
    )
    .groupBy("ods_business_dt", "relt_ibn", "rcif_customer_nbr")
    .agg(
        F.max("olb_last_login_date").alias("lst_login_olb"),
        F.max("mob_last_login_date").alias("lst_login_mob"),
    )
    .withColumn(
        "mobile_flag",
        F.when(F.col("lst_login_mob").isNull(), F.lit("Non Mobile User"))
         .otherwise(F.lit("Mobile User"))
    )
    .withColumn(
        "olb_flag",
        F.when(F.col("lst_login_olb").isNull(), F.lit("Non OLB User"))
         .otherwise(F.lit("OLB User"))
    )
    .withColumn(
        "mobile_active_flag",
        F.when(F.col("lst_login_mob").isNotNull() & (F.datediff(F.col("ods_business_dt"), F.col("lst_login_mob")) <= 90),
               F.lit("Mobile Active"))
         .otherwise(F.lit("Non Mobile Active"))
    )
    .withColumn(
        "olb_active_flag",
        F.when(F.col("lst_login_olb").isNotNull() & (F.datediff(F.col("ods_business_dt"), F.col("lst_login_olb")) <= 90),
               F.lit("OLB Active"))
         .otherwise(F.lit("Non OLB Active"))
    )
    .withColumn(
        "digitally_active_flag",
        F.when(
            (F.col("mobile_active_flag") == F.lit("Mobile Active")) |
            (F.col("olb_active_flag") == F.lit("OLB Active")),
            F.lit("Digital Active")
        ).otherwise(F.lit("Non Digital Active"))
    )
    .withColumn(
        "digital_flag",
        F.when(F.col("relt_ibn").isNull(), F.lit("Non Digital User"))
         .otherwise(F.lit("Digital User"))
    )
    .select(
        "ods_business_dt",
        F.col("relt_ibn").alias("reltibn"),
        F.col("rcif_customer_nbr").cast("string").alias("rcif_customer_number"),
        "mobile_active_flag",
        "mobile_flag",
        "olb_active_flag",
        "olb_flag",
        "digitally_active_flag",
        "digital_flag",
    )
    .dropDuplicates(["ods_business_dt", "reltibn"])
)

# sanity check (this should align to your ~3.4M for Digital Active reltibn)
digital_tbl.filter(F.col("digitally_active_flag") == "Digital Active") \
          .selectExpr("count(distinct reltibn) as digital_active_reltibn").show(truncate=False)

# =========================
# 3) INVESTPATH TABLE (Account grain: 1 row per ods_business_dt + arrangement_id)
# =========================
investpath_tbl = (
    spark.table("eil.d_involved_party_h").alias("ip")
    .join(
        spark.table("eil.d_arrangement_to_involved_party_relationship_h").alias("a2i"),
        (F.col("ip.involved_party_id") == F.col("a2i.involved_party_id")) &
        (F.col("ip.business_date") == F.col("a2i.business_date")) &
        (F.col("ip.source_system_code") == F.col("a2i.source_system_code")),
        "inner"
    )
    .join(
        spark.table("eil.d_arrangement_h").alias("ar"),
        (F.col("a2i.arrangement_id") == F.col("ar.arrangement_id")) &
        (F.col("a2i.business_date") == F.col("ar.business_date")) &
        (F.col("a2i.source_system_code") == F.col("ar.source_system_code")),
        "inner"
    )
    .where(
        (F.col("ip.source_system_code") == F.lit("CF")) &
        (F.coalesce(F.col("ip.deceased_ind"), F.lit("N")) == F.lit("N"))
    )
    .select(
        F.col("ar.business_date").alias("ods_business_dt"),
        F.col("ip.rcif_cust_nbr").cast("string").alias("rcif_number"),
        F.col("ar.arrangement_id").alias("arrangement_id"),
        F.col("ar.current_balance_amt").alias("balance"),
        F.col("ar.open_date").alias("open_date"),
    )
    .dropDuplicates(["ods_business_dt", "arrangement_id"])
)

# =========================
# 4) WRITE TABLES
# =========================
if DROP_AND_RECREATE:
    spark.sql(f"DROP TABLE IF EXISTS {WEALTH_FQN}")
    spark.sql(f"DROP TABLE IF EXISTS {DIGITAL_FQN}")
    spark.sql(f"DROP TABLE IF EXISTS {INVEST_FQN}")

wealth_tbl.write.mode("overwrite").saveAsTable(WEALTH_FQN)
digital_tbl.write.mode("overwrite").saveAsTable(DIGITAL_FQN)
investpath_tbl.write.mode("overwrite").saveAsTable(INVEST_FQN)

print("✅ Created 3 tables:")
print(f"  - {WEALTH_FQN}   (RCIF grain, expect ~270K distinct rcif)")
print(f"  - {DIGITAL_FQN}  (reltibn grain, expect ~3.4M digital active distinct reltibn)")
print(f"  - {INVEST_FQN}   (arrangement grain)")

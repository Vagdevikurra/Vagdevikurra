from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

# ==========================================================
# CONFIG
# ==========================================================
DB = "dm_ib_dev"
START_DATE = "2025-08-01"
END_DATE   = "2026-01-31"

AR_SOURCE_LIST = [
    'DA','SV','CC','MG','LS','TM','PC','LO','BW','CM','CS','EL','IC','MA','PF','PR','SD',
    'TR','BI','RN','IS_CT','IS_IT','PWM'
]

# ==========================================================
# Spark
# ==========================================================
def get_spark(app_name="wid2_exact_source_sql"):
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
# TABLES (no guardrails)
# ==========================================================
DBM  = spark.table(f"{DB}.digital_banking_master")  # dm_ib_dev.digital_banking_master
IP   = spark.table("eil.d_involved_party_h")
A2I  = spark.table("eil.d_arrangement_to_involved_party_relationship_h")
AR   = spark.table("eil.d_arrangement_h")
ADDR = spark.table("eil.d_involved_party_address_h")

# ==========================================================
# 0) Snapshot dates (GLOBAL vs WINDOW) - for debugging
# ==========================================================
global_snapshot_dt = DBM.select(F.max(F.to_date("ods_business_dt")).alias("dt")).first()["dt"]

window_snapshot_dt = (
    DBM.where(
        (F.to_date("ods_business_dt") >= F.lit(START_DATE)) &
        (F.to_date("ods_business_dt") <= F.lit(END_DATE))
    )
    .select(F.max(F.to_date("ods_business_dt")).alias("dt"))
    .first()["dt"]
)

print(f"GLOBAL max ods_business_dt: {global_snapshot_dt}")
print(f"WINDOW ({START_DATE}..{END_DATE}) max ods_business_dt: {window_snapshot_dt}")

# We will use WINDOW snapshot for comparability (as you requested)
snapshot_dt = window_snapshot_dt
print(f"✅ Using snapshot_dt = {snapshot_dt}")

# ==========================================================
# 1) Dig_Customer (exact: snapshot only, group by ibn + ods_business_dt)
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

# ==========================================================
# 1A) Digital Active IBN KPI (DBM-only)
#     This is the dashboard measure if it uses digital table only.
# ==========================================================
dig_active = (
    dig_customer
    .withColumn(
        "Digitally_Active_Flag",
        F.when(
            (F.datediff(F.col("ods_business_dt"), F.col("lst_login_mob")) <= 90) |
            (F.datediff(F.col("ods_business_dt"), F.col("lst_login_olb")) <= 90),
            F.lit("Digital Active")
        ).otherwise(F.lit("Non Digital Active"))
    )
)

print("A) DBM-only Digital Active IBN (distinct ibn where Digital Active):")
dig_active.where(F.col("Digitally_Active_Flag") == "Digital Active") \
    .selectExpr("count(distinct reltibn) as active_ibn_dbm") \
    .show(truncate=False)

# ==========================================================
# 2) rc CTE (exact join chain + filters)
# ==========================================================
ip_snapshot_dt = IP.select(F.max(F.to_date("business_date")).alias("dt")).first()["dt"]

rc = (
    IP.alias("ip")
    .join(
        A2I.alias("a2i"),
        (F.col("ip.business_date") == F.col("a2i.business_date")) &
        (F.col("ip.source_system_code") == F.col("a2i.source_system_code")) &
        (F.col("ip.involved_party_id") == F.col("a2i.involved_party_id")),
        "inner"
    )
    .join(
        AR.alias("ar"),
        (F.col("a2i.business_date") == F.col("ar.business_date")) &
        (F.col("a2i.arrangement_source_system_code") == F.col("ar.source_system_code")) &
        (F.col("a2i.arrangement_id") == F.col("ar.arrangement_id")) &
        (F.col("ar.source_system_code").isin(AR_SOURCE_LIST)),
        "inner"
    )
    .join(
        ADDR.alias("addr"),
        (F.col("ip.involved_party_id") == F.col("addr.involved_party_id")) &
        (F.col("ip.business_date") == F.col("addr.business_date")),
        "inner"
    )
    .where(F.to_date("ip.business_date") == F.lit(ip_snapshot_dt))
    .where(F.col("ip.source_system_code") == F.lit("CF"))
    .where(F.coalesce(F.col("ip.deceased_ind"), F.lit("N")) == F.lit("N"))
    # birth_date filter was commented out in your screenshot, so we do NOT enforce it
    .groupBy(
        F.col("ip.involved_party_id"),
        F.col("ip.cust_internet_banking_nbr"),
        F.col("ip.involved_party_tax_id_nbr"),
        F.col("ip.birth_date"),
        F.col("ip.involved_party_name"),
        F.col("addr.city_name"),
        F.col("addr.state_name"),
        F.col("addr.country_name")
    )
    .agg(
        F.max(F.col("ip.rcif_cust_nbr").cast("string")).alias("RCIF_NUMBER")
    )
    .select(
        F.col("RCIF_NUMBER"),
        F.col("involved_party_id"),
        F.col("cust_internet_banking_nbr"),
        F.col("involved_party_tax_id_nbr"),
        F.col("birth_date"),
        F.col("involved_party_name"),
        F.col("city_name"),
        F.col("state_name"),
        F.col("country_name"),
    )
)

# ==========================================================
# 3) RCIF_Dig (left join rc to Dig_Customer on cust_internet_banking_nbr = reltibn)
# ==========================================================
rc_norm = rc.withColumn("cust_ibn_norm", F.upper(F.trim(F.col("cust_internet_banking_nbr").cast("string"))))
dc_norm = dig_active.withColumn("reltibn_norm", F.upper(F.trim(F.col("reltibn").cast("string"))))

joined = rc_norm.join(dc_norm, rc_norm.cust_ibn_norm == dc_norm.reltibn_norm, "left")

# row_number() over(partition by RCIF_NUMBER order by cust_internet_banking_nbr)
w = Window.partitionBy("RCIF_NUMBER").orderBy(F.col("cust_internet_banking_nbr"))

wid2 = (
    joined
    .withColumn("PWRANK", F.row_number().over(w))
    .withColumn(
        "CUSTOMER_GENERATION",
        F.when((F.col("birth_date") >= F.lit("1900-01-01")) & (F.col("birth_date") <= F.lit("1924-12-31")), F.lit("GI Generation (1900-1924)"))
         .when((F.col("birth_date") >= F.lit("1925-01-01")) & (F.col("birth_date") <= F.lit("1945-12-31")), F.lit("Traditionalist (1925-1945)"))
         .when((F.col("birth_date") >= F.lit("1946-01-01")) & (F.col("birth_date") <= F.lit("1964-12-31")), F.lit("Baby Boomer (1946-1964)"))
         .when((F.col("birth_date") >= F.lit("1965-01-01")) & (F.col("birth_date") <= F.lit("1980-12-31")), F.lit("Gen X (1965-1980)"))
         .when((F.col("birth_date") >= F.lit("1981-01-01")) & (F.col("birth_date") <= F.lit("1996-12-31")), F.lit("Millennial (1981-1996)"))
         .when(F.col("birth_date") >= F.lit("1997-01-01"), F.lit("Centennial (1997-???)"))
         .otherwise(F.lit("Unknown"))
    )
    # Flags EXACT (same as your SQL)
    .withColumn(
        "Mobile_Active_Flag",
        F.when(F.datediff(F.col("ods_business_dt"), F.col("lst_login_mob")) <= 90, F.lit("Mobile Active"))
         .otherwise(F.lit("Non Mobile Active"))
    )
    .withColumn(
        "Mobile_Flag",
        F.when(F.col("lst_login_mob").isNull(), F.lit("Non Mobile User"))
         .otherwise(F.lit("Mobile User"))
    )
    .withColumn(
        "OLB_Active_Flag",
        F.when(F.datediff(F.col("ods_business_dt"), F.col("lst_login_olb")) <= 90, F.lit("OLB Active"))
         .otherwise(F.lit("Non OLB Active"))
    )
    .withColumn(
        "OLB_Flag",
        F.when(F.col("lst_login_olb").isNull(), F.lit("Non OLB User"))
         .otherwise(F.lit("OLB User"))
    )
    .withColumn(
        "Digitally_Active_Flag",
        F.when(
            (F.datediff(F.col("ods_business_dt"), F.col("lst_login_mob")) <= 90) |
            (F.datediff(F.col("ods_business_dt"), F.col("lst_login_olb")) <= 90),
            F.lit("Digital Active")
        ).otherwise(F.lit("Non Digital Active"))
    )
    .withColumn(
        "Digital_flag",
        F.when(F.col("reltibn").isNull(), F.lit("Non Digital User"))
         .otherwise(F.lit("Digital User"))
    )
    .select(
        F.col("RCIF_NUMBER").alias("rcif_number"),
        "PWRANK",
        F.col("reltibn").alias("reltibn"),
        "involved_party_id",
        "cust_internet_banking_nbr",
        "involved_party_tax_id_nbr",
        "involved_party_name",
        "city_name",
        "state_name",
        "country_name",
        "birth_date",
        "CUSTOMER_GENERATION",
        "ods_business_dt",
        "lst_login_olb",
        "lst_login_mob",
        "Mobile_Active_Flag",
        "Mobile_Flag",
        "OLB_Active_Flag",
        "OLB_Flag",
        "Digitally_Active_Flag",
        "Digital_flag"
    )
)

# ==========================================================
# Debug counts (this tells you EXACTLY where 3.27M is coming from)
# ==========================================================
print("B) wid2 Digital Active IBN (post RCIF join path):")
wid2.where(F.col("Digitally_Active_Flag") == "Digital Active") \
   .selectExpr("count(distinct reltibn) as active_ibn_wid2") \
   .show(truncate=False)

print("C) How many Digital Active IBNs are LOST because they do not map to rc.cust_internet_banking_nbr:")
active_ibn_dbm = dig_active.where(F.col("Digitally_Active_Flag") == "Digital Active") \
                           .select("reltibn").dropDuplicates()
mapped_ibn = rc_norm.select(F.col("cust_ibn_norm").alias("reltibn")).dropDuplicates()

active_ibn_dbm.join(mapped_ibn, "reltibn", "left_anti") \
    .selectExpr("count(*) as active_ibn_missing_in_rc") \
    .show(truncate=False)

# If you want to save:
# wid2.write.mode("overwrite").saveAsTable(f"{DB}.wid2")

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

# ==========================================================
# CONFIG
# ==========================================================
DB = "dm_ib_dev"

WEALTH_FQN  = f"{DB}.wealth_tbl_202507_202512"
DIGITAL_FQN = f"{DB}.digital_tbl_202507_202512"
INVEST_FQN  = f"{DB}.investpath_tbl_202507_202512"

DROP_AND_RECREATE = True

START_DATE = "2025-07-01"
END_DATE   = "2025-12-31"   # inclusive for filtering; we implement as <= END_DATE

def get_spark(app_name="wealth_digital_investpath_202507_202512"):
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

start_dt = F.to_date(F.lit(START_DATE))
end_dt   = F.to_date(F.lit(END_DATE))

# ==========================================================
# 1) DIGITAL TABLE  (grain: 1 row per RELTIBN across window)
# Columns (as per your list):
#   ods_business_dt, reltibn, rcif_customer_number,
#   mobile_active_flag, mobile_flag, olb_active_flag, olb_flag,
#   digitally_active_flag, digital_flag
# ==========================================================
dbm = (
    spark.table("dm_ib.digital_banking_master")
    .where((F.to_date("ods_business_dt") >= start_dt) & (F.to_date("ods_business_dt") <= end_dt))
)

digital_tbl = (
    dbm.groupBy("relt_ibn")
       .agg(
           F.max(F.to_date("ods_business_dt")).alias("ods_business_dt"),  # as-of within the window
           F.max("rcif_customer_nbr").cast("string").alias("rcif_customer_number"),
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
           F.when(
               F.col("lst_login_mob").isNotNull() &
               (F.datediff(F.col("ods_business_dt"), F.col("lst_login_mob")) <= 90),
               F.lit("Mobile Active")
           ).otherwise(F.lit("Non Mobile Active"))
       )
       .withColumn(
           "olb_active_flag",
           F.when(
               F.col("lst_login_olb").isNotNull() &
               (F.datediff(F.col("ods_business_dt"), F.col("lst_login_olb")) <= 90),
               F.lit("OLB Active")
           ).otherwise(F.lit("Non OLB Active"))
       )
       .withColumn(
           "digitally_active_flag",
           F.when(
               (F.col("mobile_active_flag") == "Mobile Active") |
               (F.col("olb_active_flag") == "OLB Active"),
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
           "rcif_customer_number",
           "mobile_active_flag",
           "mobile_flag",
           "olb_active_flag",
           "olb_flag",
           "digitally_active_flag",
           "digital_flag",
       )
)

# Sanity checks for Digital
digital_tbl.filter(F.col("digitally_active_flag") == "Digital Active") \
          .selectExpr("count(distinct reltibn) as digital_active_reltibn").show(truncate=False)

# ==========================================================
# 2) WEALTH TABLE (grain: 1 row per RCIF)
# Columns (as per your list):
#   ods_business_dt, accts_cnt, rcif_number, ip_id, customer_internet_banking_number,
#   city_name, business_group, division
#
# NOTE:
# - We define the "wealth population" from involved_party table within window
# - We pick the latest record per RCIF within the window (stable RCIF count)
# - Enrichment (accounts & segments) is LEFT JOIN so we never lose RCIFs
# ==========================================================
ip = (
    spark.table("eil.m_involved_party_h")
    .where((F.to_date("business_date") >= start_dt) & (F.to_date("business_date") <= end_dt))
    .where(F.coalesce(F.col("deceased_ind"), F.lit("N")) == "N")
    .select(
        F.to_date("business_date").alias("ods_business_dt"),
        F.col("rcif_cust_nbr").cast("string").alias("rcif_number"),
        F.col("involved_party_id").alias("ip_id"),
        F.col("cust_internet_banking_nbr").alias("customer_internet_banking_number"),
        F.col("private_client_code").alias("private_client_code"),
        F.col("private_client_trust_code").alias("private_client_trust_code"),
        F.col("source_system_code").alias("source_system_code"),
    )
)

# Latest row per RCIF inside the window
w_latest_rcif = Window.partitionBy("rcif_number").orderBy(F.col("ods_business_dt").desc())

ip_latest = (
    ip.withColumn("rn", F.row_number().over(w_latest_rcif))
      .where(F.col("rn") == 1)
      .drop("rn")
)

# Address -> city_name (LEFT join to not lose RCIFs)
addr = (
    spark.table("eil.m_involved_party_address_h")
    .where((F.to_date("business_date") >= start_dt) & (F.to_date("business_date") <= end_dt))
    .select(
        F.to_date("business_date").alias("addr_dt"),
        F.col("involved_party_id").alias("addr_ip_id"),
        F.col("city_name").alias("city_name")
    )
)

ip_latest = (
    ip_latest.alias("ip")
    .join(
        addr.alias("ad"),
        (F.col("ip.ip_id") == F.col("ad.addr_ip_id")) &
        (F.col("ip.ods_business_dt") == F.col("ad.addr_dt")),
        "left"
    )
    .drop("addr_dt", "addr_ip_id")
)

# Relationship table for accounts (LEFT)
a2i = (
    spark.table("eil.m_arrangement_to_involved_party_relationship_h")
    .where((F.to_date("business_date") >= start_dt) & (F.to_date("business_date") <= end_dt))
    .select(
        F.to_date("business_date").alias("rel_dt"),
        F.col("involved_party_id").alias("rel_ip_id"),
        F.col("source_system_code").alias("rel_source_system_code"),
        F.col("arrangement_id").alias("arrangement_id"),
    )
)

# Arrangement table for segment code (LEFT)
ar = (
    spark.table("eil.m_arrangement_h")
    .where((F.to_date("business_date") >= start_dt) & (F.to_date("business_date") <= end_dt))
    .select(
        F.to_date("business_date").alias("ar_dt"),
        F.col("source_system_code").alias("ar_source_system_code"),
        F.col("arrangement_id").alias("arrangement_id"),
        F.col("business_service_segment_type_code").alias("business_service_segment_type_code"),
    )
)

wealth_enriched = (
    ip_latest.alias("ip")
    .join(
        a2i.alias("a2i"),
        (F.col("ip.ip_id") == F.col("a2i.rel_ip_id")) &
        (F.col("ip.ods_business_dt") == F.col("a2i.rel_dt")) &
        (F.col("ip.source_system_code") == F.col("a2i.rel_source_system_code")),
        "left"
    )
    .join(
        ar.alias("ar"),
        (F.col("a2i.arrangement_id") == F.col("ar.arrangement_id")) &
        (F.col("ip.ods_business_dt") == F.col("ar.ar_dt")) &
        (F.col("ip.source_system_code") == F.col("ar.ar_source_system_code")),
        "left"
    )
)

wealth_tbl = (
    wealth_enriched
    .groupBy(
        F.col("ip.ods_business_dt").alias("ods_business_dt"),
        F.col("ip.rcif_number").alias("rcif_number"),
        F.col("ip.ip_id").alias("ip_id"),
        F.col("ip.customer_internet_banking_number").alias("customer_internet_banking_number"),
        F.col("city_name").alias("city_name"),
        F.col("ip.private_client_code").alias("private_client_code"),
        F.col("ip.private_client_trust_code").alias("private_client_trust_code"),
    )
    .agg(
        F.countDistinct(F.col("a2i.arrangement_id")).alias("accts_cnt"),
        F.max(F.col("ar.business_service_segment_type_code")).alias("any_segment_code"),
    )
    .withColumn(
        "business_group",
        F.when(F.col("private_client_code").isin("039","539","339"), F.lit("Private Wealth"))
         .when(F.col("private_client_trust_code").isin("239","739"), F.lit("Private Wealth"))
         .when(F.col("any_segment_code").isin("IS_CT","IS_IT"), F.lit("Institutional Services"))
         .when(F.col("any_segment_code").isin("REGIS_FC","REGIS"), F.lit("Investment Services"))
         .when(F.col("any_segment_code") == "PWM", F.lit("Private Wealth"))
         .otherwise(F.lit("Other"))
    )
    .withColumn("division", F.col("business_group"))
    .select(
        "ods_business_dt",
        "accts_cnt",
        "rcif_number",
        "ip_id",
        "customer_internet_banking_number",
        "city_name",
        "business_group",
        "division",
    )
)

# Sanity check for Wealth
wealth_tbl.selectExpr("count(*) as wealth_rows", "count(distinct rcif_number) as wealth_distinct_rcif").show(truncate=False)

# ==========================================================
# 3) INVESTPATH TABLE (grain: 1 row per ARRANGEMENT_ID within window)
# Columns (as per your list):
#   rcif_number, arrangement_id (and act_cnt alias), balance, open_date, ods_business_dt
# ==========================================================
# Build daily mapping from involved_party -> rcif for window
ip_by_day = (
    spark.table("eil.m_involved_party_h")
    .where((F.to_date("business_date") >= start_dt) & (F.to_date("business_date") <= end_dt))
    .where(F.coalesce(F.col("deceased_ind"), F.lit("N")) == "N")
    .select(
        F.to_date("business_date").alias("ods_business_dt"),
        F.col("involved_party_id").alias("ip_id"),
        F.col("source_system_code").alias("source_system_code"),
        F.col("rcif_cust_nbr").cast("string").alias("rcif_number"),
    )
)

a2i_by_day = (
    spark.table("eil.m_arrangement_to_involved_party_relationship_h")
    .where((F.to_date("business_date") >= start_dt) & (F.to_date("business_date") <= end_dt))
    .select(
        F.to_date("business_date").alias("ods_business_dt"),
        F.col("involved_party_id").alias("ip_id"),
        F.col("source_system_code").alias("source_system_code"),
        F.col("arrangement_id").alias("arrangement_id"),
    )
)

ar_by_day = (
    spark.table("eil.m_arrangement_h")
    .where((F.to_date("business_date") >= start_dt) & (F.to_date("business_date") <= end_dt))
    .select(
        F.to_date("business_date").alias("ods_business_dt"),
        F.col("source_system_code").alias("source_system_code"),
        F.col("arrangement_id").alias("arrangement_id"),
        F.col("current_balance_amt").alias("balance"),
        F.col("open_date").alias("open_date"),
    )
)

investpath_tbl = (
    a2i_by_day.alias("a2i")
    .join(
        ip_by_day.alias("ip"),
        (F.col("a2i.ip_id") == F.col("ip.ip_id")) &
        (F.col("a2i.ods_business_dt") == F.col("ip.ods_business_dt")) &
        (F.col("a2i.source_system_code") == F.col("ip.source_system_code")),
        "left"
    )
    .join(
        ar_by_day.alias("ar"),
        (F.col("a2i.arrangement_id") == F.col("ar.arrangement_id")) &
        (F.col("a2i.ods_business_dt") == F.col("ar.ods_business_dt")) &
        (F.col("a2i.source_system_code") == F.col("ar.source_system_code")),
        "left"
    )
    .select(
        F.col("a2i.ods_business_dt").alias("ods_business_dt"),
        F.col("ip.rcif_number").alias("rcif_number"),
        F.col("a2i.arrangement_id").alias("arrangement_id"),
        F.col("a2i.arrangement_id").alias("act_cnt"),   # alias as requested
        F.col("ar.balance").alias("balance"),
        F.col("ar.open_date").alias("open_date"),
    )
    .where(F.col("arrangement_id").isNotNull())
    .dropDuplicates(["ods_business_dt", "arrangement_id"])
)

# Sanity check for InvestPath
investpath_tbl.selectExpr(
    "count(*) as invest_rows",
    "count(distinct arrangement_id) as invest_distinct_arrangements",
    "count(distinct rcif_number) as invest_distinct_rcif"
).show(truncate=False)

# ==========================================================
# WRITE TABLES
# ==========================================================
if DROP_AND_RECREATE:
    spark.sql(f"DROP TABLE IF EXISTS {WEALTH_FQN}")
    spark.sql(f"DROP TABLE IF EXISTS {DIGITAL_FQN}")
    spark.sql(f"DROP TABLE IF EXISTS {INVEST_FQN}")

wealth_tbl.write.mode("overwrite").saveAsTable(WEALTH_FQN)
digital_tbl.write.mode("overwrite").saveAsTable(DIGITAL_FQN)
investpath_tbl.write.mode("overwrite").saveAsTable(INVEST_FQN)

print("✅ Created 3 tables:")
print(f"  - {WEALTH_FQN}")
print(f"  - {DIGITAL_FQN}")
print(f"  - {INVEST_FQN}")

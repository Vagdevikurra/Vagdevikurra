from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

# =========================
# CONFIG / PARAMS
# =========================
DB = "dm_ib_dev"

WEALTH_FQN  = f"{DB}.wealth_tbl_6m"
DIGITAL_FQN = f"{DB}.digital_tbl_6m"
INVEST_FQN  = f"{DB}.investpath_tbl_6m"

DROP_AND_RECREATE = True

def get_spark(app_name="wealth_digital_invest_3tables_6m"):
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

# =========================
# DATE WINDOW (last 6 full months)
# start = first day of month 6 months ago
# end   = first day of current month (exclusive)
# =========================
this_month = F.trunc(F.current_date(), "MM")
start_dt = F.add_months(this_month, -6)
end_dt   = this_month

# =========================
# 1) DIGITAL TABLE (reltibn grain, 1 row per reltibn across window)
# This supports your DAX:
#   CALCULATE(DISTINCTCOUNT(Digital[reltibn]), Digital[digitally_active_flag]="Digital Active")
# =========================
dbm = (
    spark.table("dm_ib.digital_banking_master")
    .where((F.col("ods_business_dt") >= start_dt) & (F.col("ods_business_dt") < end_dt))
)

digital_tbl = (
    dbm.groupBy("relt_ibn")
       .agg(
           F.max("ods_business_dt").alias("ods_business_dt"),              # as-of date per reltibn
           F.max("rcif_customer_nbr").cast("string").alias("rcif_customer_number"),
           F.max("olb_last_login_date").alias("lst_login_olb"),
           F.max("mob_last_login_date").alias("lst_login_mob"),
       )
       .withColumn(
           "mobile_flag",
           F.when(F.col("lst_login_mob").isNull(), F.lit("Non Mobile User")).otherwise(F.lit("Mobile User"))
       )
       .withColumn(
           "olb_flag",
           F.when(F.col("lst_login_olb").isNull(), F.lit("Non OLB User")).otherwise(F.lit("OLB User"))
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
           F.when(F.col("relt_ibn").isNull(), F.lit("Non Digital User")).otherwise(F.lit("Digital User"))
       )
       .select(
           F.col("ods_business_dt"),
           F.col("relt_ibn").alias("reltibn"),
           "rcif_customer_number",
           "mobile_active_flag", "mobile_flag",
           "olb_active_flag", "olb_flag",
           "digitally_active_flag", "digital_flag",
       )
)

# sanity check: should be near your expected 3,428,446
digital_tbl.filter(F.col("digitally_active_flag") == "Digital Active") \
          .selectExpr("count(distinct reltibn) as digital_active_reltibn").show(truncate=False)

# =========================
# 2) WEALTH TABLE (RCIF grain: latest row per RCIF within window)
# Target: distinct rcif_number ~ 269,148 (your expectation)
# IMPORTANT: Use eil.m_involved_party_h (your screenshots heavily use m_)
# =========================
ip = (
    spark.table("eil.m_involved_party_h")
    .where((F.to_date("business_date") >= start_dt) & (F.to_date("business_date") < end_dt))
    .where(F.coalesce(F.col("deceased_ind"), F.lit("N")) == "N")
)

# pick latest business_date row per rcif within the 6-month window (keeps RCIF count stable)
w_rcif = Window.partitionBy(F.col("rcif_cust_nbr").cast("string")).orderBy(F.to_date("business_date").desc())

ip_latest = (
    ip.select(
        F.to_date("business_date").alias("ods_business_dt"),
        F.col("rcif_cust_nbr").cast("string").alias("rcif_number"),
        F.col("involved_party_id").alias("ip_id"),
        F.col("cust_internet_banking_nbr").alias("customer_internet_banking_number"),
        F.col("private_client_code"),
        F.col("private_client_trust_code"),
        F.col("source_system_code"),
    )
    .withColumn("rn", F.row_number().over(w_rcif))
    .where(F.col("rn") == 1)
    .drop("rn")
)

# Enrichment join (LEFT) so we do NOT lose RCIFs:
# Use relationship + arrangement on same date/source where available, but never drop RCIFs.
a2i = (
    spark.table("eil.m_arrangement_to_involved_party_relationship_h")
    .where((F.to_date("business_date") >= start_dt) & (F.to_date("business_date") < end_dt))
    .select(
        F.to_date("business_date").alias("ods_business_dt"),
        "involved_party_id",
        "source_system_code",
        "arrangement_id"
    )
)

ar = (
    spark.table("eil.m_arrangement_h")
    .where((F.to_date("business_date") >= start_dt) & (F.to_date("business_date") < end_dt))
    .select(
        F.to_date("business_date").alias("ods_business_dt"),
        "source_system_code",
        "arrangement_id",
        "business_service_segment_type_code"
    )
)

wealth_enriched = (
    ip_latest.alias("ip")
    .join(
        a2i.alias("a2i"),
        (F.col("ip.ip_id") == F.col("a2i.involved_party_id")) &
        (F.col("ip.ods_business_dt") == F.col("a2i.ods_business_dt")) &
        (F.col("ip.source_system_code") == F.col("a2i.source_system_code")),
        "left"
    )
    .join(
        ar.alias("ar"),
        (F.col("a2i.arrangement_id") == F.col("ar.arrangement_id")) &
        (F.col("a2i.ods_business_dt") == F.col("ar.ods_business_dt")) &
        (F.col("a2i.source_system_code") == F.col("ar.source_system_code")),
        "left"
    )
)

wealth_tbl = (
    wealth_enriched
    .groupBy("ip.ods_business_dt", "ip.rcif_number", "ip.ip_id", "ip.customer_internet_banking_number",
             "ip.private_client_code", "ip.private_client_trust_code")
    .agg(
        F.countDistinct("ar.arrangement_id").alias("accts_cnt"),
        F.max("ar.business_service_segment_type_code").alias("any_segment_code")  # simple enrichment
    )
    .withColumn(
        "business_group",
        F.when(F.col("private_client_code").isin("039", "539", "339"), F.lit("Private Wealth"))
         .when(F.col("private_client_trust_code").isin("239", "739"), F.lit("Private Wealth"))
         .when(F.col("any_segment_code").isin("IS_CT", "IS_IT"), F.lit("Institutional Services"))
         .when(F.col("any_segment_code").isin("REGIS_FC", "REGIS"), F.lit("Investment Services"))
         .when(F.col("any_segment_code") == "PWM", F.lit("Private Wealth"))
         .otherwise(F.lit("Other"))
    )
    .withColumn("division", F.col("business_group"))  # replace if you have separate division logic
    .select(
        "ods_business_dt",
        "rcif_number",
        "ip_id",
        "customer_internet_banking_number",
        "accts_cnt",
        "business_group",
        "division"
    )
)

# sanity check: should be near your expected 269,148
wealth_tbl.selectExpr("count(distinct rcif_number) as wealth_rcif").show(truncate=False)

# =========================
# 3) INVESTPATH TABLE (arrangement grain; NEVER 0 unless source tables are empty)
# Build from a2i within window, attach rcif via ip within same date/source (LEFT to avoid drops)
# =========================
ip_rcif_by_day = (
    ip.select(
        F.to_date("business_date").alias("ods_business_dt"),
        "involved_party_id",
        "source_system_code",
        F.col("rcif_cust_nbr").cast("string").alias("rcif_number")
    )
)

invest_tbl = (
    a2i.alias("a2i")
    .join(
        ip_rcif_by_day.alias("ipd"),
        (F.col("a2i.involved_party_id") == F.col("ipd.involved_party_id")) &
        (F.col("a2i.ods_business_dt") == F.col("ipd.ods_business_dt")) &
        (F.col("a2i.source_system_code") == F.col("ipd.source_system_code")),
        "left"
    )
    .join(
        spark.table("eil.m_arrangement_h").alias("ar"),
        (F.col("a2i.arrangement_id") == F.col("ar.arrangement_id")) &
        (F.to_date(F.col("ar.business_date")) == F.col("a2i.ods_business_dt")) &
        (F.col("a2i.source_system_code") == F.col("ar.source_system_code")),
        "left"
    )
    .select(
        F.col("a2i.ods_business_dt").alias("ods_business_dt"),
        F.col("ipd.rcif_number").alias("rcif_number"),
        F.col("a2i.arrangement_id").alias("arrangement_id"),
        F.col("ar.current_balance_amt").alias("balance"),
        F.col("ar.open_date").alias("open_date"),
    )
    .where(F.col("arrangement_id").isNotNull())
    .dropDuplicates(["ods_business_dt", "arrangement_id"])
)

invest_tbl.selectExpr("count(*) as invest_rows", "count(distinct arrangement_id) as distinct_arrangement").show(truncate=False)

# =========================
# WRITE TABLES
# =========================
if DROP_AND_RECREATE:
    spark.sql(f"DROP TABLE IF EXISTS {WEALTH_FQN}")
    spark.sql(f"DROP TABLE IF EXISTS {DIGITAL_FQN}")
    spark.sql(f"DROP TABLE IF EXISTS {INVEST_FQN}")

wealth_tbl.write.mode("overwrite").saveAsTable(WEALTH_FQN)
digital_tbl.write.mode("overwrite").saveAsTable(DIGITAL_FQN)
invest_tbl.write.mode("overwrite").saveAsTable(INVEST_FQN)

print("✅ Created 3 tables:")
print(f"  - {WEALTH_FQN}   (RCIF grain, expect ~269k distinct rcif)")
print(f"  - {DIGITAL_FQN}  (reltibn grain, expect ~3.428M digital active distinct reltibn)")
print(f"  - {INVEST_FQN}   (arrangement grain)")

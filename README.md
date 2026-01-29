from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

# ==========================================================
# CONFIG
# ==========================================================
DB = "dm_ib_dev"

CUST_FQN   = f"{DB}.customer_universal_2t_202507_202512"
ACCT_FQN   = f"{DB}.investpath_accounts_2t_202507_202512"
DIGIBN_FQN = f"{DB}.digital_ibn_202507_202512"

DROP_AND_RECREATE = True
START_DATE = "2025-07-01"
END_DATE   = "2025-12-31"   # inclusive

def get_spark(app_name="wealth_digital_ibn_invest_202507_202512"):
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
# 1) DIGITAL_IBN table (grain: 1 row per IBN across window)
#    This is what you need for:
#      DISTINCTCOUNT(Digital_IBN[ibn]) with digitally_active_flag="Digital Active"
# ==========================================================
dbm = (
    spark.table("dm_ib.digital_banking_master")
    .where((F.to_date("ods_business_dt") >= start_dt) & (F.to_date("ods_business_dt") <= end_dt))
    .select(
        F.to_date("ods_business_dt").alias("ods_business_dt"),
        F.col("ibn").alias("ibn"),
        F.col("rcif_customer_nbr").cast("string").alias("rcif_customer_number"),
        F.col("olb_last_login_date").alias("lst_login_olb"),
        F.col("mob_last_login_date").alias("lst_login_mob"),
    )
    .where(F.col("ibn").isNotNull())
)

digital_ibn = (
    dbm.groupBy("ibn")
       .agg(
           F.max("ods_business_dt").alias("ods_business_dt"),
           F.max("rcif_customer_number").alias("rcif_customer_number"),
           F.max("lst_login_olb").alias("lst_login_olb"),
           F.max("lst_login_mob").alias("lst_login_mob"),
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
           F.lit("Digital User")
       )
       .select(
           "ods_business_dt",
           "ibn",
           "rcif_customer_number",
           "mobile_active_flag",
           "mobile_flag",
           "olb_active_flag",
           "olb_flag",
           "digitally_active_flag",
           "digital_flag"
       )
)

print("=== DIGITAL_IBN CHECK (target ~3.4M) ===")
digital_ibn.filter(F.col("digitally_active_flag")=="Digital Active") \
          .selectExpr("count(distinct ibn) as digital_active_ibn").show(truncate=False)

# ==========================================================
# 2) DIGITAL aggregated to RCIF (for RCIF-level flags in customer table)
#    Grain: 1 row per rcif_customer_number
# ==========================================================
dbm_flags = (
    dbm.withColumn("mobile_user_ind",  F.when(F.col("lst_login_mob").isNull(), F.lit(0)).otherwise(F.lit(1)))
       .withColumn("olb_user_ind",     F.when(F.col("lst_login_olb").isNull(), F.lit(0)).otherwise(F.lit(1)))
       .withColumn("mobile_active_ind",
                   F.when(F.col("lst_login_mob").isNotNull() &
                          (F.datediff(F.col("ods_business_dt"), F.col("lst_login_mob")) <= 90), F.lit(1)).otherwise(F.lit(0)))
       .withColumn("olb_active_ind",
                   F.when(F.col("lst_login_olb").isNotNull() &
                          (F.datediff(F.col("ods_business_dt"), F.col("lst_login_olb")) <= 90), F.lit(1)).otherwise(F.lit(0)))
)

digital_rcif = (
    dbm_flags.groupBy("rcif_customer_number")
             .agg(
                 F.max("ods_business_dt").alias("ods_business_dt"),
                 F.max("mobile_user_ind").alias("mobile_user_ind"),
                 F.max("olb_user_ind").alias("olb_user_ind"),
                 F.max("mobile_active_ind").alias("mobile_active_ind"),
                 F.max("olb_active_ind").alias("olb_active_ind"),
                 F.countDistinct("ibn").alias("ibn_cnt"),
             )
             .withColumn("mobile_flag", F.when(F.col("mobile_user_ind")==1, "Mobile User").otherwise("Non Mobile User"))
             .withColumn("olb_flag",    F.when(F.col("olb_user_ind")==1, "OLB User").otherwise("Non OLB User"))
             .withColumn("mobile_active_flag", F.when(F.col("mobile_active_ind")==1, "Mobile Active").otherwise("Non Mobile Active"))
             .withColumn("olb_active_flag",    F.when(F.col("olb_active_ind")==1, "OLB Active").otherwise("Non OLB Active"))
             .withColumn("digitally_active_flag",
                         F.when((F.col("mobile_active_ind")==1) | (F.col("olb_active_ind")==1),
                                "Digital Active").otherwise("Non Digital Active"))
             .withColumn("digital_flag",
                         F.when(F.col("ibn_cnt")>0, "Digital User").otherwise("Non Digital User"))
             .select(
                 "ods_business_dt",
                 "rcif_customer_number",
                 "mobile_active_flag",
                 "mobile_flag",
                 "olb_active_flag",
                 "olb_flag",
                 "digitally_active_flag",
                 "digital_flag",
                 "ibn_cnt"
             )
)

# ==========================================================
# 3) WEALTH RCIF base (1 row per RCIF) + enrichment
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
        F.col("private_client_code"),
        F.col("private_client_trust_code"),
        F.col("source_system_code"),
    )
)

w_latest = Window.partitionBy("rcif_number").orderBy(F.col("ods_business_dt").desc())
ip_latest = ip.withColumn("rn", F.row_number().over(w_latest)).where("rn=1").drop("rn")

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
    .join(addr.alias("ad"),
          (F.col("ip.ip_id")==F.col("ad.addr_ip_id")) & (F.col("ip.ods_business_dt")==F.col("ad.addr_dt")),
          "left")
    .drop("addr_dt","addr_ip_id")
)

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
    .join(a2i.alias("a2i"),
          (F.col("ip.ip_id")==F.col("a2i.rel_ip_id")) &
          (F.col("ip.ods_business_dt")==F.col("a2i.rel_dt")) &
          (F.col("ip.source_system_code")==F.col("a2i.rel_source_system_code")),
          "left")
    .join(ar.alias("ar"),
          (F.col("a2i.arrangement_id")==F.col("ar.arrangement_id")) &
          (F.col("ip.ods_business_dt")==F.col("ar.ar_dt")) &
          (F.col("ip.source_system_code")==F.col("ar.ar_source_system_code")),
          "left")
)

wealth_rcif = (
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
        "division"
    )
)

# ==========================================================
# 4) CUSTOMER_UNIVERSAL_2T (RCIF grain) = Wealth + digital flags (RCIF-level)
# ==========================================================
customer_universal_2t = (
    wealth_rcif.alias("w")
    .join(digital_rcif.alias("d"),
          F.col("w.rcif_number") == F.col("d.rcif_customer_number"),
          "left")
    .select(
        # Wealth/customer
        F.col("w.ods_business_dt").alias("ods_business_dt"),
        F.col("w.accts_cnt").alias("accts_cnt"),
        F.col("w.rcif_number").alias("rcif_number"),
        F.col("w.ip_id").alias("ip_id"),
        F.col("w.customer_internet_banking_number").alias("customer_internet_banking_number"),
        F.col("w.city_name").alias("city_name"),
        F.col("w.business_group").alias("business_group"),
        F.col("w.division").alias("division"),

        # Digital RCIF + flags
        F.col("d.rcif_customer_number").alias("rcif_customer_number"),
        F.col("d.mobile_active_flag").alias("mobile_active_flag"),
        F.col("d.mobile_flag").alias("mobile_flag"),
        F.col("d.olb_active_flag").alias("olb_active_flag"),
        F.col("d.olb_flag").alias("olb_flag"),
        F.col("d.digitally_active_flag").alias("digitally_active_flag"),
        F.col("d.digital_flag").alias("digital_flag"),
        F.col("d.ibn_cnt").alias("ibn_cnt"),
    )
)

print("=== CUSTOMER CHECK ===")
customer_universal_2t.selectExpr(
    "count(*) as rows",
    "count(distinct rcif_number) as distinct_wealth_rcif"
).show(truncate=False)

# ==========================================================
# 5) INVESTPATH_ACCOUNTS_2T (arrangement grain)
# ==========================================================
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

investpath_accounts_2t = (
    a2i_by_day.alias("a2i")
    .join(ip_by_day.alias("ip"),
          (F.col("a2i.ip_id")==F.col("ip.ip_id")) &
          (F.col("a2i.ods_business_dt")==F.col("ip.ods_business_dt")) &
          (F.col("a2i.source_system_code")==F.col("ip.source_system_code")),
          "left")
    .join(ar_by_day.alias("ar"),
          (F.col("a2i.arrangement_id")==F.col("ar.arrangement_id")) &
          (F.col("a2i.ods_business_dt")==F.col("ar.ods_business_dt")) &
          (F.col("a2i.source_system_code")==F.col("ar.source_system_code")),
          "left")
    .select(
        F.col("a2i.ods_business_dt").alias("ods_business_dt"),
        F.col("ip.rcif_number").alias("rcif_number"),
        F.col("a2i.arrangement_id").alias("arrangement_id"),
        F.col("a2i.arrangement_id").alias("act_cnt"),
        F.col("ar.balance").alias("balance"),
        F.col("ar.open_date").alias("open_date"),
    )
    .where(F.col("arrangement_id").isNotNull())
    .dropDuplicates(["ods_business_dt", "arrangement_id"])
)

print("=== ACCOUNTS CHECK ===")
investpath_accounts_2t.selectExpr(
    "count(*) as rows",
    "count(distinct arrangement_id) as distinct_arrangements",
    "count(distinct rcif_number) as distinct_rcif"
).show(truncate=False)

# ==========================================================
# WRITE TABLES
# ==========================================================
if DROP_AND_RECREATE:
    spark.sql(f"DROP TABLE IF EXISTS {CUST_FQN}")
    spark.sql(f"DROP TABLE IF EXISTS {ACCT_FQN}")
    spark.sql(f"DROP TABLE IF EXISTS {DIGIBN_FQN}")

customer_universal_2t.write.mode("overwrite").saveAsTable(CUST_FQN)
investpath_accounts_2t.write.mode("overwrite").saveAsTable(ACCT_FQN)
digital_ibn.write.mode("overwrite").saveAsTable(DIGIBN_FQN)

print("✅ Created 3 tables:")
print(f"  - {CUST_FQN}   (RCIF grain)")
print(f"  - {ACCT_FQN}   (arrangement grain)")
print(f"  - {DIGIBN_FQN} (IBN grain for 3.4M KPI)")

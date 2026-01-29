from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

# =========================
# CONFIG
# =========================
DB = "dm_ib_dev"

CUST_FQN   = f"{DB}.customer_universal_2t_202507_202512"
ACCT_FQN   = f"{DB}.investpath_accounts_2t_202507_202512"
DIGIBN_FQN = f"{DB}.digital_ibn_202507_202512"

DROP_AND_RECREATE = True
START_DATE = "2025-07-01"
END_DATE   = "2025-12-31"  # inclusive

# *** INVESTPATH SUBSET FILTER ***
# Update this list to the segment/type codes that define InvestPath accounts in your data.
# If you leave it too broad you WILL get millions of accounts.
INVEST_SEGMENT_CODES = ["REGIS", "REGIS_FC"]  # <-- EDIT if your InvestPath definition differs

def get_spark(app_name="wealth_digital_investpath_fixed_202507_202512"):
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

def read_first_existing_table(*candidates: str):
    last_err = None
    for t in candidates:
        try:
            return spark.table(t)
        except Exception as e:
            last_err = e
    raise last_err

# =========================
# 1) DIGITAL_IBN (1 row per IBN) — for 3.4M KPI
# =========================
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

# compute flags per row, then "any-day active" rollup to ibn
dbm_row = (
    dbm
    .withColumn("mobile_user_ind", F.when(F.col("lst_login_mob").isNotNull(), 1).otherwise(0))
    .withColumn("olb_user_ind",    F.when(F.col("lst_login_olb").isNotNull(), 1).otherwise(0))
    .withColumn(
        "mobile_active_ind",
        F.when(F.col("lst_login_mob").isNotNull() & (F.datediff("ods_business_dt","lst_login_mob") <= 90), 1).otherwise(0)
    )
    .withColumn(
        "olb_active_ind",
        F.when(F.col("lst_login_olb").isNotNull() & (F.datediff("ods_business_dt","lst_login_olb") <= 90), 1).otherwise(0)
    )
    .withColumn(
        "digitally_active_ind",
        F.when((F.col("mobile_active_ind")==1) | (F.col("olb_active_ind")==1), 1).otherwise(0)
    )
)

digital_ibn = (
    dbm_row
    .groupBy("ibn")
    .agg(
        F.max("ods_business_dt").alias("ods_business_dt"),
        F.max("rcif_customer_number").alias("rcif_customer_number"),
        F.max("mobile_user_ind").alias("mobile_user_ind"),
        F.max("olb_user_ind").alias("olb_user_ind"),
        F.max("mobile_active_ind").alias("mobile_active_ind"),
        F.max("olb_active_ind").alias("olb_active_ind"),
        F.max("digitally_active_ind").alias("digitally_active_ind"),
    )
    .withColumn("mobile_flag", F.when(F.col("mobile_user_ind")==1, "Mobile User").otherwise("Non Mobile User"))
    .withColumn("olb_flag",    F.when(F.col("olb_user_ind")==1, "OLB User").otherwise("Non OLB User"))
    .withColumn("mobile_active_flag", F.when(F.col("mobile_active_ind")==1, "Mobile Active").otherwise("Non Mobile Active"))
    .withColumn("olb_active_flag",    F.when(F.col("olb_active_ind")==1, "OLB Active").otherwise("Non OLB Active"))
    .withColumn("digitally_active_flag", F.when(F.col("digitally_active_ind")==1, "Digital Active").otherwise("Non Digital Active"))
    .withColumn("digital_flag", F.lit("Digital User"))
    .select(
        "ods_business_dt","ibn","rcif_customer_number",
        "mobile_active_flag","mobile_flag","olb_active_flag","olb_flag",
        "digitally_active_flag","digital_flag"
    )
)

# =========================
# 2) ACCOUNTS (InvestPath) — 1 row per arrangement_id (latest in window)
#     This is what fixes 210M → sane.
# =========================
ar_raw = (
    read_first_existing_table("eil.m_arrangement_h", "eil.d_arrangement_h")
    .where((F.to_date("business_date") >= start_dt) & (F.to_date("business_date") <= end_dt))
    .select(
        F.to_date("business_date").alias("ods_business_dt"),
        F.col("source_system_code").alias("source_system_code"),
        F.col("arrangement_id").alias("arrangement_id"),
        F.col("current_balance_amt").alias("balance"),
        F.col("open_date").alias("open_date"),
        F.col("business_service_segment_type_code").alias("segment_code"),
    )
    .where(F.col("arrangement_id").isNotNull())
)

# Filter to InvestPath subset (otherwise you'll always have millions)
ar_raw = ar_raw.where(F.col("segment_code").isin(INVEST_SEGMENT_CODES))

# Latest row per arrangement_id in the window
w_ar = Window.partitionBy("arrangement_id").orderBy(F.col("ods_business_dt").desc())
ar_latest = (
    ar_raw.withColumn("rn", F.row_number().over(w_ar))
          .where("rn=1")
          .drop("rn")
)

# Relationship table: pick latest mapping per arrangement_id to avoid many-party explosion
a2i_raw = (
    read_first_existing_table("eil.m_arrangement_to_involved_party_relationship_h",
                              "eil.d_arrangement_to_involved_party_relationship_h")
    .where((F.to_date("business_date") >= start_dt) & (F.to_date("business_date") <= end_dt))
    .select(
        F.to_date("business_date").alias("rel_dt"),
        F.col("source_system_code").alias("source_system_code"),
        F.col("arrangement_id").alias("arrangement_id"),
        F.col("involved_party_id").alias("ip_id"),
    )
    .where(F.col("arrangement_id").isNotNull())
)

# Latest relationship row per arrangement_id
w_rel = Window.partitionBy("arrangement_id").orderBy(F.col("rel_dt").desc())
a2i_latest = (
    a2i_raw.withColumn("rn", F.row_number().over(w_rel))
           .where("rn=1")
           .drop("rn")
)

# IP -> RCIF mapping (latest per involved_party_id)
ip_raw = (
    read_first_existing_table("eil.m_involved_party_h", "eil.d_involved_party_h")
    .where((F.to_date("business_date") >= start_dt) & (F.to_date("business_date") <= end_dt))
    .where(F.coalesce(F.col("deceased_ind"), F.lit("N")) == "N")
    .select(
        F.to_date("business_date").alias("ip_dt"),
        F.col("involved_party_id").alias("ip_id"),
        F.col("rcif_cust_nbr").cast("string").alias("rcif_number"),
    )
    .where(F.col("rcif_number").isNotNull())
)

w_ip = Window.partitionBy("ip_id").orderBy(F.col("ip_dt").desc())
ip_latest = (
    ip_raw.withColumn("rn", F.row_number().over(w_ip))
          .where("rn=1")
          .drop("rn")
)

# Final Accounts table (1 row per arrangement_id)
investpath_accounts_2t = (
    ar_latest.alias("ar")
    .join(a2i_latest.alias("a2i"), "arrangement_id", "left")
    .join(ip_latest.alias("ip"), F.col("a2i.ip_id") == F.col("ip.ip_id"), "left")
    .select(
        F.col("ar.ods_business_dt").alias("ods_business_dt"),
        F.col("ip.rcif_number").alias("rcif_number"),
        F.col("ar.arrangement_id").alias("arrangement_id"),
        F.col("ar.arrangement_id").alias("act_cnt"),
        F.col("ar.balance").alias("balance"),
        F.col("ar.open_date").alias("open_date"),
    )
)

# =========================
# 3) CUSTOMER_UNIVERSAL_2T — STRICT 1 row per RCIF
# =========================
ip_cust_raw = (
    read_first_existing_table("eil.m_involved_party_h", "eil.d_involved_party_h")
    .where((F.to_date("business_date") >= start_dt) & (F.to_date("business_date") <= end_dt))
    .where(F.coalesce(F.col("deceased_ind"), F.lit("N")) == "N")
    .select(
        F.to_date("business_date").alias("ods_business_dt"),
        F.col("rcif_cust_nbr").cast("string").alias("rcif_number"),
        F.col("involved_party_id").alias("ip_id"),
        F.col("cust_internet_banking_nbr").alias("customer_internet_banking_number"),
        F.col("private_client_code"),
        F.col("private_client_trust_code"),
    )
)

# Latest row per RCIF (hard guarantee)
w_rcif = Window.partitionBy("rcif_number").orderBy(F.col("ods_business_dt").desc())
ip_cust = (
    ip_cust_raw.withColumn("rn", F.row_number().over(w_rcif))
               .where("rn=1")
               .drop("rn")
)

# Address (try both m_ and d_)
addr_tbl = read_first_existing_table("eil.m_involved_party_address_h", "eil.d_involved_party_address_h")
addr = (
    addr_tbl
    .where((F.to_date("business_date") >= start_dt) & (F.to_date("business_date") <= end_dt))
    .select(
        F.to_date("business_date").alias("addr_dt"),
        F.col("involved_party_id").alias("ip_id"),
        F.col("city_name").alias("city_name")
    )
)

# Latest address per involved_party_id (avoid duplicates)
w_addr = Window.partitionBy("ip_id").orderBy(F.col("addr_dt").desc())
addr_latest = (
    addr.withColumn("rn", F.row_number().over(w_addr))
        .where("rn=1")
        .drop("rn")
)

# accts_cnt from our *deduped* accounts table (count arrangements per rcif)
accts_cnt = (
    investpath_accounts_2t
    .groupBy("rcif_number")
    .agg(F.countDistinct("arrangement_id").alias("accts_cnt"))
)

# Digital flags at RCIF-level (from Digital_IBN)
digital_rcif = (
    digital_ibn.groupBy("rcif_customer_number")
               .agg(
                   F.max("mobile_active_flag").alias("mobile_active_flag"),
                   F.max("mobile_flag").alias("mobile_flag"),
                   F.max("olb_active_flag").alias("olb_active_flag"),
                   F.max("olb_flag").alias("olb_flag"),
                   F.max("digitally_active_flag").alias("digitally_active_flag"),
                   F.max("digital_flag").alias("digital_flag"),
                   F.countDistinct("ibn").alias("ibn_cnt")
               )
)

customer_universal_2t = (
    ip_cust.alias("c")
    .join(addr_latest.alias("a"), F.col("c.ip_id") == F.col("a.ip_id"), "left")
    .join(accts_cnt.alias("ac"),  "rcif_number", "left")
    .join(digital_rcif.alias("d"), F.col("c.rcif_number") == F.col("d.rcif_customer_number"), "left")
    .withColumn(
        "business_group",
        F.when(F.col("private_client_code").isin("039","539","339"), F.lit("Private Wealth"))
         .when(F.col("private_client_trust_code").isin("239","739"), F.lit("Private Wealth"))
         .otherwise(F.lit("Other"))
    )
    .withColumn("division", F.col("business_group"))
    .select(
        # Customer / Wealth columns you listed
        F.col("c.ods_business_dt").alias("ods_business_dt"),
        F.coalesce(F.col("ac.accts_cnt"), F.lit(0)).alias("accts_cnt"),
        F.col("c.rcif_number").alias("rcif_number"),
        F.col("c.ip_id").alias("ip_id"),
        F.col("c.customer_internet_banking_number").alias("customer_internet_banking_number"),
        F.col("a.city_name").alias("city_name"),
        F.col("business_group").alias("business_group"),
        F.col("division").alias("division"),

        # Digital columns (RCIF-level)
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

# =========================
# SANITY CHECKS (these should now be sane)
# =========================
print("=== DIGITAL ACTIVE IBN (target ~3.428M) ===")
digital_ibn.filter(F.col("digitally_active_flag")=="Digital Active") \
          .selectExpr("count(distinct ibn) as digital_active_ibn").show(truncate=False)

print("=== CUSTOMER TABLE (should be ~270k rows and same distinct RCIF) ===")
customer_universal_2t.selectExpr(
    "count(*) as rows",
    "count(distinct rcif_number) as distinct_rcif"
).show(truncate=False)

print("=== ACCOUNTS TABLE (should be reasonable; NOT 210M) ===")
investpath_accounts_2t.selectExpr(
    "count(*) as rows",
    "count(distinct arrangement_id) as distinct_arrangement",
    "count(distinct rcif_number) as distinct_rcif"
).show(truncate=False)

# =========================
# WRITE TABLES
# =========================
if DROP_AND_RECREATE:
    spark.sql(f"DROP TABLE IF EXISTS {CUST_FQN}")
    spark.sql(f"DROP TABLE IF EXISTS {ACCT_FQN}")
    spark.sql(f"DROP TABLE IF EXISTS {DIGIBN_FQN}")

customer_universal_2t.write.mode("overwrite").saveAsTable(CUST_FQN)
investpath_accounts_2t.write.mode("overwrite").saveAsTable(ACCT_FQN)
digital_ibn.write.mode("overwrite").saveAsTable(DIGIBN_FQN)

print("✅ Created tables:")
print(f"  - {CUST_FQN}   (RCIF grain, 1 row per rcif_number)")
print(f"  - {ACCT_FQN}   (arrangement grain, 1 row per arrangement_id)")
print(f"  - {DIGIBN_FQN} (IBN grain for Digital Active KPI)")

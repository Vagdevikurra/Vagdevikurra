from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

# --------------------------
# CONFIG
# --------------------------
DB = "dm_ib_dev"
START_DATE = "2025-07-01"
END_DATE   = "2025-12-31"  # inclusive
DROP_AND_RECREATE = True

WEALTH_FQN = f"{DB}.wealth_rcif_202507_202512"
DIG_FQN    = f"{DB}.digital_ibn_202507_202512"
INV_FQN    = f"{DB}.investpath_202507_202512"

# Wealth (PW1) filters
AR_SOURCE_SYSTEM_LIST = ['BI','RN','TR','DA','SV','CC','LS','MG','TM','PC','LO','BW','CS','IC','MA','PF','PR','SD','CM','EL']
AR_CLOSED_ONLY = "N"

# InvestPath (INV) filters
INV_ACCOUNT_TYPE_CODE = "IP"
INV_AR_SOURCE_SYSTEM  = "RN"
INV_CLOSED_ONLY       = "N"

def get_spark(app_name="final_3tables_202507_202512"):
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

def T(*candidates):
    last = None
    for c in candidates:
        try:
            return spark.table(c)
        except Exception as e:
            last = e
    raise last

INVOLVED_PARTY = T("eil.m_involved_party_h", "eil.d_involved_party_h")
A2I_REL        = T("eil.m_arrangement_to_involved_party_relationship_h", "eil.d_arrangement_to_involved_party_relationship_h")
ARRANGEMENT    = T("eil.m_arrangement_h", "eil.d_arrangement_h")

# last_date inside requested window (matches your SQL pattern)
last_date = (
    INVOLVED_PARTY
    .where((F.to_date("business_date") >= start_dt) & (F.to_date("business_date") <= end_dt))
    .select(F.max(F.to_date("business_date")).alias("last_dt"))
    .first()["last_dt"]
)

# ==========================================================
# 1) WEALTH_RCIF (PW1) — RCIF grain (1 row per rcif_number)
# ONLY columns you asked + accts_cnt
# ==========================================================
ind = (
    INVOLVED_PARTY.alias("ind")
    .where(F.to_date("ind.business_date") == F.lit(last_date))
    .where(F.col("ind.source_system_code") == F.lit("CF"))
    .where(F.coalesce(F.col("ind.deceased_ind"), F.lit("N")) == F.lit("N"))
)

a2i = (
    A2I_REL.alias("a2i")
    .where(F.to_date("a2i.business_date") == F.lit(last_date))
)

ar = (
    ARRANGEMENT.alias("ar")
    .where(F.to_date("ar.business_date") == F.lit(last_date))
    .where(F.col("ar.source_system_code").isin(AR_SOURCE_SYSTEM_LIST))
    .where(F.col("ar.closed_ind") == F.lit(AR_CLOSED_ONLY))
)

pw_join = (
    ind.join(
        a2i,
        (F.col("ind.involved_party_id") == F.col("a2i.involved_party_id")) &
        (F.col("ind.source_system_code") == F.col("a2i.source_system_code")) &
        (F.col("ind.business_date") == F.col("a2i.business_date")),
        "inner"
    )
    .join(
        ar,
        (F.col("a2i.arrangement_id") == F.col("ar.arrangement_id")) &
        (F.col("a2i.arrangement_source_system_code") == F.col("ar.source_system_code")) &
        (F.col("a2i.business_date") == F.col("ar.business_date")),
        "inner"
    )
)

business_group = (
    F.when(F.col("ind.private_client_code").isin("039","539","339"), F.lit("Private Wealth"))
     .when(F.col("ind.private_client_trust_code").isin("239","739"), F.lit("Private Wealth"))
     .otherwise(
        F.when(F.col("ar.business_service_segment_type_code").isin("IS_CT","IS_IT"), F.lit("Institutional Services"))
         .when(F.col("ar.business_service_segment_type_code").isin("REGIS_FC","REGIS"), F.lit("Investment Services"))
         .when(F.col("ar.business_service_segment_type_code") == F.lit("PWM"), F.lit("Private Wealth"))
         .otherwise(F.lit("Other"))
     )
)

w_tmp = (
    pw_join.select(
        F.to_date(F.col("ind.business_date")).alias("business_date"),
        F.col("ind.rcif_cust_nbr").cast("string").alias("rcif_number"),
        F.col("ind.involved_party_id").alias("ip_id"),
        F.col("ind.cust_internet_banking_nbr").alias("cust_internet_banking_nbr"),
        business_group.alias("business_group"),
        F.col("ar.business_service_segment_type_code").alias("seg_code"),
        F.col("ar.source_system_code").alias("ar_source_system_code"),
        F.col("ar.arrangement_id").alias("arrangement_id")
    )
)

# keep only the bucket segments you intended
w_tmp = w_tmp.where(
    (F.col("seg_code").isin("IS_CT","IS_IT","REGIS_FC","REGIS","PWM")) |
    (F.col("business_group").isin("Private Wealth","Institutional Services","Investment Services"))
)

w_agg = (
    w_tmp.groupBy("rcif_number")
    .agg(
        F.max("business_date").alias("business_date"),
        F.max("ip_id").alias("ip_id"),
        F.max("cust_internet_banking_nbr").alias("cust_internet_banking_nbr"),
        F.max("business_group").alias("business_group"),
        F.countDistinct("arrangement_id").alias("accts_cnt"),

        # needed for division logic
        F.countDistinct(F.when(F.col("seg_code")=="REGIS_FC",F.col("arrangement_id"))).alias("investment_count"),
        F.countDistinct(F.when(F.col("seg_code")=="REGIS",   F.col("arrangement_id"))).alias("insurance_count"),
        F.countDistinct(F.when(F.col("ar_source_system_code")=="TR",F.col("arrangement_id"))).alias("trust_count"),
        F.countDistinct(F.when(F.col("ar_source_system_code").isin(
            'DA','SV','CC','MG','LS','TM','PC','LO','BW','CM','CS','EL','IC','MA','PF','PR','SD'
        ), F.col("arrangement_id"))).alias("banking_count"),
        F.countDistinct(F.when(F.col("seg_code")=="IS_CT",   F.col("arrangement_id"))).alias("corporate_trust_count"),
        F.countDistinct(F.when(F.col("seg_code")=="IS_IT",   F.col("arrangement_id"))).alias("institutional_trust_count"),
        F.countDistinct(F.when(F.col("seg_code")=="PWM",     F.col("arrangement_id"))).alias("pwm_count"),
    )
)

wealth_rcif = (
    w_agg
    .withColumn(
        "division",
        F.when(
            F.col("business_group") == "Private Wealth",
            F.when((F.col("trust_count") > 0) & (F.col("banking_count") > 0), F.lit("Banking & IM&T"))
             .otherwise(
                F.when((F.col("investment_count")+F.col("trust_count") > 0) & (F.col("banking_count")==0), F.lit("Investments Only"))
                 .otherwise(F.lit("Banking only"))
             )
        )
        .when(
            F.col("business_group") == "Investment Services",
            F.when((F.col("investment_count") > 0) & (F.col("insurance_count") == 0), F.lit("Investment"))
             .when((F.col("investment_count") == 0) & (F.col("insurance_count") > 0), F.lit("Insurance"))
             .otherwise(F.lit("Insurance & Investment"))
        )
        .otherwise(
            F.when((F.col("corporate_trust_count") > 0) & (F.col("institutional_trust_count") == 0), F.lit("Corporate Trust"))
             .when((F.col("corporate_trust_count") == 0) & (F.col("institutional_trust_count") > 0), F.lit("Institutional Trust"))
             .when(F.col("pwm_count") > 0, F.lit("Banking only"))
             .otherwise(F.lit("Corporate & Institutional Trust"))
        )
    )
    .select(
        "business_date","rcif_number","ip_id","cust_internet_banking_nbr",
        "business_group","division","accts_cnt"
    )
)

# ==========================================================
# 2) DIGITAL_IBN — use SOURCE digitally_active_flag only (your request)
# 1 row per ibn across window, “any-day active” rollup.
# ==========================================================
digital_src = (
    spark.table("dm_ib.digital_banking_master")
    .where((F.to_date("ods_business_dt") >= start_dt) & (F.to_date("ods_business_dt") <= end_dt))
    .select(
        F.to_date("ods_business_dt").alias("ods_business_dt"),
        F.upper(F.trim(F.col("ibn"))).alias("ibn"),
        F.col("rcif_customer_nbr").cast("string").alias("rcif_customer_number"),
        F.col("digitally_active_flag").alias("digitally_active_flag")
    )
    .where(F.col("ibn").isNotNull() & (F.length(F.col("ibn")) > 0))
)

digital_ibn = (
    digital_src.groupBy("ibn")
    .agg(
        F.max("ods_business_dt").alias("ods_business_dt"),
        F.max("rcif_customer_number").alias("rcif_customer_number"),
        F.max(F.when(F.col("digitally_active_flag")=="Digital Active", F.lit(1)).otherwise(F.lit(0))).alias("active_ind")
    )
    .withColumn(
        "digitally_active_flag",
        F.when(F.col("active_ind")==1, F.lit("Digital Active")).otherwise(F.lit("Non Digital Active"))
    )
    .drop("active_ind")
    .select("ods_business_dt","ibn","rcif_customer_number","digitally_active_flag")
)

# ==========================================================
# 3) INVESTPATH — act_cnt + ip_id grain (single table for both measures)
# This is the key to satisfy:
#   - accounts (114) via DISTINCTCOUNT(act_cnt)
#   - funded accounts (108) via DISTINCTCOUNT(act_cnt) w/ balance > 0
#   - customers (119) via DISTINCTCOUNT(ip_id)
# ==========================================================
inv_ind = (
    INVOLVED_PARTY.alias("ind")
    .where(F.to_date("ind.business_date") == F.lit(last_date))
    .where(F.col("ind.source_system_code") == F.lit("CF"))
    .where(F.coalesce(F.col("ind.deceased_ind"), F.lit("N")) == F.lit("N"))
)

inv_a2i = (
    A2I_REL.alias("a2i")
    .where(F.to_date("a2i.business_date") == F.lit(last_date))
)

inv_ar = (
    ARRANGEMENT.alias("ar")
    .where(F.to_date("ar.business_date") == F.lit(last_date))
    .where(F.col("ar.closed_ind") == F.lit(INV_CLOSED_ONLY))
    .where(F.col("ar.account_type_code") == F.lit(INV_ACCOUNT_TYPE_CODE))
    .where(F.col("ar.source_system_code") == F.lit(INV_AR_SOURCE_SYSTEM))
)

inv_join = (
    inv_ind.join(
        inv_a2i,
        (F.col("ind.involved_party_id") == F.col("a2i.involved_party_id")) &
        (F.col("ind.business_date") == F.col("a2i.business_date")) &
        (F.col("ind.source_system_code") == F.col("a2i.source_system_code")),
        "inner"
    )
    .join(
        inv_ar,
        (F.col("a2i.arrangement_id") == F.col("ar.arrangement_id")) &
        (F.col("a2i.arrangement_source_system_code") == F.col("ar.source_system_code")) &
        (F.col("a2i.business_date") == F.col("ar.business_date")),
        "inner"
    )
)

# Deduplicate balance/open_date per account, but KEEP multiple ip_id rows per account
# We do this by computing "latest account facts" per act_cnt and joining back.

acc_facts = (
    inv_join.select(
        F.col("ar.arrangement_id").alias("act_cnt"),
        F.col("ar.current_balance_amt").alias("balance"),
        F.col("ar.open_date").alias("open_date"),
        F.to_date(F.col("ar.business_date")).alias("business_date")
    )
)
w_acc = Window.partitionBy("act_cnt").orderBy(F.col("business_date").desc())
acc_facts_latest = (
    acc_facts.withColumn("rn", F.row_number().over(w_acc)).where("rn=1").drop("rn")
)

investpath = (
    inv_join.select(
        F.col("ar.arrangement_id").alias("act_cnt"),
        F.col("ind.rcif_cust_nbr").cast("string").alias("rcif_number"),
        F.col("a2i.involved_party_id").alias("ip_id"),
    )
    .dropDuplicates(["act_cnt","ip_id"])
    .join(acc_facts_latest, "act_cnt", "left")
    .select("business_date","rcif_number","ip_id","act_cnt","balance","open_date")
)

# ==========================================================
# SANITY CHECKS (match your expectations)
# ==========================================================
print("WEALTH: distinct rcif (expect 269148)")
wealth_rcif.selectExpr("count(distinct rcif_number) as wealth_rcif").show(truncate=False)

print("WEALTH: accounts via SUM(accts_cnt) (expect ~303414)")
wealth_rcif.selectExpr("sum(accts_cnt) as wealth_accounts_sum").show(truncate=False)

print("DIGITAL: active distinct ibn (expect 3428446)")
digital_ibn.filter(F.col("digitally_active_flag")=="Digital Active") \
          .selectExpr("count(distinct ibn) as digital_active_ibn").show(truncate=False)

print("INVESTPATH: accounts(114), funded(108), customers ip_id(119)")
investpath.selectExpr(
    "count(distinct act_cnt) as invest_accounts",
    "count(distinct case when balance > 0 then act_cnt end) as invest_accounts_funded",
    "count(distinct ip_id) as invest_customers_ip"
).show(truncate=False)

# ==========================================================
# WRITE TABLES
# ==========================================================
if DROP_AND_RECREATE:
    spark.sql(f"DROP TABLE IF EXISTS {WEALTH_FQN}")
    spark.sql(f"DROP TABLE IF EXISTS {DIG_FQN}")
    spark.sql(f"DROP TABLE IF EXISTS {INV_FQN}")

wealth_rcif.write.mode("overwrite").saveAsTable(WEALTH_FQN)
digital_ibn.write.mode("overwrite").saveAsTable(DIG_FQN)
investpath.write.mode("overwrite").saveAsTable(INV_FQN)

print("✅ Created 3 tables:")
print(WEALTH_FQN)
print(DIG_FQN)
print(INV_FQN)

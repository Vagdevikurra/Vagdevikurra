from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

# =========================
# CONFIG
# =========================
DB = "dm_ib_dev"
START_DATE = "2025-07-01"
END_DATE   = "2025-12-31"  # inclusive
DROP_AND_RECREATE = True

WEALTH_FQN = f"{DB}.wealth_pw1_202507_202512"
DIG_FQN    = f"{DB}.digital_ibn_202507_202512"
INV_FQN    = f"{DB}.investpath_ip_202507_202512"

# Wealth filters exactly like your screenshot
AR_SOURCE_SYSTEM_LIST = ['BI','RN','TR','DA','SV','CC','LS','MG','TM','PC','LO','BW','CS','IC','MA','PF','PR','SD','CM','EL']  # (LS duplicated in screenshot; harmless)
AR_CLOSED_ONLY = "N"

# InvestPath filters exactly like your screenshot
INV_ACCOUNT_TYPE_CODE = "IP"
INV_AR_SOURCE_SYSTEM  = "RN"
INV_CLOSED_ONLY       = "N"

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

def T(*candidates):
    """Return first existing table among candidates."""
    last = None
    for c in candidates:
        try:
            return spark.table(c)
        except Exception as e:
            last = e
    raise last

# Choose m_ or d_ versions safely
INVOLVED_PARTY = T("eil.m_involved_party_h", "eil.d_involved_party_h")
A2I_REL        = T("eil.m_arrangement_to_involved_party_relationship_h", "eil.d_arrangement_to_involved_party_relationship_h")
ARRANGEMENT    = T("eil.m_arrangement_h", "eil.d_arrangement_h")

# =========================
# Common: last_date inside the requested window (you use "Lst_Date" in InvestPath block)
# =========================
last_date = (
    INVOLVED_PARTY
    .where((F.to_date("business_date") >= start_dt) & (F.to_date("business_date") <= end_dt))
    .select(F.max(F.to_date("business_date")).alias("last_dt"))
    .first()["last_dt"]
)

# =========================
# 1) WEALTH (PW1 logic)  -- RCIF grain (safe)
# Implements:
#   - ind.source_system_code='CF'
#   - nvl(deceased_ind,'N')='N'
#   - join a2i + ar on business_date + keys
#   - ar.source_system_code IN (...) and ar.closed_ind='N'
#   - case-based Business_Group and counts by segment_type_code + banking/trust
#   - division logic from your 2nd screenshot
# =========================
ind = (
    INVOLVED_PARTY.alias("ind")
    .where(F.to_date("ind.business_date") == F.lit(last_date))            # matches your PW1 join to last_ip_date pattern at "latest"
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

# Join chain exactly like your SQL
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

# Business_Group exactly like your screenshot
business_group = (
    F.when(F.col("ind.private_client_code").isin("039","539","339"), F.lit("Private Wealth"))
     .when(F.col("ind.private_client_trust_code").isin("239","739"), F.lit("Private Wealth"))
     .otherwise(
        F.when(F.col("ar.business_service_segment_type_code").isin("IS_CT","IS_IT"), F.lit("Institutional Services"))
         .when(F.col("ar.business_service_segment_type_code").isin("REGIS_FC","REGIS"), F.lit("Investment Services"))
         .when(F.col("ar.business_service_segment_type_code") == F.lit("PWM"), F.lit("Private Wealth"))
         .otherwise(F.concat(F.col("ar.business_service_segment_type_code"), F.lit("_Category???")))
     )
)

# Build PW1-style counts at RCIF level
wealth_intermediate = (
    pw_join
    .select(
        F.to_date(F.col("ind.business_date")).alias("business_date"),
        F.col("ind.rcif_cust_nbr").cast("string").alias("rcif_number"),
        F.col("ind.cust_internet_banking_nbr").alias("cust_internet_banking_nbr"),
        F.col("ind.involved_party_id").alias("ip_id"),
        business_group.alias("business_group"),
        F.col("ar.business_service_segment_type_code").alias("seg_code"),
        F.col("ar.source_system_code").alias("ar_source_system_code"),
        F.col("ar.arrangement_id").alias("arrangement_id")
    )
)

# Apply your “include only these buckets” WHERE filter (from screenshot)
# (private codes or seg_code in IS_CT,IS_IT,REGIS_FC,REGIS,PWM)
wealth_intermediate = wealth_intermediate.where(
    (F.col("seg_code").isin("IS_CT","IS_IT","REGIS_FC","REGIS","PWM")) |
    (F.col("business_group").isin("Private Wealth","Institutional Services","Investment Services"))
)

wealth_pw1 = (
    wealth_intermediate
    .groupBy("rcif_number")
    .agg(
        F.max("business_date").alias("business_date"),
        F.max("ip_id").alias("ip_id"),
        F.max("cust_internet_banking_nbr").alias("cust_internet_banking_nbr"),
        F.max("business_group").alias("business_group"),

        F.countDistinct(F.when(F.col("seg_code")=="IS_CT",  F.col("arrangement_id"))).alias("corporate_trust_count"),
        F.countDistinct(F.when(F.col("seg_code")=="IS_IT",  F.col("arrangement_id"))).alias("institutional_trust_count"),
        F.countDistinct(F.when(F.col("seg_code")=="REGIS_FC",F.col("arrangement_id"))).alias("investment_count"),
        F.countDistinct(F.when(F.col("seg_code")=="REGIS",   F.col("arrangement_id"))).alias("insurance_count"),
        F.countDistinct(F.when(F.col("seg_code")=="PWM",     F.col("arrangement_id"))).alias("pwm_count"),

        F.countDistinct(F.when(F.col("ar_source_system_code")=="TR", F.col("arrangement_id"))).alias("trust_count"),
        F.countDistinct(F.when(F.col("ar_source_system_code").isin('DA','SV','CC','MG','LS','TM','PC','LO','BW','CM','CS','EL','IC','MA','PF','PR','SD'),
                               F.col("arrangement_id"))).alias("banking_count"),

        F.countDistinct("arrangement_id").alias("accts_cnt")
    )
)

# Division logic exactly like your 2nd screenshot
wealth_pw1 = (
    wealth_pw1
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
        "business_group","division",
        "corporate_trust_count","institutional_trust_count","investment_count","insurance_count",
        "pwm_count","trust_count","banking_count","accts_cnt"
    )
)

# =========================
# 2) DIGITAL (your Dig_Customer logic) -- IBN grain by month
# Implements:
#   - group by TRUNC(ods_business_dt,'MM'), ibn, rcif_customer_nbr
#   - max logins
#   - flags (Mobile/OLB/Digital Active) computed like your screenshot
# NOTE: uses ibn (not relt_ibn)
# =========================
dbm = (
    spark.table("dm_ib.digital_banking_master").alias("dbm")
    .where((F.to_date("dbm.ods_business_dt") >= start_dt) & (F.to_date("dbm.ods_business_dt") <= end_dt))
    .select(
        F.trunc(F.to_date("dbm.ods_business_dt"), "MM").alias("month_dt"),
        F.to_date("dbm.ods_business_dt").alias("ods_business_dt"),
        F.col("dbm.ibn").alias("ibn"),
        F.col("dbm.rcif_customer_nbr").cast("string").alias("rcif_customer_number"),
        F.col("dbm.olb_last_login_date").alias("lst_login_olb"),
        F.col("dbm.mob_last_login_date").alias("lst_login_mob"),
    )
    .where(F.col("ibn").isNotNull())
)

dig_customer = (
    dbm.groupBy("month_dt","ibn","rcif_customer_number")
       .agg(
           F.max("lst_login_olb").alias("lst_login_olb"),
           F.max("lst_login_mob").alias("lst_login_mob"),
           F.max("ods_business_dt").alias("ods_business_dt")
       )
       .withColumn(
           "mobile_active_flag",
           F.when(F.datediff(F.col("ods_business_dt"), F.col("lst_login_mob")) <= 90, F.lit("Mobile Active"))
            .otherwise(F.lit("Non Mobile Active"))
       )
       .withColumn(
           "mobile_flag",
           F.when(F.col("lst_login_mob").isNull(), F.lit("Non Mobile User")).otherwise(F.lit("Mobile User"))
       )
       .withColumn(
           "olb_active_flag",
           F.when(F.datediff(F.col("ods_business_dt"), F.col("lst_login_olb")) <= 90, F.lit("OLB Active"))
            .otherwise(F.lit("Non OLB Active"))
       )
       .withColumn(
           "olb_flag",
           F.when(F.col("lst_login_olb").isNull(), F.lit("Non OLB User")).otherwise(F.lit("OLB User"))
       )
       .withColumn(
           "digitally_active_flag",
           F.when(
               (F.datediff(F.col("ods_business_dt"), F.col("lst_login_mob")) <= 90) |
               (F.datediff(F.col("ods_business_dt"), F.col("lst_login_olb")) <= 90),
               F.lit("Digital Active")
           ).otherwise(F.lit("Non Digital Active"))
       )
       .withColumn(
           "digital_flag",
           F.lit("Digital User")
       )
       .select(
           "month_dt","ods_business_dt","ibn","rcif_customer_number",
           "mobile_active_flag","mobile_flag","olb_active_flag","olb_flag",
           "digitally_active_flag","digital_flag"
       )
)

# =========================
# 3) INVESTPATH (INV logic) -- arrangement grain (safe)
# Implements:
#   - last_date snapshot inside window
#   - joins ind -> a2i -> ar
#   - ar.account_type_code='IP', ar.source_system_code='RN', ar.closed_ind='N'
# =========================
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

# Dedup to 1 row per arrangement_id (prevents explosion)
w_arr = Window.partitionBy(F.col("ar.arrangement_id")).orderBy(F.col("ar.business_date").desc())

investpath = (
    inv_join
    .select(
        F.col("ind.rcif_cust_nbr").cast("string").alias("rcif_number"),
        F.col("ind.involved_party_id").alias("ip_id"),
        F.col("ar.arrangement_id").alias("arrangement_id"),
        F.col("ar.current_balance_amt").alias("balance"),
        F.col("ar.open_date").alias("open_date"),
        F.to_date(F.col("ar.business_date")).alias("business_date")
    )
    .withColumn("rn", F.row_number().over(w_arr))
    .where(F.col("rn") == 1)
    .drop("rn")
    .withColumnRenamed("arrangement_id", "act_cnt")  # matches your "act_cnt as Accounts"
)

# =========================
# SANITY CHECKS (these should look like your expectations)
# =========================
print("WEALTH distinct RCIF:")
wealth_pw1.selectExpr("count(distinct rcif_number) as wealth_rcif").show(truncate=False)

print("DIGITAL active distinct IBN (month-sliced table):")
dig_customer.filter(F.col("digitally_active_flag")=="Digital Active") \
            .selectExpr("count(distinct ibn) as digital_active_ibn").show(truncate=False)

print("INVESTPATH distinct accounts and customers:")
investpath.selectExpr(
    "count(distinct act_cnt) as investpath_accounts",
    "count(distinct rcif_number) as investpath_customers"
).show(truncate=False)

# =========================
# WRITE TABLES
# =========================
if DROP_AND_RECREATE:
    spark.sql(f"DROP TABLE IF EXISTS {WEALTH_FQN}")
    spark.sql(f"DROP TABLE IF EXISTS {DIG_FQN}")
    spark.sql(f"DROP TABLE IF EXISTS {INV_FQN}")

wealth_pw1.write.mode("overwrite").saveAsTable(WEALTH_FQN)
dig_customer.write.mode("overwrite").saveAsTable(DIG_FQN)
investpath.write.mode("overwrite").saveAsTable(INV_FQN)

print("✅ Created 3 tables:")
print(WEALTH_FQN)
print(DIG_FQN)
print(INV_FQN)

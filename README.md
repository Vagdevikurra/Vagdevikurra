from pyspark.sql import SparkSession, functions as F, Window

# ==========================================================
# CONFIG
# ==========================================================
DB = "dm_ib_dev"

START_DATE = "2025-08-01"
END_DATE   = "2026-01-31"   # <-- use month end since you said 08/01–01/31

DROP_AND_RECREATE = True

WIC_FQN = f"{DB}.wic2"   # wealth RCIF DIM (1 row per RCIF)
WIA_FQN = f"{DB}.wia2"   # wealth accounts FACT (arrangement grain)
WID_FQN = f"{DB}.wid2"   # digital SNAPSHOT FACT (max ods_business_dt in window)

AR_SOURCE_SYSTEM_LIST = ['BI','RN','TR','DA','SV','CC','LS','MG','TM','PC','LO','BW','CS','IC','MA','PF','PR','SD','CM','EL']
AR_CLOSED_ONLY = "N"

WEALTH_SEG_KEEP = ["IS_CT","IS_IT","REGIS_FC","REGIS","PWM"]
WEALTH_BG_KEEP  = ["Private Wealth","Institutional Services","Investment Services"]

# ==========================================================
# Spark
# ==========================================================
def get_spark(app_name="final_3table_snapshot_fix"):
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
# Helpers
# ==========================================================
def first_existing_col(df, *candidates):
    cols = set(df.columns)
    for c in candidates:
        if c in cols:
            return c
    raise ValueError(f"Missing columns. Tried {candidates}. Sample cols: {list(cols)[:80]}")

def maybe_col(df, *candidates):
    cols = set(df.columns)
    for c in candidates:
        if c in cols:
            return c
    return None

def T(*candidates):
    last = None
    for c in candidates:
        try:
            return spark.table(c)
        except Exception as e:
            last = e
    raise last

# ==========================================================
# Source tables
# ==========================================================
INVOLVED_PARTY = T("eil.m_involved_party_h", "eil.d_involved_party_h")
A2I_REL        = T("eil.m_arrangement_to_involved_party_relationship_h", "eil.d_arrangement_to_involved_party_relationship_h")
ARRANGEMENT    = T("eil.m_arrangement_h", "eil.d_arrangement_h")
ADDRESS        = T("eil.m_involved_party_address_h", "eil.d_involved_party_address_h")
DBM            = spark.table("dm_ib.digital_banking_master")

# EIL column guardrails
ip_deceased_col = first_existing_col(INVOLVED_PARTY, "deceased_ind", "deceased_indicator", "deceased_flag")
ip_rcif_col     = first_existing_col(INVOLVED_PARTY, "rcif_cust_nbr", "rcif_customer_nbr", "rcif_nbr")
ip_ibn_col      = first_existing_col(INVOLVED_PARTY, "cust_internet_banking_nbr", "cust_internet_banking_number", "cust_internet_banking_no")
ip_id_col       = first_existing_col(INVOLVED_PARTY, "involved_party_id", "ip_id")

a2i_ip_col      = first_existing_col(A2I_REL, "involved_party_id", "ip_id")
a2i_arr_col     = first_existing_col(A2I_REL, "arrangement_id", "act_cnt")
a2i_arr_src_col = first_existing_col(A2I_REL, "arrangement_source_system_code", "arrangement_source_system_cd", "arrangement_src_system_code")
a2i_src_col     = first_existing_col(A2I_REL, "source_system_code", "src_system_code")

ar_arr_col      = first_existing_col(ARRANGEMENT, "arrangement_id", "act_cnt")
ar_src_col      = first_existing_col(ARRANGEMENT, "source_system_code", "src_system_code")
ar_closed_col   = first_existing_col(ARRANGEMENT, "closed_ind", "closed_indicator", "closed_flag")
ar_seg_col      = first_existing_col(ARRANGEMENT, "business_service_segment_type_code", "business_service_segment_code", "segment_type_code")
ar_open_col     = first_existing_col(ARRANGEMENT, "open_date", "account_open_date")
ar_balance_col  = first_existing_col(ARRANGEMENT, "current_balance_amt", "balance", "current_balance_amount")

addr_state_col  = first_existing_col(ADDRESS, "state_name", "state", "state_cd", "state_code")
addr_ip_col     = first_existing_col(ADDRESS, "involved_party_id", "ip_id")

# Wealth snapshot date = last business_date in the window
last_date = (
    INVOLVED_PARTY
    .where((F.to_date("business_date") >= start_dt) & (F.to_date("business_date") <= end_dt))
    .select(F.max(F.to_date("business_date")).alias("last_dt"))
    .first()["last_dt"]
)

# ==========================================================
# 1) DIGITAL SNAPSHOT FACT (wid2)
#    Match your SQL: ods_business_dt = max(ods_business_dt) within the window
# ==========================================================
ibn_col  = first_existing_col(DBM, "relt_ibn", "ibn")
rcif_col = maybe_col(DBM, "rcif_customer_nbr", "rcif_cust_nbr", "rcif_nbr")  # optional
olb_col  = first_existing_col(DBM, "olb_last_login_date", "olb_last_login_dt")
mob_col  = first_existing_col(DBM, "mob_last_login_date", "mob_last_login_dt")
state_col = maybe_col(DBM, "state_name", "state", "state_cd", "state_code", "customer_state", "mailing_state")

digital_windowed = (
    DBM
    .where((F.to_date("ods_business_dt") >= start_dt) & (F.to_date("ods_business_dt") <= end_dt))
    .select(
        F.to_date("ods_business_dt").alias("ods_business_dt"),
        F.upper(F.trim(F.col(ibn_col).cast("string"))).alias("reltibn"),
        (F.col(rcif_col).cast("string").alias("rcif_customer_nbr") if rcif_col else F.lit(None).cast("string").alias("rcif_customer_nbr")),
        F.to_date(F.col(olb_col)).alias("olb_last_login_dt"),
        F.to_date(F.col(mob_col)).alias("mob_last_login_dt"),
        (F.col(state_col).cast("string").alias("state_name") if state_col else F.lit(None).cast("string").alias("state_name"))
    )
    .where(F.col("reltibn").isNotNull() & (F.length("reltibn") > 0))
)

snapshot_dt = digital_windowed.select(F.max("ods_business_dt").alias("mx")).first()["mx"]

# One row per IBN at snapshot (keep max login fields)
wid2 = (
    digital_windowed
    .where(F.col("ods_business_dt") == F.lit(snapshot_dt))
    .groupBy("ods_business_dt", "reltibn")
    .agg(
        F.max("rcif_customer_nbr").alias("rcif_customer_nbr"),
        F.max("olb_last_login_dt").alias("olb_last_login_dt"),
        F.max("mob_last_login_dt").alias("mob_last_login_dt"),
        F.max("state_name").alias("state_name")
    )
    .withColumn("olb_flag",    F.when(F.col("olb_last_login_dt").isNotNull(), F.lit("OLB User")).otherwise(F.lit("Non OLB User")))
    .withColumn("mobile_flag", F.when(F.col("mob_last_login_dt").isNotNull(), F.lit("Mobile User")).otherwise(F.lit("Non Mobile User")))
    .withColumn(
        "olb_active_flag",
        F.when(
            F.col("olb_last_login_dt").isNotNull() &
            (F.datediff(F.col("ods_business_dt"), F.col("olb_last_login_dt")) <= 90),
            F.lit("OLB Active")
        ).otherwise(F.lit("OLB Not Active"))
    )
    .withColumn(
        "mobile_activity_flag",
        F.when(
            F.col("mob_last_login_dt").isNotNull() &
            (F.datediff(F.col("ods_business_dt"), F.col("mob_last_login_dt")) <= 90),
            F.lit("Mobile Active")
        ).otherwise(F.lit("Mobile Not Active"))
    )
    .withColumn(
        "digitally_active_flag",
        F.when(
            (F.col("olb_active_flag") == "OLB Active") | (F.col("mobile_activity_flag") == "Mobile Active"),
            F.lit("Digital Active")
        ).otherwise(F.lit("Non Digital Active"))
    )
    .withColumn("digital_flag", F.lit("Digital User"))
)

active_ibn_snapshot = (
    wid2.where(F.col("digitally_active_flag") == "Digital Active")
        .select("reltibn")
        .dropDuplicates()
)

# ==========================================================
# 2) WEALTH ACCOUNTS FACT (wia2) — arrangement grain
# ==========================================================
ind = (
    INVOLVED_PARTY.alias("ind")
    .where(F.to_date("ind.business_date") == F.lit(last_date))
    .where(F.col("ind.source_system_code") == F.lit("CF"))
    .where(F.coalesce(F.col(f"ind.{ip_deceased_col}"), F.lit("N")) == F.lit("N"))
)

addr = (
    ADDRESS.alias("addr")
    .where(F.to_date("addr.business_date") == F.lit(last_date))
    .select(
        F.col(f"addr.{addr_ip_col}").alias("ip_id_addr"),
        F.col(f"addr.{addr_state_col}").cast("string").alias("state_name")
    )
)

a2i = A2I_REL.alias("a2i").where(F.to_date("a2i.business_date") == F.lit(last_date))

ar = (
    ARRANGEMENT.alias("ar")
    .where(F.to_date("ar.business_date") == F.lit(last_date))
    .where(F.col(f"ar.{ar_src_col}").isin(AR_SOURCE_SYSTEM_LIST))
    .where(F.col(f"ar.{ar_closed_col}") == F.lit(AR_CLOSED_ONLY))
)

pw_join = (
    ind.join(
        a2i,
        (F.col(f"ind.{ip_id_col}") == F.col(f"a2i.{a2i_ip_col}")) &
        (F.col("ind.source_system_code") == F.col(f"a2i.{a2i_src_col}")) &
        (F.col("ind.business_date") == F.col("a2i.business_date")),
        "inner"
    )
    .join(
        ar,
        (F.col(f"a2i.{a2i_arr_col}") == F.col(f"ar.{ar_arr_col}")) &
        (F.col(f"a2i.{a2i_arr_src_col}") == F.col(f"ar.{ar_src_col}")) &
        (F.col("a2i.business_date") == F.col("ar.business_date")),
        "inner"
    )
    .join(
        addr,
        F.col(f"ind.{ip_id_col}") == F.col("ip_id_addr"),
        "left"
    )
)

business_group_expr = (
    F.when(F.col("ind.private_client_code").isin("039","539","339"), F.lit("Private Wealth"))
     .when(F.col("ind.private_client_trust_code").isin("239","739"), F.lit("Private Wealth"))
     .otherwise(
        F.when(F.col(f"ar.{ar_seg_col}").isin("IS_CT","IS_IT"), F.lit("Institutional Services"))
         .when(F.col(f"ar.{ar_seg_col}").isin("REGIS_FC","REGIS"), F.lit("Investment Services"))
         .when(F.col(f"ar.{ar_seg_col}") == F.lit("PWM"), F.lit("Private Wealth"))
         .otherwise(F.lit("Other"))
     )
)

wia2_raw = (
    pw_join.select(
        F.to_date(F.col("ind.business_date")).alias("business_date"),
        F.col(f"ind.{ip_rcif_col}").cast("string").alias("rcif_number"),
        F.col(f"ind.{ip_id_col}").alias("ip_id"),
        F.upper(F.trim(F.col(f"ind.{ip_ibn_col}").cast("string"))).alias("customer_internet_banking_number"),
        F.col("state_name").alias("state_name"),
        business_group_expr.alias("business_group"),
        F.col(f"ar.{ar_seg_col}").alias("seg_code"),
        F.col(f"ar.{ar_src_col}").alias("ar_source_system_code"),
        F.col(f"ar.{ar_arr_col}").alias("arrangement_id"),
        F.to_date(F.col(f"ar.{ar_open_col}")).alias("open_date"),
        F.col(f"ar.{ar_balance_col}").alias("balance")
    )
    .dropna(subset=["rcif_number", "arrangement_id"])
)

# Wealth population filter (keeps Wealth Users ~269k)
wia2_filtered = (
    wia2_raw.where(
        (F.col("seg_code").isin(WEALTH_SEG_KEEP)) |
        (F.col("business_group").isin(WEALTH_BG_KEEP))
    )
)

# De-dupe at arrangement_id + rcif_number
wia2 = (
    wia2_filtered
    .dropDuplicates(["arrangement_id", "rcif_number"])
    .select(
        "business_date","rcif_number","ip_id","arrangement_id","open_date","balance",
        "seg_code","ar_source_system_code",
        "customer_internet_banking_number","business_group","state_name"
    )
)

# ==========================================================
# 3) WEALTH RCIF DIM (wic2) — enroll via IBN ∈ active IBN snapshot
# ==========================================================
wic_counts = (
    wia2.groupBy("rcif_number")
        .agg(
            F.max("business_date").alias("business_date"),
            F.max("ip_id").alias("ip_id"),
            F.max("customer_internet_banking_number").alias("customer_internet_banking_number"),
            F.max("business_group").alias("business_group"),
            F.max("state_name").alias("state_name"),
            F.countDistinct("arrangement_id").alias("accts_cnt"),
            F.min("open_date").alias("open_date"),

            F.countDistinct(F.when(F.col("seg_code")=="REGIS_FC", F.col("arrangement_id"))).alias("investment_count"),
            F.countDistinct(F.when(F.col("seg_code")=="REGIS",    F.col("arrangement_id"))).alias("insurance_count"),
            F.countDistinct(F.when(F.col("ar_source_system_code")=="TR", F.col("arrangement_id"))).alias("trust_count"),
            F.countDistinct(F.when(F.col("ar_source_system_code").isin(
                'DA','SV','CC','MG','LS','TM','PC','LO','BW','CM','CS','EL','IC','MA','PF','PR','SD'
            ), F.col("arrangement_id"))).alias("banking_count"),
            F.countDistinct(F.when(F.col("seg_code")=="IS_CT", F.col("arrangement_id"))).alias("corporate_trust_count"),
            F.countDistinct(F.when(F.col("seg_code")=="IS_IT", F.col("arrangement_id"))).alias("institutional_trust_count"),
            F.countDistinct(F.when(F.col("seg_code")=="PWM",   F.col("arrangement_id"))).alias("pwm_count"),
        )
)

wic2_base = (
    wic_counts
    .withColumn(
        "division",
        F.when(
            F.col("business_group") == "Private Wealth",
            F.when((F.col("trust_count") > 0) & (F.col("banking_count") > 0), F.lit("Banking & IM&T"))
             .otherwise(
                F.when(((F.col("investment_count")+F.col("trust_count")) > 0) & (F.col("banking_count")==0), F.lit("Investments Only"))
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
)

# Enrollment flag: Wealth IBN is in active digital IBN snapshot
wic2 = (
    wic2_base
    .join(
        active_ibn_snapshot.withColumnRenamed("reltibn", "ibn_active").withColumn("is_active_latest", F.lit(1)),
        F.upper(F.trim(F.col("customer_internet_banking_number"))) == F.col("ibn_active"),
        "left"
    )
    .withColumn(
        "digital_enrollment_flag_latest_month",
        F.when(F.col("is_active_latest") == 1, F.lit("Digital Active")).otherwise(F.lit("Non Digital Active"))
    )
    .drop("ibn_active", "is_active_latest")
    .select(
        "business_date",
        "rcif_number",
        "ip_id",
        "customer_internet_banking_number",
        "state_name",
        "business_group",
        "division",
        "open_date",
        "accts_cnt",
        "digital_enrollment_flag_latest_month"
    )
)

# ==========================================================
# SANITY OUTPUTS
# ==========================================================
print("Digital snapshot ods_business_dt used:", snapshot_dt)

print("Wealth Users (expect ~269148):")
wic2.selectExpr("count(distinct rcif_number) as wealth_users").show(truncate=False)

print("Accounts Total (expect ~303k):")
wia2.selectExpr("count(distinct arrangement_id) as accounts_total").show(truncate=False)

print("Top Digital Active IBN on snapshot day (expect ~3427877):")
wid2.where(F.col("digitally_active_flag")=="Digital Active") \
   .selectExpr("count(distinct reltibn) as digital_active_ibn_snapshot").show(truncate=False)

print("Digital Enrollments Wealth (expect ~121k):")
wic2.where(F.col("digital_enrollment_flag_latest_month")=="Digital Active") \
   .selectExpr("count(distinct rcif_number) as digital_enrollments_wealth").show(truncate=False)

# ==========================================================
# WRITE TABLES
# ==========================================================
if DROP_AND_RECREATE:
    spark.sql(f"DROP TABLE IF EXISTS {WIC_FQN}")
    spark.sql(f"DROP TABLE IF EXISTS {WIA_FQN}")
    spark.sql(f"DROP TABLE IF EXISTS {WID_FQN}")

wic2.write.mode("overwrite").saveAsTable(WIC_FQN)
wia2.write.mode("overwrite").saveAsTable(WIA_FQN)
wid2.write.mode("overwrite").saveAsTable(WID_FQN)

print("✅ Created tables:")
print(WIC_FQN)
print(WIA_FQN)
print(WID_FQN)

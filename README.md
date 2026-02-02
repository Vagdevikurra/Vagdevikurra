from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

# ==========================================================
# CONFIG
# ==========================================================
DB = "dm_ib_dev"
START_DATE = "2025-07-01"
END_DATE   = "2025-12-31"
DROP_AND_RECREATE = True

WEALTH_FQN = f"{DB}.wic2"
DIG_FQN    = f"{DB}.wid2"

# Wealth filters (keep aligned with your earlier script)
AR_SOURCE_SYSTEM_LIST = ['BI','RN','TR','DA','SV','CC','LS','MG','TM','PC','LO','BW','CS','IC','MA','PF','PR','SD','CM','EL']
AR_CLOSED_ONLY = "N"

# ==========================================================
# Spark
# ==========================================================
def get_spark(app_name="wealth_digital_2tables_snapshot_match"):
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
    raise ValueError(f"Missing columns. Tried {candidates}. Sample cols: {list(cols)[:50]}")

def T(*candidates):
    last = None
    for c in candidates:
        try:
            return spark.table(c)
        except Exception as e:
            last = e
    raise last

# ==========================================================
# SOURCE TABLES
# ==========================================================
INVOLVED_PARTY = T("eil.m_involved_party_h", "eil.d_involved_party_h")
A2I_REL        = T("eil.m_arrangement_to_involved_party_relationship_h", "eil.d_arrangement_to_involved_party_relationship_h")
ARRANGEMENT    = T("eil.m_arrangement_h", "eil.d_arrangement_h")
DBM            = spark.table("dm_ib.digital_banking_master")

# Column guards
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

# ==========================================================
# SNAPSHOT DATES
# ==========================================================
# wealth snapshot = last business_date in window
last_wealth_date = (
    INVOLVED_PARTY
    .where((F.to_date("business_date") >= start_dt) & (F.to_date("business_date") <= end_dt))
    .select(F.max(F.to_date("business_date")).alias("dt"))
    .first()["dt"]
)

# ==========================================================
# 1) DIGITAL (wid2) — month-end grain + OLB/MOB flags + Digital Active
# ==========================================================
ibn_col  = first_existing_col(DBM, "relt_ibn", "ibn")
rcif_col = first_existing_col(DBM, "rcif_customer_nbr", "rcif_cust_nbr", "rcif_nbr")
olb_col  = first_existing_col(DBM, "olb_last_login_date", "olb_last_login_dt")
mob_col  = first_existing_col(DBM, "mob_last_login_date", "mob_last_login_dt")

digital_raw = (
    DBM
    .where((F.to_date("ods_business_dt") >= start_dt) & (F.to_date("ods_business_dt") <= end_dt))
    .select(
        F.last_day(F.to_date("ods_business_dt")).alias("month_dt"),      # ✅ month-end (matches report)
        F.to_date("ods_business_dt").alias("ods_business_dt"),
        F.upper(F.trim(F.col(ibn_col).cast("string"))).alias("reltibn"),
        F.col(rcif_col).cast("string").alias("rcif_customer_nbr"),
        F.to_date(F.col(olb_col)).alias("olb_last_login_dt"),
        F.to_date(F.col(mob_col)).alias("mob_last_login_dt"),
    )
    .where(F.col("reltibn").isNotNull() & (F.length("reltibn") > 0))
)

wid2 = (
    digital_raw
    .groupBy("month_dt", "reltibn", "rcif_customer_nbr")
    .agg(
        F.max("ods_business_dt").alias("ods_business_dt"),
        F.max("olb_last_login_dt").alias("olb_last_login_dt"),
        F.max("mob_last_login_dt").alias("mob_last_login_dt"),
    )
    .withColumn(
        "olb_active_flag",
        F.when(
            F.col("olb_last_login_dt").isNotNull() &
            (F.datediff(F.col("ods_business_dt"), F.col("olb_last_login_dt")) <= 90),
            F.lit("OLB Active")
        ).otherwise(F.lit("OLB Not Active"))
    )
    .withColumn(
        "mob_active_flag",
        F.when(
            F.col("mob_last_login_dt").isNotNull() &
            (F.datediff(F.col("ods_business_dt"), F.col("mob_last_login_dt")) <= 90),
            F.lit("MOB Active")
        ).otherwise(F.lit("MOB Not Active"))
    )
    .withColumn(
        "digitally_active_flag",
        F.when(
            (F.col("olb_active_flag") == "OLB Active") | (F.col("mob_active_flag") == "MOB Active"),
            F.lit("Digital Active")
        ).otherwise(F.lit("Non Digital Active"))
    )
    .withColumn("digital_flag", F.lit("Digital User"))
)

latest_month = wid2.select(F.max("month_dt").alias("mx")).first()["mx"]

# ==========================================================
# 2) WEALTH (wic2) — RCIF grain + accounts
# ==========================================================
ind = (
    INVOLVED_PARTY.alias("ind")
    .where(F.to_date("ind.business_date") == F.lit(last_wealth_date))
    .where(F.col("ind.source_system_code") == "CF")
    .where(F.coalesce(F.col(f"ind.{ip_deceased_col}"), F.lit("N")) == "N")
)

a2i = (
    A2I_REL.alias("a2i")
    .where(F.to_date("a2i.business_date") == F.lit(last_wealth_date))
)

ar = (
    ARRANGEMENT.alias("ar")
    .where(F.to_date("ar.business_date") == F.lit(last_wealth_date))
    .where(F.col(f"ar.{ar_src_col}").isin(AR_SOURCE_SYSTEM_LIST))
    .where(F.col(f"ar.{ar_closed_col}") == AR_CLOSED_ONLY)
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
)

business_group = (
    F.when(F.col("ind.private_client_code").isin("039","539","339"), F.lit("Private Wealth"))
     .when(F.col("ind.private_client_trust_code").isin("239","739"), F.lit("Private Wealth"))
     .otherwise(
        F.when(F.col(f"ar.{ar_seg_col}").isin("IS_CT","IS_IT"), F.lit("Institutional Services"))
         .when(F.col(f"ar.{ar_seg_col}").isin("REGIS_FC","REGIS"), F.lit("Investment Services"))
         .when(F.col(f"ar.{ar_seg_col}") == "PWM", F.lit("Private Wealth"))
         .otherwise(F.lit("Other"))
     )
)

wic2_base = (
    pw_join.select(
        F.to_date("ind.business_date").alias("business_date"),
        F.col(f"ind.{ip_rcif_col}").cast("string").alias("rcif_number"),
        F.col(f"ind.{ip_id_col}").alias("ip_id"),
        F.upper(F.trim(F.col(f"ind.{ip_ibn_col}").cast("string"))).alias("cust_internet_banking_nbr"),
        business_group.alias("business_group"),
        F.col(f"ar.{ar_arr_col}").alias("arrangement_id")
    )
    .dropna(subset=["rcif_number"])
)

wic2_agg = (
    wic2_base
    .groupBy("rcif_number")
    .agg(
        F.max("business_date").alias("business_date"),
        F.max("ip_id").alias("ip_id"),
        F.max("cust_internet_banking_nbr").alias("cust_internet_banking_nbr"),
        F.max("business_group").alias("business_group"),
        F.countDistinct("arrangement_id").alias("accts_cnt")
    )
)

# Enrollment flag in wealth = RCIF is Digital Active in latest month (RCIF-based like your model)
latest_month_active_rcif = (
    wid2
    .where((F.col("month_dt") == F.lit(latest_month)) & (F.col("digitally_active_flag") == "Digital Active"))
    .select(F.col("rcif_customer_nbr").cast("string").alias("rcif_number"))
    .dropDuplicates()
)

wic2 = (
    wic2_agg
    .join(latest_month_active_rcif.withColumn("rcif_digital_active_latest_month", F.lit(1)),
          on="rcif_number", how="left")
    .withColumn(
        "rcif_digital_active_latest_month",
        F.when(F.col("rcif_digital_active_latest_month") == 1, F.lit("Digital Active"))
         .otherwise(F.lit("Non Digital Active"))
    )
)

# ==========================================================
# SANITY CHECKS (your expected cards)
# ==========================================================
print("Wealth Users (expect 269148):")
wic2.selectExpr("count(distinct rcif_number) as wealth_users").show(truncate=False)

print("Accounts (expect 303414):")
wic2.selectExpr("sum(accts_cnt) as accounts_total").show(truncate=False)

print("Top of company Total Digital Active (latest month) expect 3428446:")
wid2.where((F.col("month_dt")==F.lit(latest_month)) & (F.col("digitally_active_flag")=="Digital Active")) \
   .selectExpr("count(distinct reltibn) as digital_active_ibn").show(truncate=False)

print("Digital Enrollments Wealth (expect 121933):")
wic2.where(F.col("rcif_digital_active_latest_month")=="Digital Active") \
   .selectExpr("count(distinct rcif_number) as digital_enrollments_wealth").show(truncate=False)

# ==========================================================
# WRITE TABLES (2 only)
# ==========================================================
if DROP_AND_RECREATE:
    spark.sql(f"DROP TABLE IF EXISTS {WEALTH_FQN}")
    spark.sql(f"DROP TABLE IF EXISTS {DIG_FQN}")

wic2.write.mode("overwrite").saveAsTable(WEALTH_FQN)
wid2.write.mode("overwrite").saveAsTable(DIG_FQN)

print("✅ Created 2 tables:")
print(WEALTH_FQN)
print(DIG_FQN)

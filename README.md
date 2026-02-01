from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

# ==========================================================
# CONFIG (FIXED WINDOW)
# ==========================================================
DB = "dm_ib_dev"
START_DATE = "2025-07-01"
END_DATE   = "2025-12-31"  # inclusive
DROP_AND_RECREATE = True

WEALTH_FQN = f"{DB}.wealth_rcif_202507_202512"
DIG_FQN    = f"{DB}.digital_202507_202512"
INV_FQN    = f"{DB}.investpath_202507_202512"

AR_SOURCE_SYSTEM_LIST = ['BI','RN','TR','DA','SV','CC','LS','MG','TM','PC','LO','BW','CS','IC','MA','PF','PR','SD','CM','EL']
AR_CLOSED_ONLY = "N"

INV_ACCOUNT_TYPE_CODE = "IP"
INV_AR_SOURCE_SYSTEM  = "RN"
INV_CLOSED_ONLY       = "N"

# ==========================================================
# Spark
# ==========================================================
def get_spark(app_name="wealth_digital_investpath_3tables_202507_202512"):
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

# Pick m_ or d_ tables safely
INVOLVED_PARTY = T("eil.m_involved_party_h", "eil.d_involved_party_h")
A2I_REL        = T("eil.m_arrangement_to_involved_party_relationship_h", "eil.d_arrangement_to_involved_party_relationship_h")
ARRANGEMENT    = T("eil.m_arrangement_h", "eil.d_arrangement_h")

# Column-name guardrails
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
ar_balance_col  = first_existing_col(ARRANGEMENT, "current_balance_amt", "balance", "current_balance_amount")
ar_open_col     = first_existing_col(ARRANGEMENT, "open_date", "account_open_date")
ar_acct_type_col= first_existing_col(ARRANGEMENT, "account_type_code", "acct_type_code")

# last_date inside requested window (your pattern)
last_date = (
    INVOLVED_PARTY
    .where((F.to_date("business_date") >= start_dt) & (F.to_date("business_date") <= end_dt))
    .select(F.max(F.to_date("business_date")).alias("last_dt"))
    .first()["last_dt"]
)

# ==========================================================
# 1) WEALTH (RCIF grain) — matches repo style
# ==========================================================
ind = (
    INVOLVED_PARTY.alias("ind")
    .where(F.to_date(F.col("ind.business_date")) == F.lit(last_date))
    .where(F.col("ind.source_system_code") == F.lit("CF"))
    .where(F.coalesce(F.col(f"ind.{ip_deceased_col}"), F.lit("N")) == F.lit("N"))
)

a2i = (
    A2I_REL.alias("a2i")
    .where(F.to_date(F.col("a2i.business_date")) == F.lit(last_date))
)

ar = (
    ARRANGEMENT.alias("ar")
    .where(F.to_date(F.col("ar.business_date")) == F.lit(last_date))
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
)

business_group = (
    F.when(F.col("ind.private_client_code").isin("039","539","339"), F.lit("Private Wealth"))
     .when(F.col("ind.private_client_trust_code").isin("239","739"), F.lit("Private Wealth"))
     .otherwise(
        F.when(F.col(f"ar.{ar_seg_col}").isin("IS_CT","IS_IT"), F.lit("Institutional Services"))
         .when(F.col(f"ar.{ar_seg_col}").isin("REGIS_FC","REGIS"), F.lit("Investment Services"))
         .when(F.col(f"ar.{ar_seg_col}") == F.lit("PWM"), F.lit("Private Wealth"))
         .otherwise(F.lit("Other"))
     )
)

w_tmp = (
    pw_join.select(
        F.to_date(F.col("ind.business_date")).alias("business_date"),
        F.col(f"ind.{ip_rcif_col}").cast("string").alias("rcif_number"),
        F.col(f"ind.{ip_id_col}").alias("ip_id"),
        F.upper(F.trim(F.col(f"ind.{ip_ibn_col}").cast("string"))).alias("cust_internet_banking_nbr"),
        business_group.alias("business_group"),
        F.col(f"ar.{ar_seg_col}").alias("seg_code"),
        F.col(f"ar.{ar_src_col}").alias("ar_source_system_code"),
        F.col(f"ar.{ar_arr_col}").alias("arrangement_id"),
    )
    .where(
        (F.col("seg_code").isin("IS_CT","IS_IT","REGIS_FC","REGIS","PWM")) |
        (F.col("business_group").isin("Private Wealth","Institutional Services","Investment Services"))
    )
)

w_agg = (
    w_tmp.groupBy("rcif_number")
    .agg(
        F.max("business_date").alias("business_date"),
        F.max("ip_id").alias("ip_id"),
        F.max("cust_internet_banking_nbr").alias("cust_internet_banking_nbr"),
        F.max("business_group").alias("business_group"),
        F.countDistinct("arrangement_id").alias("accts_cnt"),

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

wealth_rcif = (
    w_agg
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
    .select(
        "business_date","rcif_number","ip_id","cust_internet_banking_nbr",
        "business_group","division","accts_cnt"
    )
)

# ==========================================================
# 2) DIGITAL — repo-aligned (uses relt_ibn, month grain)
# ==========================================================
dbm_src = spark.table("dm_ib.digital_banking_master")
ibn_col = first_existing_col(dbm_src, "relt_ibn", "ibn")  # MUST use relt_ibn if available

dbm = (
    dbm_src.alias("dbm")
    .where((F.to_date(F.col("dbm.ods_business_dt")) >= start_dt) & (F.to_date(F.col("dbm.ods_business_dt")) <= end_dt))
    .select(
        F.trunc(F.to_date(F.col("dbm.ods_business_dt")), "MM").alias("month_dt"),
        F.to_date(F.col("dbm.ods_business_dt")).alias("ods_business_dt"),
        F.upper(F.trim(F.col(f"dbm.{ibn_col}").cast("string"))).alias("reltibn"),
        F.col("dbm.rcif_customer_nbr").cast("string").alias("rcif_customer_nbr"),
        F.to_date(F.col("dbm.olb_last_login_date")).alias("lst_login_olb"),
        F.to_date(F.col("dbm.mob_last_login_date")).alias("lst_login_mob"),
    )
    .where(F.col("reltibn").isNotNull() & (F.length(F.col("reltibn")) > 0))
)

dig_customer = (
    dbm.groupBy("month_dt", "reltibn", "rcif_customer_nbr")
       .agg(
           F.max("lst_login_olb").alias("lst_login_olb"),
           F.max("lst_login_mob").alias("lst_login_mob"),
           F.max("ods_business_dt").alias("ods_business_dt")
       )
)

digital = (
    dig_customer
    .withColumn(
        "mobile_active_flag",
        F.when(
            F.col("lst_login_mob").isNotNull() &
            (F.datediff(F.col("ods_business_dt"), F.col("lst_login_mob")) <= 90),
            F.lit("Mobile Active")
        ).otherwise(F.lit("Non Mobile Active"))
    )
    .withColumn(
        "mobile_flag",
        F.when(F.col("lst_login_mob").isNull(), F.lit("Non Mobile User")).otherwise(F.lit("Mobile User"))
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
        "olb_flag",
        F.when(F.col("lst_login_olb").isNull(), F.lit("Non OLB User")).otherwise(F.lit("OLB User"))
    )
    .withColumn(
        "digitally_active_flag",
        F.when(
            (
                F.col("lst_login_mob").isNotNull() &
                (F.datediff(F.col("ods_business_dt"), F.col("lst_login_mob")) <= 90)
            ) |
            (
                F.col("lst_login_olb").isNotNull() &
                (F.datediff(F.col("ods_business_dt"), F.col("lst_login_olb")) <= 90)
            ),
            F.lit("Digital Active")
        ).otherwise(F.lit("Non Digital Active"))
    )
    .withColumn("digital_flag", F.lit("Digital User"))
    .select(
        "month_dt","ods_business_dt","reltibn","rcif_customer_nbr",
        "mobile_active_flag","mobile_flag",
        "olb_active_flag","olb_flag",
        "digitally_active_flag","digital_flag"
    )
)

# ==========================================================
# 3) INVESTPATH (unchanged)
# ==========================================================
inv_ind = (
    INVOLVED_PARTY.alias("ind")
    .where(F.to_date(F.col("ind.business_date")) == F.lit(last_date))
    .where(F.col("ind.source_system_code") == F.lit("CF"))
    .where(F.coalesce(F.col(f"ind.{ip_deceased_col}"), F.lit("N")) == F.lit("N"))
)

inv_a2i = (
    A2I_REL.alias("a2i")
    .where(F.to_date(F.col("a2i.business_date")) == F.lit(last_date))
)

inv_ar = (
    ARRANGEMENT.alias("ar")
    .where(F.to_date(F.col("ar.business_date")) == F.lit(last_date))
    .where(F.col(f"ar.{ar_closed_col}") == F.lit(INV_CLOSED_ONLY))
    .where(F.col(f"ar.{ar_acct_type_col}") == F.lit(INV_ACCOUNT_TYPE_CODE))
    .where(F.col(f"ar.{ar_src_col}") == F.lit(INV_AR_SOURCE_SYSTEM))
)

inv_join = (
    inv_ind.join(
        inv_a2i,
        (F.col(f"ind.{ip_id_col}") == F.col(f"a2i.{a2i_ip_col}")) &
        (F.col("ind.business_date") == F.col("a2i.business_date")) &
        (F.col("ind.source_system_code") == F.col(f"a2i.{a2i_src_col}")),
        "inner"
    )
    .join(
        inv_ar,
        (F.col(f"a2i.{a2i_arr_col}") == F.col(f"ar.{ar_arr_col}")) &
        (F.col(f"a2i.{a2i_arr_src_col}") == F.col(f"ar.{ar_src_col}")) &
        (F.col("a2i.business_date") == F.col("ar.business_date")),
        "inner"
    )
)

acc_facts = (
    inv_join.select(
        F.col(f"ar.{ar_arr_col}").alias("act_cnt"),
        F.col(f"ar.{ar_balance_col}").alias("balance"),
        F.col(f"ar.{ar_open_col}").alias("open_date"),
        F.to_date(F.col("ar.business_date")).alias("business_date")
    )
)

w_acc = Window.partitionBy("act_cnt").orderBy(F.col("business_date").desc())
acc_facts_latest = acc_facts.withColumn("rn", F.row_number().over(w_acc)).where("rn=1").drop("rn")

investpath = (
    inv_join.select(
        F.col(f"ar.{ar_arr_col}").alias("act_cnt"),
        F.col(f"ind.{ip_rcif_col}").cast("string").alias("rcif_number"),
        F.col(f"a2i.{a2i_ip_col}").alias("ip_id")
    )
    .dropDuplicates(["act_cnt","ip_id"])
    .join(acc_facts_latest, "act_cnt", "left")
    .select("business_date","rcif_number","ip_id","act_cnt","balance","open_date")
)

# ==========================================================
# SANITY CHECKS — aligned to your expectations
# ==========================================================
print("WEALTH distinct RCIF (expect 269148):")
wealth_rcif.selectExpr("count(distinct rcif_number) as wealth_rcif").show(truncate=False)

print("WEALTH total accounts = SUM(accts_cnt) (expect ~303k):")
wealth_rcif.selectExpr("sum(accts_cnt) as wealth_accounts_total").show(truncate=False)

latest_month = digital.select(F.max("month_dt").alias("mx")).first()["mx"]
print("Latest month_dt in DIGITAL (within 07/01-12/31):", latest_month)

print("DIGITAL active distinct reltibn (latest month) expect 3428446:")
digital.filter(
    (F.col("month_dt") == F.lit(latest_month)) &
    (F.col("digitally_active_flag") == "Digital Active")
).selectExpr("count(distinct reltibn) as digital_active_ibn").show(truncate=False)

print("WEALTH digital RCIF (latest month, Digital Active) expect ~121933:")
wealth_digital_rcif = (
    wealth_rcif.select("rcif_number").dropDuplicates()
    .join(
        digital.filter(
            (F.col("month_dt") == F.lit(latest_month)) &
            (F.col("digitally_active_flag") == "Digital Active")
        ).select(F.col("rcif_customer_nbr").alias("rcif_number")).dropDuplicates(),
        on="rcif_number",
        how="inner"
    )
)
wealth_digital_rcif.selectExpr("count(distinct rcif_number) as wealth_digital_rcif").show(truncate=False)

print("INVESTPATH accounts=114, funded=108, customers(ip_id)=119:")
investpath.selectExpr(
    "count(distinct act_cnt) as invest_accounts",
    "count(distinct case when balance > 0 then act_cnt end) as invest_accounts_funded",
    "count(distinct ip_id) as invest_customers_ip"
).show(truncate=False)

# ==========================================================
# WRITE TABLES (3 only)
# ==========================================================
if DROP_AND_RECREATE:
    spark.sql(f"DROP TABLE IF EXISTS {WEALTH_FQN}")
    spark.sql(f"DROP TABLE IF EXISTS {DIG_FQN}")
    spark.sql(f"DROP TABLE IF EXISTS {INV_FQN}")

wealth_rcif.write.mode("overwrite").saveAsTable(WEALTH_FQN)
digital.write.mode("overwrite").saveAsTable(DIG_FQN)
investpath.write.mode("overwrite").saveAsTable(INV_FQN)

print("✅ Created 3 tables:")
print(WEALTH_FQN)
print(DIG_FQN)
print(INV_FQN)

# =============================================================================
# Wealth Insights — Customer & Account Tables
# Date Range  : 2025-09-01 to 2026-02-28
#
# KEY FIX: DBM grouped by MONTH (TRUNC(ods_business_dt,'MM'))
#   Active = datediff(MAX(ods_business_dt) per month, last_login) <= 90
#   Each month uses its OWN max snapshot date — matches original SQL exactly
#
# PC and BW excluded from source codes
# =============================================================================

from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .appName("WealthInsights")
    .enableHiveSupport()
    .getOrCreate()
)

final_db             = "dm_ib_dev"
final_table_customer = "wealth_Insights_Customer"
final_table_account  = "wealth_Insights_Account"

# Hardcoded from SQL validation — confirmed month-end dates
MONTH_END_DATES = [
    '2025-09-30',
    '2025-10-31',
    '2025-11-28',
    '2025-12-31',
    '2026-01-30',
    '2026-02-27'
]
MAX_IP_DATE = '2026-02-27'

WEALTH_SOURCE_CODES = [
    'DA','SV','CC','MG','LS','TM','LO','CM','CS','EL',
    'IC','MA','PF','PR','SD','TR','BI','RN',
    'IS_CT','IS_IT','IS_IF','PNB'
]
BANKING_SOURCE_CODES = [
    'DA','SV','CC','MG','LS','TM','LO',
    'CM','CS','EL','IC','MA','PF','PR','SD'
]
WEALTH_SEGMENT_CODES = ['IS_CT','IS_IT','REGIS_FC','REGIS','PWM']

# =============================================================================
# STEP 1 — DIGITAL BANKING MASTER grouped by MONTH
#
# Mirrors original SQL exactly:
#   GROUP BY TRUNC(ods_business_dt,'MM'), ibn
#   MAX(ods_business_dt) per month = snap_dt for that month's active flag
#   datediff(snap_dt, last_login) <= 90
#
# Join to wealth_customer on: trunc(business_date,'MM') == dbm_month AND ibn
# =============================================================================

dbm = (
    spark.table("dm_ib.digital_banking_master")
    .filter(
        (F.col("ods_business_dt") >= F.lit("2025-09-01")) &
        (F.col("ods_business_dt") <= F.lit("2026-02-28")) &
        F.col("ibn").isNotNull() &
        (F.col("ibn").cast("string") != "")
    )
    .groupBy(
        F.trunc(F.col("ods_business_dt"), "MM").alias("dbm_month"),
        F.col("ibn").cast("string").alias("ibn")
    )
    .agg(
        F.max("olb_last_login_date").alias("lst_login_olb"),
        F.max("mob_last_login_date").alias("lst_login_mob"),
        F.max("ods_business_dt").alias("snap_dt")   # per-month snap date
    )
    # Active = datediff(this month's snap_dt, last_login) <= 90
    .withColumn("olb_active_flag",
        F.when(
            F.col("lst_login_olb").isNotNull() &
            (F.datediff(F.col("snap_dt"), F.col("lst_login_olb")) <= 90),
            F.lit("OLB Active")
        ).otherwise(F.lit("Non OLB Active"))
    )
    .withColumn("mob_active_flag",
        F.when(
            F.col("lst_login_mob").isNotNull() &
            (F.datediff(F.col("snap_dt"), F.col("lst_login_mob")) <= 90),
            F.lit("Mobile Active")
        ).otherwise(F.lit("Non Mobile Active"))
    )
    .withColumn("digitally_active_flag",
        F.when(
            (F.col("lst_login_olb").isNotNull() &
             (F.datediff(F.col("snap_dt"), F.col("lst_login_olb")) <= 90)) |
            (F.col("lst_login_mob").isNotNull() &
             (F.datediff(F.col("snap_dt"), F.col("lst_login_mob")) <= 90)),
            F.lit("Digital Active")
        ).otherwise(F.lit("Non Digital Active"))
    )
    .withColumn("olb_enrolled",
        F.when(F.col("lst_login_olb").isNotNull(), F.lit("OLB Enrolled"))
         .otherwise(F.lit("Non OLB Enrolled"))
    )
    .withColumn("mob_enrolled",
        F.when(F.col("lst_login_mob").isNotNull(), F.lit("Mobile Enrolled"))
         .otherwise(F.lit("Non Mobile Enrolled"))
    )
    .withColumn("digital_enrolled",
        F.when(
            F.col("lst_login_olb").isNotNull() | F.col("lst_login_mob").isNotNull(),
            F.lit("Digital Enrolled")
        ).otherwise(F.lit("Non Digital Enrolled"))
    )
)

print("DBM monthly snapshot dates:")
dbm.groupBy("dbm_month").agg(F.max("snap_dt").alias("snap")).orderBy("dbm_month").show()

# =============================================================================
# STEP 2 — WEALTH SEGMENTATION per RCIF per MONTH  (daily tables)
# =============================================================================

ip_pw = (
    spark.table("eil.d_involved_party_h")
    .filter(
        F.col("business_date").isin(MONTH_END_DATES) &
        (F.col("source_system_code") == "CF") &
        (F.coalesce(F.col("deceased_ind"), F.lit("N")) == "N")
    )
)
a2i_pw = spark.table("eil.d_arrangement_to_involved_party_relationship_h")
ar_pw = (
    spark.table("eil.d_arrangement_h")
    .filter(
        F.col("source_system_code").isin(WEALTH_SOURCE_CODES) &
        (F.col("closed_ind") == "N")
    )
)

pw1 = (
    ip_pw
    .join(a2i_pw,
        (ip_pw["involved_party_id"]  == a2i_pw["involved_party_id"]) &
        (ip_pw["business_date"]      == a2i_pw["business_date"]) &
        (ip_pw["source_system_code"] == a2i_pw["source_system_code"]),
        "inner")
    .join(ar_pw,
        (a2i_pw["arrangement_id"]                 == ar_pw["arrangement_id"]) &
        (a2i_pw["arrangement_source_system_code"] == ar_pw["source_system_code"]) &
        (a2i_pw["business_date"]                  == ar_pw["business_date"]),
        "inner")
    .filter(
        F.when(ip_pw["private_client_code"].isin("039","539","339"), F.lit(1))
         .when(ip_pw["private_client_trust_code"].isin("239","739"), F.lit(1))
         .otherwise(
             F.when(ar_pw["business_service_segment_type_code"]
                    .isin(WEALTH_SEGMENT_CODES), F.lit(1))
              .otherwise(F.lit(0))
         ) == 1
    )
    .withColumn("business_group",
        F.when(ip_pw["private_client_code"].isin("039","539","339"),
               F.lit("Private Wealth"))
         .when(ip_pw["private_client_trust_code"].isin("239","739"),
               F.lit("Private Wealth"))
         .otherwise(
             F.when(ar_pw["business_service_segment_type_code"].isin("IS_CT","IS_IT"),
                    F.lit("Institutional Services"))
              .when(ar_pw["business_service_segment_type_code"].isin("REGIS_FC","REGIS"),
                    F.lit("Investment Services"))
              .when(ar_pw["business_service_segment_type_code"] == "PWM",
                    F.lit("Private Wealth"))
              .otherwise(F.coalesce(ar_pw["business_service_segment_type_code"],
                                    F.lit("Category???")))
         )
    )
    .groupBy(
        ip_pw["rcif_cust_nbr"].cast("string").alias("RCIF_NUMBER"),
        ip_pw["business_date"].cast(T.DateType()).alias("business_date"),
        F.col("business_group")
    )
    .agg(
        F.countDistinct(F.when(ar_pw["business_service_segment_type_code"] == "IS_CT",
            ar_pw["arrangement_id"])).alias("corporate_trust_cnt"),
        F.countDistinct(F.when(ar_pw["business_service_segment_type_code"] == "IS_IT",
            ar_pw["arrangement_id"])).alias("institutional_trust_cnt"),
        F.countDistinct(F.when(ar_pw["business_service_segment_type_code"].isin("REGIS_FC","REGIS"),
            ar_pw["arrangement_id"])).alias("investment_cnt"),
        F.countDistinct(F.when(ar_pw["business_service_segment_type_code"] == "REGIS",
            ar_pw["arrangement_id"])).alias("insurance_cnt"),
        F.countDistinct(F.when(ar_pw["business_service_segment_type_code"] == "PWM",
            ar_pw["arrangement_id"])).alias("pwm_cnt"),
        F.countDistinct(F.when(ar_pw["source_system_code"] == "TR",
            ar_pw["arrangement_id"])).alias("trust_cnt"),
        F.countDistinct(F.when(ar_pw["source_system_code"].isin(BANKING_SOURCE_CODES),
            ar_pw["arrangement_id"])).alias("banking_cnt"),
        F.countDistinct(ar_pw["arrangement_id"]).alias("wealth_accts_cnt")
    )
    .withColumn("division",
        F.when(F.col("business_group") == "Private Wealth",
            F.when((F.col("trust_cnt") > 0) & (F.col("banking_cnt") > 0),
                   F.lit("Banking & IM&T"))
             .when((F.col("investment_cnt") + F.col("trust_cnt") > 0) &
                   (F.col("banking_cnt") == 0), F.lit("Investments Only"))
             .otherwise(F.lit("Banking only"))
        )
        .when(F.col("business_group") == "Investment Services",
            F.when((F.col("investment_cnt") > 0) & (F.col("insurance_cnt") == 0),
                   F.lit("Investment"))
             .when((F.col("investment_cnt") == 0) & (F.col("insurance_cnt") > 0),
                   F.lit("Insurance"))
             .otherwise(F.lit("Insurance & Investment"))
        )
        .otherwise(
            F.when((F.col("corporate_trust_cnt") > 0) &
                   (F.col("institutional_trust_cnt") == 0), F.lit("Corporate Trust"))
             .when((F.col("corporate_trust_cnt") == 0) &
                   (F.col("institutional_trust_cnt") > 0), F.lit("Institutional Trust"))
             .when(F.col("pwm_cnt") > 0, F.lit("Banking only"))
             .otherwise(F.lit("Corporate & Institutional Trust"))
        )
    )
)

# =============================================================================
# STEP 3 — RCIF SNAPSHOTS per MONTH-END
# =============================================================================

ip_d = (
    spark.table("eil.d_involved_party_h")
    .filter(
        F.col("business_date").isin(MONTH_END_DATES) &
        (F.col("source_system_code") == "CF") &
        (F.coalesce(F.col("deceased_ind"), F.lit("N")) == "N")
    )
)
a2i_d  = spark.table("eil.d_arrangement_to_involved_party_relationship_h")
ar_d   = spark.table("eil.d_arrangement_h").filter(
    F.col("source_system_code").isin(WEALTH_SOURCE_CODES)
)
addr_d = spark.table("eil.d_involved_party_address_h")

# Build one row per involved_party per RCIF per month, then pick ONE per RCIF
# ordered by cust_ibn ASC (matches original PWRANK logic) BEFORE joining DBM
rc_raw = (
    ip_d
    .join(a2i_d,
        (ip_d["involved_party_id"]  == a2i_d["involved_party_id"]) &
        (ip_d["business_date"]      == a2i_d["business_date"]) &
        (ip_d["source_system_code"] == a2i_d["source_system_code"]),
        "inner")
    .join(ar_d,
        (a2i_d["arrangement_id"]                 == ar_d["arrangement_id"]) &
        (a2i_d["arrangement_source_system_code"] == ar_d["source_system_code"]) &
        (a2i_d["business_date"]                  == ar_d["business_date"]),
        "inner")
    .join(addr_d,
        (ip_d["involved_party_id"] == addr_d["involved_party_id"]) &
        (ip_d["business_date"]     == addr_d["business_date"]),
        "left")
    .select(
        ip_d["rcif_cust_nbr"].cast("string").alias("RCIF_NUMBER"),
        ip_d["business_date"].cast(T.DateType()).alias("business_date"),
        ip_d["involved_party_id"].alias("ip_id"),
        ip_d["cust_internet_banking_nbr"].alias("cust_ibn"),
        addr_d["state_name"]
    )
    .distinct()
)

# Dedup: ONE row per RCIF per month, ordered by cust_ibn ASC nulls last
# This must happen BEFORE DBM join so active flag uses the ONE correct ibn
w_rc = Window.partitionBy("RCIF_NUMBER", "business_date").orderBy(
    F.col("cust_ibn").asc_nulls_last()
)
rc = (
    rc_raw
    .withColumn("_rn", F.row_number().over(w_rc))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

# =============================================================================
# STEP 4 — INVESTPATH COUNT per RCIF per MONTH
# =============================================================================

ind_i = (
    spark.table("eil.d_involved_party_h")
    .filter(
        F.col("business_date").isin(MONTH_END_DATES) &
        (F.col("source_system_code") == "CF") &
        (F.coalesce(F.col("deceased_ind"), F.lit("N")) == "N")
    )
)
a2i_i = spark.table("eil.d_arrangement_to_involved_party_relationship_h")
ar_i  = spark.table("eil.d_arrangement_h").filter(
    (F.col("closed_ind") == "N") &
    (F.col("account_type_code") == "IP") &
    (F.col("source_system_code") == "RN")
)

ip_accts_cnt = (
    ind_i
    .join(a2i_i,
        (ind_i["involved_party_id"]  == a2i_i["involved_party_id"]) &
        (ind_i["business_date"]      == a2i_i["business_date"]) &
        (ind_i["source_system_code"] == a2i_i["source_system_code"]),
        "inner")
    .join(ar_i,
        (a2i_i["arrangement_id"]                 == ar_i["arrangement_id"]) &
        (a2i_i["arrangement_source_system_code"] == ar_i["source_system_code"]) &
        (a2i_i["business_date"]                  == ar_i["business_date"]),
        "inner")
    .groupBy(
        ind_i["rcif_cust_nbr"].cast("string").alias("RCIF_NUMBER"),
        ind_i["business_date"].cast(T.DateType()).alias("business_date")
    )
    .agg(F.countDistinct(ar_i["arrangement_id"]).alias("ip_accts_cnt"))
)

# =============================================================================
# STEP 5 — BUILD wealth_Insights_Customer
#
# DBM join: trunc(business_date,'MM') == dbm_month  AND  cust_ibn == ibn
# This gives each month its own per-month active flag — matches original SQL
# =============================================================================

wealth_customer = (
    rc
    .join(pw1,
        (rc["RCIF_NUMBER"]   == pw1["RCIF_NUMBER"]) &
        (rc["business_date"] == pw1["business_date"]),
        "inner")
    .drop(pw1["RCIF_NUMBER"]).drop(pw1["business_date"])
    .join(ip_accts_cnt,
        (rc["RCIF_NUMBER"]   == ip_accts_cnt["RCIF_NUMBER"]) &
        (rc["business_date"] == ip_accts_cnt["business_date"]),
        "left")
    .drop(ip_accts_cnt["RCIF_NUMBER"]).drop(ip_accts_cnt["business_date"])
    # rc is already ONE row per RCIF per month — DBM join on month+ibn will be 1:1
    .join(dbm,
        (F.trunc(rc["business_date"].cast("date"), "MM") == dbm["dbm_month"]) &
        (rc["cust_ibn"] == dbm["ibn"]),
        "left")
    .select(
        rc["RCIF_NUMBER"],
        rc["business_date"],
        F.col("state_name"),
        F.col("business_group"),
        F.col("division"),
        F.coalesce(F.col("wealth_accts_cnt"), F.lit(0)).alias("wealth_accts_cnt"),
        F.coalesce(F.col("ip_accts_cnt"),     F.lit(0)).alias("ip_accts_cnt"),
        rc["cust_ibn"].alias("ibn"),
        F.col("snap_dt").alias("dbm_snap_dt"),
        F.coalesce(F.col("digital_enrolled"),      F.lit("Non Digital Enrolled")).alias("digital_enrolled"),
        F.coalesce(F.col("olb_enrolled"),          F.lit("Non OLB Enrolled")).alias("olb_enrolled"),
        F.coalesce(F.col("mob_enrolled"),          F.lit("Non Mobile Enrolled")).alias("mob_enrolled"),
        F.coalesce(F.col("digitally_active_flag"), F.lit("Non Digital Active")).alias("digitally_active_flag"),
        F.coalesce(F.col("olb_active_flag"),       F.lit("Non OLB Active")).alias("olb_active_flag"),
        F.coalesce(F.col("mob_active_flag"),       F.lit("Non Mobile Active")).alias("mob_active_flag"),
        F.col("lst_login_olb"),
        F.col("lst_login_mob"),
        F.lit("Wealth").alias("fact_type")
    )
)

# =============================================================================
# STEP 6 — BUILD wealth_Insights_Account
# =============================================================================

ind_a = (
    spark.table("eil.d_involved_party_h")
    .filter(
        (F.col("business_date")      == MAX_IP_DATE) &
        (F.col("source_system_code") == "CF") &
        (F.coalesce(F.col("deceased_ind"), F.lit("N")) == "N")
    )
)
a2i_a = spark.table("eil.d_arrangement_to_involved_party_relationship_h")
ar_a  = spark.table("eil.d_arrangement_h").filter(
    (F.col("business_date")      == MAX_IP_DATE) &
    (F.col("closed_ind")         == "N") &
    (F.col("account_type_code")  == "IP") &
    (F.col("source_system_code") == "RN")
)

wealth_account = (
    ind_a
    .join(a2i_a,
        (ind_a["involved_party_id"]  == a2i_a["involved_party_id"]) &
        (ind_a["business_date"]      == a2i_a["business_date"]) &
        (ind_a["source_system_code"] == a2i_a["source_system_code"]),
        "inner")
    .join(ar_a,
        (a2i_a["arrangement_id"]                 == ar_a["arrangement_id"]) &
        (a2i_a["arrangement_source_system_code"] == ar_a["source_system_code"]) &
        (a2i_a["business_date"]                  == ar_a["business_date"]),
        "inner")
    .select(
        ind_a["rcif_cust_nbr"].cast("string").alias("RCIF_NUMBER"),
        ind_a["involved_party_id"].alias("ip_id"),
        ar_a["arrangement_id"].alias("ip_account_number"),
        ar_a["open_date"].alias("ip_open_date"),
        ar_a["current_balance_amt"].alias("ip_balance"),
        F.when(ar_a["current_balance_amt"] > 0,
               F.lit("Funded")).otherwise(F.lit("Not Funded")).alias("is_funded"),
        F.lit(MAX_IP_DATE).cast(T.DateType()).alias("business_date"),
        F.lit("Account").alias("fact_type")
    )
)

# =============================================================================
# STEP 7 — WRITE
# =============================================================================

wealth_customer.repartition(200).write \
    .mode("overwrite").option("overwriteSchema","true") \
    .saveAsTable("{}.{}".format(final_db, final_table_customer))

wealth_account.write \
    .mode("overwrite").option("overwriteSchema","true") \
    .saveAsTable("{}.{}".format(final_db, final_table_account))

# =============================================================================
# STEP 8 — VERIFY
# Expected OLB Active: ~64,420 (Sep) → ~65,800 (Feb)
# =============================================================================

final = spark.table("{}.{}".format(final_db, final_table_customer))
accts = spark.table("{}.{}".format(final_db, final_table_account))

print("\nWealth rows : {}".format(final.count()))
print("Account rows: {}".format(accts.count()))

print("\nDBM snap dates used per month (confirm per-month reference):")
final.filter(F.col("dbm_snap_dt").isNotNull()) \
     .groupBy("business_date", "dbm_snap_dt").count() \
     .orderBy("business_date").show(10)

print("\nOLB Active by month (target: ~64K-66K):")
final.groupBy("business_date","olb_active_flag") \
     .count().orderBy("business_date","olb_active_flag").show(20)

print("\nMobile Active by month:")
final.groupBy("business_date","mob_active_flag") \
     .count().orderBy("business_date","mob_active_flag").show(20)

print("\nDigital Active by month:")
final.groupBy("business_date","digitally_active_flag") \
     .count().orderBy("business_date","digitally_active_flag").show(20)

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark import SparkConf

# ── Configuration ─────────────────────────────────────────────────────────────
DEFAULT_DB = "dm_ib_dev"
EIL_DB     = "eil"
DMIB_DB    = "dm_ib"

# Pin to Feb 28 — original SQL ran in Feb so max(ods_business_dt) was Feb 28
END_DT = "2026-02-28"

conf = (
    SparkConf()
    .setAppName("wealth_insights")
    .set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .set("spark.sql.autoBroadcastJoinThreshold", "-1")
)
spark = (
    SparkSession.builder
    .config(conf=conf)
    .enableHiveSupport()
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ── Source Tables ─────────────────────────────────────────────────────────────
customer = spark.table(f"{EIL_DB}.d_involved_party_h")
bridge   = spark.table(f"{EIL_DB}.d_arrangement_to_involved_party_relationship_h")
account  = spark.table(f"{EIL_DB}.d_arrangement_h")
digital  = spark.table(f"{DMIB_DB}.digital_banking_master")
address  = spark.table(f"{EIL_DB}.d_involved_party_address_h")

VALID_SOURCE_CODES = [
    'DA','SV','CC','MG','LS','TM','LO','CM','CS',
    'EL','IC','MA','PF','PR','SD','TR','BI','RN',
    'IS_CT','IS_IT','PWM'
]
WEALTH_SOURCE_CODES = [
    'BI','RN','TR','DA','SV','CC','LS','MG','TM',
    'LO','CS','IC','MA','PF','PR','SD','CM','EL'
]
BANKING_SOURCE_CODES = [
    'DA','SV','CC','MG','LS','TM','LO','CM','CS',
    'EL','IC','MA','PF','PR','SD'
]

# =============================================================================
# CTE 1 — Dig_Customer
# Original: WHERE ods_business_dt = max(ods_business_dt)
# Pinned to <= END_DT so we get Feb snapshot not March
# =============================================================================
max_dig_dt = (
    digital
    .filter(F.col("ods_business_dt") <= END_DT)
    .agg(F.max("ods_business_dt"))
    .collect()[0][0]
)

dig_customer = (
    digital
    .filter(F.col("ods_business_dt") == max_dig_dt)
    .groupBy(
        F.col("ibn"),
        F.col("ods_business_dt")
    )
    .agg(
        F.max("olb_last_login_date").alias("lst_login_olb"),
        F.max("mob_last_login_date").alias("lst_login_mob")
    )
)

last_biz_date = customer.agg(F.max("business_date")).collect()[0][0]

# =============================================================================
# CTE 2 — INV (Investpath)
# =============================================================================
bridge_base = (
    bridge
    .filter(F.col("business_date") == last_biz_date)
    .select(
        F.col("involved_party_id").alias("a2i_ip_id"),
        F.col("business_date").alias("a2i_biz_dt"),
        F.col("source_system_code").alias("a2i_src_code"),
        F.col("arrangement_id").alias("a2i_arr_id"),
        F.col("arrangement_source_system_code").alias("a2i_arr_src_code")
    )
)

inv_df = (
    customer
    .filter(
        (F.col("business_date")      == last_biz_date) &
        (F.col("source_system_code") == "CF")          &
        (F.coalesce(F.col("deceased_ind"), F.lit("N")) == "N")
    )
    .select(
        F.col("involved_party_id").alias("c_ip_id"),
        F.col("business_date").alias("c_biz_dt"),
        F.col("source_system_code").alias("c_src_code"),
        F.col("rcif_cust_nbr").alias("rcif_nbr"),
        F.col("cust_internet_banking_nbr")
    )
    .join(bridge_base,
        (F.col("c_ip_id")  == F.col("a2i_ip_id")) &
        (F.col("c_biz_dt") == F.col("a2i_biz_dt")),
        "inner"
    )
    .join(
        account
        .filter(
            (F.col("closed_ind")        == "N") &
            (F.col("account_type_code") == "IP") &
            (F.col("source_system_code")== "RN")
        )
        .select(
            F.col("arrangement_id").alias("ar_arr_id"),
            F.col("source_system_code").alias("ar_src_code"),
            F.col("business_date").alias("ar_biz_dt"),
            F.col("current_balance_amt").alias("ip_balance"),
            F.col("open_date").alias("ip_open_date")
        ),
        (F.col("a2i_arr_id")       == F.col("ar_arr_id"))   &
        (F.col("a2i_arr_src_code") == F.col("ar_src_code")) &
        (F.col("a2i_biz_dt")       == F.col("ar_biz_dt")),
        "inner"
    )
    .select(
        F.col("rcif_nbr"),
        F.col("cust_internet_banking_nbr"),
        F.col("c_ip_id").alias("ip_id"),
        F.col("ip_balance"),
        F.col("ip_open_date"),
        F.col("a2i_arr_id").alias("ip_accounts_cnt")
    )
)

# =============================================================================
# CTE 3 — RC
# =============================================================================
rc = (
    customer
    .filter(
        (F.col("business_date")      == last_biz_date) &
        (F.col("source_system_code") == "CF")          &
        (F.coalesce(F.col("deceased_ind"), F.lit("N")) == "N")
    )
    .select(
        F.col("involved_party_id").alias("c_ip_id"),
        F.col("business_date").alias("c_biz_dt"),
        F.col("source_system_code").alias("c_src_code"),
        F.col("rcif_cust_nbr"),
        F.col("cust_internet_banking_nbr").alias("ibn")
    )
    .join(bridge_base,
        (F.col("c_ip_id")    == F.col("a2i_ip_id"))  &
        (F.col("c_biz_dt")   == F.col("a2i_biz_dt")) &
        (F.col("c_src_code") == F.col("a2i_src_code")),
        "inner"
    )
    .join(
        account
        .filter(F.col("source_system_code").isin(VALID_SOURCE_CODES))
        .select(
            F.col("arrangement_id").alias("ar_arr_id"),
            F.col("source_system_code").alias("ar_src_code"),
            F.col("business_date").alias("ar_biz_dt")
        ),
        (F.col("a2i_arr_id")       == F.col("ar_arr_id"))   &
        (F.col("a2i_arr_src_code") == F.col("ar_src_code")) &
        (F.col("a2i_biz_dt")       == F.col("ar_biz_dt")),
        "inner"
    )
    .join(
        address
        .filter(F.col("business_date") == last_biz_date)
        .select(
            F.col("involved_party_id").alias("addr_ip_id"),
            F.col("state_name")
        )
        .dropDuplicates(["addr_ip_id"]),
        F.col("c_ip_id") == F.col("addr_ip_id"),
        "left"
    )
    .groupBy(
        F.col("c_ip_id").alias("involved_party_id"),
        F.col("ibn"),
        F.col("state_name")
    )
    .agg(F.max(F.col("rcif_cust_nbr").cast("string")).alias("RCIF_NUMBER"))
)

# =============================================================================
# CTE 4 — RCIF_Dig (exact original flag logic)
# =============================================================================
dig_customer_sel = dig_customer.select(
    F.col("ibn").alias("dig_ibn"),
    F.col("lst_login_olb"),
    F.col("lst_login_mob")
)

rcif_dig = (
    rc
    .join(
        dig_customer_sel,
        rc["ibn"] == dig_customer_sel["dig_ibn"],
        "left"
    )
    .withColumn("Mobile_Active_Flag",
        F.when(F.datediff(F.lit(END_DT).cast("date"), F.col("lst_login_mob")) <= 90, "Mobile Active")
         .otherwise("Non Mobile Active")
    )
    .withColumn("Mobile_Flag",
        F.when(F.col("lst_login_mob").isNull(), "Non Mobile User").otherwise("Mobile User")
    )
    .withColumn("OLB_Active_Flag",
        F.when(F.datediff(F.lit(END_DT).cast("date"), F.col("lst_login_olb")) <= 90, "OLB Active")
         .otherwise("Non OLB Active")
    )
    .withColumn("OLB_Flag",
        F.when(F.col("lst_login_olb").isNull(), "Non OLB User").otherwise("OLB User")
    )
    .withColumn("Digitally_Active_Flag",
        F.when(
            (F.datediff(F.lit(END_DT).cast("date"), F.col("lst_login_mob")) <= 90) |
            (F.datediff(F.lit(END_DT).cast("date"), F.col("lst_login_olb")) <= 90),
            "Digital Active"
        ).otherwise("Non Digital Active")
    )
    .withColumn("Digital_flag",
        F.when(F.col("dig_ibn").isNull(), "Non Digital User").otherwise("Digital User")
    )
    .select(
        "RCIF_NUMBER", "involved_party_id",
        F.col("ibn").alias("cust_internet_banking_nbr"),
        "Mobile_Active_Flag", "Mobile_Flag",
        "OLB_Active_Flag", "OLB_Flag",
        "Digitally_Active_Flag", "Digital_flag"
    )
)

# =============================================================================
# CTE 5 — add_rcifs
# =============================================================================
six_mo_ago = F.add_months(F.current_date(), -6)
add_rcifs = (
    customer
    .filter(F.col("business_date") >= six_mo_ago)
    .select(F.col("rcif_cust_nbr").cast("string").alias("rcif_number"))
    .distinct()
    .union(
        digital
        .filter(F.col("ods_business_dt") >= six_mo_ago)
        .select(F.col("rcif_customer_nbr").cast("string").alias("rcif_number"))
    )
    .filter(F.col("rcif_number").isNotNull() & (F.col("rcif_number") != ""))
    .distinct()
)

# =============================================================================
# CTE 6 — PW1 (Wealth)
# =============================================================================
pw1 = (
    customer
    .filter(
        (F.col("business_date")      == last_biz_date) &
        (F.col("source_system_code") == "CF")          &
        (F.coalesce(F.col("deceased_ind"), F.lit("N")) == "N")
    )
    .select(
        F.col("involved_party_id").alias("c_ip_id"),
        F.col("business_date").alias("c_biz_dt"),
        F.col("source_system_code").alias("c_src_code"),
        F.col("rcif_cust_nbr").alias("RCIF_NUMBER"),
        F.col("cust_internet_banking_nbr").alias("ibn"),
        F.col("private_client_code"),
        F.col("private_client_trust_code")
    )
    .join(bridge_base,
        (F.col("c_ip_id")    == F.col("a2i_ip_id"))  &
        (F.col("c_biz_dt")   == F.col("a2i_biz_dt")) &
        (F.col("c_src_code") == F.col("a2i_src_code")),
        "inner"
    )
    .join(
        account
        .filter(
            F.col("source_system_code").isin(WEALTH_SOURCE_CODES) &
            (F.col("closed_ind") == "N")
        )
        .select(
            F.col("arrangement_id").alias("ar_arr_id"),
            F.col("source_system_code").alias("ar_src_code"),
            F.col("business_date").alias("ar_biz_dt"),
            F.col("business_service_segment_type_code")
        ),
        (F.col("a2i_arr_id")       == F.col("ar_arr_id"))   &
        (F.col("a2i_arr_src_code") == F.col("ar_src_code")) &
        (F.col("a2i_biz_dt")       == F.col("ar_biz_dt")),
        "inner"
    )
    .filter(
        F.when(F.col("private_client_code").isin('039','539','339'), F.lit(1))
         .when(F.col("private_client_trust_code").isin('239','739'), F.lit(1))
         .otherwise(
             F.when(F.col("business_service_segment_type_code")
                     .isin('IS_CT','IS_IT','REGIS_FC','REGIS','PWM'), F.lit(1))
              .otherwise(F.lit(0))
         ) == 1
    )
    .withColumn("Business_Group",
        F.when(F.col("private_client_code").isin('039','539','339'),                  "Private Wealth")
         .when(F.col("private_client_trust_code").isin('239','739'),                  "Private Wealth")
         .when(F.col("business_service_segment_type_code") == "IS_CT",               "Institutional Services")
         .when(F.col("business_service_segment_type_code") == "IS_IT",               "Institutional Services")
         .when(F.col("business_service_segment_type_code").isin('REGIS_FC','REGIS'), "Investment Services")
         .when(F.col("business_service_segment_type_code") == "PWM",                 "Private Wealth")
         .otherwise(F.concat(F.col("business_service_segment_type_code"), F.lit(" Category????")))
    )
    .groupBy(
        F.col("c_ip_id").alias("ip_id"),
        F.col("RCIF_NUMBER"),
        F.col("ibn"),
        F.col("business_service_segment_type_code"),
        F.col("private_client_code"),
        F.col("private_client_trust_code"),
        F.col("Business_Group")
    )
    .agg(
        F.countDistinct(F.when(F.col("business_service_segment_type_code") == "IS_CT",    F.col("ar_arr_id"))).alias("Corporate_Trust_Count"),
        F.countDistinct(F.when(F.col("business_service_segment_type_code") == "IS_IT",    F.col("ar_arr_id"))).alias("Institutional_Trust_Count"),
        F.countDistinct(F.when(F.col("business_service_segment_type_code") == "REGIS_FC", F.col("ar_arr_id"))).alias("Investment_Count"),
        F.countDistinct(F.when(F.col("business_service_segment_type_code") == "REGIS",    F.col("ar_arr_id"))).alias("Insurance_Count"),
        F.countDistinct(F.when(F.col("business_service_segment_type_code") == "PWM",      F.col("ar_arr_id"))).alias("PWM_Count"),
        F.countDistinct(F.when(F.col("business_service_segment_type_code") == "TR",       F.col("ar_arr_id"))).alias("Trust_Count"),
        F.countDistinct(F.when(F.col("ar_src_code").isin(BANKING_SOURCE_CODES),           F.col("ar_arr_id"))).alias("Banking_Count"),
        F.count(F.col("ar_arr_id")).alias("accts_cnt")
    )
    .withColumn("division",
        F.when(F.col("Business_Group") == "Private Wealth",
            F.when((F.col("Trust_Count") > 0) & (F.col("Banking_Count") > 0),                              "Banking & IM&T")
             .when((F.col("Investment_Count") + F.col("Trust_Count") > 0) & (F.col("Banking_Count") == 0), "Investments Only")
             .otherwise("Banking only")
        )
        .when(F.col("Business_Group") == "Investment Services",
            F.when((F.col("Investment_Count") > 0) & (F.col("Insurance_Count") == 0), "Investment")
             .when((F.col("Investment_Count") == 0) & (F.col("Insurance_Count") > 0), "Insurance")
             .otherwise("Insurance & Investment")
        )
        .otherwise(
            F.when((F.col("Corporate_Trust_Count") > 0) & (F.col("Institutional_Trust_Count") == 0), "Corporate Trust")
             .when((F.col("Corporate_Trust_Count") == 0) & (F.col("Institutional_Trust_Count") > 0), "Institutional Trust")
             .when(F.col("PWM_Count") > 0,                                                            "Banking only")
             .otherwise("Corporate & Institutional Trust")
        )
    )
)

# =============================================================================
# FINAL OUTPUT
# =============================================================================
bg_priority = (
    F.when(F.col("Business_Group") == "Private Wealth",        1)
     .when(F.col("Business_Group") == "Institutional Services",2)
     .when(F.col("Business_Group") == "Investment Services",   3)
     .otherwise(4)
)

# One row per RCIF — top Business_Group by priority
pw1_top = (
    pw1
    .withColumn("bg_rank", bg_priority)
    .withColumn("rn", F.row_number().over(
        Window.partitionBy("RCIF_NUMBER").orderBy("bg_rank")
    ))
    .filter(F.col("rn") == 1)
    .select(
        F.col("RCIF_NUMBER").alias("pw_rcif"),
        F.col("ip_id").alias("pw_ip_id"),
        "Business_Group", "division"
    )
)

# SUM accts_cnt across all segments per RCIF
pw1_accts = (
    pw1
    .groupBy("RCIF_NUMBER")
    .agg(F.sum("accts_cnt").alias("wealth_accts_cnt"))
)

pw1_per_rcif = (
    pw1_top
    .join(pw1_accts, pw1_top["pw_rcif"] == pw1_accts["RCIF_NUMBER"], "left")
    .select("pw_rcif", "pw_ip_id", "Business_Group", "division", "wealth_accts_cnt")
)

# ── wealth_insights_customer ──────────────────────────────────────────────────
wealth_insights_customer = (
    rcif_dig
    .join(add_rcifs,    rcif_dig["RCIF_NUMBER"] == add_rcifs["rcif_number"],  "inner")
    .join(pw1_per_rcif, rcif_dig["RCIF_NUMBER"] == pw1_per_rcif["pw_rcif"],   "left")
    .withColumn("fact_type",
        F.when(
            F.col("Business_Group").isNotNull() & (F.col("Digital_flag") == "Digital User"),
            "Both"
        )
        .when(F.col("Business_Group").isNotNull(), "Wealth")
        .when(F.col("Digital_flag") == "Digital User", "Digital")
        .otherwise("Neither")
    )
    .select(
        rcif_dig["RCIF_NUMBER"].alias("rcif_number"),
        F.col("cust_internet_banking_nbr"),
        F.col("pw_ip_id").alias("ip_id"),
        F.col("Business_Group").alias("business_group"),
        F.col("division"),
        F.col("wealth_accts_cnt"),
        F.col("Mobile_Flag").alias("mobile_flag"),
        F.col("Mobile_Active_Flag").alias("mobile_active_flag"),
        F.col("OLB_Flag").alias("olb_flag"),
        F.col("OLB_Active_Flag").alias("olb_active_flag"),
        F.col("Digital_flag").alias("digital_flag"),
        F.col("Digitally_Active_Flag").alias("digital_active_flag"),
        F.col("fact_type")
    )
    .distinct()
)

# ── wealth_insights_account ───────────────────────────────────────────────────
wealth_insights_account = (
    inv_df
    .join(
        rc.select(
            F.col("involved_party_id").alias("s_ip_id"),
            "state_name"
        ).dropDuplicates(["s_ip_id"]),
        inv_df["ip_id"] == F.col("s_ip_id"),
        "left"
    )
    .select(
        F.col("rcif_nbr").alias("rcif_number"),
        F.col("cust_internet_banking_nbr"),
        inv_df["ip_id"],
        F.col("ip_balance"),
        F.col("ip_open_date"),
        F.col("ip_accounts_cnt"),
        F.col("state_name")
    )
)

# ── Write ─────────────────────────────────────────────────────────────────────
wealth_insights_customer.write.mode("overwrite").saveAsTable(f"{DEFAULT_DB}.wealth_insights_customer")
wealth_insights_account.write.mode("overwrite").saveAsTable(f"{DEFAULT_DB}.wealth_insights_account")

print(f"✅ wealth_insights_customer: {wealth_insights_customer.count():,} rows")
print(f"✅ wealth_insights_account:  {wealth_insights_account.count():,} rows")

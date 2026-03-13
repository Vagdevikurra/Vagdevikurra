from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.window import Window
from pyspark import SparkConf

# ── Configuration ─────────────────────────────────────────────────────────────
DEFAULT_DB = "dm_ib_dev"
EIL_DB     = "eil"
DMIB_DB    = "dm_ib"

START_DT = "2025-09-01"
END_DT   = "2026-02-28"

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

# =============================================================================
# SOURCE TABLES
# NOTE: D_ARRANGEMENT_H has NO involved_party_id.
#       Bridge table is REQUIRED to link customer → account.
# =============================================================================
customer = spark.table(f"{EIL_DB}.d_involved_party_h")
bridge   = spark.table(f"{EIL_DB}.d_arrangement_to_involved_party_relationship_h")
account  = spark.table(f"{EIL_DB}.d_arrangement_h")
digital  = spark.table(f"{DMIB_DB}.digital_banking_master")

# ── Confirmed D_INVOLVED_PARTY_H columns used: ────────────────────────────────
#   INVOLVED_PARTY_ID, SOURCE_SYSTEM_CODE, BUSINESS_DATE,
#   RCIF_CUST_NBR, CUST_INTERNET_BANKING_NBR (= ibn),
#   INVOLVED_PARTY_NAME, INVOLVED_PARTY_TAX_ID_NBR,
#   BIRTH_DATE, DECEASED_IND,
#   PRIVATE_CLIENT_CODE, PRIVATE_CLIENT_TRUST_CODE

# ── Confirmed D_ARRANGEMENT_H columns used: ───────────────────────────────────
#   ARRANGEMENT_ID, SOURCE_SYSTEM_CODE, BUSINESS_DATE,
#   CLOSED_IND, CURRENT_BALANCE_AMT, OPEN_DATE,
#   ACCOUNT_TYPE_CODE, BUSINESS_SERVICE_SEGMENT_TYPE_CODE

# ── Valid Source Codes (PC and BW removed per requirement) ────────────────────
VALID_SOURCE_CODES = [
    'DA','SV','CC','MG','LS','TM','LO',
    'CM','CS','EL','IC','MA','PF','PR',
    'SD','TR','BI','RN','IS_CT','IS_IT','PWM'
]

WEALTH_SOURCE_CODES = [
    'BI','RN','TR','DA','SV','CC','LS',
    'MG','TM','LO','CS','IC','MA','PF',
    'PR','SD','CM','EL'
]

BANKING_SOURCE_CODES = [
    'DA','SV','CC','MG','LS','TM','LO',
    'CM','CS','EL','IC','MA','PF','PR','SD'
]

# =============================================================================
# SHARED: Latest business dates
# =============================================================================
last_biz_date = customer.agg(F.max("business_date")).collect()[0][0]
max_ods_dt    = digital.agg(F.max("ods_business_dt")).collect()[0][0]

# =============================================================================
# QUERY 1 — Digital Activity (Dig_Customer)
# =============================================================================
dig_customer = (
    digital
    .filter(F.col("ods_business_dt") == max_ods_dt)
    .groupBy(
        F.col("ibn"),
        F.col("ods_business_dt")
    )
    .agg(
        F.max("olb_last_login_date").alias("lst_login_olb"),
        F.max("mob_last_login_date").alias("lst_login_mob")
    )
)

# =============================================================================
# SHARED BASE VIEWS
# Pre-rename ALL columns before any join — prevents AnalysisException
# =============================================================================

# ── Customer base ─────────────────────────────────────────────────────────────
cust_base = (
    customer
    .filter(
        (F.col("business_date")      == last_biz_date) &
        (F.col("source_system_code") == "CF")          &
        (F.coalesce(F.col("deceased_ind"), F.lit("N")) == "N")
    )
    .select(
        F.col("involved_party_id").alias("ind_ip_id"),
        F.col("business_date").alias("ind_biz_dt"),
        F.col("rcif_cust_nbr").alias("RCIF_NUMBER"),
        F.col("cust_internet_banking_nbr").alias("ibn"),
        F.col("involved_party_name"),
        F.col("involved_party_tax_id_nbr"),
        F.col("birth_date"),
        F.col("private_client_code"),
        F.col("private_client_trust_code")
    )
)

# ── Bridge base ───────────────────────────────────────────────────────────────
# Bridge SOURCE_SYSTEM_CODE = relationship record source (NOT customer source)
# Join keys: involved_party_id + business_date + arrangement keys only
bridge_base = (
    bridge
    .filter(F.col("business_date") == last_biz_date)
    .select(
        F.col("involved_party_id").alias("a2i_ip_id"),
        F.col("business_date").alias("a2i_biz_dt"),
        F.col("arrangement_id").alias("a2i_arr_id"),
        F.col("arrangement_source_system_code").alias("a2i_arr_src_code")
    )
)

# ── Account base ──────────────────────────────────────────────────────────────
acct_base = (
    account
    .filter(F.col("closed_ind") == "N")
    .select(
        F.col("arrangement_id").alias("ar_arr_id"),
        F.col("source_system_code").alias("ar_src_code"),
        F.col("business_date").alias("ar_biz_dt"),
        F.col("current_balance_amt").alias("balance"),
        F.col("open_date"),
        F.col("account_type_code"),
        F.col("business_service_segment_type_code"),
        F.col("closed_ind")
    )
)

# =============================================================================
# QUERY 2 — Investpath: customer → bridge → account (IP/RN only)
# =============================================================================
acct_inv = acct_base.filter(
    (F.col("account_type_code") == "IP") &
    (F.col("ar_src_code")       == "RN")
)

inv_df = (
    cust_base
    .join(bridge_base,
        (F.col("ind_ip_id")  == F.col("a2i_ip_id")) &
        (F.col("ind_biz_dt") == F.col("a2i_biz_dt")),
        "inner"
    )
    .join(acct_inv,
        (F.col("a2i_arr_id")       == F.col("ar_arr_id"))   &
        (F.col("a2i_arr_src_code") == F.col("ar_src_code")) &
        (F.col("a2i_biz_dt")       == F.col("ar_biz_dt")),
        "inner"
    )
    .select(
        F.col("RCIF_NUMBER").alias("rcif_nbr"),
        F.col("ind_ip_id").alias("ip_id"),
        F.col("balance"),
        F.col("open_date"),
        F.col("ar_arr_id").alias("arrangement_id")
    )
)

# =============================================================================
# QUERY 3 — RCIF_Dig: customer → bridge → account (all valid source codes)
# =============================================================================
acct_rc = acct_base.filter(F.col("ar_src_code").isin(VALID_SOURCE_CODES))

rc = (
    cust_base
    .join(bridge_base,
        (F.col("ind_ip_id")  == F.col("a2i_ip_id")) &
        (F.col("ind_biz_dt") == F.col("a2i_biz_dt")),
        "inner"
    )
    .join(acct_rc,
        (F.col("a2i_arr_id")       == F.col("ar_arr_id"))   &
        (F.col("a2i_arr_src_code") == F.col("ar_src_code")) &
        (F.col("a2i_biz_dt")       == F.col("ar_biz_dt")),
        "inner"
    )
    .groupBy(
        F.col("ind_ip_id").alias("involved_party_id"),
        F.col("ibn"),
        F.col("involved_party_name"),
        F.col("involved_party_tax_id_nbr"),
        F.col("birth_date")
    )
    .agg(F.max("RCIF_NUMBER").alias("RCIF_NUMBER"))
)

rcif_dig = (
    rc
    .join(dig_customer, rc["ibn"] == dig_customer["ibn"], "left")
    .drop(dig_customer["ibn"])
    # Customer Generation
    .withColumn("CUSTOMER_GENERATION",
        F.when((F.col("birth_date") >= "1900-01-01") & (F.col("birth_date") <= "1924-12-31"), "GI Generation (1900-1924)")
         .when((F.col("birth_date") >= "1925-01-01") & (F.col("birth_date") <= "1945-12-31"), "Traditionalist (1925-1945)")
         .when((F.col("birth_date") >= "1946-01-01") & (F.col("birth_date") <= "1964-12-31"), "Baby Boomer (1946-1964)")
         .when((F.col("birth_date") >= "1965-01-01") & (F.col("birth_date") <= "1980-12-31"), "Gen X (1965-1980)")
         .when((F.col("birth_date") >= "1981-01-01") & (F.col("birth_date") <= "1996-12-31"), "Millennial (1981-1996)")
         .when( F.col("birth_date") >= "1997-01-01",                                          "Centennial (1997-???)")
         .otherwise("Unknown")
    )
    # Digital flags
    .withColumn("Mobile_Active_Flag",
        F.when(F.datediff(F.col("ods_business_dt"), F.col("lst_login_mob")) <= 90, "Mobile Active")
         .otherwise("Non Mobile Active")
    )
    .withColumn("Mobile_Flag",
        F.when(F.col("lst_login_mob").isNull(), "Non Mobile User").otherwise("Mobile User")
    )
    .withColumn("OLB_Active_Flag",
        F.when(F.datediff(F.col("ods_business_dt"), F.col("lst_login_olb")) <= 90, "OLB Active")
         .otherwise("Non OLB Active")
    )
    .withColumn("OLB_Flag",
        F.when(F.col("lst_login_olb").isNull(), "Non OLB User").otherwise("OLB User")
    )
    .withColumn("Digitally_Active_Flag",
        F.when(
            (F.datediff(F.col("ods_business_dt"), F.col("lst_login_mob")) <= 90) |
            (F.datediff(F.col("ods_business_dt"), F.col("lst_login_olb")) <= 90),
            "Digital Active"
        ).otherwise("Non Digital Active")
    )
    .withColumn("Digital_flag",
        F.when(F.col("ibn").isNull(), "Non Digital User").otherwise("Digital User")
    )
)

# =============================================================================
# QUERY 4 — RCIF Number List (last 6 months union)
# =============================================================================
six_mo_ago = F.add_months(F.current_date(), -6)

rcif_from_customer = (
    customer
    .filter(F.col("business_date") >= six_mo_ago)
    .select(F.col("rcif_cust_nbr").cast("string").alias("rcif_number"))
    .distinct()
)

rcif_from_digital = (
    digital
    .filter(F.col("ods_business_dt") >= six_mo_ago)
    .select(F.col("rcif_customer_nbr").cast("string").alias("rcif_number"))
)

add_rcifs = (
    rcif_from_customer
    .union(rcif_from_digital)
    .filter(
        F.col("rcif_number").isNotNull() &
        (F.col("rcif_number") != "")
    )
    .distinct()
)

# =============================================================================
# QUERY 5 — Wealth: Business Group + Division + Account Counts
# =============================================================================
acct_pw = acct_base.filter(F.col("ar_src_code").isin(WEALTH_SOURCE_CODES))

pw1 = (
    cust_base
    .join(bridge_base,
        (F.col("ind_ip_id")  == F.col("a2i_ip_id")) &
        (F.col("ind_biz_dt") == F.col("a2i_biz_dt")),
        "inner"
    )
    .join(acct_pw,
        (F.col("a2i_arr_id")       == F.col("ar_arr_id"))   &
        (F.col("a2i_arr_src_code") == F.col("ar_src_code")) &
        (F.col("a2i_biz_dt")       == F.col("ar_biz_dt")),
        "inner"
    )
    # Wealth eligibility filter
    .filter(
        F.when(F.col("private_client_code").isin('039','539','339'),       F.lit(1))
         .when(F.col("private_client_trust_code").isin('239','739'),       F.lit(1))
         .otherwise(
             F.when(
                 F.col("business_service_segment_type_code")
                  .isin('IS_CT','IS_IT','REGIS_FC','REGIS','PWM'),         F.lit(1)
             ).otherwise(F.lit(0))
         ) == 1
    )
    .withColumn("Business_Group",
        F.when(F.col("private_client_code").isin('039','539','339'),                   "Private Wealth")
         .when(F.col("private_client_trust_code").isin('239','739'),                   "Private Wealth")
         .when(F.col("business_service_segment_type_code") == "IS_CT",                "Institutional Services")
         .when(F.col("business_service_segment_type_code") == "IS_IT",                "Institutional Services")
         .when(F.col("business_service_segment_type_code").isin('REGIS_FC','REGIS'),  "Investment Services")
         .when(F.col("business_service_segment_type_code") == "PWM",                  "Private Wealth")
         .otherwise(F.concat(F.col("business_service_segment_type_code"), F.lit(" Category????")))
    )
    .groupBy(
        F.col("ind_ip_id").alias("ip_id"),
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
)

wealth_df = (
    pw1
    .withColumn("division",
        F.when(F.col("Business_Group") == "Private Wealth",
            F.when((F.col("Trust_Count") > 0) & (F.col("Banking_Count") > 0),                                        "Banking & IM&T")
             .when((F.col("Investment_Count") + F.col("Trust_Count") > 0) & (F.col("Banking_Count") == 0),           "Investments Only")
             .otherwise("Banking only")
        )
        .when(F.col("Business_Group") == "Investment Services",
            F.when((F.col("Investment_Count") > 0) & (F.col("Insurance_Count") == 0),                                "Investment")
             .when((F.col("Investment_Count") == 0) & (F.col("Insurance_Count") > 0),                                "Insurance")
             .otherwise("Insurance & Investment")
        )
        .otherwise(
            F.when((F.col("Corporate_Trust_Count") > 0) & (F.col("Institutional_Trust_Count") == 0),                 "Corporate Trust")
             .when((F.col("Corporate_Trust_Count") == 0) & (F.col("Institutional_Trust_Count") > 0),                 "Institutional Trust")
             .when(F.col("PWM_Count") > 0,                                                                            "Banking only")
             .otherwise("Corporate & Institutional Trust")
        )
    )
)

# =============================================================================
# FINAL OUTPUT — wealth_insights_customer & wealth_insights_account
# =============================================================================

# ── wealth_insights_customer ──────────────────────────────────────────────────
wealth_df_cust = (
    wealth_df
    .select(
        F.col("RCIF_NUMBER").alias("w_rcif"),
        "Business_Group",
        "division"
    )
    .distinct()
)

wealth_insights_customer = (
    rcif_dig
    .join(add_rcifs,      rcif_dig["RCIF_NUMBER"] == add_rcifs["rcif_number"],    "inner")
    .join(wealth_df_cust, rcif_dig["RCIF_NUMBER"] == wealth_df_cust["w_rcif"],    "left")
    .select(
        rcif_dig["RCIF_NUMBER"],
        F.col("involved_party_id"),
        rcif_dig["ibn"],
        F.col("involved_party_name"),
        F.col("involved_party_tax_id_nbr"),
        F.col("birth_date"),
        F.col("CUSTOMER_GENERATION"),
        F.col("Mobile_Active_Flag"),
        F.col("Mobile_Flag"),
        F.col("OLB_Active_Flag"),
        F.col("OLB_Flag"),
        F.col("Digitally_Active_Flag"),
        F.col("Digital_flag"),
        F.col("Business_Group"),
        F.col("division")
    )
    .distinct()
)

# ── wealth_insights_account ───────────────────────────────────────────────────
wealth_df_acct = wealth_df.select(
    F.col("ip_id").alias("w_ip_id"),
    "Business_Group", "division", "accts_cnt",
    "Corporate_Trust_Count", "Institutional_Trust_Count",
    "Investment_Count", "Insurance_Count",
    "PWM_Count", "Trust_Count", "Banking_Count"
)

wealth_insights_account = (
    inv_df
    .join(wealth_df_acct, inv_df["ip_id"] == wealth_df_acct["w_ip_id"], "left")
    .select(
        F.col("rcif_nbr").alias("RCIF_NUMBER"),
        inv_df["ip_id"],
        F.col("balance"),
        F.col("open_date"),
        F.col("arrangement_id"),
        F.col("Business_Group"),
        F.col("division"),
        F.col("accts_cnt"),
        F.col("Corporate_Trust_Count"),
        F.col("Institutional_Trust_Count"),
        F.col("Investment_Count"),
        F.col("Insurance_Count"),
        F.col("PWM_Count"),
        F.col("Trust_Count"),
        F.col("Banking_Count")
    )
)

# ── Write Output Tables ───────────────────────────────────────────────────────
wealth_insights_customer.write.mode("overwrite").saveAsTable(f"{DEFAULT_DB}.wealth_insights_customer")
wealth_insights_account.write.mode("overwrite").saveAsTable(f"{DEFAULT_DB}.wealth_insights_account")

print("✅ wealth_insights_customer written successfully.")
print("✅ wealth_insights_account written successfully.")

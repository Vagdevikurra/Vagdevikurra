from pyspark.sql import SparkSession, functions as F
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

# ── Source Tables ─────────────────────────────────────────────────────────────
customer = spark.table(f"{EIL_DB}.d_involved_party_h")
bridge   = spark.table(f"{EIL_DB}.d_arrangement_to_involved_party_relationship_h")
account  = spark.table(f"{EIL_DB}.d_arrangement_h")
digital  = spark.table(f"{DMIB_DB}.digital_banking_master")

# ── Source Code Lists (BW and PC removed) ────────────────────────────────────
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
# STEP 1 — Dig_Customer CTE (exact original logic)
# Original: group by relt_ibn (= ibn), ods_business_dt at max date
# =============================================================================
max_ods_dt = digital.agg(F.max("ods_business_dt")).collect()[0][0]

dig_customer = (
    digital
    .filter(F.col("ods_business_dt") == max_ods_dt)
    .groupBy(
        F.col("ibn").alias("reltibn"),
        F.col("ods_business_dt")
    )
    .agg(
        F.max("olb_last_login_date").alias("lst_login_olb"),
        F.max("mob_last_login_date").alias("lst_login_mob")
    )
)

# =============================================================================
# STEP 2 — Latest business date
# =============================================================================
last_biz_date = customer.agg(F.max("business_date")).collect()[0][0]

# =============================================================================
# STEP 3 — INV CTE (exact original Query 2 - InvestPath)
# Original joins: customer → bridge (ip_id + biz_date + src_code) →
#                 account (arr_id + arr_src + biz_date + closed=N + type=IP + src=RN)
# Filter: source_system_code='CF', not deceased
# =============================================================================
cust_inv = (
    customer
    .filter(
        (F.col("business_date")      == last_biz_date) &
        (F.col("source_system_code") == "CF")          &
        (F.coalesce(F.col("deceased_ind"), F.lit("N")) == "N")
    )
    .select(
        F.col("involved_party_id").alias("ind_ip_id"),
        F.col("business_date").alias("ind_biz_dt"),
        F.col("source_system_code").alias("ind_src_code"),
        F.col("rcif_cust_nbr").alias("rcif_nbr")
    )
)

bridge_inv = (
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

acct_inv = (
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
        F.col("current_balance_amt").alias("balance"),
        F.col("open_date")
    )
)

# INV = customer → bridge → account (matching original join keys exactly)
inv_df = (
    cust_inv
    .join(bridge_inv,
        (F.col("ind_ip_id")   == F.col("a2i_ip_id"))   &
        (F.col("ind_biz_dt")  == F.col("a2i_biz_dt"))  &
        (F.col("ind_src_code")== F.col("a2i_src_code")),
        "inner"
    )
    .join(acct_inv,
        (F.col("a2i_arr_id")       == F.col("ar_arr_id"))   &
        (F.col("a2i_arr_src_code") == F.col("ar_src_code")) &
        (F.col("a2i_biz_dt")       == F.col("ar_biz_dt")),
        "inner"
    )
    .select(
        F.col("rcif_nbr"),
        F.col("ind_ip_id").alias("ip_id"),
        F.col("balance"),
        F.col("open_date"),
        F.col("a2i_arr_id").alias("Accounts")   # act_cnt in original
    )
)

# =============================================================================
# STEP 4 — RC CTE (exact original Query 3 - RCIF part)
# customer → bridge (ip_id + biz_date + src_code) →
# account (arr_id + arr_src + biz_date) filtered to VALID_SOURCE_CODES
# Group by customer fields, max(rcif_cust_nbr) as RCIF_NUMBER
# =============================================================================
bridge_rc = (
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

acct_rc = (
    account
    .filter(F.col("source_system_code").isin(VALID_SOURCE_CODES))
    .select(
        F.col("arrangement_id").alias("ar_arr_id"),
        F.col("source_system_code").alias("ar_src_code"),
        F.col("business_date").alias("ar_biz_dt")
    )
)

cust_rc = (
    customer
    .filter(
        (F.col("business_date")      == last_biz_date) &
        (F.col("source_system_code") == "CF")          &
        (F.coalesce(F.col("deceased_ind"), F.lit("N")) == "N")
    )
    .select(
        F.col("involved_party_id").alias("ind_ip_id"),
        F.col("business_date").alias("ind_biz_dt"),
        F.col("source_system_code").alias("ind_src_code"),
        F.col("rcif_cust_nbr"),
        F.col("cust_internet_banking_nbr").alias("ibn"),
        F.col("involved_party_name"),
        F.col("involved_party_tax_id_nbr"),
        F.col("birth_date")
    )
)

rc = (
    cust_rc
    .join(bridge_rc,
        (F.col("ind_ip_id")   == F.col("a2i_ip_id"))   &
        (F.col("ind_biz_dt")  == F.col("a2i_biz_dt"))  &
        (F.col("ind_src_code")== F.col("a2i_src_code")),
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
    .agg(
        F.max(F.col("rcif_cust_nbr").cast("string")).alias("RCIF_NUMBER")
    )
)

# =============================================================================
# STEP 5 — RCIF_Dig (exact original Query 3 final select)
# rc LEFT JOIN Dig_Customer on rc.ibn = dig_customer.reltibn
# ALL flags use dig_customer columns (c.*) — null when not matched
# Digital_flag checks c.reltibn (from dig_customer), NOT customer.ibn
# =============================================================================
rcif_dig = (
    rc
    .join(
        dig_customer,
        rc["ibn"] == dig_customer["reltibn"],
        "left"
    )
    # Generation (exact original CASE)
    .withColumn("CUSTOMER_GENERATION",
        F.when((F.col("birth_date") >= "1900-01-01") & (F.col("birth_date") <= "1924-12-31"), "GI Generation (1900-1924)")
         .when((F.col("birth_date") >= "1925-01-01") & (F.col("birth_date") <= "1945-12-31"), "Traditionalist (1925-1945)")
         .when((F.col("birth_date") >= "1946-01-01") & (F.col("birth_date") <= "1964-12-31"), "Baby Boomer (1946-1964)")
         .when((F.col("birth_date") >= "1965-01-01") & (F.col("birth_date") <= "1980-12-31"), "Gen X (1965-1980)")
         .when((F.col("birth_date") >= "1981-01-01") & (F.col("birth_date") <= "1996-12-31"), "Millennial (1981-1996)")
         .when( F.col("birth_date") >= "1997-01-01",                                          "Centennial (1997-???)")
         .otherwise("Unknown")
    )
    # Mobile Active: uses c.ods_business_dt & c.lst_login_mob (null if not in dig_customer)
    .withColumn("Mobile_Active_Flag",
        F.when(F.datediff(F.col("ods_business_dt"), F.col("lst_login_mob")) <= 90, "Mobile Active")
         .otherwise("Non Mobile Active")
    )
    # Mobile User: c.lst_login_mob null → Non Mobile User
    .withColumn("Mobile_Flag",
        F.when(F.col("lst_login_mob").isNull(), "Non Mobile User")
         .otherwise("Mobile User")
    )
    # OLB Active: uses c.ods_business_dt & c.lst_login_olb
    .withColumn("OLB_Active_Flag",
        F.when(F.datediff(F.col("ods_business_dt"), F.col("lst_login_olb")) <= 90, "OLB Active")
         .otherwise("Non OLB Active")
    )
    # OLB User: c.lst_login_olb null → Non OLB User
    .withColumn("OLB_Flag",
        F.when(F.col("lst_login_olb").isNull(), "Non OLB User")
         .otherwise("OLB User")
    )
    # Digitally Active: either login within 90 days
    .withColumn("Digitally_Active_Flag",
        F.when(
            (F.datediff(F.col("ods_business_dt"), F.col("lst_login_mob")) <= 90) |
            (F.datediff(F.col("ods_business_dt"), F.col("lst_login_olb")) <= 90),
            "Digital Active"
        ).otherwise("Non Digital Active")
    )
    # Digital User: checks c.reltibn (dig_customer column), null = not in dig_customer
    .withColumn("Digital_flag",
        F.when(F.col("reltibn").isNull(), "Non Digital User")
         .otherwise("Digital User")
    )
)

# =============================================================================
# STEP 6 — add_rcifs (exact original Query 4)
# Union of RCIF from customer (last 6 months) + digital (last 6 months)
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
# STEP 7 — PW1 (exact original Query 5 - Wealth)
# customer → bridge (ip_id + biz_date + src_code) →
# account (WEALTH_SOURCE_CODES, closed=N)
# Wealth eligibility filter (private_client_code OR trust_code OR segment codes)
# Group by customer + segment → account counts
# KEEP one row per customer+segment (original logic — Business_Group per segment)
# =============================================================================
bridge_pw = (
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

acct_pw = (
    account
    .filter(
        F.col("source_system_code").isin(WEALTH_SOURCE_CODES) &
        (F.col("closed_ind") == "N")
    )
    .select(
        F.col("arrangement_id").alias("ar_arr_id"),
        F.col("source_system_code").alias("ar_src_code"),
        F.col("business_date").alias("ar_biz_dt"),
        F.col("business_service_segment_type_code"),
        F.col("current_balance_amt").alias("balance")
    )
)

cust_pw = (
    customer
    .filter(
        (F.col("business_date")      == last_biz_date) &
        (F.col("source_system_code") == "CF")          &
        (F.coalesce(F.col("deceased_ind"), F.lit("N")) == "N")
    )
    .select(
        F.col("involved_party_id").alias("ind_ip_id"),
        F.col("business_date").alias("ind_biz_dt"),
        F.col("source_system_code").alias("ind_src_code"),
        F.col("rcif_cust_nbr").alias("RCIF_NUMBER"),
        F.col("cust_internet_banking_nbr").alias("ibn"),
        F.col("private_client_code"),
        F.col("private_client_trust_code")
    )
)

pw1_raw = (
    cust_pw
    .join(bridge_pw,
        (F.col("ind_ip_id")   == F.col("a2i_ip_id"))   &
        (F.col("ind_biz_dt")  == F.col("a2i_biz_dt"))  &
        (F.col("ind_src_code")== F.col("a2i_src_code")),
        "inner"
    )
    .join(acct_pw,
        (F.col("a2i_arr_id")       == F.col("ar_arr_id"))   &
        (F.col("a2i_arr_src_code") == F.col("ar_src_code")) &
        (F.col("a2i_biz_dt")       == F.col("ar_biz_dt")),
        "inner"
    )
    # Wealth eligibility filter (exact original)
    .filter(
        F.when(F.col("private_client_code").isin('039','539','339'), F.lit(1))
         .when(F.col("private_client_trust_code").isin('239','739'), F.lit(1))
         .otherwise(
             F.when(
                 F.col("business_service_segment_type_code")
                  .isin('IS_CT','IS_IT','REGIS_FC','REGIS','PWM'), F.lit(1)
             ).otherwise(F.lit(0))
         ) == 1
    )
    # Business_Group (exact original CASE per segment row)
    .withColumn("Business_Group",
        F.when(F.col("private_client_code").isin('039','539','339'),                  "Private Wealth")
         .when(F.col("private_client_trust_code").isin('239','739'),                  "Private Wealth")
         .when(F.col("business_service_segment_type_code") == "IS_CT",               "Institutional Services")
         .when(F.col("business_service_segment_type_code") == "IS_IT",               "Institutional Services")
         .when(F.col("business_service_segment_type_code").isin('REGIS_FC','REGIS'), "Investment Services")
         .when(F.col("business_service_segment_type_code") == "PWM",                 "Private Wealth")
         .otherwise(
             F.concat(F.col("business_service_segment_type_code"), F.lit(" Category????"))
         )
    )
    # Group by customer + segment (original groupBy)
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

# Division (exact original CASE on pw1)
pw1 = (
    pw1_raw
    .withColumn("division",
        F.when(F.col("Business_Group") == "Private Wealth",
            F.when((F.col("Trust_Count") > 0) & (F.col("Banking_Count") > 0),
                   "Banking & IM&T")
             .when((F.col("Investment_Count") + F.col("Trust_Count") > 0) & (F.col("Banking_Count") == 0),
                   "Investments Only")
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
             .when(F.col("PWM_Count") > 0, "Banking only")
             .otherwise("Corporate & Institutional Trust")
        )
    )
)

# =============================================================================
# FINAL — Build 2 output tables
#
# wealth_insights_customer:
#   rcif_dig (filtered to add_rcifs) LEFT JOIN pw1 on RCIF_NUMBER
#   pw1 may have multiple rows per RCIF → take highest priority Business_Group
#
# wealth_insights_account:
#   inv_df LEFT JOIN pw1 on ip_id
# =============================================================================

# For customer table: collapse pw1 to ONE row per RCIF_NUMBER
# Priority: Private Wealth > Institutional Services > Investment Services
bg_priority = F.when(F.col("Business_Group") == "Private Wealth",       1) \
               .when(F.col("Business_Group") == "Institutional Services",2) \
               .when(F.col("Business_Group") == "Investment Services",   3) \
               .otherwise(4)

pw1_per_rcif = (
    pw1
    .withColumn("bg_rank", bg_priority)
    .withColumn("row_num",
        F.row_number().over(
            Window.partitionBy("RCIF_NUMBER")
                  .orderBy(F.col("bg_rank"))
        )
    )
    .filter(F.col("row_num") == 1)
    .select(
        F.col("RCIF_NUMBER").alias("pw_rcif"),
        F.col("ip_id").alias("pw_ip_id"),
        "Business_Group",
        "division",
        "accts_cnt",
        "Corporate_Trust_Count",
        "Institutional_Trust_Count",
        "Investment_Count",
        "Insurance_Count",
        "PWM_Count",
        "Trust_Count",
        "Banking_Count"
    )
)

# ── wealth_insights_customer ──────────────────────────────────────────────────
wealth_insights_customer = (
    rcif_dig
    # Filter to valid RCIF universe (last 6 months)
    .join(add_rcifs, rcif_dig["RCIF_NUMBER"] == add_rcifs["rcif_number"], "inner")
    # Bring in wealth group/division/accts
    .join(pw1_per_rcif, rcif_dig["RCIF_NUMBER"] == pw1_per_rcif["pw_rcif"], "left")
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
    .distinct()
)

# ── wealth_insights_account ───────────────────────────────────────────────────
pw1_per_ip = (
    pw1
    .withColumn("bg_rank", bg_priority)
    .withColumn("row_num",
        F.row_number().over(
            Window.partitionBy("ip_id")
                  .orderBy(F.col("bg_rank"))
        )
    )
    .filter(F.col("row_num") == 1)
    .select(
        F.col("ip_id").alias("pw_ip_id"),
        "Business_Group",
        "division",
        "accts_cnt",
        "Corporate_Trust_Count",
        "Institutional_Trust_Count",
        "Investment_Count",
        "Insurance_Count",
        "PWM_Count",
        "Trust_Count",
        "Banking_Count"
    )
)

wealth_insights_account = (
    inv_df
    .join(pw1_per_ip, inv_df["ip_id"] == pw1_per_ip["pw_ip_id"], "left")
    .select(
        F.col("rcif_nbr").alias("RCIF_NUMBER"),
        inv_df["ip_id"],
        F.col("balance"),
        F.col("open_date"),
        F.col("Accounts").alias("arrangement_id"),
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

# ── Write tables ──────────────────────────────────────────────────────────────
wealth_insights_customer.write.mode("overwrite").saveAsTable(f"{DEFAULT_DB}.wealth_insights_customer")
wealth_insights_account.write.mode("overwrite").saveAsTable(f"{DEFAULT_DB}.wealth_insights_account")

print(f"✅ wealth_insights_customer: {wealth_insights_customer.count():,} rows")
print(f"✅ wealth_insights_account:  {wealth_insights_account.count():,} rows")

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
address  = spark.table(f"{EIL_DB}.d_involved_party_address_h")

# ── Source Code Lists (BW and PC removed per requirement) ────────────────────
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
# Exact original:
#   SELECT relt_ibn as reltibn,
#          max(olb_last_login_date) as lst_login_olb,
#          max(mob_last_login_date) as lst_login_mob,
# =============================================================================
# STEP 1 — Dig_Customer
# Exact original SQL:
#   WHERE ods_business_dt = (SELECT max(ods_business_dt) FROM digital_banking_master)
#   GROUP BY relt_ibn, ods_business_dt
#   SELECT relt_ibn as reltibn, max(olb_last_login_date), max(mob_last_login_date), ods_business_dt
# =============================================================================
# Dig_Customer — exact original SQL logic
# Original: WHERE ods_business_dt = max(ods_business_dt)
# GROUP BY relt_ibn, ods_business_dt
# SELECT relt_ibn, max(olb_last_login_date), max(mob_last_login_date), ods_business_dt
#
# Snapshot fix: filter to END_DT so we get Feb 28 snapshot
# AND cap login dates to END_DT so March logins don't inflate 90-day window
max_ods_dt = digital.filter(
    F.col("ods_business_dt") <= END_DT
).agg(F.max("ods_business_dt")).collect()[0][0]

dig_customer = (
    digital
    .filter(F.col("ods_business_dt") == max_ods_dt)
    .groupBy(
        F.col("ibn").alias("reltibn"),
        F.col("ods_business_dt")
    )
    .agg(
        # Cap login dates to END_DT - excludes any logins after snapshot date
        F.max(
            F.when(F.col("olb_last_login_date") <= END_DT,
                   F.col("olb_last_login_date"))
        ).alias("lst_login_olb"),
        F.max(
            F.when(F.col("mob_last_login_date") <= END_DT,
                   F.col("mob_last_login_date"))
        ).alias("lst_login_mob")
    )
)

# =============================================================================
# CTE 2 — INV (Investpath)
# customer → bridge → account (closed=N, type=IP, src=RN)
# =============================================================================
last_biz_date = customer.agg(F.max("business_date")).collect()[0][0]

cust_inv = (
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
        F.col("rcif_cust_nbr").alias("rcif_nbr")
    )
)

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

inv_df = (
    cust_inv
    .join(bridge_base,
        (F.col("c_ip_id")    == F.col("a2i_ip_id"))  &
        (F.col("c_biz_dt")   == F.col("a2i_biz_dt")) &
        (F.col("c_src_code") == F.col("a2i_src_code")),
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
        F.col("c_ip_id").alias("ip_id"),
        F.col("balance"),
        F.col("open_date"),
        F.col("a2i_arr_id").alias("Accounts")
    )
)

# =============================================================================
# CTE 3 — RC
# customer → bridge → account (VALID_SOURCE_CODES)
# group by customer fields, max(rcif_cust_nbr) as RCIF_NUMBER
# =============================================================================
cust_rc = (
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
        F.col("cust_internet_banking_nbr").alias("ibn"),
        F.col("involved_party_name"),
        F.col("involved_party_tax_id_nbr"),
        F.col("birth_date")
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

rc = (
    cust_rc
    .join(bridge_base,
        (F.col("c_ip_id")    == F.col("a2i_ip_id"))  &
        (F.col("c_biz_dt")   == F.col("a2i_biz_dt")) &
        (F.col("c_src_code") == F.col("a2i_src_code")),
        "inner"
    )
    .join(acct_rc,
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
            F.col("state_name"),
            F.col("city_name"),
            F.col("country_name")
        ),
        F.col("c_ip_id") == F.col("addr_ip_id"),
        "left"
    )
    .groupBy(
        F.col("c_ip_id").alias("involved_party_id"),
        F.col("ibn"),
        F.col("involved_party_name"),
        F.col("involved_party_tax_id_nbr"),
        F.col("birth_date"),
        F.col("state_name"),
        F.col("city_name"),
        F.col("country_name")
    )
    .agg(F.max(F.col("rcif_cust_nbr").cast("string")).alias("RCIF_NUMBER"))
)

# =============================================================================
# CTE 4 — RCIF_Dig
# rc LEFT JOIN Dig_Customer c ON rc.ibn = c.reltibn
# Exact original flag logic — c.* columns (null when no match = not digital)
# =============================================================================
# =============================================================================
# CTE 4 — RCIF_Dig
# Exact original SQL:
#   from rc
#   left join Dig_Customer c on rc.cust_internet_banking_nbr = c.reltibn
# All flags use c.* columns — null when customer not in digital universe
# =============================================================================
rcif_dig = (
    rc
    .join(
        dig_customer.select(
            F.col("reltibn"),           # c.reltibn — null check for Digital_flag
            F.col("ods_business_dt"),   # c.ods_business_Dt — reference date for datediff
            F.col("lst_login_olb"),     # c.lst_login_olb
            F.col("lst_login_mob")      # c.lst_login_mob
        ),
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
    # Exact original: datediff(c.ods_business_Dt, c.lst_login_mob) <= 90
    .withColumn("Mobile_Active_Flag",
        F.when(F.datediff(F.col("ods_business_dt"), F.col("lst_login_mob")) <= 90, "Mobile Active")
         .otherwise("Non Mobile Active")
    )
    # Exact original: c.lst_login_mob is null then Non Mobile User
    .withColumn("Mobile_Flag",
        F.when(F.col("lst_login_mob").isNull(), "Non Mobile User")
         .otherwise("Mobile User")
    )
    # Exact original: datediff(c.ods_business_Dt, c.lst_login_olb) <= 90
    .withColumn("OLB_Active_Flag",
        F.when(F.datediff(F.col("ods_business_dt"), F.col("lst_login_olb")) <= 90, "OLB Active")
         .otherwise("Non OLB Active")
    )
    # Exact original: c.lst_login_olb is null then Non OLB User
    .withColumn("OLB_Flag",
        F.when(F.col("lst_login_olb").isNull(), "Non OLB User")
         .otherwise("OLB User")
    )
    # Exact original: datediff mob <= 90 OR datediff olb <= 90
    .withColumn("Digitally_Active_Flag",
        F.when(
            (F.datediff(F.col("ods_business_dt"), F.col("lst_login_mob")) <= 90) |
            (F.datediff(F.col("ods_business_dt"), F.col("lst_login_olb")) <= 90),
            "Digital Active"
        ).otherwise("Non Digital Active")
    )
    # Exact original: c.reltibn is null then Non Digital User
    .withColumn("Digital_flag",
        F.when(F.col("reltibn").isNull(), "Non Digital User")
         .otherwise("Digital User")
    )
)

# =============================================================================
# CTE 5 — add_rcifs (RCIF universe last 6 months)
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
# customer → bridge → account (WEALTH_SOURCE_CODES, closed=N)
# Wealth eligibility filter
# Group by customer + segment → one row per customer per segment
# =============================================================================
cust_pw = (
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
        F.col("business_service_segment_type_code")
    )
)

pw1 = (
    cust_pw
    .join(bridge_base,
        (F.col("c_ip_id")    == F.col("a2i_ip_id"))  &
        (F.col("c_biz_dt")   == F.col("a2i_biz_dt")) &
        (F.col("c_src_code") == F.col("a2i_src_code")),
        "inner"
    )
    .join(acct_pw,
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
# FINAL — wealth_insights_customer & wealth_insights_account
# =============================================================================

# pw1 collapsed to ONE row per RCIF:
# - Top Business_Group by priority
# - SUM accts_cnt across all segments → total accounts per customer
bg_priority = (
    F.when(F.col("Business_Group") == "Private Wealth",        1)
     .when(F.col("Business_Group") == "Institutional Services",2)
     .when(F.col("Business_Group") == "Investment Services",   3)
     .otherwise(4)
)

pw1_top = (
    pw1
    .withColumn("bg_rank", bg_priority)
    .withColumn("rn", F.row_number().over(
        Window.partitionBy("RCIF_NUMBER").orderBy("bg_rank")
    ))
    .filter(F.col("rn") == 1)
    .select("RCIF_NUMBER", F.col("ip_id").alias("pw_ip_id"),
            "Business_Group", "division")
)

pw1_sums = (
    pw1
    .groupBy("RCIF_NUMBER")
    .agg(
        F.sum("accts_cnt").alias("accts_cnt"),
        F.sum("Corporate_Trust_Count").alias("Corporate_Trust_Count"),
        F.sum("Institutional_Trust_Count").alias("Institutional_Trust_Count"),
        F.sum("Investment_Count").alias("Investment_Count"),
        F.sum("Insurance_Count").alias("Insurance_Count"),
        F.sum("PWM_Count").alias("PWM_Count"),
        F.sum("Trust_Count").alias("Trust_Count"),
        F.sum("Banking_Count").alias("Banking_Count")
    )
)

pw1_per_rcif = (
    pw1_top
    .join(pw1_sums, "RCIF_NUMBER", "left")
    .select(
        F.col("RCIF_NUMBER").alias("pw_rcif"),
        "pw_ip_id", "Business_Group", "division", "accts_cnt",
        "Corporate_Trust_Count", "Institutional_Trust_Count",
        "Investment_Count", "Insurance_Count",
        "PWM_Count", "Trust_Count", "Banking_Count"
    )
)

# ── wealth_insights_customer ──────────────────────────────────────────────────
# Base: rcif_dig filtered to add_rcifs (~7M rows)
# LEFT JOIN pw1_per_rcif for wealth fields
wealth_insights_customer = (
    rcif_dig
    .join(add_rcifs,    rcif_dig["RCIF_NUMBER"] == add_rcifs["rcif_number"],  "inner")
    .join(pw1_per_rcif, rcif_dig["RCIF_NUMBER"] == pw1_per_rcif["pw_rcif"],   "left")
    .select(
        # Identity
        rcif_dig["RCIF_NUMBER"],
        F.col("pw_ip_id").alias("ip_id"),
        rcif_dig["ibn"],
        F.col("involved_party_name"),
        F.col("involved_party_tax_id_nbr"),
        F.col("birth_date"),
        F.col("CUSTOMER_GENERATION"),
        # Digital flags (from Dig_Customer LEFT JOIN)
        F.col("state_name"),
        F.col("city_name"),
        F.col("country_name"),
        F.col("Mobile_Active_Flag"),
        F.col("Mobile_Flag"),
        F.col("OLB_Active_Flag"),
        F.col("OLB_Flag"),
        F.col("Digitally_Active_Flag"),
        F.col("Digital_flag"),
        # Wealth fields
        F.col("Business_Group"),
        F.col("division"),
        # Account counts (SUM across all segments)
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
    .withColumn("rn", F.row_number().over(
        Window.partitionBy("ip_id").orderBy("bg_rank")
    ))
    .filter(F.col("rn") == 1)
    .select(
        F.col("ip_id").alias("pw_ip_id"),
        "Business_Group", "division", "accts_cnt",
        "Corporate_Trust_Count", "Institutional_Trust_Count",
        "Investment_Count", "Insurance_Count",
        "PWM_Count", "Trust_Count", "Banking_Count"
    )
)

# state_name lookup from rc (one row per ip_id)
rc_state = (
    rc.select(
        F.col("involved_party_id").alias("s_ip_id"),
        F.col("state_name"),
        F.col("city_name"),
        F.col("country_name")
    ).distinct()
)

wealth_insights_account = (
    inv_df
    .join(pw1_per_ip, inv_df["ip_id"] == pw1_per_ip["pw_ip_id"], "left")
    .join(rc_state,   inv_df["ip_id"] == rc_state["s_ip_id"],     "left")
    .select(
        # Identity
        F.col("rcif_nbr").alias("RCIF_NUMBER"),
        inv_df["ip_id"],
        # InvestPath columns
        F.col("balance"),
        F.col("open_date"),
        F.col("Accounts").alias("arrangement_id"),
        # Wealth fields
        F.col("Business_Group"),
        F.col("division"),
        # Account counts
        F.col("accts_cnt"),
        F.col("Corporate_Trust_Count"),
        F.col("Institutional_Trust_Count"),
        F.col("Investment_Count"),
        F.col("Insurance_Count"),
        F.col("PWM_Count"),
        F.col("Trust_Count"),
        F.col("Banking_Count"),
        # Location
        F.col("state_name"),
        F.col("city_name"),
        F.col("country_name")
    )
)

# ── Write tables ──────────────────────────────────────────────────────────────
wealth_insights_customer.write.mode("overwrite").saveAsTable(f"{DEFAULT_DB}.wealth_insights_customer")
wealth_insights_account.write.mode("overwrite").saveAsTable(f"{DEFAULT_DB}.wealth_insights_account")

print(f"✅ wealth_insights_customer: {wealth_insights_customer.count():,} rows")
print(f"✅ wealth_insights_account:  {wealth_insights_account.count():,} rows")

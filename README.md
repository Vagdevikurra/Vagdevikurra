# =============================================================================
# Wealth Insights - Customer & Account Tables
# Date Range: 2025-08-01 to 2026-01-31
# Python 3.6 compatible
#
# FIXES:
#   1. date.fromisoformat() -> datetime.strptime() for Python 3.6
#   2. digital_banking_master_dbm -> digital_banking_master
#   3. relt_ibn -> ibn
#   4. Wealth eligibility filter moved AFTER all joins (fixes AnalysisException)
#   5. business_group_expr uses F.col() instead of table refs (safe after join)
# =============================================================================

from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.window import Window
from datetime import date, timedelta, datetime
import calendar

spark = (
    SparkSession.builder
    .appName("WealthInsights")
    .enableHiveSupport()
    .getOrCreate()
)

START_DT             = "2025-08-01"
END_DT               = "2026-01-31"
final_db             = "dm_ib_dev"
final_table_customer = "wealth_Insights_Customer"
final_table_account  = "wealth_Insights_Account"

# =============================================================================
# HELPER: Python 3.6 safe date parser
# =============================================================================

def parse_date(date_str):
    """Replaces date.fromisoformat() which requires Python 3.7+"""
    return datetime.strptime(date_str, "%Y-%m-%d").date()


# =============================================================================
# HELPER: Build valid business-date list
# Daily tables skip weekends & holidays. We ensure every calendar month-end
# in [START_DT, END_DT] is represented — if it falls on a weekend/holiday
# we roll back to the nearest prior date that actually exists in the table.
# Covers: Aug-31, Sep-30, Oct-31, Nov-30, Dec-31, Jan-31
# =============================================================================

def get_valid_business_dates(spark, table, start, end):
    existing_df = (
        spark.table(table)
        .filter(
            (F.col("business_date").cast("date") >= F.lit(start)) &
            (F.col("business_date").cast("date") <= F.lit(end))
        )
        .select(F.col("business_date").cast("date").alias("dt"))
        .distinct()
    )
    existing = {row["dt"] for row in existing_df.collect()}

    start_d = parse_date(start)
    end_d   = parse_date(end)

    # Generate all calendar month-ends in range
    month_ends = set()
    y, m = start_d.year, start_d.month
    while date(y, m, 1) <= end_d:
        last_day = calendar.monthrange(y, m)[1]
        me = date(y, m, last_day)
        if start_d <= me <= end_d:
            month_ends.add(me)
        if m == 12:
            y += 1; m = 1
        else:
            m += 1

    # For each month-end not in existing dates, roll back to nearest prior date
    final_dates = set(existing)
    for me in month_ends:
        if me not in existing:
            candidate = me - timedelta(days=1)
            while candidate >= start_d:
                if candidate in existing:
                    final_dates.add(candidate)
                    break
                candidate -= timedelta(days=1)

    return [str(d) for d in sorted(final_dates)]


# =============================================================================
# SOURCE CODE CONSTANTS
# PC (plastic cards) and BW intentionally excluded
# =============================================================================

WEALTH_SOURCE_CODES = [
    'DA', 'SV', 'CC', 'MG', 'LS', 'TM', 'LO',
    'CM', 'CS', 'EL', 'IC', 'MA', 'PF', 'PR', 'SD',
    'TR', 'BI',
    'RN',
    'IS_CT', 'IS_IT',
    'IS_IF', 'PNB'
]

BANKING_SOURCE_CODES = [
    'DA', 'SV', 'CC', 'MG', 'LS', 'TM', 'LO',
    'CM', 'CS', 'EL', 'IC', 'MA', 'PF', 'PR', 'SD'
]

WEALTH_SEGMENT_CODES = ['IS_CT', 'IS_IT', 'REGIS_FC', 'REGIS', 'PWM']

# =============================================================================
# STEP 1: RCIF NUMBER POOL
# =============================================================================

six_months_ago = F.add_months(F.current_date(), -6)

rcif_from_ip = (
    spark.table("eil.m_involved_party_h")
    .filter(F.col("business_date").cast("date") >= six_months_ago)
    .select(F.col("rcif_cust_nbr").alias("rcif_number"))
)

rcif_from_dbm = (
    spark.table("dm_ib.digital_banking_master")
    .filter(F.col("ods_business_dt") >= six_months_ago)
    .select(F.col("rcif_customer_nbr").alias("rcif_number"))
)

add_rcifs = (
    rcif_from_ip.union(rcif_from_dbm)
    .distinct()
    .filter(
        F.col("rcif_number").isNotNull() &
        (F.col("rcif_number") != "")
    )
)

# =============================================================================
# STEP 2: DIGITAL FLAGS
# Source: dm_ib.digital_banking_master  (corrected — no _dbm suffix)
# ibn corrected from relt_ibn
# =============================================================================

max_dbm_dt = (
    spark.table("dm_ib.digital_banking_master")
    .agg(F.max("ods_business_dt"))
    .collect()[0][0]
)

dig_customer = (
    spark.table("dm_ib.digital_banking_master")
    .filter(F.col("ods_business_dt") == max_dbm_dt)
    .groupBy(
        F.col("ibn"),                               # corrected from relt_ibn
        F.col("rcif_customer_nbr"),
        F.col("ods_business_dt")
    )
    .agg(
        F.max("olb_last_login_date").alias("lst_login_olb"),
        F.max("mob_last_login_date").alias("lst_login_mob")
    )
)

# =============================================================================
# STEP 3: RCIF CUSTOMER BASE
# Source: eil.d_involved_party_h (daily)
# =============================================================================

valid_dates_daily = get_valid_business_dates(
    spark, "eil.d_involved_party_h", START_DT, END_DT
)

ip_d   = spark.table("eil.d_involved_party_h")
a2i_d  = spark.table("eil.d_arrangement_to_involved_party_relationship_h")
ar_d   = spark.table("eil.d_arrangement_h")
addr_d = spark.table("eil.d_involved_party_address_h")

rc = (
    ip_d
    .filter(
        F.col("business_date").isin(valid_dates_daily) &
        (F.col("source_system_code") == "CF") &
        (F.coalesce(F.col("deceased_ind"), F.lit("N")) == "N")
    )
    .join(
        a2i_d,
        (ip_d["business_date"]      == a2i_d["business_date"]) &
        (ip_d["source_system_code"] == a2i_d["source_system_code"]) &
        (ip_d["involved_party_id"]  == a2i_d["involved_party_id"]),
        "inner"
    )
    .join(
        ar_d.filter(F.col("source_system_code").isin(WEALTH_SOURCE_CODES)),
        (a2i_d["business_date"]                  == ar_d["business_date"]) &
        (a2i_d["arrangement_source_system_code"] == ar_d["source_system_code"]) &
        (a2i_d["arrangement_id"]                 == ar_d["arrangement_id"]),
        "inner"
    )
    .join(
        addr_d,
        (ip_d["involved_party_id"] == addr_d["involved_party_id"]) &
        (ip_d["business_date"]     == addr_d["business_date"]),
        "inner"
    )
    .groupBy(
        ip_d["business_date"].cast("date").alias("business_date"),
        ip_d["involved_party_id"].alias("ip_id"),
        ip_d["cust_internet_banking_nbr"],
        ip_d["involved_party_tax_id_nbr"],
        ip_d["birth_date"],
        ip_d["involved_party_name"],
        addr_d["city_name"],
        addr_d["state_name"],
        addr_d["country_name"]
    )
    .agg(
        F.max(F.col("rcif_cust_nbr").cast("string")).alias("RCIF_NUMBER")
    )
)

# =============================================================================
# STEP 4: WEALTH SEGMENTATION + ACCOUNT COUNTS
# Source: eil.m_involved_party_h (monthly)
# FIX: eligibility filter moved AFTER all joins so ar_m columns are resolved
# FIX: business_group_expr uses F.col() not table refs (safe post-join)
# =============================================================================

valid_dates_monthly = get_valid_business_dates(
    spark, "eil.m_involved_party_h", START_DT, END_DT
)

ip_m  = spark.table("eil.m_involved_party_h")
a2i_m = spark.table("eil.m_arrangement_to_involved_party_relationship_h")
ar_m  = spark.table("eil.m_arrangement_h")

# Join first, filter eligibility after — this is the key fix
pw1_joined = (
    ip_m
    .filter(
        F.col("business_date").isin(valid_dates_monthly) &
        (F.col("source_system_code") == "CF") &
        (F.coalesce(F.col("deceased_ind"), F.lit("N")) == "N")
    )
    .join(
        a2i_m,
        (ip_m["involved_party_id"]  == a2i_m["involved_party_id"]) &
        (ip_m["business_date"]      == a2i_m["business_date"]) &
        (ip_m["source_system_code"] == a2i_m["source_system_code"]),
        "inner"
    )
    .join(
        ar_m.filter(
            F.col("source_system_code").isin(WEALTH_SOURCE_CODES) &
            (F.col("closed_ind") == "N")
        ),
        (a2i_m["arrangement_id"]                 == ar_m["arrangement_id"]) &
        (a2i_m["arrangement_source_system_code"] == ar_m["source_system_code"]) &
        (a2i_m["business_date"]                  == ar_m["business_date"]),
        "inner"
    )
    # Eligibility filter AFTER join — ar_m columns now resolved
    .filter(
        F.when(F.col("private_client_code").isin("039","539","339"),     F.lit(1))
         .when(F.col("private_client_trust_code").isin("239","739"),     F.lit(1))
         .otherwise(
             F.when(
                 F.col("business_service_segment_type_code").isin(WEALTH_SEGMENT_CODES),
                 F.lit(1)
             ).otherwise(F.lit(0))
         ) == 1
    )
    # Business group uses F.col() — safe now that all tables are joined
    .withColumn("Business_Group",
        F.when(F.col("private_client_code").isin("039","539","339"),     F.lit("Private Wealth"))
         .when(F.col("private_client_trust_code").isin("239","739"),     F.lit("Private Wealth"))
         .otherwise(
             F.when(F.col("business_service_segment_type_code").isin("IS_CT","IS_IT"),     F.lit("Institutional Services"))
              .when(F.col("business_service_segment_type_code").isin("REGIS_FC","REGIS"),  F.lit("Investment Services"))
              .when(F.col("business_service_segment_type_code") == "PWM",                  F.lit("Private Wealth"))
              .otherwise(F.coalesce(F.col("business_service_segment_type_code"),           F.lit("Category???")))
         )
    )
)

pw1 = (
    pw1_joined
    .groupBy(
        ip_m["rcif_cust_nbr"].cast("string").alias("RCIF_NUMBER"),
        ip_m["involved_party_id"].alias("ip_id"),
        F.col("Business_Group")
    )
    .agg(
        F.countDistinct(
            F.when(F.col("business_service_segment_type_code") == "IS_CT",                F.col("arrangement_id"))
        ).alias("corporate_trust_cnt"),
        F.countDistinct(
            F.when(F.col("business_service_segment_type_code") == "IS_IT",                F.col("arrangement_id"))
        ).alias("institutional_trust_cnt"),
        F.countDistinct(
            F.when(F.col("business_service_segment_type_code").isin("REGIS_FC","REGIS"),  F.col("arrangement_id"))
        ).alias("investment_cnt"),
        F.countDistinct(
            F.when(F.col("business_service_segment_type_code") == "REGIS",                F.col("arrangement_id"))
        ).alias("insurance_cnt"),
        F.countDistinct(
            F.when(F.col("business_service_segment_type_code") == "PWM",                  F.col("arrangement_id"))
        ).alias("pwm_cnt"),
        F.countDistinct(
            F.when(F.col("source_system_code") == "TR",                                   F.col("arrangement_id"))
        ).alias("trust_cnt"),
        F.countDistinct(
            F.when(F.col("source_system_code").isin(BANKING_SOURCE_CODES),                F.col("arrangement_id"))
        ).alias("banking_cnt"),
        F.countDistinct(F.col("arrangement_id")).alias("wealth_accts_cnt")
    )
)

pw1_with_division = pw1.withColumn("division",
    F.when(F.col("Business_Group") == "Private Wealth",
        F.when(
            (F.col("trust_cnt") > 0) & (F.col("banking_cnt") > 0), F.lit("Banking & IM&T")
        ).when(
            (F.col("investment_cnt") + F.col("trust_cnt") > 0) & (F.col("banking_cnt") == 0), F.lit("Investments Only")
        ).otherwise(F.lit("Banking only"))
    )
    .when(F.col("Business_Group") == "Investment Services",
        F.when(
            (F.col("investment_cnt") > 0) & (F.col("insurance_cnt") == 0), F.lit("Investment")
        ).when(
            (F.col("investment_cnt") == 0) & (F.col("insurance_cnt") > 0), F.lit("Insurance")
        ).otherwise(F.lit("Insurance & Investment"))
    )
    .otherwise(
        F.when(
            (F.col("corporate_trust_cnt") > 0) & (F.col("institutional_trust_cnt") == 0), F.lit("Corporate Trust")
        ).when(
            (F.col("corporate_trust_cnt") == 0) & (F.col("institutional_trust_cnt") > 0), F.lit("Institutional Trust")
        ).when(F.col("pwm_cnt") > 0, F.lit("Banking only"))
        .otherwise(F.lit("Corporate & Institutional Trust"))
    )
)

# =============================================================================
# STEP 5: INVESTPATH ACCOUNT COUNT PER CUSTOMER
# =============================================================================

max_inv_date = (
    spark.table("eil.d_involved_party_h")
    .agg(F.max("business_date"))
    .collect()[0][0]
)

ind_inv = spark.table("eil.d_involved_party_h")
a2i_inv = spark.table("eil.d_arrangement_to_involved_party_relationship_h")
ar_inv  = spark.table("eil.d_arrangement_h")

ip_accts_cnt = (
    ind_inv
    .filter(
        (F.col("business_date") == max_inv_date) &
        (F.col("source_system_code") == "CF") &
        (F.coalesce(F.col("deceased_ind"), F.lit("N")) == "N")
    )
    .join(
        a2i_inv,
        (ind_inv["involved_party_id"]  == a2i_inv["involved_party_id"]) &
        (ind_inv["business_date"]      == a2i_inv["business_date"]) &
        (ind_inv["source_system_code"] == a2i_inv["source_system_code"]),
        "inner"
    )
    .join(
        ar_inv.filter(
            (F.col("closed_ind")         == "N") &
            (F.col("account_type_code")  == "IP") &
            (F.col("source_system_code") == "RN")
        ),
        (a2i_inv["arrangement_id"]                 == ar_inv["arrangement_id"]) &
        (a2i_inv["arrangement_source_system_code"] == ar_inv["source_system_code"]) &
        (a2i_inv["business_date"]                  == ar_inv["business_date"]),
        "inner"
    )
    .groupBy(
        ind_inv["rcif_cust_nbr"].cast("string").alias("RCIF_NUMBER"),
        ind_inv["involved_party_id"].alias("ip_id")
    )
    .agg(
        F.countDistinct(ar_inv["arrangement_id"]).alias("ip_accts_cnt")
    )
)

# =============================================================================
# STEP 6: ALL ACCOUNTS BASE
# One row per account, ip_ columns populated for Investpath rows only
# =============================================================================

valid_dates_acct = get_valid_business_dates(
    spark, "eil.d_arrangement_h", START_DT, END_DT
)

ip_a  = spark.table("eil.d_involved_party_h")
a2i_a = spark.table("eil.d_arrangement_to_involved_party_relationship_h")
ar_a  = spark.table("eil.d_arrangement_h")

all_accounts_base = (
    ip_a
    .filter(
        F.col("business_date").isin(valid_dates_acct) &
        (F.col("source_system_code") == "CF") &
        (F.coalesce(F.col("deceased_ind"), F.lit("N")) == "N")
    )
    .join(
        a2i_a,
        (ip_a["involved_party_id"]  == a2i_a["involved_party_id"]) &
        (ip_a["business_date"]      == a2i_a["business_date"]) &
        (ip_a["source_system_code"] == a2i_a["source_system_code"]),
        "inner"
    )
    .join(
        ar_a.filter(
            F.col("source_system_code").isin(WEALTH_SOURCE_CODES) &
            (F.col("closed_ind") == "N")
        ),
        (a2i_a["arrangement_id"]                 == ar_a["arrangement_id"]) &
        (a2i_a["arrangement_source_system_code"] == ar_a["source_system_code"]) &
        (a2i_a["business_date"]                  == ar_a["business_date"]),
        "inner"
    )
    .withColumn("account_type",
        F.when(
            (F.col("source_system_code") == "RN") & (F.col("account_type_code") == "IP"), F.lit("Investpath")
        )
        .when(
            F.col("business_service_segment_type_code").isin("IS_CT","IS_IT") |
            F.col("source_system_code").isin("TR","BI"), F.lit("Trust")
        )
        .when(F.col("business_service_segment_type_code").isin("REGIS_FC","REGIS"), F.lit("Investment"))
        .when(F.col("business_service_segment_type_code") == "PWM",                 F.lit("PWM"))
        .when(F.col("source_system_code").isin(BANKING_SOURCE_CODES),               F.lit("Banking"))
        .otherwise(F.lit("Other"))
    )
    .select(
        ip_a["business_date"].cast("date").alias("business_date"),
        ip_a["rcif_cust_nbr"].cast("string").alias("RCIF_NUMBER"),
        ip_a["involved_party_id"].alias("ip_id"),
        ip_a["cust_internet_banking_nbr"],
        ar_a["arrangement_id"].alias("account_number"),
        ar_a["source_system_code"].alias("source_system"),
        ar_a["account_type_code"],
        ar_a["business_service_segment_type_code"],
        ar_a["current_balance_amt"].alias("balance"),
        ar_a["open_date"],
        ar_a["closed_ind"],
        F.col("account_type"),
        # ip_ columns — Investpath rows only, NULL for all other account types
        F.when(F.col("account_type") == "Investpath", ar_a["arrangement_id"])
         .otherwise(F.lit(None).cast(T.StringType())).alias("ip_account_number"),
        F.when(F.col("account_type") == "Investpath", ar_a["open_date"])
         .otherwise(F.lit(None).cast(T.DateType())).alias("ip_open_date"),
        F.when(F.col("account_type") == "Investpath", ar_a["current_balance_amt"])
         .otherwise(F.lit(None).cast(T.DoubleType())).alias("ip_balance"),
        F.when(F.col("account_type") == "Investpath", ar_a["source_system_code"])
         .otherwise(F.lit(None).cast(T.StringType())).alias("ip_source_system"),
        F.when(F.col("account_type") == "Investpath", ar_a["account_type_code"])
         .otherwise(F.lit(None).cast(T.StringType())).alias("ip_account_type_code")
    )
)

# =============================================================================
# STEP 7: FINAL CUSTOMER TABLE — one row per RCIF_NUMBER
# =============================================================================

window_rcif = Window.partitionBy("RCIF_NUMBER").orderBy("cust_internet_banking_nbr")

generation_expr = (
    F.when((F.col("birth_date") >= "1900-01-01") & (F.col("birth_date") <= "1924-12-31"), F.lit("GI Generation (1900-1924)"))
     .when((F.col("birth_date") >= "1925-01-01") & (F.col("birth_date") <= "1945-12-31"), F.lit("Traditionalist (1925-1945)"))
     .when((F.col("birth_date") >= "1946-01-01") & (F.col("birth_date") <= "1964-12-31"), F.lit("Baby Boomer (1946-1964)"))
     .when((F.col("birth_date") >= "1965-01-01") & (F.col("birth_date") <= "1980-12-31"), F.lit("Gen X (1965-1980)"))
     .when((F.col("birth_date") >= "1981-01-01") & (F.col("birth_date") <= "1996-12-31"), F.lit("Millennial (1981-1996)"))
     .when( F.col("birth_date") >= "1997-01-01",                                           F.lit("Centennial (1997-???)"))
     .otherwise(F.lit("Unknown"))
)

wealth_customer = (
    rc
    .join(
        dig_customer.select(
            F.col("ibn").alias("primary_ibn"),
            F.col("rcif_customer_nbr").alias("RCIF_NUMBER"),
            "ods_business_dt",
            "lst_login_olb",
            "lst_login_mob"
        ),
        ["RCIF_NUMBER"],
        "left"
    )
    .join(
        pw1_with_division.select(
            "RCIF_NUMBER", "ip_id",
            "Business_Group", "division",
            "corporate_trust_cnt", "institutional_trust_cnt",
            "investment_cnt", "insurance_cnt",
            "pwm_cnt", "trust_cnt", "banking_cnt",
            "wealth_accts_cnt"
        ),
        ["RCIF_NUMBER", "ip_id"],
        "left"
    )
    .join(ip_accts_cnt, ["RCIF_NUMBER", "ip_id"], "left")
    .join(
        add_rcifs.withColumnRenamed("rcif_number", "RCIF_NUMBER"),
        ["RCIF_NUMBER"],
        "inner"
    )
    .withColumn("_rank", F.row_number().over(window_rcif))
    .filter(F.col("_rank") == 1)
    .drop("_rank")
    .withColumn("customer_generation", generation_expr)
    .withColumn("mobile_active_flag",
        F.when(F.datediff(F.col("ods_business_dt"), F.col("lst_login_mob")) <= 90, F.lit("Mobile Active"))
         .otherwise(F.lit("Non Mobile Active"))
    )
    .withColumn("mobile_flag",
        F.when(F.col("lst_login_mob").isNull(), F.lit("Non Mobile User")).otherwise(F.lit("Mobile User"))
    )
    .withColumn("olb_active_flag",
        F.when(F.datediff(F.col("ods_business_dt"), F.col("lst_login_olb")) <= 90, F.lit("OLB Active"))
         .otherwise(F.lit("Non OLB Active"))
    )
    .withColumn("olb_flag",
        F.when(F.col("lst_login_olb").isNull(), F.lit("Non OLB User")).otherwise(F.lit("OLB User"))
    )
    .withColumn("digitally_active_flag",
        F.when(
            (F.datediff(F.col("ods_business_dt"), F.col("lst_login_mob")) <= 90) |
            (F.datediff(F.col("ods_business_dt"), F.col("lst_login_olb")) <= 90),
            F.lit("Digital Active")
        ).otherwise(F.lit("Non Digital Active"))
    )
    .withColumn("digital_flag",
        F.when(F.col("primary_ibn").isNull(), F.lit("Non Digital User")).otherwise(F.lit("Digital User"))
    )
    .select(
        F.col("business_date"),
        F.col("RCIF_NUMBER"),
        F.col("ip_id"),
        F.col("involved_party_name").alias("customer_name"),
        F.col("involved_party_tax_id_nbr").alias("tax_id_nbr"),
        F.col("birth_date"),
        F.col("customer_generation"),
        F.col("cust_internet_banking_nbr").alias("olb_ibn"),
        F.col("primary_ibn"),
        F.col("city_name"),
        F.col("state_name"),
        F.col("country_name"),
        F.col("Business_Group").alias("business_group"),
        F.col("division"),
        F.coalesce(F.col("wealth_accts_cnt"), F.lit(0)).alias("wealth_accts_cnt"),
        F.coalesce(F.col("ip_accts_cnt"),     F.lit(0)).alias("ip_accts_cnt"),
        F.coalesce(F.col("banking_cnt"),              F.lit(0)).alias("banking_cnt"),
        F.coalesce(F.col("investment_cnt"),           F.lit(0)).alias("investment_cnt"),
        F.coalesce(F.col("trust_cnt"),                F.lit(0)).alias("trust_cnt"),
        F.coalesce(F.col("insurance_cnt"),            F.lit(0)).alias("insurance_cnt"),
        F.coalesce(F.col("pwm_cnt"),                  F.lit(0)).alias("pwm_cnt"),
        F.coalesce(F.col("corporate_trust_cnt"),      F.lit(0)).alias("corporate_trust_cnt"),
        F.coalesce(F.col("institutional_trust_cnt"),  F.lit(0)).alias("institutional_trust_cnt"),
        F.col("digital_flag"),
        F.col("digitally_active_flag"),
        F.col("mobile_flag"),
        F.col("mobile_active_flag"),
        F.col("olb_flag"),
        F.col("olb_active_flag"),
        F.col("lst_login_mob").alias("last_mobile_login_dt"),
        F.col("lst_login_olb").alias("last_olb_login_dt")
    )
)

# =============================================================================
# STEP 8: FINAL ACCOUNT TABLE — one row per account
# =============================================================================

wealth_account = (
    all_accounts_base
    .join(
        wealth_customer.select(
            "RCIF_NUMBER", "ip_id",
            "business_group", "division",
            "customer_generation",
            "digital_flag", "digitally_active_flag",
            "mobile_active_flag", "olb_active_flag",
            "mobile_flag", "olb_flag",
            "wealth_accts_cnt", "ip_accts_cnt",
            "primary_ibn",
            F.col("olb_ibn"),
            "state_name"
        ),
        ["RCIF_NUMBER", "ip_id"],
        "left"
    )
    .select(
        F.col("business_date"),
        F.col("RCIF_NUMBER"),
        F.col("ip_id"),
        F.col("olb_ibn"),
        F.col("primary_ibn"),
        F.col("account_number"),
        F.col("account_type"),
        F.col("account_type_code"),
        F.col("source_system"),
        F.col("business_service_segment_type_code"),
        F.col("open_date"),
        F.col("balance"),
        F.col("closed_ind"),
        F.col("ip_account_number"),
        F.col("ip_open_date"),
        F.col("ip_balance"),
        F.col("ip_source_system"),
        F.col("ip_account_type_code"),
        F.col("business_group"),
        F.col("division"),
        F.col("state_name"),
        F.col("customer_generation"),
        F.col("digital_flag"),
        F.col("digitally_active_flag"),
        F.col("mobile_active_flag"),
        F.col("mobile_flag"),
        F.col("olb_active_flag"),
        F.col("olb_flag"),
        F.col("wealth_accts_cnt"),
        F.col("ip_accts_cnt")
    )
)

# =============================================================================
# STEP 9: WRITE OUTPUT TABLES
# =============================================================================

(
    wealth_customer
    .write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("{}.{}".format(final_db, final_table_customer))
)

(
    wealth_account
    .write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("{}.{}".format(final_db, final_table_account))
)

print("Done: {}.{}".format(final_db, final_table_customer))
print("Done: {}.{}".format(final_db, final_table_account))

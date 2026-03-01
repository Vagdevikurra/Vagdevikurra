# =============================================================================
# Wealth Insights - Customer & Account Tables
# Date Range: 2025-08-01 to 2026-01-31
#
# OUTPUT TABLES:
#   dm_ib_dev.wealth_Insights_Customer  — one row per RCIF_NUMBER
#   dm_ib_dev.wealth_Insights_Account   — one row per account
#
# BRIDGE KEY: RCIF_NUMBER + ip_id (involved_party_id)
# NOTE: PC (plastic cards) and BW removed from all source code lists
# =============================================================================

from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.window import Window
from datetime import date, timedelta, datetime
import calendar

def parse_date(date_str):
    """Python 3.6 safe date parser — replaces date.fromisoformat() (Python 3.7+)"""
    return datetime.strptime(date_str, "%Y-%m-%d").date()

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
# HELPER: Build valid business-date list
# Daily tables skip weekends & holidays. We ensure every calendar month-end
# in [START_DT, END_DT] is represented — if it falls on a weekend/holiday,
# we roll back to the nearest prior date that actually exists in the table.
# Covers: Aug-31, Sep-30, Oct-31, Nov-30, Dec-31, Jan-31
# =============================================================================

def get_valid_business_dates(spark, table: str, start: str, end: str):
    """
    Returns a sorted list of date strings representing valid business dates,
    with month-ends always included (rolling back to last available day if needed).
    """
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
    'CM', 'CS', 'EL', 'IC', 'MA', 'PF', 'PR', 'SD',   # Banking
    'TR', 'BI',                                          # Trust
    'RN',                                                # Investpath
    'IS_CT', 'IS_IT',                                    # Institutional
    'IS_IF', 'PNB'
]

BANKING_SOURCE_CODES = [
    'DA', 'SV', 'CC', 'MG', 'LS', 'TM', 'LO',
    'CM', 'CS', 'EL', 'IC', 'MA', 'PF', 'PR', 'SD'
]

# =============================================================================
# STEP 1: RCIF NUMBER POOL
# Union of eil.m_involved_party_h and dm_ib.digital_banking_master
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
# Source: dm_ib.digital_banking_master_dbm (monthly)
# Captures: OLB, Mobile, Digital active/user flags + both IBNs
# =============================================================================

max_dbm_dt = (
    spark.table("dm_ib.digital_banking_master_dbm")
    .agg(F.max("ods_business_dt"))
    .collect()[0][0]
)

dig_customer = (
    spark.table("dm_ib.digital_banking_master_dbm")
    .filter(F.col("ods_business_dt") == max_dbm_dt)
    .groupBy(
        F.col("relt_ibn"),
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
# Source: eil.d_involved_party_h (daily) + address + arrangement tables
# Captures: demographics, cust_internet_banking_nbr (OLB IBN)
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
# Captures: Business_Group, division, all segment counts, wealth_accts_cnt
# =============================================================================

valid_dates_monthly = get_valid_business_dates(
    spark, "eil.m_involved_party_h", START_DT, END_DT
)

ip_m  = spark.table("eil.m_involved_party_h")
a2i_m = spark.table("eil.m_arrangement_to_involved_party_relationship_h")
ar_m  = spark.table("eil.m_arrangement_h")

business_group_expr = (
    F.when(ip_m["private_client_code"].isin("039","539","339"),           "Private Wealth")
     .when(ip_m["private_client_trust_code"].isin("239","739"),           "Private Wealth")
     .otherwise(
         F.when(ar_m["business_service_segment_type_code"].isin("IS_CT","IS_IT"), "Institutional Services")
          .when(ar_m["business_service_segment_type_code"].isin("REGIS_FC","REGIS"), "Investment Services")
          .when(ar_m["business_service_segment_type_code"] == "PWM",      "Private Wealth")
          .otherwise(F.coalesce(ar_m["business_service_segment_type_code"], F.lit("Category???")))
     )
)

pw1 = (
    ip_m
    .filter(
        F.col("business_date").isin(valid_dates_monthly) &
        (F.col("source_system_code") == "CF") &
        (F.coalesce(F.col("deceased_ind"), F.lit("N")) == "N") &
        (
            F.when(ip_m["private_client_code"].isin("039","539","339"),   F.lit(1))
             .when(ip_m["private_client_trust_code"].isin("239","739"),   F.lit(1))
             .otherwise(
                 F.when(
                     ar_m["business_service_segment_type_code"].isin(
                         "IS_CT","IS_IT","REGIS_FC","REGIS","PWM"
                     ), F.lit(1)
                 ).otherwise(F.lit(0))
             ) == 1
        )
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
    .withColumn("Business_Group", business_group_expr)
    .groupBy(
        ip_m["rcif_cust_nbr"].cast("string").alias("RCIF_NUMBER"),
        ip_m["involved_party_id"].alias("ip_id"),
        F.col("Business_Group")
    )
    .agg(
        F.countDistinct(
            F.when(ar_m["business_service_segment_type_code"] == "IS_CT",  ar_m["arrangement_id"])
        ).alias("corporate_trust_cnt"),
        F.countDistinct(
            F.when(ar_m["business_service_segment_type_code"] == "IS_IT",  ar_m["arrangement_id"])
        ).alias("institutional_trust_cnt"),
        F.countDistinct(
            F.when(ar_m["business_service_segment_type_code"].isin("REGIS_FC","REGIS"), ar_m["arrangement_id"])
        ).alias("investment_cnt"),
        F.countDistinct(
            F.when(ar_m["business_service_segment_type_code"] == "REGIS",  ar_m["arrangement_id"])
        ).alias("insurance_cnt"),
        F.countDistinct(
            F.when(ar_m["business_service_segment_type_code"] == "PWM",    ar_m["arrangement_id"])
        ).alias("pwm_cnt"),
        F.countDistinct(
            F.when(ar_m["source_system_code"] == "TR",                     ar_m["arrangement_id"])
        ).alias("trust_cnt"),
        F.countDistinct(
            F.when(ar_m["source_system_code"].isin(BANKING_SOURCE_CODES),  ar_m["arrangement_id"])
        ).alias("banking_cnt"),
        # Total wealth account count across ALL sources
        F.countDistinct(ar_m["arrangement_id"]).alias("wealth_accts_cnt")
    )
)

# Division logic on top of pw1
pw1_with_division = pw1.withColumn("division",
    F.when(F.col("Business_Group") == "Private Wealth",
        F.when(
            (F.col("trust_cnt") > 0) & (F.col("banking_cnt") > 0), "Banking & IM&T"
        ).when(
            (F.col("investment_cnt") + F.col("trust_cnt") > 0) & (F.col("banking_cnt") == 0),
            "Investments Only"
        ).otherwise("Banking only")
    )
    .when(F.col("Business_Group") == "Investment Services",
        F.when(
            (F.col("investment_cnt") > 0) & (F.col("insurance_cnt") == 0), "Investment"
        ).when(
            (F.col("investment_cnt") == 0) & (F.col("insurance_cnt") > 0), "Insurance"
        ).otherwise("Insurance & Investment")
    )
    .otherwise(
        F.when(
            (F.col("corporate_trust_cnt") > 0) & (F.col("institutional_trust_cnt") == 0),
            "Corporate Trust"
        ).when(
            (F.col("corporate_trust_cnt") == 0) & (F.col("institutional_trust_cnt") > 0),
            "Institutional Trust"
        ).when(F.col("pwm_cnt") > 0, "Banking only")
        .otherwise("Corporate & Institutional Trust")
    )
)

# =============================================================================
# STEP 5: INVESTPATH ACCOUNT COUNT PER CUSTOMER
# Source: eil.d_involved_party_h (daily), source_system='RN', account_type='IP'
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
# One row per account across all wealth source codes
# ip_ columns populated only for Investpath rows, NULL for all others
# =============================================================================

valid_dates_acct = get_valid_business_dates(
    spark, "eil.d_arrangement_h", START_DT, END_DT
)

ip_a  = spark.table("eil.d_involved_party_h")
a2i_a = spark.table("eil.d_arrangement_to_involved_party_relationship_h")
ar_a  = spark.table("eil.d_arrangement_h")

account_type_expr = (
    F.when(
        (ar_a["source_system_code"] == "RN") & (ar_a["account_type_code"] == "IP"), "Investpath"
    )
    .when(
        ar_a["business_service_segment_type_code"].isin("IS_CT","IS_IT") |
        ar_a["source_system_code"].isin("TR","BI"), "Trust"
    )
    .when(
        ar_a["business_service_segment_type_code"].isin("REGIS_FC","REGIS"), "Investment"
    )
    .when(ar_a["business_service_segment_type_code"] == "PWM", "PWM")
    .when(ar_a["source_system_code"].isin(BANKING_SOURCE_CODES), "Banking")
    .otherwise("Other")
)

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
    .withColumn("account_type", account_type_expr)
    .select(
        # Keys
        ip_a["business_date"].cast("date").alias("business_date"),
        ip_a["rcif_cust_nbr"].cast("string").alias("RCIF_NUMBER"),
        ip_a["involved_party_id"].alias("ip_id"),
        ip_a["cust_internet_banking_nbr"],

        # Account details
        ar_a["arrangement_id"].alias("account_number"),
        ar_a["source_system_code"].alias("source_system"),
        ar_a["account_type_code"],
        ar_a["business_service_segment_type_code"],
        ar_a["current_balance_amt"].alias("balance"),
        ar_a["open_date"],
        ar_a["closed_ind"],
        F.col("account_type"),

        # ip_ columns — only populated when account_type = Investpath
        F.when(F.col("account_type") == "Investpath", ar_a["arrangement_id"])
         .otherwise(F.lit(None).cast("string")).alias("ip_account_number"),
        F.when(F.col("account_type") == "Investpath", ar_a["open_date"])
         .otherwise(F.lit(None).cast("date")).alias("ip_open_date"),
        F.when(F.col("account_type") == "Investpath", ar_a["current_balance_amt"])
         .otherwise(F.lit(None).cast("double")).alias("ip_balance"),
        F.when(F.col("account_type") == "Investpath", ar_a["source_system_code"])
         .otherwise(F.lit(None).cast("string")).alias("ip_source_system"),
        F.when(F.col("account_type") == "Investpath", ar_a["account_type_code"])
         .otherwise(F.lit(None).cast("string")).alias("ip_account_type_code")
    )
)

# =============================================================================
# STEP 7: FINAL CUSTOMER TABLE
# One row per RCIF_NUMBER — full dimension with all flags & counts
# =============================================================================

window_rcif = Window.partitionBy("RCIF_NUMBER").orderBy("cust_internet_banking_nbr")

generation_expr = (
    F.when((F.col("birth_date") >= "1900-01-01") & (F.col("birth_date") <= "1924-12-31"), "GI Generation (1900-1924)")
     .when((F.col("birth_date") >= "1925-01-01") & (F.col("birth_date") <= "1945-12-31"), "Traditionalist (1925-1945)")
     .when((F.col("birth_date") >= "1946-01-01") & (F.col("birth_date") <= "1964-12-31"), "Baby Boomer (1946-1964)")
     .when((F.col("birth_date") >= "1965-01-01") & (F.col("birth_date") <= "1980-12-31"), "Gen X (1965-1980)")
     .when((F.col("birth_date") >= "1981-01-01") & (F.col("birth_date") <= "1996-12-31"), "Millennial (1981-1996)")
     .when(F.col("birth_date") >= "1997-01-01",                                           "Centennial (1997-???)")
     .otherwise("Unknown")
)

wealth_customer = (
    rc
    # Left join digital flags — not all customers are digital users
    .join(
        dig_customer.select(
            F.col("relt_ibn").alias("primary_ibn"),
            F.col("rcif_customer_nbr").alias("RCIF_NUMBER"),
            "ods_business_dt",
            "lst_login_olb",
            "lst_login_mob"
        ),
        ["RCIF_NUMBER"],
        "left"
    )
    # Left join wealth segmentation
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
    # Left join Investpath account count
    .join(ip_accts_cnt, ["RCIF_NUMBER", "ip_id"], "left")
    # Inner join to valid RCIF pool
    .join(
        add_rcifs.withColumnRenamed("rcif_number", "RCIF_NUMBER"),
        ["RCIF_NUMBER"],
        "inner"
    )
    # Deduplicate: one row per RCIF_NUMBER
    .withColumn("_rank", F.row_number().over(window_rcif))
    .filter(F.col("_rank") == 1)
    .drop("_rank")
    # Derived columns
    .withColumn("customer_generation", generation_expr)
    .withColumn("mobile_active_flag",
        F.when(F.datediff(F.col("ods_business_dt"), F.col("lst_login_mob")) <= 90, "Mobile Active")
         .otherwise("Non Mobile Active")
    )
    .withColumn("mobile_flag",
        F.when(F.col("lst_login_mob").isNull(), "Non Mobile User").otherwise("Mobile User")
    )
    .withColumn("olb_active_flag",
        F.when(F.datediff(F.col("ods_business_dt"), F.col("lst_login_olb")) <= 90, "OLB Active")
         .otherwise("Non OLB Active")
    )
    .withColumn("olb_flag",
        F.when(F.col("lst_login_olb").isNull(), "Non OLB User").otherwise("OLB User")
    )
    .withColumn("digitally_active_flag",
        F.when(
            (F.datediff(F.col("ods_business_dt"), F.col("lst_login_mob")) <= 90) |
            (F.datediff(F.col("ods_business_dt"), F.col("lst_login_olb")) <= 90),
            "Digital Active"
        ).otherwise("Non Digital Active")
    )
    .withColumn("digital_flag",
        F.when(F.col("primary_ibn").isNull(), "Non Digital User").otherwise("Digital User")
    )
    # ── Final column layout ──────────────────────────────────────────────────
    .select(
        # ── Bridge keys ──
        F.col("business_date"),
        F.col("RCIF_NUMBER"),
        F.col("ip_id"),                                     # bridge to Account table

        # ── Identity ──
        F.col("involved_party_name").alias("customer_name"),
        F.col("involved_party_tax_id_nbr").alias("tax_id_nbr"),
        F.col("birth_date"),
        F.col("customer_generation"),

        # ── Both IBNs ──
        F.col("cust_internet_banking_nbr").alias("olb_ibn"),    # OLB internet banking nbr
        F.col("primary_ibn"),                                    # mobile / digital IBN

        # ── Location ──
        F.col("city_name"),
        F.col("state_name"),
        F.col("country_name"),

        # ── Wealth segmentation ──
        F.col("Business_Group").alias("business_group"),
        F.col("division"),

        # ── Account counts ──
        F.coalesce(F.col("wealth_accts_cnt"), F.lit(0)).alias("wealth_accts_cnt"),   # all wealth sources
        F.coalesce(F.col("ip_accts_cnt"),     F.lit(0)).alias("ip_accts_cnt"),       # Investpath only
        F.coalesce(F.col("banking_cnt"),       F.lit(0)).alias("banking_cnt"),
        F.coalesce(F.col("investment_cnt"),    F.lit(0)).alias("investment_cnt"),
        F.coalesce(F.col("trust_cnt"),         F.lit(0)).alias("trust_cnt"),
        F.coalesce(F.col("insurance_cnt"),     F.lit(0)).alias("insurance_cnt"),
        F.coalesce(F.col("pwm_cnt"),           F.lit(0)).alias("pwm_cnt"),
        F.coalesce(F.col("corporate_trust_cnt"),     F.lit(0)).alias("corporate_trust_cnt"),
        F.coalesce(F.col("institutional_trust_cnt"), F.lit(0)).alias("institutional_trust_cnt"),

        # ── Digital flags ──
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
# STEP 8: FINAL ACCOUNT TABLE
# One row per account — enriched with customer context
# ip_ columns populated for Investpath rows only, NULL elsewhere
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
            F.col("olb_ibn").alias("olb_ibn_cust"),
            "state_name"
        ),
        ["RCIF_NUMBER", "ip_id"],
        "left"
    )
    .select(
        # ── Bridge keys ──
        F.col("business_date"),
        F.col("RCIF_NUMBER"),
        F.col("ip_id"),                                     # bridge to Customer table

        # ── IBNs ──
        F.col("cust_internet_banking_nbr").alias("olb_ibn"),
        F.col("primary_ibn"),

        # ── Account details ──
        F.col("account_number"),
        F.col("account_type"),                              # Banking/Investment/Trust/PWM/Investpath
        F.col("account_type_code"),
        F.col("source_system"),
        F.col("business_service_segment_type_code"),
        F.col("open_date"),
        F.col("balance"),
        F.col("closed_ind"),

        # ── Investpath-specific columns (NULL for non-Investpath rows) ──
        F.col("ip_account_number"),
        F.col("ip_open_date"),
        F.col("ip_balance"),
        F.col("ip_source_system"),
        F.col("ip_account_type_code"),

        # ── Customer context ──
        F.col("business_group"),
        F.col("division"),
        F.col("state_name"),
        F.col("customer_generation"),

        # ── Digital flags ──
        F.col("digital_flag"),
        F.col("digitally_active_flag"),
        F.col("mobile_active_flag"),
        F.col("mobile_flag"),
        F.col("olb_active_flag"),
        F.col("olb_flag"),

        # ── Account counts (from customer) ──
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
    .saveAsTable(f"{final_db}.{final_table_customer}")
)

(
    wealth_account
    .write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{final_db}.{final_table_account}")
)

print(f"✅ {final_db}.{final_table_customer} written — {wealth_customer.count()} rows")
print(f"✅ {final_db}.{final_table_account} written  — {wealth_account.count()} rows")

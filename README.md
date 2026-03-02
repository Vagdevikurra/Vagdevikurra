# =============================================================================
# Wealth Insights - Two Output Tables
# Date Range: 2025-08-01 to 2026-01-31
# Python 3.6 compatible
#
# TABLE 1: wealth_Insights_Customer  — one row per RCIF_NUMBER
# TABLE 2: wealth_Insights_Account   — one row per ip_account_number
#
# BRIDGE KEYS: RCIF_NUMBER + ip_id
# PC (plastic cards) and BW excluded from all source codes
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
# HELPERS
# =============================================================================

def parse_date(s):
    return datetime.strptime(s, "%Y-%m-%d").date()

def get_valid_business_dates(spark, table, start, end):
    """
    Returns all existing business dates in range ensuring every
    month-end is represented — rolls back to nearest prior business
    day if month-end falls on weekend/holiday.
    Covers: Aug-31, Sep-30, Oct-31, Nov-30, Dec-31, Jan-31
    """
    existing = {
        row["dt"] for row in (
            spark.table(table)
            .filter(
                (F.col("business_date").cast("date") >= F.lit(start)) &
                (F.col("business_date").cast("date") <= F.lit(end))
            )
            .select(F.col("business_date").cast("date").alias("dt"))
            .distinct()
            .collect()
        )
    }
    start_d, end_d = parse_date(start), parse_date(end)

    month_ends, y, m = set(), start_d.year, start_d.month
    while date(y, m, 1) <= end_d:
        me = date(y, m, calendar.monthrange(y, m)[1])
        if start_d <= me <= end_d:
            month_ends.add(me)
        if m == 12: y += 1; m = 1
        else: m += 1

    final_dates = set(existing)
    for me in month_ends:
        if me not in existing:
            c = me - timedelta(days=1)
            while c >= start_d:
                if c in existing:
                    final_dates.add(c)
                    break
                c -= timedelta(days=1)

    return [str(d) for d in sorted(final_dates)]


# =============================================================================
# CONSTANTS  (PC and BW excluded)
# =============================================================================

WEALTH_SOURCE_CODES = [
    'DA','SV','CC','MG','LS','TM','LO','CM','CS','EL',
    'IC','MA','PF','PR','SD',                           # Banking
    'TR','BI',                                           # Trust
    'RN',                                                # Investpath
    'IS_CT','IS_IT','IS_IF','PNB'                        # Institutional
]

BANKING_SOURCE_CODES = [
    'DA','SV','CC','MG','LS','TM','LO',
    'CM','CS','EL','IC','MA','PF','PR','SD'
]

WEALTH_SEGMENT_CODES = ['IS_CT','IS_IT','REGIS_FC','REGIS','PWM']

# =============================================================================
# MAX DATES
# =============================================================================

# Get one representative month-end date per month in range
# These are the dates that will appear in the Customer table
# matching Calendar[Month_Ceiling]
all_valid_dates = get_valid_business_dates(
    spark, "eil.d_involved_party_h", START_DT, END_DT
)

# From all valid dates, keep only the LAST date of each month
# This gives us Aug-31, Sep-30, Oct-31, Nov-30, Dec-31, Jan-30/31
from itertools import groupby

def month_key(d):
    dt = parse_date(d)
    return (dt.year, dt.month)

month_end_dates = []
for _, group in groupby(sorted(all_valid_dates), key=month_key):
    month_dates = list(group)
    month_end_dates.append(month_dates[-1])  # last date = month-end

# Also use max for single-date queries (account table etc.)
max_ip_date = month_end_dates[-1]

print("Monthly snapshot dates: {}".format(month_end_dates))

max_dbm_dt = (
    spark.table("dm_ib.digital_banking_master")
    .agg(F.max("ods_business_dt"))
    .collect()[0][0]
)

# =============================================================================
# STEP 1: RCIF POOL
# =============================================================================

six_months_ago = F.add_months(F.current_date(), -6)

add_rcifs = (
    spark.table("eil.m_involved_party_h")
    .filter(F.col("business_date").cast("date") >= six_months_ago)
    .select(F.col("rcif_cust_nbr").alias("RCIF_NUMBER"))
    .union(
        spark.table("dm_ib.digital_banking_master")
        .filter(F.col("ods_business_dt") >= six_months_ago)
        .select(F.col("rcif_customer_nbr").alias("RCIF_NUMBER"))
    )
    .distinct()
    .filter(F.col("RCIF_NUMBER").isNotNull() & (F.col("RCIF_NUMBER") != ""))
)

# =============================================================================
# STEP 2: WEALTH SEGMENTATION
# Monthly table — join first, filter eligibility after
# Aggregate to ONE row per RCIF_NUMBER + ip_id to prevent fan-out
# =============================================================================

valid_dates_m = get_valid_business_dates(
    spark, "eil.m_involved_party_h", START_DT, END_DT
)

ip_m  = spark.table("eil.m_involved_party_h")
a2i_m = spark.table("eil.m_arrangement_to_involved_party_relationship_h")
ar_m  = spark.table("eil.m_arrangement_h")

pw1 = (
    ip_m
    .filter(
        F.col("business_date").isin(valid_dates_m) &
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
    # Disambiguate columns in both a2i_m and ar_m
    .withColumn("ar_id",  ar_m["arrangement_id"])
    .withColumn("ar_src", ar_m["source_system_code"])
    .withColumn("ar_bss", ar_m["business_service_segment_type_code"])
    # Eligibility filter AFTER join
    .filter(
        F.when(F.col("private_client_code").isin("039","539","339"),    F.lit(1))
         .when(F.col("private_client_trust_code").isin("239","739"),    F.lit(1))
         .otherwise(
             F.when(F.col("ar_bss").isin(WEALTH_SEGMENT_CODES), F.lit(1))
              .otherwise(F.lit(0))
         ) == 1
    )
    .withColumn("business_group",
        F.when(F.col("private_client_code").isin("039","539","339"),    F.lit("Private Wealth"))
         .when(F.col("private_client_trust_code").isin("239","739"),    F.lit("Private Wealth"))
         .otherwise(
             F.when(F.col("ar_bss").isin("IS_CT","IS_IT"),              F.lit("Institutional Services"))
              .when(F.col("ar_bss").isin("REGIS_FC","REGIS"),           F.lit("Investment Services"))
              .when(F.col("ar_bss") == "PWM",                           F.lit("Private Wealth"))
              .otherwise(F.coalesce(F.col("ar_bss"),                    F.lit("Category???")))
         )
    )
    # ONE row per RCIF_NUMBER + ip_id
    .groupBy(
        ip_m["rcif_cust_nbr"].cast("string").alias("RCIF_NUMBER"),
        ip_m["involved_party_id"].alias("ip_id")
    )
    .agg(
        F.first("business_group").alias("business_group"),
        F.countDistinct(
            F.when(F.col("ar_bss") == "IS_CT",               F.col("ar_id"))
        ).alias("corporate_trust_cnt"),
        F.countDistinct(
            F.when(F.col("ar_bss") == "IS_IT",               F.col("ar_id"))
        ).alias("institutional_trust_cnt"),
        F.countDistinct(
            F.when(F.col("ar_bss").isin("REGIS_FC","REGIS"), F.col("ar_id"))
        ).alias("investment_cnt"),
        F.countDistinct(
            F.when(F.col("ar_bss") == "REGIS",               F.col("ar_id"))
        ).alias("insurance_cnt"),
        F.countDistinct(
            F.when(F.col("ar_bss") == "PWM",                 F.col("ar_id"))
        ).alias("pwm_cnt"),
        F.countDistinct(
            F.when(F.col("ar_src") == "TR",                  F.col("ar_id"))
        ).alias("trust_cnt"),
        F.countDistinct(
            F.when(F.col("ar_src").isin(BANKING_SOURCE_CODES), F.col("ar_id"))
        ).alias("banking_cnt"),
        F.countDistinct(F.col("ar_id")).alias("wealth_accts_cnt")
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
# STEP 3: CUSTOMER BASE  (daily — max date only, no date explosion)
# =============================================================================

ip_d   = spark.table("eil.d_involved_party_h")
a2i_d  = spark.table("eil.d_arrangement_to_involved_party_relationship_h")
ar_d   = spark.table("eil.d_arrangement_h")
addr_d = spark.table("eil.d_involved_party_address_h")

def build_snapshot(snap_date):
    """Build one row per RCIF for a given month-end date."""
    return (
        ip_d
        .filter(
            (F.col("business_date") == snap_date) &
            (F.col("source_system_code") == "CF") &
            (F.coalesce(F.col("deceased_ind"), F.lit("N")) == "N")
        )
        .join(
            a2i_d.filter(F.col("business_date") == snap_date),
            (ip_d["involved_party_id"]  == a2i_d["involved_party_id"]) &
            (ip_d["business_date"]      == a2i_d["business_date"]) &
            (ip_d["source_system_code"] == a2i_d["source_system_code"]),
            "inner"
        )
        .join(
            ar_d.filter(
                (F.col("business_date") == snap_date) &
                F.col("source_system_code").isin(WEALTH_SOURCE_CODES)
            ),
            (a2i_d["arrangement_id"]                 == ar_d["arrangement_id"]) &
            (a2i_d["arrangement_source_system_code"] == ar_d["source_system_code"]) &
            (a2i_d["business_date"]                  == ar_d["business_date"]),
            "inner"
        )
        .join(
            addr_d.filter(F.col("business_date") == snap_date),
            (ip_d["involved_party_id"] == addr_d["involved_party_id"]) &
            (ip_d["business_date"]     == addr_d["business_date"]),
            "inner"
        )
        .groupBy(
            ip_d["rcif_cust_nbr"].cast("string").alias("RCIF_NUMBER")
        )
        .agg(
            F.lit(snap_date).cast(T.DateType()).alias("business_date"),
            F.first(ip_d["involved_party_id"]).alias("ip_id"),
            F.first(addr_d["state_name"]).alias("state_name")
        )
    )

# Build one snapshot per month-end and union all together
# Result: one row per RCIF per month ~265K x 6 = ~1.5M rows
rc_snapshots = [build_snapshot(d) for d in month_end_dates]
rc = rc_snapshots[0]
for snap in rc_snapshots[1:]:
    rc = rc.union(snap)

# =============================================================================
# STEP 4: INVESTPATH ACCOUNT COUNT PER CUSTOMER
# =============================================================================

ind_inv = spark.table("eil.d_involved_party_h")
a2i_inv = spark.table("eil.d_arrangement_to_involved_party_relationship_h")
ar_inv  = spark.table("eil.d_arrangement_h")

ip_accts_cnt = (
    ind_inv
    .filter(
        F.col("business_date").isin(month_end_dates) &
        (F.col("source_system_code") == "CF") &
        (F.coalesce(F.col("deceased_ind"), F.lit("N")) == "N")
    )
    .join(
        a2i_inv.filter(F.col("business_date").isin(month_end_dates)),
        (ind_inv["involved_party_id"]  == a2i_inv["involved_party_id"]) &
        (ind_inv["business_date"]      == a2i_inv["business_date"]) &
        (ind_inv["source_system_code"] == a2i_inv["source_system_code"]),
        "inner"
    )
    .join(
        ar_inv.filter(
            F.col("business_date").isin(month_end_dates) &
            (F.col("closed_ind")        == "N") &
            (F.col("account_type_code") == "IP") &
            (F.col("source_system_code")== "RN")
        ),
        (a2i_inv["arrangement_id"]                 == ar_inv["arrangement_id"]) &
        (a2i_inv["arrangement_source_system_code"] == ar_inv["source_system_code"]) &
        (a2i_inv["business_date"]                  == ar_inv["business_date"]),
        "inner"
    )
    .withColumn("ar_inv_id", ar_inv["arrangement_id"])
    .groupBy(
        ind_inv["rcif_cust_nbr"].cast("string").alias("RCIF_NUMBER"),
        ind_inv["involved_party_id"].alias("ip_id"),
        ind_inv["business_date"].cast(T.DateType()).alias("business_date")
    )
    .agg(F.countDistinct(F.col("ar_inv_id")).alias("ip_accts_cnt"))
)

# =============================================================================
# STEP 5: DIGITAL FLAGS  (latest snapshot only)
# =============================================================================

dig_latest = (
    spark.table("dm_ib.digital_banking_master")
    .filter(F.col("ods_business_dt") == max_dbm_dt)
    .groupBy(F.col("rcif_customer_nbr").alias("RCIF_NUMBER"))
    .agg(
        F.max("ibn").alias("primary_ibn"),
        F.max("olb_last_login_date").alias("lst_login_olb"),
        F.max("mob_last_login_date").alias("lst_login_mob"),
        F.max("ods_business_dt").alias("ods_business_dt")
    )
)

# =============================================================================
# TABLE 1: CUSTOMER
# One row per RCIF_NUMBER — strictly enforced via row_number
# =============================================================================

# Dedup per RCIF per month — allows one row per RCIF per business_date
window_dedup = Window.partitionBy("RCIF_NUMBER", "business_date").orderBy(
    F.col("RCIF_NUMBER")
)

wealth_customer = (
    rc
    .join(pw1,          ["RCIF_NUMBER", "ip_id"],                "inner")
    .join(ip_accts_cnt, ["RCIF_NUMBER", "ip_id", "business_date"], "left")
    .join(dig_latest,   ["RCIF_NUMBER"],                           "left")
    # Strictly one row per RCIF_NUMBER
    .withColumn("_rank", F.row_number().over(window_dedup))
    .filter(F.col("_rank") == 1)
    .drop("_rank")
    # Digital flags
    .withColumn("mobile_active_flag",
        F.when(F.datediff(F.col("ods_business_dt"), F.col("lst_login_mob")) <= 90,
               F.lit("Mobile Active")).otherwise(F.lit("Non Mobile Active"))
    )
    .withColumn("mobile_flag",
        F.when(F.col("lst_login_mob").isNull(), F.lit("Non Mobile User"))
         .otherwise(F.lit("Mobile User"))
    )
    .withColumn("olb_active_flag",
        F.when(F.datediff(F.col("ods_business_dt"), F.col("lst_login_olb")) <= 90,
               F.lit("OLB Active")).otherwise(F.lit("Non OLB Active"))
    )
    .withColumn("olb_flag",
        F.when(F.col("lst_login_olb").isNull(), F.lit("Non OLB User"))
         .otherwise(F.lit("OLB User"))
    )
    .withColumn("digitally_active_flag",
        F.when(
            (F.datediff(F.col("ods_business_dt"), F.col("lst_login_mob")) <= 90) |
            (F.datediff(F.col("ods_business_dt"), F.col("lst_login_olb")) <= 90),
            F.lit("Digital Active")
        ).otherwise(F.lit("Non Digital Active"))
    )
    .withColumn("digital_flag",
        F.when(F.col("primary_ibn").isNull(), F.lit("Non Digital User"))
         .otherwise(F.lit("Digital User"))
    )
    .select(
        # Bridge keys
        F.col("RCIF_NUMBER"),
        F.col("ip_id"),
        # Core columns
        F.col("business_date"),
        F.col("primary_ibn"),
        F.col("state_name"),
        F.col("business_group"),
        F.col("division"),
        # Account counts
        F.coalesce(F.col("wealth_accts_cnt"), F.lit(0)).alias("wealth_accts_cnt"),
        F.coalesce(F.col("ip_accts_cnt"),     F.lit(0)).alias("ip_accts_cnt"),
        # Digital flags
        F.col("digital_flag"),
        F.col("digitally_active_flag"),
        F.col("mobile_flag"),
        F.col("mobile_active_flag"),
        F.col("olb_flag"),
        F.col("olb_active_flag")
    )
)

# =============================================================================
# TABLE 2: INVESTPATH ACCOUNT
# One row per ip_account_number — exactly from original Investpath query
# =============================================================================

ind_a  = spark.table("eil.d_involved_party_h")
a2i_a  = spark.table("eil.d_arrangement_to_involved_party_relationship_h")
ar_a   = spark.table("eil.d_arrangement_h")

wealth_account = (
    ind_a
    .filter(
        (F.col("business_date") == max_ip_date) &
        (F.col("source_system_code") == "CF") &
        (F.coalesce(F.col("deceased_ind"), F.lit("N")) == "N")
    )
    .join(
        a2i_a.filter(F.col("business_date") == max_ip_date),
        (ind_a["involved_party_id"]  == a2i_a["involved_party_id"]) &
        (ind_a["business_date"]      == a2i_a["business_date"]) &
        (ind_a["source_system_code"] == a2i_a["source_system_code"]),
        "inner"
    )
    .join(
        ar_a.filter(
            (F.col("business_date") == max_ip_date) &
            (F.col("closed_ind")        == "N") &
            (F.col("account_type_code") == "IP") &
            (F.col("source_system_code")== "RN")
        ),
        (a2i_a["arrangement_id"]                 == ar_a["arrangement_id"]) &
        (a2i_a["arrangement_source_system_code"] == ar_a["source_system_code"]) &
        (a2i_a["business_date"]                  == ar_a["business_date"]),
        "inner"
    )
    # Disambiguate ar_a columns
    .withColumn("ar_account_number", ar_a["arrangement_id"])
    .withColumn("ar_open_date",      ar_a["open_date"])
    .withColumn("ar_balance",        ar_a["current_balance_amt"])
    # is_funded — derived for Power BI KPI "InvestPath Accounts Funded"
    .withColumn("is_funded",
        F.when(ar_a["current_balance_amt"] > 0, F.lit("Funded"))
         .otherwise(F.lit("Not Funded"))
    )
    .select(
        # Bridge keys
        ind_a["rcif_cust_nbr"].cast("string").alias("RCIF_NUMBER"),
        ind_a["involved_party_id"].alias("ip_id"),
        # Investpath columns (from original query)
        F.col("ar_account_number").alias("ip_account_number"),
        F.col("ar_open_date").alias("ip_open_date"),
        F.col("ar_balance").alias("ip_balance"),
        F.col("is_funded")
    )
)

# =============================================================================
# TABLE 3: SLIM DIGITAL TABLE
# Source: dm_ib.digital_banking_master
# Purpose: Powers "Top of Company Total Digital Active" KPI = 3.5M
# One row per RCIF per month — ALL company customers not just wealth
# Columns: RCIF_NUMBER, business_date, ibn, digitallyactiveflag
# =============================================================================

digital = (
    spark.table("dm_ib.digital_banking_master")
    .filter(
        (F.col("ods_business_dt").cast("date") >= F.lit(START_DT)) &
        (F.col("ods_business_dt").cast("date") <= F.lit(END_DT))
    )
    .groupBy(
        F.col("rcif_customer_nbr").alias("RCIF_NUMBER"),
        F.col("ods_business_dt").alias("business_date"),
        F.col("ibn")
    )
    .agg(
        F.max("olb_last_login_date").alias("lst_login_olb"),
        F.max("mob_last_login_date").alias("lst_login_mob")
    )
    .withColumn("digitallyactiveflag",
        F.when(
            (F.datediff(F.col("business_date"), F.col("lst_login_mob")) <= 90) |
            (F.datediff(F.col("business_date"), F.col("lst_login_olb")) <= 90),
            F.lit("Digital Active")
        ).otherwise(F.lit("Non Digital Active"))
    )
    .select(
        "RCIF_NUMBER",
        "business_date",
        "ibn",
        "digitallyactiveflag"
    )
)

# =============================================================================
# WRITE TABLES
# =============================================================================

wealth_customer.write.mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("{}.{}".format(final_db, final_table_customer))

wealth_account.write.mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("{}.{}".format(final_db, final_table_account))

digital.write.mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("{}.{}".format(final_db, "wealth_Insights_Digital"))

print("Customer rows : {}".format(wealth_customer.count()))
print("Account rows  : {}".format(wealth_account.count()))
print("Digital rows  : {}".format(digital.count()))

# =============================================================================
# Wealth Insights - Two Output Tables
# Date Range: 2025-08-01 to 2026-01-31
# Python 3.6 compatible
#
# TABLE 1: dm_ib_dev.wealth_Insights_Customer  — one row per RCIF_NUMBER
# TABLE 2: dm_ib_dev.wealth_Insights_Account   — one row per ip_account_number
#
# KEY FIX: all 90-day active flags guard against future login dates
#   WRONG: datediff(snap, login) <= 90   ← negative passes when login > snap!
#   RIGHT: login <= snap AND datediff(snap, login) <= 90
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
# CONSTANTS  (PC and BW excluded everywhere)
# =============================================================================

WEALTH_SOURCE_CODES = [
    'DA','SV','CC','MG','LS','TM','LO','CM','CS','EL',
    'IC','MA','PF','PR','SD',
    'TR','BI',
    'RN',
    'IS_CT','IS_IT','IS_IF','PNB'
]
BANKING_SOURCE_CODES = [
    'DA','SV','CC','MG','LS','TM','LO',
    'CM','CS','EL','IC','MA','PF','PR','SD'
]
WEALTH_SEGMENT_CODES = ['IS_CT','IS_IT','REGIS_FC','REGIS','PWM']

# =============================================================================
# HELPER: compute 90-day active flag safely (guards against future dates)
# =============================================================================

def active_90(snap_col, login_col):
    """
    Returns "Active" only when:
      - login_col is not null
      - login_col <= snap_col  (login is NOT in the future)
      - snap_col - login_col <= 90 days
    Without the second guard, datediff returns negative for future logins,
    and negative <= 90 is TRUE — causing inflated counts in older months.
    """
    return (
        F.col(login_col).isNotNull() &
        (F.col(login_col) <= F.col(snap_col)) &
        (F.datediff(F.col(snap_col), F.col(login_col)) <= 90)
    )


# =============================================================================
# MAX DATES
# =============================================================================

max_ip_date = (
    spark.table("eil.d_involved_party_h")
    .filter(
        (F.col("business_date").cast("date") >= F.lit(START_DT)) &
        (F.col("business_date").cast("date") <= F.lit(END_DT))
    )
    .agg(F.max("business_date"))
    .collect()[0][0]
)

max_dbm_dt = (
    spark.table("dm_ib.digital_banking_master")
    .filter(
        (F.col("ods_business_dt").cast("date") >= F.lit(START_DT)) &
        (F.col("ods_business_dt").cast("date") <= F.lit(END_DT))
    )
    .agg(F.max("ods_business_dt"))
    .collect()[0][0]
)

print("max_ip_date  :", max_ip_date)
print("max_dbm_dt   :", max_dbm_dt)

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
# STEP 2: WEALTH SEGMENTATION  (monthly, one row per RCIF + ip_id)
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
    .withColumn("ar_id",  ar_m["arrangement_id"])
    .withColumn("ar_src", ar_m["source_system_code"])
    .withColumn("ar_bss", ar_m["business_service_segment_type_code"])
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
    .groupBy(
        ip_m["rcif_cust_nbr"].cast("string").alias("RCIF_NUMBER"),
        ip_m["involved_party_id"].alias("ip_id")
    )
    .agg(
        F.first("business_group").alias("business_group"),
        F.countDistinct(F.when(F.col("ar_bss") == "IS_CT",               F.col("ar_id"))).alias("corporate_trust_cnt"),
        F.countDistinct(F.when(F.col("ar_bss") == "IS_IT",               F.col("ar_id"))).alias("institutional_trust_cnt"),
        F.countDistinct(F.when(F.col("ar_bss").isin("REGIS_FC","REGIS"), F.col("ar_id"))).alias("investment_cnt"),
        F.countDistinct(F.when(F.col("ar_bss") == "REGIS",               F.col("ar_id"))).alias("insurance_cnt"),
        F.countDistinct(F.when(F.col("ar_bss") == "PWM",                 F.col("ar_id"))).alias("pwm_cnt"),
        F.countDistinct(F.when(F.col("ar_src") == "TR",                  F.col("ar_id"))).alias("trust_cnt"),
        F.countDistinct(F.when(F.col("ar_src").isin(BANKING_SOURCE_CODES), F.col("ar_id"))).alias("banking_cnt"),
        F.countDistinct(F.col("ar_id")).alias("wealth_accts_cnt")
    )
    .withColumn("division",
        F.when(F.col("business_group") == "Private Wealth",
            F.when((F.col("trust_cnt") > 0) & (F.col("banking_cnt") > 0), F.lit("Banking & IM&T"))
             .when((F.col("investment_cnt") + F.col("trust_cnt") > 0) & (F.col("banking_cnt") == 0), F.lit("Investments Only"))
             .otherwise(F.lit("Banking only"))
        )
        .when(F.col("business_group") == "Investment Services",
            F.when((F.col("investment_cnt") > 0) & (F.col("insurance_cnt") == 0), F.lit("Investment"))
             .when((F.col("investment_cnt") == 0) & (F.col("insurance_cnt") > 0), F.lit("Insurance"))
             .otherwise(F.lit("Insurance & Investment"))
        )
        .otherwise(
            F.when((F.col("corporate_trust_cnt") > 0) & (F.col("institutional_trust_cnt") == 0), F.lit("Corporate Trust"))
             .when((F.col("corporate_trust_cnt") == 0) & (F.col("institutional_trust_cnt") > 0), F.lit("Institutional Trust"))
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

rc = (
    ip_d
    .filter(
        (F.col("business_date") == max_ip_date) &
        (F.col("source_system_code") == "CF") &
        (F.coalesce(F.col("deceased_ind"), F.lit("N")) == "N")
    )
    .join(
        a2i_d.filter(F.col("business_date") == max_ip_date),
        (ip_d["involved_party_id"]  == a2i_d["involved_party_id"]) &
        (ip_d["business_date"]      == a2i_d["business_date"]) &
        (ip_d["source_system_code"] == a2i_d["source_system_code"]),
        "inner"
    )
    .join(
        ar_d.filter(
            (F.col("business_date") == max_ip_date) &
            F.col("source_system_code").isin(WEALTH_SOURCE_CODES)
        ),
        (a2i_d["arrangement_id"]                 == ar_d["arrangement_id"]) &
        (a2i_d["arrangement_source_system_code"] == ar_d["source_system_code"]) &
        (a2i_d["business_date"]                  == ar_d["business_date"]),
        "inner"
    )
    .join(
        addr_d.filter(F.col("business_date") == max_ip_date),
        (ip_d["involved_party_id"] == addr_d["involved_party_id"]) &
        (ip_d["business_date"]     == addr_d["business_date"]),
        "inner"
    )
    .groupBy(
        ip_d["involved_party_id"].alias("ip_id"),
        ip_d["cust_internet_banking_nbr"]
    )
    .agg(
        F.max(ip_d["business_date"].cast("date")).alias("business_date"),
        F.max(ip_d["rcif_cust_nbr"].cast("string")).alias("RCIF_NUMBER"),
        F.first(addr_d["state_name"]).alias("state_name")
    )
)

# =============================================================================
# STEP 4: INVESTPATH ACCOUNT COUNT PER CUSTOMER
# =============================================================================

ind_inv = spark.table("eil.d_involved_party_h")
a2i_inv = spark.table("eil.d_arrangement_to_involved_party_relationship_h")
ar_inv  = spark.table("eil.d_arrangement_h")

ip_accts_cnt = (
    ind_inv
    .filter(
        (F.col("business_date") == max_ip_date) &
        (F.col("source_system_code") == "CF") &
        (F.coalesce(F.col("deceased_ind"), F.lit("N")) == "N")
    )
    .join(
        a2i_inv.filter(F.col("business_date") == max_ip_date),
        (ind_inv["involved_party_id"]  == a2i_inv["involved_party_id"]) &
        (ind_inv["business_date"]      == a2i_inv["business_date"]) &
        (ind_inv["source_system_code"] == a2i_inv["source_system_code"]),
        "inner"
    )
    .join(
        ar_inv.filter(
            (F.col("business_date") == max_ip_date) &
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
        ind_inv["involved_party_id"].alias("ip_id")
    )
    .agg(F.countDistinct(F.col("ar_inv_id")).alias("ip_accts_cnt"))
)

# =============================================================================
# STEP 5: DIGITAL FLAGS — latest snapshot only, for Customer table
#
# Uses active_90() which guards: login <= snap AND datediff <= 90
# This prevents future login dates from inflating historical months
# =============================================================================

dig_latest = (
    spark.table("dm_ib.digital_banking_master")
    .filter(F.col("ods_business_dt") == max_dbm_dt)
    .groupBy(F.col("rcif_customer_nbr").alias("RCIF_NUMBER"))
    .agg(
        F.max("ibn").alias("customer_banking_number"),
        F.max("olb_last_login_date").alias("lst_login_olb"),
        F.max("mob_last_login_date").alias("lst_login_mob"),
        F.max("ods_business_dt").alias("snap_dt")
    )
    # Apply safe 90-day flags
    .withColumn("olb_active_flag",
        F.when(active_90("snap_dt", "lst_login_olb"), F.lit("OLB Active"))
         .otherwise(F.lit("Non OLB Active"))
    )
    .withColumn("mob_active_flag",
        F.when(active_90("snap_dt", "lst_login_mob"), F.lit("Mobile Active"))
         .otherwise(F.lit("Non Mobile Active"))
    )
    .withColumn("digitally_active_flag",
        F.when(
            active_90("snap_dt", "lst_login_mob") | active_90("snap_dt", "lst_login_olb"),
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
        F.when(F.col("customer_banking_number").isNotNull(), F.lit("Digital Enrolled"))
         .otherwise(F.lit("Non Digital Enrolled"))
    )
)

# =============================================================================
# TABLE 1: CUSTOMER — one row per RCIF_NUMBER
# =============================================================================

window_dedup = Window.partitionBy("RCIF_NUMBER").orderBy(
    F.col("cust_internet_banking_nbr").desc_nulls_last()
)

wealth_customer = (
    rc
    .join(pw1,          ["RCIF_NUMBER", "ip_id"], "left")
    .join(ip_accts_cnt, ["RCIF_NUMBER", "ip_id"], "left")
    .join(dig_latest,   ["RCIF_NUMBER"],           "left")
    .join(add_rcifs,    ["RCIF_NUMBER"],            "inner")
    .withColumn("_rank", F.row_number().over(window_dedup))
    .filter(F.col("_rank") == 1)
    .drop("_rank")
    .select(
        F.col("RCIF_NUMBER"),
        F.col("ip_id"),
        F.col("business_date"),
        F.col("customer_banking_number"),
        F.col("state_name"),
        F.col("business_group"),
        F.col("division"),
        F.coalesce(F.col("wealth_accts_cnt"), F.lit(0)).alias("wealth_accts_cnt"),
        F.coalesce(F.col("ip_accts_cnt"),     F.lit(0)).alias("ip_accts_cnt"),
        F.col("digital_enrolled"),
        F.col("digitally_active_flag"),
        F.col("olb_enrolled"),
        F.col("olb_active_flag"),
        F.col("mob_enrolled"),
        F.col("mob_active_flag"),
        F.col("lst_login_olb").alias("last_olb_login"),
        F.col("lst_login_mob").alias("last_mob_login")
    )
)

# =============================================================================
# TABLE 2: INVESTPATH ACCOUNT — one row per ip_account_number
# =============================================================================

ind_a  = spark.table("eil.d_involved_party_h")
a2i_a  = spark.table("eil.d_arrangement_to_involved_party_relationship_h")
ar_a   = spark.table("eil.d_arrangement_h")
addr_a = spark.table("eil.d_involved_party_address_h")

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
    .join(
        addr_a.filter(F.col("business_date") == max_ip_date),
        (ind_a["involved_party_id"] == addr_a["involved_party_id"]) &
        (ind_a["business_date"]     == addr_a["business_date"]),
        "left"
    )
    .withColumn("ar_account_number", ar_a["arrangement_id"])
    .withColumn("ar_open_date",      ar_a["open_date"])
    .withColumn("ar_balance",        ar_a["current_balance_amt"])
    .withColumn("is_funded",
        F.when(ar_a["current_balance_amt"] > 0, F.lit("Funded"))
         .otherwise(F.lit("Not Funded"))
    )
    .select(
        ind_a["rcif_cust_nbr"].cast("string").alias("RCIF_NUMBER"),
        ind_a["involved_party_id"].alias("ip_id"),
        F.col("ar_account_number").alias("ip_account_number"),
        F.col("ar_open_date").alias("ip_open_date"),
        F.col("ar_balance").alias("ip_balance"),
        F.col("is_funded"),
        addr_a["state_name"].alias("state_name")
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

print("Customer rows : {}".format(wealth_customer.count()))
print("Account rows  : {}".format(wealth_account.count()))

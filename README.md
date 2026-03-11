# =============================================================================
# Wealth Insights — Customer & Account Tables
# Date Range  : 2025-09-01 to 2026-02-28
# Digital Source: dm_ib.digital_banking_master ONLY (loaded from 2025-08-02)
#
# OUTPUT TABLES:
#   dm_ib_dev.wealth_Insights_Customer
#   dm_ib_dev.wealth_Insights_Account
#
# DIGITAL JOIN  : EIL.cust_internet_banking_nbr == DBM.ibn  (per month)
# ACTIVE LOGIC  : datediff(dbm_snap_dt, last_login_date) <= 90
# PC and BW excluded from all source codes
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

START_DT             = "2025-09-01"
END_DT               = "2026-02-28"
final_db             = "dm_ib_dev"
final_table_customer = "wealth_Insights_Customer"
final_table_account  = "wealth_Insights_Account"

# =============================================================================
# CONSTANTS  (PC = plastic cards and BW excluded)
# =============================================================================

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
# STEP 1 — MONTH-END DATES
# For each calendar month in range, find the last business day that actually
# exists in the daily table (handles weekend/holiday month-ends)
# =============================================================================

def get_month_end_dates(spark, table, start, end):
    existing = set()
    for row in (
        spark.table(table)
        .filter(
            (F.col("business_date").cast("date") >= F.lit(start)) &
            (F.col("business_date").cast("date") <= F.lit(end))
        )
        .select(F.col("business_date").cast("date").alias("dt"))
        .distinct()
        .collect()
    ):
        existing.add(row["dt"])

    start_d = datetime.strptime(start, "%Y-%m-%d").date()
    end_d   = datetime.strptime(end,   "%Y-%m-%d").date()

    result = []
    y, m = start_d.year, start_d.month
    while date(y, m, 1) <= end_d:
        last_day = calendar.monthrange(y, m)[1]
        me = date(y, m, last_day)
        if start_d <= me <= end_d:
            target = me
            while target >= start_d:
                if target in existing:
                    result.append(str(target))
                    break
                target -= timedelta(days=1)
        if m == 12:
            y += 1; m = 1
        else:
            m += 1

    return sorted(set(result))

month_end_dates = get_month_end_dates(
    spark, "eil.d_involved_party_h", START_DT, END_DT
)
max_ip_date = month_end_dates[-1]

print("Month-end dates : {}".format(month_end_dates))
print("Max IP date     : {}".format(max_ip_date))

# =============================================================================
# STEP 2 — DIGITAL BANKING MASTER
# Grouped by month + ibn — one row per IBN per month
# Active = datediff(dbm_snap_dt, last_login) <= 90
# =============================================================================

dbm = (
    spark.table("dm_ib.digital_banking_master")
    .filter(
        (F.col("ods_business_dt") >= F.lit(START_DT)) &
        (F.col("ods_business_dt") <= F.lit(END_DT))
    )
    .filter(
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
        F.max("ods_business_dt").cast("date").alias("dbm_snap_dt")
    )
    .withColumn("olb_enrolled",
        F.when(F.col("lst_login_olb").isNotNull(),
               F.lit("OLB Enrolled"))
         .otherwise(F.lit("Non OLB Enrolled"))
    )
    .withColumn("mob_enrolled",
        F.when(F.col("lst_login_mob").isNotNull(),
               F.lit("Mobile Enrolled"))
         .otherwise(F.lit("Non Mobile Enrolled"))
    )
    .withColumn("digital_enrolled",
        F.when(
            F.col("lst_login_olb").isNotNull() | F.col("lst_login_mob").isNotNull(),
            F.lit("Digital Enrolled")
        ).otherwise(F.lit("Non Digital Enrolled"))
    )
    .withColumn("olb_active_flag",
        F.when(
            F.col("lst_login_olb").isNotNull() &
            (F.datediff(F.col("dbm_snap_dt"), F.col("lst_login_olb")) <= 90),
            F.lit("OLB Active")
        ).otherwise(F.lit("Non OLB Active"))
    )
    .withColumn("mob_active_flag",
        F.when(
            F.col("lst_login_mob").isNotNull() &
            (F.datediff(F.col("dbm_snap_dt"), F.col("lst_login_mob")) <= 90),
            F.lit("Mobile Active")
        ).otherwise(F.lit("Non Mobile Active"))
    )
    .withColumn("digitally_active_flag",
        F.when(
            (
                F.col("lst_login_olb").isNotNull() &
                (F.datediff(F.col("dbm_snap_dt"), F.col("lst_login_olb")) <= 90)
            ) | (
                F.col("lst_login_mob").isNotNull() &
                (F.datediff(F.col("dbm_snap_dt"), F.col("lst_login_mob")) <= 90)
            ),
            F.lit("Digital Active")
        ).otherwise(F.lit("Non Digital Active"))
    )
)

# =============================================================================
# STEP 3 — WEALTH SEGMENTATION per RCIF per MONTH
# Uses monthly EIL tables. business_date kept in groupBy so each RCIF
# gets segmentation specific to that month — no fan-out across months.
# =============================================================================

valid_dates_m = get_month_end_dates(
    spark, "eil.m_involved_party_h", START_DT, END_DT
)

# Pre-filter deceased_ind BEFORE join to avoid ambiguous column error
ip_m = (
    spark.table("eil.m_involved_party_h")
    .filter(
        F.col("business_date").isin(valid_dates_m) &
        (F.col("source_system_code") == "CF") &
        (F.coalesce(F.col("deceased_ind"), F.lit("N")) == "N")
    )
)
a2i_m = spark.table("eil.m_arrangement_to_involved_party_relationship_h")
ar_m  = (
    spark.table("eil.m_arrangement_h")
    .filter(
        F.col("source_system_code").isin(WEALTH_SOURCE_CODES) &
        (F.col("closed_ind") == "N")
    )
)

pw1 = (
    ip_m
    .join(
        a2i_m,
        (ip_m["involved_party_id"]  == a2i_m["involved_party_id"]) &
        (ip_m["business_date"]      == a2i_m["business_date"]) &
        (ip_m["source_system_code"] == a2i_m["source_system_code"]),
        "inner"
    )
    .join(
        ar_m,
        (a2i_m["arrangement_id"]                 == ar_m["arrangement_id"]) &
        (a2i_m["arrangement_source_system_code"] == ar_m["source_system_code"]) &
        (a2i_m["business_date"]                  == ar_m["business_date"]),
        "inner"
    )
    .filter(
        F.when(ip_m["private_client_code"].isin("039","539","339"),       F.lit(1))
         .when(ip_m["private_client_trust_code"].isin("239","739"),       F.lit(1))
         .otherwise(
             F.when(ar_m["business_service_segment_type_code"]
                    .isin(WEALTH_SEGMENT_CODES), F.lit(1))
              .otherwise(F.lit(0))
         ) == 1
    )
    .withColumn("business_group",
        F.when(ip_m["private_client_code"].isin("039","539","339"),
               F.lit("Private Wealth"))
         .when(ip_m["private_client_trust_code"].isin("239","739"),
               F.lit("Private Wealth"))
         .otherwise(
             F.when(ar_m["business_service_segment_type_code"].isin("IS_CT","IS_IT"),
                    F.lit("Institutional Services"))
              .when(ar_m["business_service_segment_type_code"].isin("REGIS_FC","REGIS"),
                    F.lit("Investment Services"))
              .when(ar_m["business_service_segment_type_code"] == "PWM",
                    F.lit("Private Wealth"))
              .otherwise(F.coalesce(ar_m["business_service_segment_type_code"],
                                    F.lit("Category???")))
         )
    )
    # Keep business_date in groupBy — one row per RCIF per MONTH
    .groupBy(
        ip_m["rcif_cust_nbr"].cast("string").alias("RCIF_NUMBER"),
        ip_m["business_date"].cast(T.DateType()).alias("business_date"),
        F.col("business_group")
    )
    .agg(
        F.countDistinct(
            F.when(ar_m["business_service_segment_type_code"] == "IS_CT",
                   ar_m["arrangement_id"])
        ).alias("corporate_trust_cnt"),
        F.countDistinct(
            F.when(ar_m["business_service_segment_type_code"] == "IS_IT",
                   ar_m["arrangement_id"])
        ).alias("institutional_trust_cnt"),
        F.countDistinct(
            F.when(ar_m["business_service_segment_type_code"].isin("REGIS_FC","REGIS"),
                   ar_m["arrangement_id"])
        ).alias("investment_cnt"),
        F.countDistinct(
            F.when(ar_m["business_service_segment_type_code"] == "REGIS",
                   ar_m["arrangement_id"])
        ).alias("insurance_cnt"),
        F.countDistinct(
            F.when(ar_m["business_service_segment_type_code"] == "PWM",
                   ar_m["arrangement_id"])
        ).alias("pwm_cnt"),
        F.countDistinct(
            F.when(ar_m["source_system_code"] == "TR",
                   ar_m["arrangement_id"])
        ).alias("trust_cnt"),
        F.countDistinct(
            F.when(ar_m["source_system_code"].isin(BANKING_SOURCE_CODES),
                   ar_m["arrangement_id"])
        ).alias("banking_cnt"),
        F.countDistinct(ar_m["arrangement_id"]).alias("wealth_accts_cnt")
    )
    .withColumn("division",
        F.when(F.col("business_group") == "Private Wealth",
            F.when(
                (F.col("trust_cnt") > 0) & (F.col("banking_cnt") > 0),
                F.lit("Banking & IM&T")
            ).when(
                (F.col("investment_cnt") + F.col("trust_cnt") > 0) &
                (F.col("banking_cnt") == 0),
                F.lit("Investments Only")
            ).otherwise(F.lit("Banking only"))
        )
        .when(F.col("business_group") == "Investment Services",
            F.when(
                (F.col("investment_cnt") > 0) & (F.col("insurance_cnt") == 0),
                F.lit("Investment")
            ).when(
                (F.col("investment_cnt") == 0) & (F.col("insurance_cnt") > 0),
                F.lit("Insurance")
            ).otherwise(F.lit("Insurance & Investment"))
        )
        .otherwise(
            F.when(
                (F.col("corporate_trust_cnt") > 0) &
                (F.col("institutional_trust_cnt") == 0),
                F.lit("Corporate Trust")
            ).when(
                (F.col("corporate_trust_cnt") == 0) &
                (F.col("institutional_trust_cnt") > 0),
                F.lit("Institutional Trust")
            ).when(F.col("pwm_cnt") > 0, F.lit("Banking only"))
             .otherwise(F.lit("Corporate & Institutional Trust"))
        )
    )
)

# =============================================================================
# STEP 4 — MONTHLY RCIF SNAPSHOTS (daily EIL)
# One row per RCIF per month — demographics, IBN, state
# =============================================================================

# Pre-filter deceased_ind BEFORE join
ip_d = (
    spark.table("eil.d_involved_party_h")
    .filter(
        F.col("business_date").isin(month_end_dates) &
        (F.col("source_system_code") == "CF") &
        (F.coalesce(F.col("deceased_ind"), F.lit("N")) == "N")
    )
)
a2i_d  = spark.table("eil.d_arrangement_to_involved_party_relationship_h")
ar_d   = (
    spark.table("eil.d_arrangement_h")
    .filter(F.col("source_system_code").isin(WEALTH_SOURCE_CODES))
)
addr_d = spark.table("eil.d_involved_party_address_h")

rc = (
    ip_d
    .join(
        a2i_d,
        (ip_d["involved_party_id"]  == a2i_d["involved_party_id"]) &
        (ip_d["business_date"]      == a2i_d["business_date"]) &
        (ip_d["source_system_code"] == a2i_d["source_system_code"]),
        "inner"
    )
    .join(
        ar_d,
        (a2i_d["arrangement_id"]                 == ar_d["arrangement_id"]) &
        (a2i_d["arrangement_source_system_code"] == ar_d["source_system_code"]) &
        (a2i_d["business_date"]                  == ar_d["business_date"]),
        "inner"
    )
    .join(
        addr_d,
        (ip_d["involved_party_id"] == addr_d["involved_party_id"]) &
        (ip_d["business_date"]     == addr_d["business_date"]),
        "left"
    )
    .groupBy(
        ip_d["rcif_cust_nbr"].cast("string").alias("RCIF_NUMBER"),
        ip_d["business_date"].cast(T.DateType()).alias("business_date")
    )
    .agg(
        F.first(ip_d["involved_party_id"]).alias("ip_id"),
        F.first(ip_d["cust_internet_banking_nbr"]).alias("cust_ibn"),
        F.first(addr_d["state_name"]).alias("state_name")
    )
)

# =============================================================================
# STEP 5 — INVESTPATH ACCOUNT COUNT PER RCIF PER MONTH
# =============================================================================

ind_i = (
    spark.table("eil.d_involved_party_h")
    .filter(
        F.col("business_date").isin(month_end_dates) &
        (F.col("source_system_code") == "CF") &
        (F.coalesce(F.col("deceased_ind"), F.lit("N")) == "N")
    )
)
a2i_i = spark.table("eil.d_arrangement_to_involved_party_relationship_h")
ar_i  = (
    spark.table("eil.d_arrangement_h")
    .filter(
        (F.col("closed_ind")         == "N") &
        (F.col("account_type_code")  == "IP") &
        (F.col("source_system_code") == "RN")
    )
)

ip_accts_cnt = (
    ind_i
    .join(
        a2i_i,
        (ind_i["involved_party_id"]  == a2i_i["involved_party_id"]) &
        (ind_i["business_date"]      == a2i_i["business_date"]) &
        (ind_i["source_system_code"] == a2i_i["source_system_code"]),
        "inner"
    )
    .join(
        ar_i,
        (a2i_i["arrangement_id"]                 == ar_i["arrangement_id"]) &
        (a2i_i["arrangement_source_system_code"] == ar_i["source_system_code"]) &
        (a2i_i["business_date"]                  == ar_i["business_date"]),
        "inner"
    )
    .groupBy(
        ind_i["rcif_cust_nbr"].cast("string").alias("RCIF_NUMBER"),
        ind_i["business_date"].cast(T.DateType()).alias("business_date")
    )
    .agg(F.countDistinct(ar_i["arrangement_id"]).alias("ip_accts_cnt"))
)

# =============================================================================
# STEP 6 — BUILD wealth_Insights_Customer
#
# rc   INNER JOIN pw1  on RCIF_NUMBER + business_date (same month)
#      LEFT  JOIN ip_accts_cnt on RCIF_NUMBER + business_date
#      LEFT  JOIN dbm on trunc(business_date,'MM') + cust_ibn == ibn
# =============================================================================

window_dedup = (
    Window
    .partitionBy("RCIF_NUMBER", "business_date")
    .orderBy(F.col("RCIF_NUMBER"))
)

wealth_customer = (
    rc
    # Inner join pw1 on RCIF + same month — prevents cross-month fan-out
    .join(
        pw1,
        (rc["RCIF_NUMBER"]   == pw1["RCIF_NUMBER"]) &
        (rc["business_date"] == pw1["business_date"]),
        "inner"
    )
    .drop(pw1["RCIF_NUMBER"])
    .drop(pw1["business_date"])
    .join(ip_accts_cnt,
          (rc["RCIF_NUMBER"]   == ip_accts_cnt["RCIF_NUMBER"]) &
          (rc["business_date"] == ip_accts_cnt["business_date"]),
          "left")
    .drop(ip_accts_cnt["RCIF_NUMBER"])
    .drop(ip_accts_cnt["business_date"])
    # Join DBM on same calendar month + IBN
    .join(
        dbm,
        (F.trunc(rc["business_date"], "MM") == dbm["dbm_month"]) &
        (rc["cust_ibn"] == dbm["ibn"]),
        "left"
    )
    .withColumn("_rn", F.row_number().over(window_dedup))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
    .select(
        rc["RCIF_NUMBER"],
        rc["business_date"],
        F.col("state_name"),
        F.col("business_group"),
        F.col("division"),
        F.coalesce(F.col("wealth_accts_cnt"), F.lit(0)).alias("wealth_accts_cnt"),
        F.coalesce(F.col("ip_accts_cnt"),     F.lit(0)).alias("ip_accts_cnt"),
        rc["cust_ibn"].alias("ibn"),
        F.col("dbm_snap_dt"),
        F.coalesce(F.col("digital_enrolled"),    F.lit("Non Digital Enrolled")).alias("digital_enrolled"),
        F.coalesce(F.col("olb_enrolled"),        F.lit("Non OLB Enrolled")).alias("olb_enrolled"),
        F.coalesce(F.col("mob_enrolled"),        F.lit("Non Mobile Enrolled")).alias("mob_enrolled"),
        F.coalesce(F.col("digitally_active_flag"),F.lit("Non Digital Active")).alias("digitally_active_flag"),
        F.coalesce(F.col("olb_active_flag"),     F.lit("Non OLB Active")).alias("olb_active_flag"),
        F.coalesce(F.col("mob_active_flag"),     F.lit("Non Mobile Active")).alias("mob_active_flag"),
        F.col("lst_login_olb"),
        F.col("lst_login_mob"),
        F.lit("Wealth").alias("fact_type")
    )
)

# =============================================================================
# STEP 7 — BUILD wealth_Insights_Account
# InvestPath accounts at max_ip_date
# =============================================================================

ind_a = (
    spark.table("eil.d_involved_party_h")
    .filter(
        (F.col("business_date")      == max_ip_date) &
        (F.col("source_system_code") == "CF") &
        (F.coalesce(F.col("deceased_ind"), F.lit("N")) == "N")
    )
)
a2i_a = spark.table("eil.d_arrangement_to_involved_party_relationship_h")
ar_a  = (
    spark.table("eil.d_arrangement_h")
    .filter(
        (F.col("business_date")      == max_ip_date) &
        (F.col("closed_ind")         == "N") &
        (F.col("account_type_code")  == "IP") &
        (F.col("source_system_code") == "RN")
    )
)

wealth_account = (
    ind_a
    .join(
        a2i_a,
        (ind_a["involved_party_id"]  == a2i_a["involved_party_id"]) &
        (ind_a["business_date"]      == a2i_a["business_date"]) &
        (ind_a["source_system_code"] == a2i_a["source_system_code"]),
        "inner"
    )
    .join(
        ar_a,
        (a2i_a["arrangement_id"]                 == ar_a["arrangement_id"]) &
        (a2i_a["arrangement_source_system_code"] == ar_a["source_system_code"]) &
        (a2i_a["business_date"]                  == ar_a["business_date"]),
        "inner"
    )
    .select(
        ind_a["rcif_cust_nbr"].cast("string").alias("RCIF_NUMBER"),
        ind_a["involved_party_id"].alias("ip_id"),
        ar_a["arrangement_id"].alias("ip_account_number"),
        ar_a["open_date"].alias("ip_open_date"),
        ar_a["current_balance_amt"].alias("ip_balance"),
        F.when(ar_a["current_balance_amt"] > 0,
               F.lit("Funded")).otherwise(F.lit("Not Funded")).alias("is_funded"),
        F.lit(max_ip_date).cast(T.DateType()).alias("business_date"),
        F.lit("Account").alias("fact_type")
    )
)

# =============================================================================
# STEP 8 — WRITE
# =============================================================================

wealth_customer.repartition(200).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("{}.{}".format(final_db, final_table_customer))

wealth_account.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("{}.{}".format(final_db, final_table_account))

# =============================================================================
# STEP 9 — VERIFY
# =============================================================================

final = spark.table("{}.{}".format(final_db, final_table_customer))
accts = spark.table("{}.{}".format(final_db, final_table_account))

print("Wealth rows    : {}".format(final.count()))
print("Account rows   : {}".format(accts.count()))

print("\nOLB Active by month:")
final.groupBy("business_date", "olb_active_flag") \
     .count().orderBy("business_date", "olb_active_flag").show(20)

print("\nMobile Active by month:")
final.groupBy("business_date", "mob_active_flag") \
     .count().orderBy("business_date", "mob_active_flag").show(20)

print("\nDigital Active by month:")
final.groupBy("business_date", "digitally_active_flag") \
     .count().orderBy("business_date", "digitally_active_flag").show(20)

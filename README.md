# =============================================================================
# Wealth Insights — Customer & Account Tables
# Date Range  : 2025-08-01 to 2026-01-31
# Digital Source: dm_ib.digital_banking_master ONLY (loaded from 2025-08-02)
# NO transmit data used anywhere
#
# OUTPUT TABLES:
#   dm_ib_dev.wealth_Insights_Customer  — one row per RCIF per month-end
#   dm_ib_dev.wealth_Insights_Account   — one row per InvestPath account
#
# DIGITAL JOIN  : EIL.cust_internet_banking_nbr == DBM.relt_ibn  (per month)
# ACTIVE LOGIC  : datediff(dbm_snap_dt, last_login_date) <= 90
# PC and BW excluded from all source codes
# =============================================================================

from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.window import Window
from datetime import date, timedelta, datetime
from itertools import groupby
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
# CONSTANTS  (PC = plastic cards, BW excluded)
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
# HELPER: Get last valid business day per month in date range
# Daily tables skip weekends/holidays — this ensures month-end is always covered
# by rolling back to nearest prior business day that exists in the table
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
            # Use actual month-end if it exists, else roll back to last business day
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

# =============================================================================
# STEP 1 — MONTH-END DATES
# =============================================================================

month_end_dates = get_month_end_dates(
    spark, "eil.d_involved_party_h", START_DT, END_DT
)
max_ip_date = month_end_dates[-1]

print("Month-end dates : {}".format(month_end_dates))
print("Max IP date     : {}".format(max_ip_date))

# =============================================================================
# STEP 2 — DIGITAL BANKING MASTER (monthly, _dbm)
#
# One row per month per IBN.
# Grouped by month so each wealth row joins to its own month's DBM snapshot.
# Active = datediff(dbm_snap_dt, last_login) <= 90  — original SQL logic.
# Enrolled = has ever had a login date recorded.
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
        F.max("ods_business_dt").cast("date").alias("dbm_snap_dt"),
        F.first("rcif_customer_nbr").cast("string").alias("rcif_from_dbm")
    )
    # --- Enrolled flags ---
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
    # --- Active flags using DBM's own snapshot date ---
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
# STEP 3 — WEALTH SEGMENTATION  (monthly EIL tables)
# Business group, division, account counts per RCIF
# =============================================================================

ip_m  = spark.table("eil.m_involved_party_h")
a2i_m = spark.table("eil.m_arrangement_to_involved_party_relationship_h")
ar_m  = spark.table("eil.m_arrangement_h")

valid_dates_m = get_month_end_dates(
    spark, "eil.m_involved_party_h", START_DT, END_DT
)

pw1 = (
    ip_m
    .filter(
        F.col("business_date").isin(valid_dates_m) &
        (F.col("source_system_code") == "CF") &
        (F.coalesce(ip_m["deceased_ind"], F.lit("N")) == "N")
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
    .withColumn("ar_bss", ar_m["business_service_segment_type_code"])
    .withColumn("ar_src", ar_m["source_system_code"])
    .withColumn("ar_id",  ar_m["arrangement_id"])
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
             F.when(F.col("ar_bss").isin("IS_CT","IS_IT"),
                    F.lit("Institutional Services"))
              .when(F.col("ar_bss").isin("REGIS_FC","REGIS"),
                    F.lit("Investment Services"))
              .when(F.col("ar_bss") == "PWM",
                    F.lit("Private Wealth"))
              .otherwise(F.coalesce(F.col("ar_bss"), F.lit("Category???")))
         )
    )
    .groupBy(
        ip_m["rcif_cust_nbr"].cast("string").alias("RCIF_NUMBER"),
        ip_m["involved_party_id"].alias("ip_id"),
        F.col("business_group")
    )
    .agg(
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
# STEP 4 — MONTHLY RCIF SNAPSHOTS  (daily EIL tables, one per month-end)
# One row per RCIF per month with state, IBN, ip_id
# =============================================================================

ip_d   = spark.table("eil.d_involved_party_h")
a2i_d  = spark.table("eil.d_arrangement_to_involved_party_relationship_h")
ar_d   = spark.table("eil.d_arrangement_h")
addr_d = spark.table("eil.d_involved_party_address_h")

rc = (
    ip_d
    .filter(
        F.col("business_date").isin(month_end_dates) &
        (F.col("source_system_code") == "CF") &
        (F.coalesce(ip_d["deceased_ind"], F.lit("N")) == "N")
    )
    .join(
        a2i_d.filter(F.col("business_date").isin(month_end_dates)),
        (ip_d["involved_party_id"]  == a2i_d["involved_party_id"]) &
        (ip_d["business_date"]      == a2i_d["business_date"]) &
        (ip_d["source_system_code"] == a2i_d["source_system_code"]),
        "inner"
    )
    .join(
        ar_d.filter(
            F.col("business_date").isin(month_end_dates) &
            F.col("source_system_code").isin(WEALTH_SOURCE_CODES)
        ),
        (a2i_d["arrangement_id"]                 == ar_d["arrangement_id"]) &
        (a2i_d["arrangement_source_system_code"] == ar_d["source_system_code"]) &
        (a2i_d["business_date"]                  == ar_d["business_date"]),
        "inner"
    )
    .join(
        addr_d.filter(F.col("business_date").isin(month_end_dates)),
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

ind_i = spark.table("eil.d_involved_party_h")
a2i_i = spark.table("eil.d_arrangement_to_involved_party_relationship_h")
ar_i  = spark.table("eil.d_arrangement_h")

ip_accts_cnt = (
    ind_i
    .filter(
        F.col("business_date").isin(month_end_dates) &
        (F.col("source_system_code") == "CF") &
        (F.coalesce(ind_i["deceased_ind"], F.lit("N")) == "N")
    )
    .join(
        a2i_i.filter(F.col("business_date").isin(month_end_dates)),
        (ind_i["involved_party_id"]  == a2i_i["involved_party_id"]) &
        (ind_i["business_date"]      == a2i_i["business_date"]) &
        (ind_i["source_system_code"] == a2i_i["source_system_code"]),
        "inner"
    )
    .join(
        ar_i.filter(
            F.col("business_date").isin(month_end_dates) &
            (F.col("closed_ind")         == "N") &
            (F.col("account_type_code")  == "IP") &
            (F.col("source_system_code") == "RN")
        ),
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
# rc        — one row per RCIF per month-end (demographics + IBN)
# pw1       — wealth segmentation (business_group, division, acct counts)
# ip_accts  — InvestPath account count per month
# dbm       — digital flags joined on month + IBN
#
# Dedup by RCIF + business_date (row_number = 1) to handle any fan-out
# =============================================================================

window_dedup = (
    Window
    .partitionBy("RCIF_NUMBER", "business_date")
    .orderBy(F.col("RCIF_NUMBER"))
)

wealth_customer = (
    rc
    # Wealth segmentation — inner join keeps only wealth customers
    .join(pw1, ["RCIF_NUMBER"], "inner")
    # InvestPath counts per month
    .join(ip_accts_cnt, ["RCIF_NUMBER", "business_date"], "left")
    # DBM flags — join on same month + IBN
    .join(
        dbm,
        (F.trunc(rc["business_date"].cast("date"), "MM") == dbm["dbm_month"]) &
        (rc["cust_ibn"] == dbm["ibn"]),
        "left"
    )
    .withColumn("_rn", F.row_number().over(window_dedup))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
    .select(
        F.col("RCIF_NUMBER"),
        F.col("business_date"),
        F.col("state_name"),
        F.col("business_group"),
        F.col("division"),
        F.coalesce(F.col("wealth_accts_cnt"), F.lit(0)).alias("wealth_accts_cnt"),
        F.coalesce(F.col("ip_accts_cnt"),     F.lit(0)).alias("ip_accts_cnt"),
        F.col("cust_ibn").alias("ibn"),
        # DBM snapshot date (the DBM month-end used for active calculation)
        F.col("dbm_snap_dt"),
        # Enrolled flags
        F.coalesce(F.col("digital_enrolled"), F.lit("Non Digital Enrolled")).alias("digital_enrolled"),
        F.coalesce(F.col("olb_enrolled"),     F.lit("Non OLB Enrolled")).alias("olb_enrolled"),
        F.coalesce(F.col("mob_enrolled"),     F.lit("Non Mobile Enrolled")).alias("mob_enrolled"),
        # Active flags
        F.coalesce(F.col("digitally_active_flag"), F.lit("Non Digital Active")).alias("digitally_active_flag"),
        F.coalesce(F.col("olb_active_flag"),       F.lit("Non OLB Active")).alias("olb_active_flag"),
        F.coalesce(F.col("mob_active_flag"),       F.lit("Non Mobile Active")).alias("mob_active_flag"),
        # Last login dates
        F.col("lst_login_olb"),
        F.col("lst_login_mob"),
        F.lit("Wealth").alias("fact_type")
    )
)

# =============================================================================
# STEP 7 — BUILD wealth_Insights_Account
# InvestPath accounts at max_ip_date, one row per account
# =============================================================================

ind_a = spark.table("eil.d_involved_party_h")
a2i_a = spark.table("eil.d_arrangement_to_involved_party_relationship_h")
ar_a  = spark.table("eil.d_arrangement_h")

wealth_account = (
    ind_a
    .filter(
        (F.col("business_date") == max_ip_date) &
        (F.col("source_system_code") == "CF") &
        (F.coalesce(ind_a["deceased_ind"], F.lit("N")) == "N")
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
            (F.col("business_date")      == max_ip_date) &
            (F.col("closed_ind")         == "N") &
            (F.col("account_type_code")  == "IP") &
            (F.col("source_system_code") == "RN")
        ),
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
# STEP 8 — WRITE OUTPUT TABLES
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

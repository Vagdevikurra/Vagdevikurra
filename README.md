
# =============================================================================
# Wealth Insights - Customer & Account Tables
# Date Range: 2025-08-01 to 2026-01-31
# =============================================================================

from pyspark.sql import SparkSession, functions as F, types as T
from datetime import date, timedelta
import calendar

spark = (
    SparkSession.builder
    .appName("WealthInsights")
    .enableHiveSupport()
    .getOrCreate()
)

START_DT    = "2025-08-01"
END_DT      = "2026-01-31"
final_db    = "dm_ib_dev"
final_table_customer = "wealth_Insights_Customer"
final_table_account  = "wealth_Insights_Account"

# =============================================================================
# HELPER: Build valid business-date list
# Daily tables skip weekends AND holidays, but we must also capture
# month-end dates that fall on weekends (Aug-31-2025, Jan-31-2026, etc.)
# Strategy: collect every distinct business_date that EXISTS in the daily
# table, then ADD any month-end dates that are missing by rolling back to
# the nearest prior business day already in the table.
# =============================================================================

def get_last_business_dates(spark, table: str, start: str, end: str):
    """
    Returns a Python set of date strings that are valid business dates,
    ensuring every calendar month-end in [start, end] is represented by
    the last available business day in that month.
    """
    # All distinct business dates actually present in the daily table
    existing_dates_df = (
        spark.table(table)
        .filter(
            (F.col("business_date").cast("date") >= F.lit(start)) &
            (F.col("business_date").cast("date") <= F.lit(end))
        )
        .select(F.col("business_date").cast("date").alias("dt"))
        .distinct()
    )
    existing_dates = {row["dt"] for row in existing_dates_df.collect()}

    # Build month-end dates in range
    from datetime import date
    import calendar

    start_d = date.fromisoformat(start)
    end_d   = date.fromisoformat(end)

    month_ends = set()
    y, m = start_d.year, start_d.month
    while date(y, m, 1) <= end_d:
        last_day = calendar.monthrange(y, m)[1]
        me = date(y, m, last_day)
        if me >= start_d and me <= end_d:
            month_ends.add(me)
        if m == 12:
            y += 1; m = 1
        else:
            m += 1

    # For any month-end not in existing_dates, find nearest prior date that IS present
    final_dates = set(existing_dates)
    for me in month_ends:
        if me not in existing_dates:
            candidate = me - timedelta(days=1)
            while candidate >= start_d:
                if candidate in existing_dates:
                    final_dates.add(candidate)
                    break
                candidate -= timedelta(days=1)

    return [str(d) for d in sorted(final_dates)]


# =============================================================================
# 1. RCIF NUMBER  (source pool of valid RCIFs in the window)
# =============================================================================

six_months_ago = F.add_months(F.current_date(), -6)

involved_party_rcif = (
    spark.table("eil.m_involved_party_h")
    .filter(F.col("business_date").cast("date") >= six_months_ago)
    .select(F.col("rcif_cust_nbr").alias("rcif_number"))
)

digital_banking_rcif = (
    spark.table("dm_ib.digital_banking_master")
    .filter(F.col("ods_business_dt") >= six_months_ago)
    .select(F.col("rcif_customer_nbr").alias("rcif_number"))
)

add_rcifs = (
    involved_party_rcif.union(digital_banking_rcif)
    .distinct()
    .filter(
        F.col("rcif_number").isNotNull() &
        (F.col("rcif_number") != "")
    )
)

# =============================================================================
# 2. DIGITAL CUSTOMER FLAGS  (monthly table dm_ib.digital_banking_master_dbm)
# =============================================================================

max_dbm_dt = (
    spark.table("dm_ib.digital_banking_master_dbm")
    .agg(F.max("ods_business_dt"))
    .collect()[0][0]
)

dig_customer = (
    spark.table("dm_ib.digital_banking_master_dbm")
    .filter(F.col("ods_business_dt") == max_dbm_dt)
    .groupBy("relt_ibn", "ods_business_dt")
    .agg(
        F.max("olb_last_login_date").alias("lst_login_olb"),
        F.max("mob_last_login_date").alias("lst_login_mob")
    )
)

# =============================================================================
# 3. RCIF CUSTOMER BASE  (daily table eil.d_involved_party_h)
# =============================================================================

valid_dates_rcif = get_last_business_dates(
    spark, "eil.d_involved_party_h", START_DT, END_DT
)

ip_daily   = spark.table("eil.d_involved_party_h")
a2i_daily  = spark.table("eil.d_arrangement_to_involved_party_relationship_h")
ar_daily   = spark.table("eil.d_arrangement_h")
addr_daily = spark.table("eil.d_involved_party_address_h")

ar_source_codes = [
    'DA','SV','CC','MG','LS','TM','PC','LO',
    'CM','CS','EL','IC','MA','PF','PR','SD','TR',
    'BI','RN','IS_CT','IS_IF','PNB'
]
# NOTE: 'PC' (plastic cards) and 'BW' removed per requirements

rc = (
    ip_daily
    .filter(
        F.col("business_date").isin(valid_dates_rcif) &
        (F.col("source_system_code") == "CF") &
        (F.coalesce(F.col("deceased_ind"), F.lit("N")) == "N")
    )
    .join(
        a2i_daily,
        (ip_daily["business_date"]      == a2i_daily["business_date"]) &
        (ip_daily["source_system_code"] == a2i_daily["source_system_code"]) &
        (ip_daily["involved_party_id"]  == a2i_daily["involved_party_id"]),
        "inner"
    )
    .join(
        ar_daily.filter(F.col("source_system_code").isin(ar_source_codes)),
        (a2i_daily["business_date"]                  == ar_daily["business_date"]) &
        (a2i_daily["arrangement_source_system_code"] == ar_daily["source_system_code"]) &
        (a2i_daily["arrangement_id"]                 == ar_daily["arrangement_id"]),
        "inner"
    )
    .join(
        addr_daily,
        (ip_daily["involved_party_id"] == addr_daily["involved_party_id"]) &
        (ip_daily["business_date"]     == addr_daily["business_date"]),
        "inner"
    )
    .groupBy(
        ip_daily["business_date"].cast("date").alias("business_date"),
        ip_daily["involved_party_id"],
        ip_daily["cust_internet_banking_nbr"],
        ip_daily["involved_party_tax_id_nbr"],
        ip_daily["birth_date"],
        ip_daily["involved_party_name"],
        addr_daily["city_name"],
        addr_daily["state_name"],
        addr_daily["country_name"]
    )
    .agg(F.max(F.col("rcif_cust_nbr").cast("string")).alias("RCIF_NUMBER"))
)

# =============================================================================
# 4. WEALTH CUSTOMER  (monthly table eil.m_involved_party_h)
# =============================================================================

valid_dates_wealth = get_last_business_dates(
    spark, "eil.m_involved_party_h", START_DT, END_DT
)

ip_m   = spark.table("eil.m_involved_party_h")
a2i_m  = spark.table("eil.m_arrangement_to_involved_party_relationship_h")
ar_m   = spark.table("eil.m_arrangement_h")

banking_codes_wealth = [
    'BI','RN','TR','DA','SV','CC','LO','MG','TM','PC',
    'CS','IC','MA','PF','PR','SD','CM','EL','LS'
]
# NOTE: 'PC' (plastic cards) and 'BW' removed per requirements

pw1 = (
    ip_m
    .filter(
        F.col("business_date").isin(valid_dates_wealth) &
        (F.col("source_system_code") == "CF") &
        (F.coalesce(F.col("deceased_ind"), F.lit("N")) == "N") &
        (
            F.when(F.col("private_client_code").isin("039","539","339"), F.lit(1))
             .when(F.col("private_client_trust_code").isin("239","739"), F.lit(1))
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
            F.col("source_system_code").isin(banking_codes_wealth) &
            (F.col("closed_ind") == "N")
        ),
        (a2i_m["arrangement_id"]                 == ar_m["arrangement_id"]) &
        (a2i_m["arrangement_source_system_code"] == ar_m["source_system_code"]) &
        (a2i_m["business_date"]                  == ar_m["business_date"]),
        "inner"
    )
    .withColumn("Business_Group",
        F.when(ip_m["private_client_code"].isin("039","539","339"), F.lit("Private Wealth"))
         .when(ip_m["private_client_trust_code"].isin("239","739"), F.lit("Private Wealth"))
         .otherwise(
             F.when(ar_m["business_service_segment_type_code"].isin("IS_CT","IS_IT"), "Institutional Services")
              .when(ar_m["business_service_segment_type_code"].isin("REGIS_FC","REGIS"), "Investment Services")
              .when(ar_m["business_service_segment_type_code"] == "PWM", "Private Wealth")
              .otherwise(F.coalesce(ar_m["business_service_segment_type_code"], F.lit("Category???")))
         )
    )
    .groupBy(
        ip_m["business_date"].cast("date").alias("business_date"),
        ip_m["involved_party_id"],
        ip_m["rcif_cust_nbr"].alias("RCIF_NUMBER"),
        ip_m["cust_internet_banking_nbr"],
        ip_m["private_client_code"],
        ip_m["private_client_trust_code"],
        ar_m["business_service_segment_type_code"],
        F.col("Business_Group")
    )
    .agg(
        F.countDistinct(
            F.when(ar_m["business_service_segment_type_code"] == "IS_CT",  ar_m["arrangement_id"])
        ).alias("Corporate_Trust_Count"),
        F.countDistinct(
            F.when(ar_m["business_service_segment_type_code"] == "IS_IT",  ar_m["arrangement_id"])
        ).alias("Institutional_Trust_Count"),
        F.countDistinct(
            F.when(ar_m["business_service_segment_type_code"].isin("REGIS_FC","REGIS"), ar_m["arrangement_id"])
        ).alias("Investment_Count"),
        F.countDistinct(
            F.when(ar_m["business_service_segment_type_code"] == "REGIS",  ar_m["arrangement_id"])
        ).alias("Insurance_Count"),
        F.countDistinct(
            F.when(ar_m["business_service_segment_type_code"] == "PWM",    ar_m["arrangement_id"])
        ).alias("PWM_Count"),
        F.countDistinct(
            F.when(ar_m["source_system_code"] == "TR",                     ar_m["arrangement_id"])
        ).alias("Trust_Count"),
        F.countDistinct(
            F.when(
                ar_m["source_system_code"].isin(
                    'DA','SV','CC','MG','LS','TM','LO','CM','CS','EL','IC','MA','PF','PR','SD'
                ),                                                          ar_m["arrangement_id"])
        ).alias("Banking_Count"),
        F.count(ar_m["arrangement_id"]).alias("accts_cnt")
    )
)

pw1_with_division = pw1.withColumn("division",
    F.when(F.col("Business_Group") == "Private Wealth",
        F.when(
            (F.col("Trust_Count") > 0) & (F.col("Banking_Count") > 0), "Banking & IM&T"
        ).otherwise(
            F.when(
                (F.col("Investment_Count") + F.col("Trust_Count") > 0) & (F.col("Banking_Count") == 0),
                "Investments Only"
            ).otherwise("Banking only")
        )
    ).when(F.col("Business_Group") == "Investment Services",
        F.when(
            (F.col("Investment_Count") > 0) & (F.col("Insurance_Count") == 0), "Investment"
        ).when(
            (F.col("Investment_Count") == 0) & (F.col("Insurance_Count") > 0), "Insurance"
        ).otherwise("Insurance & Investment")
    ).otherwise(
        F.when(
            (F.col("Corporate_Trust_Count") > 0) & (F.col("Institutional_Trust_Count") == 0),
            "Corporate Trust"
        ).when(
            (F.col("Corporate_Trust_Count") == 0) & (F.col("Institutional_Trust_Count") > 0),
            "Institutional Trust"
        ).when(F.col("PWM_Count") > 0, "Banking only")
        .otherwise("Corporate & Institutional Trust")
    )
)

# =============================================================================
# 5. INVESTPATH ACCOUNTS  (daily table eil.d_involved_party_h)
# =============================================================================

max_inv_date = (
    spark.table("eil.d_involved_party_h")
    .agg(F.max("business_date"))
    .collect()[0][0]
)

ind_inv = spark.table("eil.d_involved_party_h")
a2i_inv = spark.table("eil.d_arrangement_to_involved_party_relationship_h")
ar_inv  = spark.table("eil.d_arrangement_h")

investpath = (
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
            (F.col("closed_ind")        == "N") &
            (F.col("account_type_code") == "IP") &
            (F.col("source_system_code") == "RN")
        ),
        (a2i_inv["arrangement_id"]                 == ar_inv["arrangement_id"]) &
        (a2i_inv["arrangement_source_system_code"] == ar_inv["source_system_code"]) &
        (a2i_inv["business_date"]                  == ar_inv["business_date"]),
        "inner"
    )
    .select(
        ind_inv["rcif_cust_nbr"].alias("rcif_nbr"),
        ind_inv["involved_party_id"].alias("ip_id"),
        ar_inv["current_balance_amt"].alias("balance"),
        ar_inv["open_date"],
        ar_inv["arrangement_id"].alias("account_number"),
        F.lit("Investpath").alias("account_type"),
        ar_inv["source_system_code"].alias("source_system")
    )
)

# =============================================================================
# 6. ALL ACCOUNTS  (Banking + Investment + Trust)
# =============================================================================

# Source codes by account type (PC plastic cards and BW removed)
acct_type_map = {
    "Banking":     ['DA','SV','CC','MG','LS','TM','LO','CM','CS','EL','IC','MA','PF','PR','SD'],
    "Investment":  ['REGIS_FC','REGIS'],
    "Trust":       ['TR','IS_CT','IS_IT'],
    "PWM":         ['PWM'],
    "Investpath":  ['RN']
}

all_source_codes = [c for codes in acct_type_map.values() for c in codes]

ar_all = spark.table("eil.d_arrangement_h")
a2i_all = spark.table("eil.d_arrangement_to_involved_party_relationship_h")
ip_all  = spark.table("eil.d_involved_party_h")

# Build account type label
account_type_expr = (
    F.when(ar_all["source_system_code"].isin(acct_type_map["Investpath"]) &
           (ar_all["account_type_code"] == "IP"),                        "Investpath")
     .when(ar_all["source_system_code"].isin(acct_type_map["Investment"]) |
           ar_all["business_service_segment_type_code"].isin("REGIS_FC","REGIS"), "Investment")
     .when(ar_all["source_system_code"].isin(acct_type_map["Trust"]) |
           ar_all["business_service_segment_type_code"].isin("IS_CT","IS_IT"),    "Trust")
     .when(ar_all["business_service_segment_type_code"] == "PWM",                "PWM")
     .when(ar_all["source_system_code"].isin(acct_type_map["Banking"]),           "Banking")
     .otherwise("Other")
)

valid_dates_acct = get_last_business_dates(
    spark, "eil.d_arrangement_h", START_DT, END_DT
)

all_accounts = (
    ip_all
    .filter(
        F.col("business_date").isin(valid_dates_acct) &
        (F.col("source_system_code") == "CF") &
        (F.coalesce(F.col("deceased_ind"), F.lit("N")) == "N")
    )
    .join(
        a2i_all,
        (ip_all["involved_party_id"]  == a2i_all["involved_party_id"]) &
        (ip_all["business_date"]      == a2i_all["business_date"]) &
        (ip_all["source_system_code"] == a2i_all["source_system_code"]),
        "inner"
    )
    .join(
        ar_all.filter(
            F.col("source_system_code").isin(all_source_codes) &
            (F.col("closed_ind") == "N")
        ),
        (a2i_all["arrangement_id"]                 == ar_all["arrangement_id"]) &
        (a2i_all["arrangement_source_system_code"] == ar_all["source_system_code"]) &
        (a2i_all["business_date"]                  == ar_all["business_date"]),
        "inner"
    )
    .withColumn("account_type_label", account_type_expr)
    .select(
        ip_all["business_date"].cast("date").alias("business_date"),
        ip_all["rcif_cust_nbr"].alias("RCIF_NUMBER"),
        ip_all["involved_party_id"].alias("ip_id"),
        ip_all["cust_internet_banking_nbr"],
        ar_all["arrangement_id"].alias("account_number"),
        ar_all["source_system_code"].alias("source_system"),
        ar_all["account_type_code"],
        ar_all["business_service_segment_type_code"],
        ar_all["current_balance_amt"].alias("balance"),
        ar_all["open_date"],
        ar_all["closed_ind"],
        F.col("account_type_label").alias("account_type")
    )
)

# =============================================================================
# 7. BUILD FINAL CUSTOMER TABLE
# =============================================================================
from pyspark.sql.window import Window

window_spec = Window.partitionBy("RCIF_NUMBER").orderBy("cust_internet_banking_nbr")

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
    .join(dig_customer, rc["cust_internet_banking_nbr"] == dig_customer["reltibn"], "left")
    .join(
        pw1_with_division.select(
            "RCIF_NUMBER","business_date","Business_Group","division",
            "Corporate_Trust_Count","Institutional_Trust_Count",
            "Investment_Count","Insurance_Count","PWM_Count",
            "Trust_Count","Banking_Count","accts_cnt"
        ),
        ["RCIF_NUMBER"],
        "left"
    )
    .join(add_rcifs.withColumnRenamed("rcif_number","RCIF_NUMBER"), ["RCIF_NUMBER"], "inner")
    .withColumn("PWRANK", F.row_number().over(window_spec))
    .withColumn("CUSTOMER_GENERATION", generation_expr)
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
        F.when(F.col("reltibn").isNull(), "Non Digital User").otherwise("Digital User")
    )
    .select(
        rc["business_date"],
        "RCIF_NUMBER",
        rc["involved_party_id"].alias("ip_id"),
        "cust_internet_banking_nbr",
        "involved_party_tax_id_nbr",
        "involved_party_name",
        "birth_date",
        "CUSTOMER_GENERATION",
        "city_name",
        "state_name",
        "country_name",
        "Business_Group",
        "division",
        "Mobile_Active_Flag",
        "Mobile_Flag",
        "OLB_Active_Flag",
        "OLB_Flag",
        "Digitally_Active_Flag",
        "Digital_flag",
        "PWRANK",
        "Corporate_Trust_Count",
        "Institutional_Trust_Count",
        "Investment_Count",
        "Insurance_Count",
        "PWM_Count",
        "Trust_Count",
        "Banking_Count",
        "accts_cnt"
    )
)

# =============================================================================
# 8. BUILD FINAL ACCOUNT TABLE
# =============================================================================

wealth_account = (
    all_accounts
    .join(
        wealth_customer.select("RCIF_NUMBER","ip_id","Business_Group","division",
                               "CUSTOMER_GENERATION","Digital_flag","Digitally_Active_Flag"),
        ["RCIF_NUMBER","ip_id"],
        "left"
    )
    .select(
        "business_date",
        "RCIF_NUMBER",
        "ip_id",
        "cust_internet_banking_nbr",
        "account_number",
        "source_system",
        "account_type_code",
        "account_type",
        "business_service_segment_type_code",
        "balance",
        "open_date",
        "closed_ind",
        "Business_Group",
        "division",
        "CUSTOMER_GENERATION",
        "Digital_flag",
        "Digitally_Active_Flag"
    )
)

# =============================================================================
# 9. WRITE OUTPUT TABLES
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

print(f"✅ {final_db}.{final_table_customer} written successfully.")
print(f"✅ {final_db}.{final_table_account} written successfully.")

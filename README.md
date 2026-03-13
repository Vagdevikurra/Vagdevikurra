from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.window import Window
from pyspark import StorageLevel
from pyspark import SparkConf

# ── Configuration ─────────────────────────────────────────────────────────────
DEFAULT_DB = "dm_ib_dev"
EIL_DB     = "eil"
DMIB_DB    = "dm_ib"

START_DT = "2025-09-01"
END_DT   = "2026-02-28"

spark = (
    SparkSession.builder
    .config(conf=conf)
    .enableHiveSupport()
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ── Source Tables ─────────────────────────────────────────────────────────────
customer = spark.table(f"{EIL_DB}.d_involved_party_h")
account  = spark.table(f"{EIL_DB}.d_arrangement_h")

# ── Valid Source Codes (PC and BW removed) ────────────────────────────────────
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

# =============================================================================
# QUERY 1 — Digital Activity (Dig_Customer)
# =============================================================================
max_ods_dt = (
    spark.table(f"{DMIB_DB}.digital_banking_master")
    .agg(F.max("ods_business_dt").alias("max_dt"))
    .collect()[0]["max_dt"]
)

dig_customer = (
    spark.table(f"{DMIB_DB}.digital_banking_master")
    .filter(F.col("ods_business_dt") == max_ods_dt)
    .groupBy("ibn", "ods_business_dt")
    .agg(
        F.max("olb_last_login_date").alias("lst_login_olb"),
        F.max("mob_last_login_date").alias("lst_login_mob")
    )
)

# =============================================================================
# QUERY 2 — Investpath (INV)
# =============================================================================
last_biz_date = customer.agg(F.max("business_date").alias("last_date")).collect()[0]["last_date"]

inv_df = (
    customer.alias("c")
    .filter(
        (F.col("c.business_date")      == last_biz_date) &
        (F.col("c.source_system_code") == "CF")          &
        (F.coalesce(F.col("c.deceased_ind"), F.lit("N")) == "N")
    )
    .join(
        account.alias("ar"),
        (F.col("c.involved_party_id")  == F.col("ar.involved_party_id"))  &
        (F.col("c.business_date")      == F.col("ar.business_date"))       &
        (F.col("c.source_system_code") == F.col("ar.source_system_code"))  &
        (F.col("ar.closed_ind")        == "N")                             &
        (F.col("ar.account_type_code") == "IP")                            &
        (F.col("ar.source_system_code")== "RN"),
        "inner"
    )
    .select(
        F.col("c.rcif_cust_nbr").alias("rcif_nbr"),
        F.col("c.involved_party_id").alias("ip_id"),
        F.col("ar.current_balance_amt").alias("balance"),
        F.col("ar.open_date"),
        F.col("ar.arrangement_id").alias("Accounts")
    )
)

# =============================================================================
# QUERY 3 — RCIF_Dig (Customer master + digital flags + generation)
# =============================================================================
rc = (
    customer.alias("c")
    .filter(
        (F.col("c.business_date")      == last_biz_date) &
        (F.col("c.source_system_code") == "CF")          &
        (F.coalesce(F.col("c.deceased_ind"), F.lit("N")) == "N")
    )
    .join(
        account.alias("ar"),
        (F.col("c.involved_party_id")  == F.col("ar.involved_party_id"))  &
        (F.col("c.business_date")      == F.col("ar.business_date"))       &
        (F.col("c.source_system_code") == F.col("ar.source_system_code"))  &
        F.col("ar.source_system_code").isin(VALID_SOURCE_CODES),
        "inner"
    )
    .groupBy(
        F.col("c.involved_party_id"),
        F.col("c.cust_internet_banking_nbr"),
        F.col("c.involved_party_tax_id_nbr"),
        F.col("c.involved_party_name"),
        F.col("c.birth_date"),
        F.col("c.city_name"),
        F.col("c.state_name"),
        F.col("c.country_name")
    )
    .agg(F.max(F.col("c.rcif_cust_nbr").cast("string")).alias("RCIF_NUMBER"))
)

rcif_dig = (
    rc.join(
        dig_customer,
        rc["cust_internet_banking_nbr"] == dig_customer["ibn"],
        "left"
    )
    .withColumn("CUSTOMER_GENERATION",
        F.when((F.col("birth_date") >= "1900-01-01") & (F.col("birth_date") <= "1924-12-31"), "GI Generation (1900-1924)")
         .when((F.col("birth_date") >= "1925-01-01") & (F.col("birth_date") <= "1945-12-31"), "Traditionalist (1925-1945)")
         .when((F.col("birth_date") >= "1946-01-01") & (F.col("birth_date") <= "1964-12-31"), "Baby Boomer (1946-1964)")
         .when((F.col("birth_date") >= "1965-01-01") & (F.col("birth_date") <= "1980-12-31"), "Gen X (1965-1980)")
         .when((F.col("birth_date") >= "1981-01-01") & (F.col("birth_date") <= "1996-12-31"), "Millennial (1981-1996)")
         .when( F.col("birth_date") >= "1997-01-01",                                          "Centennial (1997-???)")
         .otherwise("Unknown")
    )
    .withColumn("Mobile_Active_Flag",
        F.when(F.datediff("ods_business_dt", "lst_login_mob") <= 90, "Mobile Active")
         .otherwise("Non Mobile Active")
    )
    .withColumn("Mobile_Flag",
        F.when(F.col("lst_login_mob").isNull(), "Non Mobile User").otherwise("Mobile User")
    )
    .withColumn("OLB_Active_Flag",
        F.when(F.datediff("ods_business_dt", "lst_login_olb") <= 90, "OLB Active")
         .otherwise("Non OLB Active")
    )
    .withColumn("OLB_Flag",
        F.when(F.col("lst_login_olb").isNull(), "Non OLB User").otherwise("OLB User")
    )
    .withColumn("Digitally_Active_Flag",
        F.when(
            (F.datediff("ods_business_dt", "lst_login_mob") <= 90) |
            (F.datediff("ods_business_dt", "lst_login_olb") <= 90),
            "Digital Active"
        ).otherwise("Non Digital Active")
    )
    .withColumn("Digital_flag",
        F.when(F.col("ibn").isNull(), "Non Digital User").otherwise("Digital User")
    )
)

# =============================================================================
# QUERY 4 — RCIF Number List
# =============================================================================
six_mo_ago = F.add_months(F.current_date(), -6)

rcif_from_customer = (
    customer
    .filter(F.col("business_date") >= six_mo_ago)
    .select(F.col("rcif_cust_nbr").cast("string").alias("rcif_number"))
    .distinct()
)

rcif_from_digital = (
    spark.table(f"{DMIB_DB}.digital_banking_master")
    .filter(F.col("ods_business_dt") >= six_mo_ago)
    .select(F.col("rcif_customer_nbr").cast("string").alias("rcif_number"))
)

add_rcifs = (
    rcif_from_customer.union(rcif_from_digital)
    .filter(F.col("rcif_number").isNotNull() & (F.col("rcif_number") != ""))
    .distinct()
)

# =============================================================================
# QUERY 5 — Wealth (Business Group + Division + Account Counts)
# =============================================================================
pw1 = (
    customer.alias("ind")
    .filter(
        (F.col("ind.business_date")      == last_biz_date) &
        (F.col("ind.source_system_code") == "CF")          &
        (F.coalesce(F.col("ind.deceased_ind"), F.lit("N")) == "N")
    )
    .join(
        account.alias("ar"),
        (F.col("ind.involved_party_id")  == F.col("ar.involved_party_id"))  &
        (F.col("ind.business_date")      == F.col("ar.business_date"))       &
        (F.col("ind.source_system_code") == F.col("ar.source_system_code"))  &
        F.col("ar.source_system_code").isin(WEALTH_SOURCE_CODES)             &
        (F.col("ar.closed_ind")          == "N"),
        "inner"
    )
    # Wealth eligibility filter
    .filter(
        F.when(
            F.col("ind.private_client_code").isin('039','539','339'), F.lit(1)
        ).when(
            F.col("ind.private_client_trust_code").isin('239','739'), F.lit(1)
        ).otherwise(
            F.when(
                F.col("ar.business_service_segment_type_code").isin('IS_CT','IS_IT','REGIS_FC','REGIS','PWM'),
                F.lit(1)
            ).otherwise(F.lit(0))
        ) == 1
    )
    .withColumn("Business_Group",
        F.when(F.col("ind.private_client_code").isin('039','539','339'),         "Private Wealth")
         .when(F.col("ind.private_client_trust_code").isin('239','739'),         "Private Wealth")
         .when(F.col("ar.business_service_segment_type_code").isin('IS_CT'),     "Institutional Services")
         .when(F.col("ar.business_service_segment_type_code").isin('IS_IT'),     "Institutional Services")
         .when(F.col("ar.business_service_segment_type_code").isin('REGIS_FC','REGIS'), "Investment Services")
         .when(F.col("ar.business_service_segment_type_code") == "PWM",          "Private Wealth")
         .otherwise(F.concat(F.col("ar.business_service_segment_type_code"), F.lit(" Category????")))
    )
    .groupBy(
        F.col("ind.business_date"),
        F.col("ind.involved_party_id").alias("ip_id"),
        F.col("ind.rcif_cust_nbr").alias("RCIF_NUMBER"),
        F.col("ind.cust_internet_banking_nbr").alias("ibn"),
        F.col("ar.business_service_segment_type_code"),
        F.col("ind.private_client_code"),
        F.col("ind.private_client_trust_code"),
        "Business_Group"
    )
    .agg(
        F.countDistinct(F.when(F.col("ar.business_service_segment_type_code") == "IS_CT",  F.col("ar.arrangement_id"))).alias("Corporate_Trust_Count"),
        F.countDistinct(F.when(F.col("ar.business_service_segment_type_code") == "IS_IT",  F.col("ar.arrangement_id"))).alias("Institutional_Trust_Count"),
        F.countDistinct(F.when(F.col("ar.business_service_segment_type_code") == "REGIS_FC",F.col("ar.arrangement_id"))).alias("Investment_Count"),
        F.countDistinct(F.when(F.col("ar.business_service_segment_type_code") == "REGIS",  F.col("ar.arrangement_id"))).alias("Insurance_Count"),
        F.countDistinct(F.when(F.col("ar.business_service_segment_type_code") == "PWM",    F.col("ar.arrangement_id"))).alias("PWM_Count"),
        F.countDistinct(F.when(F.col("ar.business_service_segment_type_code") == "TR",     F.col("ar.arrangement_id"))).alias("Trust_Count"),
        F.countDistinct(F.when(
            F.col("ar.source_system_code").isin('DA','SV','CC','MG','LS','TM','LO','CM','CS','EL','IC','MA','PF','PR','SD'),
            F.col("ar.arrangement_id")
        )).alias("Banking_Count"),
        F.count("ar.arrangement_id").alias("accts_cnt")
    )
)

wealth_df = (
    pw1
    .withColumn("division",
        F.when(F.col("Business_Group") == "Private Wealth",
            F.when((F.col("Trust_Count") > 0) & (F.col("Banking_Count") > 0), "Banking & IM&T")
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
             .when(F.col("PWM_Count") > 0, "Banking only")
             .otherwise("Corporate & Institutional Trust")
        )
    )
)

# =============================================================================
# FINAL OUTPUT — wealth_insights_customer & wealth_insights_account
# =============================================================================

# ── wealth_insights_customer ──────────────────────────────────────────────────
wealth_insights_customer = (
    rcif_dig
    .join(add_rcifs, rcif_dig["RCIF_NUMBER"] == add_rcifs["rcif_number"], "inner")
    .join(
        wealth_df.select("RCIF_NUMBER", "Business_Group", "division", "ibn"),
        "RCIF_NUMBER", "left"
    )
    .select(
        "RCIF_NUMBER",
        "involved_party_id",
        "ibn",
        "cust_internet_banking_nbr",
        "involved_party_tax_id_nbr",
        "involved_party_name",
        "birth_date",
        "city_name",
        "state_name",
        "country_name",
        "CUSTOMER_GENERATION",
        "Mobile_Active_Flag",
        "Mobile_Flag",
        "OLB_Active_Flag",
        "OLB_Flag",
        "Digitally_Active_Flag",
        "Digital_flag",
        "Business_Group",
        "division"
    )
    .distinct()
)

# ── wealth_insights_account ───────────────────────────────────────────────────
wealth_insights_account = (
    inv_df
    .join(
        wealth_df.select(
            "RCIF_NUMBER", "ip_id", "Business_Group", "division",
            "accts_cnt", "Corporate_Trust_Count", "Institutional_Trust_Count",
            "Investment_Count", "Insurance_Count", "PWM_Count",
            "Trust_Count", "Banking_Count"
        ),
        inv_df["ip_id"] == wealth_df["ip_id"],
        "left"
    )
    .select(
        inv_df["rcif_nbr"].alias("RCIF_NUMBER"),
        inv_df["ip_id"],
        inv_df["balance"],
        inv_df["open_date"],
        inv_df["Accounts"].alias("arrangement_id"),
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

# ── Write Output Tables ───────────────────────────────────────────────────────
wealth_insights_customer.write.mode("overwrite").saveAsTable(f"{DEFAULT_DB}.wealth_insights_customer")
wealth_insights_account.write.mode("overwrite").saveAsTable(f"{DEFAULT_DB}.wealth_insights_account")

print("✅ wealth_insights_customer written successfully.")
print("✅ wealth_insights_account written successfully.")

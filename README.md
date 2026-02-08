# ==================================================================================
# Wealth Management Data Consolidation - PySpark
# Convert 5 SQL tables into 2 Spark tables: wias1 (Customer) and wics1 (Account)
# Date Range: 2025-08-01 to 2026-01-31 (Past 6 months)
# ==================================================================================

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

# ==================================================================================
# CONFIG / DB context
# ==================================================================================
DEFAULT_DB = "dm_ib_dev"
EIL_DB = "eil"
DMIB_DB = "dm_ib"

# ==================================================================================
# Build Spark Session
# ==================================================================================
spark = (
    SparkSession.builder
    .appName("wic2_wid2_wia2_full_build")
    .enableHiveSupport()
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("=" * 80)
print("Wealth Data Consolidation - 2025-08-01 to 2026-01-31")
print("=" * 80)

# ==================================================================================
# SOURCE 1: Digital Customer Activity (RCIF_Dig)
# ==================================================================================
print("\n[1/5] Building Digital Customer data...")

Dig_Customer = spark.sql(f"""
    SELECT
        dbm.relt_ibn as ibn,
        dbm.rcif_customer_nbr,
        max(dbm.olb_last_login_date) as lst_login_olb,
        max(dbm.mob_last_login_date) as lst_login_mob,
        max(dbm.ods_business_dt) AS ods_business_dt
    FROM {DMIB_DB}.digital_banking_master dbm
    WHERE dbm.ods_business_dt >= add_months(trunc(current_date, 'MM'), -6)
        AND dbm.ods_business_dt < trunc(current_date, 'MM')
    GROUP BY TRUNC(dbm.ods_business_dt, 'MM'),
        dbm.relt_ibn,
        dbm.rcif_customer_nbr
""")

RCIF_Dig = Dig_Customer.withColumn(
    "PWRANK",
    F.row_number().over(
        Window.partitionBy("rcif_customer_nbr")
        .orderBy(F.col("rcif_customer_nbr").desc())
    )
).withColumn(
    "ods_business_dt", F.col("ods_business_dt")
).withColumn(
    "ibn", F.col("ibn")
).withColumn(
    "rcif_customer_nbr", F.col("rcif_customer_nbr")
).withColumn(
    "Mobile_Active_Flag",
    F.when(F.datediff(F.col("ods_business_dt"), F.col("lst_login_mob")) <= 90, "Mobile Active")
    .otherwise("Non Mobile Active")
).withColumn(
    "Mobile_Flag",
    F.when(F.col("lst_login_mob").isNull(), "Non Mobile User")
    .otherwise("Mobile User")
).withColumn(
    "OLB_Active_Flag",
    F.when(F.datediff(F.col("ods_business_dt"), F.col("lst_login_olb")) <= 90, "OLB Active")
    .otherwise("Non OLB Active")
).withColumn(
    "OLB_Flag",
    F.when(F.col("lst_login_olb").isNull(), "Non OLB User")
    .otherwise("OLB User")
).withColumn(
    "Digitally_Active_Flag",
    F.when(
        (F.datediff(F.col("ods_business_dt"), F.col("lst_login_mob")) <= 90) |
        (F.datediff(F.col("ods_business_dt"), F.col("lst_login_olb")) <= 90),
        "Digital Active"
    ).otherwise("Non Digital Active")
).withColumn(
    "Digital_Flag",
    F.when(F.col("ibn").isNull(), "Non Digital User")
    .otherwise("Digital User")
)

print(f"   Digital records: {RCIF_Dig.count()}")

# ==================================================================================
# SOURCE 2: RCIF Customer Master (rc)
# ==================================================================================
print("\n[2/5] Building RCIF Customer Master...")

rc = spark.sql(f"""
    SELECT 
        max(cast(ip.rcif_cust_nbr AS string)) AS RCIF_NUMBER,
        ip.involved_party_id,
        ip.cust_internet_banking_nbr,
        ip.involved_party_tax_id_nbr,
        ip.birth_date,
        ip.involved_party_name,
        addr.city_name,
        addr.state_name,
        addr.country_name
    FROM {EIL_DB}.d_involved_party_h ip
    
    INNER JOIN {EIL_DB}.d_arrangement_to_involved_party_relationship_h a2i
        ON ip.business_date = a2i.business_date
        AND ip.source_system_code = a2i.source_system_code
        AND ip.involved_party_id = a2i.involved_party_id
    
    INNER JOIN {EIL_DB}.d_arrangement_h ar
        ON a2i.business_date = ar.business_date
        AND a2i.arrangement_source_system_code = ar.source_system_code
        AND a2i.arrangement_id = ar.arrangement_id
        AND ar.source_system_code IN ('BI','BN','TR','DA','SV','CC','MG','LS','TM','PC','LO','BM','CS','IC','MA','PF','PR','SD','CN','EL','LS')
    
    INNER JOIN {EIL_DB}.d_involved_party_address_h addr
        ON ip.involved_party_id = addr.involved_party_id
        AND ip.business_date = addr.business_date
    
    WHERE ip.business_date = (SELECT max(business_date) FROM {EIL_DB}.d_involved_party_h)
        AND ip.source_system_code = 'CF'
        AND nvl(ip.deceased_ind, 'N') = 'N'
        AND ip.birth_date IS NOT NULL
    
    GROUP BY 
        ip.involved_party_id,
        ip.cust_internet_banking_nbr,
        ip.involved_party_tax_id_nbr,
        ip.birth_date,
        ip.involved_party_name,
        addr.city_name,
        addr.state_name,
        addr.country_name
""")

# Add generation cohort
RCIF_Master = rc.withColumn(
    "CUSTOMER_GENERATION",
    F.when(F.col("birth_date").between("1900-01-01", "1924-12-31"), "GI Generation (1900-1924)")
    .when(F.col("birth_date").between("1925-01-01", "1945-12-31"), "Traditionalist (1925-1945)")
    .when(F.col("birth_date").between("1946-01-01", "1964-12-31"), "Baby Boomer (1946-1964)")
    .when(F.col("birth_date").between("1965-01-01", "1980-12-31"), "Gen X (1965-1980)")
    .when(F.col("birth_date").between("1981-01-01", "1996-12-31"), "Millennial (1981-1996)")
    .when(F.col("birth_date") >= "1997-01-01", "Centennial (1997-???)")
    .otherwise("Unknown")
)

print(f"   RCIF Master records: {RCIF_Master.count()}")

# ==================================================================================
# SOURCE 3: RCIF Number List (add_rcifs)
# ==================================================================================
print("\n[3/5] Building RCIF Number list...")

add_rcifs = spark.sql(f"""
    SELECT DISTINCT rcif_cust_nbr as rcif_number
    FROM (
        SELECT rcif_cust_nbr 
        FROM {EIL_DB}.m_involved_party_h
        WHERE cast(business_date as date) >= add_months(current_date, -6)
        
        UNION
        
        SELECT rcif_customer_nbr 
        FROM {DMIB_DB}.digital_banking_master
        WHERE ods_business_dt >= add_months(current_date, -6)
    ) add_rcifs
""")

print(f"   RCIF Numbers: {add_rcifs.count()}")

# ==================================================================================
# SOURCE 4: InvestPath Accounts (INV)
# ==================================================================================
print("\n[4/5] Building InvestPath data...")

Lst_Date = spark.sql(f"""
    SELECT max(ip.business_date) as last_date
    FROM {EIL_DB}.d_involved_party_h ip
""")

last_date_value = Lst_Date.collect()[0]['last_date']

INV = spark.sql(f"""
    SELECT
        ind.rcif_cust_nbr as rcif_nbr,
        ind.involved_party_id as ip_id,
        ar.current_balance_amt as balance,
        ar.open_date,
        ar.arrangement_id as act_cnt
    FROM {EIL_DB}.d_involved_party_h ind
    
    INNER JOIN {EIL_DB}.d_arrangement_to_involved_party_relationship_h a2i
        ON ind.involved_party_id = a2i.involved_party_id
        AND ind.business_date = a2i.business_date
        AND ind.source_system_code = a2i.source_system_code
    
    INNER JOIN {EIL_DB}.d_arrangement_h ar
        ON a2i.arrangement_id = ar.arrangement_id
        AND a2i.arrangement_source_system_code = ar.source_system_code
        AND a2i.business_date = ar.business_date
        AND ar.closed_ind = 'N'
        AND ar.account_type_code = 'IP'
        AND ar.source_system_code = 'RN'
    
    WHERE ind.source_system_code = 'CF'
        AND nvl(ind.deceased_ind, 'N') = 'N'
        AND ind.business_Date = '{last_date_value}'
        AND (SELECT max(business_date) FROM {EIL_DB}.d_involved_party_h) = '{last_date_value}'
""")

print(f"   InvestPath records: {INV.count()}")

# ==================================================================================
# SOURCE 5: Wealth Accounts with Business Segments (PWI)
# ==================================================================================
print("\n[5/5] Building Wealth data...")

last_ip_date = spark.sql(f"""
    SELECT distinct business_date as last_dt
    FROM {EIL_DB}.m_involved_party_h
    WHERE cast(business_date as date) >= add_months(current_date, -6)
""")

PWI = spark.sql(f"""
    SELECT 
        cast(ind.business_date as date) as business_date,
        ind.rcif_cust_nbr AS RCIF_NUMBER,
        ind.involved_party_id as ip_id,
        ind.cust_internet_banking_nbr,
        -- sum(ar.current_balance_amt) as balance,
        
        CASE
            WHEN ind.private_client_code in ('039','539', '339') Then 'Private Wealth'
            WHEN ind.private_client_trust_code in ('239','739') Then 'Private Wealth'
            ELSE CASE
                WHEN ar.business_service_segment_type_code in ('IS_CT', 'IS_IT') then 'Institutional Services'
                WHEN ar.business_service_segment_type_code in ('REGIS_FC', 'REGIS') then 'Investment Services'
                WHEN ar.business_service_segment_type_code in ('PWM') then 'Private Wealth'
                ELSE concat(ar.business_service_segment_type_code, 'Category2??')
            end
        end as Business_Group,
        
        count(DISTINCT(case when ar.business_service_segment_type_code = 'IS_CT' then ar.arrangement_id else null END)) as Corporate_Trust_Count,
        count(DISTINCT(case when ar.business_service_segment_type_code = 'IS_IT' then ar.arrangement_id else null END)) as Institutional_Trust_Count,
        count(DISTINCT(case when ar.business_service_segment_type_code = 'REGIS_FC' then ar.arrangement_id else null END)) as Investment_Count,
        count(DISTINCT(case when ar.business_service_segment_type_code = 'REGIS' then ar.arrangement_id else null END)) as Insurance_Count,
        count(DISTINCT(case when ar.business_service_segment_type_code = 'PWM' then ar.arrangement_id else null END)) as PWM_Count,
        count(DISTINCT(case when ar.source_system_code = 'TR' then ar.arrangement_id else null END)) as Trust_Count,
        count(DISTINCT((case when ar.source_system_code in ('DA','SV','CC','MG','LS','TM','PC','LO','BM','CS','IC','MA','PF','PR','SD') then ar.arrangement_id else null END))) as Banking_Count,
        count(ar.arrangement_id) as accts_cnt
        
    FROM {EIL_DB}.m_involved_party_h ind
    
    INNER JOIN {EIL_DB}.d_arrangement_to_involved_party_relationship_h a2i
        ON ind.business_date = a2i.business_date
        AND ind.involved_party_id = a2i.involved_party_id
        AND ind.source_system_code = a2i.source_system_code
    
    INNER JOIN {EIL_DB}.d_arrangement_h ar
        ON a2i.arrangement_id = ar.arrangement_id
        AND a2i.arrangement_source_system_code = ar.source_system_code
        AND a2i.business_date = ar.business_date
        AND ar.source_system_code in ('BI','BN','TR','DA','SV','CC','MG','LS','TM','PC','LO','BM','CS','IC','MA','PF','PR','SD','CN','EL','LS')
        AND ar.closed_ind = 'N'
    
    WHERE ind.source_system_code = 'CF'
        AND nvl(ind.deceased_ind, 'N') = 'N'
        AND (case
            when ind.private_client_code in ('039','539', '339') Then 1
            when ind.private_client_trust_code in ('239','739') Then 1
            Else Case
                When ar.business_service_segment_type_code in ('IS_CT', 'IS_IT','REGIS_FC', 'REGIS', 'PWM') then 1
                Else 0
            end
        end) = 1
    
    GROUP BY ind.business_date,
        ind.involved_party_id,
        ind.rcif_cust_nbr,
        Business_Group,
        ind.cust_internet_banking_nbr,
        ar.business_service_segment_type_code,
        ind.private_client_code,
        ind.private_client_trust_code
""")

# Add division logic
PWI_with_division = PWI.withColumn(
    "division",
    F.when(F.col("Business_Group") == "Private Wealth",
        F.when((F.col("Trust_Count") > 0) & (F.col("Banking_Count") > 0), "Banking & IMAT")
        .when((F.col("Investment_Count") + F.col("Trust_Count") > 0) & (F.col("Banking_Count") == 0), "Investments Only")
        .otherwise("Banking only")
    ).when(F.col("Business_Group") == "Investment Services",
        F.when((F.col("Investment_Count") > 0) & (F.col("Insurance_Count") == 0), "Investment")
        .when((F.col("Investment_Count") > 0) & (F.col("Insurance_Count") > 0), "Insurance")
        .otherwise("Insurance & Investment")
    ).when(
        (F.col("Corporate_Trust_Count") > 0) & (F.col("Institutional_Trust_Count") == 0), "Corporate Trust"
    ).when(
        (F.col("Corporate_Trust_Count") == 0) & (F.col("Institutional_Trust_Count") > 0), "Institutional Trust"
    ).when(
        F.col("PWM_Count") > 0, "Banking only"
    ).otherwise("Corporate & Institutional Trust")
)

print(f"   Wealth records: {PWI_with_division.count()}")

# ==================================================================================
# BUILD TABLE 1: wias1 (Customer Dimension)
# ==================================================================================
print("\n" + "=" * 80)
print("Building wias1 (Customer Dimension Table)")
print("=" * 80)

# Join RCIF Master with Digital flags
wias1_base = RCIF_Master.join(
    RCIF_Dig.select(
        "rcif_customer_nbr",
        "ibn",
        "ods_business_dt",
        "Mobile_Active_Flag",
        "Mobile_Flag",
        "OLB_Active_Flag",
        "OLB_Flag",
        "Digitally_Active_Flag",
        "Digital_Flag"
    ),
    RCIF_Master["RCIF_NUMBER"] == RCIF_Dig["rcif_customer_nbr"],
    "left"
)

# Join with Wealth to get Business Group
wias1 = wias1_base.join(
    PWI_with_division.select(
        F.col("RCIF_NUMBER").alias("PWI_RCIF"),
        "business_date",
        "Business_Group",
        "division"
    ),
    wias1_base["RCIF_NUMBER"] == F.col("PWI_RCIF"),
    "left"
).select(
    F.col("RCIF_NUMBER").alias("rcif_number"),
    F.col("involved_party_id").alias("ip_id"),
    F.col("cust_internet_banking_nbr").alias("ibn"),
    F.col("involved_party_tax_id_nbr").alias("tax_id"),
    F.col("birth_date"),
    F.col("involved_party_name").alias("customer_name"),
    F.col("city_name"),
    F.col("state_name"),
    F.col("country_name"),
    F.col("CUSTOMER_GENERATION").alias("generation"),
    F.col("Business_Group").alias("business_group"),
    F.col("division"),
    F.col("ods_business_dt"),
    F.col("Mobile_Active_Flag").alias("mobile_active"),
    F.col("Mobile_Flag").alias("mobile_user"),
    F.col("OLB_Active_Flag").alias("olb_active"),
    F.col("OLB_Flag").alias("olb_user"),
    F.col("Digitally_Active_Flag").alias("digitally_active"),
    F.col("Digital_Flag").alias("digital_user")
)

print(f"\nwias1 Customer records: {wias1.count()}")
wias1.createOrReplaceTempView("wias1")

# Save to table
wias1.write.mode("overwrite").saveAsTable(f"{DEFAULT_DB}.wias1")
print(f"✓ wias1 saved to {DEFAULT_DB}.wias1")

# ==================================================================================
# BUILD TABLE 2: wics1 (Account Fact Table)
# ==================================================================================
print("\n" + "=" * 80)
print("Building wics1 (Account Fact Table)")
print("=" * 80)

# Combine InvestPath and Wealth account data
wics1 = PWI_with_division.select(
    F.col("business_date"),
    F.col("RCIF_NUMBER").alias("rcif_number"),
    F.col("ip_id"),
    F.col("cust_internet_banking_nbr").alias("ibn"),
    F.col("Business_Group").alias("business_group"),
    F.col("division"),
    F.col("accts_cnt").alias("account_count"),
    F.col("Corporate_Trust_Count").alias("corporate_trust_cnt"),
    F.col("Institutional_Trust_Count").alias("institutional_trust_cnt"),
    F.col("Investment_Count").alias("investment_cnt"),
    F.col("Insurance_Count").alias("insurance_cnt"),
    F.col("PWM_Count").alias("pwm_cnt"),
    F.col("Trust_Count").alias("trust_cnt"),
    F.col("Banking_Count").alias("banking_cnt")
).union(
    INV.select(
        F.lit(last_date_value).alias("business_date"),
        F.col("rcif_nbr").alias("rcif_number"),
        F.col("ip_id"),
        F.lit(None).cast("string").alias("ibn"),
        F.lit("Investment Services").alias("business_group"),
        F.lit("InvestPath").alias("division"),
        F.col("act_cnt").cast("long").alias("account_count"),
        F.lit(0).cast("long").alias("corporate_trust_cnt"),
        F.lit(0).cast("long").alias("institutional_trust_cnt"),
        F.lit(1).cast("long").alias("investment_cnt"),
        F.lit(0).cast("long").alias("insurance_cnt"),
        F.lit(0).cast("long").alias("pwm_cnt"),
        F.lit(0).cast("long").alias("trust_cnt"),
        F.lit(0).cast("long").alias("banking_cnt")
    )
)

print(f"\nwics1 Account records: {wics1.count()}")
wics1.createOrReplaceTempView("wics1")

# Save to table
wics1.write.mode("overwrite").saveAsTable(f"{DEFAULT_DB}.wics1")
print(f"✓ wics1 saved to {DEFAULT_DB}.wics1")

# ==================================================================================
# SUMMARY
# ==================================================================================
print("\n" + "=" * 80)
print("CONSOLIDATION COMPLETE")
print("=" * 80)
print(f"✓ wias1 (Customer): {wias1.count()} records")
print(f"✓ wics1 (Account): {wics1.count()} records")
print("\nTables created:")
print(f"  - {DEFAULT_DB}.wias1")
print(f"  - {DEFAULT_DB}.wics1")
print("=" * 80)

# Show sample data
print("\nSample wias1 (Customer) data:")
wias1.show(5, truncate=False)

print("\nSample wics1 (Account) data:")
wics1.show(5, truncate=False)

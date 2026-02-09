# ==================================================================================
# Wealth Data Consolidation - SIMPLE VERSION
# Just run the 5 original SQL queries and save them as-is
# Then create 2 simple output tables
# ==================================================================================

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

DEFAULT_DB = "dm_ib_dev"
EIL_DB = "eil"
DMIB_DB = "dm_ib"

spark = (
    SparkSession.builder
    .appName("wic2_wid2_wia2_full_build")
    .enableHiveSupport()
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("=" * 80)
print("Wealth Data Consolidation - SIMPLE EXACT REPLICATION")
print("=" * 80)

# ==================================================================================
# TABLE 1: wealth (PWI) - EXACTLY as in your original SQL
# ==================================================================================
print("\n[1/5] Building Wealth (PWI)...")

# Get the last date from involved_party
last_date = spark.sql(f"""
    SELECT max(business_date) as last_dt
    FROM {EIL_DB}.d_involved_party_h
""").collect()[0]['last_dt']

print(f"   Using latest business_date: {last_date}")

# Get distinct dates from last 6 months
last_ip_dates = spark.sql(f"""
    SELECT distinct business_date as last_dt
    FROM {EIL_DB}.m_involved_party_h
    WHERE cast(business_date as date) >= add_months(current_date, -6)
""")
last_ip_dates.createOrReplaceTempView("last_ip_date")

print(f"   Found {last_ip_dates.count()} distinct business dates in last 6 months")

# Now build PWI exactly as original
PWI = spark.sql(f"""
    SELECT 
        cast(ind.business_date as date) as business_date,
        ind.rcif_cust_nbr AS RCIF_NUMBER,
        ind.cust_internet_banking_nbr,
        ind.involved_party_id as ip_id,
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
    
    INNER JOIN last_ip_date 
        ON ind.business_date = last_ip_date.last_dt
    
    INNER JOIN {EIL_DB}.d_arrangement_to_involved_party_relationship_h a2i
        ON ind.involved_party_id = a2i.involved_party_id
        AND ind.business_date = a2i.business_date
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

# Add division
PWI_final = PWI.withColumn(
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
).select(
    "business_date",
    "ip_id",
    "cust_internet_banking_nbr",
    "RCIF_NUMBER",
    "Business_Group",
    "division",
    "accts_cnt"
)

print(f"   PWI records: {PWI_final.count()}")
print(f"   PWI distinct RCIFs: {PWI_final.select('RCIF_NUMBER').distinct().count()}")

PWI_final.createOrReplaceTempView("PWI")

# ==================================================================================
# TABLE 2: RCIF (rc) - Customer master
# ==================================================================================
print("\n[2/5] Building RCIF (rc)...")

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
        AND ar.source_system_code in ('BI','BN','TR','DA','SV','CC','MG','LS','TM','PC','LO','BM','CS','IC','MA','PF','PR','SD','CN','EL','LS')
    
    INNER JOIN {EIL_DB}.d_involved_party_address_h addr
        ON ip.involved_party_id = addr.involved_party_id
        AND ip.business_date = addr.business_date
    
    WHERE ip.business_date = '{last_date}'
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

print(f"   RCIF records: {rc.count()}")
rc.createOrReplaceTempView("rc")

# ==================================================================================
# TABLE 3: Digital (RCIF_Dig)
# ==================================================================================
print("\n[3/5] Building Digital (RCIF_Dig)...")

Dig_Customer = spark.sql(f"""
    SELECT
        dbm.relt_ibn,
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
    F.when(F.col("relt_ibn").isNull(), "Non Digital User")
    .otherwise("Digital User")
)

print(f"   Digital records: {RCIF_Dig.count()}")
RCIF_Dig.createOrReplaceTempView("RCIF_Dig")

# ==================================================================================
# TABLE 4: InvestPath (INV)
# ==================================================================================
print("\n[4/5] Building InvestPath (INV)...")

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
        AND ind.business_Date = '{last_date}'
""")

print(f"   InvestPath records: {INV.count()}")
INV.createOrReplaceTempView("INV")

# ==================================================================================
# CREATE wias1 (Customer Dimension)
# ==================================================================================
print("\n" + "=" * 80)
print("Creating wias1 (Customer Dimension)")
print("=" * 80)

wias1 = spark.sql("""
    SELECT 
        rc.RCIF_NUMBER as rcif_number,
        rc.involved_party_id as ip_id,
        rc.cust_internet_banking_nbr as ibn,
        rc.state_name,
        dig.Mobile_Active_Flag,
        dig.Mobile_Flag,
        dig.OLB_Active_Flag,
        dig.OLB_Flag,
        dig.Digitally_Active_Flag,
        dig.Digital_Flag
    FROM rc
    LEFT JOIN RCIF_Dig dig
        ON rc.cust_internet_banking_nbr = dig.relt_ibn
""")

print(f"wias1 records: {wias1.count()}")
print(f"wias1 distinct RCIFs: {wias1.select('rcif_number').distinct().count()}")

wias1.write.mode("overwrite").saveAsTable(f"{DEFAULT_DB}.wias1")
print(f"✓ Saved to {DEFAULT_DB}.wias1")

# ==================================================================================
# CREATE wics1 (Account Fact)
# ==================================================================================
print("\n" + "=" * 80)
print("Creating wics1 (Account Fact)")
print("=" * 80)

# Wealth accounts
wics1_wealth = spark.sql("""
    SELECT 
        business_date,
        RCIF_NUMBER as rcif_number,
        ip_id,
        cust_internet_banking_nbr as ibn,
        Business_Group as business_group,
        division,
        accts_cnt
    FROM PWI
""")

# InvestPath accounts
wics1_invest = spark.sql(f"""
    SELECT 
        date('{last_date}') as business_date,
        rcif_nbr as rcif_number,
        ip_id,
        CAST(NULL AS STRING) as ibn,
        'Investment Services' as business_group,
        'InvestPath' as division,
        1 as accts_cnt
    FROM INV
""")

wics1 = wics1_wealth.union(wics1_invest)

print(f"wics1 records: {wics1.count()}")
print(f"wics1 total accts_cnt: {wics1.agg(F.sum('accts_cnt')).collect()[0][0]}")
print(f"wics1 distinct RCIFs: {wics1.select('rcif_number').distinct().count()}")

wics1.write.mode("overwrite").saveAsTable(f"{DEFAULT_DB}.wics1")
print(f"✓ Saved to {DEFAULT_DB}.wics1")

print("\n" + "=" * 80)
print("CONSOLIDATION COMPLETE")
print("=" * 80)

# Quick validation
print("\nQuick Validation:")
spark.sql("SELECT COUNT(DISTINCT rcif_number) as wealth_customers FROM PWI").show()
spark.sql("SELECT SUM(accts_cnt) as total_accounts FROM PWI").show()

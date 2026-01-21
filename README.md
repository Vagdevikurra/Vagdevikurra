#!/usr/bin/env python3
# ============================================================
# Wealth Insights – FINAL Corrected 2-Table ETL
# Builds ONLY:
#   1) dm_ib_dev.wealth_insights_customer_dim
#   2) dm_ib_dev.wealth_insights_accounts_fact
# ============================================================

from pyspark.sql import SparkSession

APP_NAME = "wealth_insights_final_etl"
DB = "dm_ib_dev"
DIM = f"{DB}.wealth_insights_customer_dim"
FACT = f"{DB}.wealth_insights_accounts_fact"

def spark_session():
    spark = (
        SparkSession.builder
        .appName(APP_NAME)
        .enableHiveSupport()
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )
    spark.sql(f"USE {DB}")
    spark.sparkContext.setLogLevel("WARN")
    spark.sql("SET hive.exec.dynamic.partition=true")
    spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict")
    return spark

def main():
    spark = spark_session()

    # ========================================================
    # 1) DATE WINDOW – LAST 6 COMPLETE MONTHS
    # ========================================================
    spark.sql("""
    CREATE OR REPLACE TEMP VIEW date_window AS
    SELECT
        add_months(trunc(current_date(),'MM'), -6) AS start_month,
        trunc(current_date(),'MM') AS end_month
    """)

    # ========================================================
    # 2) RCIF UNIVERSE (STRICTLY WINDOWED)
    # ========================================================
    spark.sql("""
    CREATE OR REPLACE TEMP VIEW rcif_universe AS
    SELECT DISTINCT CAST(rcif_number AS STRING) AS rcif_number
    FROM (
        SELECT CAST(rcif_cust_nbr AS STRING) rcif_number
        FROM eil.m_involved_party_h
        WHERE business_date >= (SELECT start_month FROM date_window)

        UNION ALL

        SELECT CAST(rcif_customer_nbr AS STRING)
        FROM dm_ib.digital_banking_master
        WHERE ods_business_dt >= (SELECT start_month FROM date_window)
    ) u
    WHERE rcif_number IS NOT NULL AND TRIM(rcif_number) <> ''
    """)

    # ========================================================
    # 3) CUSTOMER DIM (LATEST SNAPSHOT)
    # ========================================================
    spark.sql("""
    CREATE OR REPLACE TEMP VIEW latest_ip_dt AS
    SELECT MAX(CAST(business_date AS DATE)) AS dt
    FROM eil.d_involved_party_h
    """)

    spark.sql("""
    CREATE OR REPLACE TEMP VIEW customer_dim AS
    SELECT
        CAST(ip.rcif_cust_nbr AS STRING) AS rcif_number,
        CAST(ip.involved_party_id AS STRING) AS involved_party_id,
        CAST(ip.cust_internet_banking_nbr AS STRING) AS cust_internet_banking_nbr,
        CAST(ip.involved_party_name AS STRING) AS involved_party_name,
        CAST(ip.birth_date AS DATE) AS birth_date,
        CASE
            WHEN ip.birth_date BETWEEN DATE '1946-01-01' AND DATE '1964-12-31' THEN 'Baby Boomer'
            WHEN ip.birth_date BETWEEN DATE '1965-01-01' AND DATE '1980-12-31' THEN 'Gen X'
            WHEN ip.birth_date BETWEEN DATE '1981-01-01' AND DATE '1996-12-31' THEN 'Millennial'
            WHEN ip.birth_date >= DATE '1997-01-01' THEN 'Gen Z'
            ELSE 'Unknown'
        END AS customer_generation,
        CAST(addr.city_name AS STRING) AS city_name,
        CAST(addr.state_name AS STRING) AS state_name,
        CAST(addr.country_name AS STRING) AS country_name,
        (SELECT dt FROM latest_ip_dt) AS as_of_date
    FROM eil.d_involved_party_h ip
    JOIN latest_ip_dt d
      ON CAST(ip.business_date AS DATE) = d.dt
    LEFT JOIN eil.d_involved_party_address_h addr
      ON ip.involved_party_id = addr.involved_party_id
     AND ip.business_date = addr.business_date
    WHERE ip.source_system_code = 'CF'
      AND NVL(ip.deceased_ind,'N') = 'N'
    """)

    spark.sql(f"DROP TABLE IF EXISTS {DIM}")
    spark.sql(f"""
    CREATE TABLE {DIM}
    STORED AS PARQUET AS
    SELECT d.*
    FROM customer_dim d
    JOIN rcif_universe u
      ON d.rcif_number = u.rcif_number
    """)

    # ========================================================
    # 4) DIGITAL MONTHLY (CORRECT LOGIC)
    # ========================================================
    spark.sql("""
    CREATE OR REPLACE TEMP VIEW digital_customer_month AS
    SELECT
        trunc(ods_business_dt,'MM') AS month_dt,
        CAST(rcif_customer_nbr AS STRING) AS rcif_number,
        COUNT(DISTINCT ibn) AS digital_enrollments_cnt,
        MAX(CASE WHEN datediff(trunc(ods_business_dt,'MM'), mob_last_login_date) <= 90 THEN 1 ELSE 0 END) AS mobile_active_ind,
        MAX(CASE WHEN datediff(trunc(ods_business_dt,'MM'), olb_last_login_date) <= 90 THEN 1 ELSE 0 END) AS olb_active_ind,
        MAX(CASE
            WHEN datediff(trunc(ods_business_dt,'MM'), mob_last_login_date) <= 90
              OR datediff(trunc(ods_business_dt,'MM'), olb_last_login_date) <= 90
            THEN 1 ELSE 0 END) AS digital_active_ind,
        MAX(CASE WHEN mob_last_login_date IS NOT NULL THEN 1 ELSE 0 END) AS mobile_user_ind,
        MAX(CASE WHEN olb_last_login_date IS NOT NULL THEN 1 ELSE 0 END) AS olb_user_ind
    FROM dm_ib.digital_banking_master
    WHERE ods_business_dt >= (SELECT start_month FROM date_window)
      AND ods_business_dt <  (SELECT end_month FROM date_window)
    GROUP BY trunc(ods_business_dt,'MM'), rcif_customer_nbr
    """)

    # ========================================================
    # 5) WEALTH MONTHLY (STRICT SNAPSHOT JOIN)
    # ========================================================
    spark.sql("""
    CREATE OR REPLACE TEMP VIEW wealth_customer_month AS
    SELECT
        trunc(ind.business_date,'MM') AS month_dt,
        CAST(ind.rcif_cust_nbr AS STRING) AS rcif_number,
        CASE
            WHEN ind.private_client_code IN ('039','539','339')
              OR ind.private_client_trust_code IN ('239','739')
            THEN 'Private Wealth'
            WHEN ar.business_service_segment_type_code IN ('IS_CT','IS_IT')
            THEN 'Institutional Services'
            ELSE 'Investment Services'
        END AS business_group,
        COUNT(DISTINCT ar.arrangement_id) AS wealth_accounts_cnt,
        COUNT(DISTINCT CASE WHEN ar.account_type_code = 'TR' THEN ar.arrangement_id END) AS trust_count,
        COUNT(DISTINCT CASE WHEN ar.account_type_code IN ('DA','SV','CC') THEN ar.arrangement_id END) AS banking_count,
        COUNT(DISTINCT CASE WHEN ar.account_type_code = 'REGIS_FC' THEN ar.arrangement_id END) AS investment_count,
        COUNT(DISTINCT CASE WHEN ar.account_type_code = 'REGIS' THEN ar.arrangement_id END) AS insurance_count
    FROM eil.m_involved_party_h ind
    JOIN eil.m_arrangement_to_involved_party_relationship_h a2i
      ON ind.involved_party_id = a2i.involved_party_id
     AND ind.business_date = a2i.business_date
    JOIN eil.m_arrangement_h ar
      ON a2i.arrangement_id = ar.arrangement_id
     AND a2i.business_date = ar.business_date
    WHERE ind.source_system_code = 'CF'
      AND NVL(ind.deceased_ind,'N') = 'N'
      AND ar.closed_ind = 'N'
      AND ind.business_date >= (SELECT start_month FROM date_window)
      AND ind.business_date <  (SELECT end_month FROM date_window)
    GROUP BY trunc(ind.business_date,'MM'), ind.rcif_cust_nbr,
             ind.private_client_code, ind.private_client_trust_code,
             ar.business_service_segment_type_code
    """)

    # ========================================================
    # 6) INVESTPATH – PINNED TO LATEST MONTH
    # ========================================================
    spark.sql("""
    CREATE OR REPLACE TEMP VIEW latest_fact_month AS
    SELECT MAX(month_dt) AS month_dt
    FROM digital_customer_month
    """)

    spark.sql("""
    CREATE OR REPLACE TEMP VIEW investpath_customer_month AS
    SELECT
        (SELECT month_dt FROM latest_fact_month) AS month_dt,
        CAST(ind.rcif_cust_nbr AS STRING) AS rcif_number,
        COUNT(DISTINCT ind.involved_party_id) AS investpath_customers_cnt,
        COUNT(DISTINCT ar.arrangement_id) AS investpath_accounts_cnt,
        SUM(ar.current_balance_amt) AS investpath_balance_amt,
        SUM(CASE WHEN ar.current_balance_amt > 0 THEN 1 ELSE 0 END) AS investpath_accounts_funded_cnt,
        MIN(ar.open_date) AS investpath_first_open_date
    FROM eil.d_involved_party_h ind
    JOIN eil.d_arrangement_to_involved_party_relationship_h a2i
      ON ind.involved_party_id = a2i.involved_party_id
     AND ind.business_date = a2i.business_date
    JOIN eil.d_arrangement_h ar
      ON a2i.arrangement_id = ar.arrangement_id
     AND a2i.business_date = ar.business_date
    WHERE ar.account_type_code = 'IP'
      AND ar.source_system_code = 'RN'
      AND ar.closed_ind = 'N'
    GROUP BY ind.rcif_cust_nbr
    """)

    # ========================================================
    # 7) FINAL FACT
    # ========================================================
    spark.sql(f"DROP TABLE IF EXISTS {FACT}")
    spark.sql(f"""
    CREATE TABLE {FACT}
    STORED AS PARQUET AS
    SELECT
        COALESCE(d.month_dt, w.month_dt, i.month_dt) AS month_dt,
        COALESCE(d.rcif_number, w.rcif_number, i.rcif_number) AS rcif_number,

        d.digital_enrollments_cnt,
        d.digital_active_ind,
        d.mobile_active_ind,
        d.olb_active_ind,
        d.mobile_user_ind,
        d.olb_user_ind,

        w.business_group,
        w.wealth_accounts_cnt,
        w.trust_count,
        w.banking_count,
        w.investment_count,
        w.insurance_count,

        i.investpath_customers_cnt,
        i.investpath_accounts_cnt,
        i.investpath_balance_amt,
        i.investpath_accounts_funded_cnt,
        i.investpath_first_open_date

    FROM digital_customer_month d
    FULL OUTER JOIN wealth_customer_month w
      ON d.month_dt = w.month_dt AND d.rcif_number = w.rcif_number
    FULL OUTER JOIN investpath_customer_month i
      ON COALESCE(d.rcif_number, w.rcif_number) = i.rcif_number
     AND COALESCE(d.month_dt, w.month_dt) = i.month_dt
    """)

    spark.stop()

if __name__ == "__main__":
    main()

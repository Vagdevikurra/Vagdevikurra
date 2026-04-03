"""
STEP 0 — Customer Dimension
Output: dm_ib_dev.v1_customer_dim
"""

import v1_config as cfg

spark = cfg.get_spark()

print("Building FIXED customer dimension...")
latest_bd = spark.sql("""
    SELECT MAX(BUSINESS_DATE) AS BD
    FROM EIL.M_INVOLVED_PARTY_H
    WHERE RCIF_CUST_NBR IS NOT NULL
""").first()["BD"]

print(f"Using BUSINESS_DATE = {latest_bd}")
cust_sql = f"""
    WITH IP_DEDUP AS (
        SELECT
            IP.*,
            ROW_NUMBER() OVER (
                PARTITION BY IP.RCIF_CUST_NBR
                ORDER BY IP.PRIORITY_HOUSEHOLD_IND DESC
            ) AS rn
        FROM EIL.M_INVOLVED_PARTY_H IP
        WHERE IP.BUSINESS_DATE = '{latest_bd}'
          AND IP.RCIF_CUST_NBR IS NOT NULL
          AND IP.DECEASED_DATE IS NULL
    ),

    DB_DEDUP AS (
        SELECT
            RCIF_CUSTOMER_NBR,
            MAX(IBN) AS IBN
        FROM DM_IB.DIGITAL_BANKING_MASTER
        GROUP BY RCIF_CUSTOMER_NBR
    )

    SELECT
        IP.RCIF_CUST_NBR,
        IP.INVOLVED_PARTY_ID,
        IP.INVOLVED_PARTY_NAME,
        IP.CUST_INTERNET_BANKING_NBR,

        CASE
            WHEN DB.IBN IS NOT NULL THEN 'Digital Customer'
            ELSE 'Non-Digital Customer'
        END AS DIGITAL_CUSTOMER_CHECK,

        IP.BIRTH_DATE,
        IP.GENDER_TYPE_CODE AS CUSTOMER_GENDER,

        CASE
            WHEN IP.BIRTH_DATE BETWEEN '1946-01-01' AND '1964-12-31' THEN 'Baby Boomer'
            WHEN IP.BIRTH_DATE BETWEEN '1965-01-01' AND '1980-12-31' THEN 'Gen X'
            WHEN IP.BIRTH_DATE BETWEEN '1981-01-01' AND '1996-12-31' THEN 'Millennial'
            WHEN IP.BIRTH_DATE >= '1997-01-01'                       THEN 'Centennial'
            ELSE 'Other/Unknown'
        END AS CUSTOMER_GENERATION,

        CASE
            WHEN ROUND(DATEDIFF('{latest_bd}', IP.BIRTH_DATE)/365,0) <= 17  THEN 'Less than 18'
            WHEN ROUND(DATEDIFF('{latest_bd}', IP.BIRTH_DATE)/365,0) <= 24  THEN '18-24'
            WHEN ROUND(DATEDIFF('{latest_bd}', IP.BIRTH_DATE)/365,0) <= 34  THEN '25-34'
            WHEN ROUND(DATEDIFF('{latest_bd}', IP.BIRTH_DATE)/365,0) <= 44  THEN '35-44'
            WHEN ROUND(DATEDIFF('{latest_bd}', IP.BIRTH_DATE)/365,0) <= 54  THEN '45-54'
            WHEN ROUND(DATEDIFF('{latest_bd}', IP.BIRTH_DATE)/365,0) <= 64  THEN '55-64'
            WHEN ROUND(DATEDIFF('{latest_bd}', IP.BIRTH_DATE)/365,0) >= 65  THEN '65+'
            ELSE 'Unknown'
        END AS AGE_BANDING

    FROM IP_DEDUP IP
    LEFT JOIN DB_DEDUP DB
        ON IP.RCIF_CUST_NBR = DB.RCIF_CUSTOMER_NBR
    WHERE IP.rn = 1
"""

cust_df = spark.sql(cust_sql)

cfg.save_to_hive(cust_df, "v1_customer_dim")
print("Customer dimension ready.")

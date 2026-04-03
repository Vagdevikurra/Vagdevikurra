"""
STEP 0 — Customer Dimension (with FI Status)
Output: dm_ib_dev.v1_customer_dim
"""

import v1_config as cfg

spark = cfg.get_spark()

print("Building customer dimension with FI status...")
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
    ),

    FI_STATUS AS (
        SELECT
            IP2.RCIF_CUST_NBR,
            CASE
                WHEN SUM(CASE WHEN ARR.CLOSED_IND = 'N' THEN 1 ELSE 0 END) > 0
                THEN 'FI Enabled'
                ELSE 'FI Disabled'
            END AS FI_STATUS
        FROM EIL.M_ARRANGEMENT_H ARR
        INNER JOIN EIL.M_ARRANGEMENT_TO_INVOLVED_PARTY_RELATIONSHIP_H REL
            ON ARR.ARRANGEMENT_ID = REL.ARRANGEMENT_ID
           AND ARR.BUSINESS_DATE  = REL.BUSINESS_DATE
        INNER JOIN EIL.M_INVOLVED_PARTY_H IP2
            ON REL.INVOLVED_PARTY_ID = IP2.INVOLVED_PARTY_ID
           AND REL.BUSINESS_DATE     = IP2.BUSINESS_DATE
        WHERE ARR.BUSINESS_DATE = '{latest_bd}'
        GROUP BY IP2.RCIF_CUST_NBR
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
        END AS AGE_BANDING,

        COALESCE(FI.FI_STATUS, 'Unknown') AS FI_STATUS

    FROM IP_DEDUP IP
    LEFT JOIN DB_DEDUP DB
        ON IP.RCIF_CUST_NBR = DB.RCIF_CUSTOMER_NBR
    LEFT JOIN FI_STATUS FI
        ON IP.RCIF_CUST_NBR = FI.RCIF_CUST_NBR
    WHERE IP.rn = 1
"""

cust_df = spark.sql(cust_sql)

cfg.save_to_hive(cust_df, "v1_customer_dim")
print("Customer dimension with FI status ready.")

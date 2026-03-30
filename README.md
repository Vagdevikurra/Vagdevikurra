"""
V1 Script 02: TSMT Tables (CDSW) — SAFE VERSION
==================================================
Runs ONE DAY at a time. Ordered easiest-to-hardest.
If a query crashes, comment it out and run the rest.
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("V1_02_TSMT") \
    .enableHiveSupport() \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

DAY = "2025-03-01"
BIZ = "2025-03-01"


# --- TEST: confirm access ---
print("TEST: row count check")
spark.sql(f"""
    SELECT COUNT(*) AS cnt
    FROM sl1_tsmt.tsmt_authentication_h
    WHERE ODS_BUSINESS_DT = '{DAY}' AND FAILURE = 0
""").show()


# --- REQ #9: Auth Events (no JSON, direct columns) ---
print("=" * 50 + "\nREQ #9: Auth Events\n" + "=" * 50)
spark.sql(f"""
    SELECT ACTION, COUNT(*) AS events, COUNT(DISTINCT SESSION_ID) AS sessions
    FROM sl1_tsmt.tsmt_authentication_h
    WHERE ODS_BUSINESS_DT = '{DAY}' AND FAILURE = 0
    GROUP BY ACTION ORDER BY events DESC
""").show(30, truncate=False)


# --- REQ #10: Timestamps sample ---
print("=" * 50 + "\nREQ #10: Timestamps\n" + "=" * 50)
spark.sql(f"""
    SELECT ODS_BUSINESS_DT, ODS_BUSINESS_TS, SESSION_ID, ACTION,
           AUTHENTICATION_METHOD, APPLICATION_ID
    FROM sl1_tsmt.tsmt_authentication_h
    WHERE ODS_BUSINESS_DT = '{DAY}' AND FAILURE = 0 LIMIT 10
""").show(10, truncate=False)


# --- REQ #3: Device & Platform (DEVICE_H is small ~500K/day) ---
print("=" * 50 + "\nREQ #3: Device & Platform\n" + "=" * 50)
spark.sql(f"""
    SELECT DEVICE_OS_TYPE,
        get_json_object(DATA, '$.hw_type') AS hw_type,
        get_json_object(DATA, '$.device_model') AS device_model,
        COUNT(*) AS events, COUNT(DISTINCT SESSION_ID) AS sessions
    FROM sl1_tsmt.tsmt_device_h
    WHERE ODS_BUSINESS_DT = '{DAY}' AND FAILURE = 0
    GROUP BY DEVICE_OS_TYPE,
        get_json_object(DATA, '$.hw_type'),
        get_json_object(DATA, '$.device_model')
    ORDER BY sessions DESC
""").show(30, truncate=False)


# --- REQ #14: FI Enabled vs Disabled (DEVICE_H) ---
print("=" * 50 + "\nREQ #14: FI Enabled vs Disabled\n" + "=" * 50)
spark.sql(f"""
    SELECT get_json_object(DATA, '$.device_public_key') AS pub_key,
        get_json_object(DATA, '$.rsaEncryptionEnabled') AS rsa,
        COUNT(*) AS events, COUNT(DISTINCT SESSION_ID) AS sessions
    FROM sl1_tsmt.tsmt_device_h
    WHERE ODS_BUSINESS_DT = '{DAY}' AND FAILURE = 0
    GROUP BY get_json_object(DATA, '$.device_public_key'),
        get_json_object(DATA, '$.rsaEncryptionEnabled')
    ORDER BY sessions DESC
""").show(20, truncate=False)


# --- REQ #1/#8/#15: Channel + Connection + Aggregator (ENRICHMENT_H) ---
print("=" * 50 + "\nREQ #1/#8/#15: Channel/Connection/Aggregator\n" + "=" * 50)
spark.sql(f"""
    SELECT channel, connection_type, aggregator_flag,
        COUNT(*) AS events, COUNT(DISTINCT session_id) AS sessions
    FROM (
        SELECT SESSION_ID,
            get_json_object(DATA, '$.channel') AS channel,
            get_json_object(DATA, '$.connection') AS connection_type,
            get_json_object(DATA, '$.AggregatorRequest') AS aggregator_flag
        FROM sl1_tsmt.tsmt_enrichment_h
        WHERE ODS_BUSINESS_DT = '{DAY}' AND FAILURE = 0 AND DATA IS NOT NULL
    ) t
    GROUP BY channel, connection_type, aggregator_flag
    ORDER BY events DESC
""").show(40, truncate=False)


# --- REQ #2: ChannelSessionId sample ---
print("=" * 50 + "\nREQ #2: ChannelSessionId\n" + "=" * 50)
spark.sql(f"""
    SELECT SESSION_ID,
        get_json_object(DATA, '$.channel') AS channel,
        get_json_object(DATA, '$.ChannelSessionId') AS ch_sess_id
    FROM sl1_tsmt.tsmt_enrichment_h
    WHERE ODS_BUSINESS_DT = '{DAY}' AND FAILURE = 0 AND DATA IS NOT NULL
      AND get_json_object(DATA, '$.ChannelSessionId') IS NOT NULL
    LIMIT 10
""").show(10, truncate=False)


# --- REQ #4: Customer Age Groups ---
print("=" * 50 + "\nREQ #4: Customer Age Groups\n" + "=" * 50)
spark.sql(f"""
    WITH enr AS (
        SELECT DISTINCT
            get_json_object(DATA, '$.RCIFId') AS rcif_id,
            get_json_object(DATA, '$.channel') AS channel,
            SESSION_ID
        FROM sl1_tsmt.tsmt_enrichment_h
        WHERE ODS_BUSINESS_DT = '{DAY}' AND FAILURE = 0
          AND get_json_object(DATA, '$.RCIFId') IS NOT NULL
    ),
    cust AS (
        SELECT db.rcif_customer_nbr,
            CASE
                WHEN ROUND(DATEDIFF(ip.BUSINESS_DATE, ip.BIRTH_DATE)/365,0) BETWEEN 18 AND 24 THEN '18-24'
                WHEN ROUND(DATEDIFF(ip.BUSINESS_DATE, ip.BIRTH_DATE)/365,0) BETWEEN 25 AND 34 THEN '25-34'
                WHEN ROUND(DATEDIFF(ip.BUSINESS_DATE, ip.BIRTH_DATE)/365,0) BETWEEN 35 AND 44 THEN '35-44'
                WHEN ROUND(DATEDIFF(ip.BUSINESS_DATE, ip.BIRTH_DATE)/365,0) BETWEEN 45 AND 54 THEN '45-54'
                WHEN ROUND(DATEDIFF(ip.BUSINESS_DATE, ip.BIRTH_DATE)/365,0) BETWEEN 55 AND 64 THEN '55-64'
                WHEN ROUND(DATEDIFF(ip.BUSINESS_DATE, ip.BIRTH_DATE)/365,0) >= 65 THEN '65+'
                ELSE 'Other'
            END AS age_banding
        FROM dm_ib.digital_banking_master db
        INNER JOIN eil.m_involved_party_h ip
            ON db.rcif_customer_nbr = ip.rcif_cust_nbr
           AND ip.BUSINESS_DATE = '{BIZ}' AND ip.SOURCE_SYSTEM_CODE = 'CF'
           AND ip.DECEASED_DATE IS NULL
        WHERE db.ods_business_dt = '{BIZ}'
    )
    SELECT c.age_banding,
        COUNT(DISTINCT e.rcif_id) AS customers,
        COUNT(DISTINCT e.SESSION_ID) AS sessions,
        SUM(CASE WHEN UPPER(e.channel) = 'MBANK' THEN 1 ELSE 0 END) AS mobile
    FROM enr e INNER JOIN cust c ON e.rcif_id = c.rcif_customer_nbr
    GROUP BY c.age_banding ORDER BY c.age_banding
""").show(20, truncate=False)


# --- REQ #5-7: OTP by Type x Age ---
print("=" * 50 + "\nREQ #5-7: OTP by Type x Age\n" + "=" * 50)
spark.sql(f"""
    WITH otp AS (
        SELECT SESSION_ID,
            get_json_object(DATA, '$.channel.channel_type') AS otp_type
        FROM sl1_tsmt.tsmt_session_h
        WHERE ODS_BUSINESS_DT = '{DAY}' AND FAILURE = 0
          AND get_json_object(DATA, '$.channel.channel_type') IS NOT NULL
    ),
    enr AS (
        SELECT DISTINCT SESSION_ID,
            get_json_object(DATA, '$.RCIFId') AS rcif_id
        FROM sl1_tsmt.tsmt_enrichment_h
        WHERE ODS_BUSINESS_DT = '{DAY}' AND FAILURE = 0
          AND get_json_object(DATA, '$.RCIFId') IS NOT NULL
    ),
    cust AS (
        SELECT db.rcif_customer_nbr,
            CASE
                WHEN ROUND(DATEDIFF(ip.BUSINESS_DATE, ip.BIRTH_DATE)/365,0) BETWEEN 18 AND 24 THEN '18-24'
                WHEN ROUND(DATEDIFF(ip.BUSINESS_DATE, ip.BIRTH_DATE)/365,0) BETWEEN 25 AND 34 THEN '25-34'
                WHEN ROUND(DATEDIFF(ip.BUSINESS_DATE, ip.BIRTH_DATE)/365,0) BETWEEN 35 AND 44 THEN '35-44'
                WHEN ROUND(DATEDIFF(ip.BUSINESS_DATE, ip.BIRTH_DATE)/365,0) BETWEEN 45 AND 54 THEN '45-54'
                WHEN ROUND(DATEDIFF(ip.BUSINESS_DATE, ip.BIRTH_DATE)/365,0) BETWEEN 55 AND 64 THEN '55-64'
                WHEN ROUND(DATEDIFF(ip.BUSINESS_DATE, ip.BIRTH_DATE)/365,0) >= 65 THEN '65+'
                ELSE 'Other'
            END AS age_banding
        FROM dm_ib.digital_banking_master db
        INNER JOIN eil.m_involved_party_h ip
            ON db.rcif_customer_nbr = ip.rcif_cust_nbr
           AND ip.BUSINESS_DATE = '{BIZ}' AND ip.SOURCE_SYSTEM_CODE = 'CF'
           AND ip.DECEASED_DATE IS NULL
        WHERE db.ods_business_dt = '{BIZ}'
    )
    SELECT otp.otp_type, c.age_banding,
        COUNT(DISTINCT e.rcif_id) AS users, COUNT(*) AS events
    FROM otp
    INNER JOIN enr e ON otp.SESSION_ID = e.SESSION_ID
    INNER JOIN cust c ON e.rcif_id = c.rcif_customer_nbr
    GROUP BY otp.otp_type, c.age_banding
    ORDER BY otp.otp_type, c.age_banding
""").show(30, truncate=False)


print("=== Script 02 Complete ===")
spark.stop()

"""
V1 Script 02: TSMT Tables (CDSW)
==================================
Covers: #1 Channel, #2 ChannelSessionId, #3 Device/Platform,
        #4 Age Groups, #5-7 OTP by Age, #8 UID/PWD, #9 Auth Events,
        #10 Timestamps, #14 FI Enabled, #15 Aggregators

Table refs: sl1_tsmt.tsmt_*  (CDSW/Hive)
JSON parse: get_json_object on DATA column
  - ENRICHMENT_H DATA: RCIFId, channel, ChannelSessionId, connection, AggregatorRequest, UserAgent
  - SESSION_H DATA:    channel.channel_type (sms/email), device_public_key
  - DEVICE_H DATA:     hw_type, os_type, device_model, rsaEncryptionEnabled
  - AUTHENTICATION_H:  DATA is just tokens — use direct columns only
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("V1_02_TSMT") \
    .enableHiveSupport() \
    .getOrCreate()

# Narrow date range first — expand once confirmed working
START = "2025-03-01"
END   = "2025-03-07"
BIZ   = "2025-03-01"


# =============================================================
# REQ #1: Channel Type  +  REQ #2: ChannelSessionId
# REQ #15 (partial): AggregatorRequest
# REQ #8 (partial): connection type (UID/PWD detection)
# All from ENRICHMENT_H DATA JSON
# =============================================================
print("=" * 50)
print("REQ #1: Channel Type")
print("REQ #2: ChannelSessionId")
print("REQ #8: Connection/Auth Method")
print("REQ #15: Aggregator Flag")
print("=" * 50)

spark.sql(f"""
    SELECT
        ODS_BUSINESS_DT,
        get_json_object(DATA, '$.channel') AS channel,
        get_json_object(DATA, '$.connection') AS connection_type,
        get_json_object(DATA, '$.AggregatorRequest') AS aggregator_flag,
        COUNT(*) AS events,
        COUNT(DISTINCT SESSION_ID) AS sessions
    FROM sl1_tsmt.tsmt_enrichment_h
    WHERE ODS_BUSINESS_DT BETWEEN '{START}' AND '{END}'
      AND FAILURE = 0
      AND DATA IS NOT NULL
    GROUP BY
        ODS_BUSINESS_DT,
        get_json_object(DATA, '$.channel'),
        get_json_object(DATA, '$.connection'),
        get_json_object(DATA, '$.AggregatorRequest')
    ORDER BY ODS_BUSINESS_DT, events DESC
""").show(40, truncate=False)

# ChannelSessionId sample
print("REQ #2: ChannelSessionId (sample)")
spark.sql(f"""
    SELECT
        ODS_BUSINESS_DT,
        SESSION_ID,
        get_json_object(DATA, '$.channel') AS channel,
        get_json_object(DATA, '$.ChannelSessionId') AS channel_session_id
    FROM sl1_tsmt.tsmt_enrichment_h
    WHERE ODS_BUSINESS_DT = '{START}'
      AND FAILURE = 0
      AND DATA IS NOT NULL
      AND get_json_object(DATA, '$.ChannelSessionId') IS NOT NULL
    LIMIT 10
""").show(10, truncate=False)


# =============================================================
# REQ #3: Device & Platform
# From DEVICE_H — direct columns + DATA JSON
# =============================================================
print("=" * 50)
print("REQ #3: Device & Platform")
print("=" * 50)

spark.sql(f"""
    SELECT
        DEVICE_OS_TYPE,
        get_json_object(DATA, '$.hw_type') AS hw_type,
        get_json_object(DATA, '$.os_type') AS os_from_data,
        get_json_object(DATA, '$.device_model') AS device_model,
        COUNT(*) AS events,
        COUNT(DISTINCT SESSION_ID) AS sessions
    FROM sl1_tsmt.tsmt_device_h
    WHERE ODS_BUSINESS_DT BETWEEN '{START}' AND '{END}'
      AND FAILURE = 0
    GROUP BY
        DEVICE_OS_TYPE,
        get_json_object(DATA, '$.hw_type'),
        get_json_object(DATA, '$.os_type'),
        get_json_object(DATA, '$.device_model')
    ORDER BY sessions DESC
""").show(30, truncate=False)


# =============================================================
# REQ #4: Customer Age Groups (Digital Enrolled vs Mobile Active)
# Link: ENRICHMENT_H DATA → $.RCIFId → dm_ib.digital_banking_master
# =============================================================
print("=" * 50)
print("REQ #4: Customer Age Groups")
print("=" * 50)

spark.sql(f"""
    WITH enr_rcif AS (
        SELECT DISTINCT
            get_json_object(DATA, '$.RCIFId') AS rcif_id,
            get_json_object(DATA, '$.channel') AS channel,
            SESSION_ID
        FROM sl1_tsmt.tsmt_enrichment_h
        WHERE ODS_BUSINESS_DT BETWEEN '{START}' AND '{END}'
          AND FAILURE = 0
          AND get_json_object(DATA, '$.RCIFId') IS NOT NULL
    ),
    cust AS (
        SELECT
            db.rcif_customer_nbr,
            CASE
                WHEN ROUND(DATEDIFF(ip.BUSINESS_DATE, ip.BIRTH_DATE)/365, 0) BETWEEN 18 AND 24 THEN '18-24'
                WHEN ROUND(DATEDIFF(ip.BUSINESS_DATE, ip.BIRTH_DATE)/365, 0) BETWEEN 25 AND 34 THEN '25-34'
                WHEN ROUND(DATEDIFF(ip.BUSINESS_DATE, ip.BIRTH_DATE)/365, 0) BETWEEN 35 AND 44 THEN '35-44'
                WHEN ROUND(DATEDIFF(ip.BUSINESS_DATE, ip.BIRTH_DATE)/365, 0) BETWEEN 45 AND 54 THEN '45-54'
                WHEN ROUND(DATEDIFF(ip.BUSINESS_DATE, ip.BIRTH_DATE)/365, 0) BETWEEN 55 AND 64 THEN '55-64'
                WHEN ROUND(DATEDIFF(ip.BUSINESS_DATE, ip.BIRTH_DATE)/365, 0) >= 65 THEN '65+'
                ELSE 'Other'
            END AS age_banding
        FROM dm_ib.digital_banking_master db
        INNER JOIN eil.m_involved_party_h ip
            ON db.rcif_customer_nbr = ip.rcif_cust_nbr
           AND ip.BUSINESS_DATE = '{BIZ}'
           AND ip.SOURCE_SYSTEM_CODE = 'CF'
           AND ip.DECEASED_DATE IS NULL
        WHERE db.ods_business_dt = '{BIZ}'
    )
    SELECT
        c.age_banding,
        COUNT(DISTINCT e.rcif_id) AS unique_customers,
        COUNT(DISTINCT e.SESSION_ID) AS total_sessions,
        COUNT(DISTINCT CASE WHEN UPPER(e.channel) = 'MBANK' THEN e.SESSION_ID END) AS mobile_sessions,
        COUNT(DISTINCT CASE WHEN UPPER(e.channel) != 'MBANK' OR e.channel IS NULL THEN e.SESSION_ID END) AS non_mobile_sessions
    FROM enr_rcif e
    INNER JOIN cust c ON e.rcif_id = c.rcif_customer_nbr
    GROUP BY c.age_banding
    ORDER BY c.age_banding
""").show(20, truncate=False)


# =============================================================
# REQ #5-7: OTP by Type (SMS/Email) x Age Group
# SESSION_H DATA → $.channel.channel_type joined to ENRICHMENT_H → RCIFId
# =============================================================
print("=" * 50)
print("REQ #5-7: OTP by Type and Age Group")
print("=" * 50)

spark.sql(f"""
    WITH otp AS (
        SELECT SESSION_ID,
            get_json_object(DATA, '$.channel.channel_type') AS otp_type
        FROM sl1_tsmt.tsmt_session_h
        WHERE ODS_BUSINESS_DT BETWEEN '{START}' AND '{END}'
          AND FAILURE = 0
          AND get_json_object(DATA, '$.channel.channel_type') IS NOT NULL
    ),
    enr_rcif AS (
        SELECT DISTINCT SESSION_ID,
            get_json_object(DATA, '$.RCIFId') AS rcif_id
        FROM sl1_tsmt.tsmt_enrichment_h
        WHERE ODS_BUSINESS_DT BETWEEN '{START}' AND '{END}'
          AND FAILURE = 0
          AND get_json_object(DATA, '$.RCIFId') IS NOT NULL
    ),
    cust AS (
        SELECT
            db.rcif_customer_nbr,
            CASE
                WHEN ROUND(DATEDIFF(ip.BUSINESS_DATE, ip.BIRTH_DATE)/365, 0) BETWEEN 18 AND 24 THEN '18-24'
                WHEN ROUND(DATEDIFF(ip.BUSINESS_DATE, ip.BIRTH_DATE)/365, 0) BETWEEN 25 AND 34 THEN '25-34'
                WHEN ROUND(DATEDIFF(ip.BUSINESS_DATE, ip.BIRTH_DATE)/365, 0) BETWEEN 35 AND 44 THEN '35-44'
                WHEN ROUND(DATEDIFF(ip.BUSINESS_DATE, ip.BIRTH_DATE)/365, 0) BETWEEN 45 AND 54 THEN '45-54'
                WHEN ROUND(DATEDIFF(ip.BUSINESS_DATE, ip.BIRTH_DATE)/365, 0) BETWEEN 55 AND 64 THEN '55-64'
                WHEN ROUND(DATEDIFF(ip.BUSINESS_DATE, ip.BIRTH_DATE)/365, 0) >= 65 THEN '65+'
                ELSE 'Other'
            END AS age_banding
        FROM dm_ib.digital_banking_master db
        INNER JOIN eil.m_involved_party_h ip
            ON db.rcif_customer_nbr = ip.rcif_cust_nbr
           AND ip.BUSINESS_DATE = '{BIZ}'
           AND ip.SOURCE_SYSTEM_CODE = 'CF'
           AND ip.DECEASED_DATE IS NULL
        WHERE db.ods_business_dt = '{BIZ}'
    )
    SELECT
        otp.otp_type,
        c.age_banding,
        COUNT(DISTINCT e.rcif_id) AS unique_users,
        COUNT(*) AS otp_events
    FROM otp
    INNER JOIN enr_rcif e ON otp.SESSION_ID = e.SESSION_ID
    INNER JOIN cust c ON e.rcif_id = c.rcif_customer_nbr
    GROUP BY otp.otp_type, c.age_banding
    ORDER BY otp.otp_type, c.age_banding
""").show(30, truncate=False)


# =============================================================
# REQ #9: Auth Events — Login/Logout/Token Refresh/Pwd Reset
# AUTHENTICATION_H direct columns (no DATA parsing)
# =============================================================
print("=" * 50)
print("REQ #9: Auth Events by ACTION")
print("=" * 50)

spark.sql(f"""
    SELECT
        ODS_BUSINESS_DT,
        ACTION,
        COUNT(*) AS events,
        COUNT(DISTINCT SESSION_ID) AS sessions,
        COUNT(DISTINCT USER_ID) AS users
    FROM sl1_tsmt.tsmt_authentication_h
    WHERE ODS_BUSINESS_DT BETWEEN '{START}' AND '{END}'
      AND FAILURE = 0
    GROUP BY ODS_BUSINESS_DT, ACTION
    ORDER BY ODS_BUSINESS_DT, events DESC
""").show(40, truncate=False)


# =============================================================
# REQ #10: Timestamps (sample for PBI time-series)
# =============================================================
print("=" * 50)
print("REQ #10: Timestamps (sample)")
print("=" * 50)

spark.sql(f"""
    SELECT ODS_BUSINESS_DT, ODS_BUSINESS_TS, SESSION_ID,
           ACTION, AUTHENTICATION_METHOD, APPLICATION_ID
    FROM sl1_tsmt.tsmt_authentication_h
    WHERE ODS_BUSINESS_DT = '{START}'
      AND FAILURE = 0
    LIMIT 10
""").show(10, truncate=False)


# =============================================================
# REQ #14: FI Enabled vs Disabled
# DEVICE_H DATA → device_public_key, rsaEncryptionEnabled
# =============================================================
print("=" * 50)
print("REQ #14: FI Enabled vs Disabled")
print("=" * 50)

spark.sql(f"""
    SELECT
        get_json_object(DATA, '$.device_public_key') AS device_public_key,
        get_json_object(DATA, '$.rsaEncryptionEnabled') AS rsa_enabled,
        get_json_object(DATA, '$.generateKeyPairSuccess') AS keygen_success,
        COUNT(*) AS events,
        COUNT(DISTINCT SESSION_ID) AS sessions
    FROM sl1_tsmt.tsmt_device_h
    WHERE ODS_BUSINESS_DT BETWEEN '{START}' AND '{END}'
      AND FAILURE = 0
    GROUP BY
        get_json_object(DATA, '$.device_public_key'),
        get_json_object(DATA, '$.rsaEncryptionEnabled'),
        get_json_object(DATA, '$.generateKeyPairSuccess')
    ORDER BY sessions DESC
""").show(20, truncate=False)


print("=== Script 02 Complete ===")
spark.stop()

"""
V1 Enterprise Auth Dashboard — Script 02: TSMT Tables
=======================================================
Requirements covered:
  #1   Channel Type         — ENRICHMENT_H DATA → channel (e.g. "MBANK")
  #2   Channel Session ID   — ENRICHMENT_H DATA → ChannelSessionId
  #3   Device & Platform    — DEVICE_H cols + DATA (hw_type, os_type, device_model)
  #5-7 OTP by Age Group     — SESSION_H DATA → channel.channel_type (sms/email)
                               joined to customer dim via ENRICHMENT_H → RCIFId
  #8   UID/PWD (session)    — ENRICHMENT_H DATA → connection, SESSION_H → AUTHENTICATION_METHOD
  #9   Auth Events          — AUTHENTICATION_H → ACTION
  #10  Timestamps           — AUTHENTICATION_H / ENRICHMENT_H → ODS_BUSINESS_TS
  #14  FI Disabled/Enabled  — DEVICE_H DATA → rsaEncryptionEnabled, device_public_key
  #15  SSO / Aggregators    — ENRICHMENT_H DATA → AggregatorRequest

DATA COLUMN STRUCTURES (from actual samples):
  AUTHENTICATION_H DATA: {"token":"<hex>"}  — just a token, not useful
  SESSION_H DATA:        {"otp_provider_id":"...","channel":{"channel_type":"sms","target":"..."},"device_public_key":true}
  ENRICHMENT_H DATA:     {RCIFId, channel, ChannelSessionId, connection, AggregatorRequest, 
                           UserAgent, TMXResponse, fraud_score, loginCount, ...}  — GOLDMINE
  DEVICE_H DATA:         {hw_type, os_type, device_model, device_public_key, 
                           rsaEncryptionEnabled, generateKeyPairSuccess, ...}
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, get_json_object, when, upper, trim, count, countDistinct,
    sum as _sum, round as _round, coalesce, lit
)

spark = SparkSession.builder \
    .appName("V1_02_TSMT") \
    .enableHiveSupport() \
    .getOrCreate()

TSMT = "PRODUCTION.SL1_TSMT"
START_DATE = "2025-01-01"
END_DATE   = "2025-03-31"
BIZ_DATE   = "2025-03-01"  # for customer dimension snapshot


# =============================================================
# LOAD BASE TABLES
# =============================================================

# ENRICHMENT_H — our main table (has RCIFId, channel, connection, aggregator)
df_enr = spark.sql(f"""
    SELECT ODS_BUSINESS_DT, ODS_BUSINESS_TS, SESSION_ID, DEVICE_ID,
           POLICY_ID, APPLICATION_ID, SESSION_TYPE, USER_ID, DATA
    FROM {TSMT}.TSMT_ENRICHMENT_H
    WHERE ODS_BUSINESS_DT BETWEEN '{START_DATE}' AND '{END_DATE}'
      AND FAILURE = 0 AND DATA IS NOT NULL
""")

# Parse key fields from ENRICHMENT DATA JSON
df_enr = df_enr \
    .withColumn("rcif_id", get_json_object(col("DATA"), "$.RCIFId")) \
    .withColumn("channel", get_json_object(col("DATA"), "$.channel")) \
    .withColumn("channel_session_id", get_json_object(col("DATA"), "$.ChannelSessionId")) \
    .withColumn("connection_type", get_json_object(col("DATA"), "$.connection")) \
    .withColumn("aggregator_request", get_json_object(col("DATA"), "$.AggregatorRequest")) \
    .withColumn("user_agent", get_json_object(col("DATA"), "$.UserAgent")) \
    .withColumn("login_count", get_json_object(col("DATA"), "$.loginCount")) \
    .withColumn("is_social", get_json_object(col("DATA"), "$.isSocial")) \
    .withColumn("given_name", get_json_object(col("DATA"), "$.given_name")) \
    .withColumn("description", get_json_object(col("DATA"), "$.Description"))

df_enr.cache()
print(f"ENRICHMENT_H loaded: {df_enr.count()} rows")


# SESSION_H — has OTP channel info in DATA
df_sess = spark.sql(f"""
    SELECT ODS_BUSINESS_DT, SESSION_ID, AUTHENTICATION_METHOD,
           ACTION, ACTION2, ERROR_CODE, ERROR_MESSAGE, DATA
    FROM {TSMT}.TSMT_SESSION_H
    WHERE ODS_BUSINESS_DT BETWEEN '{START_DATE}' AND '{END_DATE}'
      AND FAILURE = 0 AND DATA IS NOT NULL
""")

# Parse OTP fields from SESSION DATA JSON
# Structure: {"otp_provider_id":"otp-internal-hotp","channel":{"channel_type":"sms",...},"device_public_key":true}
df_sess = df_sess \
    .withColumn("otp_channel_type", get_json_object(col("DATA"), "$.channel.channel_type")) \
    .withColumn("otp_target", get_json_object(col("DATA"), "$.channel.target")) \
    .withColumn("otp_provider_id", get_json_object(col("DATA"), "$.otp_provider_id")) \
    .withColumn("device_public_key", get_json_object(col("DATA"), "$.device_public_key")) \
    .withColumn("failures_count", get_json_object(col("DATA"), "$.failures_count"))

df_sess.cache()
print(f"SESSION_H loaded: {df_sess.count()} rows")


# AUTHENTICATION_H — direct columns only (DATA is just tokens)
df_auth = spark.sql(f"""
    SELECT ODS_BUSINESS_DT, ODS_BUSINESS_TS, SESSION_ID, DEVICE_ID,
           AUTHENTICATION_METHOD, ACTION, ACTION2, ACTION3,
           TOKEN_NAME, POLICY_ID, APPLICATION_ID, SESSION_TYPE, USER_ID
    FROM {TSMT}.TSMT_AUTHENTICATION_H
    WHERE ODS_BUSINESS_DT BETWEEN '{START_DATE}' AND '{END_DATE}'
      AND FAILURE = 0
""")
print(f"AUTHENTICATION_H loaded: {df_auth.count()} rows")


# DEVICE_H — device details in DATA
df_dev = spark.sql(f"""
    SELECT ODS_BUSINESS_DT, SESSION_ID, DEVICE_ID, DEVICE_OS_TYPE,
           DEVICE_MODEL, USER_ID, DATA
    FROM {TSMT}.TSMT_DEVICE_H
    WHERE ODS_BUSINESS_DT BETWEEN '{START_DATE}' AND '{END_DATE}'
      AND FAILURE = 0
""")

df_dev = df_dev \
    .withColumn("hw_type", get_json_object(col("DATA"), "$.hw_type")) \
    .withColumn("dev_os_type", get_json_object(col("DATA"), "$.os_type")) \
    .withColumn("dev_model", get_json_object(col("DATA"), "$.device_model")) \
    .withColumn("dev_public_key", get_json_object(col("DATA"), "$.device_public_key")) \
    .withColumn("rsa_enabled", get_json_object(col("DATA"), "$.rsaEncryptionEnabled")) \
    .withColumn("keygen_success", get_json_object(col("DATA"), "$.generateKeyPairSuccess"))

print(f"DEVICE_H loaded: {df_dev.count()} rows")


# CUSTOMER DIMENSION — dm_ib + EIL
df_cust = spark.sql(f"""
    SELECT
        db.rcif_customer_nbr AS RCIF_CUST_NBR,
        db.ibn,
        db.customer_type,
        db.enroll_date,
        db.active_relationship,
        db.30d_olb_login_count,
        db.30d_mob_login_count,
        ip.GENDER_TYPE_CODE,
        ip.BIRTH_DATE,
        ROUND(DATEDIFF(ip.BUSINESS_DATE, ip.BIRTH_DATE) / 365, 0) AS customer_age,
        CASE
            WHEN ROUND(DATEDIFF(ip.BUSINESS_DATE, ip.BIRTH_DATE)/365,0) <= 17 THEN 'Under 18'
            WHEN ROUND(DATEDIFF(ip.BUSINESS_DATE, ip.BIRTH_DATE)/365,0) BETWEEN 18 AND 24 THEN '18-24'
            WHEN ROUND(DATEDIFF(ip.BUSINESS_DATE, ip.BIRTH_DATE)/365,0) BETWEEN 25 AND 34 THEN '25-34'
            WHEN ROUND(DATEDIFF(ip.BUSINESS_DATE, ip.BIRTH_DATE)/365,0) BETWEEN 35 AND 44 THEN '35-44'
            WHEN ROUND(DATEDIFF(ip.BUSINESS_DATE, ip.BIRTH_DATE)/365,0) BETWEEN 45 AND 54 THEN '45-54'
            WHEN ROUND(DATEDIFF(ip.BUSINESS_DATE, ip.BIRTH_DATE)/365,0) BETWEEN 55 AND 64 THEN '55-64'
            WHEN ROUND(DATEDIFF(ip.BUSINESS_DATE, ip.BIRTH_DATE)/365,0) >= 65 THEN '65+'
            ELSE 'Unknown'
        END AS age_banding,
        CASE
            WHEN ip.BIRTH_DATE BETWEEN '1946-01-01' AND '1964-12-31' THEN 'Baby Boomer'
            WHEN ip.BIRTH_DATE BETWEEN '1965-01-01' AND '1980-12-31' THEN 'Gen X'
            WHEN ip.BIRTH_DATE BETWEEN '1981-01-01' AND '1996-12-31' THEN 'Millennial'
            WHEN ip.BIRTH_DATE >= '1997-01-01' THEN 'Gen Z'
            ELSE 'Other'
        END AS generation
    FROM dm_ib.digital_banking_master db
    INNER JOIN EIL.M_INVOLVED_PARTY_H ip
        ON db.rcif_customer_nbr = ip.rcif_cust_nbr
        AND ip.BUSINESS_DATE = '{BIZ_DATE}'
        AND ip.SOURCE_SYSTEM_CODE = 'CF'
        AND ip.DECEASED_DATE IS NULL
    WHERE db.ods_business_dt = '{BIZ_DATE}'
""")
df_cust.cache()
print(f"Customer dimension: {df_cust.count()} rows")


# =============================================================
# REQ #1: Channel Type
# =============================================================
print("\n" + "=" * 50)
print("REQ #1: Channel Type")

df_enr.groupBy("ODS_BUSINESS_DT", "channel") \
    .agg(count("*").alias("events"), countDistinct("SESSION_ID").alias("sessions")) \
    .orderBy("ODS_BUSINESS_DT", "channel") \
    .show(30, truncate=False)


# =============================================================
# REQ #2: Channel Session ID (sample)
# =============================================================
print("=" * 50)
print("REQ #2: Channel Session ID (sample)")

df_enr.select("ODS_BUSINESS_DT", "SESSION_ID", "channel", "channel_session_id") \
    .filter(col("channel_session_id").isNotNull()) \
    .show(10, truncate=False)


# =============================================================
# REQ #3: Device & Platform
# =============================================================
print("=" * 50)
print("REQ #3: Device & Platform")

# From DEVICE_H direct columns + parsed DATA
df_dev.groupBy("DEVICE_OS_TYPE", "hw_type", "dev_model") \
    .agg(count("*").alias("events"), countDistinct("SESSION_ID").alias("sessions")) \
    .orderBy(col("sessions").desc()) \
    .show(30, truncate=False)


# =============================================================
# REQ #4: Customer Age Groups — Digital Enrolled vs Mobile Active
# Link: ENRICHMENT_H → RCIFId → dm_ib.digital_banking_master
# =============================================================
print("=" * 50)
print("REQ #4: Customer Age Groups")

# Get distinct RCIFs from enrichment (these are active auth users)
enr_rcif = df_enr.select(
    col("rcif_id").alias("RCIF_CUST_NBR"),
    col("SESSION_ID"),
    col("channel")
).filter(col("RCIF_CUST_NBR").isNotNull()).distinct()

# Join to customer dimension
enr_with_cust = enr_rcif.join(df_cust, on="RCIF_CUST_NBR", how="inner")

# Age group summary
enr_with_cust.groupBy("age_banding", "generation") \
    .agg(
        countDistinct("RCIF_CUST_NBR").alias("unique_customers"),
        countDistinct("SESSION_ID").alias("total_sessions"),
        _sum(when(upper(col("channel")).isin("MBANK"), 1).otherwise(0)).alias("mobile_sessions")
    ).orderBy("age_banding") \
    .show(20, truncate=False)


# =============================================================
# REQ #5-7: OTP by Type (SMS/Email/Voice) × Age Group
# Link: SESSION_H (otp_channel_type) → ENRICHMENT_H (RCIFId) via SESSION_ID
# =============================================================
print("=" * 50)
print("REQ #5-7: OTP by Type and Age Group")

# Get OTP events from SESSION_H
otp_events = df_sess.filter(col("otp_channel_type").isNotNull()) \
    .select("SESSION_ID", "otp_channel_type", "otp_target", "device_public_key")

# Join to enrichment to get RCIFId
otp_with_rcif = otp_events.join(
    df_enr.select("SESSION_ID", "rcif_id").filter(col("rcif_id").isNotNull()).distinct(),
    on="SESSION_ID", how="inner"
).withColumnRenamed("rcif_id", "RCIF_CUST_NBR")

# Join to customer dim for age
otp_with_age = otp_with_rcif.join(df_cust, on="RCIF_CUST_NBR", how="inner")

otp_with_age.groupBy("otp_channel_type", "age_banding") \
    .agg(
        countDistinct("RCIF_CUST_NBR").alias("unique_users"),
        count("*").alias("otp_events")
    ).orderBy("otp_channel_type", "age_banding") \
    .show(30, truncate=False)


# =============================================================
# REQ #8: UID/PWD Auth (from TSMT)
# ENRICHMENT_H DATA → connection = "Username-Password-Authentication"
# =============================================================
print("=" * 50)
print("REQ #8: Auth Method Breakdown (from ENRICHMENT_H)")

df_enr.withColumn(
    "auth_category",
    when(upper(col("connection_type")).contains("PASSWORD"), "UID/PWD")
    .when(upper(col("connection_type")).contains("BIOMETRIC"), "Biometric")
    .when(upper(col("connection_type")).contains("PASSKEY"), "Passkey")
    .otherwise(coalesce(col("connection_type"), lit("Unknown")))
).groupBy("ODS_BUSINESS_DT", "auth_category") \
 .agg(count("*").alias("events"), countDistinct("SESSION_ID").alias("sessions")) \
 .orderBy("ODS_BUSINESS_DT", "auth_category") \
 .show(30, truncate=False)

# Also from SESSION_H AUTHENTICATION_METHOD
print("REQ #8: Auth Method (from SESSION_H.AUTHENTICATION_METHOD)")
df_sess.filter(col("AUTHENTICATION_METHOD").isNotNull()) \
    .groupBy("ODS_BUSINESS_DT", "AUTHENTICATION_METHOD") \
    .agg(count("*").alias("events")) \
    .orderBy("ODS_BUSINESS_DT", col("events").desc()) \
    .show(30, truncate=False)


# =============================================================
# REQ #9: Auth Events — Login/Logout/Token Refresh/Pwd Reset
# =============================================================
print("=" * 50)
print("REQ #9: Auth Events by ACTION")

df_auth.groupBy("ODS_BUSINESS_DT", "ACTION") \
    .agg(
        count("*").alias("events"),
        countDistinct("SESSION_ID").alias("sessions"),
        countDistinct("USER_ID").alias("users")
    ).orderBy("ODS_BUSINESS_DT", col("events").desc()) \
    .show(40, truncate=False)


# =============================================================
# REQ #10: Timestamps (sample for PBI time-series)
# =============================================================
print("=" * 50)
print("REQ #10: Event Timestamps (sample)")

df_auth.select("ODS_BUSINESS_DT", "ODS_BUSINESS_TS", "SESSION_ID", "ACTION",
               "AUTHENTICATION_METHOD", "APPLICATION_ID") \
    .show(10, truncate=False)


# =============================================================
# REQ #14: FI Disabled vs Enabled
# DEVICE_H DATA → device_public_key, rsaEncryptionEnabled
# =============================================================
print("=" * 50)
print("REQ #14: FI Enabled vs Disabled (device_public_key / rsa)")

df_dev.groupBy("dev_public_key", "rsa_enabled", "keygen_success") \
    .agg(count("*").alias("events"), countDistinct("SESSION_ID").alias("sessions")) \
    .orderBy(col("sessions").desc()) \
    .show(20, truncate=False)


# =============================================================
# REQ #15: Aggregator Detection (from ENRICHMENT_H)
# =============================================================
print("=" * 50)
print("REQ #15: Aggregator Requests")

df_enr.groupBy("ODS_BUSINESS_DT", "aggregator_request") \
    .agg(count("*").alias("events"), countDistinct("SESSION_ID").alias("sessions")) \
    .orderBy("ODS_BUSINESS_DT") \
    .show(20, truncate=False)


print("=== Script 02 Complete ===")
spark.stop()

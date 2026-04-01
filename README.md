"""
=================================================================
V1 Enterprise Auth Dashboard — CDSW Run Guide
=================================================================

ALL DATA LAKE / HIVE — NO SNOWFLAKE NEEDED.
Broken into small scripts so CDSW doesn't crash.

RUN ORDER (one at a time, let each finish):
-------------------------------------------
1.  python step0_customer_dim.py       <-- builds customer lookup
2.  python step1_channel_device.py     <-- Reqs #1-3
3.  python step2_otp_age_groups.py     <-- Reqs #4-7
4.  python step3_auth_metrics.py       <-- Reqs #8-10
5.  python step4_passkey.py            <-- Reqs #11-13
6.  python step5_fi_sso_mfa.py         <-- Reqs #14-15

BEFORE RUNNING:
- Open v1_config.py
- Update TSMT_DB if your Hive database name is different
  (currently set to "SL1_TSMT")
- Update BUSINESS_DATE, START_DATE, END_DATE
- Update OUTPUT_DB if you want a different Hive output database

OUTPUT TABLES CREATED (in dm_auth_dashboard):
---------------------------------------------
v1_customer_dim              — customer lookup
v1_channel_device            — channel type, code, device, platform
v1_otp_by_age_group          — OTP events by channel x age group
v1_otp_stats_summary         — OTP totals from DM_CIAMOP
v1_auth_metrics              — auth method %, success/fail by age
v1_auth_events               — logout, token refresh, pwd reset
v1_passkey_metrics           — passkey + passwordless daily trend
v1_passkey_enrollment_vs_login — enrollment vs login trend
v1_fi_status                 — FI disabled vs enabled by age/channel
v1_aggregator_trend          — aggregator login daily trend
v1_mfa_breakdown             — MFA type, policy, review status
v1_sso_events                — SSO events by channel

All 12 tables feed directly into PBI.
=================================================================
"""


over

"""
=================================================================
V1 Auth Dashboard — Config & Helpers (Data Lake / Hive ONLY)
=================================================================
All tables read from Hive. No Snowflake connector needed.
Import this in each individual requirement script.
=================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, when, lit, count, countDistinct,
    sum as _sum, round as _round,
    to_date, get_json_object, trim, coalesce, datediff
)

# -----------------------------------------------------------------
# DATA LAKE / HIVE TABLE REFERENCES  (same schemas as Snowflake)
# -----------------------------------------------------------------

# TSMT tables — now reading from Hive data lake, NOT Snowflake
TSMT_DB = "SL1_TSMT"

TSMT_AUTHENTICATION_H  = f"{TSMT_DB}.TSMT_AUTHENTICATION_H"
TSMT_DEVICE_H          = f"{TSMT_DB}.TSMT_DEVICE_H"
TSMT_ENRICHMENT_H      = f"{TSMT_DB}.TSMT_ENRICHMENT_H"
TSMT_SESSION_H         = f"{TSMT_DB}.TSMT_SESSION_H"
TSMT_PINDROP_H         = f"{TSMT_DB}.TSMT_PINDROP_H"
TSMT_TMX_H             = f"{TSMT_DB}.TSMT_TMX_H"

# DM_CIAMOP tables (already Hive)
DM = "DM_CIAMOP"

LOGIN_AND_PASSKEY_STATS  = f"{DM}.login_and_passkey_stats"
MFA_APP                  = f"{DM}.mfa_app"
OTP_STATS_LOOP           = f"{DM}.otp_stats_loop"
PASSKEY_ENROLLED_USERS   = f"{DM}.passkey_enrolled_users"
PASSKEY_ENROLLMENT_LOGIN = f"{DM}.passkey_enrollment_login"
TMX_TSMT_REVIEW_OTP      = f"{DM}.tmx_tsmt_review_otp"

# EIL customer tables (Hive)
EIL_INVOLVED_PARTY       = "EIL.M_INVOLVED_PARTY_H"
EIL_DIGITAL_BANKING      = "EIL.DIGITAL_BANKING_MASTER_H"
EIL_ADDRESS              = "EIL.D_INVOLVED_PARTY_ADDRESS_H"
EIL_ARRANGEMENT          = "EIL.M_ARRANGEMENT_H"

# -----------------------------------------------------------------
# DATE PARAMETERS — change these before running
# -----------------------------------------------------------------
BUSINESS_DATE = "2025-03-01"
START_DATE    = "2025-01-01"
END_DATE      = "2025-03-31"

# -----------------------------------------------------------------
# OUTPUT — Hive database for PBI
# -----------------------------------------------------------------
OUTPUT_DB = "dm_auth_dashboard"


# -----------------------------------------------------------------
# HELPER: get or create a lean Spark session
# -----------------------------------------------------------------
def get_spark():
    return SparkSession.builder \
        .appName("V1_Auth_Dashboard") \
        .config("spark.sql.shuffle.partitions", "50") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .enableHiveSupport() \
        .getOrCreate()


# -----------------------------------------------------------------
# HELPER: parse JSON fields from the DATA column
# -----------------------------------------------------------------
def parse_json_fields(df, field_map):
    """
    field_map = {"alias": "$.json.path", ...}
    Returns df with new columns extracted from DATA.
    """
    for alias, path in field_map.items():
        df = df.withColumn(alias, get_json_object(col("DATA"), path))
    return df


# -----------------------------------------------------------------
# HELPER: save a small DataFrame to Hive
# -----------------------------------------------------------------
def save_to_hive(df, table_name):
    full = f"{OUTPUT_DB}.{table_name}"
    print(f"  -> Saving to {full} ...")
    df.write.mode("overwrite").saveAsTable(full)
    print(f"  -> Done. {df.count()} rows written.")

over

"""
=================================================================
STEP 0 — Build Customer Dimension  (run this FIRST)
=================================================================
Creates a small customer lookup table in Hive that all other
scripts join to. Lightweight — only pulls the columns we need.
=================================================================
"""

from v1_config import *

spark = get_spark()

print("Building customer dimension...")

cust_sql = f"""
SELECT DISTINCT
    IP.RCIF_CUST_NBR,
    DB.IBN_CUST_INTERNET_BANKING_NBR,

    CASE WHEN DB.IBN_CUST_INTERNET_BANKING_NBR IS NOT NULL
         THEN 'Digital Customer' ELSE 'Non-Digital Customer'
    END AS DIGITAL_CUSTOMER_CHECK,

    ROUND(DATEDIFF(IP.BUSINESS_DATE, IP.BIRTH_DATE) / 365, 0) AS CUSTOMER_AGE,

    CASE
        WHEN ROUND(DATEDIFF(IP.BUSINESS_DATE, IP.BIRTH_DATE)/365,0) <= 17  THEN 'Less than 18'
        WHEN ROUND(DATEDIFF(IP.BUSINESS_DATE, IP.BIRTH_DATE)/365,0) <= 24  THEN '18-24'
        WHEN ROUND(DATEDIFF(IP.BUSINESS_DATE, IP.BIRTH_DATE)/365,0) <= 34  THEN '25-34'
        WHEN ROUND(DATEDIFF(IP.BUSINESS_DATE, IP.BIRTH_DATE)/365,0) <= 44  THEN '35-44'
        WHEN ROUND(DATEDIFF(IP.BUSINESS_DATE, IP.BIRTH_DATE)/365,0) <= 54  THEN '45-54'
        WHEN ROUND(DATEDIFF(IP.BUSINESS_DATE, IP.BIRTH_DATE)/365,0) <= 64  THEN '55-64'
        WHEN ROUND(DATEDIFF(IP.BUSINESS_DATE, IP.BIRTH_DATE)/365,0) >= 65  THEN '65+'
        ELSE 'Unknown'
    END AS AGE_BANDING,

    CASE
        WHEN IP.BIRTH_DATE BETWEEN '1946-01-01' AND '1964-12-31' THEN 'Baby Boomer'
        WHEN IP.BIRTH_DATE BETWEEN '1965-01-01' AND '1980-12-31' THEN 'Gen X'
        WHEN IP.BIRTH_DATE BETWEEN '1981-01-01' AND '1996-12-31' THEN 'Millennial'
        WHEN IP.BIRTH_DATE >= '1997-01-01'                       THEN 'Centennial'
        ELSE 'Other/Unknown'
    END AS CUSTOMER_GENERATION,

    IP.GENDER_CODE AS CUSTOMER_GENDER,

    CASE WHEN AD.CITY_NAME LIKE '%HOLD STATEMENT%' THEN 'UNKNOWN'
         ELSE AD.CITY_NAME END AS CITY,
    CASE WHEN AD.STATE_NAME LIKE '%N/A%' THEN 'UNKNOWN'
         ELSE AD.STATE_NAME END AS STATE

FROM {EIL_INVOLVED_PARTY} IP
INNER JOIN {EIL_ADDRESS} AD
    ON IP.SOURCE_SYSTEM_CODE = AD.SOURCE_SYSTEM_CODE
   AND IP.INVOLVED_PARTY_ID  = AD.INVOLVED_PARTY_ID
LEFT JOIN {EIL_DIGITAL_BANKING} DB
    ON IP.RCIF_CUST_NBR = DB.RCIF_CUST_NBR

WHERE IP.BUSINESS_DATE = '{BUSINESS_DATE}'
  AND IP.RCIF_CUST_NBR IS NOT NULL
"""

cust_df = spark.sql(cust_sql)
save_to_hive(cust_df, "v1_customer_dim")

print("Customer dimension ready.")


over

"""
=================================================================
STEP 1 — Reqs #1, #2, #3:  Channel Type, Channel Code, Device
=================================================================
Reads: TSMT_AUTHENTICATION_H + TSMT_DEVICE_H  (data lake)
Joins: customer dimension
Output: v1_channel_device
=================================================================
"""

from v1_config import *

spark = get_spark()

print("=== Reqs #1-3: Channel, Code, Device ===")

# --- Only select the columns we need (keeps memory low) ---
auth_sql = f"""
SELECT
    get_json_object(DATA, '$.RCIF_CUST_NBR')      AS rcif,
    get_json_object(DATA, '$.channel')             AS channel_type,
    get_json_object(DATA, '$.channelIndicator')    AS channel_code,
    get_json_object(DATA, '$.channelSessionId')    AS channel_session_id,
    get_json_object(DATA, '$.ACTION')              AS action,
    KAFKA_PROCESS_DT
FROM {TSMT_AUTHENTICATION_H}
WHERE KAFKA_PROCESS_DT BETWEEN '{START_DATE}' AND '{END_DATE}'
"""

dev_sql = f"""
SELECT
    get_json_object(DATA, '$.RCIF_CUST_NBR')      AS rcif,
    get_json_object(DATA, '$.deviceType')          AS device_type,
    get_json_object(DATA, '$.platform')            AS platform,
    get_json_object(DATA, '$.osType')              AS os_type,
    get_json_object(DATA, '$.browserType')         AS browser_type,
    KAFKA_PROCESS_DT
FROM {TSMT_DEVICE_H}
WHERE KAFKA_PROCESS_DT BETWEEN '{START_DATE}' AND '{END_DATE}'
"""

auth_df = spark.sql(auth_sql)
dev_df  = spark.sql(dev_sql)

# Join auth + device on rcif + date
combined = auth_df.alias("a").join(
    dev_df.alias("d"),
    (col("a.rcif") == col("d.rcif")) &
    (col("a.KAFKA_PROCESS_DT") == col("d.KAFKA_PROCESS_DT")),
    "left"
)

# Join customer dim
cust = spark.table(f"{OUTPUT_DB}.v1_customer_dim")

result = combined.join(cust, col("a.rcif") == cust.RCIF_CUST_NBR, "left") \
    .select(
        col("a.rcif").alias("RCIF_CUST_NBR"),
        "channel_type", "channel_code", "channel_session_id",
        "device_type", "platform", "os_type", "browser_type",
        "action",
        col("a.KAFKA_PROCESS_DT").alias("event_date"),
        "AGE_BANDING", "CUSTOMER_GENERATION",
        "DIGITAL_CUSTOMER_CHECK"
    )

save_to_hive(result, "v1_channel_device")

# Free memory
auth_df.unpersist()
dev_df.unpersist()

print("=== Reqs #1-3 complete ===")

over

"""
=================================================================
STEP 2 — Reqs #4-7:  Age Groups + OTP Breakdowns
=================================================================
Reads: TSMT_ENRICHMENT_H (data lake) + DM_CIAMOP tables
Joins: customer dimension
Output: v1_otp_by_age_group, v1_otp_stats_summary
=================================================================
"""

from v1_config import *

spark = get_spark()

print("=== Reqs #4-7: Age Groups + OTP ===")

# ---------------------------------------------------------------
# APPROACH A: Use tmx_tsmt_review_otp (cleaner, pre-aggregated)
# ---------------------------------------------------------------
print("  Loading tmx_tsmt_review_otp...")
try:
    otp_review = spark.table(TMX_TSMT_REVIEW_OTP)

    otp_summary = otp_review.groupBy("otp_channel", "otp_type", "session_type") \
        .agg(
            _sum("otps_sent").alias("total_sent"),
            _sum("otp_success").alias("total_success"),
            _sum("otps_failed").alias("total_failed"),
            _sum("otp_abandoned").alias("total_abandoned")
        )

    save_to_hive(otp_summary, "v1_otp_stats_summary")
    print("  tmx_tsmt_review_otp loaded OK.")

except Exception as e:
    print(f"  tmx_tsmt_review_otp not available ({e}), using otp_stats_loop...")

    otp_loop = spark.table(OTP_STATS_LOOP)

    otp_summary = otp_loop.groupBy("policy_id", "application_id") \
        .agg(
            _sum("emailcount").alias("total_email_otp"),
            _sum("smscount").alias("total_sms_otp")
        )

    save_to_hive(otp_summary, "v1_otp_stats_summary")


# ---------------------------------------------------------------
# APPROACH B: TSMT_ENRICHMENT_H for OTP by customer age group
# ---------------------------------------------------------------
print("  Loading enrichment for OTP x age group...")

enrich_sql = f"""
SELECT
    get_json_object(DATA, '$.RCIF_CUST_NBR')  AS rcif,
    get_json_object(DATA, '$.OTPCHANNEL')      AS otp_channel,
    get_json_object(DATA, '$.otpType')         AS otp_type,
    get_json_object(DATA, '$.channel')         AS channel,
    get_json_object(DATA, '$.appName')         AS app_name,
    KAFKA_PROCESS_DT
FROM {TSMT_ENRICHMENT_H}
WHERE KAFKA_PROCESS_DT BETWEEN '{START_DATE}' AND '{END_DATE}'
  AND get_json_object(DATA, '$.OTPCHANNEL') IS NOT NULL
"""

enrich_df = spark.sql(enrich_sql)

cust = spark.table(f"{OUTPUT_DB}.v1_customer_dim")

otp_by_age = enrich_df.join(cust, enrich_df.rcif == cust.RCIF_CUST_NBR, "inner") \
    .groupBy("otp_channel", "AGE_BANDING", "CUSTOMER_GENERATION", "channel") \
    .agg(
        count("*").alias("otp_events"),
        countDistinct("rcif").alias("unique_users")
    )

save_to_hive(otp_by_age, "v1_otp_by_age_group")

enrich_df.unpersist()

print("=== Reqs #4-7 complete ===")

over
"""
=================================================================
STEP 3 — Reqs #8, #9, #10:  Auth Metrics, Events, Timestamps
=================================================================
Reads: TSMT_AUTHENTICATION_H + TSMT_SESSION_H  (data lake)
Joins: customer dimension
Output: v1_auth_metrics, v1_auth_events
=================================================================
"""

from v1_config import *

spark = get_spark()

print("=== Reqs #8-10: Auth Metrics, Events, Timestamps ===")

# ---------------------------------------------------------------
# Req #8: UID/PWD authentication percentage
# ---------------------------------------------------------------
print("  Req #8: UID/PWD %...")

session_sql = f"""
SELECT
    get_json_object(DATA, '$.RCIF_CUST_NBR')    AS rcif,
    get_json_object(DATA, '$.authMethod')        AS auth_method,
    get_json_object(DATA, '$.errorCode')         AS error_code,
    get_json_object(DATA, '$.errorMessage')      AS error_message,
    KAFKA_PROCESS_DT AS event_date
FROM {TSMT_SESSION_H}
WHERE KAFKA_PROCESS_DT BETWEEN '{START_DATE}' AND '{END_DATE}'
"""

sess_df = spark.sql(session_sql)

cust = spark.table(f"{OUTPUT_DB}.v1_customer_dim")

auth_metrics = sess_df.join(cust, sess_df.rcif == cust.RCIF_CUST_NBR, "left") \
    .groupBy("auth_method", "AGE_BANDING", "DIGITAL_CUSTOMER_CHECK") \
    .agg(
        count("*").alias("total_events"),
        countDistinct("rcif").alias("unique_users"),
        _sum(when(col("error_code").isNull(), 1).otherwise(0)).alias("success_count"),
        _sum(when(col("error_code").isNotNull(), 1).otherwise(0)).alias("failure_count")
    )

save_to_hive(auth_metrics, "v1_auth_metrics")

sess_df.unpersist()

# ---------------------------------------------------------------
# Req #9 & #10: Auth events (logout, token refresh, pwd reset)
# ---------------------------------------------------------------
print("  Req #9-10: Auth events + timestamps...")

events_sql = f"""
SELECT
    get_json_object(DATA, '$.RCIF_CUST_NBR')    AS rcif,
    get_json_object(DATA, '$.ACTION')            AS action,
    get_json_object(DATA, '$.channel')           AS channel,
    get_json_object(DATA, '$.eventTimestamp')     AS event_timestamp,
    KAFKA_PROCESS_DT AS event_date
FROM {TSMT_AUTHENTICATION_H}
WHERE KAFKA_PROCESS_DT BETWEEN '{START_DATE}' AND '{END_DATE}'
  AND (
      upper(get_json_object(DATA, '$.ACTION')) LIKE '%LOGOUT%'
   OR upper(get_json_object(DATA, '$.ACTION')) LIKE '%TOKEN%'
   OR upper(get_json_object(DATA, '$.ACTION')) LIKE '%REFRESH%'
   OR upper(get_json_object(DATA, '$.ACTION')) LIKE '%RESET%'
   OR upper(get_json_object(DATA, '$.ACTION')) LIKE '%PASSWORD%'
  )
"""

events_df = spark.sql(events_sql)

auth_events = events_df.join(cust, events_df.rcif == cust.RCIF_CUST_NBR, "left") \
    .groupBy("action", "channel", "event_date", "AGE_BANDING") \
    .agg(
        count("*").alias("event_count"),
        countDistinct("rcif").alias("unique_users")
    )

save_to_hive(auth_events, "v1_auth_events")

events_df.unpersist()

print("=== Reqs #8-10 complete ===")

over

"""
=================================================================
STEP 4 — Reqs #11, #12, #13:  Passkey & Passwordless
=================================================================
Reads: DM_CIAMOP tables (already Hive)
Output: v1_passkey_metrics
=================================================================
"""

from v1_config import *

spark = get_spark()

print("=== Reqs #11-13: Passkey & Passwordless ===")

# ---------------------------------------------------------------
# Req #11: Passkey enrollments
# ---------------------------------------------------------------
print("  Loading passkey_enrolled_users...")
enrolled = spark.table(PASSKEY_ENROLLED_USERS)
enrollment_count = enrolled.select(
    countDistinct("user_id").alias("total_passkey_enrolled")
).collect()[0][0]

print(f"  Total passkey enrolled users: {enrollment_count}")

# ---------------------------------------------------------------
# Req #12 & #13: Passkey logins + passwordless %
# ---------------------------------------------------------------
print("  Loading login_and_passkey_stats...")
lps = spark.table(LOGIN_AND_PASSKEY_STATS)

passkey_stats = lps.agg(
    _sum("passkey_logins").alias("total_passkey_logins"),
    _sum("total_person_logins").alias("total_person_logins"),
    _sum("mbank_biometric_logins").alias("total_biometric_logins"),
    _sum("onepass_logins").alias("total_onepass_logins"),
    _sum("mbank_creds_logins").alias("total_creds_logins"),
    _sum("total_aggregator_logins").alias("total_aggregator_logins")
)

# Also get the daily trend for PBI time-series
daily_trend = lps.groupBy("kafka_process_dt") \
    .agg(
        _sum("passkey_logins").alias("passkey_logins"),
        _sum("total_person_logins").alias("person_logins"),
        _sum("mbank_biometric_logins").alias("biometric_logins"),
        _sum("onepass_logins").alias("onepass_logins"),
        _sum("mbank_creds_logins").alias("creds_logins"),
        _sum("total_aggregator_logins").alias("aggregator_logins")
    ) \
    .withColumn("passwordless_logins",
        col("passkey_logins") + col("biometric_logins") + col("onepass_logins")
    ) \
    .withColumn("passwordless_pct",
        _round(
            col("passwordless_logins") /
            (col("person_logins") + lit(0.0001)) * 100, 2
        )
    )

save_to_hive(daily_trend, "v1_passkey_metrics")

# ---------------------------------------------------------------
# Also pull passkey_enrollment_login for enrollment vs login trend
# ---------------------------------------------------------------
print("  Loading passkey_enrollment_login...")
try:
    pel = spark.table(PASSKEY_ENROLLMENT_LOGIN)
    save_to_hive(pel, "v1_passkey_enrollment_vs_login")
except Exception as e:
    print(f"  passkey_enrollment_login skipped: {e}")

print("=== Reqs #11-13 complete ===")

over

"""
=================================================================
STEP 5 — Reqs #14, #15:  FI Status + SSO / Aggregators / MFA
=================================================================
Reads: TSMT_AUTHENTICATION_H (data lake) + DM_CIAMOP.mfa_app
Output: v1_fi_status, v1_sso_aggregator_mfa
=================================================================
"""

from v1_config import *

spark = get_spark()

print("=== Reqs #14-15: FI Status, SSO, Aggregators, MFA ===")

# ---------------------------------------------------------------
# Req #14: FI Disabled vs Enabled
# ---------------------------------------------------------------
print("  Req #14: FI status...")

fi_sql = f"""
SELECT
    get_json_object(DATA, '$.RCIF_CUST_NBR')  AS rcif,
    get_json_object(DATA, '$.FI_STATUS')       AS fi_status,
    get_json_object(DATA, '$.channel')         AS channel,
    KAFKA_PROCESS_DT AS event_date
FROM {TSMT_AUTHENTICATION_H}
WHERE KAFKA_PROCESS_DT BETWEEN '{START_DATE}' AND '{END_DATE}'
  AND get_json_object(DATA, '$.FI_STATUS') IS NOT NULL
"""

fi_df = spark.sql(fi_sql)

cust = spark.table(f"{OUTPUT_DB}.v1_customer_dim")

fi_result = fi_df.join(cust, fi_df.rcif == cust.RCIF_CUST_NBR, "left") \
    .groupBy("fi_status", "channel", "AGE_BANDING") \
    .agg(
        countDistinct("rcif").alias("unique_users"),
        count("*").alias("total_events")
    )

save_to_hive(fi_result, "v1_fi_status")
fi_df.unpersist()

# ---------------------------------------------------------------
# Req #15: SSO count, Aggregator count, MFA type
# ---------------------------------------------------------------
print("  Req #15: SSO / Aggregators / MFA...")

# 15a: Aggregator logins from login_and_passkey_stats
lps = spark.table(LOGIN_AND_PASSKEY_STATS)
agg_trend = lps.groupBy("kafka_process_dt") \
    .agg(_sum("total_aggregator_logins").alias("aggregator_logins"))

save_to_hive(agg_trend, "v1_aggregator_trend")

# 15b: MFA breakdown from mfa_app
print("  Loading mfa_app...")
try:
    mfa = spark.table(MFA_APP)

    mfa_summary = mfa.groupBy("mfa_app", "policy", "review_status") \
        .agg(_sum("events").alias("total_events"))

    save_to_hive(mfa_summary, "v1_mfa_breakdown")

except Exception as e:
    print(f"  mfa_app skipped: {e}")

# 15c: SSO — derive from auth action containing SSO keywords
print("  Deriving SSO from authentication actions...")

sso_sql = f"""
SELECT
    get_json_object(DATA, '$.RCIF_CUST_NBR')  AS rcif,
    get_json_object(DATA, '$.ACTION')          AS action,
    get_json_object(DATA, '$.channel')         AS channel,
    KAFKA_PROCESS_DT AS event_date
FROM {TSMT_AUTHENTICATION_H}
WHERE KAFKA_PROCESS_DT BETWEEN '{START_DATE}' AND '{END_DATE}'
  AND upper(get_json_object(DATA, '$.ACTION')) LIKE '%SSO%'
"""

sso_df = spark.sql(sso_sql)

sso_result = sso_df.groupBy("action", "channel", "event_date") \
    .agg(
        count("*").alias("sso_events"),
        countDistinct("rcif").alias("unique_users")
    )

save_to_hive(sso_result, "v1_sso_events")
sso_df.unpersist()

print("=== Reqs #14-15 complete ===")




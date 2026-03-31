"""
V1 Enterprise Auth Dashboard — CDSW PySpark (FINAL)
=====================================================
This script runs in CDSW and covers everything EXCEPT ENRICHMENT_H JSON parsing.
For ENRICHMENT_H (Reqs #1, #2, #4, #5-7 age link), run v1_snowflake_enrichment.sql
in Snowflake Worksheet first.

OUTPUT: Hive tables in dm_ciamop (or your target database) for PBI to connect.

Requirements covered here:
  From DM_CIAMOP:  #5-7 OTP, #8 UID/PWD, #11 Passkey Enroll, #12 Passkey Auth,
                   #13 Passwordless %, #15 Aggregators/MFA
  From TSMT:       #3 Device/Platform, #9 Auth Events, #10 Timestamps, #14 FI Status
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("V1_Auth_Dashboard_Final") \
    .enableHiveSupport() \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

START = "2025-01-01"
END   = "2025-03-31"
TARGET_DB = "dm_ciamop"  # change if you want output tables elsewhere


# ==============================================================
# DM_CIAMOP TABLES (pre-aggregated, fast)
# ==============================================================

# --- REQ #12 + #13: Passkey Auth + Passwordless % ---
print("Creating: v1_login_passkey_daily")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TARGET_DB}.v1_login_passkey_daily AS
    SELECT
        kafka_process_dt AS business_date,
        total_person_logins,
        passkey_logins,
        mbank_biometric_logins,
        mbank_creds_logins,
        onepass_logins,
        total_aggregator_logins,
        (COALESCE(passkey_logins,0) + COALESCE(mbank_biometric_logins,0) + COALESCE(onepass_logins,0)) AS passwordless_total,
        ROUND(
            (COALESCE(passkey_logins,0) + COALESCE(mbank_biometric_logins,0) + COALESCE(onepass_logins,0))
            / NULLIF(total_person_logins, 0) * 100, 2
        ) AS passwordless_pct
    FROM dm_ciamop.login_and_passkey_stats
    WHERE kafka_process_dt BETWEEN '{START}' AND '{END}'
""")
print("  Done.")


# --- REQ #8 + #11: UID/PWD Mix + Passkey Enrollments ---
print("Creating: v1_passkey_enrollment_daily")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TARGET_DB}.v1_passkey_enrollment_daily AS
    SELECT
        kafka_process_dt AS business_date,
        passkey_enrollments,
        passkey_logins,
        unpw_logins,
        (COALESCE(unpw_logins,0) + COALESCE(passkey_logins,0)) AS total_logins,
        ROUND(COALESCE(unpw_logins,0) / NULLIF(COALESCE(unpw_logins,0) + COALESCE(passkey_logins,0), 0) * 100, 2) AS uid_pwd_pct
    FROM dm_ciamop.passkey_enrollment_login
    WHERE kafka_process_dt BETWEEN '{START}' AND '{END}'
""")
print("  Done.")


# --- REQ #11: Enrolled Users ---
print("Creating: v1_passkey_enrolled_users")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TARGET_DB}.v1_passkey_enrolled_users AS
    SELECT
        kafka_process_dt AS business_date,
        COUNT(DISTINCT user_id) AS enrolled_user_count
    FROM dm_ciamop.passkey_enrolled_users
    WHERE kafka_process_dt BETWEEN '{START}' AND '{END}'
    GROUP BY kafka_process_dt
""")
print("  Done.")


# --- REQ #15: MFA Breakdown ---
print("Creating: v1_mfa_breakdown")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TARGET_DB}.v1_mfa_breakdown AS
    SELECT
        day AS business_date,
        mfa_app,
        policy,
        review_status,
        SUM(events) AS total_events
    FROM dm_ciamop.mfa_app
    WHERE day BETWEEN '{START}' AND '{END}'
    GROUP BY day, mfa_app, policy, review_status
""")
print("  Done.")


# --- REQ #5-7: OTP from tmx_tsmt_review_otp ---
print("Creating: v1_otp_review_detail")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TARGET_DB}.v1_otp_review_detail AS
    SELECT
        day AS business_date,
        otp_channel,
        otp_type,
        application_id,
        session_type,
        mfa_app,
        SUM(otps_sent) AS total_sent,
        SUM(otp_success) AS total_success,
        SUM(otps_failed) AS total_failed,
        SUM(otp_abandoned) AS total_abandoned,
        SUM(tmx_count) AS total_tmx_events,
        ROUND(SUM(otp_success) / NULLIF(SUM(otps_sent), 0) * 100, 2) AS success_rate
    FROM dm_ciamop.tmx_tsmt_review_otp
    WHERE day BETWEEN '{START}' AND '{END}'
    GROUP BY day, otp_channel, otp_type, application_id, session_type, mfa_app
""")
print("  Done.")


# --- REQ #5-7: OTP from otp_stats_loop ---
print("Creating: v1_otp_stats")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TARGET_DB}.v1_otp_stats AS
    SELECT
        kafka_process_dt AS business_date,
        policy_id,
        application_id,
        SUM(smscount) AS sms_total,
        SUM(emailcount) AS email_total
    FROM dm_ciamop.otp_stats_loop
    WHERE kafka_process_dt BETWEEN '{START}' AND '{END}'
    GROUP BY kafka_process_dt, policy_id, application_id
""")
print("  Done.")


# ==============================================================
# TSMT TABLES — direct columns only (no ENRICHMENT_H JSON)
# ==============================================================

# --- REQ #9: Auth Events ---
print("Creating: v1_auth_events")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TARGET_DB}.v1_auth_events AS
    SELECT
        ODS_BUSINESS_DT AS business_date,
        ACTION,
        COUNT(*) AS events,
        COUNT(DISTINCT SESSION_ID) AS sessions,
        COUNT(DISTINCT USER_ID) AS users
    FROM sl1_tsmt.tsmt_authentication_h
    WHERE ODS_BUSINESS_DT BETWEEN '{START}' AND '{END}'
      AND FAILURE = 0
    GROUP BY ODS_BUSINESS_DT, ACTION
""")
print("  Done.")


# --- REQ #3: Device & Platform ---
print("Creating: v1_device_platform")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TARGET_DB}.v1_device_platform AS
    SELECT
        ODS_BUSINESS_DT AS business_date,
        DEVICE_OS_TYPE,
        get_json_object(DATA, '$.hw_type') AS hw_type,
        get_json_object(DATA, '$.device_model') AS device_model,
        COUNT(*) AS events,
        COUNT(DISTINCT SESSION_ID) AS sessions
    FROM sl1_tsmt.tsmt_device_h
    WHERE ODS_BUSINESS_DT BETWEEN '{START}' AND '{END}'
      AND FAILURE = 0
    GROUP BY
        ODS_BUSINESS_DT,
        DEVICE_OS_TYPE,
        get_json_object(DATA, '$.hw_type'),
        get_json_object(DATA, '$.device_model')
""")
print("  Done.")


# --- REQ #14: FI Enabled vs Disabled ---
print("Creating: v1_fi_status")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TARGET_DB}.v1_fi_status AS
    SELECT
        ODS_BUSINESS_DT AS business_date,
        get_json_object(DATA, '$.device_public_key') AS device_public_key,
        get_json_object(DATA, '$.rsaEncryptionEnabled') AS rsa_enabled,
        COUNT(*) AS events,
        COUNT(DISTINCT SESSION_ID) AS sessions
    FROM sl1_tsmt.tsmt_device_h
    WHERE ODS_BUSINESS_DT BETWEEN '{START}' AND '{END}'
      AND FAILURE = 0
    GROUP BY
        ODS_BUSINESS_DT,
        get_json_object(DATA, '$.device_public_key'),
        get_json_object(DATA, '$.rsaEncryptionEnabled')
""")
print("  Done.")


# --- REQ #10: Auth Timestamps (for time-series in PBI) ---
# Only keep aggregated hourly data to keep table size manageable
print("Creating: v1_auth_hourly")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TARGET_DB}.v1_auth_hourly AS
    SELECT
        ODS_BUSINESS_DT AS business_date,
        SUBSTR(ODS_BUSINESS_TS, 12, 2) AS hour_of_day,
        ACTION,
        AUTHENTICATION_METHOD,
        APPLICATION_ID,
        COUNT(*) AS events,
        COUNT(DISTINCT SESSION_ID) AS sessions
    FROM sl1_tsmt.tsmt_authentication_h
    WHERE ODS_BUSINESS_DT BETWEEN '{START}' AND '{END}'
      AND FAILURE = 0
    GROUP BY
        ODS_BUSINESS_DT,
        SUBSTR(ODS_BUSINESS_TS, 12, 2),
        ACTION,
        AUTHENTICATION_METHOD,
        APPLICATION_ID
""")
print("  Done.")


print("""
====================================================
ALL TABLES CREATED SUCCESSFULLY
====================================================

Tables created in {0}:
  v1_login_passkey_daily     — Reqs #12, #13, #15 (passkey, passwordless, aggregators)
  v1_passkey_enrollment_daily — Reqs #8, #11 (UID/PWD mix, enrollments)
  v1_passkey_enrolled_users  — Req #11 (enrolled user counts)
  v1_mfa_breakdown           — Req #15 (MFA app type)
  v1_otp_review_detail       — Reqs #5-7 (OTP by channel with success rates)
  v1_otp_stats               — Reqs #5-7 (SMS/email counts)
  v1_auth_events             — Req #9 (login/logout/session events)
  v1_device_platform         — Req #3 (device OS, model, browser)
  v1_fi_status               — Req #14 (FI enabled/disabled)
  v1_auth_hourly             — Req #10 (hourly time-series)

For Reqs #1, #2, #4 (channel, session ID, age groups):
  Run v1_snowflake_enrichment.sql in Snowflake Worksheet.
  Those create tables in SANDBOX.AUTH_DASHBOARD.

When ready for PBI, connect to these tables directly.
""".format(TARGET_DB))

spark.stop()

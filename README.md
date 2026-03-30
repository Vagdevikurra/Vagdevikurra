"""
V1 Script 01: DM_CIAMOP Requirements
=====================================
Covers:
  #5-7   OTP stats (SMS/Email by channel) — from otp_stats_loop + tmx_tsmt_review_otp
  #8     UID/PWD vs Passkey login mix     — from passkey_enrollment_login
  #11    Passkey Enrollments              — from passkey_enrolled_users + passkey_enrollment_login
  #12    Passkey Authentications           — from login_and_passkey_stats
  #13    % Passwordless Auth              — from login_and_passkey_stats
  #15    SSO / Aggregators / MFA type     — from login_and_passkey_stats + mfa_app

Run: spark-submit v1_script_01_dm_ciamop.py
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("V1_01_DM_CIAMOP") \
    .enableHiveSupport() \
    .getOrCreate()

# -------------------------------------------------------
# CONFIG — adjust dates as needed
# -------------------------------------------------------
START_DATE = "2025-01-01"
END_DATE   = "2025-03-31"


# =======================================================
# REQ #11: Passkey Enrollments
# =======================================================
print(">>> REQ #11: Passkey Enrollments")

# Daily enrollment counts
req11_enrollments = spark.sql(f"""
    SELECT
        kafka_process_dt AS business_date,
        passkey_enrollments,
        passkey_logins,
        unpw_logins
    FROM dm_ciamop.passkey_enrollment_login
    WHERE kafka_process_dt BETWEEN '{START_DATE}' AND '{END_DATE}'
    ORDER BY kafka_process_dt
""")
req11_enrollments.show(20, truncate=False)

# Unique enrolled users
req11_users = spark.sql(f"""
    SELECT
        kafka_process_dt AS business_date,
        COUNT(DISTINCT user_id) AS enrolled_user_count
    FROM dm_ciamop.passkey_enrolled_users
    WHERE kafka_process_dt BETWEEN '{START_DATE}' AND '{END_DATE}'
    GROUP BY kafka_process_dt
    ORDER BY kafka_process_dt
""")
req11_users.show(20, truncate=False)


# =======================================================
# REQ #12: Passkey Authentications
# =======================================================
print(">>> REQ #12: Passkey Authentications")

req12 = spark.sql(f"""
    SELECT
        kafka_process_dt AS business_date,
        passkey_logins,
        total_person_logins,
        ROUND(passkey_logins / NULLIF(total_person_logins, 0) * 100, 2) AS passkey_login_pct
    FROM dm_ciamop.login_and_passkey_stats
    WHERE kafka_process_dt BETWEEN '{START_DATE}' AND '{END_DATE}'
    ORDER BY kafka_process_dt
""")
req12.show(20, truncate=False)


# =======================================================
# REQ #13: % Passwordless Auth
# =======================================================
print(">>> REQ #13: % Passwordless Auth")

req13 = spark.sql(f"""
    SELECT
        kafka_process_dt AS business_date,
        total_person_logins,
        passkey_logins,
        mbank_biometric_logins,
        onepass_logins,
        (passkey_logins + mbank_biometric_logins + onepass_logins) AS passwordless_logins,
        mbank_creds_logins AS credential_logins,
        ROUND(
            (passkey_logins + mbank_biometric_logins + onepass_logins) 
            / NULLIF(total_person_logins, 0) * 100, 2
        ) AS passwordless_pct
    FROM dm_ciamop.login_and_passkey_stats
    WHERE kafka_process_dt BETWEEN '{START_DATE}' AND '{END_DATE}'
    ORDER BY kafka_process_dt
""")
req13.show(20, truncate=False)


# =======================================================
# REQ #15 (partial): Aggregator Logins
# =======================================================
print(">>> REQ #15: Aggregator Logins")

req15_agg = spark.sql(f"""
    SELECT
        kafka_process_dt AS business_date,
        total_aggregator_logins,
        total_person_logins,
        ROUND(
            total_aggregator_logins 
            / NULLIF(total_person_logins + total_aggregator_logins, 0) * 100, 2
        ) AS aggregator_pct
    FROM dm_ciamop.login_and_passkey_stats
    WHERE kafka_process_dt BETWEEN '{START_DATE}' AND '{END_DATE}'
    ORDER BY kafka_process_dt
""")
req15_agg.show(20, truncate=False)


# =======================================================
# REQ #15 (partial): MFA App Breakdown
# =======================================================
print(">>> REQ #15: MFA App Breakdown")

req15_mfa = spark.sql(f"""
    SELECT
        day AS business_date,
        mfa_app,
        policy,
        review_status,
        SUM(events) AS total_events
    FROM dm_ciamop.mfa_app
    WHERE day BETWEEN '{START_DATE}' AND '{END_DATE}'
    GROUP BY day, mfa_app, policy, review_status
    ORDER BY day, mfa_app
""")
req15_mfa.show(30, truncate=False)


# =======================================================
# REQ #8 (partial): UID/PWD vs Passkey Login Mix
# =======================================================
print(">>> REQ #8: UID/PWD vs Passkey Login Mix")

req08 = spark.sql(f"""
    SELECT
        kafka_process_dt AS business_date,
        unpw_logins,
        passkey_logins,
        (unpw_logins + passkey_logins) AS total_logins,
        ROUND(unpw_logins / NULLIF(unpw_logins + passkey_logins, 0) * 100, 2) AS uid_pwd_pct,
        ROUND(passkey_logins / NULLIF(unpw_logins + passkey_logins, 0) * 100, 2) AS passkey_pct
    FROM dm_ciamop.passkey_enrollment_login
    WHERE kafka_process_dt BETWEEN '{START_DATE}' AND '{END_DATE}'
    ORDER BY kafka_process_dt
""")
req08.show(20, truncate=False)


# =======================================================
# REQ #5-7 (partial): OTP Stats — SMS & Email counts
# =======================================================
print(">>> REQ #5-7: OTP Stats (otp_stats_loop)")

req05_07_basic = spark.sql(f"""
    SELECT
        kafka_process_dt AS business_date,
        policy_id,
        application_id,
        SUM(smscount) AS total_sms,
        SUM(emailcount) AS total_email
    FROM dm_ciamop.otp_stats_loop
    WHERE kafka_process_dt BETWEEN '{START_DATE}' AND '{END_DATE}'
    GROUP BY kafka_process_dt, policy_id, application_id
    ORDER BY kafka_process_dt
""")
req05_07_basic.show(30, truncate=False)


# =======================================================
# REQ #5-7 (detailed): OTP from tmx_tsmt_review_otp
# — has otp_channel (SMS/Voice/Email), success/fail rates
# =======================================================
print(">>> REQ #5-7: OTP Detail (tmx_tsmt_review_otp)")

req05_07_detail = spark.sql(f"""
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
        ROUND(
            SUM(otp_success) / NULLIF(SUM(otps_sent), 0) * 100, 2
        ) AS success_rate_pct
    FROM dm_ciamop.tmx_tsmt_review_otp
    WHERE day BETWEEN '{START_DATE}' AND '{END_DATE}'
    GROUP BY day, otp_channel, otp_type, application_id, session_type, mfa_app
    ORDER BY day, otp_channel
""")
req05_07_detail.show(30, truncate=False)


print("=== Script 01 Complete ===")
print("Review the outputs above. If they look right, we'll move to TSMT tables next.")

spark.stop()



"""
Helper: Sample the DATA column from TSMT tables
================================================
Run this first so we can see the actual JSON structure.
Then we'll know exactly what fields to parse.
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("V1_DATA_SAMPLE") \
    .enableHiveSupport() \
    .getOrCreate()

SAMPLE_DATE = "2025-03-01"  # Pick a recent date with data

# Sample from AUTHENTICATION_H
print("=" * 60)
print("TSMT_AUTHENTICATION_H — DATA sample")
print("=" * 60)
spark.sql(f"""
    SELECT DATA
    FROM PRODUCTION.SL1_TSMT.TSMT_AUTHENTICATION_H
    WHERE ODS_BUSINESS_DT = '{SAMPLE_DATE}'
      AND FAILURE = 0
      AND DATA IS NOT NULL
    LIMIT 3
""").show(3, truncate=200)

# Sample from ENRICHMENT_H
print("=" * 60)
print("TSMT_ENRICHMENT_H — DATA sample")
print("=" * 60)
spark.sql(f"""
    SELECT DATA
    FROM PRODUCTION.SL1_TSMT.TSMT_ENRICHMENT_H
    WHERE ODS_BUSINESS_DT = '{SAMPLE_DATE}'
      AND FAILURE = 0
      AND DATA IS NOT NULL
    LIMIT 3
""").show(3, truncate=200)

# Sample from SESSION_H
print("=" * 60)
print("TSMT_SESSION_H — DATA sample")
print("=" * 60)
spark.sql(f"""
    SELECT DATA
    FROM PRODUCTION.SL1_TSMT.TSMT_SESSION_H
    WHERE ODS_BUSINESS_DT = '{SAMPLE_DATE}'
      AND FAILURE = 0
      AND DATA IS NOT NULL
    LIMIT 3
""").show(3, truncate=200)

# Sample from DEVICE_H
print("=" * 60)
print("TSMT_DEVICE_H — DATA sample")
print("=" * 60)
spark.sql(f"""
    SELECT DATA
    FROM PRODUCTION.SL1_TSMT.TSMT_DEVICE_H
    WHERE ODS_BUSINESS_DT = '{SAMPLE_DATE}'
      AND FAILURE = 0
      AND DATA IS NOT NULL
    LIMIT 3
""").show(3, truncate=200)

spark.stop()

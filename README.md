"""
V1 Enterprise Auth Dashboard — Script 01: DM_CIAMOP Metrics
=============================================================
Requirements covered:
  #5-7  OTP breakdown (SMS/Voice/Email) — tmx_tsmt_review_otp + otp_stats_loop
  #8    UID/PWD vs Passkey login mix    — passkey_enrollment_login
  #11   Passkey Enrollments             — passkey_enrolled_users + passkey_enrollment_login
  #12   Passkey Authentications         — login_and_passkey_stats
  #13   % Passwordless Auth             — login_and_passkey_stats
  #15   Aggregators / MFA type          — login_and_passkey_stats + mfa_app

These tables are in Hive (DM_CIAMOP). No JSON parsing needed.
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("V1_01_DM_CIAMOP") \
    .enableHiveSupport() \
    .getOrCreate()

START_DATE = "2025-01-01"
END_DATE   = "2025-03-31"

# ----- REQ #11: Passkey Enrollments -----
print("=" * 50)
print("REQ #11: Passkey Enrollments")

spark.sql(f"""
    SELECT kafka_process_dt, passkey_enrollments, passkey_logins, unpw_logins
    FROM dm_ciamop.passkey_enrollment_login
    WHERE kafka_process_dt BETWEEN '{START_DATE}' AND '{END_DATE}'
    ORDER BY kafka_process_dt
""").show(20, truncate=False)

spark.sql(f"""
    SELECT kafka_process_dt, COUNT(DISTINCT user_id) AS enrolled_users
    FROM dm_ciamop.passkey_enrolled_users
    WHERE kafka_process_dt BETWEEN '{START_DATE}' AND '{END_DATE}'
    GROUP BY kafka_process_dt
    ORDER BY kafka_process_dt
""").show(20, truncate=False)


# ----- REQ #12: Passkey Authentications -----
print("=" * 50)
print("REQ #12: Passkey Authentications")

spark.sql(f"""
    SELECT kafka_process_dt, passkey_logins, total_person_logins,
        ROUND(passkey_logins / NULLIF(total_person_logins, 0) * 100, 2) AS passkey_pct
    FROM dm_ciamop.login_and_passkey_stats
    WHERE kafka_process_dt BETWEEN '{START_DATE}' AND '{END_DATE}'
    ORDER BY kafka_process_dt
""").show(20, truncate=False)


# ----- REQ #13: % Passwordless Auth -----
print("=" * 50)
print("REQ #13: % Passwordless Auth")

spark.sql(f"""
    SELECT kafka_process_dt,
        total_person_logins,
        passkey_logins,
        mbank_biometric_logins,
        onepass_logins,
        (passkey_logins + mbank_biometric_logins + onepass_logins) AS passwordless_total,
        mbank_creds_logins AS credential_logins,
        ROUND(
            (passkey_logins + mbank_biometric_logins + onepass_logins)
            / NULLIF(total_person_logins, 0) * 100, 2
        ) AS passwordless_pct
    FROM dm_ciamop.login_and_passkey_stats
    WHERE kafka_process_dt BETWEEN '{START_DATE}' AND '{END_DATE}'
    ORDER BY kafka_process_dt
""").show(20, truncate=False)


# ----- REQ #8: UID/PWD vs Passkey Mix -----
print("=" * 50)
print("REQ #8: UID/PWD vs Passkey Login Mix")

spark.sql(f"""
    SELECT kafka_process_dt,
        unpw_logins,
        passkey_logins,
        (unpw_logins + passkey_logins) AS total,
        ROUND(unpw_logins / NULLIF(unpw_logins + passkey_logins, 0) * 100, 2) AS uid_pwd_pct,
        ROUND(passkey_logins / NULLIF(unpw_logins + passkey_logins, 0) * 100, 2) AS passkey_pct
    FROM dm_ciamop.passkey_enrollment_login
    WHERE kafka_process_dt BETWEEN '{START_DATE}' AND '{END_DATE}'
    ORDER BY kafka_process_dt
""").show(20, truncate=False)


# ----- REQ #15: Aggregators + MFA -----
print("=" * 50)
print("REQ #15: Aggregator Logins")

spark.sql(f"""
    SELECT kafka_process_dt,
        total_aggregator_logins,
        total_person_logins
    FROM dm_ciamop.login_and_passkey_stats
    WHERE kafka_process_dt BETWEEN '{START_DATE}' AND '{END_DATE}'
    ORDER BY kafka_process_dt
""").show(20, truncate=False)

print("REQ #15: MFA App Breakdown")
spark.sql(f"""
    SELECT day, mfa_app, policy, review_status, SUM(events) AS total_events
    FROM dm_ciamop.mfa_app
    WHERE day BETWEEN '{START_DATE}' AND '{END_DATE}'
    GROUP BY day, mfa_app, policy, review_status
    ORDER BY day, mfa_app
""").show(30, truncate=False)


# ----- REQ #5-7: OTP by Channel (SMS/Voice/Email) -----
print("=" * 50)
print("REQ #5-7: OTP Detail from tmx_tsmt_review_otp")

spark.sql(f"""
    SELECT day,
        otp_channel,
        otp_type,
        application_id,
        session_type,
        mfa_app,
        SUM(otps_sent) AS sent,
        SUM(otp_success) AS success,
        SUM(otps_failed) AS failed,
        SUM(otp_abandoned) AS abandoned,
        ROUND(SUM(otp_success) / NULLIF(SUM(otps_sent), 0) * 100, 2) AS success_rate
    FROM dm_ciamop.tmx_tsmt_review_otp
    WHERE day BETWEEN '{START_DATE}' AND '{END_DATE}'
    GROUP BY day, otp_channel, otp_type, application_id, session_type, mfa_app
    ORDER BY day, otp_channel
""").show(30, truncate=False)

print("REQ #5-7: OTP from otp_stats_loop")
spark.sql(f"""
    SELECT kafka_process_dt, policy_id, application_id,
        SUM(smscount) AS sms_total,
        SUM(emailcount) AS email_total
    FROM dm_ciamop.otp_stats_loop
    WHERE kafka_process_dt BETWEEN '{START_DATE}' AND '{END_DATE}'
    GROUP BY kafka_process_dt, policy_id, application_id
    ORDER BY kafka_process_dt
""").show(30, truncate=False)


print("=== Script 01 Complete — Review outputs above ===")
spark.stop()

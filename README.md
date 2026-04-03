"""
STEP 1 — Session Events Fact Table
====================================
ALL direct columns. ZERO JSON.
All 5 TSMT tables included — same column names carry
DIFFERENT event entities across tables.

Output: dm_ib_dev.v1_session_events
PBI join: user_id = v1_customer_dim.RCIF_CUST_NBR
"""

import v1_config as cfg

spark = cfg.get_spark()

SD = cfg.START_DATE
ED = cfg.END_DATE

# -------------------------------------------------------------------
# AUTH_H — reqs 9,10,12,16,19 (OTP, UID/PWD, passwordless, MFA)
# Has: authentication_method, action3, token_name
# -------------------------------------------------------------------
print("1/5  TSMT_AUTHENTICATION_H ...")

spark.sql(f"""
    SELECT
        'AUTH'                  AS source_table,
        user_id,
        session_id,
        session_type,
        application_id,
        authentication_method,
        category,
        action,
        action2,
        action3,
        device_os_type,
        device_os_version,
        device_model,
        token_name,
        flow_id,
        failure,
        policy_id,
        time                    AS event_time,
        CAST(NULL AS STRING)    AS authentication_result,
        CAST(NULL AS STRING)    AS risk_score,
        CAST(NULL AS STRING)    AS phone_number,
        CAST(NULL AS STRING)    AS call_id,
        CAST(NULL AS STRING)    AS pindrop_device_type,
        CAST(NULL AS STRING)    AS line_of_business,
        CAST(NULL AS STRING)    AS duration,
        kafka_process_dt        AS event_date
    FROM {cfg.TSMT_AUTHENTICATION_H}
    WHERE kafka_process_dt BETWEEN '{SD}' AND '{ED}'
""").createOrReplaceTempView("auth_part")


# -------------------------------------------------------------------
# SESSION_H — reqs 2,3,4,8,15,18 (channel, timestamp, SSO, active)
# Has: token_name. No authentication_method, no action3.
# -------------------------------------------------------------------
print("2/5  TSMT_SESSION_H ...")

spark.sql(f"""
    SELECT
        'SESSION'               AS source_table,
        user_id,
        session_id,
        session_type,
        application_id,
        CAST(NULL AS STRING)    AS authentication_method,
        category,
        action,
        action2,
        CAST(NULL AS STRING)    AS action3,
        device_os_type,
        device_os_version,
        device_model,
        token_name,
        flow_id,
        failure,
        policy_id,
        time                    AS event_time,
        CAST(NULL AS STRING)    AS authentication_result,
        CAST(NULL AS STRING)    AS risk_score,
        CAST(NULL AS STRING)    AS phone_number,
        CAST(NULL AS STRING)    AS call_id,
        CAST(NULL AS STRING)    AS pindrop_device_type,
        CAST(NULL AS STRING)    AS line_of_business,
        CAST(NULL AS STRING)    AS duration,
        kafka_process_dt        AS event_date
    FROM {cfg.TSMT_SESSION_H}
    WHERE kafka_process_dt BETWEEN '{SD}' AND '{ED}'
""").createOrReplaceTempView("session_part")


# -------------------------------------------------------------------
# ENRICHMENT_H — reqs 13,14 (logout, token refresh)
# Filtered to only relevant actions to keep it small.
# -------------------------------------------------------------------
print("3/5  TSMT_ENRICHMENT_H (logout + token_refresh) ...")

spark.sql(f"""
    SELECT
        'ENRICHMENT'            AS source_table,
        user_id,
        session_id,
        session_type,
        application_id,
        CAST(NULL AS STRING)    AS authentication_method,
        category,
        action,
        action2,
        CAST(NULL AS STRING)    AS action3,
        device_os_type,
        device_os_version,
        device_model,
        CAST(NULL AS STRING)    AS token_name,
        flow_id,
        failure,
        policy_id,
        time                    AS event_time,
        CAST(NULL AS STRING)    AS authentication_result,
        CAST(NULL AS STRING)    AS risk_score,
        CAST(NULL AS STRING)    AS phone_number,
        CAST(NULL AS STRING)    AS call_id,
        CAST(NULL AS STRING)    AS pindrop_device_type,
        CAST(NULL AS STRING)    AS line_of_business,
        CAST(NULL AS STRING)    AS duration,
        kafka_process_dt        AS event_date
    FROM {cfg.TSMT_ENRICHMENT_H}
    WHERE kafka_process_dt BETWEEN '{SD}' AND '{ED}'
      AND action IN ('session_logout', 'token_refresh')
""").createOrReplaceTempView("enrichment_part")


# -------------------------------------------------------------------
# DEVICE_H — req 5 (device/platform)
# No authentication_method, no token_name, no action3.
# -------------------------------------------------------------------
print("4/5  TSMT_DEVICE_H ...")

spark.sql(f"""
    SELECT
        'DEVICE'                AS source_table,
        user_id,
        session_id,
        session_type,
        application_id,
        CAST(NULL AS STRING)    AS authentication_method,
        category,
        action,
        action2,
        CAST(NULL AS STRING)    AS action3,
        device_os_type,
        device_os_version,
        device_model,
        CAST(NULL AS STRING)    AS token_name,
        flow_id,
        failure,
        policy_id,
        time                    AS event_time,
        CAST(NULL AS STRING)    AS authentication_result,
        CAST(NULL AS STRING)    AS risk_score,
        CAST(NULL AS STRING)    AS phone_number,
        CAST(NULL AS STRING)    AS call_id,
        CAST(NULL AS STRING)    AS pindrop_device_type,
        CAST(NULL AS STRING)    AS line_of_business,
        CAST(NULL AS STRING)    AS duration,
        kafka_process_dt        AS event_date
    FROM {cfg.TSMT_DEVICE_H}
    WHERE kafka_process_dt BETWEEN '{SD}' AND '{ED}'
""").createOrReplaceTempView("device_part")


# -------------------------------------------------------------------
# PINDROP_H — req 11 (voice call MFA)
# Completely different schema: uid (not user_id),
# no session_id, no session_type, no application_id, etc.
# -------------------------------------------------------------------
print("5/5  TSMT_PINDROP_H ...")

spark.sql(f"""
    SELECT
        'PINDROP'               AS source_table,
        uid                     AS user_id,
        CAST(NULL AS STRING)    AS session_id,
        CAST(NULL AS STRING)    AS session_type,
        CAST(NULL AS STRING)    AS application_id,
        CAST(NULL AS STRING)    AS authentication_method,
        CAST(NULL AS STRING)    AS category,
        CAST(NULL AS STRING)    AS action,
        CAST(NULL AS STRING)    AS action2,
        CAST(NULL AS STRING)    AS action3,
        CAST(NULL AS STRING)    AS device_os_type,
        CAST(NULL AS STRING)    AS device_os_version,
        CAST(NULL AS STRING)    AS device_model,
        CAST(NULL AS STRING)    AS token_name,
        CAST(NULL AS STRING)    AS flow_id,
        CAST(NULL AS TINYINT)   AS failure,
        CAST(NULL AS STRING)    AS policy_id,
        call_start_time         AS event_time,
        authentication_result,
        risk_score,
        phone_number,
        call_id,
        device_type             AS pindrop_device_type,
        line_of_business,
        duration,
        kafka_process_dt        AS event_date
    FROM {cfg.TSMT_PINDROP_H}
    WHERE kafka_process_dt BETWEEN '{SD}' AND '{ED}'
""").createOrReplaceTempView("pindrop_part")


# -------------------------------------------------------------------
# COMBINE ALL
# -------------------------------------------------------------------
print("Combining all 5 sources ...")

combined = spark.sql("""
    SELECT * FROM auth_part
    UNION ALL
    SELECT * FROM session_part
    UNION ALL
    SELECT * FROM enrichment_part
    UNION ALL
    SELECT * FROM device_part
    UNION ALL
    SELECT * FROM pindrop_part
""")

print("Saving v1_session_events ...")
cfg.save_to_hive(combined, "v1_session_events")

print("")
print("DONE. 2 tables ready for PBI:")
print(f"  {cfg.OUTPUT_DB}.v1_customer_dim")
print(f"  {cfg.OUTPUT_DB}.v1_session_events")
print("")
print("PBI join: v1_session_events.user_id = v1_customer_dim.RCIF_CUST_NBR")

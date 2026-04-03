"""
STEP 1 — Session Events Fact Table
====================================
ALL direct columns. ZERO JSON.

AUTH_H alone covers reqs 2-5, 8-10, 12, 15-16, 18-19 because
it already has: session_type, application_id, session_id, time,
flow_id, token_name, device_os_type, device_model, user_id.
So SESSION_H and DEVICE_H are NOT needed — saves a huge load.

We only add:
  ENRICHMENT_H  → logout + token_refresh rows (reqs 13, 14)
  PINDROP_H     → voice call MFA (req 11, tiny table)

Output: dm_ib_dev.v1_session_events

PBI joins this to v1_customer_dim on user_id = RCIF_CUST_NBR
"""

import v1_config as cfg

spark = cfg.get_spark()

SD = cfg.START_DATE
ED = cfg.END_DATE

# -------------------------------------------------------------------
# PART A: TSMT_AUTHENTICATION_H  (main table, covers most reqs)
# -------------------------------------------------------------------
print("Part A: TSMT_AUTHENTICATION_H ...")

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
        time                    AS event_time,
        CAST(NULL AS STRING)    AS authentication_result,
        kafka_process_dt        AS event_date
    FROM {cfg.TSMT_AUTHENTICATION_H}
    WHERE kafka_process_dt BETWEEN '{SD}' AND '{ED}'
""").createOrReplaceTempView("auth_part")


# -------------------------------------------------------------------
# PART B: TSMT_ENRICHMENT_H  (ONLY logout + token refresh)
# Very small result set — heavily filtered.
# -------------------------------------------------------------------
print("Part B: TSMT_ENRICHMENT_H (logout + token_refresh) ...")

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
        CAST(NULL AS TINYINT)   AS failure,
        time                    AS event_time,
        CAST(NULL AS STRING)    AS authentication_result,
        kafka_process_dt        AS event_date
    FROM {cfg.TSMT_ENRICHMENT_H}
    WHERE kafka_process_dt BETWEEN '{SD}' AND '{ED}'
      AND action IN ('session_logout', 'token_refresh')
""").createOrReplaceTempView("enrichment_part")


# -------------------------------------------------------------------
# PART C: TSMT_PINDROP_H  (voice call MFA — ~7M rows total, tiny)
# NOTE: verify column names match your PINDROP schema.
#       If user_id is called UID in your table, change it below.
# -------------------------------------------------------------------
print("Part C: TSMT_PINDROP_H ...")

spark.sql(f"""
    SELECT
        'PINDROP'               AS source_table,
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
        CAST(NULL AS STRING)    AS device_os_version,
        device_model,
        CAST(NULL AS STRING)    AS token_name,
        flow_id,
        failure,
        time                    AS event_time,
        authentication_result,
        kafka_process_dt        AS event_date
    FROM {cfg.TSMT_PINDROP_H}
    WHERE kafka_process_dt BETWEEN '{SD}' AND '{ED}'
""").createOrReplaceTempView("pindrop_part")


# -------------------------------------------------------------------
# COMBINE and SAVE
# -------------------------------------------------------------------
print("Combining and saving ...")

combined = spark.sql("""
    SELECT * FROM auth_part
    UNION ALL
    SELECT * FROM enrichment_part
    UNION ALL
    SELECT * FROM pindrop_part
""")

cfg.save_to_hive(combined, "v1_session_events")

print("")
print("=== v1_session_events saved ===")
print("")
print("DONE. You now have 2 tables:")
print(f"  1. {cfg.OUTPUT_DB}.v1_customer_dim     (customer demographics)")
print(f"  2. {cfg.OUTPUT_DB}.v1_session_events   (all auth/session events)")
print("")
print("PBI join: v1_session_events.user_id = v1_customer_dim.RCIF_CUST_NBR")
print("")
print("PBI FILTER CHEAT SHEET:")
print("  Channel Type       → session_type")
print("  Channel Code       → application_id")
print("  Channel Session ID → session_id")
print("  Device / Platform  → device_os_type, device_os_version, device_model")
print("  Digitally Active   → DISTINCT user_id in date range")
print("  SMS OTP            → authentication_method='otp' AND category='sms'")
print("  Email OTP          → authentication_method='otp' AND category='email'")
print("  Voice Call MFA     → source_table='PINDROP' AND authentication_result='PASS'")
print("  % UID/PWD          → authentication_method='password' AND action='assertion_end'")
print("  Logout Events      → source_table='ENRICHMENT' AND action='session_logout'")
print("  Token Refresh      → source_table='ENRICHMENT' AND action='token_refresh'")
print("  Event Timestamp    → event_time")
print("  Passwordless %     → authentication_method<>'password' AND action='assertion_end'")
print("  SSO / Aggregators  → flow_id, token_name, application_id")
print("  MFA Type           → authentication_method, category")

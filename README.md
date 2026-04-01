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
EIL_DIGITAL_BANKING      = "DM_IB.DIGITAL_BANKING_MASTER"
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


"""
=================================================================
STEP 0 — Build Customer Dimension  (run this FIRST)
=================================================================
"""

from v1_config import *

spark = get_spark()

print("Building customer dimension...")

cust_sql = f"""
SELECT DISTINCT
    IP.RCIF_CUST_NBR,
    DB.IBN,

    CASE WHEN DB.IBN IS NOT NULL
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

    IP.GENDER_TYPE_CODE AS CUSTOMER_GENDER,

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

"""
=================================================================
STEP 1 — Reqs #1, #2, #3:  Channel Type, Channel Code, Device
=================================================================
DIRECT columns: action, device_os_type, device_model, session_type,
                application_id, user_id, authentication_method
FROM DATA (JSON): channel, channelIndicator, channelSessionId
=================================================================
"""

from v1_config import *

spark = get_spark()

print("=== Reqs #1-3: Channel, Code, Device ===")

# ---------------------------------------------------------------
# TSMT_AUTHENTICATION_H — direct cols + JSON for channel fields
# ---------------------------------------------------------------
auth_sql = f"""
SELECT
    user_id,
    action,
    authentication_method,
    device_os_type,
    device_model,
    session_type,
    application_id,
    category,
    get_json_object(data, '$.channel')            AS channel_type,
    get_json_object(data, '$.channelIndicator')   AS channel_code,
    get_json_object(data, '$.channelSessionId')   AS channel_session_id,
    kafka_process_dt                               AS event_date
FROM {TSMT_AUTHENTICATION_H}
WHERE kafka_process_dt BETWEEN '{START_DATE}' AND '{END_DATE}'
"""

print("  Running auth query...")
auth_df = spark.sql(auth_sql)

# ---------------------------------------------------------------
# Join to customer dimension via user_id
# NOTE: if user_id doesn't match RCIF_CUST_NBR, we may need to
#       join via IBN or another key — check results.
# ---------------------------------------------------------------
cust = spark.table(f"{OUTPUT_DB}.v1_customer_dim")

result = auth_df.join(
    cust,
    auth_df.user_id == cust.RCIF_CUST_NBR,
    "left"
).select(
    auth_df.user_id.alias("USER_ID"),
    "channel_type", "channel_code", "channel_session_id",
    "action", "authentication_method",
    "device_os_type", "device_model",
    "session_type", "application_id", "category",
    "event_date",
    "AGE_BANDING", "CUSTOMER_GENERATION",
    "DIGITAL_CUSTOMER_CHECK"
)

save_to_hive(result, "v1_channel_device")

auth_df.unpersist()

print("=== Reqs #1-3 complete ===")





"""
==============================================================================
V1 Enterprise Authentication Dashboard - PySpark Module
==============================================================================
Purpose:  Gathers all V1 priority data elements for the Enterprise 
          Authentication Methods PBI Dashboard.
Platform: Spark SQL (PySpark) reading from:
            - Snowflake: PRODUCTION.SL1_TSMT.TSMT_* tables
            - Hive:      DM_CIAMOP.* tables
            - EIL:       Customer dimension (BuildCustAcctDF)
Author:   Auto-generated
==============================================================================

V1 REQUIREMENTS COVERED:
  #1  Channel Type
  #2  Channel ID / Code / ChannelSessionID
  #3  Device & Platform
  #4  Customer Age Groups - Digitally Enrolled vs Active Mobile App Users
  #5  # Users using SMS OTP by Age Group
  #6  # Users using Voice Call OTP by Age Group
  #7  # Users using Email OTP by Age Group & Channel
  #8  AUTH Metrics - % of UID/PWD
  #9  Auth Events - Logout / Token Refresh / Pwd Resets
  #10 Timestamps - Event Time
  #11 # Passkey Enrollments
  #12 # Passkey Authentications
  #13 % of Passwordless Auth
  #14 FI Disabled vs Enabled
  #15 # SSO, # Aggregators, and Type of MFA
==============================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, when, lit, count, countDistinct, sum as _sum,
    avg, round as _round, max as _max, min as _min,
    to_date, get_json_object, expr, concat_ws, trim,
    upper, lower, coalesce, datediff, current_date
)
from pyspark.sql.types import IntegerType, StringType, DateType


# =============================================================================
# SECTION 0: CONFIGURATION
# =============================================================================

# Snowflake TSMT schema
TSMT_SCHEMA = "PRODUCTION.SL1_TSMT"

# DM_CIAMOP Hive database
DM_CIAMOP = "DM_CIAMOP"

# Table fully qualified names (Snowflake)
TSMT_AUTHENTICATION_H = f"{TSMT_SCHEMA}.TSMT_AUTHENTICATION_H"
TSMT_DEVICE_H         = f"{TSMT_SCHEMA}.TSMT_DEVICE_H"
TSMT_ENRICHMENT_H     = f"{TSMT_SCHEMA}.TSMT_ENRICHMENT_H"
TSMT_SESSION_H        = f"{TSMT_SCHEMA}.TSMT_SESSION_H"
TSMT_PINDROP_H        = f"{TSMT_SCHEMA}.TSMT_PINDROP_H"
TSMT_TMX_H            = f"{TSMT_SCHEMA}.TSMT_TMX_H"

# DM_CIAMOP tables (Hive)
LOGIN_AND_PASSKEY_STATS = f"{DM_CIAMOP}.login_and_passkey_stats"
MFA_APP                 = f"{DM_CIAMOP}.mfa_app"
OTP_STATS_LOOP          = f"{DM_CIAMOP}.otp_stats_loop"
PASSKEY_ENROLLED_USERS  = f"{DM_CIAMOP}.passkey_enrolled_users"
PASSKEY_ENROLLMENT_LOGIN = f"{DM_CIAMOP}.passkey_enrollment_login"
TMX_TSMT_REVIEW_OTP      = f"{DM_CIAMOP}.tmx_tsmt_review_otp"


# =============================================================================
# SECTION 1: HELPER - JSON DATA COLUMN PARSING
# =============================================================================
# The DATA column in TSMT tables is a large VARCHAR containing JSON.
# We use get_json_object() to extract specific fields.

def parse_tsmt_data(df, json_paths, prefix=""):
    """
    Extract fields from the JSON DATA column in TSMT tables.
    
    Args:
        df: DataFrame with a DATA column containing JSON
        json_paths: dict of {alias: json_path} 
                    e.g. {"rcif_cust_nbr": "$.RCIF_CUST_NBR"}
        prefix: optional prefix for column names
    Returns:
        DataFrame with new columns extracted from DATA
    """
    for alias, path in json_paths.items():
        col_name = f"{prefix}{alias}" if prefix else alias
        df = df.withColumn(col_name, get_json_object(col("DATA"), path))
    return df


# =============================================================================
# SECTION 2: BASE TABLE LOADERS WITH DATE FILTER
# =============================================================================

def load_tsmt_authentication(spark, start_date, end_date):
    """Load TSMT_AUTHENTICATION_H filtered by date range."""
    return spark.sql(f"""
        SELECT 
            ODS_BUSINESS_DT,
            ODS_BUSINESS_TS,
            SESSION_ID,
            DEVICE_ID,
            POLICY_ID,
            DEVICE_OS_TYPE,
            DEVICE_SESSION_ID,
            AUTHENTICATION_METHOD,
            ACTION,
            ACTION2,
            ACTION3,
            TOKEN_NAME,
            USER_ID,
            URI,
            CATEGORY,
            FLOW_ID,
            APPLICATION_ID,
            SESSION_TYPE,
            DATA
        FROM {TSMT_AUTHENTICATION_H}
        WHERE ODS_BUSINESS_DT BETWEEN '{start_date}' AND '{end_date}'
          AND FAILURE = 0
    """)


def load_tsmt_device(spark, start_date, end_date):
    """Load TSMT_DEVICE_H filtered by date range."""
    return spark.sql(f"""
        SELECT 
            ODS_BUSINESS_DT,
            ODS_BUSINESS_TS,
            SESSION_ID,
            DEVICE_ID,
            POLICY_ID,
            DEVICE_OS_TYPE,
            DEVICE_MODEL,
            DEVICE_SESSION_ID,
            ACTION,
            ACTION2,
            USER_ID,
            URI,
            CATEGORY,
            APPLICATION_ID,
            SESSION_TYPE,
            DATA
        FROM {TSMT_DEVICE_H}
        WHERE ODS_BUSINESS_DT BETWEEN '{start_date}' AND '{end_date}'
          AND FAILURE = 0
    """)


def load_tsmt_enrichment(spark, start_date, end_date):
    """Load TSMT_ENRICHMENT_H filtered by date range."""
    return spark.sql(f"""
        SELECT 
            ODS_BUSINESS_DT,
            ODS_BUSINESS_TS,
            SESSION_ID,
            DEVICE_ID,
            POLICY_ID,
            DEVICE_OS_TYPE,
            DEVICE_SESSION_ID,
            ACTION,
            ACTION2,
            USER_ID,
            URI,
            CATEGORY,
            APPLICATION_ID,
            SESSION_TYPE,
            TOKEN_NAME,
            DATA
        FROM {TSMT_ENRICHMENT_H}
        WHERE ODS_BUSINESS_DT BETWEEN '{start_date}' AND '{end_date}'
          AND FAILURE = 0
    """)


def load_tsmt_session(spark, start_date, end_date):
    """Load TSMT_SESSION_H filtered by date range."""
    return spark.sql(f"""
        SELECT 
            ODS_BUSINESS_DT,
            ODS_BUSINESS_TS,
            SESSION_ID,
            DEVICE_ID,
            POLICY_ID,
            DEVICE_OS_TYPE,
            DEVICE_SESSION_ID,
            AUTHENTICATION_METHOD,
            ACTION,
            ACTION2,
            ACTION3,
            USER_ID,
            URI,
            CATEGORY,
            ERROR_CODE,
            ERROR_MESSAGE,
            APPLICATION_ID,
            SESSION_TYPE,
            FLOW_ID,
            DATA
        FROM {TSMT_SESSION_H}
        WHERE ODS_BUSINESS_DT BETWEEN '{start_date}' AND '{end_date}'
          AND FAILURE = 0
    """)


def load_tsmt_pindrop(spark, start_date, end_date):
    """Load TSMT_PINDROP_H filtered by date range."""
    return spark.sql(f"""
        SELECT 
            ODS_BUSINESS_DT,
            UID,
            PHONE_NUMBER,
            CALL_START_TIME,
            CALL_END_TIME,
            DURATION,
            LINE_OF_BUSINESS,
            ACCOUNT,
            DEVICE_TYPE,
            RISK_SCORE,
            RISK_COLOR,
            AUTHENTICATION_RESULT,
            CALLER_ID_BLACKLISTED,
            CALLER_ID_WHITELISTED,
            DEVICE_RISK,
            BEHAVIOR_RISK,
            VOICE_RISK
        FROM {TSMT_PINDROP_H}
        WHERE ODS_BUSINESS_DT BETWEEN '{start_date}' AND '{end_date}'
    """)


def load_tsmt_tmx(spark, start_date, end_date):
    """Load TSMT_TMX_H filtered by date range (key columns only)."""
    return spark.sql(f"""
        SELECT 
            ODS_BUSINESS_DT,
            SESSION_ID,
            APPLICATION_NAME,
            BROWSER_STRING,
            DEVICE_ID,
            DEVICE_RESULT,
            DEVICE_SCORE,
            DIGITAL_ID,
            DNS_IP,
            EVENT_DATETIME,
            EVENT_TYPE,
            OS,
            OS_VERSION,
            POLICY,
            POLICY_SCORE,
            REASON_CODE,
            REQUEST_RESULT,
            REVIEW_STATUS,
            RISK_RATING,
            TMX_REASON_CODE,
            TMX_RISK_RATING,
            TRUE_IP,
            SUMMARY_RISK_SCORE,
            KAFKA_PROCESS_DT
        FROM {TSMT_TMX_H}
        WHERE ODS_BUSINESS_DT BETWEEN '{start_date}' AND '{end_date}'
    """)


# =============================================================================
# SECTION 3: DM_CIAMOP TABLE LOADERS
# =============================================================================

def load_login_and_passkey_stats(spark, start_date, end_date):
    """Load login_and_passkey_stats from DM_CIAMOP."""
    return spark.sql(f"""
        SELECT 
            total_aggregator_logins,
            total_person_logins,
            mbank_biometric_logins,
            mbank_creds_logins,
            onepass_logins,
            passkey_logins,
            kafka_process_dt
        FROM {LOGIN_AND_PASSKEY_STATS}
        WHERE kafka_process_dt BETWEEN '{start_date}' AND '{end_date}'
    """)


def load_mfa_app(spark, start_date, end_date):
    """Load mfa_app from DM_CIAMOP."""
    return spark.sql(f"""
        SELECT 
            day,
            periodenddate,
            mfa_app,
            policy,
            review_status,
            events
        FROM {MFA_APP}
        WHERE day BETWEEN '{start_date}' AND '{end_date}'
    """)


def load_otp_stats_loop(spark, start_date, end_date):
    """Load otp_stats_loop from DM_CIAMOP."""
    return spark.sql(f"""
        SELECT 
            month_number,
            week_number,
            day_of_month,
            policy_id,
            application_id,
            emailcount,
            smscount,
            kafka_process_dt
        FROM {OTP_STATS_LOOP}
        WHERE kafka_process_dt BETWEEN '{start_date}' AND '{end_date}'
    """)


def load_passkey_enrolled_users(spark, start_date, end_date):
    """Load passkey_enrolled_users from DM_CIAMOP."""
    return spark.sql(f"""
        SELECT 
            user_id,
            kafka_process_dt
        FROM {PASSKEY_ENROLLED_USERS}
        WHERE kafka_process_dt BETWEEN '{start_date}' AND '{end_date}'
    """)


def load_passkey_enrollment_login(spark, start_date, end_date):
    """Load passkey_enrollment_login from DM_CIAMOP."""
    return spark.sql(f"""
        SELECT 
            passkey_enrollments,
            passkey_logins,
            unpw_logins,
            kafka_process_dt
        FROM {PASSKEY_ENROLLMENT_LOGIN}
        WHERE kafka_process_dt BETWEEN '{start_date}' AND '{end_date}'
    """)


def load_tmx_tsmt_review_otp(spark, start_date, end_date):
    """Load tmx_tsmt_review_otp from DM_CIAMOP — rich OTP & MFA data."""
    return spark.sql(f"""
        SELECT 
            periodenddate,
            day,
            application_id,
            policy_id,
            tmx_pol,
            tmx_event_type,
            tmx_review_status,
            tmx_count,
            session_type,
            otp_channel,
            otps_sent,
            otps_failed,
            otp_success,
            otp_abandoned,
            otp_type,
            mfa_app
        FROM {TMX_TSMT_REVIEW_OTP}
        WHERE day BETWEEN '{start_date}' AND '{end_date}'
    """)


# =============================================================================
# SECTION 4: CUSTOMER DIMENSION
# =============================================================================
# This reuses logic from your existing BuildCustAcctDF function.
# We only need a subset of columns for V1.

def build_customer_dimension(spark, business_date):
    """
    Build a customer dimension with RCIF_CUST_NBR, age banding, 
    digital customer check, gender, and geography.
    
    This is a simplified version of your BuildCustAcctDF.
    Adjust table references if your EIL schema differs.
    """
    customer_dim = spark.sql(f"""
        WITH IBN_DATA AS (
            SELECT
                CONCAT(DIGITAL_BANKING_MASTER_H.ONF_RELT_TIN,
                       DIGITAL_BANKING_MASTER_H.ONF_RELT_TB) AS IBN_CUST_INTERNET_BANKING_NBR
            FROM EIL.DIGITAL_BANKING_MASTER_H
            INNER JOIN EIL.M_INVOLVED_PARTY_H ON
                CONCAT(DIGITAL_BANKING_MASTER_H.ONF_RELT_TIN,
                       DIGITAL_BANKING_MASTER_H.ONF_RELT_TB) = M_INVOLVED_PARTY_H.CUST_INTERNET_BANKING_NBR
                AND DIGITAL_BANKING_MASTER_H.ODS_BUSINESS_DT = M_INVOLVED_PARTY_H.BUSINESS_DATE
            WHERE M_INVOLVED_PARTY_H.BUSINESS_DATE = '{business_date}'
        ),

        CUST_POP AS (
            SELECT
                IP.BUSINESS_DATE,
                IP.involved_party_id,
                IP.involved_party_name,
                IP.rcif_cust_nbr AS RCIF_CUST_NBR,
                IP.CUST_INTERNET_BANKING_NBR,
                IP.INVOLVED_PARTY_TYPE_CODE,
                
                -- Gender
                CASE
                    WHEN IP.GENDER_TYPE_CODE = 'F' THEN 'Female'
                    WHEN IP.GENDER_TYPE_CODE = 'M' THEN 'Male'
                    ELSE 'Unknown'
                END AS CUSTOMER_GENDER,
                
                IP.BIRTH_DATE,
                
                -- Customer Age
                ROUND(DATEDIFF(IP.BUSINESS_DATE, IP.BIRTH_DATE) / 365, 0) AS CUSTOMER_AGE,
                
                -- Over 40 Check
                CASE
                    WHEN ROUND(DATEDIFF(IP.BUSINESS_DATE, IP.BIRTH_DATE) / 365, 0) < 40 
                        THEN 'Under 40'
                    ELSE 'Over 40'
                END AS OVER_40_CHECK,
                
                -- Age Banding
                CASE
                    WHEN ROUND(DATEDIFF(IP.BUSINESS_DATE, IP.BIRTH_DATE) / 365, 0) <= 17 
                        THEN 'Less than 18'
                    WHEN ROUND(DATEDIFF(IP.BUSINESS_DATE, IP.BIRTH_DATE) / 365, 0) BETWEEN 18 AND 24 
                        THEN 'Between 18 and 24'
                    WHEN ROUND(DATEDIFF(IP.BUSINESS_DATE, IP.BIRTH_DATE) / 365, 0) BETWEEN 25 AND 34 
                        THEN 'Between 25 and 34'
                    WHEN ROUND(DATEDIFF(IP.BUSINESS_DATE, IP.BIRTH_DATE) / 365, 0) BETWEEN 35 AND 44 
                        THEN 'Between 35 and 44'
                    WHEN ROUND(DATEDIFF(IP.BUSINESS_DATE, IP.BIRTH_DATE) / 365, 0) BETWEEN 45 AND 54 
                        THEN 'Between 45 and 54'
                    WHEN ROUND(DATEDIFF(IP.BUSINESS_DATE, IP.BIRTH_DATE) / 365, 0) BETWEEN 55 AND 64 
                        THEN 'Between 55 and 64'
                    WHEN ROUND(DATEDIFF(IP.BUSINESS_DATE, IP.BIRTH_DATE) / 365, 0) >= 65 
                        THEN '65+'
                    ELSE 'Unknown'
                END AS AGE_BANDING,
                
                -- Customer Generation
                CASE
                    WHEN IP.BIRTH_DATE BETWEEN '1900-01-01' AND '1924-12-31' 
                        THEN 'GI Generation (1900-1924)'
                    WHEN IP.BIRTH_DATE BETWEEN '1925-01-01' AND '1945-12-31' 
                        THEN 'Traditionalist (1925-1945)'
                    WHEN IP.BIRTH_DATE BETWEEN '1946-01-01' AND '1964-12-31' 
                        THEN 'Baby Boomer (1946-1964)'
                    WHEN IP.BIRTH_DATE BETWEEN '1965-01-01' AND '1980-12-31' 
                        THEN 'Gen X (1965-1980)'
                    WHEN IP.BIRTH_DATE BETWEEN '1981-01-01' AND '1996-12-31' 
                        THEN 'Millennial (1981-1996)'
                    WHEN IP.BIRTH_DATE >= '1997-01-01' 
                        THEN 'Centennial (1997-Current)'
                    ELSE 'Unknown'
                END AS CUSTOMER_GENERATION,
                
                -- Geography
                CASE
                    WHEN AD.CITY_NAME LIKE '%%HOLD STATEMENT%%' THEN 'UNKNOWN'
                    ELSE AD.CITY_NAME
                END AS CITY,
                CASE
                    WHEN AD.STATE_NAME LIKE '%%N/A%%' THEN 'UNKNOWN'
                    ELSE AD.STATE_NAME
                END AS STATE,
                CASE
                    WHEN AD.COUNTRY_NAME LIKE '%%N/A%%' THEN 'UNKNOWN'
                    ELSE AD.COUNTRY_NAME
                END AS COUNTRY

            FROM EIL.M_INVOLVED_PARTY_H IP
            INNER JOIN EIL.D_INVOLVED_PARTY_ADDRESS_H AD ON
                IP.SOURCE_SYSTEM_CODE = AD.SOURCE_SYSTEM_CODE AND
                IP.INVOLVED_PARTY_ID = AD.INVOLVED_PARTY_ID
            WHERE
                IP.BUSINESS_DATE = '{business_date}'
                AND IP.DECEASED_DATE IS NULL
                AND IP.SOURCE_SYSTEM_CODE = 'CF'
        )

        SELECT DISTINCT
            cp.RCIF_CUST_NBR,
            cp.CUST_INTERNET_BANKING_NBR,
            CASE
                WHEN ibn.IBN_CUST_INTERNET_BANKING_NBR IS NOT NULL 
                    THEN 'Digital Customer'
                ELSE 'Non-Digital Customer'
            END AS DIGITAL_CUSTOMER_CHECK,
            cp.CUSTOMER_GENDER,
            cp.CUSTOMER_AGE,
            cp.OVER_40_CHECK,
            cp.AGE_BANDING,
            cp.CUSTOMER_GENERATION,
            cp.CITY,
            cp.STATE,
            cp.COUNTRY
        FROM CUST_POP cp
        LEFT JOIN IBN_DATA ibn ON
            cp.CUST_INTERNET_BANKING_NBR = ibn.IBN_CUST_INTERNET_BANKING_NBR
    """)
    
    return customer_dim


# =============================================================================
# SECTION 5: V1 REQUIREMENT QUERIES
# =============================================================================

# ---------------------------------------------------------------------------
# REQUIREMENT 1: Channel Type
# Source: TSMT_DEVICE_H, TSMT_ENRICHMENT_H (APPLICATION_NAME from TMX)
# ---------------------------------------------------------------------------
def v1_req01_channel_type(df_enrichment, df_tmx):
    """
    Extract channel type from TSMT_ENRICHMENT_H (Application_Name / Data(channel))
    and TSMT_TMX_H.APPLICATION_NAME.
    """
    # From enrichment: parse channel from DATA JSON
    enr_channel = parse_tsmt_data(
        df_enrichment,
        {"channel": "$.channel", "application_name": "$.Application_Name"}
    ).select(
        col("ODS_BUSINESS_DT").alias("business_date"),
        col("SESSION_ID"),
        coalesce(col("application_name"), col("APPLICATION_ID")).alias("channel_type"),
        col("channel").alias("channel_code")
    ).distinct()

    # From TMX: APPLICATION_NAME is a direct column
    tmx_channel = df_tmx.select(
        col("ODS_BUSINESS_DT").alias("business_date"),
        col("SESSION_ID"),
        col("APPLICATION_NAME").alias("channel_type")
    ).distinct()

    # Combine — enrichment is primary, TMX fills gaps
    channel_type = enr_channel.join(
        tmx_channel,
        on=["business_date", "SESSION_ID"],
        how="left"
    ).withColumn(
        "channel_type_final",
        coalesce(enr_channel["channel_type"], tmx_channel["channel_type"])
    )

    return channel_type


# ---------------------------------------------------------------------------
# REQUIREMENT 2: Channel ID / Code / ChannelSessionID
# Source: TSMT_ENRICHMENT_H (ChannelSessionID from DATA JSON)
# ---------------------------------------------------------------------------
def v1_req02_channel_session_id(df_enrichment):
    """
    Extract ChannelSessionID from TSMT_ENRICHMENT_H DATA JSON.
    """
    return parse_tsmt_data(
        df_enrichment,
        {"channel_session_id": "$.ChannelSessionID"}
    ).select(
        col("ODS_BUSINESS_DT").alias("business_date"),
        col("SESSION_ID"),
        col("APPLICATION_ID"),
        col("channel_session_id")
    ).distinct()


# ---------------------------------------------------------------------------
# REQUIREMENT 3: Device & Platform
# Source: TSMT_DEVICE_H (DEVICE_OS_TYPE), TSMT_TMX_H (BROWSER_STRING),
#         TSMT_ENRICHMENT_H (DEVICE_OS_TYPE)
# ---------------------------------------------------------------------------
def v1_req03_device_platform(df_device, df_tmx, df_enrichment):
    """
    Combine device/platform info from DEVICE_H, TMX, and ENRICHMENT.
    """
    # From DEVICE_H
    device_info = df_device.select(
        col("ODS_BUSINESS_DT").alias("business_date"),
        col("SESSION_ID"),
        col("DEVICE_ID"),
        col("DEVICE_OS_TYPE").alias("device_os_type"),
        col("DEVICE_MODEL").alias("device_model")
    ).distinct()

    # From TMX — browser string gives us more platform detail
    tmx_platform = df_tmx.select(
        col("ODS_BUSINESS_DT").alias("business_date"),
        col("SESSION_ID"),
        col("BROWSER_STRING"),
        col("OS").alias("tmx_os"),
        col("OS_VERSION").alias("tmx_os_version")
    ).distinct()

    # From ENRICHMENT — DEVICE_OS_TYPE
    enr_platform = df_enrichment.select(
        col("ODS_BUSINESS_DT").alias("business_date"),
        col("SESSION_ID"),
        col("DEVICE_OS_TYPE").alias("enr_device_os_type")
    ).distinct()

    # Combine all three sources
    device_platform = device_info.join(
        tmx_platform, on=["business_date", "SESSION_ID"], how="left"
    ).join(
        enr_platform, on=["business_date", "SESSION_ID"], how="left"
    ).withColumn(
        "platform",
        coalesce(col("device_os_type"), col("enr_device_os_type"), col("tmx_os"))
    ).withColumn(
        "device_category",
        when(upper(col("platform")).isin("IOS", "ANDROID"), "Mobile")
        .when(upper(col("platform")).isin("WINDOWS", "MACOS", "LINUX"), "Desktop")
        .when(upper(col("platform")).contains("BROWSER"), "Web")
        .otherwise("Other")
    )

    return device_platform


# ---------------------------------------------------------------------------
# REQUIREMENT 4: Customer Age Groups — Digitally Enrolled vs Active Mobile
# Source: Customer dimension + TSMT_TMX_H (DATA → RCIF_CUST_NBR)
# ---------------------------------------------------------------------------
def v1_req04_customer_age_groups(df_tmx, df_device, customer_dim):
    """
    Count digitally enrolled customers vs active mobile app users 
    per age group.
    """
    # Parse RCIF from TMX DATA column to identify active users
    tmx_with_rcif = parse_tsmt_data(
        df_tmx,
        {"rcif_cust_nbr": "$.RCIF_CUST_NBR"}
    ).filter(col("rcif_cust_nbr").isNotNull())

    # Active mobile users: sessions from mobile devices
    active_mobile = df_device.filter(
        upper(col("DEVICE_OS_TYPE")).isin("IOS", "ANDROID")
    ).select("SESSION_ID", "USER_ID").distinct()

    # Join TMX users to customer dimension for age banding
    tmx_customers = tmx_with_rcif.select(
        col("rcif_cust_nbr").alias("RCIF_CUST_NBR"),
        col("SESSION_ID")
    ).distinct()

    # All digitally active users by age group
    digitally_enrolled_by_age = tmx_customers.join(
        customer_dim, on="RCIF_CUST_NBR", how="inner"
    ).groupBy("AGE_BANDING", "CUSTOMER_GENERATION", "DIGITAL_CUSTOMER_CHECK") \
     .agg(
         countDistinct("RCIF_CUST_NBR").alias("total_digital_customers"),
         countDistinct("SESSION_ID").alias("total_sessions")
     )

    # Mobile-active users by age group
    mobile_sessions = tmx_customers.join(
        active_mobile, on="SESSION_ID", how="inner"
    )
    mobile_by_age = mobile_sessions.join(
        customer_dim, on="RCIF_CUST_NBR", how="inner"
    ).groupBy("AGE_BANDING", "CUSTOMER_GENERATION") \
     .agg(
         countDistinct("RCIF_CUST_NBR").alias("active_mobile_users")
     )

    # Combine
    age_group_summary = digitally_enrolled_by_age.join(
        mobile_by_age, on=["AGE_BANDING", "CUSTOMER_GENERATION"], how="left"
    ).fillna(0, subset=["active_mobile_users"])

    return age_group_summary


# ---------------------------------------------------------------------------
# REQUIREMENTS 5-7: OTP Usage (SMS / Voice / Email) by Age Group & Channel
# Source: PRIMARY — DM_CIAMOP.tmx_tsmt_review_otp (otp_channel, otp_type,
#         otps_sent, otp_success, otps_failed, otp_abandoned)
#         SECONDARY — TSMT_ENRICHMENT_H (OTPCHANNEL from DATA JSON)
#         DM_CIAMOP.otp_stats_loop for email/sms aggregate counts
#         Customer dimension for age banding
# ---------------------------------------------------------------------------
def v1_req05_06_07_otp_by_type_and_age(
    df_enrichment, df_session, df_otp_stats, df_tmx_review_otp, customer_dim
):
    """
    Break down OTP usage by type (SMS, Voice, Email), age group, and channel.
    
    Approach:
      - Primary: DM_CIAMOP.tmx_tsmt_review_otp (has otp_channel, otp_type,
        otps_sent, otp_success, otps_failed, otp_abandoned directly)
      - Secondary: Parse OTPCHANNEL from TSMT_ENRICHMENT_H DATA JSON
        and join to customer dim for age-group level detail
      - Tertiary: DM_CIAMOP.otp_stats_loop for email/sms aggregate counts
    """

    # ==================================================================
    # A) PRIMARY: tmx_tsmt_review_otp — pre-aggregated OTP metrics
    #    This gives us channel-level OTP stats with success/failure rates
    # ==================================================================
    
    # Classify OTP channel into standard categories
    review_otp = df_tmx_review_otp.withColumn(
        "OTP_CATEGORY",
        when(upper(col("otp_channel")).contains("SMS"), "SMS")
        .when(upper(col("otp_channel")).contains("VOICE"), "Voice")
        .when(upper(col("otp_channel")).contains("EMAIL"), "Email")
        .when(upper(col("otp_type")).contains("SMS"), "SMS")
        .when(upper(col("otp_type")).contains("VOICE"), "Voice")
        .when(upper(col("otp_type")).contains("EMAIL"), "Email")
        .otherwise("Other")
    )

    # OTP summary by type, channel (application_id), and day
    otp_by_type_channel = review_otp.groupBy(
        col("day").alias("business_date"),
        "OTP_CATEGORY",
        "otp_channel",
        "otp_type",
        "application_id",
        "session_type",
        "mfa_app"
    ).agg(
        _sum("otps_sent").alias("total_otps_sent"),
        _sum("otp_success").alias("total_otp_success"),
        _sum("otps_failed").alias("total_otps_failed"),
        _sum("otp_abandoned").alias("total_otp_abandoned"),
        _sum("tmx_count").alias("total_tmx_events")
    ).withColumn(
        "otp_success_rate",
        _round(col("total_otp_success") / col("total_otps_sent") * 100, 2)
    )

    # SMS OTP summary
    sms_otp_summary = otp_by_type_channel.filter(col("OTP_CATEGORY") == "SMS")

    # Voice OTP summary
    voice_otp_summary = otp_by_type_channel.filter(col("OTP_CATEGORY") == "Voice")

    # Email OTP summary (by channel/application)
    email_otp_summary = otp_by_type_channel.filter(col("OTP_CATEGORY") == "Email")

    # ==================================================================
    # B) SECONDARY: TSMT_ENRICHMENT_H — user-level OTP with RCIF linkage
    #    This lets us join to customer_dim for age group breakdowns
    # ==================================================================
    enr_otp = parse_tsmt_data(
        df_enrichment,
        {
            "otp_channel": "$.OTPCHANNEL",
            "rcif_cust_nbr": "$.RCIF_CUST_NBR"
        }
    ).filter(
        col("otp_channel").isNotNull()
    ).select(
        col("ODS_BUSINESS_DT").alias("business_date"),
        col("SESSION_ID"),
        col("APPLICATION_ID"),
        col("POLICY_ID"),
        col("rcif_cust_nbr").alias("RCIF_CUST_NBR"),
        upper(trim(col("otp_channel"))).alias("OTP_TYPE")
    )

    # Classify OTP type
    enr_otp = enr_otp.withColumn(
        "OTP_CATEGORY",
        when(col("OTP_TYPE").contains("SMS"), "SMS")
        .when(col("OTP_TYPE").contains("VOICE"), "Voice")
        .when(col("OTP_TYPE").contains("EMAIL"), "Email")
        .otherwise("Other")
    )

    # Join to customer dimension for age-based breakdowns
    otp_with_age = enr_otp.join(
        customer_dim, on="RCIF_CUST_NBR", how="inner"
    )

    # Req #5: SMS OTP by age group
    sms_otp_by_age = otp_with_age.filter(
        col("OTP_CATEGORY") == "SMS"
    ).groupBy("AGE_BANDING", "CUSTOMER_GENERATION") \
     .agg(
         countDistinct("RCIF_CUST_NBR").alias("sms_otp_users"),
         count("SESSION_ID").alias("sms_otp_events")
     )

    # Req #6: Voice OTP by age group
    voice_otp_by_age = otp_with_age.filter(
        col("OTP_CATEGORY") == "Voice"
    ).groupBy("AGE_BANDING", "CUSTOMER_GENERATION") \
     .agg(
         countDistinct("RCIF_CUST_NBR").alias("voice_otp_users"),
         count("SESSION_ID").alias("voice_otp_events")
     )

    # Req #7: Email OTP by age group AND channel
    email_otp_by_age_channel = otp_with_age.filter(
        col("OTP_CATEGORY") == "Email"
    ).groupBy("AGE_BANDING", "CUSTOMER_GENERATION", "APPLICATION_ID") \
     .agg(
         countDistinct("RCIF_CUST_NBR").alias("email_otp_users"),
         count("SESSION_ID").alias("email_otp_events")
     )

    # ==================================================================
    # C) TERTIARY: DM_CIAMOP.otp_stats_loop — simple sms/email totals
    # ==================================================================
    otp_agg = df_otp_stats.groupBy("kafka_process_dt", "policy_id", "application_id") \
        .agg(
            _sum("smscount").alias("total_sms_count"),
            _sum("emailcount").alias("total_email_count")
        )

    return {
        "sms_otp_by_age": sms_otp_by_age,
        "voice_otp_by_age": voice_otp_by_age,
        "email_otp_by_age_channel": email_otp_by_age_channel,
        "otp_aggregates": otp_agg,
        "otp_by_type_channel": otp_by_type_channel,       # NEW — from tmx_tsmt_review_otp
        "sms_otp_summary": sms_otp_summary,                # NEW — SMS detail
        "voice_otp_summary": voice_otp_summary,             # NEW — Voice detail
        "email_otp_summary": email_otp_summary              # NEW — Email detail
    }


# ---------------------------------------------------------------------------
# REQUIREMENT 8: AUTH Metrics — % of UID/PWD
# Source: TSMT_SESSION_H (AUTHENTICATION_METHOD), TSMT_AUTHENTICATION_H (URI),
#         DM_CIAMOP.passkey_enrollment_login (unpw_logins)
# ---------------------------------------------------------------------------
def v1_req08_uid_pwd_metrics(df_session, df_authentication, df_passkey_enroll_login):
    """
    Calculate the percentage of UID/PWD authentication vs other methods.
    """
    # From TSMT_SESSION_H: count by authentication method
    session_auth_methods = df_session.filter(
        col("AUTHENTICATION_METHOD").isNotNull()
    ).groupBy("ODS_BUSINESS_DT", "AUTHENTICATION_METHOD") \
     .agg(
         count("SESSION_ID").alias("session_count"),
         countDistinct("USER_ID").alias("unique_users")
     )

    # From TSMT_AUTHENTICATION_H: URI-based UID detection
    auth_uri = df_authentication.withColumn(
        "is_uid_pwd",
        when(
            (upper(col("URI")).contains("UID")) | 
            (upper(col("AUTHENTICATION_METHOD")).isin(
                "PASSWORD", "UID_PWD", "USERNAME_PASSWORD", "LDAP"
            )),
            lit(1)
        ).otherwise(lit(0))
    )

    uid_pwd_daily = auth_uri.groupBy("ODS_BUSINESS_DT").agg(
        _sum("is_uid_pwd").alias("uid_pwd_count"),
        count("*").alias("total_auth_count")
    ).withColumn(
        "uid_pwd_pct",
        _round(col("uid_pwd_count") / col("total_auth_count") * 100, 2)
    )

    # From DM_CIAMOP: daily unpw_logins
    dm_unpw = df_passkey_enroll_login.select(
        col("kafka_process_dt").alias("business_date"),
        col("unpw_logins")
    )

    return {
        "session_auth_methods": session_auth_methods,
        "uid_pwd_daily": uid_pwd_daily,
        "dm_unpw_logins": dm_unpw
    }


# ---------------------------------------------------------------------------
# REQUIREMENT 9: Auth Events — Logout / Token Refresh / Pwd Resets
# Source: TSMT_AUTHENTICATION_H (ACTION), TSMT_ENRICHMENT_H (POLICY_ID)
# ---------------------------------------------------------------------------
def v1_req09_auth_events(df_authentication, df_enrichment):
    """
    Count auth events: Login, Logout, Token Refresh, Password Reset.
    """
    # Classify events based on ACTION column
    auth_events = df_authentication.withColumn(
        "event_type",
        when(upper(col("ACTION")).contains("LOGIN"), "Login")
        .when(upper(col("ACTION")).contains("LOGOUT"), "Logout")
        .when(
            (upper(col("ACTION")).contains("TOKEN")) | 
            (upper(col("ACTION")).contains("REFRESH")),
            "Token Refresh"
        )
        .when(
            (upper(col("ACTION")).contains("PASSWORD")) | 
            (upper(col("ACTION")).contains("PWD")) |
            (upper(col("ACTION")).contains("RESET")),
            "Password Reset"
        )
        .otherwise("Other")
    )

    # Daily event summary
    event_summary = auth_events.groupBy(
        col("ODS_BUSINESS_DT").alias("business_date"),
        "event_type"
    ).agg(
        count("*").alias("event_count"),
        countDistinct("SESSION_ID").alias("unique_sessions"),
        countDistinct("USER_ID").alias("unique_users")
    )

    # Enrichment-based: events by POLICY_ID
    enr_events = df_enrichment.groupBy(
        col("ODS_BUSINESS_DT").alias("business_date"),
        "POLICY_ID"
    ).agg(
        count("*").alias("event_count")
    )

    return {
        "event_summary": event_summary,
        "events_by_policy": enr_events
    }


# ---------------------------------------------------------------------------
# REQUIREMENT 10: Timestamps — Event Time
# Source: TSMT_AUTHENTICATION_H, TSMT_ENRICHMENT_H
# ---------------------------------------------------------------------------
def v1_req10_timestamps(df_authentication, df_enrichment):
    """
    Extract event timestamps for time-series analysis in PBI.
    """
    # Authentication events with timestamps
    auth_timestamps = df_authentication.select(
        col("ODS_BUSINESS_DT").alias("business_date"),
        col("ODS_BUSINESS_TS").alias("event_timestamp"),
        col("SESSION_ID"),
        col("ACTION").alias("event_action"),
        col("AUTHENTICATION_METHOD"),
        col("APPLICATION_ID"),
        col("USER_ID")
    )

    # Enrichment events with timestamps
    enr_timestamps = df_enrichment.select(
        col("ODS_BUSINESS_DT").alias("business_date"),
        col("ODS_BUSINESS_TS").alias("event_timestamp"),
        col("SESSION_ID"),
        col("ACTION").alias("event_action"),
        col("APPLICATION_ID"),
        col("USER_ID")
    )

    # Union all timestamped events
    all_timestamps = auth_timestamps.unionByName(
        enr_timestamps, allowMissingColumns=True
    ).distinct()

    return all_timestamps


# ---------------------------------------------------------------------------
# REQUIREMENT 11: # Passkey Enrollments
# Source: DM_CIAMOP.passkey_enrolled_users, DM_CIAMOP.passkey_enrollment_login,
#         TSMT_DEVICE_H, TSMT_TMX_H
# ---------------------------------------------------------------------------
def v1_req11_passkey_enrollments(df_passkey_users, df_passkey_enroll_login, df_device):
    """
    Count passkey enrollments.
    """
    # From DM_CIAMOP: total enrolled users over time
    enrolled_users_daily = df_passkey_users.groupBy("kafka_process_dt") \
        .agg(countDistinct("user_id").alias("passkey_enrolled_user_count"))

    # From DM_CIAMOP: passkey_enrollment_login has daily enrollment count
    enrollment_daily = df_passkey_enroll_login.select(
        col("kafka_process_dt").alias("business_date"),
        col("passkey_enrollments")
    )

    # From TSMT_DEVICE_H: detect passkey enrollment events
    # Look for passkey-related actions in the device table
    device_passkey = df_device.filter(
        (upper(col("ACTION")).contains("PASSKEY")) |
        (upper(col("ACTION2")).contains("PASSKEY")) |
        (upper(col("CATEGORY")).contains("PASSKEY"))
    ).groupBy(col("ODS_BUSINESS_DT").alias("business_date")) \
     .agg(
         count("*").alias("device_passkey_events"),
         countDistinct("USER_ID").alias("device_passkey_users")
     )

    return {
        "enrolled_users_daily": enrolled_users_daily,
        "enrollment_daily": enrollment_daily,
        "device_passkey_events": device_passkey
    }


# ---------------------------------------------------------------------------
# REQUIREMENT 12: # Passkey Authentications
# Source: DM_CIAMOP.login_and_passkey_stats, DM_CIAMOP.passkey_enrollment_login
# ---------------------------------------------------------------------------
def v1_req12_passkey_authentications(df_login_passkey_stats, df_passkey_enroll_login):
    """
    Count passkey-based authentications (logins).
    """
    # From login_and_passkey_stats
    passkey_auth_daily = df_login_passkey_stats.select(
        col("kafka_process_dt").alias("business_date"),
        col("passkey_logins").alias("passkey_logins_from_stats")
    )

    # From passkey_enrollment_login
    passkey_auth_enroll = df_passkey_enroll_login.select(
        col("kafka_process_dt").alias("business_date"),
        col("passkey_logins").alias("passkey_logins_from_enrollment")
    )

    # Combine both sources
    passkey_auth = passkey_auth_daily.join(
        passkey_auth_enroll, on="business_date", how="full_outer"
    ).withColumn(
        "passkey_authentications",
        coalesce(col("passkey_logins_from_stats"), col("passkey_logins_from_enrollment"))
    )

    return passkey_auth


# ---------------------------------------------------------------------------
# REQUIREMENT 13: % of Passwordless Auth
# Source: DM_CIAMOP.login_and_passkey_stats, TSMT_DEVICE_H
# ---------------------------------------------------------------------------
def v1_req13_passwordless_pct(df_login_passkey_stats, df_passkey_enroll_login):
    """
    Calculate percentage of passwordless authentication.
    Passwordless = passkey_logins + mbank_biometric_logins + onepass_logins
    Total logins = total_person_logins
    """
    passwordless = df_login_passkey_stats.withColumn(
        "passwordless_logins",
        col("passkey_logins") + col("mbank_biometric_logins") + col("onepass_logins")
    ).withColumn(
        "passwordless_pct",
        _round(
            col("passwordless_logins") / col("total_person_logins") * 100, 2
        )
    ).select(
        col("kafka_process_dt").alias("business_date"),
        col("total_person_logins"),
        col("passwordless_logins"),
        col("passwordless_pct"),
        col("passkey_logins"),
        col("mbank_biometric_logins"),
        col("onepass_logins"),
        col("mbank_creds_logins").alias("credential_logins")
    )

    # Also compute from passkey_enrollment_login
    pwd_from_enroll = df_passkey_enroll_login.withColumn(
        "total_logins",
        col("passkey_logins") + col("unpw_logins")
    ).withColumn(
        "passwordless_pct_enroll",
        _round(col("passkey_logins") / col("total_logins") * 100, 2)
    ).select(
        col("kafka_process_dt").alias("business_date"),
        col("passkey_logins").alias("pk_logins"),
        col("unpw_logins"),
        col("total_logins"),
        col("passwordless_pct_enroll")
    )

    return {
        "passwordless_from_stats": passwordless,
        "passwordless_from_enrollment": pwd_from_enroll
    }


# ---------------------------------------------------------------------------
# REQUIREMENT 14: FI Disabled vs Enabled
# Source: TSMT_DEVICE_H, DM_CIAMOP.login_and_passkey_stats
# ---------------------------------------------------------------------------
def v1_req14_fi_disabled_enabled(df_device):
    """
    Determine FI (Financial Institution) disabled vs enabled status.
    Parse from TSMT_DEVICE_H DATA JSON or ACTION columns.
    """
    # Parse relevant fields from DATA JSON
    device_fi = parse_tsmt_data(
        df_device,
        {
            "fi_status": "$.FI_STATUS",
            "fi_enabled": "$.FI_ENABLED",
            "passkey_enabled": "$.PASSKEY_ENABLED"
        }
    )

    fi_summary = device_fi.withColumn(
        "fi_category",
        when(
            (upper(col("fi_status")).isin("DISABLED", "FALSE", "0")) |
            (upper(col("fi_enabled")).isin("FALSE", "0", "N")),
            "FI Disabled"
        ).when(
            (upper(col("fi_status")).isin("ENABLED", "TRUE", "1")) |
            (upper(col("fi_enabled")).isin("TRUE", "1", "Y")),
            "FI Enabled"
        ).otherwise("Unknown")
    ).groupBy(
        col("ODS_BUSINESS_DT").alias("business_date"),
        "fi_category"
    ).agg(
        count("*").alias("event_count"),
        countDistinct("USER_ID").alias("unique_users")
    )

    return fi_summary


# ---------------------------------------------------------------------------
# REQUIREMENT 15: # SSO, # Aggregators, and Type of MFA
# Source: TSMT_AUTHENTICATION_H, DM_CIAMOP.mfa_app,
#         DM_CIAMOP.login_and_passkey_stats, DM_CIAMOP.tmx_tsmt_review_otp
# ---------------------------------------------------------------------------
def v1_req15_sso_aggregator_mfa(
    df_authentication, df_mfa_app, df_login_passkey_stats, df_tmx_review_otp
):
    """
    Count SSO logins, aggregator logins, and MFA type breakdown.
    Now enriched with tmx_tsmt_review_otp for mfa_app cross-reference.
    """
    # --- Aggregator logins from DM_CIAMOP ---
    aggregator_daily = df_login_passkey_stats.select(
        col("kafka_process_dt").alias("business_date"),
        col("total_aggregator_logins"),
        col("total_person_logins")
    )

    # --- MFA type breakdown from DM_CIAMOP.mfa_app ---
    mfa_breakdown = df_mfa_app.groupBy("day", "mfa_app", "policy") \
        .agg(
            _sum("events").alias("total_mfa_events")
        ).withColumnRenamed("day", "business_date")

    # --- MFA from tmx_tsmt_review_otp (richer: includes otp_channel context) ---
    mfa_from_review = df_tmx_review_otp.filter(
        col("mfa_app").isNotNull()
    ).groupBy(
        col("day").alias("business_date"),
        "mfa_app",
        "otp_channel",
        "session_type",
        "application_id"
    ).agg(
        _sum("otps_sent").alias("mfa_otps_sent"),
        _sum("otp_success").alias("mfa_otp_success"),
        _sum("otps_failed").alias("mfa_otps_failed"),
        _sum("tmx_count").alias("mfa_tmx_count")
    )

    # --- SSO from TSMT_AUTHENTICATION_H ---
    sso_events = df_authentication.filter(
        (upper(col("SESSION_TYPE")).contains("SSO")) |
        (upper(col("CATEGORY")).contains("SSO")) |
        (upper(col("ACTION")).contains("SSO"))
    ).groupBy(
        col("ODS_BUSINESS_DT").alias("business_date")
    ).agg(
        count("*").alias("sso_event_count"),
        countDistinct("SESSION_ID").alias("sso_sessions"),
        countDistinct("USER_ID").alias("sso_users")
    )

    return {
        "aggregator_daily": aggregator_daily,
        "mfa_breakdown": mfa_breakdown,
        "mfa_from_review_otp": mfa_from_review,   # NEW — enriched MFA detail
        "sso_events": sso_events
    }


# =============================================================================
# SECTION 6: MAIN ORCHESTRATOR
# =============================================================================

def build_v1_dashboard(spark, business_date, start_date, end_date):
    """
    Main entry point: loads all tables, runs all V1 requirement queries,
    and returns a dictionary of DataFrames ready for PBI export.
    
    Args:
        spark: SparkSession
        business_date: str, e.g. '2025-03-01' (for customer dimension snapshot)
        start_date: str, e.g. '2025-01-01' (V1 date range start)
        end_date: str, e.g. '2025-03-31' (V1 date range end)
    
    Returns:
        dict of {metric_name: DataFrame}
    """
    print(f"=== V1 Auth Dashboard Build ===")
    print(f"Business Date:  {business_date}")
    print(f"Date Range:     {start_date} to {end_date}")

    # ------------------------------------------
    # STEP 1: Load all base tables
    # ------------------------------------------
    print("Loading TSMT tables...")
    df_auth       = load_tsmt_authentication(spark, start_date, end_date)
    df_device     = load_tsmt_device(spark, start_date, end_date)
    df_enrichment = load_tsmt_enrichment(spark, start_date, end_date)
    df_session    = load_tsmt_session(spark, start_date, end_date)
    df_pindrop    = load_tsmt_pindrop(spark, start_date, end_date)
    df_tmx        = load_tsmt_tmx(spark, start_date, end_date)

    print("Loading DM_CIAMOP tables...")
    df_login_passkey    = load_login_and_passkey_stats(spark, start_date, end_date)
    df_mfa              = load_mfa_app(spark, start_date, end_date)
    df_otp_stats        = load_otp_stats_loop(spark, start_date, end_date)
    df_passkey_users    = load_passkey_enrolled_users(spark, start_date, end_date)
    df_passkey_enroll   = load_passkey_enrollment_login(spark, start_date, end_date)
    df_tmx_review_otp   = load_tmx_tsmt_review_otp(spark, start_date, end_date)

    # ------------------------------------------
    # STEP 2: Build Customer Dimension
    # ------------------------------------------
    print("Building customer dimension...")
    customer_dim = build_customer_dimension(spark, business_date)
    customer_dim.cache()
    print(f"Customer dimension row count: {customer_dim.count()}")

    # ------------------------------------------
    # STEP 3: Run all V1 requirement queries
    # ------------------------------------------
    results = {}

    print("Req #1: Channel Type...")
    results["req01_channel_type"] = v1_req01_channel_type(df_enrichment, df_tmx)

    print("Req #2: Channel Session ID...")
    results["req02_channel_session_id"] = v1_req02_channel_session_id(df_enrichment)

    print("Req #3: Device & Platform...")
    results["req03_device_platform"] = v1_req03_device_platform(
        df_device, df_tmx, df_enrichment
    )

    print("Req #4: Customer Age Groups...")
    results["req04_customer_age_groups"] = v1_req04_customer_age_groups(
        df_tmx, df_device, customer_dim
    )

    print("Reqs #5-7: OTP by Type and Age...")
    otp_results = v1_req05_06_07_otp_by_type_and_age(
        df_enrichment, df_session, df_otp_stats, df_tmx_review_otp, customer_dim
    )
    results["req05_sms_otp_by_age"]          = otp_results["sms_otp_by_age"]
    results["req06_voice_otp_by_age"]        = otp_results["voice_otp_by_age"]
    results["req07_email_otp_by_age_channel"] = otp_results["email_otp_by_age_channel"]
    results["req_otp_aggregates"]            = otp_results["otp_aggregates"]
    results["req_otp_by_type_channel"]       = otp_results["otp_by_type_channel"]
    results["req_sms_otp_summary"]           = otp_results["sms_otp_summary"]
    results["req_voice_otp_summary"]         = otp_results["voice_otp_summary"]
    results["req_email_otp_summary"]         = otp_results["email_otp_summary"]

    print("Req #8: UID/PWD Metrics...")
    uid_pwd = v1_req08_uid_pwd_metrics(df_session, df_auth, df_passkey_enroll)
    results["req08_session_auth_methods"] = uid_pwd["session_auth_methods"]
    results["req08_uid_pwd_daily"]        = uid_pwd["uid_pwd_daily"]
    results["req08_dm_unpw_logins"]       = uid_pwd["dm_unpw_logins"]

    print("Req #9: Auth Events...")
    events = v1_req09_auth_events(df_auth, df_enrichment)
    results["req09_event_summary"]    = events["event_summary"]
    results["req09_events_by_policy"] = events["events_by_policy"]

    print("Req #10: Timestamps...")
    results["req10_timestamps"] = v1_req10_timestamps(df_auth, df_enrichment)

    print("Req #11: Passkey Enrollments...")
    pk_enroll = v1_req11_passkey_enrollments(
        df_passkey_users, df_passkey_enroll, df_device
    )
    results["req11_enrolled_users_daily"]  = pk_enroll["enrolled_users_daily"]
    results["req11_enrollment_daily"]      = pk_enroll["enrollment_daily"]
    results["req11_device_passkey_events"] = pk_enroll["device_passkey_events"]

    print("Req #12: Passkey Authentications...")
    results["req12_passkey_auth"] = v1_req12_passkey_authentications(
        df_login_passkey, df_passkey_enroll
    )

    print("Req #13: Passwordless %...")
    pwdless = v1_req13_passwordless_pct(df_login_passkey, df_passkey_enroll)
    results["req13_passwordless_from_stats"]      = pwdless["passwordless_from_stats"]
    results["req13_passwordless_from_enrollment"] = pwdless["passwordless_from_enrollment"]

    print("Req #14: FI Disabled vs Enabled...")
    results["req14_fi_disabled_enabled"] = v1_req14_fi_disabled_enabled(df_device)

    print("Req #15: SSO / Aggregators / MFA...")
    sso_mfa = v1_req15_sso_aggregator_mfa(
        df_auth, df_mfa, df_login_passkey, df_tmx_review_otp
    )
    results["req15_aggregator_daily"]    = sso_mfa["aggregator_daily"]
    results["req15_mfa_breakdown"]       = sso_mfa["mfa_breakdown"]
    results["req15_mfa_from_review_otp"] = sso_mfa["mfa_from_review_otp"]
    results["req15_sso_events"]          = sso_mfa["sso_events"]

    # Include customer dimension for PBI
    results["customer_dimension"] = customer_dim

    print("=== V1 Build Complete ===")
    return results


# =============================================================================
# SECTION 7: EXPORT HELPERS (for PBI ingestion)
# =============================================================================

def save_results_to_parquet(results, output_path):
    """
    Save all result DataFrames as Parquet files for PBI DirectQuery/Import.
    """
    for name, df in results.items():
        path = f"{output_path}/{name}"
        print(f"Saving {name} to {path}...")
        df.write.mode("overwrite").parquet(path)
    print("All results saved.")


def save_results_to_hive(results, database="dm_auth_dashboard"):
    """
    Save all result DataFrames as Hive tables for PBI connectivity.
    """
    for name, df in results.items():
        table_name = f"{database}.{name}"
        print(f"Saving {name} to {table_name}...")
        df.write.mode("overwrite").saveAsTable(table_name)
    print("All results saved to Hive.")


# =============================================================================
# SECTION 8: ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("V1_Enterprise_Auth_Dashboard") \
        .enableHiveSupport() \
        .getOrCreate()

    # -------------------------------------------------------
    # CONFIGURE YOUR DATE PARAMETERS HERE
    # -------------------------------------------------------
    BUSINESS_DATE = "2025-03-01"   # Customer dimension snapshot date
    START_DATE    = "2025-01-01"   # V1 reporting period start
    END_DATE      = "2025-03-31"   # V1 reporting period end
    OUTPUT_PATH   = "/user/hive/warehouse/dm_auth_dashboard"

    # Build all V1 metrics
    results = build_v1_dashboard(spark, BUSINESS_DATE, START_DATE, END_DATE)

    # Save results — choose one:
    # Option A: Parquet files
    save_results_to_parquet(results, OUTPUT_PATH)
    
    # Option B: Hive tables (uncomment to use)
    # save_results_to_hive(results)

    spark.stop()

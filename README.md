from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    when,
    lit,
    count,
    countDistinct,
    sum as _sum,
    round as _round,
    to_date,
    trim,
    coalesce,
    datediff,
)

# -------------------------------------------------------------------
# TSMT tables (Hive data lake)
# -------------------------------------------------------------------
TSMT_DB = "SL1_TSMT"

TSMT_AUTHENTICATION_H = f"{TSMT_DB}.TSMT_AUTHENTICATION_H"
TSMT_DEVICE_H         = f"{TSMT_DB}.TSMT_DEVICE_H"
TSMT_ENRICHMENT_H     = f"{TSMT_DB}.TSMT_ENRICHMENT_H"
TSMT_SESSION_H        = f"{TSMT_DB}.TSMT_SESSION_H"
TSMT_PINDROP_H        = f"{TSMT_DB}.TSMT_PINDROP_H"
TSMT_TMX_H            = f"{TSMT_DB}.TSMT_TMX_H"

# -------------------------------------------------------------------
# DM_CIAMOP tables (Hive)
# -------------------------------------------------------------------
DM = "DM_CIAMOP"

LOGIN_AND_PASSKEY_STATS  = f"{DM}.login_and_passkey_stats"
MFA_APP                  = f"{DM}.mfa_app"
OTP_STATS_LOOP           = f"{DM}.otp_stats_loop"
PASSKEY_ENROLLED_USERS   = f"{DM}.passkey_enrolled_users"
PASSKEY_ENROLLMENT_LOGIN = f"{DM}.passkey_enrollment_login"
TMX_TSMT_REVIEW_OTP      = f"{DM}.tmx_tsmt_review_otp"

# -------------------------------------------------------------------
# Customer / EIL tables
# -------------------------------------------------------------------
EIL_INVOLVED_PARTY = "EIL.M_INVOLVED_PARTY_H"
EIL_ADDRESS        = "EIL.D_INVOLVED_PARTY_ADDRESS_H"
EIL_ARRANGEMENT    = "EIL.M_ARRANGEMENT_H"

# -------------------------------------------------------------------
# DATE PARAMETERS
# -------------------------------------------------------------------
BUSINESS_DATE = "2025-03-01"
START_DATE    = "2025-01-01"
END_DATE      = "2025-03-31"

# -------------------------------------------------------------------
# OUTPUT — CORRECT DB
# -------------------------------------------------------------------
OUTPUT_DB = "dm_ib_dev"


# -------------------------------------------------------------------
# HELPERS
# -------------------------------------------------------------------
def get_spark():
    return (
        SparkSession.builder
        .appName("V1_Auth_Dashboard")
        .config("spark.sql.shuffle.partitions", "50")
        .enableHiveSupport()
        .getOrCreate()
    )


def save_to_hive(df, table_name):
    df.write.mode("overwrite").saveAsTable(f"{OUTPUT_DB}.{table_name}")

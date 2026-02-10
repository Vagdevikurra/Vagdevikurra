# ============================================================
# WIC2: Keep Wealth rows intact + append InvestPath as extra slice
# Single fact table (3-table model safe pattern)
# ============================================================

from pyspark.sql import SparkSession, functions as F, types as T

# ------------------------------------------------------------
# 0) Spark / Hive "connection" (works in Databricks + spark-submit)
# ------------------------------------------------------------
try:
    spark  # noqa: F821
except NameError:
    spark = (
        SparkSession.builder
        .appName("WIC2 Wealth + InvestPath Union")
        .enableHiveSupport()
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", "800")
        .config("spark.sql.broadcastTimeout", "1200")
        .getOrCreate()
    )

# Common tuning (safe in most managed clusters)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "800")
spark.conf.set("spark.sql.broadcastTimeout", "1200")

# If your environment honors these Hive settings
try:
    spark.sql("SET hive.exec.dynamic.partition=true")
    spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict")
except Exception:
    pass

# ------------------------------------------------------------
# 1) Config (change these ONLY)
# ------------------------------------------------------------
DEFAULT_DB = "dm_ib_dev"          # your target DB
WEALTH_FACT_TABLE = "wic2_wealth_fact"  # existing wealth-only fact table name
FULL_WEALTH_TABLE = f"{DEFAULT_DB}.{WEALTH_FACT_TABLE}"

# Source tables (do not change unless your schema differs)
T_INVP = "eil.d_involved_party_h"
T_A2I  = "eil.d_arrangement_to_involved_party_relationship_h"
T_AR   = "eil.d_arrangement_h"

# InvestPath filters (adjust if your codes differ)
INV_INVP_SOURCE_SYSTEM = "CF"
INV_AR_SOURCE_SYSTEM   = "RN"
INV_AR_ACCOUNT_TYPE    = "IP"

# ------------------------------------------------------------
# 2) Helper: align schemas so UNION never fails
# ------------------------------------------------------------
def align_to_columns(df, ordered_cols_with_types):
    """
    ordered_cols_with_types: list of tuples -> [(col_name, spark_type), ...]
    Ensures df has ALL cols, casts them, and returns in EXACT order.
    Missing cols are created as NULL of the right type.
    """
    for c, typ in ordered_cols_with_types:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None).cast(typ))
        else:
            df = df.withColumn(c, F.col(c).cast(typ))
    return df.select([c for c, _ in ordered_cols_with_types])

# ------------------------------------------------------------
# 3) Read existing Wealth fact (must already exist and be correct)
# ------------------------------------------------------------
wealth_df = spark.table(FULL_WEALTH_TABLE)
wealth_df.createOrReplaceTempView("wealth_fact_existing")

# Auto-detect whether state_name exists in wealth table
HAS_STATE = "state_name" in wealth_df.columns

# Build final fact schema (include state_name only if present)
FACT_COLS = [
    ("business_date",   T.DateType()),
    ("rcif_number",     T.StringType()),
    ("business_group",  T.StringType()),
    ("division",        T.StringType()),
    ("accts_cnt",       T.IntegerType()),
]
if HAS_STATE:
    FACT_COLS.append(("state_name", T.StringType()))

FACT_COLS += [
    ("fact_type",       T.StringType()),
    ("ip_id",           T.StringType()),
    ("ip_account_id",   T.StringType()),
    ("ip_balance",      T.DoubleType()),
    ("ip_open_date",    T.DateType()),
]

print("Wealth table:", FULL_WEALTH_TABLE)
print("Detected state_name:", HAS_STATE)
print("Final FACT_COLS order:", [c for c, _ in FACT_COLS])

# ------------------------------------------------------------
# 4) Get latest business_date (same dt for IP slice)
# ------------------------------------------------------------
mx = spark.sql(f"""
  SELECT MAX(CAST(business_date AS date)) AS dt
  FROM {T_INVP}
""").collect()[0]["dt"]

if mx is None:
    raise RuntimeError(f"Could not find MAX(business_date) in {T_INVP}")

print("Latest business_date =", mx)
spark.sql(f"CREATE OR REPLACE TEMP VIEW mx_dt AS SELECT DATE('{mx}') AS dt")

# ------------------------------------------------------------
# 5) Wealth rows: add fact_type + InvestPath nullable columns
#    IMPORTANT: Wealth rows are kept exactly as-is, plus extra columns
# ------------------------------------------------------------
# Minimal select for wealth (keep your existing wealth columns here)
# If you have MORE wealth columns you want to keep, add them to FACT_COLS + select below.
if HAS_STATE:
    wealth_typed_sql = """
    CREATE OR REPLACE TEMP VIEW wealth_fact_typed AS
    SELECT
      CAST(business_date AS date) AS business_date,
      CAST(rcif_number AS string) AS rcif_number,
      CAST(business_group AS string) AS business_group,
      CAST(division AS string) AS division,
      CAST(accts_cnt AS int) AS accts_cnt,
      CAST(state_name AS string) AS state_name,

      'WEALTH' AS fact_type,
      CAST(NULL AS string) AS ip_id,
      CAST(NULL AS string) AS ip_account_id,
      CAST(NULL AS double) AS ip_balance,
      CAST(NULL AS date) AS ip_open_date
    FROM wealth_fact_existing
    """
else:
    wealth_typed_sql = """
    CREATE OR REPLACE TEMP VIEW wealth_fact_typed AS
    SELECT
      CAST(business_date AS date) AS business_date,
      CAST(rcif_number AS string) AS rcif_number,
      CAST(business_group AS string) AS business_group,
      CAST(division AS string) AS division,
      CAST(accts_cnt AS int) AS accts_cnt,

      'WEALTH' AS fact_type,
      CAST(NULL AS string) AS ip_id,
      CAST(NULL AS string) AS ip_account_id,
      CAST(NULL AS double) AS ip_balance,
      CAST(NULL AS date) AS ip_open_date
    FROM wealth_fact_existing
    """

spark.sql(wealth_typed_sql)

# ------------------------------------------------------------
# 6) InvestPath rows (latest dt only)
# ------------------------------------------------------------
if HAS_STATE:
    ip_sql = f"""
    CREATE OR REPLACE TEMP VIEW investpath_rows AS
    WITH dt AS (SELECT dt FROM mx_dt),
    inv AS (
      SELECT
        CAST(ind.rcif_cust_nbr AS string)         AS rcif_number,
        CAST(ind.involved_party_id AS string)    AS ip_id,
        CAST(ar.arrangement_id AS string)        AS ip_account_id,
        CAST(ar.current_balance_amt AS double)   AS ip_balance,
        CAST(ar.open_date AS date)               AS ip_open_date
      FROM {T_INVP} ind
      JOIN dt ON CAST(ind.business_date AS date) = dt.dt
      JOIN {T_A2I} a2i
        ON ind.involved_party_id = a2i.involved_party_id
       AND ind.business_date = a2i.business_date
       AND ind.source_system_code = a2i.source_system_code
      JOIN {T_AR} ar
        ON a2i.arrangement_id = ar.arrangement_id
       AND a2i.arrangement_source_system_code = ar.source_system_code
       AND a2i.business_date = ar.business_date
      WHERE ind.source_system_code = '{INV_INVP_SOURCE_SYSTEM}'
        AND NVL(ind.deceased_ind, 'N') = 'N'
        AND ar.closed_ind = 'N'
        AND ar.account_type_code = '{INV_AR_ACCOUNT_TYPE}'
        AND ar.source_system_code = '{INV_AR_SOURCE_SYSTEM}'
    )
    SELECT
      (SELECT dt FROM mx_dt)         AS business_date,
      rcif_number,
      'Investment Services'         AS business_group,
      'InvestPath'                  AS division,
      1                             AS accts_cnt,
      CAST(NULL AS string)          AS state_name,

      'INVESTPATH'                  AS fact_type,
      ip_id,
      ip_account_id,
      ip_balance,
      ip_open_date
    FROM inv
    WHERE rcif_number IS NOT NULL
    """
else:
    ip_sql = f"""
    CREATE OR REPLACE TEMP VIEW investpath_rows AS
    WITH dt AS (SELECT dt FROM mx_dt),
    inv AS (
      SELECT
        CAST(ind.rcif_cust_nbr AS string)         AS rcif_number,
        CAST(ind.involved_party_id AS string)    AS ip_id,
        CAST(ar.arrangement_id AS string)        AS ip_account_id,
        CAST(ar.current_balance_amt AS double)   AS ip_balance,
        CAST(ar.open_date AS date)               AS ip_open_date
      FROM {T_INVP} ind
      JOIN dt ON CAST(ind.business_date AS date) = dt.dt
      JOIN {T_A2I} a2i
        ON ind.involved_party_id = a2i.involved_party_id
       AND ind.business_date = a2i.business_date
       AND ind.source_system_code = a2i.source_system_code
      JOIN {T_AR} ar
        ON a2i.arrangement_id = ar.arrangement_id
       AND a2i.arrangement_source_system_code = ar.source_system_code
       AND a2i.business_date = ar.business_date
      WHERE ind.source_system_code = '{INV_INVP_SOURCE_SYSTEM}'
        AND NVL(ind.deceased_ind, 'N') = 'N'
        AND ar.closed_ind = 'N'
        AND ar.account_type_code = '{INV_AR_ACCOUNT_TYPE}'
        AND ar.source_system_code = '{INV_AR_SOURCE_SYSTEM}'
    )
    SELECT
      (SELECT dt FROM mx_dt)         AS business_date,
      rcif_number,
      'Investment Services'         AS business_group,
      'InvestPath'                  AS division,
      1                             AS accts_cnt,

      'INVESTPATH'                  AS fact_type,
      ip_id,
      ip_account_id,
      ip_balance,
      ip_open_date
    FROM inv
    WHERE rcif_number IS NOT NULL
    """

spark.sql(ip_sql)

# ------------------------------------------------------------
# 7) Union wealth + investpath safely (schema aligned)
# ------------------------------------------------------------
wealth_typed_df = spark.table("wealth_fact_typed")
ip_rows_df      = spark.table("investpath_rows")

wealth_aligned = align_to_columns(wealth_typed_df, FACT_COLS)
ip_aligned     = align_to_columns(ip_rows_df, FACT_COLS)

final_df = wealth_aligned.unionByName(ip_aligned)
final_df.createOrReplaceTempView("wic2_wealth_fact_final")

# ------------------------------------------------------------
# 8) VALIDATIONS (must match Wealth before/after)
# ------------------------------------------------------------
print("\n--- Wealth baseline (from existing wealth table) ---")
spark.sql("""
WITH dt AS (SELECT MAX(CAST(business_date AS date)) AS dt FROM wealth_fact_existing)
SELECT
  (SELECT dt FROM dt) AS latest_dt,
  COUNT(DISTINCT CASE WHEN CAST(business_date AS date)=(SELECT dt FROM dt) THEN rcif_number END) AS wealth_rcifs_latest,
  SUM(CASE WHEN CAST(business_date AS date)=(SELECT dt FROM dt) THEN accts_cnt ELSE 0 END)        AS wealth_accounts_latest
FROM wealth_fact_existing
""").show(truncate=False)

print("\n--- Wealth after (filtered to fact_type='WEALTH') ---")
spark.sql("""
WITH dt AS (SELECT MAX(CAST(business_date AS date)) AS dt FROM wealth_fact_existing)
SELECT
  COUNT(DISTINCT CASE WHEN fact_type='WEALTH' AND CAST(business_date AS date)=(SELECT dt FROM dt) THEN rcif_number END) AS wealth_rcifs_latest_after,
  SUM(CASE WHEN fact_type='WEALTH' AND CAST(business_date AS date)=(SELECT dt FROM dt) THEN accts_cnt ELSE 0 END)        AS wealth_accounts_latest_after
FROM wic2_wealth_fact_final
""").show(truncate=False)

print("\n--- InvestPath stats (new slice) ---")
spark.sql("""
SELECT
  COUNT(*)                      AS ip_rows,
  COUNT(DISTINCT ip_id)         AS ip_customers,
  COUNT(DISTINCT ip_account_id) AS ip_accounts,
  SUM(ip_balance)               AS ip_aum,
  SUM(CASE WHEN ip_balance > 0 THEN 1 ELSE 0 END) AS ip_funded_accounts
FROM wic2_wealth_fact_final
WHERE fact_type='INVESTPATH'
""").show(truncate=False)

# ------------------------------------------------------------
# 9) Save overwrite (ONLY after validation looks good)
# ------------------------------------------------------------
# If you want a safety net, you can save to a new table first, then swap names.
# Example: f"{DEFAULT_DB}.wic2_wealth_fact_tmp"

(
    spark.table("wic2_wealth_fact_final")
    .write
    .mode("overwrite")
    .saveAsTable(FULL_WEALTH_TABLE)
)

print("✅ Saved final combined fact table to:", FULL_WEALTH_TABLE)

# ============================================================
# WIC2: Keep Wealth rows intact + append InvestPath as extra slice
# COMPLETE FIXED CODE (validation rewritten to DataFrame API)
# ============================================================

from pyspark.sql import SparkSession, functions as F, types as T

# ------------------------------------------------------------
# 0) Spark / Hive (works in Databricks + spark-submit)
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

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "800")
spark.conf.set("spark.sql.broadcastTimeout", "1200")

try:
    spark.sql("SET hive.exec.dynamic.partition=true")
    spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict")
except Exception:
    pass

# ------------------------------------------------------------
# 1) Config (change ONLY these)
# ------------------------------------------------------------
DEFAULT_DB = "dm_ib_dev"
WEALTH_FACT_TABLE = "wic2_wealth_fact"   # existing wealth-only fact table
FULL_WEALTH_TABLE = f"{DEFAULT_DB}.{WEALTH_FACT_TABLE}"

# Source tables
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
    for c, typ in ordered_cols_with_types:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None).cast(typ))
        else:
            df = df.withColumn(c, F.col(c).cast(typ))
    return df.select([c for c, _ in ordered_cols_with_types])

# ------------------------------------------------------------
# 3) Read existing Wealth fact (must already be correct)
# ------------------------------------------------------------
wealth_df = spark.table(FULL_WEALTH_TABLE)
HAS_STATE = "state_name" in wealth_df.columns

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

# ------------------------------------------------------------
# 5) Wealth rows: add fact_type + InvestPath nullable columns
# ------------------------------------------------------------
wealth_fact_existing = wealth_df  # keep as DF

if HAS_STATE:
    wealth_fact_typed = wealth_fact_existing.select(
        F.to_date("business_date").alias("business_date"),
        F.col("rcif_number").cast("string").alias("rcif_number"),
        F.col("business_group").cast("string").alias("business_group"),
        F.col("division").cast("string").alias("division"),
        F.col("accts_cnt").cast("int").alias("accts_cnt"),
        F.col("state_name").cast("string").alias("state_name"),
        F.lit("WEALTH").alias("fact_type"),
        F.lit(None).cast("string").alias("ip_id"),
        F.lit(None).cast("string").alias("ip_account_id"),
        F.lit(None).cast("double").alias("ip_balance"),
        F.lit(None).cast("date").alias("ip_open_date"),
    )
else:
    wealth_fact_typed = wealth_fact_existing.select(
        F.to_date("business_date").alias("business_date"),
        F.col("rcif_number").cast("string").alias("rcif_number"),
        F.col("business_group").cast("string").alias("business_group"),
        F.col("division").cast("string").alias("division"),
        F.col("accts_cnt").cast("int").alias("accts_cnt"),
        F.lit("WEALTH").alias("fact_type"),
        F.lit(None).cast("string").alias("ip_id"),
        F.lit(None).cast("string").alias("ip_account_id"),
        F.lit(None).cast("double").alias("ip_balance"),
        F.lit(None).cast("date").alias("ip_open_date"),
    )

# ------------------------------------------------------------
# 6) InvestPath rows (latest dt only) using Spark SQL (same logic)
# ------------------------------------------------------------
spark.sql(f"CREATE OR REPLACE TEMP VIEW mx_dt AS SELECT DATE('{mx}') AS dt")

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
ip_rows_df = spark.table("investpath_rows")

# ------------------------------------------------------------
# 7) Union wealth + investpath safely (schema aligned)
# ------------------------------------------------------------
wealth_aligned = align_to_columns(wealth_fact_typed, FACT_COLS)
ip_aligned     = align_to_columns(ip_rows_df, FACT_COLS)

final_df = wealth_aligned.unionByName(ip_aligned)
final_df.createOrReplaceTempView("wic2_wealth_fact_final")

# ------------------------------------------------------------
# 8) VALIDATIONS (DataFrame API only — avoids Spark SQL binder errors)
# ------------------------------------------------------------
latest_dt = wealth_fact_existing.select(F.max(F.to_date("business_date")).alias("dt")).collect()[0]["dt"]
print("\nLatest wealth_dt used for validation:", latest_dt)

# Baseline wealth (existing table)
baseline_df = (
    wealth_fact_existing
    .withColumn("bdt", F.to_date("business_date"))
    .where(F.col("bdt") == F.lit(latest_dt))
    .agg(
        F.countDistinct(F.col("rcif_number")).alias("wealth_rcifs_latest"),
        F.sum(F.col("accts_cnt").cast("long")).alias("wealth_accounts_latest")
    )
)

print("\n--- Wealth baseline (existing) ---")
baseline_df.show(truncate=False)

# Wealth after union (filtered to WEALTH)
after_df = (
    final_df
    .where((F.col("fact_type") == F.lit("WEALTH")) & (F.col("business_date") == F.lit(latest_dt)))
    .agg(
        F.countDistinct(F.col("rcif_number")).alias("wealth_rcifs_latest_after"),
        F.sum(F.col("accts_cnt").cast("long")).alias("wealth_accounts_latest_after")
    )
)

print("\n--- Wealth after union (filtered to WEALTH) ---")
after_df.show(truncate=False)

# InvestPath stats
ip_stats_df = (
    final_df
    .where(F.col("fact_type") == F.lit("INVESTPATH"))
    .agg(
        F.count(F.lit(1)).alias("ip_rows"),
        F.countDistinct("ip_id").alias("ip_customers"),
        F.countDistinct("ip_account_id").alias("ip_accounts"),
        F.sum(F.col("ip_balance").cast("double")).alias("ip_aum"),
        F.sum(F.when(F.col("ip_balance") > 0, F.lit(1)).otherwise(F.lit(0))).alias("ip_funded_accounts")
    )
)

print("\n--- InvestPath stats (new slice) ---")
ip_stats_df.show(truncate=False)

# Optional: hard fail if wealth changed
base = baseline_df.collect()[0].asDict()
aft  = after_df.collect()[0].asDict()
if base["wealth_rcifs_latest"] != aft["wealth_rcifs_latest_after"] or base["wealth_accounts_latest"] != aft["wealth_accounts_latest_after"]:
    raise RuntimeError(
        f"WEALTH CHANGED! baseline_rcifs={base['wealth_rcifs_latest']} after_rcifs={aft['wealth_rcifs_latest_after']}, "
        f"baseline_accts={base['wealth_accounts_latest']} after_accts={aft['wealth_accounts_latest_after']}"
    )

print("\n✅ Validation passed: Wealth baseline == Wealth after union")

# ------------------------------------------------------------
# 9) Save overwrite (ONLY after validation looks good)
# ------------------------------------------------------------
final_df.write.mode("overwrite").saveAsTable(FULL_WEALTH_TABLE)
print("✅ Saved final combined fact table to:", FULL_WEALTH_TABLE)

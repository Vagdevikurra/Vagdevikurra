# ============================================================
# COMPLETE, HARDENED SCRIPT (CDH-safe + overwrite-safe)
# Writes to TEMP table first, then swaps into final name
# ============================================================

from pyspark.sql import SparkSession, functions as F, types as T
import time

# ------------------------------------------------------------
# 0) Spark / Hive
# ------------------------------------------------------------
try:
    spark  # noqa: F821
except NameError:
    spark = (
        SparkSession.builder
        .appName("WIC2 Wealth + InvestPath Union (Swap Save)")
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

# ------------------------------------------------------------
# 1) Config (edit ONLY these)
# ------------------------------------------------------------
DEFAULT_DB = "dm_ib_dev"
WEALTH_FACT_TABLE = "wic2_wealth_fact"
FULL_WEALTH_TABLE = f"{DEFAULT_DB}.{WEALTH_FACT_TABLE}"

TMP_TABLE = f"{DEFAULT_DB}.{WEALTH_FACT_TABLE}__tmp_{int(time.time())}"

T_INVP = "eil.d_involved_party_h"
T_A2I  = "eil.d_arrangement_to_involved_party_relationship_h"
T_AR   = "eil.d_arrangement_h"

INV_INVP_SOURCE_SYSTEM = "CF"
INV_AR_SOURCE_SYSTEM   = "RN"
INV_AR_ACCOUNT_TYPE    = "IP"

# ------------------------------------------------------------
# 2) Helpers
# ------------------------------------------------------------
def pick_col(df, candidates, table_name):
    cols_lower = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in cols_lower:
            return cols_lower[cand.lower()]
    raise RuntimeError(f"[{table_name}] Missing column. Tried {candidates}. Available={df.columns}")

def align_to_columns(df, ordered_cols_with_types):
    for c, typ in ordered_cols_with_types:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None).cast(typ))
        else:
            df = df.withColumn(c, F.col(c).cast(typ))
    return df.select([c for c, _ in ordered_cols_with_types])

# ------------------------------------------------------------
# 3) Read Wealth fact (existing)
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
print("Temp table:", TMP_TABLE)
print("Detected state_name:", HAS_STATE)

# ------------------------------------------------------------
# 4) Latest business_date (from involved party)
# ------------------------------------------------------------
mx = spark.sql(f"SELECT MAX(CAST(business_date AS date)) AS dt FROM {T_INVP}").collect()[0]["dt"]
if mx is None:
    raise RuntimeError(f"Could not find MAX(business_date) in {T_INVP}")
mx_date = F.lit(mx).cast("date")
print("Latest business_date =", mx)

# ------------------------------------------------------------
# 5) Wealth typed (metadata only)
# ------------------------------------------------------------
if HAS_STATE:
    wealth_fact_typed = wealth_df.select(
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
    wealth_fact_typed = wealth_df.select(
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
# 6) InvestPath rows (explicit DF joins)
# ------------------------------------------------------------
ind = spark.table(T_INVP)
a2i = spark.table(T_A2I)
ar  = spark.table(T_AR)

IND_BD   = pick_col(ind, ["business_date"], T_INVP)
IND_SYS  = pick_col(ind, ["source_system_code", "source_system_cd"], T_INVP)
IND_IPID = pick_col(ind, ["involved_party_id"], T_INVP)
IND_RCIF = pick_col(ind, ["rcif_cust_nbr", "rcif_number", "rcif_nbr"], T_INVP)
IND_DEC  = pick_col(ind, ["deceased_ind"], T_INVP)

A2I_BD   = pick_col(a2i, ["business_date"], T_A2I)
A2I_SYS  = pick_col(a2i, ["source_system_code", "source_system_cd"], T_A2I)
A2I_IPID = pick_col(a2i, ["involved_party_id"], T_A2I)
A2I_ARR  = pick_col(a2i, ["arrangement_id"], T_A2I)
A2I_ARR_SYS = pick_col(a2i, ["arrangement_source_system_code", "arrangement_source_system_cd", "arrangement_src_sys_cd"], T_A2I)

AR_BD    = pick_col(ar, ["business_date"], T_AR)
AR_SYS   = pick_col(ar, ["source_system_code", "source_system_cd"], T_AR)
AR_ARR   = pick_col(ar, ["arrangement_id"], T_AR)
AR_BAL   = pick_col(ar, ["current_balance_amt", "current_balance_amount", "current_balance"], T_AR)
AR_OPEN  = pick_col(ar, ["open_date", "account_open_date"], T_AR)
AR_CLS   = pick_col(ar, ["closed_ind"], T_AR)
AR_TYP   = pick_col(ar, ["account_type_code", "account_type_cd"], T_AR)

ind_f = (
    ind.withColumn("bdt", F.to_date(F.col(IND_BD)))
       .where(F.col("bdt") == mx_date)
       .where(F.col(IND_SYS) == F.lit(INV_INVP_SOURCE_SYSTEM))
       .where(F.coalesce(F.col(IND_DEC), F.lit("N")) == F.lit("N"))
)

a2i_f = (
    a2i.withColumn("bdt", F.to_date(F.col(A2I_BD)))
       .where(F.col("bdt") == mx_date)
)

ar_f = (
    ar.withColumn("bdt", F.to_date(F.col(AR_BD)))
      .where(F.col("bdt") == mx_date)
      .where(F.col(AR_SYS) == F.lit(INV_AR_SOURCE_SYSTEM))
      .where(F.col(AR_CLS) == F.lit("N"))
      .where(F.col(AR_TYP) == F.lit(INV_AR_ACCOUNT_TYPE))
)

j1 = ind_f.alias("ind").join(
    a2i_f.alias("a2i"),
    on=[
        F.col(f"ind.{IND_IPID}") == F.col(f"a2i.{A2I_IPID}"),
        F.col(f"ind.{IND_BD}") == F.col(f"a2i.{A2I_BD}"),
        F.col(f"ind.{IND_SYS}") == F.col(f"a2i.{A2I_SYS}"),
    ],
    how="inner",
)

j2 = j1.join(
    ar_f.alias("ar"),
    on=[
        F.col(f"a2i.{A2I_ARR}") == F.col(f"ar.{AR_ARR}"),
        F.col(f"a2i.{A2I_ARR_SYS}") == F.col(f"ar.{AR_SYS}"),
        F.col(f"a2i.{A2I_BD}") == F.col(f"ar.{AR_BD}"),
    ],
    how="inner",
)

if HAS_STATE:
    ip_rows_df = j2.select(
        mx_date.alias("business_date"),
        F.col(f"ind.{IND_RCIF}").cast("string").alias("rcif_number"),
        F.lit("Investment Services").alias("business_group"),
        F.lit("InvestPath").alias("division"),
        F.lit(1).cast("int").alias("accts_cnt"),
        F.lit(None).cast("string").alias("state_name"),
        F.lit("INVESTPATH").alias("fact_type"),
        F.col(f"ind.{IND_IPID}").cast("string").alias("ip_id"),
        F.col(f"ar.{AR_ARR}").cast("string").alias("ip_account_id"),
        F.col(f"ar.{AR_BAL}").cast("double").alias("ip_balance"),
        F.to_date(F.col(f"ar.{AR_OPEN}")).alias("ip_open_date"),
    )
else:
    ip_rows_df = j2.select(
        mx_date.alias("business_date"),
        F.col(f"ind.{IND_RCIF}").cast("string").alias("rcif_number"),
        F.lit("Investment Services").alias("business_group"),
        F.lit("InvestPath").alias("division"),
        F.lit(1).cast("int").alias("accts_cnt"),
        F.lit("INVESTPATH").alias("fact_type"),
        F.col(f"ind.{IND_IPID}").cast("string").alias("ip_id"),
        F.col(f"ar.{AR_ARR}").cast("string").alias("ip_account_id"),
        F.col(f"ar.{AR_BAL}").cast("double").alias("ip_balance"),
        F.to_date(F.col(f"ar.{AR_OPEN}")).alias("ip_open_date"),
    )

ip_rows_df = ip_rows_df.where(F.col("rcif_number").isNotNull())

# ------------------------------------------------------------
# 7) Union
# ------------------------------------------------------------
final_df = align_to_columns(wealth_fact_typed, FACT_COLS).unionByName(align_to_columns(ip_rows_df, FACT_COLS))

# ------------------------------------------------------------
# 8) Validate Wealth unchanged
# ------------------------------------------------------------
latest_dt = wealth_df.select(F.max(F.to_date("business_date")).alias("dt")).collect()[0]["dt"]

base = (
    wealth_df.withColumn("bdt", F.to_date("business_date"))
    .where(F.col("bdt") == F.lit(latest_dt))
    .agg(
        F.countDistinct("rcif_number").alias("wealth_rcifs_latest"),
        F.sum(F.col("accts_cnt").cast("long")).alias("wealth_accounts_latest"),
    )
).collect()[0].asDict()

aft = (
    final_df.where((F.col("fact_type") == "WEALTH") & (F.col("business_date") == F.lit(latest_dt)))
    .agg(
        F.countDistinct("rcif_number").alias("wealth_rcifs_latest_after"),
        F.sum(F.col("accts_cnt").cast("long")).alias("wealth_accounts_latest_after"),
    )
).collect()[0].asDict()

print("Baseline wealth:", base)
print("After wealth   :", aft)

if base["wealth_rcifs_latest"] != aft["wealth_rcifs_latest_after"] or base["wealth_accounts_latest"] != aft["wealth_accounts_latest_after"]:
    raise RuntimeError(f"WEALTH CHANGED! baseline={base} after={aft}")

print("✅ Wealth unchanged")

# ------------------------------------------------------------
# 9) SAVE SAFELY: write to temp, then swap
# ------------------------------------------------------------
print("Writing to temp table:", TMP_TABLE)
final_df.write.mode("overwrite").saveAsTable(TMP_TABLE)

print("Swapping temp -> final")
spark.sql(f"DROP TABLE IF EXISTS {FULL_WEALTH_TABLE}")
spark.sql(f"ALTER TABLE {TMP_TABLE} RENAME TO {FULL_WEALTH_TABLE}")

print("✅ Done. Final table:", FULL_WEALTH_TABLE)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# =========================
# CONFIG
# =========================
DB = "dm_ib_dev"                 # <-- keep your DB/schema
START_DATE = "2025-07-01"
END_DATE   = "2025-12-31"        # inclusive
END_MONTH_DT = "2025-12-01"      # if month_dt is first-of-month snapshots

# OUTPUT TABLES (3 only)
OUT_WEALTH     = f"{DB}.wealth_customer_kpi_0701_1231"
OUT_DIGITAL    = f"{DB}.digital_active_ibn_0701_1231"
OUT_INVESTPATH = f"{DB}.investpath_kpi_0701_1231"


# =========================
# SOURCE TABLES (UPDATE THESE)
# =========================
# 1) WEALTH/CUSTOMER source
SRC_WEALTH_CUSTOMER = f"{DB}.<YOUR_WEALTH_CUSTOMER_TABLE>"   # <-- CHANGE

# Required wealth columns (rename in select if your names differ):
#   rcif
#   month_dt
#   accts_cnt
#   cust_internet_banking_nbr

# 2) DIGITAL source
SRC_DIGITAL = f"{DB}.<YOUR_DIGITAL_TABLE>"                  # <-- CHANGE

# Required digital columns:
#   month_dt
#   reltibn
#   digitally_active_flg  (1/0 or Y/N)

# 3) INVESTPATH source
SRC_INVESTPATH = f"{DB}.<YOUR_INVESTPATH_TABLE>"            # <-- CHANGE

# Required investpath columns (adjust to your real schema):
#   month_dt
#   invest_account_id     (or arrangement/acct id)
#   funded_flg            (1/0 or Y/N)
#   invest_customer_ip_id (customer identifier for the "119" count)


# =========================
# LOAD
# =========================
wealth_raw  = spark.table(SRC_WEALTH_CUSTOMER)
digital_raw = spark.table(SRC_DIGITAL)
ip_raw      = spark.table(SRC_INVESTPATH)


# ============================================================
# TABLE 2 of 3: DIGITAL ACTIVE IBN (unique set, no duplication)
# ============================================================
digital_active_ibn_df = (
    digital_raw
      .filter(F.col("month_dt").between(F.lit(START_DATE), F.lit(END_MONTH_DT)))
      .filter(
          (F.col("digitally_active_flg") == 1) |
          (F.upper(F.col("digitally_active_flg")) == F.lit("Y")) |
          (F.upper(F.col("digitally_active_flg")) == F.lit("YES")) |
          (F.upper(F.col("digitally_active_flg")) == F.lit("TRUE"))
      )
      .select(
          F.col("reltibn").cast("string").alias("cust_internet_banking_nbr")
      )
      .where(F.col("cust_internet_banking_nbr").isNotNull())
      .distinct()
)

# Write DIGITAL table (2/3)
(digital_active_ibn_df
 .write
 .mode("overwrite")
 .format("parquet")
 .saveAsTable(OUT_DIGITAL)
)


# =====================================================================
# TABLE 1 of 3: WEALTH CUSTOMER KPI (1 row per RCIF; no many-to-many)
#   - This is where your "SUM(accts_cnt)" was getting inflated.
#   - Fix: collapse to EXACTLY one row per RCIF (latest month in window)
# =====================================================================
wealth_win = (
    wealth_raw
      .filter(F.col("month_dt").between(F.lit(START_DATE), F.lit(END_MONTH_DT)))
      .select(
          F.col("rcif").cast("string").alias("rcif"),
          F.col("month_dt"),
          F.col("accts_cnt").cast("long").alias("accts_cnt"),
          F.col("cust_internet_banking_nbr").cast("string").alias("cust_internet_banking_nbr")
      )
      .where(F.col("rcif").isNotNull())
)

# pick latest snapshot per RCIF inside window
w_latest = Window.partitionBy("rcif").orderBy(F.col("month_dt").desc())

wealth_latest = (
    wealth_win
      .withColumn("rn", F.row_number().over(w_latest))
      .filter(F.col("rn") == 1)
      .drop("rn")
)

# defensive final collapse: guarantees 1 row per RCIF even if duplicates exist
wealth_1row = (
    wealth_latest
      .groupBy("rcif")
      .agg(
          F.max("month_dt").alias("month_dt"),
          F.max("accts_cnt").alias("accts_cnt"),
          F.max("cust_internet_banking_nbr").alias("cust_internet_banking_nbr")
      )
)

# Create digital flag WITHOUT multiplying rows (semi join logic)
digital_active_ibn_set = spark.table(OUT_DIGITAL)

wealth_kpi_df = (
    wealth_1row
      .withColumn("cust_internet_banking_nbr", F.col("cust_internet_banking_nbr").cast("string"))
      .join(
          digital_active_ibn_set.select("cust_internet_banking_nbr").distinct(),
          on="cust_internet_banking_nbr",
          how="left"
      )
      .withColumn("digital_active_flg",
                  F.when(F.col("cust_internet_banking_nbr").isNotNull(), F.lit(1)).otherwise(F.lit(0))
      )
      .select(
          "rcif", "month_dt", "accts_cnt", "cust_internet_banking_nbr", "digital_active_flg"
      )
)

# Write WEALTH table (1/3)
(wealth_kpi_df
 .write
 .mode("overwrite")
 .format("parquet")
 .saveAsTable(OUT_WEALTH)
)


# =====================================================================
# TABLE 3 of 3: INVESTPATH KPI (collapse to safe grains; no many-to-many)
# =====================================================================
ip_win = (
    ip_raw
      .filter(F.col("month_dt").between(F.lit(START_DATE), F.lit(END_MONTH_DT)))
      .select(
          F.col("month_dt"),
          F.col("invest_account_id").cast("string").alias("invest_account_id"),
          F.col("funded_flg").alias("funded_flg"),
          F.col("invest_customer_ip_id").cast("string").alias("invest_customer_ip_id")
      )
)

# Reduce to unique account grain for counts (no duplicates)
ip_accounts = ip_win.select("invest_account_id").where(F.col("invest_account_id").isNotNull()).distinct()

ip_funded_accounts = (
    ip_win
      .filter(
          (F.col("funded_flg") == 1) |
          (F.upper(F.col("funded_flg")) == F.lit("Y")) |
          (F.upper(F.col("funded_flg")) == F.lit("YES")) |
          (F.upper(F.col("funded_flg")) == F.lit("TRUE"))
      )
      .select("invest_account_id")
      .where(F.col("invest_account_id").isNotNull())
      .distinct()
)

ip_customers = (
    ip_win
      .select("invest_customer_ip_id")
      .where(F.col("invest_customer_ip_id").isNotNull())
      .distinct()
)

investpath_kpi_df = (
    ip_accounts.agg(F.count("*").alias("invest_accounts"))
      .crossJoin(ip_funded_accounts.agg(F.count("*").alias("invest_accounts_funded")))
      .crossJoin(ip_customers.agg(F.count("*").alias("invest_customers_ip")))
)

# Write INVESTPATH table (3/3)
(investpath_kpi_df
 .write
 .mode("overwrite")
 .format("parquet")
 .saveAsTable(OUT_INVESTPATH)
)


# =========================
# VALIDATION PRINTS
# =========================
print("==== WEALTH RCIF ====")
spark.table(OUT_WEALTH).select(F.countDistinct("rcif").alias("wealth_rcif")).show()

print("==== WEALTH total accounts (SUM accts_cnt after 1 row per RCIF) ====")
spark.table(OUT_WEALTH).agg(F.sum("accts_cnt").alias("wealth_accounts_total")).show()

print("==== WEALTH digital RCIF (distinct rcif where digital_active_flg=1) ====")
spark.table(OUT_WEALTH).filter(F.col("digital_active_flg")==1).select(F.countDistinct("rcif").alias("wealth_digital_rcif")).show()

print("==== DIGITAL active IBN distinct reltibn ====")
spark.table(OUT_DIGITAL).agg(F.count("*").alias("digital_active_ibn")).show()

print("==== INVESTPATH KPI ====")
spark.table(OUT_INVESTPATH).show(truncate=False)

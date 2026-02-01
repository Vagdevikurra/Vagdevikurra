from pyspark.sql import functions as F
from pyspark.sql.window import Window

# =========================
# CONFIG (your fixed window)
# =========================
START_DATE = "2025-07-01"
END_DATE   = "2025-12-31"   # inclusive

# If your month_dt is always first-of-month, your effective last month in window is 2025-12-01.
END_MONTH_DT = "2025-12-01"

# =========================
# INPUT DFS (use your existing ones)
# =========================
# customer_df: must include at least:
#   rcif (string/int), month_dt (date), accts_cnt (int), cust_internet_banking_nbr (string/int)
#
# digital_df: must include at least:
#   month_dt (date), reltibn (string/int), digitally_active_flg (0/1 or Y/N)
#
# NOTE: keep your existing load logic. Just plug these transformations in.

# -------------------------
# 1) DIGITAL ACTIVE IBN LIST (unique)
# -------------------------
digital_active_ibn = (
    digital_df
      .filter(F.col("month_dt").between(F.lit(START_DATE), F.lit(END_MONTH_DT)))
      .filter(
          (F.col("digitally_active_flg") == 1) |
          (F.upper(F.col("digitally_active_flg")) == F.lit("Y")) |
          (F.upper(F.col("digitally_active_flg")) == F.lit("YES")) |
          (F.upper(F.col("digitally_active_flg")) == F.lit("TRUE"))
      )
      .select(F.col("reltibn").cast("string").alias("cust_internet_banking_nbr"))
      .where(F.col("cust_internet_banking_nbr").isNotNull())
      .distinct()
)

# -------------------------
# 2) WEALTH CUSTOMER BASE (collapse to 1 row per RCIF)
#    This is the KEY fix for the accounts_total overcount.
# -------------------------

# Filter to your date window first
wealth_cust_win = (
    customer_df
      .filter(F.col("month_dt").between(F.lit(START_DATE), F.lit(END_MONTH_DT)))
      .select(
          F.col("rcif").cast("string").alias("rcif"),
          F.col("month_dt"),
          F.col("accts_cnt").cast("long").alias("accts_cnt"),
          F.col("cust_internet_banking_nbr").cast("string").alias("cust_internet_banking_nbr"),
      )
      .where(F.col("rcif").isNotNull())
)

# Pick the latest month row per RCIF inside the window
w_latest = Window.partitionBy("rcif").orderBy(F.col("month_dt").desc())

wealth_cust_latest = (
    wealth_cust_win
      .withColumn("rn", F.row_number().over(w_latest))
      .filter(F.col("rn") == 1)
      .drop("rn")
)

# Defensive collapse again in case you still have duplicates even after rn=1 (dirty data)
# Ensures EXACTLY 1 row per RCIF for downstream sums/counts
wealth_cust_1row = (
    wealth_cust_latest
      .groupBy("rcif")
      .agg(
          F.max("accts_cnt").alias("accts_cnt"),  # use max to avoid double counting if duplicates exist
          F.max("cust_internet_banking_nbr").alias("cust_internet_banking_nbr")
      )
)

# -------------------------
# 3) FIXED METRICS
# -------------------------

# (A) WEALTH total accounts = SUM(accts_cnt) AFTER collapsing to 1 row per RCIF
wealth_accounts_total = wealth_cust_1row.agg(F.sum("accts_cnt").alias("wealth_accounts_total"))

# (B) WEALTH digital RCIF = count distinct RCIF where customer's IBN is digitally active
# left_semi prevents any row multiplication (no many-to-many explosion possible)
wealth_digital_rcif = (
    wealth_cust_1row
      .join(digital_active_ibn, on="cust_internet_banking_nbr", how="left_semi")
      .select("rcif")
      .distinct()
      .agg(F.count("*").alias("wealth_digital_rcif"))
)

# Optional: your wealth_rcif count (should remain what you expect)
wealth_rcif = wealth_cust_1row.select("rcif").distinct().agg(F.count("*").alias("wealth_rcif"))

# -------------------------
# 4) SHOW RESULTS (same style you’re printing)
# -------------------------
print("WEALTH rcif:")
wealth_rcif.show(truncate=False)

print("WEALTH total accounts = SUM(accts_cnt) (after 1 row per RCIF):")
wealth_accounts_total.show(truncate=False)

print("WEALTH digital RCIF (wealth RCIF with digitally active IBN):")
wealth_digital_rcif.show(truncate=False)

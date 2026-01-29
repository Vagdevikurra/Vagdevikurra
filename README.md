from pyspark.sql import functions as F
from pyspark.sql.window import Window

# =========================
# Inputs you already have
# =========================
DB = "dm_ib_dev"
START_DATE = "2025-07-01"
END_DATE   = "2025-12-31"
DIG_FQN    = f"{DB}.digital_202507_202512"

start_dt = F.to_date(F.lit(START_DATE))
end_dt   = F.to_date(F.lit(END_DATE))

# -------------------------
# Helper: pick column safely
# -------------------------
def first_existing_col(df, *candidates):
    cols = set(df.columns)
    for c in candidates:
        if c in cols:
            return c
    raise ValueError(f"Missing columns. Tried {candidates}. Sample cols: {list(cols)[:50]}")

dbm_src = spark.table("dm_ib.digital_banking_master")

# IMPORTANT: repo uses relt_ibn as the IBN key
ibn_col = first_existing_col(dbm_src, "relt_ibn", "ibn")

dbm = (
    dbm_src.alias("dbm")
    .where((F.to_date(F.col("dbm.ods_business_dt")) >= start_dt) & (F.to_date(F.col("dbm.ods_business_dt")) <= end_dt))
    .select(
        F.trunc(F.to_date(F.col("dbm.ods_business_dt")), "MM").alias("month_dt"),
        F.to_date(F.col("dbm.ods_business_dt")).alias("ods_business_dt"),
        F.upper(F.trim(F.col(f"dbm.{ibn_col}").cast("string"))).alias("reltibn"),
        F.col("dbm.rcif_customer_nbr").cast("string").alias("rcif_customer_nbr"),
        F.to_date(F.col("dbm.olb_last_login_date")).alias("lst_login_olb"),
        F.to_date(F.col("dbm.mob_last_login_date")).alias("lst_login_mob"),
    )
    .where(F.col("reltibn").isNotNull() & (F.length(F.col("reltibn")) > 0))
)

# -------------------------
# Dig_Customer CTE (repo)
# group by month_dt, reltibn, rcif_customer_nbr
# take max logins + max ods date
# -------------------------
dig_customer = (
    dbm.groupBy("month_dt", "reltibn", "rcif_customer_nbr")
       .agg(
           F.max("lst_login_olb").alias("lst_login_olb"),
           F.max("lst_login_mob").alias("lst_login_mob"),
           F.max("ods_business_dt").alias("ods_business_dt")
       )
)

# -------------------------
# Flags (repo): datediff(c.ods_business_dt, c.lst_login_*) <= 90
# NOTE: this matches your screenshot exactly
# -------------------------
digital = (
    dig_customer
    .withColumn(
        "mobile_active_flag",
        F.when(
            F.col("lst_login_mob").isNotNull() &
            (F.datediff(F.col("ods_business_dt"), F.col("lst_login_mob")) <= 90),
            F.lit("Mobile Active")
        ).otherwise(F.lit("Non Mobile Active"))
    )
    .withColumn(
        "mobile_flag",
        F.when(F.col("lst_login_mob").isNull(), F.lit("Non Mobile User"))
         .otherwise(F.lit("Mobile User"))
    )
    .withColumn(
        "olb_active_flag",
        F.when(
            F.col("lst_login_olb").isNotNull() &
            (F.datediff(F.col("ods_business_dt"), F.col("lst_login_olb")) <= 90),
            F.lit("OLB Active")
        ).otherwise(F.lit("Non OLB Active"))
    )
    .withColumn(
        "olb_flag",
        F.when(F.col("lst_login_olb").isNull(), F.lit("Non OLB User"))
         .otherwise(F.lit("OLB User"))
    )
    .withColumn(
        "digitally_active_flag",
        F.when(
            (
                F.col("lst_login_mob").isNotNull() &
                (F.datediff(F.col("ods_business_dt"), F.col("lst_login_mob")) <= 90)
            ) |
            (
                F.col("lst_login_olb").isNotNull() &
                (F.datediff(F.col("ods_business_dt"), F.col("lst_login_olb")) <= 90)
            ),
            F.lit("Digital Active")
        ).otherwise(F.lit("Non Digital Active"))
    )
    .withColumn("digital_flag", F.lit("Digital User"))
    .select(
        "month_dt", "ods_business_dt",
        "reltibn", "rcif_customer_nbr",
        "mobile_active_flag", "mobile_flag",
        "olb_active_flag", "olb_flag",
        "digitally_active_flag", "digital_flag"
    )
)

# =========================
# WRITE DIGITAL TABLE ONLY
# =========================
spark.sql(f"DROP TABLE IF EXISTS {DIG_FQN}")
digital.write.mode("overwrite").saveAsTable(DIG_FQN)

print("✅ Created digital table:", DIG_FQN)

# ==========================================================
# SANITY CHECKS — DO IT THE SAME WAY THE REPORT DOES:
# use the LATEST month in the table, not the whole 6-month window
# ==========================================================
latest_month = digital.select(F.max("month_dt").alias("mx")).first()["mx"]
print("Latest month_dt in digital:", latest_month)

print("DIGITAL active distinct IBN (latest month):")
digital.filter(
        (F.col("month_dt") == F.lit(latest_month)) &
        (F.col("digitally_active_flag") == "Digital Active")
    ).selectExpr("count(distinct reltibn) as digital_active_ibn").show(truncate=False)

# If you want Wealth Digital RCIF (latest month) using your Wealth table already created as WEALTH_FQN:
WEALTH_FQN = f"{DB}.wealth_rcif_202507_202512"
wealth_rcif = spark.table(WEALTH_FQN).select(
    F.col("rcif_number").cast("string").alias("rcif_number"),
    F.upper(F.trim(F.col("cust_internet_banking_nbr").cast("string"))).alias("cust_internet_banking_nbr")
)

print("WEALTH digital RCIF (latest month, using RCIF join like your model expects):")
wealth_digital_rcif = (
    wealth_rcif.join(
        digital.filter(
            (F.col("month_dt") == F.lit(latest_month)) &
            (F.col("digitally_active_flag") == "Digital Active")
        ).select(F.col("rcif_customer_nbr").alias("rcif_number")).dropDuplicates(),
        on="rcif_number",
        how="inner"
    )
)
wealth_digital_rcif.selectExpr("count(distinct rcif_number) as wealth_digital_rcif").show(truncate=False)

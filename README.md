from pyspark.sql import functions as F

START_DATE = "2025-08-01"
END_DATE   = "2026-01-31"

DBM = spark.table("dm_ib.digital_banking_master")
IP  = spark.table("eil.d_involved_party_h")

# -----------------------------
# column guards
# -----------------------------
ibn_col = first_existing_col(DBM, "relt_ibn", "ibn")
olb_col = first_existing_col(DBM, "olb_last_login_date", "olb_last_login_dt")
mob_col = first_existing_col(DBM, "mob_last_login_date", "mob_last_login_dt")

ip_ibn_col  = first_existing_col(IP, "cust_internet_banking_nbr")
ip_rcif_col = first_existing_col(IP, "rcif_cust_nbr", "rcif_customer_nbr", "rcif_nbr")
ip_id_col   = first_existing_col(IP, "involved_party_id", "ip_id")

# -----------------------------
# snapshot date WITHIN window
# -----------------------------
snapshot_dt = (
    DBM
    .where(
        (F.to_date("ods_business_dt") >= F.lit(START_DATE)) &
        (F.to_date("ods_business_dt") <= F.lit(END_DATE))
    )
    .select(F.max(F.to_date("ods_business_dt")).alias("dt"))
    .first()["dt"]
)

# -----------------------------
# Dig_Customer (exact logic)
# -----------------------------
dig_customer = (
    DBM
    .where(F.to_date("ods_business_dt") == F.lit(snapshot_dt))
    .select(
        F.upper(F.trim(F.col(ibn_col))).alias("reltibn"),
        F.to_date("ods_business_dt").alias("ods_business_dt"),
        F.to_date(F.col(olb_col)).alias("lst_login_olb"),
        F.to_date(F.col(mob_col)).alias("lst_login_mob"),
    )
    .groupBy("reltibn", "ods_business_dt")
    .agg(
        F.max("lst_login_olb").alias("lst_login_olb"),
        F.max("lst_login_mob").alias("lst_login_mob")
    )
)

# -----------------------------
# RCIF mapping (mandatory)
# -----------------------------
ip_snapshot_dt = (
    IP.select(F.max(F.to_date("business_date")).alias("dt"))
      .first()["dt"]
)

rcif_map = (
    IP
    .where(F.to_date("business_date") == F.lit(ip_snapshot_dt))
    .where(F.col("source_system_code") == "CF")
    .where(F.coalesce(F.col("deceased_ind"), F.lit("N")) == "N")
    .select(
        F.upper(F.trim(F.col(ip_ibn_col))).alias("reltibn"),
        F.col(ip_rcif_col).cast("string").alias("rcif_number"),
        F.col(ip_ibn_col).alias("cust_internet_banking_nbr")
    )
)

# -----------------------------
# wid2 FINAL
# -----------------------------
wid2 = (
    dig_customer
    .join(rcif_map, on="reltibn", how="inner")
    .withColumn(
        "mobile_active_flag",
        F.when(F.datediff("ods_business_dt", "lst_login_mob") <= 90,
               F.lit("Mobile Active"))
         .otherwise(F.lit("Non Mobile Active"))
    )
    .withColumn(
        "olb_active_flag",
        F.when(F.datediff("ods_business_dt", "lst_login_olb") <= 90,
               F.lit("OLB Active"))
         .otherwise(F.lit("Non OLB Active"))
    )
    .withColumn(
        "digitally_active_flag",
        F.when(
            (F.datediff("ods_business_dt", "lst_login_mob") <= 90) |
            (F.datediff("ods_business_dt", "lst_login_olb") <= 90),
            F.lit("Digital Active")
        ).otherwise(F.lit("Non Digital Active"))
    )
    .select(
        "ods_business_dt",
        "reltibn",
        "rcif_number",
        "cust_internet_banking_nbr",
        "mobile_active_flag",
        "olb_active_flag",
        "digitally_active_flag"
    )
)

# -----------------------------
# sanity check
# -----------------------------
print("Snapshot date used:", snapshot_dt)

wid2.where(F.col("digitally_active_flag") == "Digital Active") \
    .selectExpr("count(distinct reltibn) as digital_active_ibn") \
    .show(truncate=False)

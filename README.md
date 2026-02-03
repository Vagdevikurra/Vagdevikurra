from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.getOrCreate()

DB = "dm_ib_dev"
WIC_FQN = f"{DB}.wic2"

IND_TBL = "eil.m_involved_party_h"
A2I_TBL = "eil.m_arrangement_to_involved_party_relationship_h"
AR_TBL  = "eil.m_arrangement_h"

AR_SOURCE_SYSTEM_LIST = ['BI','RN','TR','DA','SV','CC','LS','MG','TM','PC','LO','BW','CS','IC','MA','PF','PR','SD','CM','EL']
AR_CLOSED_ONLY = "N"
IND_SOURCE_SYSTEM = "CF"

# 1) AS-OF date (latest business_date in last 6 months)
as_of_dt = (
    spark.table(IND_TBL)
    .select(F.to_date("business_date").alias("bd"))
    .where(F.to_date("business_date") >= F.add_months(F.current_date(), -6))
    .agg(F.max("bd").alias("as_of_dt"))
    .collect()[0]["as_of_dt"]
)
if as_of_dt is None:
    raise RuntimeError("No business_date found in last 6 months in eil.m_involved_party_h")

# 2) involved_party_h at AS-OF date + CF + not deceased (only needed cols)
ind = (
    spark.table(IND_TBL).alias("ind")
    .where(F.to_date("ind.business_date") == F.lit(as_of_dt))
    .where(F.col("ind.source_system_code") == F.lit(IND_SOURCE_SYSTEM))
    .where(F.coalesce(F.col("ind.deceased_ind"), F.lit("N")) == F.lit("N"))
    .select(
        F.to_date("ind.business_date").alias("business_date"),
        F.col("ind.involved_party_id"),
        F.col("ind.rcif_cust_nbr").alias("rcif_number"),
        F.col("ind.cust_internet_banking_nbr").alias("cust_internet_banking_nbr"),
        F.col("ind.private_client_code"),
        F.col("ind.private_client_trust_code"),
        F.col("ind.source_system_code").alias("ind_source_system_code"),
    )
)

# 3) a2i (only join keys + arrangement ids)
a2i = (
    spark.table(A2I_TBL).alias("a2i")
    .select(
        F.to_date("a2i.business_date").alias("business_date"),
        F.col("a2i.involved_party_id"),
        F.col("a2i.source_system_code").alias("a2i_source_system_code"),
        F.col("a2i.arrangement_id"),
        F.col("a2i.arrangement_source_system_code"),
    )
)

# 4) arrangement_h (only join keys + fields used in logic)
ar = (
    spark.table(AR_TBL).alias("ar")
    .select(
        F.to_date("ar.business_date").alias("business_date"),
        F.col("ar.arrangement_id"),
        F.col("ar.source_system_code").alias("ar_source_system_code"),
        F.col("ar.closed_ind"),
        F.col("ar.business_service_segment_type_code"),
    )
    .where(F.col("ar.source_system_code").isin(AR_SOURCE_SYSTEM_LIST))
    .where(F.col("ar.closed_ind") == F.lit(AR_CLOSED_ONLY))
)

# 5) Join chain (same as your SQL)
j = (
    ind.join(
        a2i,
        on=[
            ind["involved_party_id"] == a2i["involved_party_id"],
            ind["business_date"] == a2i["business_date"],
            ind["ind_source_system_code"] == a2i["a2i_source_system_code"],
        ],
        how="inner",
    )
    .join(
        ar,
        on=[
            a2i["arrangement_id"] == ar["arrangement_id"],
            a2i["arrangement_source_system_code"] == ar["ar_source_system_code"],
            a2i["business_date"] == ar["business_date"],
        ],
        how="inner",
    )
)

# 6) Business_Group (same logic)
business_group = (
    F.when(F.col("private_client_code").isin("039","539","339"), F.lit("Private Wealth"))
     .when(F.col("private_client_trust_code").isin("239","739"), F.lit("Private Wealth"))
     .otherwise(
        F.when(F.col("business_service_segment_type_code").isin("IS_CT","IS_IT"), F.lit("Institutional Services"))
         .when(F.col("business_service_segment_type_code").isin("REGIS_FC","REGIS"), F.lit("Investment Services"))
         .when(F.col("business_service_segment_type_code") == F.lit("PWM"), F.lit("Private Wealth"))
         .otherwise(F.concat(F.col("business_service_segment_type_code"), F.lit(": Category??")))
     )
)

# 7) Aggregate only what we need to compute division (helper counts kept only temporarily)
agg = (
    j.withColumn("business_group", business_group)
     .groupBy(
         "business_date",
         "rcif_number",
         "cust_internet_banking_nbr",
         "business_group"
     )
     .agg(
        F.countDistinct(F.when(F.col("business_service_segment_type_code") == "IS_CT", F.col("arrangement_id"))).alias("Corporate_Trust_Count"),
        F.countDistinct(F.when(F.col("business_service_segment_type_code") == "IS_IT", F.col("arrangement_id"))).alias("Institutional_Trust_Count"),
        F.countDistinct(F.when(F.col("business_service_segment_type_code") == "REGIS_FC", F.col("arrangement_id"))).alias("Investment_Count"),
        F.countDistinct(F.when(F.col("business_service_segment_type_code") == "REGIS", F.col("arrangement_id"))).alias("Insurance_Count"),
        F.countDistinct(F.when(F.col("business_service_segment_type_code") == "PWM", F.col("arrangement_id"))).alias("PWM_Count"),
        F.countDistinct(F.when(F.col("ar_source_system_code") == "TR", F.col("arrangement_id"))).alias("Trust_Count"),
        F.countDistinct(F.when(F.col("ar_source_system_code").isin(
            "DA","SV","CC","MG","LS","TM","PC","LO","BW","CM","CS","EL","IC","MA","PF","PR","SD"
        ), F.col("arrangement_id"))).alias("Banking_Count"),
        F.count(F.col("arrangement_id")).alias("accts_cnt"),
     )
)

# 8) Division (same logic)
division = (
    F.when(
        F.col("business_group") == "Private Wealth",
        F.when((F.col("Trust_Count") > 0) & (F.col("Banking_Count") > 0), F.lit("Banking & IM&T"))
         .otherwise(
            F.when((F.col("Investment_Count") + F.col("Trust_Count") > 0) & (F.col("Banking_Count") == 0), F.lit("Investments Only"))
             .otherwise(F.lit("Banking only"))
         )
    )
    .when(
        F.col("business_group") == "Investment Services",
        F.when((F.col("Investment_Count") > 0) & (F.col("Insurance_Count") == 0), F.lit("Investment"))
         .when((F.col("Investment_Count") == 0) & (F.col("Insurance_Count") > 0), F.lit("Insurance"))
         .otherwise(F.lit("Insurance & Investment"))
    )
    .otherwise(
        F.when((F.col("Corporate_Trust_Count") > 0) & (F.col("Institutional_Trust_Count") == 0), F.lit("Corporate Trust"))
         .when((F.col("Corporate_Trust_Count") == 0) & (F.col("Institutional_Trust_Count") > 0), F.lit("Institutional Trust"))
         .when(F.col("PWM_Count") > 0, F.lit("Banking only"))
         .otherwise(F.lit("Corporate & Institutional Trust"))
    )
)

# 9) FINAL wic2: only necessary columns (drop helper counts)
wic2_df = (
    agg.withColumn("division", division)
       .select(
           "business_date",
           "rcif_number",
           "cust_internet_banking_nbr",
           "business_group",
           "division",
           "accts_cnt",          # remove this line if you don’t want it
       )
)

spark.sql(f"DROP TABLE IF EXISTS {WIC_FQN}")
(
    wic2_df.write
    .mode("overwrite")
    .format("parquet")
    .saveAsTable(WIC_FQN)
)

print("AS-OF business_date:", as_of_dt)
print("wic2 rows:", wic2_df.count())
print("wic2 distinct rcif:", wic2_df.select("rcif_number").distinct().count())

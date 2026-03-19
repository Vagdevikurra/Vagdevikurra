from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark import SparkConf

# ── Configuration ────────────────────────────────────────────────────────────────
DEFAULT_DB = "dm_ib_dev"
EIL_DB     = "eil"
DMIB_DB    = "dm_ib"
START_DT   = "2025-09-01"
END_DT     = "2026-02-28"

# Wealth source systems — PC and BW excluded as requested
WEALTH_SRC_LIST = [
    "BI", "TR", "DA", "SV", "CC", "MG", "LS", "TM", "LO",
    "CS", "IC", "MA", "PF", "PR", "SD", "CM", "EL", "RN"
]

APPLY_PRIMARY_OWNER_FILTER = False
IP_FUNDED_BALANCE_THRESHOLD = 0.0
DEBUG = False

conf = (
    SparkConf()
    .setAppName("wealth_insights")
    .set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .set("spark.sql.autoBroadcastJoinThreshold", "-1")
)

spark = (
    SparkSession.builder
    .config(conf=conf)
    .enableHiveSupport()
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ── Source Tables ────────────────────────────────────────────────────────────────

# 0) Latest snapshot dates — customer & address
cust_dt = spark.sql(f"""
    SELECT MAX(CAST(business_date AS date)) AS dt
    FROM {EIL_DB}.d_involved_party_h
    WHERE source_system_code = 'CF'
""").collect()[0]["dt"]

addr_dt = spark.sql(f"""
    SELECT MAX(CAST(business_date AS date)) AS dt
    FROM {EIL_DB}.d_involved_party_address_h
""").collect()[0]["dt"]

print(f"[INFO] d_involved_party_h  (CF) snapshot : {cust_dt}")
print(f"[INFO] d_involved_party_address_h snapshot : {addr_dt}")
print(f"[INFO] Window : {START_DT} .. {END_DT}")


# ── 1) Month-end business dates ─────────────────────────────────────────────────
# d_ tables only carry weekday data.  For months whose calendar last day falls on
# a weekend / holiday (e.g. Aug 31 Sun, Nov 30 Sun, Jan 31 Sat) we pick the
# latest available business day inside that calendar month.

wealth_src_csv = ",".join([f"'{s}'" for s in WEALTH_SRC_LIST])

spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW month_ends AS
    WITH cal AS (
        SELECT add_months(date('{START_DT}'), n) AS month_start,
               add_months(date('{START_DT}'), n + 1) AS next_month_start
        FROM (
            SELECT sequence(
                0,
                CAST(months_between(date('{END_DT}'), date('{START_DT}')) AS INT)
            ) AS s
        ) t
        LATERAL VIEW posexplode(s) pe AS n, _
    ),
    all_dates AS (
        SELECT DISTINCT CAST(business_date AS date) AS bd
          FROM {EIL_DB}.d_involved_party_h
        UNION
        SELECT DISTINCT CAST(business_date AS date)
          FROM {EIL_DB}.d_arrangement_to_involved_party_relationship_h
        UNION
        SELECT DISTINCT CAST(business_date AS date)
          FROM {EIL_DB}.d_arrangement_h
    )
    SELECT MAX(a.bd) AS business_date
      FROM cal m
      LEFT JOIN all_dates a
        ON a.bd >= m.month_start AND a.bd < m.next_month_start
     GROUP BY m.month_start
     ORDER BY m.month_start
""")

spark.sql("CACHE TABLE month_ends")
_ = spark.sql("SELECT COUNT(*) AS cnt FROM month_ends").collect()

if DEBUG:
    print("\n[1] month_ends (6 month-end business dates):")
    spark.sql("SELECT * FROM month_ends ORDER BY business_date").show(truncate=False)


# ── 2) WEALTH base — arrangement grain ──────────────────────────────────────────
primary_owner_pred = "1=1"
if APPLY_PRIMARY_OWNER_FILTER:
    primary_owner_pred = "COALESCE(a2i.relationship_role,'') = 'PRIMARY'"

spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW wealth_arr AS
    SELECT
        CAST(ind.business_date AS date)        AS business_date,
        CAST(ind.rcif_cust_nbr AS string)      AS rcif_number,
        ind.involved_party_id                  AS ip_id,
        ind.cust_internet_banking_nbr          AS cust_internet_banking_nbr,
        ind.private_client_code,
        ind.private_client_trust_code,
        ar.arrangement_id,
        ar.source_system_code,
        ar.business_service_segment_type_code
    FROM {EIL_DB}.d_involved_party_h ind
    JOIN month_ends d
        ON CAST(ind.business_date AS date) = d.business_date
    JOIN {EIL_DB}.d_arrangement_to_involved_party_relationship_h a2i
        ON  ind.involved_party_id   = a2i.involved_party_id
        AND ind.business_date       = a2i.business_date
        AND ind.source_system_code  = a2i.source_system_code
    JOIN {EIL_DB}.d_arrangement_h ar
        ON  a2i.arrangement_id                 = ar.arrangement_id
        AND a2i.arrangement_source_system_code = ar.source_system_code
        AND a2i.business_date                  = ar.business_date
    WHERE ind.source_system_code = 'CF'
      AND NVL(ind.deceased_ind, 'N') = 'N'
      AND ar.closed_ind = 'N'
      AND {primary_owner_pred}
      AND ar.source_system_code IN ({wealth_src_csv})
      AND (
            CASE
                WHEN ind.private_client_code      IN ('039','539','339') THEN 1
                WHEN ind.private_client_trust_code IN ('239','739')      THEN 1
                ELSE CASE
                    WHEN ar.business_service_segment_type_code
                         IN ('IS_CT','IS_IT','REGIS_FC','REGIS','PWM') THEN 1
                    ELSE 0
                END
            END
          ) = 1
""")

if DEBUG:
    print("\n[2] wealth_arr sample:")
    spark.sql("SELECT * FROM wealth_arr LIMIT 10").show(truncate=False)


# ── 3) WEALTH aggregate — RCIF grain ────────────────────────────────────────────
spark.sql("""
    CREATE OR REPLACE TEMP VIEW wealth_agg AS
    WITH base AS (
        SELECT
            business_date,
            rcif_number,
            ip_id,
            cust_internet_banking_nbr,
            private_client_code,
            private_client_trust_code,
            source_system_code,
            business_service_segment_type_code,
            concat_ws('|', source_system_code, CAST(arrangement_id AS string)) AS acct_key
        FROM wealth_arr
    ),
    by_rcif AS (
        SELECT
            business_date,
            rcif_number,
            MAX(ip_id)                       AS ip_id,
            MAX(cust_internet_banking_nbr)   AS cust_internet_banking_nbr,
            COUNT(DISTINCT acct_key)         AS wealth_accts_cnt,

            -- segment / system counts for business_group & division
            COUNT(DISTINCT CASE WHEN business_service_segment_type_code = 'IS_CT'    THEN acct_key END) AS corporate_trust_count,
            COUNT(DISTINCT CASE WHEN business_service_segment_type_code = 'IS_IT'    THEN acct_key END) AS institutional_trust_count,
            COUNT(DISTINCT CASE WHEN business_service_segment_type_code = 'REGIS_FC' THEN acct_key END) AS investment_count,
            COUNT(DISTINCT CASE WHEN business_service_segment_type_code = 'REGIS'    THEN acct_key END) AS insurance_count,
            COUNT(DISTINCT CASE WHEN business_service_segment_type_code = 'PWM'      THEN acct_key END) AS pwm_count,

            COUNT(DISTINCT CASE WHEN source_system_code = 'TR' THEN acct_key END) AS trust_count,
            COUNT(DISTINCT CASE WHEN source_system_code IN
                  ('DA','SV','CC','MG','LS','TM','LO','CM','CS','EL',
                   'IC','MA','PF','PR','SD','BI','RN')
                  THEN acct_key END) AS banking_count,

            MAX(CASE WHEN private_client_code      IN ('039','539','339')
                       OR private_client_trust_code IN ('239','739')
                     THEN 1 ELSE 0 END) AS private_flag
        FROM base
        GROUP BY business_date, rcif_number
    )
    SELECT
        business_date,
        rcif_number,
        ip_id,
        cust_internet_banking_nbr,
        wealth_accts_cnt,

        -- Business group at RCIF grain
        CASE
            WHEN private_flag = 1 THEN 'Private Wealth'
            WHEN (corporate_trust_count + institutional_trust_count) > 0 THEN 'Institutional Services'
            WHEN (investment_count + insurance_count) > 0               THEN 'Investment Services'
            WHEN pwm_count > 0                                          THEN 'Private Wealth'
            ELSE 'Other'
        END AS business_group,

        -- Division at RCIF grain
        CASE
            -- ── Private Wealth sub-divisions ──
            WHEN CASE
                     WHEN private_flag = 1 THEN 'Private Wealth'
                     WHEN (corporate_trust_count + institutional_trust_count) > 0 THEN 'Institutional Services'
                     WHEN (investment_count + insurance_count) > 0               THEN 'Investment Services'
                     WHEN pwm_count > 0                                          THEN 'Private Wealth'
                     ELSE 'Other'
                 END = 'Private Wealth'
            THEN CASE
                     WHEN trust_count > 0 AND banking_count > 0                        THEN 'Banking & IMAT'
                     WHEN (investment_count + trust_count) > 0 AND banking_count = 0   THEN 'Investments Only'
                     ELSE 'Banking only'
                 END
            -- ── Investment Services sub-divisions ──
            WHEN CASE
                     WHEN private_flag = 1 THEN 'Private Wealth'
                     WHEN (corporate_trust_count + institutional_trust_count) > 0 THEN 'Institutional Services'
                     WHEN (investment_count + insurance_count) > 0               THEN 'Investment Services'
                     WHEN pwm_count > 0                                          THEN 'Private Wealth'
                     ELSE 'Other'
                 END = 'Investment Services'
            THEN CASE
                     WHEN investment_count > 0 AND insurance_count = 0 THEN 'Investment'
                     WHEN investment_count > 0 AND insurance_count > 0 THEN 'Insurance'
                     ELSE 'Insurance & Investment'
                 END
            -- ── Institutional Services sub-divisions ──
            WHEN (corporate_trust_count > 0 AND institutional_trust_count = 0) THEN 'Corporate Trust'
            WHEN (corporate_trust_count = 0 AND institutional_trust_count > 0) THEN 'Institutional Trust'
            WHEN pwm_count > 0                                                 THEN 'Banking only'
            ELSE 'Corporate & Institutional Trust'
        END AS division

    FROM by_rcif
""")

if DEBUG:
    print("\n[3] wealth_agg sample:")
    spark.sql("SELECT * FROM wealth_agg LIMIT 10").show(truncate=False)


# ── 4) INVESTPATH (RN + IP) — arrangement grain, monthly as-of ──────────────────
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW investpath_arr AS
    WITH anchors AS (
        SELECT m.business_date AS anchor_dt,
               TRUNC(m.business_date, 'MM') AS month_start
          FROM month_ends m
    ),
    -- best available snapshot date per table per month
    ind_snap AS (
        SELECT a.anchor_dt,
               MAX(CAST(ind.business_date AS DATE)) AS ind_bd
          FROM anchors a
          JOIN {EIL_DB}.d_involved_party_h ind
            ON CAST(ind.business_date AS DATE) >= a.month_start
           AND CAST(ind.business_date AS DATE) <= a.anchor_dt
         WHERE ind.source_system_code = 'CF'
         GROUP BY a.anchor_dt
    ),
    a2i_snap AS (
        SELECT a.anchor_dt,
               MAX(CAST(a2i.business_date AS DATE)) AS a2i_bd
          FROM anchors a
          JOIN {EIL_DB}.d_arrangement_to_involved_party_relationship_h a2i
            ON CAST(a2i.business_date AS DATE) >= a.month_start
           AND CAST(a2i.business_date AS DATE) <= a.anchor_dt
         GROUP BY a.anchor_dt
    ),
    ar_snap AS (
        SELECT a.anchor_dt,
               MAX(CAST(ar.business_date AS DATE)) AS ar_bd
          FROM anchors a
          JOIN {EIL_DB}.d_arrangement_h ar
            ON CAST(ar.business_date AS DATE) >= a.month_start
           AND CAST(ar.business_date AS DATE) <= a.anchor_dt
         WHERE ar.source_system_code = 'RN'
           AND ar.account_type_code  = 'IP'
           AND ar.closed_ind         = 'N'
         GROUP BY a.anchor_dt
    ),
    -- materialise rows at the best-available date for each anchor
    ind_at AS (
        SELECT s.anchor_dt                          AS business_date,
               ind.involved_party_id                AS ip_id,
               CAST(ind.rcif_cust_nbr AS STRING)    AS rcif_number,
               ind.cust_internet_banking_nbr
          FROM ind_snap s
          JOIN {EIL_DB}.d_involved_party_h ind
            ON CAST(ind.business_date AS DATE) = s.ind_bd
         WHERE ind.source_system_code      = 'CF'
           AND NVL(ind.deceased_ind, 'N')  = 'N'
    ),
    a2i_at AS (
        SELECT s.anchor_dt                              AS business_date,
               a2i.involved_party_id                    AS ip_id,
               a2i.arrangement_id,
               a2i.arrangement_source_system_code       AS arr_src
          FROM a2i_snap s
          JOIN {EIL_DB}.d_arrangement_to_involved_party_relationship_h a2i
            ON CAST(a2i.business_date AS DATE) = s.a2i_bd
    ),
    ar_at AS (
        SELECT s.anchor_dt                                              AS business_date,
               ar.arrangement_id,
               ar.source_system_code,
               CAST(COALESCE(ar.current_balance_amt, 0.0) AS DOUBLE)   AS ip_balance,
               CAST(ar.open_date AS DATE)                              AS ip_open_date
          FROM ar_snap s
          JOIN {EIL_DB}.d_arrangement_h ar
            ON CAST(ar.business_date AS DATE) = s.ar_bd
         WHERE ar.source_system_code = 'RN'
           AND ar.account_type_code  = 'IP'
           AND ar.closed_ind         = 'N'
    ),
    ip_joined AS (
        SELECT i.business_date,
               i.rcif_number,
               i.cust_internet_banking_nbr,
               i.ip_id,
               CONCAT_WS('|', ar.source_system_code,
                          CAST(ar.arrangement_id AS STRING)) AS ip_account_id,
               ar.ip_balance,
               ar.ip_open_date
          FROM ind_at  i
          JOIN a2i_at  a2i
            ON  a2i.ip_id         = i.ip_id
           AND a2i.business_date  = i.business_date
          JOIN ar_at   ar
            ON  ar.arrangement_id      = a2i.arrangement_id
           AND ar.source_system_code   = a2i.arr_src
           AND ar.business_date        = i.business_date
         WHERE {primary_owner_pred}
    )
    SELECT business_date,
           rcif_number,
           cust_internet_banking_nbr,
           ip_id,
           ip_account_id,
           MAX(ip_balance)   AS ip_balance,
           MIN(ip_open_date) AS ip_open_date
      FROM ip_joined
     GROUP BY business_date, rcif_number, cust_internet_banking_nbr,
              ip_id, ip_account_id
""")

# RCIF-level aggregate for the customer-table join
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW investpath_agg AS
    SELECT
        business_date,
        rcif_number,
        COUNT(DISTINCT ip_account_id)  AS ip_accounts_cnt,
        SUM(COALESCE(ip_balance, 0.0)) AS ip_sum,
        COUNT(DISTINCT CASE WHEN ip_balance > {IP_FUNDED_BALANCE_THRESHOLD}
                            THEN ip_account_id END) AS ip_funded_accounts_cnt,
        CASE WHEN COUNT(DISTINCT ip_account_id) > 0 THEN 1 ELSE 0 END AS ip_customers_flag
    FROM investpath_arr
    GROUP BY business_date, rcif_number
""")

if DEBUG:
    print("\n[4] investpath_arr / investpath_agg samples:")
    spark.sql("SELECT * FROM investpath_arr  LIMIT 5").show(truncate=False)
    spark.sql("SELECT * FROM investpath_agg  LIMIT 5").show(truncate=False)


# ── 5) DIGITAL monthly ──────────────────────────────────────────────────────────
# NOTE: digital_banking_master uses column `ibn` (not reltibn).
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW digital_monthly AS
    WITH dig_raw AS (
        SELECT
            TRUNC(ods_business_dt, 'MM')            AS month_dt,
            CAST(rcif_customer_nbr AS string)       AS rcif_number,
            ibn,
            MAX(olb_last_login_date)                AS last_olb,
            MAX(mob_last_login_date)                AS last_mob,
            MAX(ods_business_dt)                    AS ods_dt
        FROM {DMIB_DB}.digital_banking_master
        WHERE ods_business_dt >= date('{START_DT}')
          AND ods_business_dt <= date('{END_DT}')
        GROUP BY TRUNC(ods_business_dt, 'MM'),
                 CAST(rcif_customer_nbr AS string),
                 ibn
    )
    SELECT
        month_dt,
        rcif_number,
        ibn,
        ods_dt,

        -- Mobile flags
        CASE WHEN last_mob IS NULL THEN 'Non Mobile User'
             ELSE 'Mobile User'
        END AS mobile_flag,

        CASE WHEN last_mob IS NOT NULL
              AND datediff(ods_dt, last_mob) <= 90
             THEN 'Mobile Active'
             ELSE 'Non Mobile Active'
        END AS mobile_active_flag,

        -- OLB flags
        CASE WHEN last_olb IS NULL THEN 'Non OLB User'
             ELSE 'OLB User'
        END AS olb_flag,

        CASE WHEN last_olb IS NOT NULL
              AND datediff(ods_dt, last_olb) <= 90
             THEN 'OLB Active'
             ELSE 'Non OLB Active'
        END AS olb_active_flag,

        -- Digital enrollment flag  (ibn presence = enrolled)
        CASE WHEN ibn IS NULL THEN 'Non Digital User'
             ELSE 'Digital User'
        END AS digital_flag,

        -- Digital active = either channel active within 90 days
        CASE WHEN (last_mob IS NOT NULL AND datediff(ods_dt, last_mob) <= 90)
               OR (last_olb IS NOT NULL AND datediff(ods_dt, last_olb) <= 90)
             THEN 'Digital Active'
             ELSE 'Non Digital Active'
        END AS digital_active_flag

    FROM dig_raw
""")

# Per-month, per-RCIF: a customer may have multiple ibn rows.
# "Any active wins" — take the best flag value per customer per month.
spark.sql("""
    CREATE OR REPLACE TEMP VIEW digital_agg AS
    SELECT
        month_dt,
        rcif_number,

        CASE WHEN MAX(CASE WHEN mobile_flag        = 'Mobile User'       THEN 1 ELSE 0 END) = 1
             THEN 'Mobile User'       ELSE 'Non Mobile User'       END AS mobile_flag,
        CASE WHEN MAX(CASE WHEN mobile_active_flag  = 'Mobile Active'    THEN 1 ELSE 0 END) = 1
             THEN 'Mobile Active'     ELSE 'Non Mobile Active'     END AS mobile_active_flag,
        CASE WHEN MAX(CASE WHEN olb_flag            = 'OLB User'         THEN 1 ELSE 0 END) = 1
             THEN 'OLB User'          ELSE 'Non OLB User'          END AS olb_flag,
        CASE WHEN MAX(CASE WHEN olb_active_flag     = 'OLB Active'       THEN 1 ELSE 0 END) = 1
             THEN 'OLB Active'        ELSE 'Non OLB Active'        END AS olb_active_flag,
        CASE WHEN MAX(CASE WHEN digital_flag        = 'Digital User'     THEN 1 ELSE 0 END) = 1
             THEN 'Digital User'      ELSE 'Non Digital User'      END AS digital_flag,
        CASE WHEN MAX(CASE WHEN digital_active_flag = 'Digital Active'   THEN 1 ELSE 0 END) = 1
             THEN 'Digital Active'    ELSE 'Non Digital Active'    END AS digital_active_flag

    FROM digital_monthly
    GROUP BY month_dt, rcif_number
""")

if DEBUG:
    print("\n[5] digital_agg sample:")
    spark.sql("SELECT * FROM digital_agg LIMIT 10").show(truncate=False)


# ── 6) Address snapshot ─────────────────────────────────────────────────────────
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW rcif_address AS
    WITH addr_ranked AS (
        SELECT
            involved_party_id                 AS ip_id,
            state_name,
            ROW_NUMBER() OVER (
                PARTITION BY involved_party_id
                ORDER BY NVL(state_name, '') DESC
            ) AS rn
        FROM {EIL_DB}.d_involved_party_address_h
        WHERE CAST(business_date AS date) = date('{addr_dt}')
    )
    SELECT ip_id, state_name
      FROM addr_ranked
     WHERE rn = 1
""")


# ── 7) Customer Table — wealth_insights_cust ────────────────────────────────────
# Grain: (business_date, rcif_number) — one row per customer per month-end.
# business_date holds the 6 month-end dates (Sep–Feb).
# Wealth data joined with digital flags matched by calendar month.

spark.sql("""
    CREATE OR REPLACE TEMP VIEW wealth_insights_cust AS
    SELECT
        w.business_date,
        w.rcif_number,
        w.cust_internet_banking_nbr,
        w.ip_id,
        w.business_group,
        w.division,
        w.wealth_accts_cnt,

        -- digital flags (default to "Non …" when no digital record for this month)
        COALESCE(d.mobile_flag,        'Non Mobile User')    AS mobile_flag,
        COALESCE(d.mobile_active_flag, 'Non Mobile Active')  AS mobile_active_flag,
        COALESCE(d.olb_flag,           'Non OLB User')       AS olb_flag,
        COALESCE(d.olb_active_flag,    'Non OLB Active')     AS olb_active_flag,
        COALESCE(d.digital_flag,       'Non Digital User')   AS digital_flag,
        COALESCE(d.digital_active_flag,'Non Digital Active')  AS digital_active_flag,

        -- fact_type: defines whether the customer is wealth-only or also digital
        CASE WHEN d.rcif_number IS NOT NULL THEN 'WEALTH & DIGITAL'
             ELSE 'WEALTH'
        END AS fact_type

    FROM wealth_agg w
    LEFT JOIN digital_agg d
        ON  w.rcif_number = d.rcif_number
        AND TRUNC(w.business_date, 'MM') = d.month_dt
""")

if DEBUG:
    print("\n[7] wealth_insights_cust sample:")
    spark.sql("""
        SELECT business_date, rcif_number, business_group, division,
               wealth_accts_cnt, digital_flag, digital_active_flag, fact_type
          FROM wealth_insights_cust
         ORDER BY business_date, rcif_number
         LIMIT 20
    """).show(truncate=False)

    print("\n[7] wealth_insights_cust — flag distribution (latest month):")
    spark.sql("""
        SELECT business_date,
               COUNT(DISTINCT rcif_number) AS wealth_customers,
               COUNT(DISTINCT CASE WHEN digital_flag        = 'Digital User'    THEN rcif_number END) AS digital_enrolled,
               COUNT(DISTINCT CASE WHEN digital_active_flag = 'Digital Active'  THEN rcif_number END) AS digital_active,
               COUNT(DISTINCT CASE WHEN olb_active_flag     = 'OLB Active'      THEN rcif_number END) AS olb_active,
               COUNT(DISTINCT CASE WHEN mobile_active_flag  = 'Mobile Active'   THEN rcif_number END) AS mobile_active
          FROM wealth_insights_cust
         GROUP BY business_date
         ORDER BY business_date
    """).show(truncate=False)


# ── 8) Account Table — wealth_insights_acct ─────────────────────────────────────
# Grain: (business_date, rcif_number, ip_account_id) — one row per IP account per
# month.  ip_accounts_cnt is the customer-level total (repeated on each row).

spark.sql("""
    CREATE OR REPLACE TEMP VIEW wealth_insights_acct AS
    SELECT
        ip.business_date,
        ip.rcif_number,
        ip.cust_internet_banking_nbr,
        ip.ip_id,
        ip.ip_balance,
        ip.ip_open_date,
        agg.ip_accounts_cnt,
        addr.state_name
    FROM investpath_arr ip
    JOIN investpath_agg agg
        ON  ip.business_date = agg.business_date
        AND ip.rcif_number   = agg.rcif_number
    LEFT JOIN rcif_address addr
        ON ip.ip_id = addr.ip_id
""")

if DEBUG:
    print("\n[8] wealth_insights_acct sample:")
    spark.sql("SELECT * FROM wealth_insights_acct LIMIT 10").show(truncate=False)

    print("\n[8] wealth_insights_acct — summary (latest month):")
    spark.sql("""
        SELECT business_date,
               COUNT(DISTINCT ip_id)  AS ip_customers,
               COUNT(*)              AS ip_accounts,
               SUM(ip_balance)       AS aum,
               SUM(ip_balance) / NULLIF(COUNT(*), 0) AS avg_balance
          FROM wealth_insights_acct
         GROUP BY business_date
         ORDER BY business_date
    """).show(truncate=False)


# ── 9) Write Tables ─────────────────────────────────────────────────────────────
TARGET_WRITE_PARTITIONS = 32
MAX_RECORDS_PER_FILE    = 1_000_000

# -- Customer table
cust_df = spark.table("wealth_insights_cust")
_ = cust_df.cache().count()                       # materialise to cut lineage
cust_df = cust_df.coalesce(TARGET_WRITE_PARTITIONS)

(cust_df.write
    .mode("overwrite")
    .option("maxRecordsPerFile", MAX_RECORDS_PER_FILE)
    .saveAsTable(f"{DEFAULT_DB}.wealth_insights_cust")
)

# -- Account table
acct_df = spark.table("wealth_insights_acct")
_ = acct_df.cache().count()
acct_df = acct_df.coalesce(TARGET_WRITE_PARTITIONS)

(acct_df.write
    .mode("overwrite")
    .option("maxRecordsPerFile", MAX_RECORDS_PER_FILE)
    .saveAsTable(f"{DEFAULT_DB}.wealth_insights_acct")
)

print(f"\nSaved {DEFAULT_DB}.wealth_insights_cust")
print(f"Saved {DEFAULT_DB}.wealth_insights_acct")
print("DONE.")

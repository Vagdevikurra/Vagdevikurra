-- =============================================================================
-- VALIDATION SQL — run this in your Spark SQL notebook to verify counts
-- before running the PySpark job
-- Date Range: 2025-09-01 to 2026-02-28
-- =============================================================================

-- STEP 1: Check what partition DBM has
SELECT min(ods_business_dt) AS min_dt, max(ods_business_dt) AS max_dt
FROM dm_ib.digital_banking_master;

-- STEP 2: Check what month-end dates exist in daily EIL
SELECT DISTINCT business_date
FROM eil.d_involved_party_h
WHERE business_date >= '2025-09-01'
  AND business_date <= '2026-02-28'
  AND business_date IN (
    '2025-09-30','2025-09-29','2025-09-28',
    '2025-10-31','2025-10-30',
    '2025-11-28','2025-11-29',
    '2025-12-31','2025-12-30',
    '2026-01-30','2026-01-29',
    '2026-02-27','2026-02-28'
  )
ORDER BY business_date;

-- =============================================================================
-- MAIN VALIDATION: Active flag counts per month
-- Exactly mirrors the original SQL logic:
--   datediff(ods_business_dt, lst_login) <= 90
-- =============================================================================

WITH

-- DBM: single max partition, one row per ibn
dbm AS (
    SELECT
        ibn,
        MAX(olb_last_login_date) AS lst_login_olb,
        MAX(mob_last_login_date) AS lst_login_mob,
        MAX(ods_business_dt)     AS snap_dt
    FROM dm_ib.digital_banking_master
    WHERE ods_business_dt = (SELECT MAX(ods_business_dt) FROM dm_ib.digital_banking_master)
      AND ibn IS NOT NULL
      AND ibn != ''
    GROUP BY ibn
),

-- Month-end dates from daily table
month_ends AS (
    SELECT DISTINCT business_date
    FROM eil.d_involved_party_h
    WHERE business_date IN (
        '2025-09-30','2025-10-31','2025-11-28',
        '2025-12-31','2026-01-30','2026-02-27'
    )
),

-- RCIF base: one row per RCIF per month-end
rc AS (
    SELECT
        ip.rcif_cust_nbr                  AS RCIF_NUMBER,
        ip.business_date,
        ip.cust_internet_banking_nbr      AS cust_ibn
    FROM eil.d_involved_party_h ip
    INNER JOIN eil.d_arrangement_to_involved_party_relationship_h a2i
        ON ip.involved_party_id  = a2i.involved_party_id
       AND ip.business_date      = a2i.business_date
       AND ip.source_system_code = a2i.source_system_code
    INNER JOIN eil.d_arrangement_h ar
        ON a2i.arrangement_id                 = ar.arrangement_id
       AND a2i.arrangement_source_system_code = ar.source_system_code
       AND a2i.business_date                  = ar.business_date
    WHERE ip.business_date IN (SELECT business_date FROM month_ends)
      AND ip.source_system_code = 'CF'
      AND COALESCE(ip.deceased_ind, 'N') = 'N'
      AND ar.source_system_code IN (
            'DA','SV','CC','MG','LS','TM','LO','CM','CS','EL',
            'IC','MA','PF','PR','SD','TR','BI','RN',
            'IS_CT','IS_IT','IS_IF','PNB'
          )
    GROUP BY ip.rcif_cust_nbr, ip.business_date, ip.cust_internet_banking_nbr
),

-- Wealth filter: only wealth-classified RCIFs
pw1 AS (
    SELECT DISTINCT
        CAST(ip.rcif_cust_nbr AS STRING) AS RCIF_NUMBER,
        ip.business_date
    FROM eil.d_involved_party_h ip
    INNER JOIN eil.d_arrangement_to_involved_party_relationship_h a2i
        ON ip.involved_party_id  = a2i.involved_party_id
       AND ip.business_date      = a2i.business_date
       AND ip.source_system_code = a2i.source_system_code
    INNER JOIN eil.d_arrangement_h ar
        ON a2i.arrangement_id                 = ar.arrangement_id
       AND a2i.arrangement_source_system_code = ar.source_system_code
       AND a2i.business_date                  = ar.business_date
    WHERE ip.business_date IN (SELECT business_date FROM month_ends)
      AND ip.source_system_code = 'CF'
      AND COALESCE(ip.deceased_ind, 'N') = 'N'
      AND ar.source_system_code IN (
            'DA','SV','CC','MG','LS','TM','LO','CM','CS','EL',
            'IC','MA','PF','PR','SD','TR','BI','RN',
            'IS_CT','IS_IT','IS_IF','PNB'
          )
      AND ar.closed_ind = 'N'
      AND (
            ip.private_client_code IN ('039','539','339')
         OR ip.private_client_trust_code IN ('239','739')
         OR ar.business_service_segment_type_code IN ('IS_CT','IS_IT','REGIS_FC','REGIS','PWM')
          )
),

-- Join rc + pw1 + dbm, compute flags
final AS (
    SELECT
        rc.business_date,
        CASE
            WHEN dbm.lst_login_olb IS NOT NULL
             AND datediff(dbm.snap_dt, dbm.lst_login_olb) <= 90
            THEN 'OLB Active'
            ELSE 'Non OLB Active'
        END AS olb_active_flag,
        CASE
            WHEN dbm.lst_login_mob IS NOT NULL
             AND datediff(dbm.snap_dt, dbm.lst_login_mob) <= 90
            THEN 'Mobile Active'
            ELSE 'Non Mobile Active'
        END AS mob_active_flag,
        CASE
            WHEN (dbm.lst_login_olb IS NOT NULL AND datediff(dbm.snap_dt, dbm.lst_login_olb) <= 90)
              OR (dbm.lst_login_mob IS NOT NULL AND datediff(dbm.snap_dt, dbm.lst_login_mob) <= 90)
            THEN 'Digital Active'
            ELSE 'Non Digital Active'
        END AS digitally_active_flag
    FROM rc
    INNER JOIN pw1
        ON rc.RCIF_NUMBER   = pw1.RCIF_NUMBER
       AND rc.business_date = pw1.business_date
    LEFT JOIN dbm
        ON rc.cust_ibn = dbm.ibn
)

-- OLB counts
SELECT business_date, olb_active_flag, COUNT(*) AS cnt
FROM final
GROUP BY business_date, olb_active_flag
ORDER BY business_date, olb_active_flag;

-- Run these separately to see mobile and digital:
-- SELECT business_date, mob_active_flag, COUNT(*) AS cnt FROM final GROUP BY 1,2 ORDER BY 1,2;
-- SELECT business_date, digitally_active_flag, COUNT(*) AS cnt FROM final GROUP BY 1,2 ORDER BY 1,2;

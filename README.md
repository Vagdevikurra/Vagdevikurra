SELECT max(month_dt) AS latest_month_dt
FROM dm_ib_dev.digital_202507_202512
WHERE month_dt BETWEEN date '2025-07-01' AND date '2025-12-01';
WITH latest AS (
  SELECT max(month_dt) AS mx
  FROM dm_ib_dev.digital_202507_202512
  WHERE month_dt BETWEEN date '2025-07-01' AND date '2025-12-01'
)
SELECT count(distinct regexp_replace(upper(trim(reltibn)), '^0+', '')) AS digital_active_ibn_latest
FROM dm_ib_dev.digital_202507_202512 d
JOIN latest l
  ON d.month_dt = l.mx
WHERE d.digitally_active_flag = 'Digital Active'
  AND reltibn IS NOT NULL
  AND length(trim(reltibn)) > 0;
SELECT count(distinct rcif_number) AS wealth_rcif
FROM dm_ib_dev.wealth_rcif_202507_202512;
SELECT sum(accts_cnt) AS wealth_accounts_total
FROM dm_ib_dev.wealth_rcif_202507_202512;

WITH latest AS (
  SELECT max(month_dt) AS mx
  FROM dm_ib_dev.digital_202507_202512
  WHERE month_dt BETWEEN date '2025-07-01' AND date '2025-12-01'
),
digital_active_latest AS (
  SELECT distinct regexp_replace(upper(trim(reltibn)), '^0+', '') AS ibn_norm
  FROM dm_ib_dev.digital_202507_202512 d
  JOIN latest l
    ON d.month_dt = l.mx
  WHERE d.digitally_active_flag = 'Digital Active'
    AND reltibn IS NOT NULL
    AND length(trim(reltibn)) > 0
),
wealth_ibn AS (
  SELECT distinct
         rcif_number,
         regexp_replace(upper(trim(cust_internet_banking_nbr)), '^0+', '') AS ibn_norm
  FROM dm_ib_dev.wealth_rcif_202507_202512
  WHERE cust_internet_banking_nbr IS NOT NULL
    AND length(trim(cust_internet_banking_nbr)) > 0
)
SELECT count(distinct w.rcif_number) AS wealth_digital_rcif_latest
FROM wealth_ibn w
JOIN digital_active_latest d
  ON w.ibn_norm = d.ibn_norm;

WITH digital_active_any AS (
  SELECT distinct regexp_replace(upper(trim(reltibn)), '^0+', '') AS ibn_norm
  FROM dm_ib_dev.digital_202507_202512
  WHERE month_dt BETWEEN date '2025-07-01' AND date '2025-12-01'
    AND digitally_active_flag = 'Digital Active'
    AND reltibn IS NOT NULL
    AND length(trim(reltibn)) > 0
),
wealth_ibn AS (
  SELECT distinct
         rcif_number,
         regexp_replace(upper(trim(cust_internet_banking_nbr)), '^0+', '') AS ibn_norm
  FROM dm_ib_dev.wealth_rcif_202507_202512
  WHERE cust_internet_banking_nbr IS NOT NULL
    AND length(trim(cust_internet_banking_nbr)) > 0
)
SELECT count(distinct w.rcif_number) AS wealth_digital_rcif_any_month
FROM wealth_ibn w
JOIN digital_active_any d
  ON w.ibn_norm = d.ibn_norm;

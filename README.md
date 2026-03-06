-- Just sample 10 rows from each to eyeball the format
SELECT 'wealth' AS src, CAST(rcif_cust_nbr AS STRING) AS rcif
FROM eil.d_involved_party_h
WHERE business_date = '2026-01-31'
  AND source_system_code = 'CF'
LIMIT 10

UNION ALL

SELECT 'transmit' AS src, CAST(rcif_id AS STRING) AS rcif
FROM dm_ib.transmit_digital_logins
LIMIT 10

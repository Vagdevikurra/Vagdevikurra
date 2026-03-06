SELECT
    COUNT(DISTINCT e.rcif_cust_nbr)   AS eil_rcif,
    COUNT(DISTINCT t.rcif_id)          AS transmit_rcif,
    COUNT(DISTINCT CASE WHEN t.rcif_id IS NOT NULL
          THEN e.rcif_cust_nbr END)    AS matched_on_ibn
FROM eil.d_involved_party_h e
LEFT JOIN dm_ib.transmit_digital_logins t
    ON CAST(e.cust_internet_banking_nbr AS STRING) 
     = CAST(t.rcif_id AS STRING)
WHERE e.source_system_code = 'CF'
  AND e.business_date = '2026-01-30'
LIMIT 1

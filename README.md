SELECT 
    COUNT(DISTINCT e.rcif_cust_nbr)  AS eil_rcif,
    COUNT(DISTINCT d.rcif_customer_nbr) AS dbm_rcif,
    COUNT(DISTINCT CASE WHEN d.rcif_customer_nbr IS NOT NULL 
          THEN e.rcif_cust_nbr END)  AS matched
FROM eil.d_involved_party_h e
LEFT JOIN dm_ib.digital_banking_master d
    ON CAST(e.rcif_cust_nbr AS STRING) = CAST(d.rcif_customer_nbr AS STRING)
WHERE e.business_date = (
        SELECT MAX(business_date) 
        FROM eil.d_involved_party_h 
        WHERE source_system_code = 'CF'
    )
  AND e.source_system_code = 'CF'
LIMIT 1

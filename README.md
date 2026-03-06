-- Check how many wealth customers actually match transmit
SELECT 
    COUNT(DISTINCT ip.rcif_cust_nbr)           AS wealth_rcif_count,
    COUNT(DISTINCT t.rcif_id)                  AS transmit_rcif_count,
    COUNT(DISTINCT CASE WHEN t.rcif_id IS NOT NULL 
                   THEN ip.rcif_cust_nbr END)  AS matched_count,
    -- Check format difference
    MAX(CAST(ip.rcif_cust_nbr AS STRING))      AS sample_wealth_rcif,
    MAX(CAST(t.rcif_id AS STRING))             AS sample_transmit_rcif
FROM eil.d_involved_party_h ip
LEFT JOIN dm_ib.transmit_digital_logins t
    ON CAST(ip.rcif_cust_nbr AS STRING) = CAST(t.rcif_id AS STRING)
WHERE ip.business_date = (SELECT MAX(business_date) FROM eil.d_involved_party_h)
  AND ip.source_system_code = 'CF'

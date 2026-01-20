WITH last_dt AS (
  SELECT MAX(CAST(business_date AS DATE)) AS dt
  FROM eil.d_involved_party_h
),
ip AS (
  SELECT DISTINCT
    ind.involved_party_id AS ip_id,
    ind.rcif_cust_nbr AS rcif_number,
    ar.arrangement_id AS account_id,
    ar.current_balance_amt AS balance_amt
  FROM eil.d_involved_party_h ind
  JOIN last_dt d
    ON CAST(ind.business_date AS DATE) = d.dt
  JOIN eil.d_arrangement_to_involved_party_relationship_h a2i
    ON ind.involved_party_id = a2i.involved_party_id
   AND ind.business_date = a2i.business_date
   AND ind.source_system_code = a2i.source_system_code
  JOIN eil.d_arrangement_h ar
    ON a2i.arrangement_id = ar.arrangement_id
   AND a2i.arrangement_source_system_code = ar.source_system_code
   AND a2i.business_date = ar.business_date
  WHERE ind.source_system_code = 'CF'
    AND NVL(ind.deceased_ind,'N') = 'N'
    AND ar.closed_ind = 'N'
    AND ar.account_type_code = 'IP'
    AND ar.source_system_code = 'RN'
)
SELECT
  COUNT(DISTINCT ip_id) AS ip_customers,
  COUNT(DISTINCT account_id) AS ip_accounts,
  SUM(balance_amt) AS ip_aum
FROM ip;

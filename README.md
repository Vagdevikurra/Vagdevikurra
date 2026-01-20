Perfect — creating a **VIEW** is the safest way to support the **Open Date** visual without blowing up your 2-table FACT counts. 👍

Below is a **single Hive view** you can create that returns the monthly series Power BI needs (customers, accounts, AUM, funded accounts), based on **InvestPath account open_date**.

This will **not** change your DIM/FACT row counts at all.

---

# ✅ Create view: InvestPath accounts opened by month

This view uses the **latest EIL snapshot date** to get the current “active/open” InvestPath accounts, and then groups them by `open_date` month.

```sql
CREATE OR REPLACE VIEW dm_ib_dev.v_investpath_opened_by_month AS
WITH last_dt AS (
  SELECT MAX(CAST(business_date AS DATE)) AS dt
  FROM eil.d_involved_party_h
),
ip_raw AS (
  SELECT
    TRUNC(CAST(ar.open_date AS DATE), 'MM')          AS open_month,
    CAST(ind.involved_party_id AS STRING)            AS ip_id,
    CAST(ind.rcif_cust_nbr AS STRING)                AS rcif_number,
    CAST(ar.arrangement_id AS STRING)                AS account_id,
    CAST(ar.current_balance_amt AS DOUBLE)           AS balance_amt
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
    AND ar.open_date IS NOT NULL
)
SELECT
  open_month,
  COUNT(DISTINCT ip_id) AS ip_customers_opened,
  COUNT(DISTINCT account_id) AS ip_accounts_opened,
  SUM(balance_amt) AS ip_aum_opened,
  COUNT(DISTINCT CASE WHEN balance_amt > 0 THEN account_id END) AS ip_accounts_opened_funded
FROM ip_raw
GROUP BY open_month;
```

---

## ✅ How to use in Power BI

Load `dm_ib_dev.v_investpath_opened_by_month` and build visuals:

* **X-axis:** `open_month`
* **Cards / values:**

  * `ip_customers_opened`  → should be ~100–120 range for overall (depending on filters)
  * `ip_accounts_opened`
  * `ip_aum_opened`
  * `ip_accounts_opened_funded`

---

## Quick validation in Hive

```sql
SELECT *
FROM dm_ib_dev.v_investpath_opened_by_month
ORDER BY open_month;
```

---

# Optional: limit to a time window (keeps Power BI fast)

If you only want the last 36 months:

```sql
CREATE OR REPLACE VIEW dm_ib_dev.v_investpath_opened_by_month AS
WITH last_dt AS (
  SELECT MAX(CAST(business_date AS DATE)) AS dt
  FROM eil.d_involved_party_h
),
ip_raw AS (
  SELECT
    TRUNC(CAST(ar.open_date AS DATE), 'MM') AS open_month,
    CAST(ind.involved_party_id AS STRING)   AS ip_id,
    CAST(ar.arrangement_id AS STRING)       AS account_id,
    CAST(ar.current_balance_amt AS DOUBLE)  AS balance_amt
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
    AND ar.open_date IS NOT NULL
    AND CAST(ar.open_date AS DATE) >= add_months(trunc(current_date(),'MM'), -36)
)
SELECT
  open_month,
  COUNT(DISTINCT ip_id) AS ip_customers_opened,
  COUNT(DISTINCT account_id) AS ip_accounts_opened,
  SUM(balance_amt) AS ip_aum_opened,
  COUNT(DISTINCT CASE WHEN balance_amt > 0 THEN account_id END) AS ip_accounts_opened_funded
FROM ip_raw
GROUP BY open_month;
```

---

If your original open-date chart spans back to **2022**, tell me the earliest month you want (e.g., “May 2022”) and I’ll set the view filter to match exactly.

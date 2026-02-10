Yes — **you should correct it in Spark first**. DAX should be **thin**, not “patching” bad grain. Right now your symptom is classic:

* **Wealth Customers** (distinct RCIF) is fine (~268,984)
* **Accounts** blows up (~676k) and Accounts/User becomes ~1.6
  ➡️ That almost always means **your wealth fact has multiple rows per RCIF at latest date**, so `SUM(accts_cnt)` is double-counting.

The fix is **not** a DAX trick. The fix is: **make `wic2_wealth_fact` be 1 row per (business_date, rcif_number)** (RCIF grain) and compute accounts at that grain.

---

# 1) Prove the grain problem (run these in Hive/Spark SQL)

### A. How many rows per RCIF at the latest date?

```sql
WITH mx AS (
  SELECT max(business_date) AS dt FROM dm_ib_dev.wic2_wealth_fact
)
SELECT
  count(*) AS rows_latest,
  count(DISTINCT rcif_number) AS rcifs_latest,
  (count(*) - count(DISTINCT rcif_number)) AS extra_rows
FROM dm_ib_dev.wic2_wealth_fact w
JOIN mx ON w.business_date = mx.dt;
```

If `rows_latest` >> `rcifs_latest`, you **do not** have RCIF grain.

### B. Show the worst offenders (RCIFs repeated)

```sql
WITH mx AS (
  SELECT max(business_date) AS dt FROM dm_ib_dev.wic2_wealth_fact
)
SELECT rcif_number, count(*) AS row_cnt
FROM dm_ib_dev.wic2_wealth_fact w
JOIN mx ON w.business_date = mx.dt
GROUP BY rcif_number
HAVING count(*) > 1
ORDER BY row_cnt DESC
LIMIT 50;
```

### C. Compare “sum accounts” vs “dedup accounts”

```sql
WITH mx AS (
  SELECT max(business_date) AS dt FROM dm_ib_dev.wic2_wealth_fact
),
latest AS (
  SELECT * FROM dm_ib_dev.wic2_wealth_fact w
  JOIN mx ON w.business_date = mx.dt
)
SELECT
  sum(accts_cnt) AS sum_accounts,
  sum(max_accts_cnt) AS dedup_accounts
FROM (
  SELECT rcif_number,
         max(accts_cnt) AS max_accts_cnt
  FROM latest
  GROUP BY rcif_number
) x
CROSS JOIN (
  SELECT sum(accts_cnt) AS accts_cnt FROM latest
) y;
```

If `sum_accounts` ≈ 676k and `dedup_accounts` drops closer to your expected band, that confirms: **duplicate RCIF rows** are inflating accounts.

---

# 2) Correct the Spark table design (the real fix)

### ✅ Goal table (`wic2_wealth_fact`) grain

**ONE row per (business_date, rcif_number)** with:

* business_date
* rcif_number
* business_group (derived)
* division (derived)
* accts_cnt (true count at RCIF grain)

If you need segment/division counts, compute them **inside the same RCIF aggregation**, but still output **one row**.

---

# 3) Concrete Spark SQL pattern (use this)

Below is the “safe” structure that prevents double counting. You’ll need to map your existing source joins into `wealth_arr` (arrangement grain), then aggregate.

### Step 1 — build arrangement-grain base (one row per arrangement_id)

```sql
CREATE OR REPLACE TEMP VIEW wealth_arr AS
SELECT
  ind.business_date,
  CAST(ind.rcif_cust_nbr AS STRING) AS rcif_number,
  ar.arrangement_id,
  ar.source_system_code,
  ar.business_service_segment_type_code,
  ar.closed_ind
FROM eil_d_involved_party_h ind
JOIN eil_d_arrangement_to_involved_party_relationship_h a2i
  ON ind.involved_party_id = a2i.involved_party_id
 AND ind.business_date     = a2i.business_date
 AND ind.source_system_code= a2i.source_system_code
JOIN eil_d_arrangement_h ar
  ON a2i.arrangement_id               = ar.arrangement_id
 AND a2i.arrangement_source_system_code= ar.source_system_code
 AND a2i.business_date                = ar.business_date
WHERE ind.source_system_code = 'CF'
  AND nvl(ind.deceased_ind,'N') = 'N'
  AND ar.closed_ind = 'N'
  AND ar.source_system_code IN ('BW','RN','TR','OM','SV','CC','LS','MG','TM','PC','LO','CM','CS','EL','IC','MA','PF','PR','SD');  -- use your list
```

### Step 2 — aggregate to RCIF grain (THIS is the key)

```sql
CREATE OR REPLACE TEMP VIEW wic2_wealth_fact_fixed AS
SELECT
  business_date,
  rcif_number,
  COUNT(DISTINCT arrangement_id) AS accts_cnt,

  /* Business group derived once per RCIF */
  CASE
    WHEN SUM(CASE WHEN business_service_segment_type_code IN ('IS_CT','IS_IT') THEN 1 ELSE 0 END) > 0
      THEN 'Institutional Services'
    WHEN SUM(CASE WHEN business_service_segment_type_code IN ('REGIS_FC','REGIS') THEN 1 ELSE 0 END) > 0
      THEN 'Investment Services'
    WHEN SUM(CASE WHEN business_service_segment_type_code IN ('PWM') THEN 1 ELSE 0 END) > 0
      THEN 'Private Wealth'
    ELSE 'Other'
  END AS business_group

FROM wealth_arr
GROUP BY business_date, rcif_number;
```

### Step 3 — overwrite your final table

```sql
INSERT OVERWRITE TABLE dm_ib_dev.wic2_wealth_fact
SELECT * FROM wic2_wealth_fact_fixed;
```

This guarantees:

* **Accounts won’t inflate**
* Accounts/User becomes realistic
* DAX becomes straightforward

---

# 4) Why your current accounts is 676k

Because your current wealth code likely groups by **extra columns** (division/business group/segment/etc.) and produces **multiple rows per RCIF**, each carrying an `accts_cnt`. Then PBI sums all of them.

Fixing the grain upstream is the only clean solution.

---

# 5) What I want you to run next (no guessing)

Run the **3 validation queries** in section #1 and paste the outputs:

* rows_latest, rcifs_latest, extra_rows
* top repeated RCIFs
* sum_accounts vs dedup_accounts

With that, I can tell you **exactly** whether to:

* fix grouping keys, or
* change `accts_cnt` calc to `countDistinct(arrangement_id)`, or
* select a single “primary” division per RCIF.

If you do just one thing now: **run query A** (rows vs distinct RCIF) — that will confirm the root cause immediately.

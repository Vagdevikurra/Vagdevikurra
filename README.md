Perfect — we *can* add InvestPath **without messing up Wealth** by doing it in a controlled way:

✅ Keep **wealth rows exactly as-is**
✅ Append **investpath rows as a separate “slice”** inside the same fact table
✅ Ensure every Wealth measure filters **division != 'InvestPath'** (or `fact_type = 'WEALTH'`) so Wealth numbers never get inflated

Below is the clean pattern that works and keeps your model at **3 tables**.

---

## A) What we will do (safe design)

### Keep your existing 3 tables

* `wic2_wealth_fact`  ✅ (we will *extend* it)
* `wia2_customer` ✅
* `wir2_rcif_set` ✅ (optional/disconnected)

### Add InvestPath into `wic2_wealth_fact` as extra rows

We’ll add these columns to the fact (nullable for Wealth rows):

* `fact_type`  → `'WEALTH'` or `'INVESTPATH'`
* `ip_id`
* `ip_account_id` (arrangement_id)
* `ip_balance`
* `ip_open_date`

For Wealth rows:

* `fact_type = 'WEALTH'`
* `ip_* = NULL`

For InvestPath rows:

* `fact_type = 'INVESTPATH'`
* `business_date = latest_date` (same as wealth latest)
* `rcif_number` populated
* `accts_cnt = 1`
* `division = 'InvestPath'`
* `business_group = 'Investment Services'` (or whatever you want)
* `ip_*` populated

**This guarantees Wealth counts won’t change** as long as Wealth measures filter `fact_type = 'WEALTH'`.

---

## B) Spark code (append InvestPath into the same fact)

> Replace database/schema names with yours. I’ll assume:
>
> * wealth fact temp view already exists as `wic2_wealth_fact_base` (your current final wealth fact before saving)
> * investpath source is from `eil.d_involved_party_h`, `eil.d_arrangement_to_involved_party_relationship_h`, `eil.d_arrangement_h` (from your screenshot)

```python
# ------------------------------------------------------------
# 0) Config
# ------------------------------------------------------------
DEFAULT_DB = "dm_ib_dev"  # change if needed

# ------------------------------------------------------------
# 1) Get latest business_date (same logic you used for wealth)
# ------------------------------------------------------------
mx = spark.sql("""
  SELECT MAX(CAST(business_date AS date)) AS dt
  FROM eil.d_involved_party_h
""").collect()[0]["dt"]

print("Latest business_date =", mx)

spark.sql(f"CREATE OR REPLACE TEMP VIEW mx_dt AS SELECT DATE('{mx}') AS dt")

# ------------------------------------------------------------
# 2) Wealth fact (your existing result)
#    Assume you already built it as temp view wic2_wealth_fact (WEALTH only)
#    If you saved it already, you can read it back from Hive instead.
# ------------------------------------------------------------
wealth_df = spark.table(f"{DEFAULT_DB}.wic2_wealth_fact")  # OR your temp view
wealth_df.createOrReplaceTempView("wealth_fact_existing")

# Add investpath columns (NULL) + fact_type
spark.sql("""
CREATE OR REPLACE TEMP VIEW wealth_fact_typed AS
SELECT
  business_date,
  rcif_number,
  business_group,
  division,
  accts_cnt,
  -- keep any other wealth columns you already have here (state_name, etc.)
  state_name,

  'WEALTH' AS fact_type,
  CAST(NULL AS string) AS ip_id,
  CAST(NULL AS string) AS ip_account_id,
  CAST(NULL AS double) AS ip_balance,
  CAST(NULL AS date) AS ip_open_date
FROM wealth_fact_existing
""")

# ------------------------------------------------------------
# 3) InvestPath rows (latest dt only)
# ------------------------------------------------------------
spark.sql("""
CREATE OR REPLACE TEMP VIEW investpath_rows AS
WITH dt AS (SELECT dt FROM mx_dt),
inv AS (
  SELECT
    CAST(ind.rcif_cust_nbr AS string) AS rcif_number,
    CAST(ind.involved_party_id AS string) AS ip_id,
    CAST(ar.arrangement_id AS string) AS ip_account_id,
    CAST(ar.current_balance_amt AS double) AS ip_balance,
    CAST(ar.open_date AS date) AS ip_open_date
  FROM eil.d_involved_party_h ind
  JOIN dt ON CAST(ind.business_date AS date) = dt.dt
  JOIN eil.d_arrangement_to_involved_party_relationship_h a2i
    ON ind.involved_party_id = a2i.involved_party_id
   AND ind.business_date = a2i.business_date
   AND ind.source_system_code = a2i.source_system_code
  JOIN eil.d_arrangement_h ar
    ON a2i.arrangement_id = ar.arrangement_id
   AND a2i.arrangement_source_system_code = ar.source_system_code
   AND a2i.business_date = ar.business_date
  WHERE ind.source_system_code = 'CF'
    AND NVL(ind.deceased_ind, 'N') = 'N'
    AND ar.closed_ind = 'N'
    AND ar.account_type_code = 'IP'
    AND ar.source_system_code = 'RN'
)
SELECT
  (SELECT dt FROM mx_dt) AS business_date,
  rcif_number,
  'Investment Services' AS business_group,
  'InvestPath' AS division,
  1 AS accts_cnt,
  CAST(NULL AS string) AS state_name, -- optional, unless you want to join address
  'INVESTPATH' AS fact_type,
  ip_id,
  ip_account_id,
  ip_balance,
  ip_open_date
FROM inv
WHERE rcif_number IS NOT NULL
""")

# ------------------------------------------------------------
# 4) Union wealth + investpath into one final fact
# ------------------------------------------------------------
spark.sql("""
CREATE OR REPLACE TEMP VIEW wic2_wealth_fact_final AS
SELECT * FROM wealth_fact_typed
UNION ALL
SELECT * FROM investpath_rows
""")

# ------------------------------------------------------------
# 5) Validation checks (VERY important)
# ------------------------------------------------------------
# Wealth counts should match your current wealth counts (latest dt)
spark.sql("""
WITH dt AS (SELECT MAX(business_date) AS dt FROM wealth_fact_existing)
SELECT
  (SELECT dt FROM dt) AS dt,
  COUNT(DISTINCT CASE WHEN business_date = (SELECT dt FROM dt) THEN rcif_number END) AS wealth_rcifs_latest,
  SUM(CASE WHEN business_date = (SELECT dt FROM dt) THEN accts_cnt ELSE 0 END) AS wealth_accounts_latest
FROM wealth_fact_existing
""").show(truncate=False)

spark.sql("""
WITH dt AS (SELECT MAX(business_date) AS dt FROM wealth_fact_existing)
SELECT
  COUNT(DISTINCT CASE WHEN fact_type='WEALTH' AND business_date=(SELECT dt FROM dt) THEN rcif_number END) AS wealth_rcifs_latest_after,
  SUM(CASE WHEN fact_type='WEALTH' AND business_date=(SELECT dt FROM dt) THEN accts_cnt ELSE 0 END) AS wealth_accounts_latest_after
FROM wic2_wealth_fact_final
""").show(truncate=False)

# InvestPath basic counts
spark.sql("""
SELECT
  COUNT(*) AS ip_rows,
  COUNT(DISTINCT ip_id) AS ip_customers,
  COUNT(DISTINCT ip_account_id) AS ip_accounts,
  SUM(ip_balance) AS ip_aum,
  SUM(CASE WHEN ip_balance > 0 THEN 1 ELSE 0 END) AS ip_funded_accounts
FROM wic2_wealth_fact_final
WHERE fact_type='INVESTPATH'
""").show(truncate=False)

# ------------------------------------------------------------
# 6) Save back to same table (overwrite) - ONLY after validation matches
# ------------------------------------------------------------
spark.table("wic2_wealth_fact_final").write.mode("overwrite").saveAsTable(f"{DEFAULT_DB}.wic2_wealth_fact")
print("Saved:", f"{DEFAULT_DB}.wic2_wealth_fact")
```

### Why this won’t mess up Wealth

Because Wealth measures will filter `fact_type = 'WEALTH'` (or `division <> 'InvestPath'`), and the Wealth rows remain unchanged.

---

## C) Power BI DAX for InvestPath (pointing to same `wic2_wealth_fact`)

### InvestPath Customers

```DAX
InvestPath Customers =
CALCULATE(
    DISTINCTCOUNT ( wic2_wealth_fact[ip_id] ),
    wic2_wealth_fact[fact_type] = "INVESTPATH"
)
```

### InvestPath Accounts

```DAX
InvestPath Accounts =
CALCULATE(
    DISTINCTCOUNT ( wic2_wealth_fact[ip_account_id] ),
    wic2_wealth_fact[fact_type] = "INVESTPATH"
)
```

### InvestPath AUM

```DAX
InvestPath AUM =
CALCULATE(
    SUM ( wic2_wealth_fact[ip_balance] ),
    wic2_wealth_fact[fact_type] = "INVESTPATH"
)
```

### InvestPath Funded Accounts

```DAX
InvestPath Funded Accounts =
CALCULATE(
    DISTINCTCOUNT ( wic2_wealth_fact[ip_account_id] ),
    wic2_wealth_fact[fact_type] = "INVESTPATH",
    wic2_wealth_fact[ip_balance] > 0
)
```

### Average Balance per InvestPath Account

```DAX
InvestPath Avg Balance per Account =
DIVIDE( [InvestPath AUM], [InvestPath Accounts] )
```

---

## D) Update your existing Wealth measures (to stay pure Wealth)

Wherever you compute Wealth customers/accounts, add:

* `wic2_wealth_fact[fact_type] = "WEALTH"`

Example:

```DAX
Wealth Customers =
VAR dt = [Latest Wealth Date]
RETURN
CALCULATE(
    DISTINCTCOUNT ( wic2_wealth_fact[rcif_number] ),
    wic2_wealth_fact[business_date] = dt,
    wic2_wealth_fact[fact_type] = "WEALTH"
)
```

That one line prevents InvestPath rows from ever inflating Wealth.

---

## Quick question (so I don’t guess wrong and break it)

In your `wic2_wealth_fact` right now, do you already have a `state_name` column saved?

* If **yes**, we keep it and set NULL for InvestPath rows (safe).
* If **no**, remove `state_name` from the union on both sides.

If you tell me “state_name exists / doesn’t exist”, I’ll paste the final version with the exact column list so your `UNION ALL` never fails.

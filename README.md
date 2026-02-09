Got it. You want **Hive SQL** that matches those DAX measures so you can compare **actual vs expected** and then you’ll tell me what’s off. Below are **1:1 SQL equivalents** using your new tables.

I’m going to assume these tables/columns (based on what we built):

* **Wealth fact**: `dm_ib_dev.wic2_wealth_fact`

  * `business_date`, `rcif_number`, `accts_cnt`, `business_group`, `division`
* **Customer/digital dim**: `dm_ib_dev.wia2_customer`

  * `rcif_number`, `primary_ibn`, `digital_flag`, `digitally_active_flag`, `state_name`
* **InvestPath** (if you also created it): `dm_ib_dev.inv` or `dm_ib_dev.wia2_investpath` (you’ll replace table name)

If your table names differ, just swap them.

---

# 0) Get latest wealth snapshot date (use this in all “latest” measures)

```sql
WITH mx AS (
  SELECT max(business_date) as latest_dt
  FROM dm_ib_dev.wic2_wealth_fact
)
SELECT * FROM mx;
```

---

# 1) Wealth Customers (DAX: DISTINCTCOUNT(Wealth[pw1.rcifnumber]))

```sql
WITH mx AS (SELECT max(business_date) as dt FROM dm_ib_dev.wic2_wealth_fact)
SELECT count(distinct w.rcif_number) as wealthcustomer
FROM dm_ib_dev.wic2_wealth_fact w
JOIN mx ON w.business_date = mx.dt;
```

---

# 2) Wealth Accounts (DAX: COUNT / SUM of Wealth[pw1.acctsCnt])

Your DAX looks like `COUNT(Wealth[pw1.acctsCnt])` but what you really want is total accounts.
Use **SUM(accts_cnt)**:

```sql
WITH mx AS (SELECT max(business_date) as dt FROM dm_ib_dev.wic2_wealth_fact)
SELECT sum(w.accts_cnt) as accounts
FROM dm_ib_dev.wic2_wealth_fact w
JOIN mx ON w.business_date = mx.dt;
```

---

# 3) Top of company Digital Active (DAX: DISTINCTCOUNT(Digital[reltibn]) filtered DigitalActive)

This is IBN-based:

```sql
SELECT count(distinct primary_ibn) as top_company_digital_active_ibn
FROM dm_ib_dev.wia2_customer
WHERE digitally_active_flag = 'Digital Active'
  AND primary_ibn is not null;
```

---

# 4) Digital Enrollment Wealth (DAX: CALCULATE(Wealth[wealthcustomer], RCIF[rcifdig.digitalflag]="Digital User")

Interpretation = **Wealth RCIFs who are Digital Users**.

```sql
WITH mx AS (SELECT max(business_date) as dt FROM dm_ib_dev.wic2_wealth_fact),
wealth_latest AS (
  SELECT distinct rcif_number
  FROM dm_ib_dev.wic2_wealth_fact w
  JOIN mx ON w.business_date = mx.dt
)
SELECT count(distinct w.rcif_number) as digital_enrollment_wealth
FROM wealth_latest w
JOIN dm_ib_dev.wia2_customer c
  ON w.rcif_number = c.rcif_number
WHERE c.digital_flag = 'Digital User';
```

---

# 5) Wealth Digitally Active Customers (intersection measure you described)

**Wealth RCIFs ∩ Digital Active**

```sql
WITH mx AS (SELECT max(business_date) as dt FROM dm_ib_dev.wic2_wealth_fact),
wealth_latest AS (
  SELECT distinct rcif_number
  FROM dm_ib_dev.wic2_wealth_fact w
  JOIN mx ON w.business_date = mx.dt
)
SELECT count(distinct w.rcif_number) as wealth_digital_active_customers
FROM wealth_latest w
JOIN dm_ib_dev.wia2_customer c
  ON w.rcif_number = c.rcif_number
WHERE c.digitally_active_flag = 'Digital Active';
```

---

# 6) Wealth Digitally Active Penetration (DAX: Accounts / WealthCustomers)

You said ~6.5-ish: `Accounts / Wealth[wealthcustomer]`

```sql
WITH mx AS (SELECT max(business_date) as dt FROM dm_ib_dev.wic2_wealth_fact),
wealth_latest AS (
  SELECT *
  FROM dm_ib_dev.wic2_wealth_fact w
  JOIN mx ON w.business_date = mx.dt
),
wealthcustomer AS (
  SELECT count(distinct rcif_number) as cnt FROM wealth_latest
),
accounts AS (
  SELECT sum(accts_cnt) as cnt FROM wealth_latest
)
SELECT
  a.cnt as accounts,
  wc.cnt as wealthcustomer,
  (a.cnt * 1.0) / wc.cnt as accounts_per_wealth_customer
FROM accounts a
CROSS JOIN wealthcustomer wc;
```

---

# 7) Digital Wealth Penetration (your DAX had an INTERSECT with Digital Active)

This is basically:
**(Wealth RCIFs who are Digital Active) / (Wealth RCIFs)**

```sql
WITH mx AS (SELECT max(business_date) as dt FROM dm_ib_dev.wic2_wealth_fact),
wealth_latest AS (
  SELECT distinct rcif_number
  FROM dm_ib_dev.wic2_wealth_fact w
  JOIN mx ON w.business_date = mx.dt
),
wealth_total AS (
  SELECT count(*) as cnt FROM wealth_latest
),
wealth_digital_active AS (
  SELECT count(distinct w.rcif_number) as cnt
  FROM wealth_latest w
  JOIN dm_ib_dev.wia2_customer c
    ON w.rcif_number = c.rcif_number
  WHERE c.digitally_active_flag = 'Digital Active'
)
SELECT
  wda.cnt as wealth_digital_active_rcif,
  wt.cnt  as wealth_rcif,
  (wda.cnt * 1.0) / wt.cnt as digital_wealth_penetration
FROM wealth_digital_active wda
CROSS JOIN wealth_total wt;
```

---

# 8) Digital Wealth “IBN penetration” (you had rcif / distinct ibn)

Your line:
`DISTINCTCOUNT(Wealth[rcifnumber]) / DISTINCTCOUNT(RCIF[rcifdig.custinternetbankingnbr])`

SQL equivalent (wealth RCIFs / wealth IBNs):

```sql
WITH mx AS (SELECT max(business_date) as dt FROM dm_ib_dev.wic2_wealth_fact),
wealth_latest AS (
  SELECT distinct rcif_number
  FROM dm_ib_dev.wic2_wealth_fact w
  JOIN mx ON w.business_date = mx.dt
),
wealth_ibn AS (
  SELECT distinct c.primary_ibn
  FROM wealth_latest w
  JOIN dm_ib_dev.wia2_customer c
    ON w.rcif_number = c.rcif_number
  WHERE c.primary_ibn is not null
)
SELECT
  (SELECT count(*) FROM wealth_latest) as wealth_rcifs,
  (SELECT count(*) FROM wealth_ibn)    as wealth_ibns,
  (SELECT count(*) FROM wealth_latest) * 1.0 / (SELECT count(*) FROM wealth_ibn) as rcif_per_ibn
;
```

---

# 9) InvestPath measures (if you have investpath table)

From your screenshot:

* InvestPath Customers = DISTINCTCOUNT(InvestPath[ipid]) ~100
* # Accounts = DISTINCTCOUNT(InvestPath[accounts]) ~110
* AUM = SUM(InvestPath[balance]) ~1.52m
* Avg balance per IP account = AUM / #Accounts
* Funded accounts = DISTINCTCOUNT(accounts) where balance > 0

If your invest table is `dm_ib_dev.inv` with columns:
`ip_id`, `act_cnt` (account id), `balance`

Use:

```sql
SELECT
  count(distinct ip_id) as investpath_customers,
  count(distinct act_cnt) as investpath_accounts,
  sum(balance) as aum,
  sum(balance) / count(distinct act_cnt) as avg_balance_per_account,
  count(distinct case when balance > 0 then act_cnt end) as funded_accounts
FROM dm_ib_dev.inv;
```

(Replace `dm_ib_dev.inv` with your real invest table name.)

---

# 10) State slice versions (since you mentioned state visuals)

### Wealth customers by state (latest)

```sql
WITH mx AS (SELECT max(business_date) as dt FROM dm_ib_dev.wic2_wealth_fact),
wealth_latest AS (
  SELECT distinct rcif_number
  FROM dm_ib_dev.wic2_wealth_fact w
  JOIN mx ON w.business_date = mx.dt
)
SELECT
  c.state_name,
  count(distinct w.rcif_number) as wealth_customers
FROM wealth_latest w
JOIN dm_ib_dev.wia2_customer c
  ON w.rcif_number = c.rcif_number
GROUP BY c.state_name
ORDER BY wealth_customers DESC;
```

### Digital active IBN by state

```sql
SELECT
  state_name,
  count(distinct primary_ibn) as digital_active_ibn
FROM dm_ib_dev.wia2_customer
WHERE digitally_active_flag='Digital Active'
  AND primary_ibn is not null
GROUP BY state_name
ORDER BY digital_active_ibn DESC;
```

---

## What I need from you after you run these

Just paste the outputs for:

1. wealthcustomer
2. accounts
3. digital_active_ibn
4. digital_enrollment_wealth
5. wealth_digital_active_customers

…and tell me what you expected for each. Then I’ll tell you exactly which part is still mismatched (join key vs definition vs date window vs grain).

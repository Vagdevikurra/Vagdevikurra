Great — those two numbers tell us **exactly** what’s going on:

* **Test 1 (Digital Active @ max ods_dt)** → **95,247** (this is *lower* than your current 99k)
* **Test 2 (Digital User/Enrollment @ max ods_dt)** → **134,837** (this is *much higher*)

✅ So your “~120k” expectation is **NOT “Digital Active”**.
It’s almost certainly **Digital Enrollment Wealth** (Digital User), or your report visual is using **Digital User** while you’re checking **Digital Active**.

### What you should do now

Stop chasing 120k for “Digital Active”. Your “Digital Active” number is **correctly ~95–100k** based on multiple independent validations.

---

# The 2 exact Hive queries you need for Power BI measures (latest wealth date)

## A) Wealth Digital Enrollment (Digital User) — this should be your ~120k-ish KPI

```sql
WITH mx AS (SELECT max(business_date) dt FROM dm_ib_dev.wic2_wealth_fact),
wealth_latest AS (
  SELECT distinct rcif_number
  FROM dm_ib_dev.wic2_wealth_fact w
  JOIN mx ON w.business_date = mx.dt
)
SELECT count(distinct w.rcif_number) as wealth_digital_enrollment_rcif
FROM wealth_latest w
JOIN dm_ib_dev.wia2_customer c
  ON w.rcif_number = c.rcif_number
WHERE c.digital_flag = 'Digital User';
```

## B) Wealth Digital Active (Digital Active) — expected around ~95–100k

```sql
WITH mx AS (SELECT max(business_date) dt FROM dm_ib_dev.wic2_wealth_fact),
wealth_latest AS (
  SELECT distinct rcif_number
  FROM dm_ib_dev.wic2_wealth_fact w
  JOIN mx ON w.business_date = mx.dt
)
SELECT count(distinct w.rcif_number) as wealth_digital_active_rcif
FROM wealth_latest w
JOIN dm_ib_dev.wia2_customer c
  ON w.rcif_number = c.rcif_number
WHERE c.digitally_active_flag = 'Digital Active';
```

---

# If you still *must* hit ~120k for “Active”

Then your org’s definition of “active” is different (not 90-day login). Common alternates:

* **180-day active**
* **active = digital user with any login in last 6 months**
* **active = has relt_ibn + enrolled + not locked** (no login requirement)

If you tell me which one your business uses, I’ll give the exact SQL/Spark change.

---

## One quick check (to confirm your expected 120k is Enrollment)

Run this and paste the number:

```sql
SELECT count(distinct rcif_number) as wealth_rcifs
FROM dm_ib_dev.wic2_wealth_fact
WHERE business_date = (SELECT max(business_date) FROM dm_ib_dev.wic2_wealth_fact);
```

Then compare:

* Enrollment penetration = `wealth_digital_enrollment_rcif / wealth_rcifs`
* Active penetration = `wealth_digital_active_rcif / wealth_rcifs`

That should line up with your dashboard’s ~33% vs ~something smaller.

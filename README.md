Use your **current tables**. Don’t rerun Spark yet.

Right now we have **one clear situation**:

* Wealth table is correct (268,984 RCIF ✅)
* All digital measures are 0 because **customer/digital table is wrong** (empty, bad flags, bad RCIF format, or missing IBNs)

The fastest fix is:

1. **Run 4 tiny Hive queries (takes 2 minutes)** to pinpoint *exactly* which of the 4 causes it is
2. Then I’ll give you a **new complete Spark script** that fixes only what’s broken (customer + digital join + account counting), and keeps your wealth RCIF stable.

So: **do NOT guess and do NOT rerun yet.** Run these:

---

## Run these 4 Hive SQL checks (copy/paste)

### A) Is `wia2_customer` populated + does it have IBNs?

```sql
SELECT
  count(*) as rows,
  count(distinct rcif_number) as rcifs,
  count(distinct primary_ibn) as ibns,
  sum(case when primary_ibn is null then 1 else 0 end) as null_ibn_rows
FROM dm_ib_dev.wia2_customer;
```

### B) What are the actual values in the flags? (your filters might not match)

```sql
SELECT digitally_active_flag, count(*) as cnt
FROM dm_ib_dev.wia2_customer
GROUP BY digitally_active_flag
ORDER BY cnt DESC;
```

```sql
SELECT digital_flag, count(*) as cnt
FROM dm_ib_dev.wia2_customer
GROUP BY digital_flag
ORDER BY cnt DESC;
```

### C) Does wealth RCIF match customer RCIF at all?

```sql
WITH mx AS (SELECT max(business_date) as dt FROM dm_ib_dev.wic2_wealth_fact),
wealth_latest AS (
  SELECT distinct cast(rcif_number as string) as rcif_number
  FROM dm_ib_dev.wic2_wealth_fact w
  JOIN mx ON w.business_date = mx.dt
)
SELECT
  (SELECT count(*) FROM wealth_latest) as wealth_rcifs,
  (SELECT count(distinct cast(rcif_number as string)) FROM dm_ib_dev.wia2_customer) as customer_rcifs,
  count(*) as matched_rcifs
FROM wealth_latest w
JOIN dm_ib_dev.wia2_customer c
  ON cast(w.rcif_number as string) = cast(c.rcif_number as string);
```

---

## After you paste those 4 outputs

I will give you a **full new Spark code** that fixes all issues at once:

✅ Fix digital = not 0 (correct IBN join + correct flags + correct snapshot date)
✅ Fix wealth accounts inflation (composite key counting)
✅ Keep wealth RCIF ≈ 269k stable
✅ Output final 2–3 tables for Power BI

---

### Why I’m not dumping “new full code” immediately

Because if your issue is simply:

* flags are `'Digital Active '` (extra space) OR
* rcif has leading zeros mismatch OR
* your `wia2_customer` table wrote empty

…then a full rebuild isn’t needed, and you’ll waste hours again.

Run those 4 checks, paste results, and I’ll respond with the final corrected end-to-end Spark script.

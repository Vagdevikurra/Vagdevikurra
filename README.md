Got it — if you *do* keep **InvestPath inserted/unioned into the Wealth fact**, then the only safe way is:

* **Every Wealth measure must explicitly filter `fact_type = "WEALTH"`**
* **Every InvestPath measure must explicitly filter `fact_type = "INVESTPATH"`**
* Anything that’s meant to be “Top of company” (wealth + investpath) must decide if it includes both or not.

Below is a complete DAX set you can paste and use. I’m assuming your combined fact table is **`wic2_wealth_fact`** and it has these columns (from your earlier logic):

* `business_date`
* `rcif_number`
* `fact_type`  (values: `"WEALTH"` / `"INVESTPATH"`)
* `accts_cnt`  (wealth accounts count)
* `division`, `business_group`
* InvestPath fields (if unioned): `ip_id`, `account_id`, `balance` (or your names)

And your customer table is **`wia2_customer`** with:

* `rcif_number`
* `digital_flag` (“Digital User” / “Non Digital User”)
* `digitally_active_flag` (“Digital Active” / “Non Digital Active”)
* maybe `primary_ibn` or `ibn`

And you have a **Calendar** table related to `wic2_wealth_fact[business_date]`.

---

## 0) Base measures you MUST create first

### Latest Wealth Date (only WEALTH)

```DAX
Latest Wealth Date =
CALCULATE(
    MAX ( wic2_wealth_fact[business_date] ),
    wic2_wealth_fact[fact_type] = "WEALTH"
)
```

### Latest InvestPath Date (only INVESTPATH)

```DAX
Latest InvestPath Date =
CALCULATE(
    MAX ( wic2_wealth_fact[business_date] ),
    wic2_wealth_fact[fact_type] = "INVESTPATH"
)
```

---

## 1) Wealth measures (always force fact_type="WEALTH")

### Wealth Customers (RCIF distinct on latest wealth date)

```DAX
Wealth Customers =
CALCULATE(
    DISTINCTCOUNT ( wic2_wealth_fact[rcif_number] ),
    wic2_wealth_fact[fact_type] = "WEALTH",
    wic2_wealth_fact[business_date] = [Latest Wealth Date]
)
```

### Wealth Accounts (SUM of accts_cnt on latest wealth date)

> This is why you were seeing ~676k instead of 303k earlier. If you want “accounts” to behave like “accounts per customer ~5-6”, you usually want **SUM(accts_cnt)**, not DISTINCTCOUNT(rcif) or COUNTROWS.

```DAX
Wealth Accounts =
CALCULATE(
    SUM ( wic2_wealth_fact[accts_cnt] ),
    wic2_wealth_fact[fact_type] = "WEALTH",
    wic2_wealth_fact[business_date] = [Latest Wealth Date]
)
```

### Accounts per Wealth Customer

```DAX
Accounts per Wealth Customer =
DIVIDE( [Wealth Accounts], [Wealth Customers] )
```

---

## 2) Digital measures (from `wia2_customer`)

### Top of Company Total Digital Active (RCIF count, Digital Active)

This is NOT wealth-specific — it’s across the whole customer table (top-of-company).

```DAX
Top Company Digital Active Customers =
CALCULATE(
    DISTINCTCOUNT ( wia2_customer[rcif_number] ),
    wia2_customer[digitally_active_flag] = "Digital Active"
)
```

### Top of Company Total Digital Users (RCIF count, Digital User)

```DAX
Top Company Digital Users =
CALCULATE(
    DISTINCTCOUNT ( wia2_customer[rcif_number] ),
    wia2_customer[digital_flag] = "Digital User"
)
```

---

## 3) Wealth + Digital intersection measures

### Digital Enrollments Wealth (Wealth customers who are Digital Users)

```DAX
Digital Enrollments Wealth =
CALCULATE(
    [Wealth Customers],
    TREATAS(
        FILTER(
            VALUES ( wia2_customer[rcif_number] ),
            wia2_customer[digital_flag] = "Digital User"
        ),
        wic2_wealth_fact[rcif_number]
    )
)
```

### Wealth Digital Active (Wealth customers who are Digital Active)

```DAX
Wealth Digital Active Customers =
CALCULATE(
    [Wealth Customers],
    TREATAS(
        FILTER(
            VALUES ( wia2_customer[rcif_number] ),
            wia2_customer[digitally_active_flag] = "Digital Active"
        ),
        wic2_wealth_fact[rcif_number]
    )
)
```

### Wealth Digital Active Penetration

```DAX
Wealth Digital Active Penetration =
DIVIDE( [Wealth Digital Active Customers], [Wealth Customers] )
```

### Wealth Digital Enrollment Penetration

```DAX
Wealth Digital Enrollment Penetration =
DIVIDE( [Digital Enrollments Wealth], [Wealth Customers] )
```

---

## 4) InvestPath measures (only if InvestPath is unioned into `wic2_wealth_fact`)

### InvestPath Customers (distinct IP IDs on latest investpath date)

```DAX
InvestPath Customers =
CALCULATE(
    DISTINCTCOUNT ( wic2_wealth_fact[ip_id] ),
    wic2_wealth_fact[fact_type] = "INVESTPATH",
    wic2_wealth_fact[business_date] = [Latest InvestPath Date]
)
```

### InvestPath Accounts (distinct arrangement/account id on latest investpath date)

```DAX
InvestPath Accounts =
CALCULATE(
    DISTINCTCOUNT ( wic2_wealth_fact[account_id] ),
    wic2_wealth_fact[fact_type] = "INVESTPATH",
    wic2_wealth_fact[business_date] = [Latest InvestPath Date]
)
```

### InvestPath AUM (sum balance on latest investpath date)

```DAX
InvestPath AUM =
CALCULATE(
    SUM ( wic2_wealth_fact[balance] ),
    wic2_wealth_fact[fact_type] = "INVESTPATH",
    wic2_wealth_fact[business_date] = [Latest InvestPath Date]
)
```

### InvestPath Avg Balance per Account

```DAX
InvestPath Avg Balance per Account =
DIVIDE( [InvestPath AUM], [InvestPath Accounts] )
```

### InvestPath Funded Accounts (balance > 0)

```DAX
InvestPath Funded Accounts =
CALCULATE(
    DISTINCTCOUNT ( wic2_wealth_fact[account_id] ),
    wic2_wealth_fact[fact_type] = "INVESTPATH",
    wic2_wealth_fact[business_date] = [Latest InvestPath Date],
    wic2_wealth_fact[balance] > 0
)
```

---

## 5) Sanity-check measures (so you can instantly see if union is breaking visuals)

### Fact rows by type (card/table)

```DAX
Rows WEALTH =
CALCULATE( COUNTROWS(wic2_wealth_fact), wic2_wealth_fact[fact_type]="WEALTH" )

Rows INVESTPATH =
CALCULATE( COUNTROWS(wic2_wealth_fact), wic2_wealth_fact[fact_type]="INVESTPATH" )
```

### RCIF count by type (latest)

```DAX
RCIF Latest WEALTH =
CALCULATE(
    DISTINCTCOUNT(wic2_wealth_fact[rcif_number]),
    wic2_wealth_fact[fact_type]="WEALTH",
    wic2_wealth_fact[business_date]=[Latest Wealth Date]
)

RCIF Latest INVESTPATH =
CALCULATE(
    DISTINCTCOUNT(wic2_wealth_fact[rcif_number]),
    wic2_wealth_fact[fact_type]="INVESTPATH",
    wic2_wealth_fact[business_date]=[Latest InvestPath Date]
)
```

---

## Critical note (this is the part that decides if your report breaks)

If any of your existing visuals/measures use something like:

* `DISTINCTCOUNT(wic2_wealth_fact[rcif_number])`
* `SUM(wic2_wealth_fact[accts_cnt])`
  without filtering `fact_type`,
  then the **InvestPath rows will contaminate Wealth visuals**.

So either:

1. Replace all Wealth measures with the **WEALTH-filtered** versions above, **or**
2. Don’t union InvestPath into this table (your earlier “separate table” plan).

---

If you paste your current column names for InvestPath inside the unioned table (is it `ip_id`/`account_id`/`balance` exactly or different?), I’ll adjust the InvestPath DAX names 1:1 so you can copy-paste with no edits.

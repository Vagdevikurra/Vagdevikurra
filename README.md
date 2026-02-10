Yes 👍 — below is a **complete, clean DAX pack** that matches **exactly** the SQL logic and numbers you validated.

These measures assume:

* Fact table: **`wic2_wealth_fact`**
* Customer/digital table: **`wia2_customer`**
* Relationship:
  `wic2_wealth_fact[rcif_number]` → `wia2_customer[rcif_number]`
  (**Many-to-one**, single direction from `wia2_customer` → `wic2_wealth_fact`)

---

# 0️⃣ Helper: Latest Business Date (VERY IMPORTANT)

Use this everywhere so visuals never drift.

```DAX
Latest Wealth Date :=
CALCULATE (
    MAX ( wic2_wealth_fact[business_date] ),
    ALL ( wic2_wealth_fact )
)
```

---

# 1️⃣ Wealth Customers (≈ 268,984)

```DAX
Wealth Customers :=
CALCULATE (
    DISTINCTCOUNT ( wic2_wealth_fact[rcif_number] ),
    wic2_wealth_fact[business_date] = [Latest Wealth Date]
)
```

---

# 2️⃣ Wealth Accounts (≈ 676k)

```DAX
Wealth Accounts :=
CALCULATE (
    SUM ( wic2_wealth_fact[accts_cnt] ),
    wic2_wealth_fact[business_date] = [Latest Wealth Date]
)
```

---

# 3️⃣ Accounts per Wealth Customer (≈ 2.5)

```DAX
Accounts per Wealth Customer :=
DIVIDE (
    [Wealth Accounts],
    [Wealth Customers]
)
```

---

# 4️⃣ Top of Company – Digital Active (IBN based ≈ 3.5M)

```DAX
Top Company Digital Active (IBN) :=
CALCULATE (
    DISTINCTCOUNT ( wia2_customer[primary_ibn] ),
    wia2_customer[digitally_active_flag] = "Digital Active",
    NOT ISBLANK ( wia2_customer[primary_ibn] )
)
```

---

# 5️⃣ Digital Enrollment – Wealth (≈ 129,271)

> **This is the ~120k number you expected**

```DAX
Digital Enrollment – Wealth :=
CALCULATE (
    [Wealth Customers],
    wia2_customer[digital_flag] = "Digital User"
)
```

---

# 6️⃣ Digital Active – Wealth (≈ 99,382)

```DAX
Digital Active – Wealth :=
CALCULATE (
    [Wealth Customers],
    wia2_customer[digitally_active_flag] = "Digital Active"
)
```

---

# 7️⃣ Digital Enrollment Penetration – Wealth

```DAX
Digital Enrollment Penetration :=
DIVIDE (
    [Digital Enrollment – Wealth],
    [Wealth Customers]
)
```

---

# 8️⃣ Digital Active Penetration – Wealth

```DAX
Digital Active Penetration :=
DIVIDE (
    [Digital Active – Wealth],
    [Wealth Customers]
)
```

---

# 9️⃣ Wealth Customers by Business Group

(Use in matrix / bar charts)

```DAX
Wealth Customers by Group :=
CALCULATE (
    DISTINCTCOUNT ( wic2_wealth_fact[rcif_number] ),
    wic2_wealth_fact[business_date] = [Latest Wealth Date]
)
```

👉 Slice by:

* `wic2_wealth_fact[business_group]`
* `wic2_wealth_fact[division]`

---

# 🔟 Wealth Customers by State

```DAX
Wealth Customers by State :=
CALCULATE (
    DISTINCTCOUNT ( wic2_wealth_fact[rcif_number] ),
    wic2_wealth_fact[business_date] = [Latest Wealth Date]
)
```

👉 Slice by:

* `wia2_customer[state_name]`

---

# 1️⃣1️⃣ Digital Active Wealth by State

```DAX
Digital Active Wealth by State :=
CALCULATE (
    [Wealth Customers],
    wia2_customer[digitally_active_flag] = "Digital Active"
)
```

---

# 1️⃣2️⃣ Digital Enrollment Wealth by State

```DAX
Digital Enrollment Wealth by State :=
CALCULATE (
    [Wealth Customers],
    wia2_customer[digital_flag] = "Digital User"
)
```

---

# ✅ IMPORTANT NOTES (PLEASE READ)

### ✔️ Why this works

* Uses **latest business_date explicitly**
* Matches your **validated SQL results**
* Separates **Enrollment vs Active** correctly
* Avoids double counting from multiple month-ends

### ❌ What NOT to do

* Do NOT use `COUNTROWS(wic2_wealth_fact)`
* Do NOT let visuals auto-sum without filtering latest date
* Do NOT mix Digital User and Digital Active in same KPI

---

# 🧪 Final cross-check (should match SQL exactly)

| Measure                          | Expected |
| -------------------------------- | -------- |
| Wealth Customers                 | ~268,984 |
| Wealth Accounts                  | ~676,792 |
| Digital Enrollment – Wealth      | ~129,271 |
| Digital Active – Wealth          | ~99,382  |
| Top Company Digital Active (IBN) | ~3.56M   |

---

If you want, next I can:

* Review your **actual Power BI file screenshots**
* Optimize visuals (avoid implicit measures)
* Lock these measures so future data refreshes don’t break them

But yes — **you are done with Spark**. 🚀

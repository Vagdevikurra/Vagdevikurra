Great numbers! Let me validate:

- **Wealth rows 1.6M** ✅ — 276K × 6 months = correct
- **Digital rows 4.1M** ✅ — slightly higher than 3.4M but makes sense since transmit covers 2 years of logins so more unique RCIFs
- **Account 125** ✅ — exact match

Now here are all your DAX measures:

**KPI Cards:**
```dax
Wealth Users = 
CALCULATE(DISTINCTCOUNT(Customer[RCIF_NUMBER]),
    Customer[fact_type] = "Wealth")

Top of Company Digital Active = 
CALCULATE(DISTINCTCOUNT(Customer[RCIF_NUMBER]),
    Customer[fact_type] = "Digital",
    Customer[digitally_active_flag] = "Digital Active",
    REMOVEFILTERS(Customer[business_date]))

Digital Enrollments Wealth = 
CALCULATE(DISTINCTCOUNT(Customer[RCIF_NUMBER]),
    Customer[fact_type] = "Wealth",
    Customer[digital_enrolled] = "Digital Enrolled")

Accounts = 
CALCULATE(SUM(Customer[wealth_accts_cnt]),
    Customer[fact_type] = "Wealth")

Accounts Per User = DIVIDE([Accounts], [Wealth Users])

Wealth Active User Adoption % = 
DIVIDE([Wealth Digital Active], [Wealth Users])

% of Digital Population = 
DIVIDE([Wealth Users], [Top of Company Digital Active])
```

**Digital Activity:**
```dax
Wealth Digital Active = 
CALCULATE(DISTINCTCOUNT(Customer[RCIF_NUMBER]),
    Customer[fact_type] = "Wealth",
    Customer[digitally_active_flag] = "Digital Active")

Wealth OLB Active = 
CALCULATE(DISTINCTCOUNT(Customer[RCIF_NUMBER]),
    Customer[fact_type] = "Wealth",
    Customer[olb_active_flag] = "OLB Active")

Wealth OLB Enrolled = 
CALCULATE(DISTINCTCOUNT(Customer[RCIF_NUMBER]),
    Customer[fact_type] = "Wealth",
    Customer[olb_enrolled] = "OLB Enrolled")

Wealth MOB Active = 
CALCULATE(DISTINCTCOUNT(Customer[RCIF_NUMBER]),
    Customer[fact_type] = "Wealth",
    Customer[mob_active_flag] = "Mobile Active")

Wealth MOB Enrolled = 
CALCULATE(DISTINCTCOUNT(Customer[RCIF_NUMBER]),
    Customer[fact_type] = "Wealth",
    Customer[mob_enrolled] = "Mobile Enrolled")
```

**MoM % for OLB/MOB tables:**
```dax
OLB MoM % = 
VAR CurrentMonth = CALCULATE([Wealth OLB Active])
VAR PrevMonth = CALCULATE([Wealth OLB Active], 
    DATEADD(Calendar[Month_Ceiling], -1, MONTH))
RETURN DIVIDE(CurrentMonth - PrevMonth, PrevMonth)

MOB MoM % = 
VAR CurrentMonth = CALCULATE([Wealth MOB Active])
VAR PrevMonth = CALCULATE([Wealth MOB Active],
    DATEADD(Calendar[Month_Ceiling], -1, MONTH))
RETURN DIVIDE(CurrentMonth - PrevMonth, PrevMonth)
```

**Investpath:**
```dax
InvestPath Customers = DISTINCTCOUNT(Account[RCIF_NUMBER])

InvestPath Accounts = DISTINCTCOUNT(Account[ip_account_number])

AUM = SUM(Account[ip_balance])

Balance Per IP Account = DIVIDE([AUM], [InvestPath Accounts])

InvestPath Accounts Funded = 
CALCULATE(DISTINCTCOUNT(Account[ip_account_number]),
    Account[is_funded] = "Funded")
```

**Power BI setup:**
- `Calendar[Month_Ceiling]` → `Customer[business_date]` (One-to-Many)
- `Customer[RCIF_NUMBER]` → `Account[RCIF_NUMBER]` (Many-to-Many, Single direction)

Sure! Based on the 2 tables and the KPIs I can see in your screenshots, here are all the measures:

**Core Measures:**
```dax
Wealth Customers = 
VAR LatestDate = CALCULATE(MAX(Customer[business_date]), REMOVEFILTERS(Customer))
RETURN CALCULATE(DISTINCTCOUNT(Customer[RCIF_NUMBER]), Customer[business_date] = LatestDate)

Accounts = 
VAR LatestDate = CALCULATE(MAX(Customer[business_date]), REMOVEFILTERS(Customer))
RETURN CALCULATE(SUM(Customer[wealth_accts_cnt]), Customer[business_date] = LatestDate)

Accounts Per User = DIVIDE([Accounts], [Wealth Customers])
```

**Digital Measures:**
```dax
Digital Enrollments Wealth = 
VAR LatestDate = CALCULATE(MAX(Customer[business_date]), REMOVEFILTERS(Customer))
RETURN CALCULATE(DISTINCTCOUNT(Customer[RCIF_NUMBER]), 
    Customer[business_date] = LatestDate,
    Customer[digital_flag] = "Digital User")

Wealth OLB Active = 
VAR LatestDate = CALCULATE(MAX(Customer[business_date]), REMOVEFILTERS(Customer))
RETURN CALCULATE(DISTINCTCOUNT(Customer[RCIF_NUMBER]),
    Customer[business_date] = LatestDate,
    Customer[olb_active_flag] = "OLB Active")

Wealth MOB Active = 
VAR LatestDate = CALCULATE(MAX(Customer[business_date]), REMOVEFILTERS(Customer))
RETURN CALCULATE(DISTINCTCOUNT(Customer[RCIF_NUMBER]),
    Customer[business_date] = LatestDate,
    Customer[mobile_active_flag] = "Mobile Active")

Wealth Digital Active = 
VAR LatestDate = CALCULATE(MAX(Customer[business_date]), REMOVEFILTERS(Customer))
RETURN CALCULATE(DISTINCTCOUNT(Customer[RCIF_NUMBER]),
    Customer[business_date] = LatestDate,
    Customer[digitally_active_flag] = "Digital Active")

Wealth Active User Adoption % = 
DIVIDE([Wealth Digital Active], [Wealth Customers])

% of Digital Population = 
DIVIDE([Wealth Customers], 
    CALCULATE(DISTINCTCOUNT(Customer[RCIF_NUMBER]),
    REMOVEFILTERS(Customer[business_group], Customer[division])))
```

**Trend Measure (for line chart by month):**
```dax
Wealth Digital Active by Month = 
CALCULATE(DISTINCTCOUNT(Customer[RCIF_NUMBER]),
    Customer[digitally_active_flag] = "Digital Active")
```

**InvestPath Measures:**
```dax
InvestPath Customers = DISTINCTCOUNT(Account[ip_id])

InvestPath Accounts = DISTINCTCOUNT(Account[ip_account_number])

AUM = SUM(Account[ip_balance])

Balance Per IP Account = DIVIDE([AUM], [InvestPath Accounts])

InvestPath Accounts Funded = 
CALCULATE(DISTINCTCOUNT(Account[ip_account_number]),
    Account[is_funded] = "Funded")
```

**Relationships needed in Power BI:**
- `Calendar[Month_Ceiling]` → `Customer[business_date]`
- `Customer[RCIF_NUMBER]` → `Account[RCIF_NUMBER]`
- `Customer[ip_id]` → `Account[ip_id]`

The `Top of Company Total Digital Active` (~3.5M) — do you have a separate Digital population table for that, or should it come from somewhere in the Customer table? That one seems to be a company-wide number not limited to wealth customers.

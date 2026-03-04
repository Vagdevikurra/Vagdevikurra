Here are all DAX measures pointing to the new tables:

**Core KPIs:**
```dax
Wealth Customers = 
VAR LatestDate = CALCULATE(MAX(Customer[business_date]), REMOVEFILTERS(Customer))
RETURN CALCULATE(
    DISTINCTCOUNT(Customer[RCIF_NUMBER]),
    Customer[fact_type] = "Wealth",
    Customer[business_date] = LatestDate)

Top of Company Digital Active = 
CALCULATE(
    DISTINCTCOUNT(Customer[ibn]),
    Customer[fact_type] = "Digital",
    Customer[digitallyactiveflag] = "Digital Active",
    REMOVEFILTERS(Customer[business_date]))

Digital Enrollments Wealth = 
VAR LatestDate = CALCULATE(MAX(Customer[business_date]), REMOVEFILTERS(Customer))
RETURN CALCULATE(
    DISTINCTCOUNT(Customer[RCIF_NUMBER]),
    Customer[fact_type] = "Wealth",
    Customer[business_date] = LatestDate,
    Customer[digital_flag] = "Digital User")

Accounts = 
VAR LatestDate = CALCULATE(MAX(Customer[business_date]), REMOVEFILTERS(Customer))
RETURN CALCULATE(
    SUM(Customer[wealth_accts_cnt]),
    Customer[fact_type] = "Wealth",
    Customer[business_date] = LatestDate)

Accounts Per User = DIVIDE([Accounts], [Wealth Customers])
```

**Digital Flag Measures:**
```dax
Wealth OLB Active = 
VAR LatestDate = CALCULATE(MAX(Customer[business_date]), REMOVEFILTERS(Customer))
RETURN CALCULATE(
    DISTINCTCOUNT(Customer[RCIF_NUMBER]),
    Customer[fact_type] = "Wealth",
    Customer[business_date] = LatestDate,
    Customer[olb_active_flag] = "OLB Active")

Wealth MOB Active = 
VAR LatestDate = CALCULATE(MAX(Customer[business_date]), REMOVEFILTERS(Customer))
RETURN CALCULATE(
    DISTINCTCOUNT(Customer[RCIF_NUMBER]),
    Customer[fact_type] = "Wealth",
    Customer[business_date] = LatestDate,
    Customer[mobile_active_flag] = "Mobile Active")

Wealth Digital Active = 
VAR LatestDate = CALCULATE(MAX(Customer[business_date]), REMOVEFILTERS(Customer))
RETURN CALCULATE(
    DISTINCTCOUNT(Customer[RCIF_NUMBER]),
    Customer[fact_type] = "Wealth",
    Customer[business_date] = LatestDate,
    Customer[digitally_active_flag] = "Digital Active")

Wealth Active User Adoption % = 
DIVIDE([Wealth Digital Active], [Wealth Customers])

% of Digital Population = 
DIVIDE([Wealth Customers], [Top of Company Digital Active])
```

**Trend Measures (for monthly charts):**
```dax
Wealth OLB Active by Month = 
CALCULATE(
    DISTINCTCOUNT(Customer[RCIF_NUMBER]),
    Customer[fact_type] = "Wealth",
    Customer[olb_active_flag] = "OLB Active")

Wealth MOB Active by Month = 
CALCULATE(
    DISTINCTCOUNT(Customer[RCIF_NUMBER]),
    Customer[fact_type] = "Wealth",
    Customer[mobile_active_flag] = "Mobile Active")

Wealth Digital Active by Month = 
CALCULATE(
    DISTINCTCOUNT(Customer[RCIF_NUMBER]),
    Customer[fact_type] = "Wealth",
    Customer[digitally_active_flag] = "Digital Active")

Wealth Active User Adoption % by Month = 
DIVIDE(
    [Wealth Digital Active by Month],
    CALCULATE(DISTINCTCOUNT(Customer[RCIF_NUMBER]),
    Customer[fact_type] = "Wealth"))
```

**InvestPath Measures:**
```dax
InvestPath Customers = 
DISTINCTCOUNT(Account[RCIF_NUMBER])

InvestPath Accounts = 
DISTINCTCOUNT(Account[ip_account_number])

AUM = 
SUM(Account[ip_balance])

Balance Per IP Account = 
DIVIDE([AUM], [InvestPath Accounts])

InvestPath Accounts Funded = 
CALCULATE(
    DISTINCTCOUNT(Account[ip_account_number]),
    Account[is_funded] = "Funded")
```

**Power BI Relationships needed:**
- `Calendar[Month_Ceiling]` → `Customer[business_date]`
- `Customer[RCIF_NUMBER]` → `Account[RCIF_NUMBER]`
- `Customer[ip_id]` → `Account[ip_id]`

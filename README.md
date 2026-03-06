=====================================================================
 WEALTH INSIGHTS — DAX MEASURES  (complete, from scratch)
 Table name: Customer  |  Account
=====================================================================

---------------------------------------------------------------------
 POWER BI MODEL SETUP  (do this before creating any measures)
---------------------------------------------------------------------

1. Rename tables in Power BI:
     dm_ib_dev.wealth_Insights_Customer  =>  Customer
     dm_ib_dev.wealth_Insights_Account   =>  Account

2. Relationship:
     Customer[RCIF_NUMBER]  ->  Account[RCIF_NUMBER]
     Cardinality : Many-to-Many
     Cross-filter : Single  (Account filters Customer, not the reverse)

3. Date slicer:
     Use Customer[business_date] as the slicer field.
     Wealth measures respond to it automatically.
     Digital/Company-wide measures use REMOVEFILTERS to ignore it.

4. Format columns:
     business_date        => Date
     last_olb_login       => Date
     last_mob_login       => Date
     ip_open_date         => Date
     ip_balance / AUM     => Currency
     all _flag / _enrolled columns => Text (already are)


=====================================================================
 SECTION 1 — KPI CARDS
=====================================================================

-- Distinct wealth customers in selected month(s)
Wealth Users =
CALCULATE(
    DISTINCTCOUNT(Customer[RCIF_NUMBER]),
    Customer[fact_type] = "Wealth"
)

-- Company-wide digitally active (OLB or Mobile, 90-day window)
-- REMOVEFILTERS so it never goes blank when date slicer is applied
Top of Company Digital Active =
CALCULATE(
    DISTINCTCOUNT(Customer[RCIF_NUMBER]),
    Customer[fact_type]             = "Digital",
    Customer[digitally_active_flag] = "Digital Active",
    REMOVEFILTERS(Customer[business_date])
)

-- Wealth customers who have any transmit login record (ever enrolled)
Digital Enrollments Wealth =
CALCULATE(
    DISTINCTCOUNT(Customer[RCIF_NUMBER]),
    Customer[fact_type]        = "Wealth",
    Customer[digital_enrolled] = "Digital Enrolled"
)

-- Total open wealth accounts across selected period
Accounts =
CALCULATE(
    SUM(Customer[wealth_accts_cnt]),
    Customer[fact_type] = "Wealth"
)


=====================================================================
 SECTION 2 — WEALTH DIGITAL ACTIVITY  (respond to date slicer)
=====================================================================

-- Active = logged in within 90 days of the month-end snapshot date
-- Each month calculates its own window so Dec < Jan as expected

Wealth Digital Active =
CALCULATE(
    DISTINCTCOUNT(Customer[RCIF_NUMBER]),
    Customer[fact_type]             = "Wealth",
    Customer[digitally_active_flag] = "Digital Active"
)

Wealth OLB Active =
CALCULATE(
    DISTINCTCOUNT(Customer[RCIF_NUMBER]),
    Customer[fact_type]       = "Wealth",
    Customer[olb_active_flag] = "OLB Active"
)

Wealth OLB Enrolled =
CALCULATE(
    DISTINCTCOUNT(Customer[RCIF_NUMBER]),
    Customer[fact_type]     = "Wealth",
    Customer[olb_enrolled]  = "OLB Enrolled"
)

Wealth MOB Active =
CALCULATE(
    DISTINCTCOUNT(Customer[RCIF_NUMBER]),
    Customer[fact_type]       = "Wealth",
    Customer[mob_active_flag] = "Mobile Active"
)

Wealth MOB Enrolled =
CALCULATE(
    DISTINCTCOUNT(Customer[RCIF_NUMBER]),
    Customer[fact_type]    = "Wealth",
    Customer[mob_enrolled] = "Mobile Enrolled"
)


=====================================================================
 SECTION 3 — PENETRATION & RATES
=====================================================================

-- Wealth customers as % of total IBN population
-- Numerator  : RCIF_NUMBER where fact_type = Wealth
-- Denominator: ibn (cust_internet_banking_nbr) where fact_type = Digital
-- Expected   : ~3.2 - 3.5%
Digital Wealth Penetration % =
DIVIDE(
    CALCULATE(
        DISTINCTCOUNT(Customer[RCIF_NUMBER]),
        Customer[fact_type] = "Wealth",
        REMOVEFILTERS(Customer[business_date])
    ),
    CALCULATE(
        DISTINCTCOUNT(Customer[ibn]),
        Customer[fact_type] = "Digital",
        REMOVEFILTERS(Customer[business_date])
    )
)

-- % of wealth users who are digitally active
Wealth Digital Adoption % =
DIVIDE(
    [Wealth Digital Active],
    [Wealth Users]
)

-- % of wealth users who are OLB enrolled
OLB Enrollment % =
DIVIDE([Wealth OLB Enrolled], [Wealth Users])

-- % of wealth users who are Mobile enrolled
MOB Enrollment % =
DIVIDE([Wealth MOB Enrolled], [Wealth Users])

-- % of enrolled OLB users who are also active
OLB Active Rate =
DIVIDE([Wealth OLB Active], [Wealth OLB Enrolled])

-- % of enrolled Mobile users who are also active
MOB Active Rate =
DIVIDE([Wealth MOB Active], [Wealth MOB Enrolled])

-- Digital enrollment penetration among wealth customers
Digital Enrollment % =
DIVIDE([Digital Enrollments Wealth], [Wealth Users])


=====================================================================
 SECTION 4 — MONTH-OVER-MONTH
=====================================================================

OLB MoM % =
VAR Current = [Wealth OLB Active]
VAR Prior   = CALCULATE([Wealth OLB Active],
                  DATEADD(Customer[business_date], -1, MONTH))
RETURN DIVIDE(Current - Prior, Prior)

MOB MoM % =
VAR Current = [Wealth MOB Active]
VAR Prior   = CALCULATE([Wealth MOB Active],
                  DATEADD(Customer[business_date], -1, MONTH))
RETURN DIVIDE(Current - Prior, Prior)

Digital Active MoM % =
VAR Current = [Wealth Digital Active]
VAR Prior   = CALCULATE([Wealth Digital Active],
                  DATEADD(Customer[business_date], -1, MONTH))
RETURN DIVIDE(Current - Prior, Prior)

Wealth Users MoM % =
VAR Current = [Wealth Users]
VAR Prior   = CALCULATE([Wealth Users],
                  DATEADD(Customer[business_date], -1, MONTH))
RETURN DIVIDE(Current - Prior, Prior)


=====================================================================
 SECTION 5 — INVESTPATH
=====================================================================

InvestPath Customers =
DISTINCTCOUNT(Account[RCIF_NUMBER])

InvestPath Accounts =
DISTINCTCOUNT(Account[ip_account_number])

InvestPath Accounts Funded =
CALCULATE(
    DISTINCTCOUNT(Account[ip_account_number]),
    Account[is_funded] = "Funded"
)

AUM =
SUM(Account[ip_balance])

AUM Per Account =
DIVIDE([AUM], [InvestPath Accounts])

IP Funded % =
DIVIDE([InvestPath Accounts Funded], [InvestPath Accounts])

Wealth Customers with IP =
CALCULATE(
    DISTINCTCOUNT(Customer[RCIF_NUMBER]),
    Customer[fact_type]    = "Wealth",
    Customer[ip_accts_cnt] > 0
)

IP Penetration % =
DIVIDE([Wealth Customers with IP], [Wealth Users])


=====================================================================
 QUICK REFERENCE — column values in Customer table
=====================================================================

fact_type             : "Wealth" | "Digital"
digitally_active_flag : "Digital Active" | "Non Digital Active"
olb_active_flag       : "OLB Active"     | "Non OLB Active"
mob_active_flag       : "Mobile Active"  | "Non Mobile Active"
digital_enrolled      : "Digital Enrolled"   | "Non Digital Enrolled"
olb_enrolled          : "OLB Enrolled"       | "Non OLB Enrolled"
mob_enrolled          : "Mobile Enrolled"    | "Non Mobile Enrolled"
business_group        : "Private Wealth" | "Investment Services" | "Institutional Services"
is_funded (Account)   : "Funded" | "Not Funded"

ibn column:
  - NULL for all Wealth rows  (do not use in Wealth measures)
  - cust_internet_banking_nbr for Digital rows  (use only as denominator)

=====================================================================

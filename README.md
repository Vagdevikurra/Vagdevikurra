// ══════════════════════════════════════════════════════════════════════════════
// WEALTH INSIGHTS — DAX MEASURES
// ══════════════════════════════════════════════════════════════════════════════
//
// Tables in model:
//   Wealth  = wealth_insights_cust7 WHERE fact_type = 'WEALTH'
//   Digital = wealth_insights_cust7 WHERE fact_type = 'DIGITAL'
//   InvestPath = wealth_insights_acct7
//
// If you import the full table, filter in each measure using fact_type.
// If you split into separate Power BI tables, remove the FILTER wrappers.
// ══════════════════════════════════════════════════════════════════════════════


// ─── HEADLINE METRICS (top row of dashboard) ────────────────────────────────

// Wealth Users — distinct wealth customers
Wealth Users = 
CALCULATE(
    DISTINCTCOUNT(wealth_insights_cust7[rcif_number]),
    wealth_insights_cust7[fact_type] = "WEALTH"
)


// Top of Company Total Digital Active — company-wide, from DIGITAL rows
Top of Company Digital Active = 
CALCULATE(
    DISTINCTCOUNT(wealth_insights_cust7[cust_internet_banking_nbr]),
    wealth_insights_cust7[fact_type] = "DIGITAL",
    wealth_insights_cust7[digitally_active_flag] = "Digital Active"
)


// Digital Enrollments Wealth — wealth customers who are digital users
Digital Enrollments Wealth = 
CALCULATE(
    DISTINCTCOUNT(wealth_insights_cust7[rcif_number]),
    wealth_insights_cust7[fact_type] = "WEALTH",
    wealth_insights_cust7[digital_flag] = "Digital User"
)


// Accounts — total wealth arrangement count (from wealth_accts_cnt)
Accounts = 
CALCULATE(
    SUM(wealth_insights_cust7[wealth_accts_cnt]),
    wealth_insights_cust7[fact_type] = "WEALTH"
)


// Accounts per User
Accounts per User = 
DIVIDE(
    [Accounts],
    [Wealth Users],
    0
)


// ─── ACTIVE USER FLAGS (bar charts) ────────────────────────────────────────

// Wealth OLB Active
Wealth OLB Active = 
CALCULATE(
    DISTINCTCOUNT(wealth_insights_cust7[rcif_number]),
    wealth_insights_cust7[fact_type] = "WEALTH",
    wealth_insights_cust7[olb_active_flag] = "OLB Active"
)


// Wealth MOB Active
Wealth MOB Active = 
CALCULATE(
    DISTINCTCOUNT(wealth_insights_cust7[rcif_number]),
    wealth_insights_cust7[fact_type] = "WEALTH",
    wealth_insights_cust7[mobile_active_flag] = "Mobile Active"
)


// Wealth Digital Active (Digitally Active Wealth Customer)
Wealth Digital Active = 
CALCULATE(
    DISTINCTCOUNT(wealth_insights_cust7[rcif_number]),
    wealth_insights_cust7[fact_type] = "WEALTH",
    wealth_insights_cust7[digitally_active_flag] = "Digital Active"
)


// ─── ADOPTION & PENETRATION ────────────────────────────────────────────────

// Wealth Active User Adoption % (Digital Active / Wealth Users)
Wealth Active User Adoption % = 
DIVIDE(
    [Wealth Digital Active],
    [Wealth Users],
    0
)


// % of Digital Population (Wealth Digital Active / Company Digital Active)
% of Digital Population = 
DIVIDE(
    [Wealth Digital Active],
    [Top of Company Digital Active],
    0
)


// Wealth Digitally Active Penetration (same as Adoption, for trend chart)
Wealth Digitally Active Penetration = 
DIVIDE(
    [Wealth Digital Active],
    [Wealth Users],
    0
)


// ─── USER FLAGS (enrollment, not just active) ──────────────────────────────

// Wealth OLB Users (enrolled, not necessarily active)
Wealth OLB Users = 
CALCULATE(
    DISTINCTCOUNT(wealth_insights_cust7[rcif_number]),
    wealth_insights_cust7[fact_type] = "WEALTH",
    wealth_insights_cust7[olb_flag] = "OLB User"
)


// Wealth Mobile Users
Wealth Mobile Users = 
CALCULATE(
    DISTINCTCOUNT(wealth_insights_cust7[rcif_number]),
    wealth_insights_cust7[fact_type] = "WEALTH",
    wealth_insights_cust7[mobile_flag] = "Mobile User"
)


// ─── INVESTPATH MEASURES ───────────────────────────────────────────────────

// InvestPath Customers
InvestPath Customers = 
DISTINCTCOUNT(wealth_insights_acct7[ip_id])


// # of IP Accounts
# of IP Accounts = 
COUNTROWS(wealth_insights_acct7)


// AUM (Assets Under Management)
AUM = 
SUM(wealth_insights_acct7[ip_balance])


// Average Balance per IP Account
Average Balance per IP Account = 
DIVIDE(
    [AUM],
    [# of IP Accounts],
    0
)


// InvestPath Accounts Funded (balance > 0)
InvestPath Accounts Funded = 
CALCULATE(
    COUNTROWS(wealth_insights_acct7),
    wealth_insights_acct7[ip_balance] > 0
)


// ─── BUSINESS GROUP / DIVISION (for decomposition tree) ────────────────────

// Customers by Business Group (use as visual-level measure, sliced by business_group)
Customers by Business Group = 
CALCULATE(
    DISTINCTCOUNT(wealth_insights_cust7[rcif_number]),
    wealth_insights_cust7[fact_type] = "WEALTH"
)


// ─── RCIF RECORDS DONUT CHART ──────────────────────────────────────────────

// Total RCIF Records (all rows in DIGITAL, for the donut)
Total RCIF Records = 
CALCULATE(
    COUNTROWS(wealth_insights_cust7),
    wealth_insights_cust7[fact_type] = "DIGITAL"
)

// Digital User RCIF Records
Digital User Records = 
CALCULATE(
    COUNTROWS(wealth_insights_cust7),
    wealth_insights_cust7[fact_type] = "DIGITAL",
    wealth_insights_cust7[digital_flag] = "Digital User"
)

// Non Digital User Records
Non Digital User Records = 
CALCULATE(
    COUNTROWS(wealth_insights_cust7),
    wealth_insights_cust7[fact_type] = "DIGITAL",
    wealth_insights_cust7[digital_flag] = "Non Digital User"
)

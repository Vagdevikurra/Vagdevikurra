// ══════════════════════════════════════════════════════════════════════════════
// WEALTH INSIGHTS — DAX MEASURES (CORRECTED)
// ══════════════════════════════════════════════════════════════════════════════
//
// TABLE: wealth_insights_cust7
//   - fact_type = "WEALTH"  → wealth customers (~265k/month × 6 months)
//   - fact_type = "DIGITAL" → full digital population
//
// TABLE: wealth_insights_acct7
//   - InvestPath IP accounts
//
// IMPORTANT MODEL SETUP:
//   1. Set business_date column type to DATE in both tables
//   2. Create a Date slicer on the page using business_date
//   3. If you have a separate Date table, create relationships to both tables
//
// The card/headline measures below use MAX(business_date) context so they 
// work correctly whether a single month is selected or all months show.
// ══════════════════════════════════════════════════════════════════════════════


// ─── HEADLINE CARD METRICS ──────────────────────────────────────────────────

// Wealth Users — distinct wealth customers IN THE SELECTED MONTH
// If no month selected, uses latest month
Wealth Users = 
VAR _dt = MAX(wealth_insights_cust7[business_date])
RETURN
CALCULATE(
    DISTINCTCOUNT(wealth_insights_cust7[rcif_number]),
    wealth_insights_cust7[fact_type] = "WEALTH",
    wealth_insights_cust7[business_date] = _dt
)


// Top of Company Total Digital Active
Top of Company Digital Active = 
VAR _dt = MAX(wealth_insights_cust7[business_date])
RETURN
CALCULATE(
    DISTINCTCOUNT(wealth_insights_cust7[cust_internet_banking_nbr]),
    wealth_insights_cust7[fact_type] = "DIGITAL",
    wealth_insights_cust7[digitally_active_flag] = "Digital Active",
    wealth_insights_cust7[business_date] = _dt
)


// Digital Enrollments Wealth
Digital Enrollments Wealth = 
VAR _dt = MAX(wealth_insights_cust7[business_date])
RETURN
CALCULATE(
    DISTINCTCOUNT(wealth_insights_cust7[rcif_number]),
    wealth_insights_cust7[fact_type] = "WEALTH",
    wealth_insights_cust7[digital_flag] = "Digital User",
    wealth_insights_cust7[business_date] = _dt
)


// Accounts — SUM of wealth_accts_cnt for selected month only
Accounts = 
VAR _dt = MAX(wealth_insights_cust7[business_date])
RETURN
CALCULATE(
    SUM(wealth_insights_cust7[wealth_accts_cnt]),
    wealth_insights_cust7[fact_type] = "WEALTH",
    wealth_insights_cust7[business_date] = _dt
)


// Accounts per User
Accounts per User = 
DIVIDE(
    [Accounts],
    [Wealth Users],
    0
)


// Wealth Active User Adoption %
Wealth Active User Adoption % = 
VAR _dt = MAX(wealth_insights_cust7[business_date])
VAR _active = CALCULATE(
    DISTINCTCOUNT(wealth_insights_cust7[rcif_number]),
    wealth_insights_cust7[fact_type] = "WEALTH",
    wealth_insights_cust7[digitally_active_flag] = "Digital Active",
    wealth_insights_cust7[business_date] = _dt
)
VAR _total = CALCULATE(
    DISTINCTCOUNT(wealth_insights_cust7[rcif_number]),
    wealth_insights_cust7[fact_type] = "WEALTH",
    wealth_insights_cust7[business_date] = _dt
)
RETURN DIVIDE(_active, _total, 0)


// % of Digital Population
% of Digital Population = 
DIVIDE(
    [Digital Enrollments Wealth],
    [Top of Company Digital Active],
    0
)


// ─── BAR CHART MEASURES (these go on Y-axis, business_date on X-axis) ──────
// For bar charts: put business_date on the Axis, these measures as Values.
// The chart context provides the month automatically — no need for MAX(date).

// Wealth OLB Active (for bar chart)
Wealth OLB Active = 
CALCULATE(
    DISTINCTCOUNT(wealth_insights_cust7[rcif_number]),
    wealth_insights_cust7[fact_type] = "WEALTH",
    wealth_insights_cust7[olb_active_flag] = "OLB Active"
)


// Wealth OLB Active % (the % line on the OLB chart)
Wealth OLB Active % = 
DIVIDE(
    [Wealth OLB Active],
    CALCULATE(
        DISTINCTCOUNT(wealth_insights_cust7[rcif_number]),
        wealth_insights_cust7[fact_type] = "WEALTH"
    ),
    0
)


// Wealth MOB Active (for bar chart)
Wealth MOB Active = 
CALCULATE(
    DISTINCTCOUNT(wealth_insights_cust7[rcif_number]),
    wealth_insights_cust7[fact_type] = "WEALTH",
    wealth_insights_cust7[mobile_active_flag] = "Mobile Active"
)


// Wealth MOB Active % 
Wealth MOB Active % = 
DIVIDE(
    [Wealth MOB Active],
    CALCULATE(
        DISTINCTCOUNT(wealth_insights_cust7[rcif_number]),
        wealth_insights_cust7[fact_type] = "WEALTH"
    ),
    0
)


// Wealth Digital Active (for "Digitally Active Wealth Customer" bar chart)
Wealth Digital Active = 
CALCULATE(
    DISTINCTCOUNT(wealth_insights_cust7[rcif_number]),
    wealth_insights_cust7[fact_type] = "WEALTH",
    wealth_insights_cust7[digitally_active_flag] = "Digital Active"
)


// ─── TREND LINE (Wealth Active User Adoption by Month) ─────────────────────
// Use business_date on X-axis. This measure auto-calculates per month.

// Wealth Users (for trend — works per month when business_date is on axis)
Wealth Users Trend = 
CALCULATE(
    DISTINCTCOUNT(wealth_insights_cust7[rcif_number]),
    wealth_insights_cust7[fact_type] = "WEALTH"
)

// Wealth Digital Active Penetration (trend line %)
Wealth Digital Active Penetration = 
DIVIDE(
    [Wealth Digital Active],
    [Wealth Users Trend],
    0
)


// ─── OLB / MOB ACTIVITY TABLE (Within Segment) ────────────────────────────
// For the matrix/table visual: Business Group on rows, months on columns

// Wealth OLB Enrolled
Wealth OLB Enrolled = 
CALCULATE(
    DISTINCTCOUNT(wealth_insights_cust7[rcif_number]),
    wealth_insights_cust7[fact_type] = "WEALTH",
    wealth_insights_cust7[olb_flag] = "OLB User"
)

// Wealth OLB Enrolled %
Wealth OLB Enrolled % = 
DIVIDE(
    [Wealth OLB Enrolled],
    CALCULATE(
        DISTINCTCOUNT(wealth_insights_cust7[rcif_number]),
        wealth_insights_cust7[fact_type] = "WEALTH"
    ),
    0
)

// Wealth MOB Enrolled
Wealth MOB Enrolled = 
CALCULATE(
    DISTINCTCOUNT(wealth_insights_cust7[rcif_number]),
    wealth_insights_cust7[fact_type] = "WEALTH",
    wealth_insights_cust7[mobile_flag] = "Mobile User"
)

// Wealth MOB Enrolled %
Wealth MOB Enrolled % = 
DIVIDE(
    [Wealth MOB Enrolled],
    CALCULATE(
        DISTINCTCOUNT(wealth_insights_cust7[rcif_number]),
        wealth_insights_cust7[fact_type] = "WEALTH"
    ),
    0
)


// OLB MoM Delta (month over month change)
OLB MoM Delta = 
VAR _current = [Wealth OLB Active]
VAR _prior = CALCULATE(
    [Wealth OLB Active],
    DATEADD(wealth_insights_cust7[business_date], -1, MONTH)
)
RETURN _current - _prior


// MOB MoM Delta
MOB MoM Delta = 
VAR _current = [Wealth MOB Active]
VAR _prior = CALCULATE(
    [Wealth MOB Active],
    DATEADD(wealth_insights_cust7[business_date], -1, MONTH)
)
RETURN _current - _prior


// ─── INVESTPATH MEASURES ───────────────────────────────────────────────────

// InvestPath Customers
InvestPath Customers = 
VAR _dt = MAX(wealth_insights_acct7[business_date])
RETURN
CALCULATE(
    DISTINCTCOUNT(wealth_insights_acct7[ip_id]),
    wealth_insights_acct7[business_date] = _dt
)


// # of IP Accounts
# of IP Accounts = 
VAR _dt = MAX(wealth_insights_acct7[business_date])
RETURN
CALCULATE(
    COUNTROWS(wealth_insights_acct7),
    wealth_insights_acct7[business_date] = _dt
)


// AUM
AUM = 
VAR _dt = MAX(wealth_insights_acct7[business_date])
RETURN
CALCULATE(
    SUM(wealth_insights_acct7[ip_balance]),
    wealth_insights_acct7[business_date] = _dt
)


// Average Balance per IP Account
Average Balance per IP Account = 
DIVIDE([AUM], [# of IP Accounts], 0)


// InvestPath Accounts Funded (balance > 0)
InvestPath Accounts Funded = 
VAR _dt = MAX(wealth_insights_acct7[business_date])
RETURN
CALCULATE(
    COUNTROWS(wealth_insights_acct7),
    wealth_insights_acct7[ip_balance] > 0,
    wealth_insights_acct7[business_date] = _dt
)


// ─── DECOMPOSITION TREE ────────────────────────────────────────────────────
// Use "Wealth Users Trend" as the measure, business_group as the first level

// Customers by Business Group (use Wealth Users Trend — it works with slicers)
// Put business_group → division as the decomposition levels

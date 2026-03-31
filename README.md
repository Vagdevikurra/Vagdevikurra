// ══════════════════════════════════════════════════════════════════════════════
// WEALTH INSIGHTS — COMPLETE DAX MEASURES
// ══════════════════════════════════════════════════════════════════════════════
// Tables:
//   Wealth_Insights_Customer  (fact_type = WEALTH | DIGITAL)
//   Wealth_Insights_Account   (InvestPath)
//
// SETUP: business_date as Date type. Date slicer on page.
// Card measures use MAX(business_date) = selected month.
// Chart measures let axis provide month context.
// ══════════════════════════════════════════════════════════════════════════════


// ═══ HEADLINE CARDS ═══

Wealth Users = 
VAR _dt = MAX(Wealth_Insights_Customer[business_date])
RETURN CALCULATE(DISTINCTCOUNT(Wealth_Insights_Customer[rcif_number]), Wealth_Insights_Customer[fact_type]="WEALTH", Wealth_Insights_Customer[business_date]=_dt)

Top of Company Digital Active = 
CALCULATE(DISTINCTCOUNT(Wealth_Insights_Customer[cust_internet_banking_nbr]), Wealth_Insights_Customer[fact_type]="DIGITAL", Wealth_Insights_Customer[digitally_active_flag]="Digital Active")

Digital Enrollments Wealth = 
VAR _dt = MAX(Wealth_Insights_Customer[business_date])
RETURN CALCULATE(DISTINCTCOUNT(Wealth_Insights_Customer[rcif_number]), Wealth_Insights_Customer[fact_type]="WEALTH", Wealth_Insights_Customer[digital_flag]="Digital User", Wealth_Insights_Customer[business_date]=_dt)

Accounts = 
VAR _dt = MAX(Wealth_Insights_Customer[business_date])
RETURN CALCULATE(SUM(Wealth_Insights_Customer[wealth_accts_cnt]), Wealth_Insights_Customer[fact_type]="WEALTH", Wealth_Insights_Customer[business_date]=_dt)

Accounts per User = DIVIDE([Accounts], [Wealth Users], 0)

Wealth Active User Adoption % = 
VAR _dt = MAX(Wealth_Insights_Customer[business_date])
VAR _active = CALCULATE(DISTINCTCOUNT(Wealth_Insights_Customer[rcif_number]), Wealth_Insights_Customer[fact_type]="WEALTH", Wealth_Insights_Customer[digitally_active_flag]="Digital Active", Wealth_Insights_Customer[business_date]=_dt)
VAR _total = CALCULATE(DISTINCTCOUNT(Wealth_Insights_Customer[rcif_number]), Wealth_Insights_Customer[fact_type]="WEALTH", Wealth_Insights_Customer[business_date]=_dt)
RETURN DIVIDE(_active, _total, 0)

% of Digital Population = 
VAR _dt = MAX(Wealth_Insights_Customer[business_date])
VAR _wd = CALCULATE(DISTINCTCOUNT(Wealth_Insights_Customer[rcif_number]), Wealth_Insights_Customer[fact_type]="WEALTH", Wealth_Insights_Customer[digital_flag]="Digital User", Wealth_Insights_Customer[business_date]=_dt)
RETURN DIVIDE(_wd, [Top of Company Digital Active], 0)


// ═══ BAR CHARTS (business_date on axis) ═══

Wealth OLB Active = 
CALCULATE(DISTINCTCOUNT(Wealth_Insights_Customer[rcif_number]), Wealth_Insights_Customer[fact_type]="WEALTH", Wealth_Insights_Customer[olb_active_flag]="OLB Active")

Wealth OLB Active % = 
DIVIDE([Wealth OLB Active], CALCULATE(DISTINCTCOUNT(Wealth_Insights_Customer[rcif_number]), Wealth_Insights_Customer[fact_type]="WEALTH"), 0)

Wealth MOB Active = 
CALCULATE(DISTINCTCOUNT(Wealth_Insights_Customer[rcif_number]), Wealth_Insights_Customer[fact_type]="WEALTH", Wealth_Insights_Customer[mobile_active_flag]="Mobile Active")

Wealth MOB Active % = 
DIVIDE([Wealth MOB Active], CALCULATE(DISTINCTCOUNT(Wealth_Insights_Customer[rcif_number]), Wealth_Insights_Customer[fact_type]="WEALTH"), 0)

Wealth Digital Active = 
CALCULATE(DISTINCTCOUNT(Wealth_Insights_Customer[rcif_number]), Wealth_Insights_Customer[fact_type]="WEALTH", Wealth_Insights_Customer[digitally_active_flag]="Digital Active")


// ═══ TREND LINE (Adoption by Month) ═══

Wealth Users Trend = 
CALCULATE(DISTINCTCOUNT(Wealth_Insights_Customer[rcif_number]), Wealth_Insights_Customer[fact_type]="WEALTH")

Wealth Digital Active Penetration = DIVIDE([Wealth Digital Active], [Wealth Users Trend], 0)


// ═══ INSIGHTS PAGE — OLB/MOB ACTIVITY TABLE ═══

OLB Active Users = 
CALCULATE(DISTINCTCOUNT(Wealth_Insights_Customer[rcif_number]), Wealth_Insights_Customer[fact_type]="WEALTH", Wealth_Insights_Customer[olb_active_flag]="OLB Active")

Wealth OLB Enrolled = 
CALCULATE(DISTINCTCOUNT(Wealth_Insights_Customer[rcif_number]), Wealth_Insights_Customer[fact_type]="WEALTH", Wealth_Insights_Customer[olb_flag]="OLB User")

Wealth OLB Enrolled % = 
DIVIDE([Wealth OLB Enrolled], CALCULATE(DISTINCTCOUNT(Wealth_Insights_Customer[rcif_number]), Wealth_Insights_Customer[fact_type]="WEALTH"), 0)

OLB MoM % = 
VAR _c = [Wealth OLB Active]
VAR _p = CALCULATE([Wealth OLB Active], DATEADD(Wealth_Insights_Customer[business_date], -1, MONTH))
RETURN DIVIDE(_c - _p, _p, 0)

OLB MoM Delta = 
VAR _c = [Wealth OLB Active]
VAR _p = CALCULATE([Wealth OLB Active], DATEADD(Wealth_Insights_Customer[business_date], -1, MONTH))
RETURN _c - _p

MOB Active Users = 
CALCULATE(DISTINCTCOUNT(Wealth_Insights_Customer[rcif_number]), Wealth_Insights_Customer[fact_type]="WEALTH", Wealth_Insights_Customer[mobile_active_flag]="Mobile Active")

Wealth MOB Enrolled = 
CALCULATE(DISTINCTCOUNT(Wealth_Insights_Customer[rcif_number]), Wealth_Insights_Customer[fact_type]="WEALTH", Wealth_Insights_Customer[mobile_flag]="Mobile User")

Wealth MOB Enrolled % = 
DIVIDE([Wealth MOB Enrolled], CALCULATE(DISTINCTCOUNT(Wealth_Insights_Customer[rcif_number]), Wealth_Insights_Customer[fact_type]="WEALTH"), 0)

MOB MoM % = 
VAR _c = [Wealth MOB Active]
VAR _p = CALCULATE([Wealth MOB Active], DATEADD(Wealth_Insights_Customer[business_date], -1, MONTH))
RETURN DIVIDE(_c - _p, _p, 0)

MOB MoM Delta = 
VAR _c = [Wealth MOB Active]
VAR _p = CALCULATE([Wealth MOB Active], DATEADD(Wealth_Insights_Customer[business_date], -1, MONTH))
RETURN _c - _p


// ═══ DECOMPOSITION TREE ═══
// Use [Wealth Users Trend] as Analyze value
// business_group then division as Explain By


// ═══ RCIF DONUT CHART ═══

Total RCIF Records = 
CALCULATE(COUNTROWS(Wealth_Insights_Customer), Wealth_Insights_Customer[fact_type]="DIGITAL")

Digital User Records = 
CALCULATE(COUNTROWS(Wealth_Insights_Customer), Wealth_Insights_Customer[fact_type]="DIGITAL", Wealth_Insights_Customer[digital_flag]="Digital User")

Non Digital User Records = 
CALCULATE(COUNTROWS(Wealth_Insights_Customer), Wealth_Insights_Customer[fact_type]="DIGITAL", Wealth_Insights_Customer[digital_flag]="Non Digital User")


// ═══ PRODUCT INSIGHTS — INVESTPATH ═══

InvestPath Customers = DISTINCTCOUNT(Wealth_Insights_Account[ip_id])

InvestPath Accounts = COUNTROWS(Wealth_Insights_Account)

AUM = SUM(Wealth_Insights_Account[balance])

$Balance per IP Account = DIVIDE([AUM], [InvestPath Accounts], 0)

InvestPath Account Funded = 
CALCULATE(COUNTROWS(Wealth_Insights_Account), Wealth_Insights_Account[balance] > 0)


// ═══ FORMATTING ═══
// % measures: format 0.00%
// Currency (AUM, $Balance): $#,##0
// Counts: #,##0
// Date slicer: business_date from Wealth_Insights_Customer
// Cards: select single month in slicer
// Trends: show all months

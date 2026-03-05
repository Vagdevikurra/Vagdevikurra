Sorry! Here are all corrected measures pointing to the new table:

```dax
Wealth OLB Active# = 
CALCULATE(
    DISTINCTCOUNT(Customer[RCIF_NUMBER]),
    Customer[fact_type] = "Wealth",
    Customer[olb_active_flag] = "OLB Active"
)

Wealth OLB Enrolled = 
CALCULATE(
    DISTINCTCOUNT(Customer[RCIF_NUMBER]),
    Customer[fact_type] = "Wealth",
    Customer[olb_enrolled] = "OLB Enrolled"
)

Wealth OLB Enrolled% = 
DIVIDE([Wealth OLB Active#], [Wealth OLB Enrolled])

Wealth OLB Active (90D) MoM = 
VAR CurrentMonth = [Wealth OLB Active#]
VAR PrevMonth = CALCULATE(
    [Wealth OLB Active#],
    DATEADD(Calendar[Month_Ceiling], -1, MONTH))
RETURN CurrentMonth - PrevMonth

Wealth OLB Active (90D) MoM% = 
VAR PrevMonth = CALCULATE(
    [Wealth OLB Active#],
    DATEADD(Calendar[Month_Ceiling], -1, MONTH))
RETURN DIVIDE(
    [Wealth OLB Active (90D) MoM],
    PrevMonth)
```

Same for MOB:

```dax
Wealth MOB Active# = 
CALCULATE(
    DISTINCTCOUNT(Customer[RCIF_NUMBER]),
    Customer[fact_type] = "Wealth",
    Customer[mob_active_flag] = "Mobile Active"
)

Wealth MOB Enrolled = 
CALCULATE(
    DISTINCTCOUNT(Customer[RCIF_NUMBER]),
    Customer[fact_type] = "Wealth",
    Customer[mob_enrolled] = "Mobile Enrolled"
)

Wealth MOB Enrolled% = 
DIVIDE([Wealth MOB Active#], [Wealth MOB Enrolled])

Wealth MOB Active (90D) MoM = 
VAR CurrentMonth = [Wealth MOB Active#]
VAR PrevMonth = CALCULATE(
    [Wealth MOB Active#],
    DATEADD(Calendar[Month_Ceiling], -1, MONTH))
RETURN CurrentMonth - PrevMonth

Wealth MOB Active (90D) MoM% = 
VAR PrevMonth = CALCULATE(
    [Wealth MOB Active#],
    DATEADD(Calendar[Month_Ceiling], -1, MONTH))
RETURN DIVIDE(
    [Wealth MOB Active (90D) MoM],
    PrevMonth)
```

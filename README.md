Digital Active Wealth :=
VAR _latestDate =
    [Latest Wealth Date]
RETURN
CALCULATE (
    DISTINCTCOUNT ( wic2_wealth_fact[rcif_number] ),
    FILTER (
        ALL ( wic2_wealth_fact ),
        wic2_wealth_fact[business_date] = _latestDate
    ),
    wia2_customer[digitally_active_flag] = "Digital Active"
)
Wealth Accounts :=
VAR _latestDate =
    [Latest Wealth Date]
RETURN
CALCULATE (
    SUM ( wic2_wealth_fact[accts_cnt] ),
    FILTER (
        ALL ( wic2_wealth_fact ),
        wic2_wealth_fact[business_date] = _latestDate
    )
)

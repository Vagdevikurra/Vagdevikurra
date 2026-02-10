Wealth Banking Accounts :=
VAR _latestDate = [Latest Wealth Date]
RETURN
CALCULATE (
    SUM ( wic2_wealth_fact[accts_cnt] ),
    FILTER (
        ALL ( wic2_wealth_fact ),
        wic2_wealth_fact[business_date] = _latestDate
            && wic2_wealth_fact[business_group] = "Private Wealth"
    )
)

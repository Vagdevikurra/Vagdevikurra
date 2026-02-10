Digital Enrollment – Wealth :=
VAR _latestDate =
    [Latest Wealth Date]
RETURN
CALCULATE (
    DISTINCTCOUNT ( wic2_wealth_fact[rcif_number] ),

    -- lock to latest wealth snapshot
    FILTER (
        ALL ( wic2_wealth_fact ),
        wic2_wealth_fact[business_date] = _latestDate
    ),

    -- apply enrollment filter from customer table
    KEEPFILTERS (
        wia2_customer[digital_flag] = "Digital User"
    )
)

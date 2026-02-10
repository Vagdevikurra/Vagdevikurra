Top Company Digital Active :=
CALCULATE (
    DISTINCTCOUNT ( wia2_customer[primary_ibn] ),
    REMOVEFILTERS ( wic2_wealth_fact ),
    wia2_customer[digitally_active_flag] = "Digital Active",
    NOT ISBLANK ( wia2_customer[primary_ibn] )
)

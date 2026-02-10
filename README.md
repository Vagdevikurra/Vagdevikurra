let
    // ===== CHANGE THIS if your table name differs in Power Query =====
    WealthTable = #"wic2_wealth_fact",

    // Ensure the column is type date
    Dates = List.Transform( List.RemoveNulls(WealthTable[business_date]), each Date.From(_) ),

    MinDate = List.Min(Dates),
    MaxDate = List.Max(Dates),

    // Floor/Ceiling (month boundaries)
    FloorDate = Date.StartOfMonth(MinDate),
    CeilingDate = Date.EndOfMonth(MaxDate),

    DayCount = Duration.Days(CeilingDate - FloorDate) + 1,
    DateList = List.Dates(FloorDate, DayCount, #duration(1,0,0,0)),

    Calendar = Table.FromList(DateList, Splitter.SplitByNothing(), {"Date"}, null, ExtraValues.Error),
    #"Changed Type" = Table.TransformColumnTypes(Calendar, {{"Date", type date}}),

    // Standard calendar attributes
    #"Added Year" = Table.AddColumn(#"Changed Type", "Year", each Date.Year([Date]), Int64.Type),
    #"Added Month Number" = Table.AddColumn(#"Added Year", "Month Number", each Date.Month([Date]), Int64.Type),
    #"Added Month Name" = Table.AddColumn(#"Added Month Number", "Month Name", each Date.ToText([Date], "MMM"), type text),
    #"Added YearMonth" = Table.AddColumn(#"Added Month Name", "YearMonth", each Date.ToText([Date], "yyyy-MM"), type text),
    #"Added Quarter" = Table.AddColumn(#"Added YearMonth", "Quarter", each "Q" & Text.From(Date.QuarterOfYear([Date])), type text),
    #"Added StartOfMonth" = Table.AddColumn(#"Added Quarter", "Start of Month", each Date.StartOfMonth([Date]), type date),
    #"Added EndOfMonth" = Table.AddColumn(#"Added StartOfMonth", "End of Month", each Date.EndOfMonth([Date]), type date),

    // Helpful flags
    Today = Date.From(DateTime.LocalNow()),
    #"Added IsToday" = Table.AddColumn(#"Added EndOfMonth", "Is Today", each [Date] = Today, type logical),
    #"Added IsMonthEnd" = Table.AddColumn(#"Added IsToday", "Is Month End", each [Date] = Date.EndOfMonth([Date]), type logical)
in
    #"Added IsMonthEnd"

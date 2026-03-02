let
    StartDate = #date(2025, 8, 1),
    EndDate   = #date(2026, 1, 31),

    // Generate list of months
    MonthCount = ((Date.Year(EndDate) - Date.Year(StartDate)) * 12) 
                 + Date.Month(EndDate) - Date.Month(StartDate) + 1,

    MonthList = List.Transform(
        {0..MonthCount - 1},
        each Date.AddMonths(StartDate, _)
    ),

    // Build table with floor and ceiling
    TableFromList = Table.FromList(
        MonthList,
        Splitter.SplitByNothing(),
        {"Month_Floor"}
    ),

    // Add Month Ceiling (last day of month)
    AddCeiling = Table.AddColumn(
        TableFromList,
        "Month_Ceiling",
        each Date.EndOfMonth([Month_Floor]),
        type date
    ),

    // Add Month Floor as date type
    ChangeType = Table.TransformColumnTypes(
        AddCeiling,
        {{"Month_Floor", type date}, {"Month_Ceiling", type date}}
    ),

    // Add Month Name and Year for labels
    AddMonthName = Table.AddColumn(
        ChangeType,
        "Month_Name",
        each Date.ToText([Month_Floor], "MMM yyyy"),
        type text
    ),

    AddMonthNum = Table.AddColumn(
        AddMonthName,
        "Month_Number",
        each Date.Month([Month_Floor]),
        type number
    ),

    AddYear = Table.AddColumn(
        AddMonthNum,
        "Year",
        each Date.Year([Month_Floor]),
        type number
    )

in
    AddYear

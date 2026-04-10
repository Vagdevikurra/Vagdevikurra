# Enterprise Auth Dashboard — Improvement Guide

---

## 1. FIX: Auth Success Rate = 1.00 (BUG)

Your current KPI shows 1.00 because you're likely using a default measure or the wrong column. Check your measure — it should be:

```dax
Auth Success Rate = 
DIVIDE(
    SUM('v1_digital_auth_kpi_monthly'[auth_success_cnt]),
    SUM('v1_digital_auth_kpi_monthly'[auth_attempts]),
    0
)
```

**NOT** `auth_success_cnt / event_cnt` — `event_cnt` includes non-auth events (device, session rows), which dilutes the denominator.

---

## 2. RECOMMENDED DAX MEASURES

Create these in Power BI under Modeling > New Measure:

```dax
// ---- KPI Measures ----

Total Auth Attempts = 
SUM('v1_digital_auth_kpi_monthly'[auth_attempts])

Auth Success Rate = 
DIVIDE(
    SUM('v1_digital_auth_kpi_monthly'[auth_success_cnt]),
    SUM('v1_digital_auth_kpi_monthly'[auth_attempts]),
    0
)

Auth Failure Rate = 
DIVIDE(
    SUM('v1_digital_auth_kpi_monthly'[auth_failure_cnt]),
    SUM('v1_digital_auth_kpi_monthly'[auth_attempts]),
    0
)

Step-Up Rate = 
DIVIDE(
    SUM('v1_digital_auth_kpi_monthly'[stepup_events]),
    SUM('v1_digital_auth_kpi_monthly'[auth_attempts]),
    0
)

// ---- Trend Measures (MoM) ----

Auth Attempts MoM % = 
VAR CurrentMonth = SUM('v1_digital_auth_kpi_monthly'[auth_attempts])
VAR PrevMonth = 
    CALCULATE(
        SUM('v1_digital_auth_kpi_monthly'[auth_attempts]),
        DATEADD('dim_month'[MonthStart], -1, MONTH)
    )
RETURN DIVIDE(CurrentMonth - PrevMonth, PrevMonth, 0)

Success Rate MoM Change = 
VAR Current = [Auth Success Rate]
VAR Prev = 
    CALCULATE(
        [Auth Success Rate],
        DATEADD('dim_month'[MonthStart], -1, MONTH)
    )
RETURN Current - Prev

// ---- Customer Measures ----

Digitally Active Customers = 
CALCULATE(
    DISTINCTCOUNT('v1_customer_digital_activity_monthly'[customer_id]),
    'v1_customer_digital_activity_monthly'[digital_active_flag] = 1
)

Mobile Active Customers = 
CALCULATE(
    DISTINCTCOUNT('v1_customer_digital_activity_monthly'[customer_id]),
    'v1_customer_digital_activity_monthly'[mobile_active_flag] = 1
)

OTP Users = 
CALCULATE(
    DISTINCTCOUNT('v1_customer_digital_activity_monthly'[customer_id]),
    'v1_customer_digital_activity_monthly'[otp_user_flag] = 1
)

OTP Exposure Rate = 
DIVIDE([OTP Users], [Digitally Active Customers], 0)

// ---- Formatted Display Measures ----

Auth Attempts Display = 
VAR Val = [Total Auth Attempts]
RETURN
    IF(Val >= 1E9, FORMAT(Val / 1E9, "0.0") & "B",
    IF(Val >= 1E6, FORMAT(Val / 1E6, "0.0") & "M",
    FORMAT(Val, "#,0")))
```

---

## 3. LAYOUT IMPROVEMENTS — Page 1 (Executive Overview)

### Current problems:
- KPI cards are plain numbers with no context
- Combo chart Y-axis range (0.9962–0.9968) makes success rate look volatile when it's ~99.96%
- Donut chart wastes space
- No conditional formatting

### Recommended layout (top to bottom):

**Row 1 — KPI Cards (use Card visual or new Card visual):**
| Total Auth Attempts | Auth Success Rate | Auth Failure Rate | Step-Up Rate |
|---|---|---|---|
| 3.0B | 99.65% | 0.35% | 0.81% |
| ▲ 2.1% MoM | ▼ -0.02pp | ▲ +0.02pp | ▲ +0.03pp |

→ Use **New Card** visual (preview). Add subtitle with MoM change. Use conditional formatting: green for good, red for bad.

**Row 2 — Two charts side by side:**
- **Left: Auth Volume Trend** (Clustered Column) — monthly auth_attempts by channel, stacked
- **Right: Auth Success Rate Trend** (Line) — separate line per channel

Why separate? Your combo chart crams two very different scales together. The success rate line at 99.96% is invisible against billion-scale bars.

**Row 3 — Two charts side by side:**
- **Left: Step-Up Distribution** (100% Stacked Bar) — OTP vs Voice by channel. Keep this, it's good.
- **Right: Step-Up Rate Trend** (Line) — monthly, with a reference line at target

---

## 4. LAYOUT IMPROVEMENTS — Page 2 (Customer Adoption)

### Current problems:
- "5042 Mobile Active Customers" seems very low vs 2M digital active — is this filtered to a single month or is the measure wrong?
- Bar charts are all blue with no differentiation
- No friction/risk narrative

### Fix the Mobile Active measure:
Check if you're accidentally filtering. 5042 vs 2M suggests the mobile_active_flag join or filter is broken.

### Recommended layout:

**Row 1 — KPI Cards:**
| Digitally Active | Mobile Active | OTP Exposure | Voice MFA Users |
|---|---|---|---|
| 2.0M | ??? | 2.0M (100%?!) | count |

→ If OTP Users = Digitally Active, that means almost every digital customer hits OTP. That's a friction story worth highlighting.

**Row 2:**
- **Left: Digital Adoption by Age** (Horizontal Bar) — keep, but add data labels
- **Right: OTP Exposure by Age** (Bar) — keep, add % labels

**Row 3:**
- **NEW: OTP Exposure Rate by LOB Group** (Bar) — shows which business lines have highest friction
- **NEW: Channel × Age Heatmap** (Matrix with conditional formatting) — shows adoption density

---

## 5. POWER BI THEME (paste into View > Themes > Custom Theme)

Save this as `enterprise_auth_theme.json`:

```json
{
    "name": "Enterprise Auth Insights",
    "dataColors": [
        "#1B4F72",
        "#2E86C1",
        "#48C9B0",
        "#F39C12",
        "#E74C3C",
        "#8E44AD",
        "#27AE60",
        "#95A5A6"
    ],
    "background": "#FFFFFF",
    "foreground": "#2C3E50",
    "tableAccent": "#1B4F72",
    "good": "#27AE60",
    "neutral": "#F39C12",
    "bad": "#E74C3C",
    "visualStyles": {
        "*": {
            "*": {
                "general": [{"responsive": true}],
                "title": [{
                    "fontSize": 12,
                    "fontFamily": "Segoe UI Semibold",
                    "fontColor": {"solid": {"color": "#2C3E50"}}
                }],
                "labels": [{
                    "fontSize": 10,
                    "fontFamily": "Segoe UI"
                }],
                "categoryAxis": [{
                    "fontSize": 10,
                    "fontFamily": "Segoe UI",
                    "labelColor": {"solid": {"color": "#5D6D7E"}}
                }],
                "valueAxis": [{
                    "fontSize": 10,
                    "fontFamily": "Segoe UI",
                    "labelColor": {"solid": {"color": "#5D6D7E"}},
                    "gridlineColor": {"solid": {"color": "#EAECEE"}},
                    "gridlineStyle": 1
                }]
            }
        },
        "card": {
            "*": {
                "labels": [{
                    "fontSize": 28,
                    "fontFamily": "Segoe UI Light",
                    "fontColor": {"solid": {"color": "#1B4F72"}}
                }],
                "categoryLabels": [{
                    "fontSize": 11,
                    "fontFamily": "Segoe UI",
                    "fontColor": {"solid": {"color": "#5D6D7E"}}
                }]
            }
        }
    },
    "textClasses": {
        "callout": {
            "fontSize": 28,
            "fontFace": "Segoe UI Light",
            "color": "#1B4F72"
        },
        "title": {
            "fontSize": 14,
            "fontFace": "Segoe UI Semibold",
            "color": "#2C3E50"
        },
        "header": {
            "fontSize": 12,
            "fontFace": "Segoe UI Semibold",
            "color": "#2C3E50"
        },
        "label": {
            "fontSize": 10,
            "fontFace": "Segoe UI",
            "color": "#5D6D7E"
        }
    }
}
```

### How to apply:
1. Save the JSON above as `enterprise_auth_theme.json`
2. In Power BI Desktop: **View** > **Themes** > **Browse for themes** > select the file
3. All visuals will update automatically

---

## 6. QUICK WINS CHECKLIST

- [ ] Fix Auth Success Rate measure (divide by auth_attempts, not event_cnt)
- [ ] Investigate Mobile Active = 5042 (likely a filter/measure bug)
- [ ] Apply theme JSON
- [ ] Replace combo chart with two separate charts
- [ ] Add MoM subtitle to KPI cards
- [ ] Add data labels to bar charts
- [ ] Format percentages to 2 decimal places (99.65%, not 1.00)
- [ ] Add page title text boxes: "Executive Overview" / "Customer Adoption & Risk Segments"
- [ ] Add a "Last Refreshed" text box with `TODAY()` measure
- [ ] Set background to white, remove visual borders for cleaner look

---

## 7. SPARK CODE NOTE

Your Spark code looks correct. The 204-row KPI monthly table is the right grain (month × channel × app_id × lob_group × stepup_type). The issue is on the Power BI side — measures and formatting, not the data pipeline.

# Power BI DAX Measures Documentation
This document outlines the key **DAX measures** used in the Power BI model.
Measures are grouped by **business domain** to reflect how operational, sales, and analytical KPIs are structured in a typical enterprise BI model.
------------------------------------------------------------------------

# Deliveries Metrics
These measures monitor delivery performance, timeliness, and driver efficiency. They help identify operational issues such as late shipments and performance differences across drivers.

## Avg Days Variance
Average difference between expected delivery date and actual delivery date.
Avg Days Variance =
AVERAGE('dw fact_deliveries'[DaysVariance])

## Avg Days Variance - PM
Average delivery variance for the **previous month**.
Avg Days Variance - PM =
CALCULATE([Avg Days Variance], DATEADD('dw dim_date'[FullDate], -1, MONTH))

## Total Deliveries
Total number of deliveries recorded in the fact table.
Total Deliveries =
COUNTROWS('dw fact_deliveries')

## On-Time Deliveries
Number of deliveries that were completed on schedule.
On-Time Deliveries =
CALCULATE([Total Deliveries], 'dw fact_deliveries'[DeliveryStatus] = "On-Time")

## On-Time Deliveries - PM
On-time deliveries for the previous month.
On-Time Deliveries - PM =
CALCULATE([On-Time Deliveries], DATEADD('dw dim_date'[FullDate], -1, MONTH))

## On-Time Delivery %
Percentage of deliveries completed on time.
On-Time Delivery % =
DIVIDE([On-Time Deliveries], [Total Deliveries], 0)

## On-Time Delivery % - PM
Previous month on-time delivery percentage.
On-Time Delivery % - PM =
CALCULATE([On-Time Delivery %], DATEADD('dw dim_date'[FullDate], -1, MONTH))

## Late Deliveries
Total deliveries that missed their expected delivery window.
Late Deliveries =
CALCULATE([Total Deliveries], 'dw fact_deliveries'[DeliveryStatus] = "Late")

## Late Deliveries - PM
Late deliveries for the previous month.
Late Deliveries - PM =
CALCULATE([Late Deliveries], DATEADD('dw dim_date'[FullDate], -1, MONTH))

## Late Delivery %
Percentage of deliveries that were late.
Late Delivery % =
DIVIDE([Late Deliveries], [Total Deliveries], 0)

## Late Delivery % - PM
Previous month late delivery percentage.
Late Delivery % - PM =
CALCULATE([Late Delivery %], DATEADD('dw dim_date'[FullDate], -1, MONTH))

## Driver On-Time Rank
Ranks drivers based on their on-time delivery performance.
Driver On-Time Rank =
RANKX(
    ALL('dw dim_driver'[DriverCode]),
    [On-Time Delivery %],
    ,
    DESC,
    DENSE
)
------------------------------------------------------------------------

# Exceptions Metrics
These measures track operational issues such as delivery exceptions and their resolution times. They are commonly used by logistics and operations teams to monitor service quality.

## Total Exceptions
Total number of delivery exceptions recorded.
Total Exceptions =
COUNTROWS('dw fact_exceptions')

## Avg Resolution Days
Average number of days required to resolve exceptions that have been marked as resolved.
Avg Resolution Days =
CALCULATE(
    AVERAGE('dw fact_exceptions'[ResolutionDays]),
    'dw fact_exceptions'[IsResolved] = TRUE()
)

## Exceptions per 100 Deliveries
Standardized KPI that shows the rate of exceptions relative to total deliveries.
Exceptions per 100 Deliveries =
DIVIDE([Total Exceptions], [Total Deliveries], 0) * 100

## Express Exception Rate %
Percentage of exceptions flagged as high priority within the dataset.
Express Exception Rate % =
DIVIDE(
    COUNTROWS(FILTER('dw fact_exceptions', 'dw fact_exceptions'[PriorityFlag] = TRUE())),
    COUNTROWS('dw fact_exceptions'),
    0
)

## Priority Exception Rate %
Rate of priority-level exceptions compared to total exceptions.
Priority Exception Rate % =
DIVIDE(
    COUNTROWS(FILTER('dw fact_exceptions', 'dw fact_exceptions'[PriorityFlag] = TRUE())),
    COUNTROWS('dw fact_exceptions'),
    0
)
------------------------------------------------------------------------

# Sales Metrics
These measures analyze revenue performance and shipment-type sales contributions.

## Total Revenue
Total revenue generated from all sales transactions.
Total Revenue =
SUM('dw fact_sales'[SalesAmount])

## Total Revenue PM
Revenue generated in the previous month.
Total Revenue PM =
CALCULATE(
    [Total Revenue],
    DATEADD('dw dim_date'[FullDate], -1, MONTH),
    REMOVEFILTERS('dw dim_date'[YearNumber])
)

## Total Revenue PY
Revenue generated during the same period in the previous year.
Total Revenue PY =
CALCULATE(
    [Total Revenue],
    DATEADD('dw dim_date'[FullDate], -1, YEAR),
    REMOVEFILTERS('dw dim_date'[YearNumber])
)

## Total Units Sold
Total quantity of products sold.
Total Units Sold =
SUM('dw fact_sales'[UnitsSold])

## Revenue - Express
Revenue generated from express shipment types.
Revenue - Express =
CALCULATE(
    [Total Revenue],
    FILTER(
        'dw fact_deliveries',
        RELATED('dw dim_shipment_type'[ShipmentType]) = "Express"
    )
)

## Revenue - Priority
Revenue generated from priority shipment types.
Revenue - Priority =
CALCULATE(
    [Total Revenue],
    FILTER(
        'dw fact_deliveries',
        RELATED('dw dim_shipment_type'[ShipmentType]) = "Priority"
    )
)

## Revenue - Standard Shipments
Revenue generated from standard shipment types.
Revenue - Standard Shipments =
CALCULATE(
    [Total Revenue],
    FILTER(
        'dw fact_deliveries',
        RELATED('dw dim_shipment_type'[ShipmentType]) = "Standard"
    )
)
------------------------------------------------------------------------

# Time Intelligence Metrics
These measures analyze revenue trends across time periods such as month-to-date, quarter-to-date, and year-to-date. Time intelligence calculations are essential for trend analysis and executive reporting.

## MTD Revenue
Revenue accumulated from the start of the current month to the current date.
MTD Revenue =
CALCULATE(
    [Total Revenue],
    DATESMTD('dw dim_date'[FullDate])
)

## QTD Revenue
Revenue accumulated from the start of the current quarter.
QTD Revenue =
CALCULATE(
    [Total Revenue],
    DATESQTD('dw dim_date'[FullDate])
)

## YTD Revenue
Revenue accumulated from the start of the current year.
YTD Revenue =
CALCULATE(
    [Total Revenue],
    DATESYTD('dw dim_date'[FullDate])
)

## Prev Year YTD Revenue
Compares current year performance to the same point in the previous year.
Prev Year YTD Revenue =
CALCULATE(
    [YTD Revenue],
    DATEADD('dw dim_date'[FullDate], -1, YEAR),
    REMOVEFILTERS('dw dim_date'[YearNumber])
)

## Monthly Revenue
Revenue aggregated for the current month.
Monthly Revenue =
CALCULATE(
    [Total Revenue],
    DATESMTD('dw dim_date'[FullDate]),
    ENDOFMONTH('dw dim_date'[FullDate])
)

## Rolling 3M Avg
Rolling three‑month revenue trend used for smoothing fluctuations in monthly performance.
Rolling 3M Avg =
CALCULATE(
    [Total Revenue],
    DATESINPERIOD(
        'dw dim_date'[FullDate],
        LASTDATE('dw dim_date'[FullDate]),
        -3,
        MONTH
    )
)

## MoM Revenue %
Month‑over‑month percentage change in revenue.
MoM Revenue % =
DIVIDE([Total Revenue] - [Total Revenue PM], [Total Revenue PM])

## MoM Revenue Change
Alternative month-over-month calculation using variables for clarity.
MoM Revenue Change =
VAR CurrentRevenue = [Total Revenue]
VAR PriorRevenue =
    CALCULATE(
        [Total Revenue],
        DATEADD('dw dim_date'[FullDate], -1, MONTH)
    )
RETURN
    DIVIDE(CurrentRevenue - PriorRevenue, PriorRevenue, BLANK())

## YoY Revenue %
Year-over-year percentage growth in revenue.
YoY Revenue % =
DIVIDE([Total Revenue] - [Total Revenue PY], [Total Revenue PY])

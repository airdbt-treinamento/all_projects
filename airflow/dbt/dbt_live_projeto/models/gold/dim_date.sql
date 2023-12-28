-- Step 1: Define the date range
WITH date_range AS (
  SELECT
    DATE '2023-07-01' AS start_date,
    CURRENT_DATE() AS end_date
),

-- Step 2: Generate a list of dates between the start and end dates
calendar_dates AS (
  SELECT 
    calendar_date,
    end_date
  FROM 
    date_range
  CROSS JOIN
    UNNEST(GENERATE_DATE_ARRAY(start_date, end_date)) AS calendar_date 
),

-- Step 3: Extract the formatted date components
formatted_calendar AS (
  SELECT 
    calendar_date AS date,
    EXTRACT(YEAR FROM calendar_date) AS year,
    CONCAT(EXTRACT(YEAR FROM calendar_date), ' ', CONCAT('Q', EXTRACT(QUARTER FROM calendar_date))) AS year_quarter,
    CONCAT('Q', EXTRACT(QUARTER FROM calendar_date)) AS quarter,
    DATE_TRUNC(calendar_date, QUARTER) AS quarter_start_date,
    LAST_DAY(calendar_date, QUARTER) AS quarter_end_date,
    FORMAT_DATE('%B', calendar_date) AS month,
    FORMAT_DATE('%m', calendar_date) AS month_number,
    FORMAT_DATE("%b-%y", calendar_date) AS month_year,
    DATE_TRUNC(calendar_date, MONTH) AS month_start_date,
    LAST_DAY(calendar_date, MONTH) AS month_end_date,
    -- ISO 8601 week number, running from Sunday to Saturday
    FORMAT_DATE('%V', calendar_date + 1) AS week_number,
    CONCAT(FORMAT_DATE('%V', calendar_date + 1), '-', RIGHT(CAST(EXTRACT(ISOYEAR FROM calendar_date + 1) AS STRING), 2)) AS week_number_year, 
    DATE_TRUNC(calendar_date, WEEK(SUNDAY)) AS week_start_date, 
    DATE_TRUNC(calendar_date + 6, WEEK(SATURDAY)) AS week_end_date,
    FORMAT_DATE('%u', calendar_date + 1) AS week_day_number,
    FORMAT_DATE('%A', calendar_date) AS week_day,
    -- Report & dashboard filters for use in Data Viz tools
    CASE 
      WHEN calendar_date = end_date THEN 'Today'
      WHEN calendar_date = end_date - 1 THEN 'Yesterday'
      ELSE CAST(calendar_date AS STRING)
    END AS date_filter,
    CASE 
      WHEN calendar_date >= DATE_TRUNC(end_date, WEEK(SUNDAY)) THEN 'Current Week'
      WHEN calendar_date >= DATE_TRUNC(end_date, WEEK(SUNDAY)) - 7 AND calendar_date < DATE_TRUNC(end_date, WEEK(SUNDAY)) THEN 'Previous Week'
      WHEN calendar_date >= DATE_TRUNC(end_date, WEEK(SUNDAY)) - 14 AND calendar_date < DATE_TRUNC(end_date, WEEK(SUNDAY)) - 7 THEN 'Previous Week - 2'
      ELSE CAST(CONCAT(FORMAT_DATE('%V', calendar_date + 1), '-', RIGHT(CAST(EXTRACT(ISOYEAR FROM calendar_date + 1) AS STRING), 2)) AS STRING)
    END AS week_filter,
    CASE 
      WHEN calendar_date >= DATE_TRUNC(end_date, MONTH) THEN 'Current Month'
      WHEN calendar_date >= DATE_SUB(DATE_TRUNC(end_date, MONTH), INTERVAL 1 MONTH) AND calendar_date < DATE_TRUNC(end_date, MONTH) THEN 'Previous Month'
      ELSE FORMAT_DATE("%b-%y", calendar_date)
    END AS month_filter,
    CASE 
      WHEN FORMAT_DATE('%A', calendar_date) IN ('Sunday', 'Saturday') THEN TRUE 
      ELSE FALSE
    END AS weekend_filter,
    -- For us retailers, Black Friday / Cyber Monday (BFCM) sales generally end on the last Monday in Nov, running for two weeks prior and starting on a Sunday
    CASE 
      WHEN FORMAT_DATE('%m', calendar_date) = '11' AND calendar_date >= DATE_TRUNC(LAST_DAY(DATE(calendar_date), MONTH), WEEK(MONDAY)) - 15 
       AND FORMAT_DATE('%m', calendar_date) = '11' AND calendar_date <= DATE_TRUNC(LAST_DAY(DATE(calendar_date), MONTH), WEEK(MONDAY)) THEN TRUE
      ELSE FALSE
    END AS bfcm_filter,
    CASE 
      WHEN calendar_date >= end_date - 28 AND calendar_date < end_date THEN TRUE 
      ELSE FALSE 
    END AS previous_28_days,
    CASE 
      WHEN calendar_date >= DATE_SUB(DATE_TRUNC(end_date, WEEK(SUNDAY)), INTERVAL 4 WEEK) AND calendar_date < DATE_TRUNC(end_date, WEEK(SUNDAY)) THEN TRUE 
      ELSE FALSE 
    END AS previous_4_weeks,
    CASE 
      WHEN calendar_date >= DATE_SUB(DATE_TRUNC(end_date, WEEK(SUNDAY)), INTERVAL 12 WEEK) AND calendar_date < DATE_TRUNC(end_date, WEEK(SUNDAY)) THEN TRUE 
      ELSE FALSE 
    END AS previous_12_weeks, 
    CASE 
      WHEN calendar_date >= DATE_SUB(DATE_TRUNC(end_date, MONTH), INTERVAL 12 MONTH) AND calendar_date < DATE_TRUNC(end_date, MONTH) THEN TRUE 
      ELSE FALSE 
    END AS previous_12_months,
    CASE 
      WHEN calendar_date >= DATE_SUB(DATE_TRUNC(end_date, MONTH), INTERVAL 12 MONTH) THEN TRUE 
      ELSE FALSE 
    END AS previous_13_months
  FROM 
    calendar_dates
)

-- Step 4: Create the calendar table
SELECT 
  *
FROM 
  formatted_calendar
ORDER BY date DESC
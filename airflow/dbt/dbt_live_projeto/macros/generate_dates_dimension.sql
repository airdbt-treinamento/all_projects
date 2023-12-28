{% macro generate_dates_dimension(start_date) %}

/*
 - Assuming start of fiscal year as at 1st of July.
 - Automatically calculate the date that is 10,000 days after the start date.
 - Filter the end date as 12 month after today.
 */

WITH dates AS (
  SELECT * FROM
    UNNEST(GENERATE_date_ARRAY( '{{ start_date }}', current_date, INTERVAL 1 day)) AS v_date
),
dates_fin AS ( -- add fiscal dates
SELECT
  v_date AS Carlendar_Date,
  EXTRACT(DAYOFWEEK FROM v_date) as Day_Of_Week,
  FORMAT_DATE("%A", v_date)  as Day_Of_Week_Name,
  EXTRACT(WEEK(MONDAY) FROM v_date) AS Cal_Week_Start_Date, --Monday Start
  EXTRACT(DAY FROM v_date) AS Day_Of_Month,
  EXTRACT(MONTH FROM v_date) AS Cal_Month,
  FORMAT_DATE("%B", v_date)  as Cal_Mon_Name,
  FORMAT_DATE("%b", v_date)  as Cal_Mon_Name_Short,
  FORMAT_DATE("%Q", v_date) AS Cal_Quarter,
  CONCAT('Q',FORMAT_DATE("%Q", v_date)) AS Cal_Quarter_Name,
  FORMAT_DATE("%G", v_date) AS Cal_Year,
  CASE EXTRACT(DAYOFWEEK FROM v_date)
    WHEN 6 THEN TRUE
    WHEN 7 THEN TRUE
    ELSE FALSE
  END AS Is_Weekend,
  CASE
    WHEN EXTRACT(MONTH FROM v_date) < 7 THEN EXTRACT(YEAR FROM v_date)
    ELSE EXTRACT(YEAR FROM v_date) + 1
    END AS Fin_Year,
  CASE
    WHEN EXTRACT(MONTH FROM v_date) < 7 THEN EXTRACT(MONTH FROM v_date) + 6
    ELSE EXTRACT(MONTH FROM v_date) - 6
    END AS Fin_Period,
  CASE
    WHEN EXTRACT(MONTH FROM v_date) < 7 THEN EXTRACT(quarter FROM v_date) + 2
    ELSE EXTRACT(quarter FROM v_date) - 2
    END AS Fin_Quarter,
  CASE
    WHEN  v_date < (DATE_TRUNC(v_date, YEAR)) + INTERVAL 6 MONTH
    THEN cast(EXTRACT(WEEK FROM ( v_date - INTERVAL 6 MONTH )) as integer)
    ELSE cast(EXTRACT(WEEK FROM ( v_date + INTERVAL 6 MONTH )) as integer)
    END AS Fin_Week

FROM
  dates
WHERE
  v_date <= DATE_ADD(CURRENT_DATE(), INTERVAL 12 MONTH) --AS two_days_later; 
)
SELECT *,
CONCAT('p',Fin_Period) AS Fin_Period_Name,
CONCAT('FQ',Fin_Quarter) AS Fin_Quarter_Name,
CONCAT('wk',Fin_Week) AS Fin_Week_Name
FROM dates_fin


{% endmacro %}
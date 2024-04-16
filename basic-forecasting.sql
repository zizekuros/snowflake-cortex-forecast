--
-- BASIC ML (NO FEATURES)
--

-- 1: create a view that represents historical data for requests
create or replace view requests as
select 
    DATE(time)::TIMESTAMP_NTZ as day,
    count(*) as request_cnt
from events 
where event_type='request' and day > '2023-11-28 00:00:00.000' and day < '2024-04-09 00:00:00.000'
group by day
order by day asc;

-- 2: [optional] check the created view
select *
from requests;

-- 3: train the model on historical data
create or replace SNOWFLAKE.ML.FORECAST requests_model(
  INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'requests'),
  TIMESTAMP_COLNAME => 'day',
  TARGET_COLNAME => 'request_cnt'
);

-- 4: call the model to predict future values
--call requests_model!FORECAST(FORECASTING_PERIODS => 31,CONFIG_OBJECT => {'prediction_interval': 0.5});
call requests_model!FORECAST(FORECASTING_PERIODS => 90);

-- 5: save the results of the forecast output as a table
create or replace table requests_model_forecast as 
select * from table(result_scan(last_query_id()));

-- 6: [optional] check what's in the forecast table
select * from requests_model_forecast;

-- 7: combine historical and predicted values in a single query
select day, request_cnt, null as forecast 
from requests
union all
select ts as day, null as request_cnt, forecast 
from requests_model_forecast;

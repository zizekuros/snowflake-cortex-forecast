--
-- ADVANCED ML (WITH FEATURES)
--

-- 1: create a view that represents historical data for requests with 2 features (saturday_flg and sunday_flg)
create or replace view requests_with_features as
select 
    DATE(time)::TIMESTAMP_NTZ as day,
    count(*) as request_cnt,
    case 
        when DAYOFWEEK(time)=6 then 1 else 0 
    end as saturday_flg,
    case 
        when DAYOFWEEK(time)=0 then 1 else 0 
    end as sunday_flg
from events 
where event_type='request' and day > '2023-11-01 00:00:00.000' and day < '2024-03-01 00:00:00.000'
group by day, saturday_flg, sunday_flg
order by day asc;


-- 2: [optional] check the created view
select *
from requests_with_features;

-- 3: train the model on historical data
create or replace SNOWFLAKE.ML.FORECAST requests_with_features_model(
  INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'requests_with_features'),
  TIMESTAMP_COLNAME => 'day',
  TARGET_COLNAME => 'request_cnt'
);

-- 4: create a view with future features for the forecasting period
create or replace view future_features as 
select 
    dateadd(
        'day', 
        row_number() over (partition by null order by null), 
        '2024-02-29'
    ) as day,
    case 
        when DAYOFWEEK(day)=6 then 1 else 0 
    end as saturday_flg,
    case 
        when DAYOFWEEK(day)=0 then 1 else 0 
    end as sunday_flg
from table (generator(rowcount => 28));

-- 5: [optional] check forecasting period and its features
select * from future_features

-- 6: call the model to predict future values
call requests_with_features_model!FORECAST(
  INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'future_features'),
  TIMESTAMP_COLNAME =>'day'
);

-- 7: save the results of the forecast output as a table
create or replace table requests_with_features_model_forecast as 
select * from table(result_scan(last_query_id()));

-- 8: [optional] check what's in the forecast table
select * from requests_with_features_model_forecast;

-- 9: combine historical and predicted values in a single query
select day, request_cnt, null as forecast 
from requests_with_features
union all
select ts as day, null as request_cnt, forecast 
from requests_with_features_model_forecast

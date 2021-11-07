-- Subquery- counts events and attribute them to respective Standard cohorts
-- Using window functions to lag coherts and date, to attribute them as previous/next month value

with aggregates_table as (
  select
    MonthDate
    , USER_ID
    , LAG(this_month_status) over(partition by USER_ID order by MonthDate asc) as Last_month_status
    , this_month_status
    , ROW_NUMBER() over(partition by USER_ID order by MonthDate) as user_order
    , LAG(MonthDate) over(partition by USER_ID order by MonthDate) as previous_date
    , LEAD(MonthDate) over(partition by USER_ID order by MonthDate) as Next_date
    , LAG(event_count) over(partition by USER_ID order by MonthDate) as last_month_event_count
    , event_count
  from
    (
      select
        date_trunc('MONTH', COLLECTOR_TSTAMP) as MonthDate
        , USER_ID
        , case
          when count(EVENT) between '0'
          and '3'
            then 'infrequent'
          when count(EVENT) between '4'
          and '7'
            then 'frequent'
          when count(EVENT) > '7'
            then 'power_user'
        end as this_month_status
        , count(EVENT) as event_count
      from
        "CHALLENGE"."PUBLIC" . "EVENTS"
      group by
        1
        , 2
    )
    as temp
)

-- main_cleaned is subset of the aggregates_table above, with little bit cleaned version and this will be used further in all queries. 
-- Cleaning- Using coalese to compare non null values between dates and also find difference between dates.

, main_cleaned as (
  select
    MonthDate
    , USER_ID
    , Last_month_status
    , this_month_status
    , LAG(Last_month_status) over(partition by USER_ID order by MonthDate asc) as Last_month_status_lag
    , user_order
    , coalesce(previous_date, MonthDate) as previous_date
    , coalesce(Next_date, MonthDate) as Next_date
    , datediff(day, previous_date, MonthDate) as diff_days
    , last_month_event_count
    , event_count
  from
    aggregates_table
  order by
    USER_ID
    , MonthDate desc
)

select * from main_cleaned
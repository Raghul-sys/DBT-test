-- using lag of last_month_status(to get users who had their first event in a previous month) to find the users who have reacquired in the current month and filtering days diffrence to one month. 
-- This will output reacquired transitions with respective cohorts.

with reacquired as (
  select
    MonthDate
    , 'reacquired' as Last_month_status
    , this_month_status
    , count(user_id) as transitions
  from
    ( select * from {{ ref('aggregates') }} ) as t
  where
    Last_month_status_lag is null and diff_days < 35 and diff_days is not null
  group by
    1
    , 2
    , 3
)

select * from reacquired
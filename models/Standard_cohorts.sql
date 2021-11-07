-- Grouping user counts based on the last month and current month status from main_cleaned, For users who have last month status as null, they are categorized as New user. 
-- Excluded transitions that stayed within the same group
-- This will output transitions containing infrequent, frequent, power_user, new users

with Standard_cohorts as (
  select
    *
  from
    (
      select
        MonthDate
        , case
          when Last_month_status is null
            then 'new'
          else Last_month_status
        end as Last_month_status
        , this_month_status
        , count(USER_ID) as transitions
      from
         ( select * from {{ ref('aggregates') }} ) as t
      group by
        1
        , 2
        , 3
      order by
        MonthDate desc
    )
    as temp
  where
    Last_month_status <> this_month_status
)

select * from Standard_cohorts
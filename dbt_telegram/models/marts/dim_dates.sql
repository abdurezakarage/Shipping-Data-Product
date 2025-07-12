{{
  config(
    materialized='table'
  )
}}

with date_spine as (
    select 
        date_series::date as date_day
    from (
        select generate_series(
            (select min(message_date_day) from {{ ref('stg_telegram_messages') }}),
            (select max(message_date_day) from {{ ref('stg_telegram_messages') }}),
            interval '1 day'
        ) as date_series
    ) dates
),

final as (
    select
        -- Primary key
        date_day as date_key,
        
        -- Date components
        extract(year from date_day) as year,
        extract(month from date_day) as month,
        extract(day from date_day) as day,
        extract(dow from date_day) as day_of_week,
        extract(doy from date_day) as day_of_year,
        
        -- Week components
        date_trunc('week', date_day) as week_start_date,
        date_trunc('week', date_day) + interval '6 days' as week_end_date,
        extract(week from date_day) as week_of_year,
        
        -- Month components
        date_trunc('month', date_day) as month_start_date,
        (date_trunc('month', date_day) + interval '1 month - 1 day')::date as month_end_date,
        
        -- Quarter components
        extract(quarter from date_day) as quarter,
        date_trunc('quarter', date_day) as quarter_start_date,
        
        -- Year components
        date_trunc('year', date_day) as year_start_date,
        (date_trunc('year', date_day) + interval '1 year - 1 day')::date as year_end_date,
        
        -- Fiscal year (assuming fiscal year starts in July)
        case 
            when extract(month from date_day) >= 7 
            then extract(year from date_day) + 1 
            else extract(year from date_day) 
        end as fiscal_year,
        
        -- Day names
        to_char(date_day, 'Day') as day_name,
        to_char(date_day, 'Mon') as month_name,
        to_char(date_day, 'Month') as month_name_full,
        
        -- Flags
        case when extract(dow from date_day) in (0, 6) then true else false end as is_weekend,
        case when extract(dow from date_day) between 1 and 5 then true else false end as is_weekday,
        
        -- Ethiopian calendar (approximate - would need proper conversion)
        extract(year from date_day) - 7 as ethiopian_year
        
    from date_spine
)

select * from final 
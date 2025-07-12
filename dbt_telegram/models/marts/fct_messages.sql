{{
  config(
    materialized='table'
  )
}}

with messages as (
    select * from {{ ref('stg_telegram_messages') }}
),

channels as (
    select * from {{ ref('dim_channels') }}
),

dates as (
    select * from {{ ref('dim_dates') }}
),

final as (
    select
        -- Primary key
        message_id as message_key,
        
        -- Foreign keys
        c.channel_key,
        d.date_key,
        
        -- Message content metrics
        message_length,
        has_text,
        has_media,
        has_image,
        has_document,
        
        -- Engagement metrics
        views,
        forwards,
        replies,
        
        -- Engagement ratios
        case 
            when views > 0 then round((forwards::float / views) * 100, 2)
            else 0 
        end as forward_rate_percent,
        
        case 
            when views > 0 then round((replies::float / views) * 100, 2)
            else 0 
        end as reply_rate_percent,
        
        -- Content type flags
        case 
            when has_media = true and has_image = true then 'image'
            when has_media = true and has_document = true then 'document'
            when has_text = true then 'text_only'
            else 'other'
        end as content_type,
        
        -- Time dimensions
        message_hour,
        message_day_of_week,
        message_month,
        message_year,
        
        -- Timestamps
        message_date,
        edit_date,
        loaded_at,
        
        -- Channel information
        c.channel_username,
        c.channel_category,
        
        -- Date information
        d.day_name,
        d.month_name,
        d.is_weekend,
        d.is_weekday
        
    from messages m
    left join channels c 
        on m.channel_username = c.channel_username
    left join dates d 
        on m.message_date_day = d.date_key
)

select * from final 
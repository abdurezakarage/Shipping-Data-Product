{{
  config(
    materialized='view'
  )
}}

with source as (
    select * from {{ source('raw', 'raw_telegram_messages') }}
),

cleaned as (
    select
        -- Primary key
        id as message_id,
        
        -- Channel information
        channel_username,
        channel_title,
        
        -- Message content
        message_text,
        case 
            when message_text is null or trim(message_text) = '' then false 
            else true 
        end as has_text,
        length(coalesce(message_text, '')) as message_length,
        
        -- Timestamps
        message_date,
        edit_date,
        loaded_at,
        
        -- Media information
        has_media,
        media_type,
        media_path,
        
        -- Engagement metrics
        coalesce(views, 0) as views,
        coalesce(forwards, 0) as forwards,
        coalesce(replies, 0) as replies,
        
        -- Derived fields
        case 
            when has_media = true and media_type = 'photo' then true 
            else false 
        end as has_image,
        
        case 
            when has_media = true and media_type = 'document' then true 
            else false 
        end as has_document,
        
        -- Date dimensions
        date_trunc('day', message_date) as message_date_day,
        date_trunc('week', message_date) as message_date_week,
        date_trunc('month', message_date) as message_date_month,
        
        -- Time dimensions
        extract(hour from message_date) as message_hour,
        extract(dow from message_date) as message_day_of_week,
        extract(month from message_date) as message_month,
        extract(year from message_date) as message_year,
        
        -- Raw data for debugging
        raw_data
        
    from source
    where message_date is not null  -- Filter out messages without valid dates
)

select * from cleaned 
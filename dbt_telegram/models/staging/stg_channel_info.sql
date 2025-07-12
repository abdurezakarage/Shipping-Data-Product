{{
  config(
    materialized='view'
  )
}}

with source as (
    select * from {{ source('raw', 'raw_channel_info') }}
),

cleaned as (
    select
        -- Primary key
        id as channel_info_id,
        
        -- Channel information
        channel_username,
        channel_title,
        
        -- Scraping information
        scraped_at,
        message_count,
        loaded_at,
        
        -- Derived fields
        case 
            when channel_username like '@%' then substring(channel_username from 2)
            else channel_username 
        end as channel_username_clean,
        
        -- Channel type classification (based on username patterns)
        case 
            when channel_username ilike '%pharma%' or channel_username ilike '%med%' then 'pharmaceutical'
            when channel_username ilike '%cosmetic%' then 'cosmetics'
            when channel_username ilike '%health%' then 'healthcare'
            else 'general'
        end as channel_category,
        
        -- Raw data for debugging
        raw_data
        
    from source
    where channel_username is not null  -- Filter out records without channel username
)

select * from cleaned 
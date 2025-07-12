{{
  config(
    materialized='table'
  )
}}

with channel_data as (
    select distinct
        channel_username,
        channel_title,
        channel_username_clean,
        channel_category,
        first_value(scraped_at) over (
            partition by channel_username 
            order by scraped_at desc
        ) as last_scraped_at,
        sum(message_count) over (partition by channel_username) as total_messages_scraped,
        count(*) over (partition by channel_username) as scraping_sessions
    from {{ ref('stg_channel_info') }}
),

final as (
    select
        -- Primary key
        row_number() over (order by channel_username) as channel_key,
        
        -- Channel information
        channel_username,
        channel_title,
        channel_username_clean,
        channel_category,
        
        -- Scraping metrics
        last_scraped_at,
        total_messages_scraped,
        scraping_sessions,
        
        -- Metadata
        current_timestamp as created_at,
        current_timestamp as updated_at
        
    from channel_data
)

select * from final 
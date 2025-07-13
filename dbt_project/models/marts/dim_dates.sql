{{
  config(
    materialized='table',
    tags=['marts', 'dimensions']
  )
}}

WITH date_spine AS (
    SELECT DISTINCT message_date as date_key
    FROM {{ ref('stg_telegram_messages') }}
    WHERE message_date IS NOT NULL
),

date_attributes AS (
    SELECT
        date_key,
        EXTRACT(YEAR FROM date_key) as year,
        EXTRACT(MONTH FROM date_key) as month,
        EXTRACT(DAY FROM date_key) as day,
        EXTRACT(DOW FROM date_key) as day_of_week,
        EXTRACT(DOY FROM date_key) as day_of_year,
        EXTRACT(WEEK FROM date_key) as week_of_year,
        EXTRACT(QUARTER FROM date_key) as quarter,
        
        -- Month names
        TO_CHAR(date_key, 'Month') as month_name,
        TO_CHAR(date_key, 'Mon') as month_name_short,
        
        -- Day names
        TO_CHAR(date_key, 'Day') as day_name,
        TO_CHAR(date_key, 'Dy') as day_name_short,
        
        -- Quarter names
        CONCAT('Q', EXTRACT(QUARTER FROM date_key)) as quarter_name,
        
        -- Year-Month
        TO_CHAR(date_key, 'YYYY-MM') as year_month,
        
        -- Fiscal year (assuming fiscal year starts in April)
        CASE 
            WHEN EXTRACT(MONTH FROM date_key) >= 4 
            THEN EXTRACT(YEAR FROM date_key) + 1
            ELSE EXTRACT(YEAR FROM date_key)
        END as fiscal_year,
        
        -- Is weekend
        CASE 
            WHEN EXTRACT(DOW FROM date_key) IN (0, 6) THEN TRUE 
            ELSE FALSE 
        END as is_weekend,
        
        -- Is month end
        CASE 
            WHEN date_key = DATE_TRUNC('month', date_key) + INTERVAL '1 month - 1 day' 
            THEN TRUE 
            ELSE FALSE 
        END as is_month_end,
        
        -- Is quarter end
        CASE 
            WHEN date_key = DATE_TRUNC('quarter', date_key) + INTERVAL '3 months - 1 day' 
            THEN TRUE 
            ELSE FALSE 
        END as is_quarter_end,
        
        -- Is year end
        CASE 
            WHEN EXTRACT(MONTH FROM date_key) = 12 AND EXTRACT(DAY FROM date_key) = 31 
            THEN TRUE 
            ELSE FALSE 
        END as is_year_end
        
    FROM date_spine
),

message_stats AS (
    SELECT
        message_date as date_key,
        COUNT(*) as total_messages,
        COUNT(DISTINCT channel_id) as active_channels,
        COUNT(DISTINCT sender_id) as active_senders,
        SUM(views_count) as total_views,
        SUM(forwards_count) as total_forwards,
        SUM(replies_count) as total_replies,
        AVG(message_length) as avg_message_length,
        AVG(word_count) as avg_word_count,
        SUM(CASE WHEN has_media THEN 1 ELSE 0 END) as media_messages,
        SUM(CASE WHEN has_text THEN 1 ELSE 0 END) as text_messages
    FROM {{ ref('stg_telegram_messages') }}
    WHERE message_date IS NOT NULL
    GROUP BY message_date
)

SELECT
    da.date_key,
    da.year,
    da.month,
    da.day,
    da.day_of_week,
    da.day_of_year,
    da.week_of_year,
    da.quarter,
    da.month_name,
    da.month_name_short,
    da.day_name,
    da.day_name_short,
    da.quarter_name,
    da.year_month,
    da.fiscal_year,
    da.is_weekend,
    da.is_month_end,
    da.is_quarter_end,
    da.is_year_end,
    COALESCE(ms.total_messages, 0) as total_messages,
    COALESCE(ms.active_channels, 0) as active_channels,
    COALESCE(ms.active_senders, 0) as active_senders,
    COALESCE(ms.total_views, 0) as total_views,
    COALESCE(ms.total_forwards, 0) as total_forwards,
    COALESCE(ms.total_replies, 0) as total_replies,
    COALESCE(ms.avg_message_length, 0) as avg_message_length,
    COALESCE(ms.avg_word_count, 0) as avg_word_count,
    COALESCE(ms.media_messages, 0) as media_messages,
    COALESCE(ms.text_messages, 0) as text_messages,
    CURRENT_TIMESTAMP as dbt_updated_at
FROM date_attributes da
LEFT JOIN message_stats ms ON da.date_key = ms.date_key 
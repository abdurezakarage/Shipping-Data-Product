{{
  config(
    materialized='table',
    tags=['marts', 'dimensions']
  )
}}

WITH channel_data AS (
    SELECT DISTINCT
        channel_username,
        channel_title,
        channel_category,
        COUNT(*) as total_messages,
        MIN(message_timestamp) as first_message_date,
        MAX(message_timestamp) as last_message_date,
        AVG(message_length) as avg_message_length,
        AVG(word_count) as avg_word_count,
        SUM(CASE WHEN has_media THEN 1 ELSE 0 END) as media_messages_count,
        SUM(CASE WHEN has_text THEN 1 ELSE 0 END) as text_messages_count
    FROM {{ ref('stg_telegram_messages') }}
    WHERE channel_username IS NOT NULL
    GROUP BY channel_username, channel_title, channel_category
),

final AS (
    SELECT
        channel_username as channel_key,
        channel_title,
        channel_category,
        total_messages,
        first_message_date,
        last_message_date,
        avg_message_length,
        avg_word_count,
        media_messages_count,
        text_messages_count,
        CASE 
            WHEN total_messages > 0 
            THEN ROUND((media_messages_count::float / total_messages) * 100, 2)
            ELSE 0 
        END as media_percentage,
        CASE 
            WHEN total_messages > 0 
            THEN ROUND((text_messages_count::float / total_messages) * 100, 2)
            ELSE 0 
        END as text_percentage,
        CASE 
            WHEN total_messages > 0 
            THEN ROUND(avg_message_length, 2)
            ELSE 0 
        END as avg_message_length_rounded,
        CASE 
            WHEN total_messages > 0 
            THEN ROUND(avg_word_count, 2)
            ELSE 0 
        END as avg_word_count_rounded,
        CURRENT_TIMESTAMP as dbt_updated_at
    FROM channel_data
)

SELECT * FROM final 
{{
  config(
    materialized='table',
    tags=['marts', 'dimensions']
  )
}}

WITH channel_data AS (
    SELECT DISTINCT
        channel_name,
        channel_id,
        COUNT(*) as total_messages,
        MIN(message_timestamp) as first_message_date,
        MAX(message_timestamp) as last_message_date,
        AVG(message_length) as avg_message_length,
        AVG(word_count) as avg_word_count,
        SUM(CASE WHEN has_media THEN 1 ELSE 0 END) as media_messages_count
    FROM {{ ref('stg_telegram_messages') }}
    WHERE channel_name IS NOT NULL
    GROUP BY channel_name, channel_id
),

final AS (
    SELECT
        channel_name as channel_key,
        channel_id,
        total_messages,
        first_message_date,
        last_message_date,
        avg_message_length,
        avg_word_count,
        media_messages_count,
        CASE 
            WHEN total_messages > 0 
            THEN ROUND((media_messages_count::float / total_messages)::numeric, 2)
            ELSE 0 
        END as media_percentage,
        CASE 
            WHEN total_messages > 0 
            THEN ROUND(avg_message_length::numeric, 2)
            ELSE 0 
        END as avg_message_length_rounded,
        CASE 
            WHEN total_messages > 0 
            THEN ROUND(avg_word_count::numeric, 2)
            ELSE 0 
        END as avg_word_count_rounded,
        CURRENT_TIMESTAMP as dbt_updated_at
    FROM channel_data
)

SELECT * FROM final 
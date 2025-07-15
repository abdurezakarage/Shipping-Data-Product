{{ config(materialized='table') }}

with detections as (
    select
        d.message_id,
        d.image_path,
        d.detected_object_class,
        d.confidence_score
    from {{ source('raw', 'image_detections') }} d
),

messages as (
    select
        message_key,
        message_id,
        channel_username,
        message_date
    from {{ ref('fct_messages') }}
)

select
    m.message_key,
    d.image_path,
    d.detected_object_class,
    d.confidence_score,
    m.channel_username,
    m.message_date
from detections d
left join messages m
    on d.message_id = m.message_id
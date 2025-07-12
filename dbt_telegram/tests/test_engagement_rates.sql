-- Custom test to ensure engagement rates are within reasonable bounds
-- This test should return 0 rows to pass

select 
    message_key,
    forward_rate_percent,
    reply_rate_percent
from {{ ref('fct_messages') }}
where 
    -- Forward rate should not exceed 100%
    forward_rate_percent > 100
    or 
    -- Reply rate should not exceed 100%
    reply_rate_percent > 100
    or
    -- Rates should not be negative
    forward_rate_percent < 0
    or
    reply_rate_percent < 0 
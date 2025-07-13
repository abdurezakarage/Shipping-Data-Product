-- Custom test: Message engagement consistency
-- This test ensures that engagement metrics are consistent and logical

SELECT 
    message_key,
    views_count,
    forwards_count,
    replies_count,
    total_engagement,
    forward_rate_percent,
    reply_rate_percent
FROM {{ ref('fct_messages') }}
WHERE 
    -- Test 1: Total engagement should equal sum of individual metrics
    total_engagement != (views_count + forwards_count + replies_count)
    
    -- Test 2: Forward rate should be reasonable (not more than 100% of views)
    OR (views_count > 0 AND forwards_count > views_count)
    
    -- Test 3: Reply rate should be reasonable (not more than 100% of views)
    OR (views_count > 0 AND replies_count > views_count)
    
    -- Test 4: Negative engagement metrics are invalid
    OR views_count < 0 
    OR forwards_count < 0 
    OR replies_count < 0
    
    -- Test 5: Forward rate percentage should be reasonable
    OR (views_count > 0 AND forward_rate_percent > 100)
    
    -- Test 6: Reply rate percentage should be reasonable
    OR (views_count > 0 AND reply_rate_percent > 100) 
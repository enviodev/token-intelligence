-- ==========================================
-- UNICHAIN ERC20 ANALYTICS QUERY COLLECTION
-- ==========================================
-- 47.7M+ transfers across the entire Unichain ecosystem
-- Optimized for ClickHouse with contract-first indexing

-- ==========================================
-- 1. ECOSYSTEM OVERVIEW & STATISTICS
-- ==========================================

-- Total ecosystem stats
SELECT 
    COUNT() as total_transfers,
    COUNT(DISTINCT contract_address) as unique_tokens,
    COUNT(DISTINCT from_address) as unique_senders,
    COUNT(DISTINCT to_address) as unique_receivers,
    MIN(block_timestamp) as earliest_transfer,
    MAX(block_timestamp) as latest_transfer
FROM token_intelligence.erc20_transfers;

-- Top 20 most active tokens by transfer count
SELECT 
    contract_address,
    COUNT() as transfer_count,
    COUNT(DISTINCT from_address) as unique_senders,
    COUNT(DISTINCT to_address) as unique_receivers,
    SUM(value) as total_volume
FROM token_intelligence.erc20_transfers 
GROUP BY contract_address 
ORDER BY transfer_count DESC 
LIMIT 20;

-- Ecosystem growth by month
SELECT 
    toYYYYMM(block_timestamp) as month,
    COUNT() as monthly_transfers,
    COUNT(DISTINCT contract_address) as active_tokens,
    COUNT(DISTINCT from_address) as active_senders,
    COUNT(DISTINCT to_address) as active_receivers
FROM token_intelligence.erc20_transfers 
GROUP BY month 
ORDER BY month DESC;

-- Daily activity for last 30 days
SELECT 
    toDate(block_timestamp) as date,
    COUNT() as daily_transfers,
    COUNT(DISTINCT contract_address) as active_tokens,
    COUNT(DISTINCT from_address) as unique_users
FROM token_intelligence.erc20_transfers 
WHERE block_timestamp >= now() - INTERVAL 30 DAY
GROUP BY date 
ORDER BY date DESC;

-- ==========================================
-- 2. TOKEN-SPECIFIC ANALYSIS
-- ==========================================

-- Detailed analysis for a specific token (replace with actual contract address)
SELECT 
    COUNT() as transfers,
    COUNT(DISTINCT from_address) as holders_who_sent,
    COUNT(DISTINCT to_address) as addresses_that_received,
    SUM(value) as total_volume,
    AVG(value) as average_transfer,
    MIN(value) as smallest_transfer,
    MAX(value) as largest_transfer,
    MIN(block_timestamp) as first_transfer,
    MAX(block_timestamp) as latest_transfer
FROM token_intelligence.erc20_transfers 
WHERE contract_address = '0x4200000000000000000000000000000000000006'; -- WETH example

-- Top holders by total received for a token
SELECT 
    to_address,
    SUM(value) as total_received,
    COUNT() as transfer_count
FROM token_intelligence.erc20_transfers 
WHERE contract_address = '0x4200000000000000000000000000000000000006'
GROUP BY to_address 
ORDER BY total_received DESC 
LIMIT 50;

-- Top senders by total sent for a token
SELECT 
    from_address,
    SUM(value) as total_sent,
    COUNT() as transfer_count
FROM token_intelligence.erc20_transfers 
WHERE contract_address = '0x4200000000000000000000000000000000000006'
GROUP BY from_address 
ORDER BY total_sent DESC 
LIMIT 50;

-- Token transfer size distribution
SELECT 
    contract_address,
    COUNT() as transfers,
    quantile(0.5)(value) as median_transfer,
    quantile(0.9)(value) as p90_transfer,
    quantile(0.99)(value) as p99_transfer
FROM token_intelligence.erc20_transfers 
GROUP BY contract_address 
HAVING transfers > 1000
ORDER BY transfers DESC 
LIMIT 20;

-- ==========================================
-- 3. USER BEHAVIOR ANALYSIS
-- ==========================================

-- Most active users by sending transactions (FIXED VERSION)
WITH user_stats AS (
    SELECT 
        from_address as user,
        COUNT() as sent_txns,
        COUNT(DISTINCT contract_address) as sent_tokens,
        SUM(value) as total_sent_volume
    FROM token_intelligence.erc20_transfers 
    GROUP BY from_address
    HAVING sent_txns > 10
)
SELECT 
    user,
    sent_txns,
    sent_tokens,
    total_sent_volume
FROM user_stats 
ORDER BY sent_txns DESC 
LIMIT 100;

-- Most active users by receiving transactions
WITH receiver_stats AS (
    SELECT 
        to_address as user,
        COUNT() as received_txns,
        COUNT(DISTINCT contract_address) as received_tokens,
        SUM(value) as total_received_volume
    FROM token_intelligence.erc20_transfers 
    GROUP BY to_address
    HAVING received_txns > 10
)
SELECT 
    user,
    received_txns,
    received_tokens,
    total_received_volume
FROM receiver_stats 
ORDER BY received_txns DESC 
LIMIT 100;

-- Users who interact with the most different tokens
SELECT 
    user_address,
    unique_tokens,
    total_transactions
FROM (
    SELECT 
        from_address as user_address,
        COUNT(DISTINCT contract_address) as unique_tokens,
        COUNT() as total_transactions
    FROM token_intelligence.erc20_transfers 
    GROUP BY from_address
    UNION ALL
    SELECT 
        to_address as user_address,
        COUNT(DISTINCT contract_address) as unique_tokens,
        COUNT() as total_transactions
    FROM token_intelligence.erc20_transfers 
    GROUP BY to_address
)
GROUP BY user_address
ORDER BY unique_tokens DESC, total_transactions DESC 
LIMIT 50;

-- Whale transactions (top 0.1% by value) - TESTED WORKING
SELECT 
    contract_address,
    from_address,
    to_address,
    value,
    block_timestamp,
    block_number
FROM token_intelligence.erc20_transfers 
WHERE value > (
    SELECT quantile(0.999)(value) 
    FROM token_intelligence.erc20_transfers
)
ORDER BY value DESC 
LIMIT 100;

-- ==========================================
-- 4. TIME-BASED ANALYSIS
-- ==========================================

-- Hourly activity patterns (what hours are most active?) - TESTED WORKING
SELECT 
    toHour(block_timestamp) as hour,
    COUNT() as transfers,
    COUNT(DISTINCT from_address) as unique_users,
    AVG(value) as avg_transfer_value
FROM token_intelligence.erc20_transfers 
GROUP BY hour 
ORDER BY hour;

-- Hourly activity for recent 7 days only
SELECT 
    toHour(block_timestamp) as hour,
    COUNT() as transfers,
    COUNT(DISTINCT from_address) as unique_users,
    AVG(value) as avg_transfer_value
FROM token_intelligence.erc20_transfers 
WHERE block_timestamp >= now() - INTERVAL 7 DAY
GROUP BY hour 
ORDER BY hour;

-- Day of week patterns
SELECT 
    toDayOfWeek(block_timestamp) as day_of_week,
    CASE toDayOfWeek(block_timestamp)
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday' 
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
        WHEN 7 THEN 'Sunday'
    END as day_name,
    COUNT() as transfers,
    AVG(value) as avg_value
FROM token_intelligence.erc20_transfers 
GROUP BY day_of_week, day_name
ORDER BY day_of_week;

-- Recent 7 days activity by token
SELECT 
    contract_address,
    COUNT() as recent_transfers,
    COUNT(DISTINCT from_address) as recent_senders,
    SUM(value) as recent_volume
FROM token_intelligence.erc20_transfers 
WHERE block_timestamp >= now() - INTERVAL 7 DAY
GROUP BY contract_address 
ORDER BY recent_transfers DESC 
LIMIT 20;

-- ==========================================
-- 5. VOLUME & VALUE ANALYSIS
-- ==========================================

-- Tokens by total economic value transferred
SELECT 
    contract_address,
    COUNT() as transfer_count,
    SUM(value) as total_volume,
    AVG(value) as average_transfer,
    MAX(value) as largest_transfer
FROM token_intelligence.erc20_transfers 
GROUP BY contract_address 
ORDER BY total_volume DESC 
LIMIT 25;

-- Value flow analysis: biggest single transfers
SELECT 
    contract_address,
    from_address,
    to_address,
    value,
    block_timestamp,
    toDate(block_timestamp) as date
FROM token_intelligence.erc20_transfers 
ORDER BY value DESC 
LIMIT 50;

-- Average transfer value by token (excluding zero transfers)
SELECT 
    contract_address,
    COUNT() as non_zero_transfers,
    SUM(value) as total_volume,
    AVG(value) as avg_transfer,
    median(value) as median_transfer
FROM token_intelligence.erc20_transfers 
WHERE value > 0
GROUP BY contract_address 
HAVING non_zero_transfers >= 100
ORDER BY avg_transfer DESC 
LIMIT 30;

-- ==========================================
-- 6. NETWORK EFFECTS & RELATIONSHIPS
-- ==========================================

-- Most connected addresses (highest degree centrality)
SELECT 
    address,
    unique_counterparties,
    total_interactions
FROM (
    SELECT 
        from_address as address,
        COUNT(DISTINCT to_address) as unique_counterparties,
        COUNT() as total_interactions
    FROM token_intelligence.erc20_transfers 
    GROUP BY from_address
    UNION ALL
    SELECT 
        to_address as address,
        COUNT(DISTINCT from_address) as unique_counterparties,
        COUNT() as total_interactions
    FROM token_intelligence.erc20_transfers 
    GROUP BY to_address
)
GROUP BY address
ORDER BY unique_counterparties DESC, total_interactions DESC 
LIMIT 50;

-- Token co-usage: users who use multiple tokens
SELECT 
    t1.contract_address as token1,
    t2.contract_address as token2,
    COUNT() as shared_users
FROM (
    SELECT DISTINCT from_address, contract_address 
    FROM token_intelligence.erc20_transfers
) t1
JOIN (
    SELECT DISTINCT from_address, contract_address 
    FROM token_intelligence.erc20_transfers
) t2 ON t1.from_address = t2.from_address AND t1.contract_address < t2.contract_address
GROUP BY token1, token2
ORDER BY shared_users DESC 
LIMIT 50;

-- Minting activity (transfers from zero address)
SELECT 
    contract_address,
    COUNT() as mint_events,
    SUM(value) as total_minted,
    COUNT(DISTINCT to_address) as unique_recipients
FROM token_intelligence.erc20_transfers 
WHERE from_address = '0x0000000000000000000000000000000000000000'
GROUP BY contract_address 
ORDER BY total_minted DESC 
LIMIT 30;

-- ==========================================
-- 7. PERFORMANCE & EFFICIENCY QUERIES
-- ==========================================

-- Fastest growing tokens (by recent activity)
SELECT 
    contract_address,
    recent_transfers,
    historical_transfers,
    CASE WHEN historical_transfers > 0 
         THEN (recent_transfers::Float64 / historical_transfers) * 100 
         ELSE 0 END as growth_rate_percent
FROM (
    SELECT 
        contract_address,
        SUM(CASE WHEN block_timestamp >= now() - INTERVAL 30 DAY THEN 1 ELSE 0 END) as recent_transfers,
        SUM(CASE WHEN block_timestamp < now() - INTERVAL 30 DAY THEN 1 ELSE 0 END) as historical_transfers
    FROM token_intelligence.erc20_transfers 
    GROUP BY contract_address 
    HAVING recent_transfers > 100 AND historical_transfers > 100
)
ORDER BY growth_rate_percent DESC 
LIMIT 20;

-- Transaction frequency per token (average time between transfers)
SELECT 
    contract_address,
    COUNT() as total_transfers,
    (MAX(block_timestamp) - MIN(block_timestamp)) / COUNT() as avg_seconds_between_transfers,
    COUNT() / ((MAX(block_timestamp) - MIN(block_timestamp)) / 3600) as transfers_per_hour
FROM token_intelligence.erc20_transfers 
GROUP BY contract_address 
HAVING total_transfers >= 1000
ORDER BY transfers_per_hour DESC 
LIMIT 25;

-- ==========================================
-- 8. ADVANCED ANALYTICS
-- ==========================================

-- Token concentration analysis (how concentrated is token distribution?)
SELECT 
    contract_address,
    total_transfers,
    top_10_percent_volume,
    (top_10_percent_volume / total_volume) * 100 as concentration_ratio
FROM (
    SELECT 
        contract_address,
        COUNT() as total_transfers,
        SUM(value) as total_volume,
        SUM(CASE WHEN rank <= total_transfers * 0.1 THEN value ELSE 0 END) as top_10_percent_volume
    FROM (
        SELECT 
            contract_address,
            value,
            row_number() OVER (PARTITION BY contract_address ORDER BY value DESC) as rank,
            COUNT() OVER (PARTITION BY contract_address) as total_transfers
        FROM token_intelligence.erc20_transfers 
    )
    GROUP BY contract_address
    HAVING total_transfers >= 1000
)
ORDER BY concentration_ratio DESC 
LIMIT 20;

-- Identify potential airdrops (1-to-many distribution patterns)
SELECT 
    contract_address,
    from_address as potential_airdrop_source,
    COUNT(DISTINCT to_address) as unique_recipients,
    COUNT() as total_distributions,
    SUM(value) as total_distributed,
    MIN(block_timestamp) as first_distribution,
    MAX(block_timestamp) as last_distribution
FROM token_intelligence.erc20_transfers 
GROUP BY contract_address, from_address
HAVING unique_recipients >= 100 
   AND total_distributions >= 100
   AND (last_distribution - first_distribution) <= INTERVAL 7 DAY
ORDER BY unique_recipients DESC 
LIMIT 50;

-- Token velocity (how fast tokens move through the network)
SELECT 
    contract_address,
    COUNT() as transfers,
    COUNT(DISTINCT from_address) + COUNT(DISTINCT to_address) as unique_addresses,
    COUNT()::Float64 / (COUNT(DISTINCT from_address) + COUNT(DISTINCT to_address)) as velocity_ratio
FROM token_intelligence.erc20_transfers 
GROUP BY contract_address 
HAVING transfers >= 1000
ORDER BY velocity_ratio DESC 
LIMIT 30;

-- ==========================================
-- 9. BALANCE CALCULATIONS - TESTED WORKING
-- ==========================================

-- Calculate all token balances for a specific user (USER PORTFOLIO) - TESTED WORKING
-- Replace the address with any user you want to analyze
SELECT 
    contract_address,
    SUM(received) - SUM(sent) as balance
FROM (
    SELECT 
        contract_address,
        value as received,
        0 as sent
    FROM token_intelligence.erc20_transfers 
    WHERE to_address = '0x65081cb48d74a32e9ccfed75164b8c09972dbcf1'
    
    UNION ALL
    
    SELECT 
        contract_address,
        0 as received,
        value as sent
    FROM token_intelligence.erc20_transfers 
    WHERE from_address = '0x65081cb48d74a32e9ccfed75164b8c09972dbcf1'
)
GROUP BY contract_address
HAVING balance > 0
ORDER BY balance DESC;

-- Calculate all holder balances for a specific token (TOKEN HOLDERS) - TESTED WORKING
-- Replace the contract address with any token you want to analyze
SELECT 
    address as holder_address,
    SUM(received) - SUM(sent) as balance
FROM (
    SELECT 
        to_address as address,
        value as received,
        0 as sent
    FROM token_intelligence.erc20_transfers 
    WHERE contract_address = '0x4200000000000000000000000000000000000006' -- WETH example
      AND to_address != '0x0000000000000000000000000000000000000000'
    
    UNION ALL
    
    SELECT 
        from_address as address,
        0 as received,
        value as sent
    FROM token_intelligence.erc20_transfers 
    WHERE contract_address = '0x4200000000000000000000000000000000000006' -- WETH example
      AND from_address != '0x0000000000000000000000000000000000000000'
)
GROUP BY address
HAVING balance > 0
ORDER BY balance DESC 
LIMIT 100;

-- Simple query: Just total received amounts (without subtracting sent)
SELECT 
    to_address as holder_address,
    SUM(value) as total_received
FROM token_intelligence.erc20_transfers 
WHERE contract_address = '0x4200000000000000000000000000000000000006' -- WETH example
  AND to_address != '0x0000000000000000000000000000000000000000'
GROUP BY to_address
ORDER BY total_received DESC 
LIMIT 100;

-- Top holders summary with statistics for a token
SELECT 
    COUNT(DISTINCT holder_address) as total_holders,
    SUM(balance) as total_supply,
    AVG(balance) as avg_balance,
    median(balance) as median_balance,
    quantile(0.9)(balance) as p90_balance,
    MAX(balance) as largest_balance
FROM (
    SELECT 
        to_address as holder_address,
        SUM(value) as received,
        COALESCE((SELECT SUM(value) FROM token_intelligence.erc20_transfers s WHERE s.from_address = t.to_address AND s.contract_address = t.contract_address), 0) as sent,
        received - sent as balance
    FROM token_intelligence.erc20_transfers t
    WHERE contract_address = '0x4200000000000000000000000000000000000006'
      AND to_address != '0x0000000000000000000000000000000000000000'
    GROUP BY to_address
    HAVING balance > 0
);

-- User's transaction history for a specific token (detailed breakdown)
SELECT 
    block_timestamp,
    block_number,
    CASE 
        WHEN from_address = '0x65081cb48d74a32e9ccfed75164b8c09972dbcf1' THEN 'SENT'
        WHEN to_address = '0x65081cb48d74a32e9ccfed75164b8c09972dbcf1' THEN 'RECEIVED'
    END as direction,
    CASE 
        WHEN from_address = '0x65081cb48d74a32e9ccfed75164b8c09972dbcf1' THEN to_address
        WHEN to_address = '0x65081cb48d74a32e9ccfed75164b8c09972dbcf1' THEN from_address
    END as counterparty,
    value,
    SUM(CASE 
        WHEN to_address = '0x65081cb48d74a32e9ccfed75164b8c09972dbcf1' THEN value
        WHEN from_address = '0x65081cb48d74a32e9ccfed75164b8c09972dbcf1' THEN -value
        ELSE 0
    END) OVER (ORDER BY block_timestamp, block_number) as running_balance
FROM token_intelligence.erc20_transfers 
WHERE contract_address = '0x4200000000000000000000000000000000000006' -- WETH example
  AND (from_address = '0x65081cb48d74a32e9ccfed75164b8c09972dbcf1' OR to_address = '0x65081cb48d74a32e9ccfed75164b8c09972dbcf1')
ORDER BY block_timestamp, block_number;

-- ==========================================
-- 10. CUSTOM EXPLORATION TEMPLATES
-- ==========================================

-- Template: User Portfolio Balance (Easy to customize)
-- Replace 'YOUR_ADDRESS_HERE' with any wallet address
/*
SELECT 
    contract_address,
    SUM(received) - SUM(sent) as balance
FROM (
    SELECT contract_address, value as received, 0 as sent
    FROM token_intelligence.erc20_transfers 
    WHERE to_address = 'YOUR_ADDRESS_HERE'
    UNION ALL
    SELECT contract_address, 0 as received, value as sent
    FROM token_intelligence.erc20_transfers 
    WHERE from_address = 'YOUR_ADDRESS_HERE'
)
GROUP BY contract_address
HAVING balance > 0
ORDER BY balance DESC;
*/

-- Template: Token Holder Balances (Easy to customize)
-- Replace 'YOUR_CONTRACT_HERE' with any token contract address
/*
SELECT 
    address as holder_address,
    SUM(received) - SUM(sent) as balance
FROM (
    SELECT to_address as address, value as received, 0 as sent
    FROM token_intelligence.erc20_transfers 
    WHERE contract_address = 'YOUR_CONTRACT_HERE'
      AND to_address != '0x0000000000000000000000000000000000000000'
    UNION ALL
    SELECT from_address as address, 0 as received, value as sent
    FROM token_intelligence.erc20_transfers 
    WHERE contract_address = 'YOUR_CONTRACT_HERE'
      AND from_address != '0x0000000000000000000000000000000000000000'
)
GROUP BY address
HAVING balance > 0
ORDER BY balance DESC 
LIMIT 100;
*/

-- Template: Time-window analysis
-- Analyze activity in specific time period
/*
SELECT 
    contract_address,
    COUNT() as transfers,
    COUNT(DISTINCT from_address) as senders,
    SUM(value) as volume
FROM token_intelligence.erc20_transfers 
WHERE block_timestamp BETWEEN '2025-01-01 00:00:00' AND '2025-01-31 23:59:59'
GROUP BY contract_address 
ORDER BY transfers DESC;
*/

-- ==========================================
-- USAGE NOTES:
-- ==========================================
-- 1. Replace contract addresses in examples with actual ones from your data
-- 2. Adjust time windows as needed for your analysis
-- 3. Use LIMIT clauses to avoid overwhelming results
-- 4. Add WHERE clauses to filter by specific conditions
-- 5. Combine queries to create custom analytics
-- 6. Use these as building blocks for Metabase dashboards
-- ========================================== 
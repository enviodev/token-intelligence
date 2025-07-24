-- ==========================================
-- TOKEN INTELLIGENCE DASHBOARD ANALYTICS
-- ==========================================
-- 
-- Comprehensive ERC20 analytics across multiple blockchain networks
-- Optimized for ClickHouse + Metabase dashboard creation
--
-- 🎯 TABLE STRUCTURE:
-- - Transfer tables: erc20_transfers_{chainId} 
-- - Metadata tables: token_metadata_{chainId}
-- - ORDER BY (contract_address, block_number, log_index) → Fast + chronological
-- - PARTITION BY toDate(block_timestamp) → Daily partitions across all chains
--
-- 🌐 SUPPORTED CHAINS:
-- - 1 (Ethereum), 8453 (Base), 130 (Unichain), 137 (Polygon)
-- - 42161 (Arbitrum), 10 (Optimism), 56 (BSC), 43114 (Avalanche)
-- - 81457 (Blast), 7777777 (Zora), 480 (World), 1868 (Lightlink)

-- ==========================================
-- 📊 ECOSYSTEM OVERVIEW
-- ==========================================

-- Chain ecosystem summary
SELECT 
    COUNT() as total_transfers,
    COUNT(DISTINCT contract_address) as unique_tokens,
    COUNT(DISTINCT from_address) as unique_senders,
    COUNT(DISTINCT to_address) as unique_receivers,
    SUM(value) as total_volume,
    MIN(block_timestamp) as earliest_activity,
    MAX(block_timestamp) as latest_activity
FROM token_intelligence.erc20_transfers_130
LIMIT 1;

-- Top 20 most active tokens
SELECT 
    t.contract_address,
    m.name,
    m.symbol,
    COUNT() as transfer_count,
    COUNT(DISTINCT t.from_address) as unique_senders,
    COUNT(DISTINCT t.to_address) as unique_receivers,
    SUM(t.value) as total_volume
FROM token_intelligence.erc20_transfers_130 t
LEFT JOIN token_intelligence.token_metadata_130 m ON t.contract_address = m.contract_address
GROUP BY t.contract_address, m.name, m.symbol
ORDER BY transfer_count DESC
LIMIT 20;

-- Daily activity trend (last 30 days)
SELECT 
    toDate(block_timestamp) as date,
    COUNT() as daily_transfers,
    COUNT(DISTINCT contract_address) as active_tokens,
    COUNT(DISTINCT from_address) as unique_users,
    SUM(value) as daily_volume
FROM token_intelligence.erc20_transfers_130
WHERE block_timestamp >= now() - INTERVAL 30 DAY
GROUP BY date
ORDER BY date DESC
LIMIT 30;

-- Hourly activity pattern
SELECT 
    toHour(block_timestamp) as hour_of_day,
    COUNT() as transfer_count,
    COUNT(DISTINCT from_address) as unique_users,
    AVG(value) as avg_transfer_value
FROM token_intelligence.erc20_transfers_130
WHERE block_timestamp >= now() - INTERVAL 7 DAY
GROUP BY hour_of_day
ORDER BY hour_of_day;

-- ==========================================
-- 🚀 REAL-TIME ACTIVITY 
-- ==========================================

-- Recent activity (today only - very fast)
SELECT 
    contract_address,
    COUNT() as today_transfers,
    COUNT(DISTINCT from_address) as unique_senders,
    SUM(value) as today_volume
FROM token_intelligence.erc20_transfers_130
WHERE toDate(block_timestamp) = today()
GROUP BY contract_address
ORDER BY today_transfers DESC
LIMIT 15;

-- Last 7 days trending tokens
SELECT 
    t.contract_address,
    m.name,
    m.symbol,
    COUNT() as recent_transfers,
    COUNT(DISTINCT t.from_address) as recent_users,
    SUM(t.value) as recent_volume
FROM token_intelligence.erc20_transfers_130 t
LEFT JOIN token_intelligence.token_metadata_130 m ON t.contract_address = m.contract_address
WHERE t.block_timestamp >= now() - INTERVAL 7 DAY
GROUP BY t.contract_address, m.name, m.symbol
ORDER BY recent_transfers DESC
LIMIT 20;

-- ==========================================
-- 💰 TOKEN ANALYSIS
-- ==========================================

-- Token volume leaders
SELECT 
    t.contract_address,
    m.name,
    m.symbol,
    m.decimals,
    COUNT() as transfer_count,
    SUM(t.value) as total_volume,
    AVG(t.value) as avg_transfer_size,
    MAX(t.value) as largest_transfer
FROM token_intelligence.erc20_transfers_130 t
LEFT JOIN token_intelligence.token_metadata_130 m ON t.contract_address = m.contract_address
GROUP BY t.contract_address, m.name, m.symbol, m.decimals
ORDER BY total_volume DESC
LIMIT 20;

-- Transfer size distribution by token
SELECT 
    t.contract_address,
    m.symbol,
    COUNT() as transfers,
    quantile(0.5)(t.value) as median_transfer,
    quantile(0.9)(t.value) as p90_transfer,
    quantile(0.99)(t.value) as p99_transfer
FROM token_intelligence.erc20_transfers_130 t
LEFT JOIN token_intelligence.token_metadata_130 m ON t.contract_address = m.contract_address
GROUP BY t.contract_address, m.symbol
HAVING transfers > 1000
ORDER BY transfers DESC
LIMIT 15;

-- Whale transactions (top 0.1% by value)
SELECT 
    t.contract_address,
    m.symbol,
    t.from_address,
    t.to_address,
    t.value,
    t.block_timestamp
FROM token_intelligence.erc20_transfers_130 t
LEFT JOIN token_intelligence.token_metadata_130 m ON t.contract_address = m.contract_address
WHERE t.value > (
    SELECT quantile(0.999)(value) 
    FROM token_intelligence.erc20_transfers_130
)
ORDER BY t.value DESC
LIMIT 50;

-- ==========================================
-- 👥 USER BEHAVIOR ANALYTICS
-- ==========================================

-- Most active senders
SELECT 
    from_address,
    COUNT() as transactions_sent,
    COUNT(DISTINCT contract_address) as tokens_used,
    SUM(value) as total_sent_volume
FROM token_intelligence.erc20_transfers_130
GROUP BY from_address
HAVING transactions_sent > 10
ORDER BY transactions_sent DESC
LIMIT 50;

-- Most active receivers
SELECT 
    to_address,
    COUNT() as transactions_received,
    COUNT(DISTINCT contract_address) as tokens_received,
    SUM(value) as total_received_volume
FROM token_intelligence.erc20_transfers_130
GROUP BY to_address
HAVING transactions_received > 10
ORDER BY transactions_received DESC
LIMIT 50;

-- Multi-token users (diversified portfolios)  
WITH user_activity AS (
    SELECT 
        from_address as address,
        COUNT(DISTINCT contract_address) as token_count,
        COUNT() as total_interactions
    FROM token_intelligence.erc20_transfers_130
    GROUP BY from_address
    UNION ALL
    SELECT 
        to_address as address,
        COUNT(DISTINCT contract_address) as token_count,
        COUNT() as total_interactions
    FROM token_intelligence.erc20_transfers_130
    GROUP BY to_address
)
SELECT 
    address,
    MAX(token_count) as max_token_count,
    SUM(total_interactions) as total_interactions
FROM user_activity
GROUP BY address
HAVING MAX(token_count) >= 5
ORDER BY max_token_count DESC, total_interactions DESC
LIMIT 30;

-- ==========================================
-- 🏆 TOKEN HOLDERS & BALANCES
-- ==========================================

-- Token holder distribution (example: WETH)
SELECT 
    address as holder_address,
    balance,
    toFloat64(balance) * 100.0 / toFloat64(SUM(balance) OVER ()) as percentage_of_supply
FROM (
    SELECT 
        address,
        SUM(received) - SUM(sent) as balance
    FROM (
        SELECT to_address as address, value as received, 0 as sent
        FROM token_intelligence.erc20_transfers_130
        WHERE contract_address = '0x4200000000000000000000000000000000000006'
          AND to_address != '0x0000000000000000000000000000000000000000'
        UNION ALL
        SELECT from_address as address, 0 as received, value as sent
        FROM token_intelligence.erc20_transfers_130
        WHERE contract_address = '0x4200000000000000000000000000000000000006'
          AND from_address != '0x0000000000000000000000000000000000000000'
    )
    GROUP BY address
    HAVING balance > 0
)
ORDER BY balance DESC
LIMIT 100;

-- User portfolio (example address)
SELECT 
    t.contract_address,
    m.name,
    m.symbol,
    SUM(received) - SUM(sent) as balance
FROM (
    SELECT contract_address, value as received, 0 as sent
    FROM token_intelligence.erc20_transfers_130
    WHERE to_address = '0x65081cb48d74a32e9ccfed75164b8c09972dbcf1'
    UNION ALL
    SELECT contract_address, 0 as received, value as sent
    FROM token_intelligence.erc20_transfers_130
    WHERE from_address = '0x65081cb48d74a32e9ccfed75164b8c09972dbcf1'
) t
LEFT JOIN token_intelligence.token_metadata_130 m ON t.contract_address = m.contract_address
GROUP BY t.contract_address, m.name, m.symbol
HAVING balance > 0
ORDER BY balance DESC
LIMIT 50;

-- ==========================================
-- 📈 GROWTH & TRENDS
-- ==========================================

-- Monthly ecosystem growth
SELECT 
    toYYYYMM(block_timestamp) as month,
    COUNT() as monthly_transfers,
    COUNT(DISTINCT contract_address) as active_tokens,
    COUNT(DISTINCT from_address) as active_users,
    SUM(value) as monthly_volume
FROM token_intelligence.erc20_transfers_130
GROUP BY month
ORDER BY month DESC
LIMIT 12;

-- Day of week patterns
SELECT 
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
    COUNT(DISTINCT from_address) as unique_users,
    AVG(value) as avg_transfer_value
FROM token_intelligence.erc20_transfers_130
WHERE block_timestamp >= now() - INTERVAL 30 DAY
GROUP BY toDayOfWeek(block_timestamp), day_name
ORDER BY toDayOfWeek(block_timestamp);

-- Fastest growing tokens (30-day growth rate)
SELECT 
    t.contract_address,
    m.symbol,
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
    FROM token_intelligence.erc20_transfers_130
    GROUP BY contract_address
    HAVING recent_transfers > 100 AND historical_transfers > 100
) t
LEFT JOIN token_intelligence.token_metadata_130 m ON t.contract_address = m.contract_address
ORDER BY growth_rate_percent DESC
LIMIT 20;

-- ==========================================
-- 🌐 CROSS-CHAIN COMPARISON
-- ==========================================

-- Current chain ecosystem overview (Unichain chain 130)
SELECT 
    'Unichain' as chain_name,
    130 as chain_id,
    COUNT() as total_transfers,
    COUNT(DISTINCT contract_address) as unique_tokens,
    COUNT(DISTINCT from_address) as unique_senders,
    COUNT(DISTINCT to_address) as unique_receivers,
    MIN(block_timestamp) as earliest_activity,
    MAX(block_timestamp) as latest_activity
FROM token_intelligence.erc20_transfers_130;

-- USDC-like tokens on Unichain
SELECT 
    'Unichain' as chain_name,
    contract_address,
    name,
    symbol,
    decimals,
    COUNT() OVER() as total_usdc_variants
FROM token_intelligence.token_metadata_130
WHERE symbol ILIKE '%USDC%' OR symbol ILIKE '%USD%' OR name ILIKE '%USD%'
ORDER BY symbol;

-- ==========================================
-- 🔬 ADVANCED ANALYTICS
-- ==========================================

-- Token concentration (wealth distribution)
SELECT 
    t.contract_address,
    m.symbol,
    total_holders,
    top_10_holders_balance,
    toFloat64(top_10_holders_balance) * 100.0 / toFloat64(total_supply) as concentration_percent
FROM (
    SELECT 
        contract_address,
        COUNT() as total_holders,
        SUM(balance) as total_supply,
        SUM(CASE WHEN holder_rank <= 10 THEN balance ELSE 0 END) as top_10_holders_balance
    FROM (
        SELECT 
            contract_address,
            address,
            balance,
            row_number() OVER (PARTITION BY contract_address ORDER BY balance DESC) as holder_rank
        FROM (
            SELECT 
                contract_address,
                address,
                SUM(received) - SUM(sent) as balance
            FROM (
                SELECT contract_address, to_address as address, value as received, 0 as sent
                FROM token_intelligence.erc20_transfers_130
                WHERE to_address != '0x0000000000000000000000000000000000000000'
                UNION ALL
                SELECT contract_address, from_address as address, 0 as received, value as sent
                FROM token_intelligence.erc20_transfers_130
                WHERE from_address != '0x0000000000000000000000000000000000000000'
            )
            GROUP BY contract_address, address
            HAVING balance > 0
        )
    )
    GROUP BY contract_address
    HAVING total_holders >= 50
) t
LEFT JOIN token_intelligence.token_metadata_130 m ON t.contract_address = m.contract_address
ORDER BY concentration_percent DESC
LIMIT 20;

-- Potential airdrops (1-to-many distributions)
SELECT 
    t.contract_address,
    m.symbol,
    t.from_address as distributor,
    COUNT(DISTINCT t.to_address) as unique_recipients,
    COUNT() as total_distributions,
    SUM(t.value) as total_distributed,
    MIN(t.block_timestamp) as first_distribution,
    MAX(t.block_timestamp) as last_distribution
FROM token_intelligence.erc20_transfers_130 t
LEFT JOIN token_intelligence.token_metadata_130 m ON t.contract_address = m.contract_address
GROUP BY t.contract_address, m.symbol, t.from_address
HAVING unique_recipients >= 50 
   AND total_distributions >= 50
   AND (last_distribution - first_distribution) <= INTERVAL 7 DAY
ORDER BY unique_recipients DESC
LIMIT 25;

-- Minting activity (from zero address)
SELECT 
    t.contract_address,
    m.name,
    m.symbol,
    COUNT() as mint_events,
    SUM(t.value) as total_minted,
    COUNT(DISTINCT t.to_address) as unique_recipients
FROM token_intelligence.erc20_transfers_130 t
LEFT JOIN token_intelligence.token_metadata_130 m ON t.contract_address = m.contract_address
WHERE t.from_address = '0x0000000000000000000000000000000000000000'
GROUP BY t.contract_address, m.name, m.symbol
ORDER BY total_minted DESC
LIMIT 20;

-- Token velocity (transaction frequency)
SELECT 
    t.contract_address,
    m.symbol,
    COUNT() as transfers,
    COUNT(DISTINCT t.from_address) + COUNT(DISTINCT t.to_address) as unique_addresses,
    COUNT()::Float64 / (COUNT(DISTINCT t.from_address) + COUNT(DISTINCT t.to_address)) as velocity_ratio,
    COUNT() / ((MAX(t.block_timestamp) - MIN(t.block_timestamp)) / 3600) as transfers_per_hour
FROM token_intelligence.erc20_transfers_130 t
LEFT JOIN token_intelligence.token_metadata_130 m ON t.contract_address = m.contract_address
GROUP BY t.contract_address, m.symbol
HAVING transfers >= 1000
ORDER BY transfers_per_hour DESC
LIMIT 25;

-- ==========================================
-- 📋 METADATA & TOKEN DISCOVERY
-- ==========================================

-- Token metadata overview
SELECT 
    COUNT() as total_tokens,
    COUNT(DISTINCT symbol) as unique_symbols,
    AVG(decimals) as avg_decimals,
    COUNT(CASE WHEN decimals = 18 THEN 1 END) as tokens_18_decimals,
    COUNT(CASE WHEN decimals = 6 THEN 1 END) as tokens_6_decimals,
    COUNT(CASE WHEN decimals NOT IN (18, 6, 8) THEN 1 END) as unusual_decimals
FROM token_intelligence.token_metadata_130;

-- Tokens with unusual decimal places
SELECT 
    contract_address,
    name,
    symbol,
    decimals
FROM token_intelligence.token_metadata_130
WHERE decimals NOT IN (18, 6, 8)
ORDER BY decimals DESC
LIMIT 20;

-- Popular token symbols
SELECT 
    symbol,
    COUNT() as token_count,
    arrayStringConcat(groupArray(name), ', ') as token_names
FROM token_intelligence.token_metadata_130
WHERE symbol != ''
GROUP BY symbol
HAVING token_count > 1
ORDER BY token_count DESC
LIMIT 20;

-- ==========================================
-- 🔧 QUICK TEMPLATES FOR CUSTOMIZATION
-- ==========================================

-- Template: Specific token analysis
-- Replace contract address with your target token
/*
SELECT 
    COUNT() as total_transfers,
    COUNT(DISTINCT from_address) as unique_senders,
    COUNT(DISTINCT to_address) as unique_receivers,
    SUM(value) as total_volume,
    AVG(value) as average_transfer,
    MIN(block_timestamp) as first_transfer,
    MAX(block_timestamp) as latest_transfer
FROM token_intelligence.erc20_transfers_130
WHERE contract_address = 'YOUR_TOKEN_ADDRESS_HERE';
*/

-- Template: User activity analysis
-- Replace address with your target user
/*
SELECT 
    'Sent' as direction,
    COUNT() as transaction_count,
    COUNT(DISTINCT contract_address) as unique_tokens,
    SUM(value) as total_value
FROM token_intelligence.erc20_transfers_130
WHERE from_address = 'YOUR_USER_ADDRESS_HERE'
UNION ALL
SELECT 
    'Received' as direction,
    COUNT() as transaction_count,
    COUNT(DISTINCT contract_address) as unique_tokens,
    SUM(value) as total_value
FROM token_intelligence.erc20_transfers_130
WHERE to_address = 'YOUR_USER_ADDRESS_HERE';
*/

-- Template: Time-based filtering
-- Analyze specific time periods
/*
SELECT 
    toDate(block_timestamp) as date,
    COUNT() as daily_transfers,
    COUNT(DISTINCT contract_address) as active_tokens,
    SUM(value) as daily_volume
FROM token_intelligence.erc20_transfers_130
WHERE block_timestamp BETWEEN 'START_DATE' AND 'END_DATE'
GROUP BY date
ORDER BY date DESC;
*/ 
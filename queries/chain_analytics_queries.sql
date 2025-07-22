-- ==========================================
-- MULTI-CHAIN ERC20 ANALYTICS QUERIES
-- ==========================================
-- 
-- Each chain has its own transfer table: erc20_transfers_{chainId}
-- And token metadata table: token_metadata_{chainId}
--
-- TABLE DESIGN NOTES:
-- - ORDER BY (contract_address, block_number, log_index) → Fast token queries + perfect chronological order
-- - PARTITION BY toDate(block_timestamp) → Daily partitions (consistent across all chains)
-- - log_index ensures exact transaction ordering within blocks for perfect balance replay
--
-- PARTITIONING STRATEGY:
-- Daily partitions solve the "different block times" problem:
-- - Ethereum: ~12s/block → inconsistent partition sizes with block-based partitioning  
-- - Base/Polygon: ~2s/block → would create tiny partitions with block-based
-- - Daily partitions: consistent ~1 day of data per partition across ALL chains
-- - Easy archival: "DROP PARTITION '2024-01-01'" 
-- - Human-readable: "Show me yesterday's data"
--
-- Available Chains:
-- - Chain 1 (Ethereum): erc20_transfers_1, token_metadata_1
-- - Chain 8453 (Base): erc20_transfers_8453, token_metadata_8453  
-- - Chain 130 (Unichain): erc20_transfers_130, token_metadata_130
-- - Chain 137 (Polygon): erc20_transfers_137, token_metadata_137
-- - Chain 42161 (Arbitrum): erc20_transfers_42161, token_metadata_42161
-- - Chain 10 (Optimism): erc20_transfers_10, token_metadata_10
-- - Chain 56 (BSC): erc20_transfers_56, token_metadata_56
-- - And more...

-- ==========================================
-- BASIC CHAIN ANALYTICS
-- ==========================================

-- Perfect chronological order for a specific token (fast + accurate)
-- Uses contract_address first (fast), then chronological order within that token
-- CRITICAL: log_index preserves the exact order within each block
SELECT 
    block_number,
    log_index,
    block_timestamp,
    transaction_hash,
    contract_address,
    from_address,
    to_address,
    value
FROM token_intelligence.erc20_transfers_130  -- Change to your chain
WHERE contract_address = '0x4200000000000000000000000000000000000006'  -- Specific token
ORDER BY block_number, log_index  -- Already optimized by table ORDER BY
LIMIT 100;

-- PARTITION-OPTIMIZED QUERIES:

-- Recent activity (uses only today's partition - VERY fast)
SELECT contract_address, COUNT() as today_transfers
FROM token_intelligence.erc20_transfers_130
WHERE toDate(block_timestamp) = today()
GROUP BY contract_address
ORDER BY today_transfers DESC;

-- Last 7 days activity (uses only 7 partitions)
SELECT 
    toDate(block_timestamp) as date,
    COUNT() as daily_transfers,
    COUNT(DISTINCT contract_address) as active_tokens
FROM token_intelligence.erc20_transfers_130
WHERE block_timestamp >= now() - INTERVAL 7 DAY
GROUP BY toDate(block_timestamp)
ORDER BY date DESC;

-- Specific date range (efficient partition pruning)
SELECT contract_address, SUM(value) as volume
FROM token_intelligence.erc20_transfers_130  
WHERE toDate(block_timestamp) BETWEEN '2024-01-01' AND '2024-01-31'
GROUP BY contract_address
ORDER BY volume DESC;

-- Get transfer count and volume for a specific chain (example: Unichain)
SELECT 
    COUNT() as total_transfers,
    COUNT(DISTINCT contract_address) as unique_tokens,
    COUNT(DISTINCT from_address) as unique_senders,
    COUNT(DISTINCT to_address) as unique_receivers,
    SUM(value) as total_volume,
    MIN(block_timestamp) as first_transfer,
    MAX(block_timestamp) as latest_transfer
FROM token_intelligence.erc20_transfers_130;  -- Change to your chain

-- Most active tokens on a chain (example: Base)
SELECT 
    contract_address,
    COUNT() as transfer_count,
    COUNT(DISTINCT from_address) as unique_senders,
    COUNT(DISTINCT to_address) as unique_receivers,
    SUM(value) as total_volume
FROM token_intelligence.erc20_transfers_8453  -- Change to your chain
GROUP BY contract_address
ORDER BY transfer_count DESC
LIMIT 20;

-- Most active tokens with metadata (example: Base)
SELECT 
    t.contract_address,
    m.name,
    m.symbol,
    m.decimals,
    COUNT() as transfer_count,
    COUNT(DISTINCT t.from_address) as unique_senders,
    COUNT(DISTINCT t.to_address) as unique_receivers,
    SUM(t.value) as total_volume
FROM token_intelligence.erc20_transfers_8453 t  -- Change to your chain
LEFT JOIN token_intelligence.token_metadata_8453 m ON t.contract_address = m.contract_address
GROUP BY t.contract_address, m.name, m.symbol, m.decimals
ORDER BY transfer_count DESC
LIMIT 20;

-- ==========================================
-- USER BALANCE QUERIES (Per Chain)
-- ==========================================

-- User Portfolio Balance for a specific chain (example: Base)
-- Replace 'YOUR_ADDRESS_HERE' with any wallet address
SELECT 
    t.contract_address,
    m.name,
    m.symbol,
    m.decimals,
    SUM(received) - SUM(sent) as balance
FROM (
    SELECT contract_address, value as received, 0 as sent
    FROM token_intelligence.erc20_transfers_8453  -- Change to your chain
    WHERE to_address = 'YOUR_ADDRESS_HERE'
    UNION ALL
    SELECT contract_address, 0 as received, value as sent
    FROM token_intelligence.erc20_transfers_8453  -- Change to your chain
    WHERE from_address = 'YOUR_ADDRESS_HERE'
) t
LEFT JOIN token_intelligence.token_metadata_8453 m ON t.contract_address = m.contract_address  -- Change to your chain
GROUP BY t.contract_address, m.name, m.symbol, m.decimals
HAVING balance > 0
ORDER BY balance DESC;

-- Token Holder Balances for a specific chain (example: Unichain)
-- Replace 'YOUR_CONTRACT_HERE' with any token contract address
SELECT 
    address as holder_address,
    SUM(received) - SUM(sent) as balance
FROM (
    SELECT to_address as address, value as received, 0 as sent
    FROM token_intelligence.erc20_transfers_130  -- Change to your chain
    WHERE contract_address = 'YOUR_CONTRACT_HERE'
      AND to_address != '0x0000000000000000000000000000000000000000'
    UNION ALL
    SELECT from_address as address, 0 as received, value as sent
    FROM token_intelligence.erc20_transfers_130  -- Change to your chain
    WHERE contract_address = 'YOUR_CONTRACT_HERE'
      AND from_address != '0x0000000000000000000000000000000000000000'
)
GROUP BY address
HAVING balance > 0
ORDER BY balance DESC 
LIMIT 100;

-- ==========================================
-- CROSS-CHAIN ANALYTICS
-- ==========================================

-- Compare token activity across multiple chains
SELECT 
    'Base' as chain_name,
    8453 as chain_id,
    COUNT() as total_transfers,
    COUNT(DISTINCT contract_address) as unique_tokens,
    COUNT(DISTINCT from_address) as unique_senders
FROM token_intelligence.erc20_transfers_8453
UNION ALL
SELECT 
    'Unichain' as chain_name,
    130 as chain_id,
    COUNT() as total_transfers,
    COUNT(DISTINCT contract_address) as unique_tokens,
    COUNT(DISTINCT from_address) as unique_senders
FROM token_intelligence.erc20_transfers_130
UNION ALL
SELECT 
    'Polygon' as chain_name,
    137 as chain_id,
    COUNT() as total_transfers,
    COUNT(DISTINCT contract_address) as unique_tokens,
    COUNT(DISTINCT from_address) as unique_senders
FROM token_intelligence.erc20_transfers_137
ORDER BY total_transfers DESC;

-- Find same token symbols across different chains
SELECT 
    'Base' as chain_name,
    8453 as chain_id,
    contract_address,
    name,
    symbol,
    decimals
FROM token_intelligence.token_metadata_8453
WHERE symbol = 'USDC'  -- Change to your target symbol
UNION ALL
SELECT 
    'Ethereum' as chain_name,
    1 as chain_id,
    contract_address,
    name,
    symbol,
    decimals
FROM token_intelligence.token_metadata_1
WHERE symbol = 'USDC'
UNION ALL
SELECT 
    'Polygon' as chain_name,
    137 as chain_id,
    contract_address,
    name,
    symbol,
    decimals
FROM token_intelligence.token_metadata_137
WHERE symbol = 'USDC'
ORDER BY chain_name;

-- ==========================================
-- TIME-BASED ANALYTICS (Per Chain)
-- ==========================================

-- Daily transfer volume for a specific chain (example: Base)
SELECT 
    toDate(block_timestamp) as date,
    COUNT() as daily_transfers,
    COUNT(DISTINCT contract_address) as unique_tokens_traded,
    COUNT(DISTINCT from_address) as unique_senders,
    SUM(value) as daily_volume
FROM token_intelligence.erc20_transfers_8453  -- Change to your chain
WHERE block_timestamp >= now() - INTERVAL 30 DAY
GROUP BY toDate(block_timestamp)
ORDER BY date DESC
LIMIT 30;

-- Hourly activity pattern for a specific chain (example: Unichain)
SELECT 
    toHour(block_timestamp) as hour_of_day,
    COUNT() as transfer_count,
    AVG(value) as avg_transfer_value
FROM token_intelligence.erc20_transfers_130  -- Change to your chain
WHERE block_timestamp >= now() - INTERVAL 7 DAY
GROUP BY toHour(block_timestamp)
ORDER BY hour_of_day;

-- ==========================================
-- TOKEN-SPECIFIC ANALYTICS (Per Chain)
-- ==========================================

-- Top holders for a specific token and chain
SELECT 
    holder_address,
    balance,
    balance * 100.0 / SUM(balance) OVER () as percentage_of_supply
FROM (
    SELECT 
        address as holder_address,
        SUM(received) - SUM(sent) as balance
    FROM (
        SELECT to_address as address, value as received, 0 as sent
        FROM token_intelligence.erc20_transfers_8453  -- Change to your chain
        WHERE contract_address = 'YOUR_TOKEN_CONTRACT_HERE'
          AND to_address != '0x0000000000000000000000000000000000000000'
        UNION ALL
        SELECT from_address as address, 0 as received, value as sent
        FROM token_intelligence.erc20_transfers_8453  -- Change to your chain
        WHERE contract_address = 'YOUR_TOKEN_CONTRACT_HERE'
          AND from_address != '0x0000000000000000000000000000000000000000'
    )
    GROUP BY address
    HAVING balance > 0
)
ORDER BY balance DESC
LIMIT 50;

-- Token transfer history with metadata for a specific chain
SELECT 
    t.block_timestamp,
    t.from_address,
    t.to_address,
    t.value,
    m.name as token_name,
    m.symbol as token_symbol,
    m.decimals
FROM token_intelligence.erc20_transfers_8453 t  -- Change to your chain
LEFT JOIN token_intelligence.token_metadata_8453 m ON t.contract_address = m.contract_address
WHERE t.contract_address = 'YOUR_TOKEN_CONTRACT_HERE'
ORDER BY t.block_timestamp DESC
LIMIT 100;

-- ==========================================
-- QUICK TEMPLATES (Just change the chain ID)
-- ==========================================

-- Template: Chain Overview
/*
SELECT 
    COUNT() as total_transfers,
    COUNT(DISTINCT contract_address) as unique_tokens,
    COUNT(DISTINCT from_address) as unique_users,
    formatReadableSize(SUM(value)) as total_volume,
    MIN(block_timestamp) as first_activity,
    MAX(block_timestamp) as latest_activity
FROM token_intelligence.erc20_transfers_CHAIN_ID;  -- Replace CHAIN_ID
*/

-- Template: Token Search by Symbol
/*
SELECT 
    contract_address,
    name,
    symbol,
    decimals
FROM token_intelligence.token_metadata_CHAIN_ID  -- Replace CHAIN_ID
WHERE symbol ILIKE '%SEARCH_TERM%';  -- Replace SEARCH_TERM
*/

-- Template: User Activity
/*
SELECT 
    'Sent' as direction,
    COUNT() as transaction_count,
    SUM(value) as total_value
FROM token_intelligence.erc20_transfers_CHAIN_ID  -- Replace CHAIN_ID
WHERE from_address = 'USER_ADDRESS'  -- Replace USER_ADDRESS
UNION ALL
SELECT 
    'Received' as direction,
    COUNT() as transaction_count,
    SUM(value) as total_value
FROM token_intelligence.erc20_transfers_CHAIN_ID  -- Replace CHAIN_ID
WHERE to_address = 'USER_ADDRESS';  -- Replace USER_ADDRESS
*/ 
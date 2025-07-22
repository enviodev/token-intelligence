# Token Intelligence Platform

A multi-chain ERC20 token analytics platform that collects, stores, and analyzes token transfer data across multiple blockchain networks using Hypersync and ClickHouse.

## 🏗️ Project Structure

```
token-intelligence/
├── src/                    # Main application code
│   └── collect.js         # ERC20 transfer data collection script
├── scripts/               # Utility scripts
│   └── populate_token_cache.js  # Token metadata population script
├── queries/               # SQL analytics queries
│   ├── chain_analytics_queries.sql  # Multi-chain analytics queries
│   └── analytics_queries.sql        # Legacy analytics queries
├── docs/                  # Documentation
│   ├── README.md         # This file
│   ├── ANALYTICS_SETUP.md # Analytics setup guide
│   └── TODO.md           # Project roadmap
├── config/                # Configuration files
│   └── docker-compose.yml # ClickHouse & Metabase setup
├── .cache/                # Token metadata cache files
│   ├── tokenMetadata_1.json      # Ethereum tokens
│   ├── tokenMetadata_8453.json   # Base tokens
│   └── ...               # Other chain metadata
├── package.json          # Node.js dependencies and scripts
└── pnpm-lock.yaml       # Package manager lock file
```

## 🚀 Quick Start

### 1. Install Dependencies

```bash
pnpm install
```

### 2. Start Analytics Stack

```bash
pnpm run analytics:up
```

This starts ClickHouse (database) and Metabase (dashboards) in Docker.

### 3. Populate Token Metadata

```bash
pnpm run populate-cache
```

Loads token metadata from `.cache/` files into ClickHouse.

### 4. Collect Transfer Data

```bash
# Default (Unichain)
pnpm start

# Specific chains
pnpm run collect:base       # Base chain
pnpm run collect:ethereum   # Ethereum mainnet
pnpm run collect:polygon    # Polygon
```

## 🌐 Supported Chains

| Chain ID | Network   | Command             | Table                     |
| -------- | --------- | ------------------- | ------------------------- |
| 1        | Ethereum  | `collect:ethereum`  | `erc20_transfers_1`       |
| 8453     | Base      | `collect:base`      | `erc20_transfers_8453`    |
| 130      | Unichain  | `collect:unichain`  | `erc20_transfers_130`     |
| 137      | Polygon   | `collect:polygon`   | `erc20_transfers_137`     |
| 42161    | Arbitrum  | `collect:arbitrum`  | `erc20_transfers_42161`   |
| 10       | Optimism  | `collect:optimism`  | `erc20_transfers_10`      |
| 56       | BSC       | `collect:bsc`       | `erc20_transfers_56`      |
| 43114    | Avalanche | `collect:avalanche` | `erc20_transfers_43114`   |
| 81457    | Blast     | `collect:blast`     | `erc20_transfers_81457`   |
| 7777777  | Zora      | `collect:zora`      | `erc20_transfers_7777777` |

## 📊 Database Schema

### Transfer Tables: `erc20_transfers_{chainId}`

```sql
CREATE TABLE erc20_transfers_130 (
    block_number UInt64,
    block_timestamp DateTime,
    log_index UInt32,
    transaction_hash String,
    contract_address LowCardinality(String),
    from_address String,
    to_address String,
    value UInt256,
    db_write_timestamp DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (contract_address, block_number, log_index)
PARTITION BY toDate(block_timestamp);
```

### Token Metadata Tables: `token_metadata_{chainId}`

```sql
CREATE TABLE token_metadata_130 (
    contract_address LowCardinality(String),
    name String,
    symbol LowCardinality(String),
    decimals UInt8,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY contract_address;
```

## 🔧 Available Scripts

### Data Collection

- `pnpm start` - Start collecting (default: Unichain)
- `pnpm run collect:ethereum` - Collect Ethereum transfers
- `pnpm run collect:base` - Collect Base transfers
- `pnpm run collect:polygon` - Collect Polygon transfers
- `pnpm run collect:arbitrum` - Collect Arbitrum transfers
- `pnpm run collect:optimism` - Collect Optimism transfers
- `pnpm run collect:bsc` - Collect BSC transfers
- `pnpm run collect:avalanche` - Collect Avalanche transfers
- `pnpm run collect:blast` - Collect Blast transfers
- `pnpm run collect:zora` - Collect Zora transfers

### Utilities

- `pnpm run populate-cache` - Load token metadata into ClickHouse

### Analytics Infrastructure

- `pnpm run analytics:up` - Start ClickHouse + Metabase
- `pnpm run analytics:down` - Stop analytics stack
- `pnpm run analytics:logs` - View logs
- `pnpm run analytics:restart` - Restart services
- `pnpm run analytics:clean` - Stop and remove all data

## 📈 Analytics & Queries

### Web Interfaces

- **ClickHouse**: http://localhost:8123/play (SQL queries)
- **Metabase**: http://localhost:3000 (dashboards)

### Sample Queries

See `queries/chain_analytics_queries.sql` for comprehensive examples:

```sql
-- Chain overview
SELECT COUNT() as transfers, COUNT(DISTINCT contract_address) as tokens
FROM token_intelligence.erc20_transfers_8453;

-- Top tokens with metadata
SELECT t.contract_address, m.name, m.symbol, COUNT() as transfers
FROM token_intelligence.erc20_transfers_8453 t
LEFT JOIN token_intelligence.token_metadata_8453 m
ON t.contract_address = m.contract_address
GROUP BY t.contract_address, m.name, m.symbol
ORDER BY transfers DESC LIMIT 10;

-- User token balances
SELECT contract_address, SUM(received) - SUM(sent) as balance
FROM (
    SELECT contract_address, value as received, 0 as sent
    FROM token_intelligence.erc20_transfers_8453
    WHERE to_address = 'YOUR_ADDRESS'
    UNION ALL
    SELECT contract_address, 0 as received, value as sent
    FROM token_intelligence.erc20_transfers_8453
    WHERE from_address = 'YOUR_ADDRESS'
)
GROUP BY contract_address
HAVING balance > 0;
```

## 🛠️ Development

### Adding New Chains

1. Add chain config to `src/collect.js`:

```javascript
const CHAIN_CONFIG = {
  // ... existing chains
  999: { name: "New Chain", hypersyncUrl: "http://999.hypersync.xyz" },
};
```

2. Add npm script to `package.json`:

```json
"collect:newchain": "node src/collect.js 999"
```

3. Add token metadata file to `.cache/tokenMetadata_999.json`

### Custom Queries

Add your analytics queries to `queries/` directory and reference them in documentation.

## 📚 Additional Documentation

- **[Analytics Setup Guide](ANALYTICS_SETUP.md)** - Detailed setup instructions
- **[Project TODO](TODO.md)** - Roadmap and planned features
- **[Query Examples](../queries/)** - SQL query library

## 🔗 Technology Stack

- **[Hypersync](https://docs.envio.dev/docs/hypersync)** - High-speed blockchain data streaming
- **[ClickHouse](https://clickhouse.com/)** - Columnar database for analytics
- **[Metabase](https://www.metabase.com/)** - Business intelligence dashboards
- **[Viem](https://viem.sh/)** - Ethereum utilities
- **[Docker](https://www.docker.com/)** - Containerized infrastructure

## 📄 License

GPL v3

# Project Structure Reference

## 📁 Directory Layout

```
token-intelligence/
├── 📁 src/                 # Core application code
│   └── 🔄 collect.js      # Main ERC20 transfer collector
│
├── 📁 scripts/             # Utility & setup scripts
│   └── 📥 populate_token_cache.js  # Load token metadata
│
├── 📁 queries/             # SQL analytics queries
│   ├── 📊 chain_analytics_queries.sql  # Multi-chain queries
│   └── 📊 analytics_queries.sql        # Legacy queries
│
├── 📁 docs/                # Documentation & guides
│   ├── 📖 README.md        # Main project documentation
│   ├── 📋 ANALYTICS_SETUP.md  # Setup instructions
│   ├── 📝 TODO.md          # Project roadmap
│   └── 🏗️ PROJECT_STRUCTURE.md  # This file
│
├── 📁 config/              # Configuration files
│   └── 🐳 docker-compose.yml  # ClickHouse + Metabase
│
├── 📁 .cache/              # Token metadata cache
│   ├── 💾 tokenMetadata_1.json      # Ethereum
│   ├── 💾 tokenMetadata_8453.json   # Base
│   ├── 💾 tokenMetadata_130.json    # Unichain
│   └── 💾 ...                       # Other chains
│
├── 📁 node_modules/        # Dependencies
├── 📄 package.json         # Node.js project config
├── 🔒 pnpm-lock.yaml      # Dependency lock file
└── 🚫 .gitignore          # Git ignore rules
```

## 🎯 Key File Purposes

| File                              | Purpose                | Usage                                   |
| --------------------------------- | ---------------------- | --------------------------------------- |
| `src/collect.js`                  | Main data collector    | `pnpm start` or `pnpm run collect:base` |
| `scripts/populate_token_cache.js` | Setup token metadata   | `pnpm run populate-cache`               |
| `queries/*.sql`                   | Analytics queries      | Copy/paste into ClickHouse              |
| `config/docker-compose.yml`       | Infrastructure setup   | `pnpm run analytics:up`                 |
| `docs/README.md`                  | Complete documentation | Read first                              |

## 🚀 Quick Commands

```bash
# Setup
pnpm install                    # Install dependencies
pnpm run analytics:up          # Start databases
pnpm run populate-cache        # Load token metadata

# Data collection
pnpm run collect:base          # Collect Base transfers
pnpm run collect:ethereum      # Collect Ethereum transfers
pnpm run collect:polygon       # Collect Polygon transfers

# Analytics
open http://localhost:8123/play  # ClickHouse SQL interface
open http://localhost:3000       # Metabase dashboards
```

## 🔄 Typical Workflow

1. **Setup**: `pnpm run analytics:up` + `pnpm run populate-cache`
2. **Collect**: `pnpm run collect:base` (or your preferred chain)
3. **Analyze**: Use queries from `queries/` folder
4. **Dashboard**: Build visualizations in Metabase
5. **Scale**: Add more chains or customize queries

## 📊 Database Tables

### Per Chain:

- `erc20_transfers_{chainId}` - Transfer events
- `token_metadata_{chainId}` - Token info (name, symbol, decimals)

### Example:

- `erc20_transfers_8453` + `token_metadata_8453` (Base)
- `erc20_transfers_130` + `token_metadata_130` (Unichain)
- `erc20_transfers_1` + `token_metadata_1` (Ethereum)

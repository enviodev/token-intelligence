# Analytics Stack Setup Guide

This setup provides a complete analytics stack with ClickHouse database and Metabase visualization tool.

## Quick Start

### 1. Start the Analytics Stack

```bash
pnpm run analytics:up
```

This will start both ClickHouse and Metabase in the background.

### 2. Access the Services

- **ClickHouse HTTP Interface**: http://localhost:8123
- **Metabase Dashboard**: http://localhost:3000

Wait about 1-2 minutes for Metabase to fully initialize.

### 3. Connect Metabase to ClickHouse

1. Open Metabase at http://localhost:3000
2. Complete the initial setup (create admin account)
3. Add ClickHouse as a data source:
   - **Database type**: ClickHouse
   - **Host**: `clickhouse`
   - **Port**: `8123`
   - **Database name**: `token_intelligence`
   - **Username**: `default`
   - **Password**: (leave empty)
   - **Use SSL**: No

## Available Commands

```bash
# Start services (detached)
pnpm run analytics:up

# Stop services
pnpm run analytics:down

# View logs
pnpm run analytics:logs

# Restart services
pnpm run analytics:restart

# Stop and remove everything (including data volumes)
pnpm run analytics:clean
```

## Database Credentials

- **Username**: `default`
- **Password**: (none)
- **Database**: `token_intelligence`

## Data Persistence

Data is automatically persisted in Docker volumes:

- `clickhouse_data` - ClickHouse database files
- `metabase_data` - Metabase configuration and dashboards

## Testing ClickHouse Connection

You can test ClickHouse directly via HTTP:

```bash
curl "http://localhost:8123/ping"
# Should return "Ok."

# Test with query
curl "http://localhost:8123/?query=SELECT%20version()"
```

## Next Steps

1. Create tables in ClickHouse for your ERC20 transfer data
2. Modify your `collect.js` script to insert data into ClickHouse
3. Build dashboards in Metabase to visualize token transfer patterns

## Troubleshooting

If services don't start:

```bash
# Check logs
pnpm run analytics:logs

# Clean restart
pnpm run analytics:clean
pnpm run analytics:up
```

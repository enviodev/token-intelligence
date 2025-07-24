import { createClient } from "@clickhouse/client";

// Chain configuration - matches the main collect.js file
const CHAIN_CONFIG = {
  1: { name: "Ethereum" },
  10: { name: "Optimism" },
  56: { name: "BSC" },
  130: { name: "Unichain" },
  137: { name: "Polygon" },
  480: { name: "World Chain" },
  1868: { name: "Lightlink" },
  7777777: { name: "Zora" },
  8453: { name: "Base" },
  42161: { name: "Arbitrum" },
  43114: { name: "Avalanche" },
  81457: { name: "Blast" },
};

// Get chain ID from command line argument
const CHAIN_ID = parseInt(process.argv[2]);

// Validate chain ID
if (!CHAIN_ID || !CHAIN_CONFIG[CHAIN_ID]) {
  console.error(`❌ Invalid or missing chain ID: ${CHAIN_ID || "undefined"}`);
  console.log("Usage: node scripts/cleanup_network_data.js <CHAIN_ID>");
  console.log("Available chains:");
  Object.keys(CHAIN_CONFIG)
    .map((id) => `  ${id} (${CHAIN_CONFIG[id].name})`)
    .forEach((chain) => console.log(chain));
  process.exit(1);
}

const chainInfo = CHAIN_CONFIG[CHAIN_ID];
const tableName = `erc20_transfers_${CHAIN_ID}`;

console.log(
  `🧹 Cleaning up data for ${chainInfo.name} (Chain ID: ${CHAIN_ID})`
);
console.log(`📋 Target table: ${tableName}`);

// Initialize ClickHouse client
const clickhouse = createClient({
  url: "http://localhost:8123",
});

async function cleanupNetworkData() {
  try {
    // Check if the table exists first
    console.log("🔍 Checking if table exists...");
    const tableCheck = await clickhouse.query({
      query: `EXISTS token_intelligence.${tableName}`,
      format: "JSONEachRow",
    });
    const tableExists = await tableCheck.json();

    if (!tableExists[0]?.result) {
      console.log(
        `ℹ️  Table ${tableName} does not exist. Nothing to clean up.`
      );
      return;
    }

    // Get row count before deletion
    console.log("📊 Getting current data statistics...");
    try {
      const countResult = await clickhouse.query({
        query: `SELECT COUNT(*) as total_rows FROM token_intelligence.${tableName}`,
        format: "JSONEachRow",
      });
      const countData = await countResult.json();
      const totalRows = countData[0]?.total_rows || 0;
      console.log(`📈 Current rows in table: ${totalRows.toLocaleString()}`);
    } catch (error) {
      console.log("⚠️  Could not get row count (table may be corrupted)");
    }

    // Ask for confirmation
    console.log(
      `\n⚠️  WARNING: This will permanently delete ALL data for ${chainInfo.name}!`
    );
    console.log("Press Ctrl+C to cancel, or wait 5 seconds to proceed...");

    // Wait 5 seconds
    await new Promise((resolve) => setTimeout(resolve, 5000));

    // Try multiple cleanup strategies for corrupted tables
    console.log(`🗑️  Attempting to clean up table ${tableName}...`);

    let cleanupSuccess = false;

    // Strategy 1: Try DETACH first (faster for corrupted tables)
    try {
      console.log("📌 Strategy 1: Detaching table...");
      await clickhouse.command({
        query: `DETACH TABLE token_intelligence.${tableName}`,
      });

      console.log("🗑️  Dropping detached table...");
      await clickhouse.command({
        query: `DROP TABLE IF EXISTS token_intelligence.${tableName}`,
      });

      cleanupSuccess = true;
      console.log("✅ Successfully cleaned up using DETACH/DROP strategy!");
    } catch (detachError) {
      console.log("⚠️  DETACH strategy failed, trying direct DROP...");

      // Strategy 2: Force DROP with shorter timeout
      try {
        console.log("📌 Strategy 2: Direct DROP with timeout...");
        await Promise.race([
          clickhouse.command({
            query: `DROP TABLE IF EXISTS token_intelligence.${tableName}`,
          }),
          new Promise((_, reject) =>
            setTimeout(() => reject(new Error("Manual timeout")), 30000)
          ),
        ]);

        cleanupSuccess = true;
        console.log("✅ Successfully cleaned up using direct DROP!");
      } catch (dropError) {
        console.log("⚠️  Direct DROP failed, trying system approach...");

        // Strategy 3: Use system tables approach
        try {
          console.log("📌 Strategy 3: Using system tables...");

          // First, try to get the table UUID if it exists
          const uuidResult = await clickhouse.query({
            query: `SELECT uuid FROM system.tables WHERE database = 'token_intelligence' AND name = '${tableName}'`,
            format: "JSONEachRow",
          });
          const uuidRows = await uuidResult.json();

          if (uuidRows.length > 0) {
            const tableUUID = uuidRows[0].uuid;
            console.log(`🔑 Found table UUID: ${tableUUID}`);

            // Try to drop by UUID
            await clickhouse.command({
              query: `DROP TABLE IF EXISTS token_intelligence.\`${tableUUID}\``,
            });

            cleanupSuccess = true;
            console.log("✅ Successfully cleaned up using UUID approach!");
          } else {
            console.log(
              "ℹ️  Table not found in system.tables (may already be cleaned)"
            );
            cleanupSuccess = true;
          }
        } catch (systemError) {
          console.log("❌ All cleanup strategies failed.");
          console.log(
            "💡 The table may be severely corrupted. Manual intervention required."
          );
          console.log(
            "🛠️  Try restarting ClickHouse service or check ClickHouse logs."
          );
          throw systemError;
        }
      }
    }

    if (cleanupSuccess) {
      console.log(`✅ Successfully cleaned up ${chainInfo.name} data!`);
      console.log(`📋 Table ${tableName} has been removed.`);
      console.log(`🔄 You can now run collection again to start fresh.`);
    }
  } catch (error) {
    console.error("❌ Error during cleanup:", error.message);
    console.error(
      "💡 This might indicate database connectivity issues or permission problems."
    );
    process.exit(1);
  } finally {
    await clickhouse.close();
  }
}

async function main() {
  console.log("🚀 Starting network data cleanup...");
  await cleanupNetworkData();
  console.log("🎉 Cleanup complete!");
}

main().catch((error) => {
  console.error("Fatal error:", error);
  process.exit(1);
});

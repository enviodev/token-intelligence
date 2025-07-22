import { createClient } from "@clickhouse/client";
import { readdir, readFile } from "fs/promises";
import path from "path";

// Initialize ClickHouse client
const clickhouse = createClient({
  url: "http://localhost:8123",
});

// Initialize database and table for specific chain token metadata
async function initializeChainTokenTable(chainId) {
  const tableName = `token_metadata_${chainId}`;
  console.log(`Setting up ClickHouse table: ${tableName}...`);

  // Create database if it doesn't exist (should already exist)
  await clickhouse.command({
    query: "CREATE DATABASE IF NOT EXISTS token_intelligence",
  });

  // Drop existing table to recreate with new schema
  await clickhouse.command({
    query: `DROP TABLE IF EXISTS token_intelligence.${tableName}`,
  });

  // Create chain-specific token metadata table
  await clickhouse.command({
    query: `
      CREATE TABLE IF NOT EXISTS token_intelligence.${tableName} (
        contract_address LowCardinality(String),
        name String,
        symbol LowCardinality(String),
        decimals UInt8,
        created_at DateTime DEFAULT now(),
        
        INDEX idx_contract contract_address TYPE bloom_filter GRANULARITY 1,
        INDEX idx_symbol symbol TYPE bloom_filter GRANULARITY 1
      ) ENGINE = MergeTree()
      ORDER BY contract_address
    `,
  });

  console.log(`✅ Table ${tableName} ready!`);
  return tableName;
}

// Batch insert function for better performance
async function insertTokenBatch(tokens, tableName) {
  if (tokens.length === 0) return;

  await clickhouse.insert({
    table: `token_intelligence.${tableName}`,
    values: tokens,
    format: "JSONEachRow",
  });
}

// Extract chain ID from filename
function extractChainId(filename) {
  const match = filename.match(/tokenMetadata_(\d+)\.json$/);
  return match ? parseInt(match[1]) : null;
}

// Process a single metadata file
async function processMetadataFile(filePath, chainId) {
  console.log(
    `Processing ${path.basename(filePath)} (Chain ID: ${chainId})...`
  );

  try {
    // Initialize chain-specific table
    const tableName = await initializeChainTokenTable(chainId);

    const fileContent = await readFile(filePath, "utf8");
    const metadata = JSON.parse(fileContent);

    const tokenBatch = [];
    let processedCount = 0;

    // Process each token in the file
    for (const [checksummedAddress, tokenData] of Object.entries(metadata)) {
      // Lowercase the address to match existing data format
      const lowercaseAddress = checksummedAddress.toLowerCase();

      // Validate required fields
      if (
        !tokenData.name ||
        !tokenData.symbol ||
        typeof tokenData.decimals !== "number"
      ) {
        console.warn(
          `Skipping invalid token data for ${checksummedAddress}:`,
          tokenData
        );
        continue;
      }

      tokenBatch.push({
        contract_address: lowercaseAddress,
        name: tokenData.name,
        symbol: tokenData.symbol,
        decimals: tokenData.decimals,
      });

      processedCount++;

      // Insert in batches of 1000
      if (tokenBatch.length >= 1000) {
        await insertTokenBatch(tokenBatch, tableName);
        console.log(
          `  ✅ Inserted batch of ${tokenBatch.length} tokens (${processedCount} total)`
        );
        tokenBatch.length = 0; // Clear the batch
      }
    }

    // Insert any remaining tokens
    if (tokenBatch.length > 0) {
      await insertTokenBatch(tokenBatch, tableName);
      console.log(`  ✅ Inserted final batch of ${tokenBatch.length} tokens`);
    }

    console.log(
      `  🎉 Completed ${path.basename(
        filePath
      )}: ${processedCount} tokens processed`
    );
    return { chainId, tokenCount: processedCount, tableName };
  } catch (error) {
    console.error(
      `❌ Error processing ${path.basename(filePath)}:`,
      error.message
    );
    return { chainId, tokenCount: 0, tableName: null };
  }
}

const main = async () => {
  console.log("Starting token metadata cache population...");
  const startTime = performance.now();

  // Find all metadata files
  const cacheDir = ".cache";
  const files = await readdir(cacheDir);
  const metadataFiles = files.filter(
    (file) => file.startsWith("tokenMetadata_") && file.endsWith(".json")
  );

  console.log(`Found ${metadataFiles.length} metadata files to process:`);
  metadataFiles.forEach((file) => console.log(`  - ${file}`));
  console.log();

  let totalTokens = 0;
  let processedFiles = 0;
  const createdTables = [];

  // Process each metadata file
  for (const filename of metadataFiles) {
    const chainId = extractChainId(filename);

    if (!chainId) {
      console.warn(`⚠️  Could not extract chain ID from filename: ${filename}`);
      continue;
    }

    const filePath = path.join(cacheDir, filename);
    const result = await processMetadataFile(filePath, chainId);

    totalTokens += result.tokenCount;
    processedFiles++;
    if (result.tableName) {
      createdTables.push({
        chainId: result.chainId,
        tableName: result.tableName,
        tokenCount: result.tokenCount,
      });
    }
    console.log(); // Add spacing between files
  }

  // Print final results
  const totalTime = (performance.now() - startTime) / 1000;
  console.log(`🎉 Population complete!`);
  console.log(`📊 Summary:`);
  console.log(
    `   - Files processed: ${processedFiles}/${metadataFiles.length}`
  );
  console.log(`   - Total tokens: ${totalTokens.toLocaleString()}`);
  console.log(`   - Time taken: ${totalTime.toFixed(1)}s`);
  console.log(`   - Rate: ${(totalTokens / totalTime).toFixed(0)} tokens/sec`);

  // Print chain distribution
  console.log(`\n📈 Verifying data in ClickHouse...`);
  console.log(`Chain distribution:`);

  for (const table of createdTables) {
    try {
      const result = await clickhouse.query({
        query: `
          SELECT 
            COUNT() as token_count,
            COUNT(DISTINCT symbol) as unique_symbols
          FROM token_intelligence.${table.tableName}
        `,
        format: "JSONEachRow",
      });

      const rows = await result.json();
      if (rows.length > 0) {
        const row = rows[0];
        console.log(
          `   Chain ${
            table.chainId
          }: ${row.token_count.toLocaleString()} tokens (${
            row.unique_symbols
          } unique symbols) → table: ${table.tableName}`
        );
      }
    } catch (error) {
      console.error(
        `❌ Error verifying table ${table.tableName}:`,
        error.message
      );
    }
  }

  // Close ClickHouse connection
  await clickhouse.close();
};

main().catch((error) => {
  console.error("Error:", error);
  process.exit(1);
});

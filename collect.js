import { keccak256, toHex } from "viem";
import {
  HypersyncClient,
  LogField,
  JoinMode,
  BlockField,
  TransactionField,
  HexOutput,
  DataType,
  Decoder,
} from "@envio-dev/hypersync-client";
import { createClient } from "@clickhouse/client";

// Define ERC20 Transfer event signature
const event_signatures = ["Transfer(address,address,uint256)"];

// Create topic0 hashes from event signatures
const topic0_list = event_signatures.map((sig) => keccak256(toHex(sig)));

// Initialize Hypersync client
const client = HypersyncClient.new({
  url: "http://unichain.hypersync.xyz",
});

// Initialize ClickHouse client
const clickhouse = createClient({
  url: "http://localhost:8123",
});

// Initialize database and table
async function initializeDatabase() {
  console.log("Setting up ClickHouse database and table...");

  // Create database if it doesn't exist
  await clickhouse.command({
    query: "CREATE DATABASE IF NOT EXISTS token_intelligence",
  });

  // Drop existing table to recreate with new schema
  await clickhouse.command({
    query: "DROP TABLE IF EXISTS token_intelligence.erc20_transfers",
  });

  // Create table with updated schema
  await clickhouse.command({
    query: `
      CREATE TABLE IF NOT EXISTS token_intelligence.erc20_transfers (
        block_number UInt64,
        block_timestamp DateTime,
        contract_address String,
        from_address String,
        to_address String,
        value UInt256,
        timestamp DateTime DEFAULT now()
      ) ENGINE = MergeTree()
      ORDER BY (block_number, timestamp)
      PARTITION BY intDiv(block_number, 100000)
    `,
  });

  console.log("✅ Database and table ready!");
}

// Define query for ERC20 Transfer events
let query = {
  fromBlock: 0,
  logs: [
    {
      topics: [topic0_list],
    },
  ],
  fieldSelection: {
    block: [BlockField.Number, BlockField.Timestamp],
    log: [
      // LogField.BlockNumber,
      // LogField.LogIndex,
      // LogField.TransactionIndex,
      // LogField.TransactionHash,
      LogField.Data,
      LogField.Address,
      LogField.Topic0,
      LogField.Topic1,
      LogField.Topic2,
      LogField.Topic3,
    ],
    // transaction: [
    //   TransactionField.From,
    //   TransactionField.To,
    //   TransactionField.Hash,
    //   TransactionField.Value,
    // ],
  },
  joinMode: JoinMode.JoinTransactions,
};

// Batch insert function for better performance
async function insertTransferBatch(transfers) {
  if (transfers.length === 0) return;

  await clickhouse.insert({
    table: "token_intelligence.erc20_transfers",
    values: transfers,
    format: "JSONEachRow",
  });
}

const main = async () => {
  console.log("Starting ERC20 Transfer event scan...");

  // Initialize database and table
  await initializeDatabase();

  // Create decoder outside the loop for better performance
  const decoder = Decoder.fromSignatures([
    "Transfer(address indexed from, address indexed to, uint256 value)",
  ]);

  let totalEvents = 0;
  let totalTransferValue = BigInt(0);
  let transferBatch = [];
  const BATCH_SIZE = 1000; // Insert every 1000 records
  const startTime = performance.now();

  // Start streaming events
  const stream = await client.stream(query, {});

  while (true) {
    const res = await stream.recv();

    // Exit if we've reached the end of the chain
    if (res === null) {
      console.log("Reached the tip of the blockchain");
      break;
    }

    // Count total events
    if (res.data && res.data.logs) {
      totalEvents += res.data.logs.length;

      // Decode logs
      const decodedLogs = await decoder.decodeLogs(res.data.logs);

      // Get block data for this batch
      const blockData = res.data.blocks?.[0] || {};
      const blockNumber = blockData.number || 0;
      const blockTimestamp = blockData.timestamp
        ? new Date(Number(blockData.timestamp) * 1000)
            .toISOString()
            .slice(0, 19)
            .replace("T", " ")
        : new Date().toISOString().slice(0, 19).replace("T", " ");

      // Track if we've printed an event for this batch
      let printedEventThisBatch = false;

      // Process transfer events - need both original logs and decoded logs
      for (let i = 0; i < decodedLogs.length; i++) {
        const log = decodedLogs[i];
        const originalLog = res.data.logs[i];

        // Skip invalid logs
        if (log === null) {
          continue;
        }

        // Access the decoded values directly without using JSON.stringify
        try {
          // Get from and to addresses from indexed parameters
          const from = log.indexed[0]?.val.toString() || "0x0";
          const to = log.indexed[1]?.val.toString() || "0x0";

          // Get transfer value from body
          const value = log.body[0]?.val || BigInt(0);

          // Get contract address from original log data
          const contractAddress = originalLog.address || "0x0";

          // Track total transfer value for statistics
          totalTransferValue += value;

          // Add to batch for database insertion
          transferBatch.push({
            block_number: blockNumber,
            block_timestamp: blockTimestamp,
            contract_address: contractAddress,
            from_address: from,
            to_address: to,
            value: value.toString(), // ClickHouse will parse this as UInt256
          });

          // Print details for just the first transfer event in each batch
          if (!printedEventThisBatch) {
            console.log(
              `\nSample Transfer Event from Block ${blockNumber} (${blockTimestamp}):`
            );
            console.log(`  Contract: ${contractAddress}`);
            console.log(`  From: ${from}`);
            console.log(`  To: ${to}`);
            console.log(`  Value: ${value.toString()}`);

            // Mark that we've printed an event for this batch
            printedEventThisBatch = true;
          }
        } catch (error) {
          console.log("Error processing transfer event:", error.message);
        }
      }

      // Insert batch when it reaches the batch size
      if (transferBatch.length >= BATCH_SIZE) {
        try {
          await insertTransferBatch(transferBatch);
          console.log(
            `💾 Inserted ${transferBatch.length} transfers to database`
          );
          transferBatch = []; // Clear the batch
        } catch (error) {
          console.log("Error inserting batch:", error.message);
        }
      }
    }

    // Update query for next batch
    if (res.nextBlock) {
      query.fromBlock = res.nextBlock;
    }

    // Calculate and print simple progress metrics
    const seconds = (performance.now() - startTime) / 1000;

    console.log(
      `Block ${res.nextBlock} | ${totalEvents} events | ${
        transferBatch.length
      } pending | ${seconds.toFixed(1)}s | ${(totalEvents / seconds).toFixed(
        1
      )} events/s`
    );
  }

  // Insert any remaining transfers in the final batch
  if (transferBatch.length > 0) {
    try {
      await insertTransferBatch(transferBatch);
      console.log(
        `💾 Inserted final batch of ${transferBatch.length} transfers to database`
      );
    } catch (error) {
      console.log("Error inserting final batch:", error.message);
    }
  }

  // Print final results
  const totalTime = (performance.now() - startTime) / 1000;
  console.log(
    `\n🎉 Scan complete: ${totalEvents} transfer events in ${totalTime.toFixed(
      1
    )} seconds`
  );
  console.log(`💰 Total Transfer Value: ${totalTransferValue.toString()}`);
  console.log(
    `💾 All data saved to ClickHouse database: token_intelligence.erc20_transfers`
  );

  // Close ClickHouse connection
  await clickhouse.close();
};

main().catch((error) => {
  console.error("Error:", error);
  process.exit(1);
});

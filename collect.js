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

// Define ERC20 Transfer event signature
const event_signatures = ["Transfer(address,address,uint256)"];

// Create topic0 hashes from event signatures
const topic0_list = event_signatures.map((sig) => keccak256(toHex(sig)));

// Initialize Hypersync client
const client = HypersyncClient.new({
  url: "http://unichain.hypersync.xyz",
});

// Define query for ERC20 Transfer events
let query = {
  fromBlock: 0,
  logs: [
    {
      topics: [topic0_list],
    },
  ],
  fieldSelection: {
    // block: [BlockField.Number, BlockField.Timestamp, BlockField.Hash],
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

const main = async () => {
  console.log("Starting ERC20 Transfer event scan...");

  // Create decoder outside the loop for better performance
  const decoder = Decoder.fromSignatures([
    "Transfer(address indexed from, address indexed to, uint256 value)",
  ]);

  let totalEvents = 0;
  let totalTransferValue = BigInt(0);
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

      // Track if we've printed an event for this batch
      let printedEventThisBatch = false;

      // Process transfer events
      for (const log of decodedLogs) {
        // Skip invalid logs
        if (log === null) {
          continue;
        }

        // Access the decoded values directly without using JSON.stringify
        try {
          // Get from and to addresses from indexed parameters
          const from = log.indexed[0]?.val.toString() || "unknown";
          const to = log.indexed[1]?.val.toString() || "unknown";

          // Get transfer value from body
          const value = log.body[0]?.val || BigInt(0);

          // Track total transfer value for statistics
          totalTransferValue += value;

          // Print details for just the first transfer event in each batch
          if (!printedEventThisBatch) {
            console.log(
              "\nSample Transfer Event from Block " + res.nextBlock + ":"
            );
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
    }

    // Update query for next batch
    if (res.nextBlock) {
      query.fromBlock = res.nextBlock;
    }

    // Calculate and print simple progress metrics
    const seconds = (performance.now() - startTime) / 1000;

    console.log(
      `Block ${res.nextBlock} | ${totalEvents} events | ${seconds.toFixed(
        1
      )}s | ${(totalEvents / seconds).toFixed(1)} events/s`
    );
  }

  // Print final results
  const totalTime = (performance.now() - startTime) / 1000;
  console.log(
    `\nScan complete: ${totalEvents} transfer events in ${totalTime.toFixed(
      1
    )} seconds`
  );
  console.log(`Total Transfer Value: ${totalTransferValue.toString()}`);
};

main().catch((error) => {
  console.error("Error:", error);
  process.exit(1);
});

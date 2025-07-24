import fs from "fs/promises";
import { createClient } from "@clickhouse/client";

const CLICKHOUSE_URL = "http://localhost:8123";

// Initialize ClickHouse client
const clickhouse = createClient({
  url: CLICKHOUSE_URL,
});

// Parse SQL file to extract queries with improved handling
function parseSQLFile(content, filename) {
  const queries = [];
  const lines = content.split("\n");

  let currentQuery = "";
  let currentDescription = "";
  let inQuery = false;
  let inTemplate = false;
  let inMultiLineComment = false;

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i].trim();

    // Skip empty lines and section headers
    if (!line || line.startsWith("-- =====")) {
      continue;
    }

    // Handle multi-line comment blocks (/* ... */)
    if (line.includes("/*")) {
      inMultiLineComment = true;
      continue;
    }
    if (line.includes("*/")) {
      inMultiLineComment = false;
      continue;
    }
    if (inMultiLineComment) {
      continue;
    }

    // Check for template start/end
    if (line.startsWith("-- Template:")) {
      inTemplate = true;
      continue;
    }
    if (inTemplate && line.startsWith("*/")) {
      inTemplate = false;
      continue;
    }
    if (inTemplate) {
      continue;
    }

    // Comment line that describes a query
    if (
      line.startsWith("-- ") &&
      !line.includes("Change to your") &&
      !line.includes("Replace") &&
      !line.includes("Analyze") &&
      !line.includes("🎯") &&
      !line.includes("🌐") &&
      !line.includes("💡")
    ) {
      if (inQuery && currentQuery.trim()) {
        // Clean and save previous query
        const cleanedQuery = cleanQuery(currentQuery.trim());
        if (cleanedQuery) {
          queries.push({
            name: currentDescription || "Unnamed Query",
            sql: cleanedQuery,
            description: currentDescription,
          });
        }
        currentQuery = "";
      }
      currentDescription = line.replace("-- ", "").trim();
      inQuery = false;
    }
    // SQL line
    else if (line && !line.startsWith("--")) {
      if (!inQuery) {
        inQuery = true;
      }
      currentQuery += line + "\n";

      // End of query (semicolon at end of line)
      if (line.endsWith(";")) {
        const cleanedQuery = cleanQuery(currentQuery.trim());
        if (cleanedQuery) {
          queries.push({
            name: currentDescription || "Unnamed Query",
            sql: cleanedQuery,
            description: currentDescription,
          });
        }
        currentQuery = "";
        currentDescription = "";
        inQuery = false;
      }
    }
  }

  // Handle last query if no semicolon
  if (currentQuery.trim()) {
    const cleanedQuery = cleanQuery(currentQuery.trim());
    if (cleanedQuery) {
      queries.push({
        name: currentDescription || "Unnamed Query",
        sql: cleanedQuery,
        description: currentDescription,
      });
    }
  }

  return queries.filter((q) => q.sql && q.sql.length > 10);
}

// Clean and normalize SQL query
function cleanQuery(sql) {
  if (!sql) return null;

  // Remove extra whitespace and normalize
  sql = sql.replace(/\s+/g, " ").trim();

  // Remove trailing semicolon if present
  if (sql.endsWith(";")) {
    sql = sql.slice(0, -1);
  }

  // Remove any duplicate semicolons that might have been created
  sql = sql.replace(/;+/g, ";").replace(/;\s*$/, "");

  // Skip very short queries or obvious non-queries
  if (sql.length < 10 || !sql.toLowerCase().includes("select")) {
    return null;
  }

  return sql;
}

// Get sample data for placeholder replacement
async function getSampleData() {
  try {
    // Get a sample contract address
    const contractResult = await clickhouse.query({
      query: `
        SELECT contract_address 
        FROM token_intelligence.erc20_transfers_130 
        LIMIT 1
      `,
      format: "JSONEachRow",
    });
    const contractRows = await contractResult.json();
    const sampleContract =
      contractRows[0]?.contract_address ||
      "0x4200000000000000000000000000000000000006";

    // Get a sample user address
    const userResult = await clickhouse.query({
      query: `
        SELECT from_address 
        FROM token_intelligence.erc20_transfers_130 
        WHERE from_address != '0x0000000000000000000000000000000000000000'
        LIMIT 1
      `,
      format: "JSONEachRow",
    });
    const userRows = await userResult.json();
    const sampleUser =
      userRows[0]?.from_address || "0x65081cb48d74a32e9ccfed75164b8c09972dbcf1";

    return { sampleContract, sampleUser };
  } catch (error) {
    console.log("⚠️  Could not get sample data, using defaults");
    return {
      sampleContract: "0x4200000000000000000000000000000000000006",
      sampleUser: "0x65081cb48d74a32e9ccfed75164b8c09972dbcf1",
    };
  }
}

// Replace placeholders in SQL with real values
function replacePlaceholders(sql, sampleData) {
  return sql
    .replace(/YOUR_TOKEN_ADDRESS_HERE/g, sampleData.sampleContract)
    .replace(/YOUR_CONTRACT_HERE/g, sampleData.sampleContract)
    .replace(/YOUR_USER_ADDRESS_HERE/g, sampleData.sampleUser)
    .replace(/YOUR_ADDRESS_HERE/g, sampleData.sampleUser)
    .replace(/START_DATE/g, "2024-01-01 00:00:00")
    .replace(/END_DATE/g, "2024-12-31 23:59:59")
    .replace(/SEARCH_TERM/g, "USDC")
    .replace(/CHAIN_ID/g, "130");
}

// Smart LIMIT addition that respects SQL structure
function addLimitToQuery(sql) {
  const upperSql = sql.toUpperCase();

  // If query already has LIMIT, don't add another
  if (upperSql.includes(" LIMIT ")) {
    return sql;
  }

  // For UNION queries, we need to add LIMIT to the outer query
  if (upperSql.includes(" UNION ")) {
    // Wrap the entire query and add LIMIT
    return `SELECT * FROM (${sql}) LIMIT 5`;
  }

  // For CTEs (WITH clauses), add LIMIT to the end
  if (upperSql.startsWith("WITH ")) {
    return `${sql} LIMIT 5`;
  }

  // For simple SELECT queries
  if (upperSql.startsWith("SELECT ")) {
    return `${sql} LIMIT 5`;
  }

  // Default case - just add LIMIT
  return `${sql} LIMIT 5`;
}

// Validate SQL before execution
function validateSQL(sql) {
  // Check for common issues that cause multi-statement errors
  const issues = [];

  // Count semicolons (should be 0 after cleaning)
  const semicolonCount = (sql.match(/;/g) || []).length;
  if (semicolonCount > 0) {
    issues.push(`Contains ${semicolonCount} semicolon(s)`);
  }

  // Check for multiple SELECT statements that aren't in UNION
  const selectMatches = sql.match(/\bSELECT\b/gi) || [];
  const unionMatches = sql.match(/\bUNION\b/gi) || [];

  if (
    selectMatches.length > 1 &&
    unionMatches.length < selectMatches.length - 1
  ) {
    issues.push("Multiple SELECT statements detected without proper UNION");
  }

  return issues;
}

// Test a single query against ClickHouse
async function testQuery(query, sampleData) {
  try {
    // Replace placeholders
    let testSql = replacePlaceholders(query.sql, sampleData);

    // Add LIMIT intelligently
    testSql = addLimitToQuery(testSql);

    // Validate the SQL
    const validationIssues = validateSQL(testSql);
    if (validationIssues.length > 0) {
      console.log(`  ⚠️  SQL Issues: ${validationIssues.join(", ")}`);
      console.log(`     Query: ${testSql.substring(0, 100)}...`);
    }

    console.log(`  🧪 Testing: ${query.name.substring(0, 60)}...`);

    // Execute query with timeout
    const result = await clickhouse.query({
      query: testSql,
      format: "JSONEachRow",
    });

    const rows = await result.json();
    console.log(`    ✅ Success: ${rows.length} rows returned`);
    return { success: true, rowCount: rows.length };
  } catch (error) {
    const errorMessage = error.message.split("\n")[0];
    console.log(`    ❌ Failed: ${errorMessage}`);

    // Log problematic SQL for debugging
    if (
      errorMessage.includes("Multi-statements") ||
      errorMessage.includes("SYNTAX_ERROR")
    ) {
      console.log(`    🔍 Debug SQL: ${testSql?.substring(0, 200)}...`);
    }

    return { success: false, error: errorMessage };
  }
}

// Check database connection and table existence
async function checkDatabaseSetup() {
  console.log("🔍 Checking database setup...\n");

  try {
    // Test basic connection
    await clickhouse.query({ query: "SELECT 1", format: "JSONEachRow" });
    console.log("✅ ClickHouse connection working");

    // Check if database exists
    const dbResult = await clickhouse.query({
      query: "SHOW DATABASES",
      format: "JSONEachRow",
    });
    const databases = await dbResult.json();
    const hasTokenDB = databases.some((db) => db.name === "token_intelligence");

    if (!hasTokenDB) {
      console.log("❌ token_intelligence database not found");
      return false;
    }
    console.log("✅ token_intelligence database exists");

    // Check if tables exist
    const tablesResult = await clickhouse.query({
      query: "SHOW TABLES FROM token_intelligence",
      format: "JSONEachRow",
    });
    const tables = await tablesResult.json();
    const tableNames = tables.map((t) => t.name);

    const requiredTables = ["erc20_transfers_130", "token_metadata_130"];
    const missingTables = requiredTables.filter(
      (table) => !tableNames.includes(table)
    );

    if (missingTables.length > 0) {
      console.log(`❌ Missing tables: ${missingTables.join(", ")}`);
      console.log(`📋 Available tables: ${tableNames.join(", ")}`);
      return false;
    }

    console.log("✅ Required tables exist");

    // Check table data
    const dataResult = await clickhouse.query({
      query:
        "SELECT COUNT() as count FROM token_intelligence.erc20_transfers_130",
      format: "JSONEachRow",
    });
    const dataRows = await dataResult.json();
    const transferCount = dataRows[0]?.count || 0;

    console.log(`✅ Transfer table has ${transferCount.toLocaleString()} rows`);

    if (transferCount === 0) {
      console.log(
        "⚠️  No transfer data found - some queries may return empty results"
      );
    }

    return true;
  } catch (error) {
    console.log(`❌ Database setup check failed: ${error.message}`);
    return false;
  }
}

async function main() {
  console.log("🧪 Testing SQL Queries for Dashboard Creation\n");

  // Check database setup first
  const dbOk = await checkDatabaseSetup();
  if (!dbOk) {
    console.log(
      "\n❌ Database setup issues detected. Please fix before testing queries."
    );
    console.log("💡 Suggested actions:");
    console.log("   1. Ensure ClickHouse is running: pnpm run analytics:up");
    console.log("   2. Run data collection: pnpm start");
    console.log("   3. Populate metadata: pnpm run populate-cache");
    process.exit(1);
  }

  console.log("\n📄 Loading SQL queries...");

  // Read and parse SQL file
  const content = await fs.readFile("queries/dashboard_analytics.sql", "utf8");
  const queries = parseSQLFile(content, "dashboard_analytics.sql");

  console.log(`Found ${queries.length} queries to test\n`);

  // Get sample data for placeholder replacement
  const sampleData = await getSampleData();
  console.log(`📊 Using sample data:`);
  console.log(`   Contract: ${sampleData.sampleContract}`);
  console.log(`   User: ${sampleData.sampleUser}\n`);

  // Test each query
  let successCount = 0;
  let errorCount = 0;
  const errors = [];

  console.log("🧪 Testing queries...\n");

  for (const [index, query] of queries.entries()) {
    console.log(`[${index + 1}/${queries.length}]`);

    const result = await testQuery(query, sampleData);

    if (result.success) {
      successCount++;
    } else {
      errorCount++;
      errors.push({
        name: query.name,
        error: result.error,
        sql: query.sql.substring(0, 200) + "...",
      });
    }

    // Small delay to avoid overwhelming ClickHouse
    await new Promise((resolve) => setTimeout(resolve, 100));
  }

  // Summary
  console.log("\n🎉 Query Testing Complete!\n");
  console.log(`📊 Summary:`);
  console.log(`   ✅ Successful queries: ${successCount}/${queries.length}`);
  console.log(`   ❌ Failed queries: ${errorCount}/${queries.length}`);
  console.log(
    `   📈 Success rate: ${((successCount / queries.length) * 100).toFixed(1)}%`
  );

  if (errors.length > 0) {
    console.log("\n❌ Failed Queries:");
    errors.forEach((error, index) => {
      console.log(`\n${index + 1}. ${error.name}`);
      console.log(`   Error: ${error.error}`);
      console.log(`   SQL: ${error.sql}`);
    });

    console.log("\n💡 Next steps:");
    console.log("   1. Fix the SQL syntax errors above");
    console.log("   2. Re-run this test script");
    console.log("   3. Create dashboards when all queries pass");
  } else {
    console.log("\n🚀 All queries passed! Ready to create dashboards:");
    console.log("   pnpm run create-dashboards");
  }

  // Close ClickHouse connection
  await clickhouse.close();
}

main().catch((error) => {
  console.error("❌ Test script failed:", error.message);
  process.exit(1);
});

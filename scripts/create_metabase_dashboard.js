import fs from "fs/promises";
import path from "path";

const METABASE_URL = "http://localhost:3000";

// Parse SQL file to extract queries and their descriptions
function parseSQLFile(content, filename) {
  const queries = [];
  const lines = content.split("\n");

  let currentQuery = "";
  let currentDescription = "";
  let inQuery = false;

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i].trim();

    // Skip empty lines and section headers
    if (!line || line.startsWith("-- =====")) {
      continue;
    }

    // Comment line that describes a query
    if (
      line.startsWith("-- ") &&
      !line.includes("Change to your chain") &&
      !line.includes("Replace")
    ) {
      if (inQuery && currentQuery.trim()) {
        // Save previous query
        queries.push({
          name: currentDescription || "Unnamed Query",
          sql: currentQuery.trim(),
          description: currentDescription,
        });
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

      // End of query (semicolon)
      if (line.endsWith(";")) {
        queries.push({
          name: currentDescription || "Unnamed Query",
          sql: currentQuery.trim(),
          description: currentDescription,
        });
        currentQuery = "";
        currentDescription = "";
        inQuery = false;
      }
    }
  }

  // Handle last query if no semicolon
  if (currentQuery.trim()) {
    queries.push({
      name: currentDescription || "Unnamed Query",
      sql: currentQuery.trim(),
      description: currentDescription,
    });
  }

  return queries.filter((q) => q.sql && q.sql.length > 10); // Filter out tiny/empty queries
}

// Metabase API helper
class MetabaseAPI {
  constructor(url) {
    this.baseUrl = url;
    this.sessionToken = null;
  }

  async request(endpoint, options = {}) {
    const url = `${this.baseUrl}/api${endpoint}`;
    const headers = {
      "Content-Type": "application/json",
      ...options.headers,
    };

    if (this.sessionToken) {
      headers["X-Metabase-Session"] = this.sessionToken;
    }

    console.log(`🔍 API Request: ${options.method || "GET"} ${endpoint}`);

    // Debug payload for POST requests
    if (
      options.body &&
      (options.method === "POST" || options.method === "PUT")
    ) {
      try {
        const payload = JSON.parse(options.body);
        console.log(`📦 Payload keys: ${Object.keys(payload).join(", ")}`);
      } catch (e) {
        // Ignore parsing errors for debug
      }
    }

    const response = await fetch(url, {
      ...options,
      headers,
    });

    const responseText = await response.text();

    if (!response.ok) {
      console.log(`❌ API Error: ${response.status} ${response.statusText}`);
      console.log(`Response: ${responseText.substring(0, 200)}...`);
      throw new Error(
        `Metabase API error: ${response.status} ${response.statusText}\n${responseText}`
      );
    }

    try {
      return JSON.parse(responseText);
    } catch (error) {
      console.log(`📝 Raw response: ${responseText.substring(0, 200)}...`);
      return responseText;
    }
  }

  async checkSetup() {
    try {
      const response = await this.request("/session/properties");
      console.log("✅ Metabase is accessible");
      return response;
    } catch (error) {
      console.log("❌ Metabase setup check failed:", error.message);
      throw error;
    }
  }

  async login() {
    try {
      console.log("🔐 Attempting to authenticate with Metabase...");

      // First check if setup is complete
      await this.checkSetup();

      // Try default credentials
      const credentials = [
        { username: "admin@example.com", password: "TQPhcZvBXBz3k3" },
      ];

      for (const cred of credentials) {
        try {
          console.log(`  Trying: ${cred.username}`);

          const response = await this.request("/session", {
            method: "POST",
            body: JSON.stringify(cred),
          });

          this.sessionToken = response.id;
          console.log(`✅ Authenticated with Metabase as ${cred.username}`);
          return true;
        } catch (error) {
          console.log(`  ❌ Failed: ${cred.username}`);
        }
      }

      throw new Error("All authentication attempts failed");
    } catch (error) {
      console.log("\n❌ Authentication failed completely.");
      console.log("📋 Next steps:");
      console.log("  1. Visit http://localhost:3000");
      console.log("  2. Complete initial Metabase setup");
      console.log("  3. Note down your admin credentials");
      console.log("  4. Update the script with your credentials");
      return false;
    }
  }

  async getDatabases() {
    return this.request("/database");
  }

  async testQuery(sql, databaseId) {
    console.log("🧪 Testing query execution...");
    try {
      const response = await this.request("/dataset", {
        method: "POST",
        body: JSON.stringify({
          type: "native",
          native: {
            query: "SELECT 1 as test_column",
          },
          database: databaseId,
        }),
      });
      console.log("✅ Query execution works");
      return true;
    } catch (error) {
      console.log("❌ Query execution failed:", error.message);
      return false;
    }
  }

  async createQuestion(
    name,
    sql,
    databaseId,
    description = "",
    dashboardId = null
  ) {
    // Clean up SQL - remove comments and limit for dashboard display
    const cleanSql = sql
      .split("\n")
      .filter((line) => !line.trim().startsWith("--"))
      .join("\n")
      .trim();

    const limitedSql = cleanSql.includes("LIMIT")
      ? cleanSql
      : `${cleanSql} LIMIT 100`;

    const question = {
      name: name.substring(0, 100), // Limit name length
      description: description,
      display: "table",
      visualization_settings: {},
      dataset_query: {
        type: "native",
        native: {
          query: limitedSql,
        },
        database: databaseId,
      },
    };

    // If dashboardId is provided, create card directly on dashboard (Metabase v0.55.8 approach)
    if (dashboardId) {
      question.dashboard_id = dashboardId;
    }

    return this.request("/card", {
      method: "POST",
      body: JSON.stringify(question),
    });
  }

  async createDashboard(name, description = "") {
    return this.request("/dashboard", {
      method: "POST",
      body: JSON.stringify({
        name: name,
        description: description,
      }),
    });
  }

  async addCardToDashboard(dashboardId, cardId, options = {}) {
    console.log("  🔄 Adding card to dashboard...");

    try {
      // Use the correct Metabase v0.55.8 API: POST /api/dashboard/:dashboard_id/cards
      const payload = {
        cardId: cardId,
        row: options.row || 0,
        col: options.col || 0,
        sizeX: options.sizeX || 12,
        sizeY: options.sizeY || 8,
        parameter_mappings: [],
        visualization_settings: {},
      };

      const response = await this.request(`/dashboard/${dashboardId}/cards`, {
        method: "POST",
        body: JSON.stringify(payload),
      });

      console.log("    ✅ Card added successfully via POST /cards");
      return response;
    } catch (error) {
      console.log(`    ❌ POST /cards failed: ${error.message}`);
      throw error;
    }
  }
}

async function main() {
  console.log("🚀 Creating Metabase dashboards from SQL queries...\n");

  // Initialize Metabase API
  const mb = new MetabaseAPI(METABASE_URL);

  // Try to authenticate
  const authenticated = await mb.login();
  if (!authenticated) {
    process.exit(1);
  }

  // Find ClickHouse database
  console.log("\n🔍 Looking for ClickHouse database...");
  const databases = await mb.getDatabases();
  console.log(
    "Available databases:",
    databases.data?.map((db) => `${db.name} (${db.engine})`) || "No data field"
  );

  const clickhouseDB = databases.data?.find(
    (db) =>
      db.engine === "clickhouse" ||
      db.name.toLowerCase().includes("clickhouse") ||
      db.details?.host === "clickhouse"
  );

  if (!clickhouseDB) {
    console.log("\n❌ ClickHouse database not found in Metabase.");
    console.log("🔧 Setup steps:");
    console.log("  1. Visit http://localhost:3000/admin/databases/create");
    console.log('  2. Choose "ClickHouse"');
    console.log("  3. Host: clickhouse");
    console.log("  4. Port: 8123");
    console.log("  5. Database name: token_intelligence");
    console.log("  6. Username: default (leave empty)");
    console.log("  7. Password: (leave empty)");
    process.exit(1);
  }

  console.log(
    `✅ Found ClickHouse database: ${clickhouseDB.name} (ID: ${clickhouseDB.id})`
  );

  // Test database connection
  const queryWorks = await mb.testQuery("SELECT 1", clickhouseDB.id);
  if (!queryWorks) {
    console.log(
      "❌ Database connection test failed. Please check ClickHouse connection in Metabase."
    );
    process.exit(1);
  }

  // Read and parse SQL files
  const queryFiles = ["queries/dashboard_analytics.sql"];

  const allQueries = [];

  for (const filePath of queryFiles) {
    try {
      const content = await fs.readFile(filePath, "utf8");
      const queries = parseSQLFile(content, path.basename(filePath));

      console.log(
        `📄 Parsed ${path.basename(filePath)}: ${queries.length} queries found`
      );

      // Add file context to each query - single comprehensive dashboard
      queries.forEach((q) => {
        q.source_file = path.basename(filePath);
        q.category = "Token Intelligence Dashboard";
      });

      allQueries.push(...queries);
    } catch (error) {
      console.log(`⚠️  Could not read ${filePath}: ${error.message}`);
    }
  }

  if (allQueries.length === 0) {
    console.log("❌ No queries found to import");
    process.exit(1);
  }

  console.log(`\n📊 Total queries to import: ${allQueries.length}\n`);

  // Group queries by category for dashboard organization
  const categories = [...new Set(allQueries.map((q) => q.category))];

  for (const category of categories) {
    const categoryQueries = allQueries.filter((q) => q.category === category);

    console.log(
      `📈 Creating dashboard: "${category}" (${categoryQueries.length} queries)`
    );

    try {
      // Create dashboard
      const dashboard = await mb.createDashboard(
        `${category} Dashboard`,
        `Auto-generated dashboard containing ${categoryQueries.length} analytical queries`
      );

      let successCount = 0;
      let row = 0;

      // Create questions and add to dashboard (process all queries)
      console.log(`  Processing all ${categoryQueries.length} queries...`);

      for (const query of categoryQueries) {
        try {
          console.log(`  📝 Creating: ${query.name.substring(0, 50)}...`);

          // Method 1: Try creating card directly on dashboard (recommended for v0.55.8)
          try {
            const card = await mb.createQuestion(
              query.name,
              query.sql,
              clickhouseDB.id,
              query.description,
              dashboard.id // Include dashboard_id for direct creation
            );
            console.log("    ✅ Card created directly on dashboard");
            row += 8; // Next row (8 units down)
            successCount++;
          } catch (directError) {
            console.log(
              `    ⚠️  Direct creation failed, trying separate approach...`
            );

            // Method 2: Fallback - Create card separately then add to dashboard
            const card = await mb.createQuestion(
              query.name,
              query.sql,
              clickhouseDB.id,
              query.description
            );

            // Add to dashboard with row positioning
            await mb.addCardToDashboard(dashboard.id, card.id, {
              row: row,
              col: 0,
              sizeX: 12,
              sizeY: 8,
            });

            row += 8; // Next row (8 units down)
            successCount++;
          }
        } catch (error) {
          console.log(`    ❌ Failed: ${error.message.split("\n")[0]}`);
        }
      }

      console.log(
        `✅ Dashboard "${category}" created with ${successCount}/${categoryQueries.length} queries: http://localhost:3000/dashboard/${dashboard.id}\n`
      );
    } catch (error) {
      console.log(
        `❌ Failed to create dashboard "${category}": ${error.message}`
      );
    }
  }

  console.log("🎉 Dashboard creation complete!");
  console.log("\n💡 Next steps:");
  console.log("   1. Visit http://localhost:3000 to view your dashboards");
  console.log("   2. Customize visualizations (change from table to charts)");
  console.log("   3. Add filters and parameters");
  console.log("   4. Organize dashboard layouts");
}

main().catch((error) => {
  console.error("❌ Script failed:", error.message);
  process.exit(1);
});

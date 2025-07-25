import fs from "fs/promises";

const METABASE_URL = "http://localhost:3000";

// Metabase API helper (simplified version for cleanup)
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

    const response = await fetch(url, {
      ...options,
      headers,
    });

    const responseText = await response.text();

    if (!response.ok) {
      console.log(`❌ API Error: ${response.status} ${response.statusText}`);
      console.log(`Response: ${responseText}`);
      throw new Error(
        `Metabase API error: ${response.status} ${response.statusText}\n${responseText}`
      );
    }

    try {
      return JSON.parse(responseText);
    } catch (error) {
      return responseText;
    }
  }

  async login() {
    try {
      console.log("🔐 Attempting to authenticate with Metabase...");

      // Try the credentials that worked before
      const credentials = [
        { username: "admin@example.com", password: "TQPhcZvBXBz3k3" },
        { username: "admin@example.com", password: "admin123" },
        { username: "admin", password: "admin" },
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
      console.log(
        "\n❌ Authentication failed. Please check Metabase credentials."
      );
      return false;
    }
  }

  async getAllDashboards() {
    return this.request("/dashboard");
  }

  async getAllQuestions() {
    return this.request("/card");
  }

  async deleteDashboard(dashboardId) {
    return this.request(`/dashboard/${dashboardId}`, {
      method: "DELETE",
    });
  }

  async deleteQuestion(questionId) {
    return this.request(`/card/${questionId}`, {
      method: "DELETE",
    });
  }
}

async function main() {
  console.log(
    "🧹 Cleaning up auto-generated Metabase dashboards and questions...\n"
  );

  // Initialize Metabase API
  const mb = new MetabaseAPI(METABASE_URL);

  // Try to authenticate
  const authenticated = await mb.login();
  if (!authenticated) {
    process.exit(1);
  }

  console.log("\n🔍 Finding auto-generated content...");

  // Get all dashboards
  const dashboardsResponse = await mb.getAllDashboards();
  const allDashboards = dashboardsResponse.data || dashboardsResponse;

  // Get all questions/cards
  const questionsResponse = await mb.getAllQuestions();
  const allQuestions = questionsResponse.data || questionsResponse;

  console.log(`📊 Found ${allDashboards.length} total dashboards`);
  console.log(`📝 Found ${allQuestions.length} total questions`);

  // Identify auto-generated dashboards (by name patterns)
  const autoGeneratedDashboards = allDashboards.filter(
    (dashboard) =>
      dashboard.name.includes("Token Intelligence Dashboard") ||
      dashboard.name.includes("Multi-Chain Analytics") ||
      dashboard.name.includes("Legacy Analytics") ||
      (dashboard.name.includes("Dashboard") &&
        (dashboard.description?.includes("Auto-generated") ||
          dashboard.description?.includes("analytical queries")))
  );

  // Identify auto-generated questions (by common patterns from our SQL files)
  const autoGeneratedQuestions = allQuestions.filter((question) => {
    const name = question.name.toLowerCase();
    return (
      // From our consolidated dashboard analytics queries
      name.includes("ecosystem") ||
      name.includes("chain ecosystem") ||
      name.includes("most active tokens") ||
      name.includes("daily activity") ||
      name.includes("hourly activity") ||
      name.includes("recent activity") ||
      name.includes("trending tokens") ||
      name.includes("volume leaders") ||
      name.includes("transfer size") ||
      name.includes("whale transactions") ||
      name.includes("token holders") ||
      name.includes("user portfolio") ||
      name.includes("growth") ||
      name.includes("cross-chain") ||
      name.includes("concentration") ||
      name.includes("airdrops") ||
      name.includes("minting") ||
      name.includes("velocity") ||
      name.includes("metadata") ||
      // Legacy patterns from old queries
      name.includes("recent activity") ||
      name.includes("last 7 days") ||
      name.includes("partition") ||
      name.includes("log_index") ||
      name.includes("token metadata") ||
      name.includes("critical:") ||
      name.includes("most active") ||
      name.includes("token transfer") ||
      name.includes("holder balance") ||
      name.includes("whale transaction") ||
      name.includes("growth by month") ||
      // Generic patterns that suggest auto-generation
      (question.dataset_query?.native?.query?.includes("erc20_transfers") &&
        question.creator_id &&
        new Date(question.created_at) >
          new Date(Date.now() - 24 * 60 * 60 * 1000)) // Created in last 24 hours
    );
  });

  console.log(`\n🎯 Auto-generated content found:`);
  console.log(`📊 Dashboards to delete: ${autoGeneratedDashboards.length}`);
  autoGeneratedDashboards.forEach((d) =>
    console.log(`   - "${d.name}" (ID: ${d.id})`)
  );

  console.log(`📝 Questions to delete: ${autoGeneratedQuestions.length}`);
  autoGeneratedQuestions.forEach((q) =>
    console.log(`   - "${q.name}" (ID: ${q.id})`)
  );

  if (
    autoGeneratedDashboards.length === 0 &&
    autoGeneratedQuestions.length === 0
  ) {
    console.log("\n✅ No auto-generated content found to delete.");
    process.exit(0);
  }

  console.log("\n⚠️  This will permanently delete the above content.");
  console.log("🚀 Starting cleanup in 3 seconds... (Ctrl+C to cancel)");

  // Wait 3 seconds for user to cancel if needed
  await new Promise((resolve) => setTimeout(resolve, 3000));

  console.log("\n🧹 Starting cleanup...");

  let deletedDashboards = 0;
  let deletedQuestions = 0;
  let errors = [];

  // Delete dashboards first (they might reference questions)
  console.log("\n📊 Deleting dashboards...");
  for (const dashboard of autoGeneratedDashboards) {
    try {
      console.log(`  🗑️  Deleting dashboard: "${dashboard.name}"`);
      await mb.deleteDashboard(dashboard.id);
      deletedDashboards++;
    } catch (error) {
      console.log(
        `    ❌ Failed to delete dashboard "${dashboard.name}": ${error.message}`
      );
      errors.push(`Dashboard "${dashboard.name}": ${error.message}`);
    }
  }

  // Delete questions
  console.log("\n📝 Deleting questions...");
  for (const question of autoGeneratedQuestions) {
    try {
      console.log(`  🗑️  Deleting question: "${question.name}"`);
      await mb.deleteQuestion(question.id);
      deletedQuestions++;
    } catch (error) {
      console.log(
        `    ❌ Failed to delete question "${question.name}": ${error.message}`
      );
      errors.push(`Question "${question.name}": ${error.message}`);
    }
  }

  // Summary
  console.log("\n🎉 Cleanup complete!");
  console.log(`\n📋 Summary:`);
  console.log(
    `   ✅ Dashboards deleted: ${deletedDashboards}/${autoGeneratedDashboards.length}`
  );
  console.log(
    `   ✅ Questions deleted: ${deletedQuestions}/${autoGeneratedQuestions.length}`
  );

  if (errors.length > 0) {
    console.log(`   ❌ Errors encountered: ${errors.length}`);
    errors.forEach((error) => console.log(`      - ${error}`));
  }

  console.log("\n💡 Next steps:");
  console.log(
    "   1. Update your SQL queries with correct table names (e.g., erc20_transfers_130)"
  );
  console.log("   2. Run: pnpm run create-dashboards");
  console.log("   3. Enjoy your updated dashboards!");
}

main().catch((error) => {
  console.error("❌ Cleanup script failed:", error.message);
  process.exit(1);
});

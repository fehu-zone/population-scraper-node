import { Client } from "@elastic/elasticsearch";

// En basit client
const client = new Client({
  node: "http://localhost:9200"
});

async function test() {
  try {
    console.log("🔗 Testing Elasticsearch connection...");
    
    // 1. HTTP fetch ile test
    console.log("1. HTTP test...");
    const response = await fetch('http://localhost:9200');
    console.log("HTTP Status:", response.status);
    const data = await response.json();
    console.log("Elasticsearch Info:", data.version.number);
    
    // 2. Client ping
    console.log("2. Client ping test...");
    const pingResult = await client.ping();
    console.log("Ping result:", pingResult);
    
    // 3. Client info
    console.log("3. Client info test...");
    const info = await client.info();
    console.log("Cluster:", info.cluster_name);
    
    console.log("✅ All tests passed!");
    
  } catch (error) {
    console.error("❌ Test failed:", error.message);
    console.error("Error details:", error);
  }
}

test();
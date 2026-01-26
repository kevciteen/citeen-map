import pg from "pg";
import "dotenv/config";

const { Client } = pg;

const client = new Client({ connectionString: process.env.DATABASE_URL });

try {
  await client.connect();
  const r = await client.query("SELECT current_user, current_database();");
  console.log("OK:", r.rows[0]);
  await client.end();
} catch (e) {
  console.error("DB FAIL:", e.message);
  process.exit(1);
}

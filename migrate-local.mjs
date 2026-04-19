// Migrate data from LOCAL filesystem → Vercel Blob
import { put } from "@vercel/blob";
import fs from "fs/promises";
import path from "path";

async function upload(key, body, ct = "application/json") {
  await put(key, body, { access: "public", contentType: ct, addRandomSuffix: false, allowOverwrite: true });
  console.log(`  ↑ ${key}`);
}

async function migrateJson(localPath, remoteKey) {
  try {
    const body = await fs.readFile(localPath, "utf-8");
    await upload(remoteKey, body);
  } catch (e) { console.log(`  skipped ${localPath}: ${e.message}`); }
}

async function migrateDir(localDir, remotePrefix) {
  try {
    const files = await fs.readdir(localDir);
    for (const f of files.filter(f => f.endsWith(".json"))) {
      await migrateJson(path.join(localDir, f), `${remotePrefix}/${f}`);
    }
  } catch (e) { console.log(`  skipped ${localDir}: ${e.message}`); }
}

console.log("📁 Saved groups...");
await migrateDir("saved-groups", "saved-groups");

console.log("\n🏷️ Brands...");
await migrateJson("brands.json", "brands.json");

console.log("\n📜 Brand logs...");
await migrateDir("brand-logs", "brand-logs");

console.log("\n📝 Local contacts...");
await migrateJson("local-contacts.json", "local-contacts.json");

console.log("\n⚙️ IG config...");
await migrateJson("ig-config.json", "ig-config.json");

console.log("\n✅ Migration complete");

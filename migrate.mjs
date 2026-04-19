// Migrate data from Railway → Vercel Blob
import { put } from "@vercel/blob";

const SRC = "https://mayo2-production.up.railway.app";
const SRC_AUTH = "Basic " + Buffer.from("oren:WhatsApp2026!").toString("base64");

async function srcJson(path) {
  const r = await fetch(SRC + path, { headers: { Authorization: SRC_AUTH } });
  if (!r.ok) throw new Error(`${path}: ${r.status}`);
  return r.json();
}

async function upload(key, data) {
  const body = typeof data === "string" ? data : JSON.stringify(data, null, 2);
  await put(key, body, {
    access: "public",
    contentType: "application/json",
    addRandomSuffix: false,
    allowOverwrite: true,
  });
  console.log(`  ↑ ${key}`);
}

// 1. Saved groups
console.log("📁 Saved groups...");
const { groups } = await srcJson("/api/saved-groups");
for (const g of groups) {
  try {
    const full = await srcJson(`/api/saved-groups/${g.file}`);
    await upload(`saved-groups/${g.file}`, full);
  } catch (e) { console.error(`  ✗ ${g.file}:`, e.message); }
}

// 2. Brands — direct API
console.log("🏷️ Brands...");
try {
  const { brands } = await srcJson("/api/brands");
  // Strip computed fields
  const clean = brands.map(b => ({
    id: b.id, name: b.name, color: b.color, groupIds: b.groupIds, createdAt: b.createdAt,
  }));
  await upload("brands.json", clean);

  // Brand logs
  for (const b of brands) {
    try {
      const { log } = await srcJson(`/api/brands/${b.id}/log`);
      if (log?.length) await upload(`brand-logs/${b.id}.json`, log);
    } catch {}
  }
} catch (e) { console.error("  brands failed:", e.message); }

// 3. Local contacts — try both formats
console.log("📝 Local contacts...");
try {
  // Try direct endpoint (if exists)
  const r = await fetch(SRC + "/api/local-contacts", { headers: { Authorization: SRC_AUTH } });
  if (r.ok) {
    const { contacts } = await r.json();
    if (contacts && Object.keys(contacts).length) {
      await upload("local-contacts.json", contacts);
    } else {
      console.log("  (empty)");
    }
  } else {
    console.log("  skipped");
  }
} catch (e) { console.error("  contacts:", e.message); }

console.log("\n✅ Migration complete");

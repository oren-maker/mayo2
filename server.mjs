import express from "express";
import path from "path";
import fs from "fs/promises";
import { fileURLToPath } from "url";
import ExcelJS from "exceljs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
// DATA_DIR for persistent storage on Railway volume (defaults to __dirname for local)
const DATA_DIR = process.env.DATA_DIR || __dirname;
await fs.mkdir(DATA_DIR, { recursive: true }).catch(() => {});

const app = express();
app.use(express.json({ limit: "2mb" }));

// ========== BASIC AUTH ==========
const AUTH_USER = process.env.AUTH_USER || "oren";
const AUTH_PASS = process.env.AUTH_PASS || "WhatsApp2026!";
app.use((req, res, next) => {
  const auth = req.headers.authorization;
  if (!auth || !auth.startsWith("Basic ")) {
    res.set("WWW-Authenticate", 'Basic realm="WhatsApp Groups", charset="UTF-8"');
    return res.status(401).send("Authentication required");
  }
  try {
    const decoded = Buffer.from(auth.slice(6), "base64").toString("utf8");
    const [u, p] = decoded.split(":");
    if (u === AUTH_USER && p === AUTH_PASS) return next();
  } catch {}
  res.set("WWW-Authenticate", 'Basic realm="WhatsApp Groups"');
  res.status(401).send("Invalid credentials");
});
app.use("/exports", express.static(path.join(DATA_DIR, "exports")));
app.use("/avatars", express.static(path.join(DATA_DIR, "avatars")));

// ========== WaSender API Client (from eng-tours) ==========
const BASE = process.env.WASENDER_API_URL || "https://wasenderapi.com/api";
const KEY = process.env.WASENDER_API_KEY || "";

function authHeaders(sessionKey) {
  return {
    Authorization: `Bearer ${sessionKey || KEY}`,
    "Content-Type": "application/json",
    Accept: "application/json",
  };
}

async function wa(method, path, body, sessionKey) {
  if (!KEY && !sessionKey) return { ok: false, error: "WASENDER_API_KEY not configured" };
  try {
    const res = await fetch(`${BASE}${path}`, {
      method,
      headers: authHeaders(sessionKey),
      body: body ? JSON.stringify(body) : undefined,
      cache: "no-store",
    });
    const text = await res.text();
    let data;
    try { data = JSON.parse(text); } catch { data = text; }
    if (!res.ok) return { ok: false, status: res.status, error: data?.message || data?.error || `HTTP ${res.status}` };
    return { ok: true, data };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

// ========== SESSION MANAGEMENT ==========
app.get("/api/sessions", async (_, res) => {
  const r = await wa("GET", "/whatsapp-sessions");
  if (!r.ok) return res.status(500).json(r);
  const sessions = Array.isArray(r.data) ? r.data : r.data?.data || [];
  res.json({ sessions });
});

app.post("/api/sessions", async (req, res) => {
  const { name, phone_number } = req.body;
  const r = await wa("POST", "/whatsapp-sessions", { name, phone_number });
  res.json(r);
});

app.post("/api/sessions/:id/connect", async (req, res) => {
  const r = await wa("POST", `/whatsapp-sessions/${req.params.id}/connect`);
  res.json(r);
});

app.post("/api/sessions/:id/disconnect", async (req, res) => {
  const r = await wa("POST", `/whatsapp-sessions/${req.params.id}/disconnect`);
  res.json(r);
});

app.get("/api/sessions/:id/qr", async (req, res) => {
  const r = await wa("GET", `/whatsapp-sessions/${req.params.id}/qrcode`);
  res.json(r);
});

app.post("/api/sessions/:id/qr", async (req, res) => {
  const r = await wa("POST", `/whatsapp-sessions/${req.params.id}/regenerate-qrcode`);
  res.json(r);
});

app.get("/api/sessions/:id/status", async (req, res) => {
  const r = await wa("GET", `/whatsapp-sessions/${req.params.id}/status`);
  res.json(r);
});

app.delete("/api/sessions/:id", async (req, res) => {
  const r = await wa("DELETE", `/whatsapp-sessions/${req.params.id}`);
  res.json(r);
});

// ========== HEALTH CHECK (from eng-tours) ==========
app.get("/api/health", async (_, res) => {
  if (!KEY) return res.json({ online: false, error: "API key not configured" });
  const r = await wa("GET", "/whatsapp-sessions");
  if (!r.ok) return res.json({ online: false, error: r.error });
  const sessions = Array.isArray(r.data) ? r.data : r.data?.data || [];
  const connected = sessions.filter((s) => {
    const st = (s.status || "").toLowerCase();
    return st === "connected" || st === "ready";
  });
  res.json({
    online: connected.length > 0,
    total_sessions: sessions.length,
    connected_sessions: connected.length,
    sessions: sessions.map((s) => ({ id: s.id, name: s.name, status: s.status, phone: s.phone_number })),
    checked_at: new Date().toISOString(),
  });
});

// ========== GROUP ENDPOINTS ==========

// List all groups from a specific session (uses per-session API key)
app.get("/api/sessions/:id/groups", async (req, res) => {
  const sessionR = await wa("GET", `/whatsapp-sessions/${req.params.id}`);
  if (!sessionR.ok) return res.status(500).json(sessionR);
  const sessionKey = sessionR.data?.data?.api_key || sessionR.data?.api_key;
  if (!sessionKey) return res.status(500).json({ error: "session API key not found" });

  const r = await wa("GET", "/groups", null, sessionKey);
  if (!r.ok) return res.status(500).json(r);
  let groups = Array.isArray(r.data) ? r.data : r.data?.data || r.data?.groups || [];

  // Enrich with member count via metadata — with retry on rate limit
  const CONCURRENCY = 3;
  let i = 0;
  const enriched = [...groups];
  await Promise.all(
    Array.from({ length: CONCURRENCY }, async () => {
      while (i < enriched.length) {
        const idx = i++;
        const g = enriched[idx];
        const gid = g.id || g.jid || g.groupId;
        if (!gid) continue;
        for (let attempt = 0; attempt < 2; attempt++) {
          try {
            const m = await wa("GET", `/groups/${gid}/metadata`, null, sessionKey);
            if (m.ok) {
              const md = m.data?.data || m.data;
              const size = md?.size || md?.participants?.length;
              enriched[idx] = {
                ...g,
                size,
                desc: md?.desc || md?.description,
                subject: md?.subject || g.name,
                creation: md?.creation,
              };
              break;
            } else if (m.status === 429) {
              await new Promise(r => setTimeout(r, 2000));
              continue;
            } else break;
          } catch {
            await new Promise(r => setTimeout(r, 500));
          }
        }
      }
    })
  );

  // Check saved-groups status for each
  const savedFiles = (await fs.readdir(GROUPS_DIR).catch(() => [])).filter(f => f.endsWith(".json"));
  const savedMap = new Map();
  for (const f of savedFiles) {
    try {
      const data = JSON.parse(await fs.readFile(path.join(GROUPS_DIR, f), "utf-8"));
      savedMap.set(data.groupId, { file: f, memberCount: data.memberCount, savedAt: data.savedAt });
    } catch {}
  }
  const withStatus = enriched.map(g => {
    const gid = g.id || g.jid || g.groupId;
    const saved = savedMap.get(gid);
    return {
      ...g,
      saved: !!saved,
      savedMemberCount: saved?.memberCount || null,
      savedAt: saved?.savedAt || null,
      savedFile: saved?.file || null,
      delta: saved ? (g.size || 0) - (saved.memberCount || 0) : null,
    };
  });

  res.json({ groups: withStatus, sessionKey });
});

// Save delta — only new members (dedup against existing saved group)
app.post("/api/save-group-delta", async (req, res) => {
  const { groupId, groupName, members, metadata } = req.body;
  if (!groupId || !members) return res.status(400).json({ error: "groupId + members required" });
  const safeId = groupId.replace(/[^a-zA-Z0-9]/g, "_");
  const fp = path.join(GROUPS_DIR, `${safeId}.json`);

  let existing = null;
  try { existing = JSON.parse(await fs.readFile(fp, "utf-8")); } catch {}

  const existingPhones = new Set((existing?.members || []).map(m =>
    m._phone || (m.pn || m.jid || m.id || "").replace(/@.*/, "")
  ));
  const newMembers = members.filter(m => {
    const ph = m._phone || (m.pn || m.jid || m.id || "").replace(/@.*/, "");
    return ph && !existingPhones.has(ph);
  });

  // For brand log — replace the full current set (drop ex-members)
  // Leavers = in existing but not in current members
  const currentPhones = new Set(members.map(m => m._phone || (m.pn || m.jid || m.id || "").replace(/@.*/, "")));
  const prevPhones = new Set((existing?.members || []).map(m => m._phone || (m.pn || m.jid || m.id || "").replace(/@.*/, "")));
  const leaverPhones = [...prevPhones].filter(p => !currentPhones.has(p));

  // We preserve leavers in the saved file (don't delete — keep for history) but track delta
  const merged = [...(existing?.members || []), ...newMembers];
  const payload = {
    groupId,
    groupName,
    metadata: metadata || existing?.metadata || {},
    memberCount: merged.length,
    currentMemberCount: members.length,
    savedAt: new Date().toISOString(),
    previousSavedAt: existing?.savedAt || null,
    leaverPhones,
    members: merged,
  };
  await fs.writeFile(fp, JSON.stringify(payload, null, 2));

  // Emit brand log entries
  await emitMemberChangesToBrands(groupId, groupName, existing?.members || [], members);

  res.json({
    ok: true,
    file: `${safeId}.json`,
    total: merged.length,
    new_added: newMembers.length,
    already_existed: members.length - newMembers.length,
    previous_count: existing?.memberCount || 0,
    leavers: leaverPhones.length,
  });
});

// Get group metadata (name, description, photo, creation date)
app.get("/api/sessions/:id/groups/:groupId", async (req, res) => {
  const r = await wa("GET", `/whatsapp-sessions/${req.params.id}/groups/${req.params.groupId}`);
  res.json(r);
});

// Get group participants/members
app.get("/api/sessions/:id/groups/:groupId/participants", async (req, res) => {
  const sessionApiKey = req.query.sessionKey || null;
  const r = await wa("GET", `/groups/${req.params.groupId}/participants`, null, sessionApiKey);
  if (!r.ok) {
    // fallback to session-scoped path
    const r2 = await wa("GET", `/whatsapp-sessions/${req.params.id}/groups/${req.params.groupId}/participants`);
    if (!r2.ok) return res.status(500).json(r2);
    const participants = Array.isArray(r2.data) ? r2.data : r2.data?.data || r2.data?.participants || [];
    return res.json({ participants });
  }
  const participants = Array.isArray(r.data) ? r.data : r.data?.data || [];
  res.json({ participants });
});

// Get group metadata + picture
app.get("/api/sessions/:id/groups/:groupId/full", async (req, res) => {
  const sessionApiKey = req.query.sessionKey || null;
  const [metaR, picR] = await Promise.all([
    wa("GET", `/groups/${req.params.groupId}/metadata`, null, sessionApiKey),
    wa("GET", `/groups/${req.params.groupId}/picture`, null, sessionApiKey),
  ]);
  res.json({
    metadata: metaR.ok ? (metaR.data?.data || metaR.data) : null,
    picture: picR.ok ? (picR.data?.data?.imgUrl || picR.data?.imgUrl) : null,
  });
});

// ========== GROUP MANAGEMENT ACTIONS ==========

// Leave a group
app.post("/api/groups/:id/leave", async (req, res) => {
  const { sessionKey } = req.body;
  const r = await wa("POST", `/groups/${req.params.id}/leave`, null, sessionKey);
  res.json(r);
});

// Get invite link
app.get("/api/groups/:id/invite-link", async (req, res) => {
  const sessionKey = req.query.sessionKey || null;
  const r = await wa("GET", `/groups/${req.params.id}/invite-link`, null, sessionKey);
  res.json(r);
});

// Reset invite link
app.post("/api/groups/:id/invite-link/reset", async (req, res) => {
  const { sessionKey } = req.body;
  const r = await wa("POST", `/groups/${req.params.id}/invite-link/reset`, null, sessionKey);
  res.json(r);
});

// Add participants (requires admin on the group)
app.post("/api/groups/:id/participants/add", async (req, res) => {
  const { sessionKey, participants } = req.body;
  const r = await wa("POST", `/groups/${req.params.id}/participants/add`, { participants }, sessionKey);
  res.json(r);
});

// Remove participants
app.post("/api/groups/:id/participants/remove", async (req, res) => {
  const { sessionKey, participants } = req.body;
  const r = await wa("POST", `/groups/${req.params.id}/participants/remove`, { participants }, sessionKey);
  res.json(r);
});

// Promote/demote (action: "promote" or "demote")
app.put("/api/groups/:id/participants/update", async (req, res) => {
  const { sessionKey, participants, action } = req.body;
  const r = await wa("PUT", `/groups/${req.params.id}/participants/update`, { participants, action }, sessionKey);
  res.json(r);
});

// Update group settings (name, description)
app.put("/api/groups/:id/settings", async (req, res) => {
  const { sessionKey, subject, description } = req.body;
  const payload = {};
  if (subject) payload.subject = subject;
  if (description !== undefined) payload.description = description;
  const r = await wa("PUT", `/groups/${req.params.id}/settings`, payload, sessionKey);
  res.json(r);
});

// Create a new group
app.post("/api/groups/create", async (req, res) => {
  const { sessionKey, subject, participants } = req.body;
  const r = await wa("POST", "/groups", { subject, participants }, sessionKey);
  res.json(r);
});

// Lookup group by invite code
app.get("/api/groups/invite/:code", async (req, res) => {
  const sessionKey = req.query.sessionKey || null;
  const r = await wa("GET", `/groups/invite/${req.params.code}`, null, sessionKey);
  res.json(r);
});

// Accept group invite
app.post("/api/groups/invite/accept", async (req, res) => {
  const { sessionKey, inviteCode } = req.body;
  const r = await wa("POST", "/groups/invite/accept", { inviteCode }, sessionKey);
  res.json(r);
});

// ========== CHECK PHONE VALIDITY (batch) ==========
const validityJobs = new Map();

app.post("/api/check-validity", async (req, res) => {
  const { sessionKey, phones } = req.body;
  if (!phones?.length) return res.status(400).json({ error: "phones[] required" });
  const jobId = `val_${Date.now()}`;
  validityJobs.set(jobId, { id: jobId, status: "running", total: phones.length, done: 0, active: 0, inactive: 0, errors: 0, results: [] });
  res.json({ id: jobId });

  (async () => {
    const job = validityJobs.get(jobId);
    const CONCURRENCY = 10;
    let i = 0;
    await Promise.all(
      Array.from({ length: CONCURRENCY }, async () => {
        while (i < phones.length) {
          const p = phones[i++];
          const cleanPhone = String(p).replace(/\D/g, "");
          if (!cleanPhone) { job.errors++; job.done++; continue; }
          try {
            const r = await wa("GET", `/on-whatsapp/${cleanPhone}`, null, sessionKey);
            const exists = r.ok && (r.data?.data?.exists === true);
            job.results.push({ phone: cleanPhone, exists, error: r.ok ? null : r.error });
            if (exists) job.active++; else job.inactive++;
          } catch (e) {
            job.errors++;
            job.results.push({ phone: cleanPhone, exists: null, error: e.message });
          }
          job.done++;
        }
      })
    );
    job.status = "done";
    console.log(`[${jobId}] ✅ checked ${job.done} — active: ${job.active}, inactive: ${job.inactive}`);
  })();
});

app.get("/api/check-validity-status/:id", (req, res) => {
  const j = validityJobs.get(req.params.id);
  if (!j) return res.status(404).json({ error: "not found" });
  res.json(j);
});

// ========== CONTACT ACTIONS (single) ==========
app.post("/api/contacts/:phone/block", async (req, res) => {
  const { sessionKey } = req.body;
  const r = await wa("POST", `/contacts/${req.params.phone}/block`, null, sessionKey);
  res.json(r);
});

app.post("/api/contacts/:phone/unblock", async (req, res) => {
  const { sessionKey } = req.body;
  const r = await wa("POST", `/contacts/${req.params.phone}/unblock`, null, sessionKey);
  res.json(r);
});

// Enrich members with contact details + picture (concurrent)
const enrichJobs = new Map();

app.post("/api/enrich-members", async (req, res) => {
  const { sessionKey, phones, groupName, autoLabel } = req.body;
  if (!phones?.length) return res.status(400).json({ error: "phones[] required" });
  const jobId = `enr_${Date.now()}`;
  enrichJobs.set(jobId, { id: jobId, status: "running", total: phones.length, done: 0, results: [], error: null });
  res.json({ id: jobId });

  (async () => {
    const job = enrichJobs.get(jobId);

    // Step 1: Fetch ALL contacts once → build phone→name map
    let contactMap = new Map();
    try {
      const cR = await wa("GET", "/contacts", null, sessionKey);
      if (cR.ok) {
        const contacts = Array.isArray(cR.data) ? cR.data : cR.data?.data || [];
        for (const c of contacts) {
          const phone = (c.id || c.jid || "").replace("@s.whatsapp.net", "").replace("@c.us", "");
          if (phone) contactMap.set(phone, {
            name: c.name || c.notify || c.verifiedName,
            verifiedName: c.verifiedName,
            imgUrl: c.imgUrl,
            status: c.status,
          });
        }
        console.log(`[${jobId}] loaded ${contactMap.size} contacts`);
      }
    } catch (e) {
      console.error(`[${jobId}] contacts fetch failed:`, e.message);
    }

    // Step 2: For each phone, use contact data or fetch picture individually
    // Also merge in LOCAL_CONTACTS (user's own name overrides)
    const CONCURRENCY = 10;
    let i = 0;
    await Promise.all(
      Array.from({ length: CONCURRENCY }, async () => {
        while (i < phones.length) {
          const p = phones[i++];
          const contact = contactMap.get(p);
          let imgUrl = contact?.imgUrl || null;
          if (!imgUrl) {
            try {
              const picR = await wa("GET", `/contacts/${p}/picture`, null, sessionKey);
              if (picR.ok) imgUrl = picR.data?.data?.imgUrl || picR.data?.imgUrl || null;
            } catch {}
          }
          let localName = LOCAL_CONTACTS[p] || null;
          let name = localName || contact?.name || contact?.verifiedName || null;
          let source = localName ? "local" : (contact?.name ? "whatsapp" : "none");

          // Auto-label if missing AND autoLabel flag on
          if (!name && autoLabel && groupName) {
            const idx = phones.indexOf(p) + 1;
            name = `${groupName} - חבר ${idx}`;
            LOCAL_CONTACTS[p] = name;
            source = "auto";
            localName = name;
          }

          job.results.push({
            phone: p,
            imgUrl,
            name,
            verifiedName: contact?.verifiedName || null,
            status: contact?.status || null,
            local_name: localName,
            source,
          });
          job.done++;
        }
      })
    );

    // Persist auto-labels
    if (autoLabel) await saveLocalContacts();
    job.status = "done";
    console.log(`[${jobId}] ✅ enriched ${job.done} members`);
  })();
});

// ========== SAVE / LOAD GROUPS ==========
const GROUPS_DIR = path.join(DATA_DIR, "saved-groups");
await fs.mkdir(GROUPS_DIR, { recursive: true });

// ========== BRANDS ==========
const BRANDS_FILE = path.join(DATA_DIR, "brands.json");
const BRAND_LOGS_DIR = path.join(DATA_DIR, "brand-logs");
await fs.mkdir(BRAND_LOGS_DIR, { recursive: true });
let BRANDS = [];
try { BRANDS = JSON.parse(await fs.readFile(BRANDS_FILE, "utf-8")); } catch {}
async function saveBrands() { await fs.writeFile(BRANDS_FILE, JSON.stringify(BRANDS, null, 2)); }

async function appendBrandLog(brandId, entry) {
  const fp = path.join(BRAND_LOGS_DIR, `${brandId}.json`);
  let log = [];
  try { log = JSON.parse(await fs.readFile(fp, "utf-8")); } catch {}
  log.unshift({ at: new Date().toISOString(), ...entry });
  await fs.writeFile(fp, JSON.stringify(log.slice(0, 500), null, 2));
}

function loadSavedGroupsMap() {
  return fs.readdir(GROUPS_DIR).then(async files => {
    const map = new Map();
    for (const f of files.filter(f => f.endsWith(".json"))) {
      try {
        const d = JSON.parse(await fs.readFile(path.join(GROUPS_DIR, f), "utf-8"));
        map.set(d.groupId, { ...d, file: f });
      } catch {}
    }
    return map;
  }).catch(() => new Map());
}

app.get("/api/brands", async (_, res) => {
  const savedMap = await loadSavedGroupsMap();
  const brands = BRANDS.map(b => ({
    ...b,
    group_count: b.groupIds.length,
    total_members: b.groupIds.reduce((a, gid) => a + (savedMap.get(gid)?.memberCount || 0), 0),
  }));
  res.json({ brands });
});

app.post("/api/brands", async (req, res) => {
  const { name, color } = req.body;
  if (!name) return res.status(400).json({ error: "name required" });
  const id = `brand_${Date.now()}`;
  const brand = { id, name, color: color || "#25d366", groupIds: [], createdAt: new Date().toISOString() };
  BRANDS.push(brand);
  await saveBrands();
  await appendBrandLog(id, { type: "brand_created", name });
  res.json(brand);
});

app.put("/api/brands/:id", async (req, res) => {
  const b = BRANDS.find(x => x.id === req.params.id);
  if (!b) return res.status(404).json({ error: "not found" });
  if (req.body.name) b.name = req.body.name;
  if (req.body.color) b.color = req.body.color;
  await saveBrands();
  await appendBrandLog(b.id, { type: "brand_updated", changes: req.body });
  res.json(b);
});

app.delete("/api/brands/:id", async (req, res) => {
  const idx = BRANDS.findIndex(x => x.id === req.params.id);
  if (idx < 0) return res.status(404).json({ error: "not found" });
  BRANDS.splice(idx, 1);
  await saveBrands();
  res.json({ ok: true });
});

app.post("/api/brands/:id/groups", async (req, res) => {
  const b = BRANDS.find(x => x.id === req.params.id);
  if (!b) return res.status(404).json({ error: "not found" });
  const { groupId, groupName } = req.body;
  if (!groupId) return res.status(400).json({ error: "groupId required" });
  if (!b.groupIds.includes(groupId)) {
    b.groupIds.push(groupId);
    await saveBrands();
    await appendBrandLog(b.id, { type: "group_added", groupId, groupName });
  }
  res.json(b);
});

app.delete("/api/brands/:id/groups/:gid", async (req, res) => {
  const b = BRANDS.find(x => x.id === req.params.id);
  if (!b) return res.status(404).json({ error: "not found" });
  const gid = decodeURIComponent(req.params.gid);
  b.groupIds = b.groupIds.filter(x => x !== gid);
  await saveBrands();
  await appendBrandLog(b.id, { type: "group_removed", groupId: gid });
  res.json(b);
});

app.get("/api/brands/:id/stats", async (req, res) => {
  const b = BRANDS.find(x => x.id === req.params.id);
  if (!b) return res.status(404).json({ error: "not found" });
  const savedMap = await loadSavedGroupsMap();

  const phoneGroups = new Map(); // phone → Set of groupIds
  const groupDetails = [];
  let latestSavedAt = null;

  for (const gid of b.groupIds) {
    const d = savedMap.get(gid);
    if (!d) { groupDetails.push({ groupId: gid, missing: true }); continue; }
    if (!latestSavedAt || d.savedAt > latestSavedAt) latestSavedAt = d.savedAt;
    groupDetails.push({
      groupId: gid,
      groupName: d.groupName,
      memberCount: d.memberCount,
      savedAt: d.savedAt,
      previousSavedAt: d.previousSavedAt,
    });
    for (const m of d.members) {
      const ph = m._phone || (m.pn || m.jid || m.id || "").replace(/@.*/, "");
      if (!ph) continue;
      if (!phoneGroups.has(ph)) phoneGroups.set(ph, new Set());
      phoneGroups.get(ph).add(d.groupName || gid);
    }
  }

  const totalSum = groupDetails.reduce((a, g) => a + (g.memberCount || 0), 0);
  const uniqueMembers = phoneGroups.size;
  const duplicates = totalSum - uniqueMembers;
  const inMultiple = [...phoneGroups.entries()].filter(([, set]) => set.size > 1);
  const topOverlaps = inMultiple.slice(0, 20).map(([phone, set]) => ({ phone, groups: [...set] }));

  // Recent leavers from log
  let recentLeavers = [];
  try {
    const log = JSON.parse(await fs.readFile(path.join(BRAND_LOGS_DIR, `${b.id}.json`), "utf-8"));
    recentLeavers = log.filter(l => l.type === "member_left").slice(0, 10);
  } catch {}

  res.json({
    brand: b,
    total_sum: totalSum,
    unique_members: uniqueMembers,
    duplicates,
    members_in_multiple_groups: inMultiple.length,
    top_overlaps: topOverlaps,
    groups: groupDetails,
    latest_saved_at: latestSavedAt,
    recent_leavers: recentLeavers,
  });
});

// Get members list filtered by type (all/unique/duplicates/leavers) — JSON or Excel
async function computeBrandMembers(brandId, type = "unique") {
  const b = BRANDS.find(x => x.id === brandId);
  if (!b) return null;
  const savedMap = await loadSavedGroupsMap();
  const phoneMap = new Map(); // phone → { ...data, groups:[groupNames], count }

  for (const gid of b.groupIds) {
    const d = savedMap.get(gid);
    if (!d) continue;
    for (const m of d.members) {
      const ph = m._phone || (m.pn || m.jid || m.id || "").replace(/@.*/, "");
      if (!ph) continue;
      if (!phoneMap.has(ph)) {
        phoneMap.set(ph, {
          phone: ph,
          name: m.verifiedName || m.name || m.notify || "",
          imgUrl: m.imgUrl || m.profilePicUrl || "",
          status: m.status || "",
          isAdmin: m.isAdmin || m.isSuperAdmin ? "✓" : "",
          active: m.active,
          groups: [d.groupName || gid],
        });
      } else {
        phoneMap.get(ph).groups.push(d.groupName || gid);
      }
    }
  }

  // Leavers: collected from log
  let leaverPhones = new Set();
  try {
    const log = JSON.parse(await fs.readFile(path.join(BRAND_LOGS_DIR, `${brandId}.json`), "utf-8"));
    for (const l of log) if (l.type === "member_left" && l.phone) leaverPhones.add(l.phone);
  } catch {}

  const all = [...phoneMap.values()];
  const unique = all.map(m => ({ ...m, groups: [...new Set(m.groups)] }));
  const duplicates = unique.filter(m => m.groups.length > 1);
  const leavers = [...leaverPhones].map(p => phoneMap.get(p) || { phone: p, groups: [] });

  const result = { all, unique, duplicates, leavers };
  return { brand: b, members: result[type] || result.unique, type };
}

app.get("/api/brands/:id/members", async (req, res) => {
  const data = await computeBrandMembers(req.params.id, req.query.type || "unique");
  if (!data) return res.status(404).json({ error: "not found" });
  res.json(data);
});

app.get("/api/brands/:id/export", async (req, res) => {
  const type = req.query.type || "unique";
  const data = await computeBrandMembers(req.params.id, type);
  if (!data) return res.status(404).json({ error: "not found" });

  const wb = new ExcelJS.Workbook();
  wb.creator = "Brand Export";
  const labels = { all: "כל החברים", unique: "ייחודיים", duplicates: "כפולים", leavers: "עזיבות" };
  const ws = wb.addWorksheet(`${data.brand.name} - ${labels[type]||type}`.slice(0, 30));
  ws.columns = [
    { header: "#", key: "i", width: 6 },
    { header: "שם", key: "name", width: 30 },
    { header: "טלפון", key: "phone", width: 20 },
    { header: "כל הקבוצות", key: "groups", width: 40 },
    { header: "מספר קבוצות", key: "count", width: 12 },
    { header: "תאריך ייצוא", key: "exported", width: 20 },
    { header: "תמונה", key: "img", width: 48 },
    { header: "סטטוס", key: "status", width: 28 },
    { header: "אדמין", key: "isAdmin", width: 8 },
  ];
  ws.getRow(1).font = { bold: true, color: { argb: "FF25D366" } };
  const now = new Date().toLocaleString("he-IL");
  data.members.forEach((m, i) => ws.addRow({
    i: i + 1,
    name: m.name || "—",
    phone: m.phone,
    groups: (m.groups || []).join(", "),
    count: (m.groups || []).length,
    exported: now,
    img: m.imgUrl || "",
    status: m.status || "",
    isAdmin: m.isAdmin || "",
  }));
  await fs.mkdir(path.join(DATA_DIR, "exports"), { recursive: true });
  const safe = data.brand.name.replace(/[^a-zA-Z0-9\u0590-\u05FF]/g, "_");
  const filename = `${safe}_${type}_${Date.now()}.xlsx`;
  await wb.xlsx.writeFile(path.join(DATA_DIR, "exports", filename));
  res.json({ url: `/exports/${filename}`, filename, rows: data.members.length });
});

app.get("/api/brands/:id/log", async (req, res) => {
  try {
    const log = JSON.parse(await fs.readFile(path.join(BRAND_LOGS_DIR, `${req.params.id}.json`), "utf-8"));
    res.json({ log });
  } catch {
    res.json({ log: [] });
  }
});

// Rewrite save-group-delta to also emit brand log entries for leavers/new members
async function emitMemberChangesToBrands(groupId, groupName, previousMembers, newMembers) {
  const prevPhones = new Set((previousMembers || []).map(m => m._phone || (m.pn || m.jid || m.id || "").replace(/@.*/, "")));
  const newPhones = new Set(newMembers.map(m => m._phone || (m.pn || m.jid || m.id || "").replace(/@.*/, "")));
  const joined = [...newPhones].filter(p => !prevPhones.has(p));
  const left = [...prevPhones].filter(p => !newPhones.has(p));
  const brandsWithGroup = BRANDS.filter(b => b.groupIds.includes(groupId));
  for (const b of brandsWithGroup) {
    if (joined.length) await appendBrandLog(b.id, { type: "members_joined", groupId, groupName, count: joined.length, phones: joined.slice(0, 20) });
    for (const p of left.slice(0, 50)) await appendBrandLog(b.id, { type: "member_left", groupId, groupName, phone: p });
  }
}

// ========== LOCAL CONTACT NAMES (phone → name override) ==========
const CONTACTS_FILE = path.join(DATA_DIR, "local-contacts.json");
let LOCAL_CONTACTS = {};
try {
  LOCAL_CONTACTS = JSON.parse(await fs.readFile(CONTACTS_FILE, "utf-8"));
} catch {}

async function saveLocalContacts() {
  await fs.writeFile(CONTACTS_FILE, JSON.stringify(LOCAL_CONTACTS, null, 2));
}

app.get("/api/local-contacts", (_, res) => res.json({ contacts: LOCAL_CONTACTS, count: Object.keys(LOCAL_CONTACTS).length }));

app.post("/api/local-contacts", async (req, res) => {
  const { phone, name } = req.body;
  if (!phone) return res.status(400).json({ error: "phone required" });
  if (name) LOCAL_CONTACTS[phone] = name; else delete LOCAL_CONTACTS[phone];
  await saveLocalContacts();
  res.json({ ok: true, phone, name });
});

// Bulk import — array of {phone, name} or CSV-parsed
app.post("/api/local-contacts/bulk", async (req, res) => {
  const { contacts } = req.body;
  if (!Array.isArray(contacts)) return res.status(400).json({ error: "contacts[] required" });
  let added = 0;
  for (const c of contacts) {
    const phone = String(c.phone || "").replace(/\D/g, "");
    if (!phone) continue;
    if (c.name) { LOCAL_CONTACTS[phone] = c.name; added++; }
  }
  await saveLocalContacts();
  res.json({ ok: true, added, total: Object.keys(LOCAL_CONTACTS).length });
});

// Auto-label all members of a group with a pattern
app.post("/api/local-contacts/auto-label", async (req, res) => {
  const { phones, pattern } = req.body;
  if (!Array.isArray(phones)) return res.status(400).json({ error: "phones[] required" });
  const p = pattern || "Member-{i}";
  let added = 0;
  phones.forEach((phone, idx) => {
    const cleanPhone = String(phone).replace(/\D/g, "");
    if (!cleanPhone) return;
    // Only set if no existing name
    if (!LOCAL_CONTACTS[cleanPhone]) {
      LOCAL_CONTACTS[cleanPhone] = p.replace("{i}", idx + 1).replace("{phone}", cleanPhone);
      added++;
    }
  });
  await saveLocalContacts();
  res.json({ ok: true, added, total: Object.keys(LOCAL_CONTACTS).length });
});

app.post("/api/save-group", async (req, res) => {
  const { groupId, groupName, members, metadata } = req.body;
  if (!groupId || !members) return res.status(400).json({ error: "groupId + members required" });
  const safeId = groupId.replace(/[^a-zA-Z0-9]/g, "_");
  const payload = {
    groupId,
    groupName,
    metadata: metadata || {},
    memberCount: members.length,
    savedAt: new Date().toISOString(),
    members,
  };
  await fs.writeFile(path.join(GROUPS_DIR, `${safeId}.json`), JSON.stringify(payload, null, 2));
  res.json({ ok: true, file: `${safeId}.json`, rows: members.length });
});

app.get("/api/saved-groups", async (_, res) => {
  try {
    const files = (await fs.readdir(GROUPS_DIR)).filter(f => f.endsWith(".json"));
    const groups = await Promise.all(files.map(async f => {
      const data = JSON.parse(await fs.readFile(path.join(GROUPS_DIR, f), "utf-8"));
      return { file: f, groupId: data.groupId, groupName: data.groupName, memberCount: data.memberCount, savedAt: data.savedAt };
    }));
    groups.sort((a, b) => new Date(b.savedAt) - new Date(a.savedAt));
    res.json({ groups });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get("/api/saved-groups/:file", async (req, res) => {
  try {
    const data = JSON.parse(await fs.readFile(path.join(GROUPS_DIR, req.params.file), "utf-8"));
    res.json(data);
  } catch (e) {
    res.status(404).json({ error: "not found" });
  }
});

app.delete("/api/saved-groups/:file", async (req, res) => {
  try { await fs.unlink(path.join(GROUPS_DIR, req.params.file)); res.json({ ok: true }); }
  catch (e) { res.status(404).json({ error: e.message }); }
});

// Merge all saved groups + dedup → export Excel
app.post("/api/saved-groups/merge-export", async (req, res) => {
  const { files } = req.body; // optional — if empty, use all
  try {
    const allFiles = (await fs.readdir(GROUPS_DIR)).filter(f => f.endsWith(".json"));
    const useFiles = files?.length ? files.filter(f => allFiles.includes(f)) : allFiles;
    if (!useFiles.length) return res.status(400).json({ error: "no saved groups" });

    const seen = new Map(); // phone → { first occurrence }
    let totalBeforeDedup = 0;
    for (const f of useFiles) {
      const data = JSON.parse(await fs.readFile(path.join(GROUPS_DIR, f), "utf-8"));
      const groupName = data.groupName || f;
      const savedAt = data.savedAt;
      for (const m of data.members) {
        const phone = m._phone || (m.pn || m.jid || m.id || "").replace(/@.*/, "");
        if (!phone) continue;
        totalBeforeDedup++;
        if (!seen.has(phone)) {
          seen.set(phone, {
            phone,
            name: m.verifiedName || m.name || m.notify || m.pushname || "",
            imgUrl: m.imgUrl || m.profilePicUrl || "",
            status: m.status || "",
            isAdmin: m.isAdmin || m.isSuperAdmin ? "✓" : "",
            group_name: groupName,
            fetched_at: savedAt ? new Date(savedAt).toLocaleString("he-IL") : "",
            groups_seen: [groupName],
          });
        } else {
          // track all groups this phone appears in
          const existing = seen.get(phone);
          if (!existing.groups_seen.includes(groupName)) existing.groups_seen.push(groupName);
        }
      }
    }

    const unique = [...seen.values()];

    const wb = new ExcelJS.Workbook();
    const ws = wb.addWorksheet("All Groups Merged");
    ws.columns = [
      { header: "#", key: "i", width: 6 },
      { header: "שם", key: "name", width: 28 },
      { header: "טלפון", key: "phone", width: 18 },
      { header: "שם הקבוצה (ראשונה)", key: "group_name", width: 28 },
      { header: "כל הקבוצות", key: "all_groups", width: 40 },
      { header: "תאריך שליפה", key: "fetched_at", width: 20 },
      { header: "תמונה", key: "imgUrl", width: 45 },
      { header: "סטטוס", key: "status", width: 30 },
      { header: "אדמין", key: "isAdmin", width: 8 },
    ];
    ws.getRow(1).font = { bold: true, color: { argb: "FF25D366" } };
    unique.forEach((m, i) => ws.addRow({
      i: i + 1, name: m.name, phone: m.phone, group_name: m.group_name,
      all_groups: m.groups_seen.join(", "), fetched_at: m.fetched_at,
      imgUrl: m.imgUrl, status: m.status, isAdmin: m.isAdmin,
    }));

    const filename = `merged_all_groups_${Date.now()}.xlsx`;
    await fs.mkdir(path.join(DATA_DIR, "exports"), { recursive: true });
    await wb.xlsx.writeFile(path.join(DATA_DIR, "exports", filename));

    res.json({
      url: `/exports/${filename}`,
      filename,
      groups_merged: useFiles.length,
      total_before_dedup: totalBeforeDedup,
      unique_members: unique.length,
      duplicates_removed: totalBeforeDedup - unique.length,
    });
  } catch (e) {
    res.status(500).json({ error: String(e.message || e) });
  }
});

app.get("/api/enrich-status/:id", (req, res) => {
  const j = enrichJobs.get(req.params.id);
  if (!j) return res.status(404).json({ error: "not found" });
  res.json(j);
});

// ========== SEND MESSAGE ==========
app.post("/api/send", async (req, res) => {
  const { to, text, sessionApiKey } = req.body;
  if (!to || !text) return res.status(400).json({ error: "to + text required" });
  const r = await wa("POST", "/send-message", { to, text }, sessionApiKey);
  res.json(r);
});

// Broadcast to multiple numbers
app.post("/api/broadcast", async (req, res) => {
  const { numbers, text, sessionApiKey, delayMs } = req.body;
  if (!numbers?.length || !text) return res.status(400).json({ error: "numbers[] + text required" });
  const delay = Math.max(delayMs || 3000, 2000);
  const results = [];
  for (let i = 0; i < numbers.length; i++) {
    const phone = normalizePhone(numbers[i]);
    const r = await wa("POST", "/send-message", { to: phone, text }, sessionApiKey);
    results.push({ phone, ok: r.ok, error: r.error || null });
    if (i < numbers.length - 1) await new Promise((r) => setTimeout(r, delay));
  }
  const sent = results.filter((r) => r.ok).length;
  res.json({ total: numbers.length, sent, failed: numbers.length - sent, results });
});

// ========== EXPORT TO EXCEL ==========
app.post("/api/export-excel", async (req, res) => {
  const { groupName, participants } = req.body;
  if (!participants?.length) return res.status(400).json({ error: "no participants" });

  const wb = new ExcelJS.Workbook();
  wb.creator = "WhatsApp Groups Tool";
  const ws = wb.addWorksheet((groupName || "Members").slice(0, 30));
  const fetchedAt = new Date().toLocaleString("he-IL");

  ws.columns = [
    { header: "#", key: "index", width: 6 },
    { header: "שם", key: "name", width: 30 },
    { header: "טלפון", key: "phone", width: 20 },
    { header: "פעיל בוואטסאפ", key: "active", width: 14 },
    { header: "שם הקבוצה", key: "group_name", width: 28 },
    { header: "תאריך שליפה", key: "fetched_at", width: 20 },
    { header: "תמונה", key: "photo", width: 50 },
    { header: "סטטוס", key: "status", width: 30 },
    { header: "אדמין", key: "isAdmin", width: 8 },
  ];

  ws.getRow(1).font = { bold: true, color: { argb: "FF25D366" } };
  ws.getRow(1).fill = { type: "pattern", pattern: "solid", fgColor: { argb: "FF1A1A2E" } };

  participants.forEach((p, i) => {
    const phone = p._phone || (p.pn || p.jid || p.id || "").replace(/@.*/, "") || p.phone || "—";
    ws.addRow({
      index: i + 1,
      name: p.verifiedName || p.name || p.notify || p.pushname || "—",
      phone,
      active: p.active === true ? "✓ פעיל" : p.active === false ? "✗ לא פעיל" : "—",
      group_name: groupName || "—",
      fetched_at: fetchedAt,
      photo: p.imgUrl || p.profilePicUrl || "—",
      status: p.status || "",
      isAdmin: p.isAdmin || p.isSuperAdmin || p.admin ? "✓" : "",
    });
  });

  await fs.mkdir(path.join(DATA_DIR, "exports"), { recursive: true });
  const filename = `${(groupName || "group").replace(/[^a-zA-Z0-9\u0590-\u05FF]/g, "_")}_${Date.now()}.xlsx`;
  await wb.xlsx.writeFile(path.join(DATA_DIR, "exports", filename));

  res.json({ url: `/exports/${filename}`, filename, rows: participants.length });
});

// ========== HELPERS ==========
function normalizePhone(phone) {
  let cleaned = String(phone).replace(/[\s\-\(\)]/g, "");
  if (cleaned.startsWith("0")) cleaned = "972" + cleaned.substring(1);
  if (!cleaned.startsWith("972") && !cleaned.includes("@")) cleaned = "972" + cleaned;
  return cleaned;
}

// ========== INSTAGRAM ==========
const IG_CONFIG_PATH = path.join(DATA_DIR, "ig-config.json");
let IG_CONFIG = { engine: null, token: null, account: null };
try {
  IG_CONFIG = JSON.parse(await fs.readFile(IG_CONFIG_PATH, "utf-8"));
} catch {}

async function saveIgConfig() {
  await fs.writeFile(IG_CONFIG_PATH, JSON.stringify(IG_CONFIG, null, 2));
}

app.post("/api/ig/config", async (req, res) => {
  const { authType, engine, token, username, password, twoFa } = req.body;
  if (!authType) return res.status(400).json({ error: "authType required" });

  try {
    if (authType === "token") {
      if (!engine || !token) return res.status(400).json({ error: "engine + token required" });
      IG_CONFIG = { authType, engine, token, account: null };
      if (engine === "apify") {
        const r = await fetch(`https://api.apify.com/v2/users/me?token=${encodeURIComponent(token)}`);
        if (!r.ok) throw new Error(`Apify ${r.status}: ${(await r.text()).slice(0, 200)}`);
        const d = await r.json();
        IG_CONFIG.account = d?.data?.username || d?.data?.email || "apify user";
      } else if (engine === "graph") {
        const r = await fetch(`https://graph.facebook.com/v18.0/me?access_token=${encodeURIComponent(token)}`);
        if (!r.ok) throw new Error(`Graph ${r.status}: ${(await r.text()).slice(0, 200)}`);
        const d = await r.json();
        IG_CONFIG.account = d?.name || d?.id || "graph user";
      }
    } else if (authType === "userpass") {
      if (!username || !password) return res.status(400).json({ error: "username + password required" });
      IG_CONFIG = { authType, engine: "instagrapi", username, password, twoFa: twoFa || null, account: `@${username}` };
      // NOTE: actual Instagrapi login happens in Python subprocess — not implemented here
    } else {
      return res.status(400).json({ error: `unknown authType ${authType}` });
    }

    await saveIgConfig();
    res.json({ ok: true, account: IG_CONFIG.account, authType });
  } catch (e) {
    res.status(400).json({ error: String(e.message || e) });
  }
});

app.get("/api/ig/status", (_, res) => {
  res.json({ engine: IG_CONFIG.engine, account: IG_CONFIG.account, configured: !!IG_CONFIG.token });
});

// Fetch followers via Apify (default) or Graph
app.get("/api/ig/followers", async (req, res) => {
  const { user, type } = req.query;
  if (!IG_CONFIG.token) return res.status(400).json({ error: "Instagram not configured — go to חיבור tab" });
  if (!user) return res.status(400).json({ error: "user required" });

  const username = String(user).replace(/^@/, "").replace(/.*instagram\.com\//, "").replace(/\/.*$/, "");

  try {
    if (IG_CONFIG.engine === "apify") {
      // Apify Instagram Followers scraper
      const actorId = type === "following" ? "apify~instagram-scraper" : "apify~instagram-followers-scraper";
      const runRes = await fetch(`https://api.apify.com/v2/acts/${actorId}/run-sync-get-dataset-items?token=${encodeURIComponent(IG_CONFIG.token)}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ usernames: [username], resultsLimit: 100 }),
      });
      if (!runRes.ok) {
        const t = await runRes.text();
        throw new Error(`Apify ${runRes.status}: ${t.slice(0, 300)}`);
      }
      const data = await runRes.json();
      const followers = (Array.isArray(data) ? data : [data]).map((f) => ({
        username: f.username || f.node?.username,
        full_name: f.fullName || f.full_name || f.node?.full_name,
        profile_pic_url: f.profilePicUrl || f.profile_pic_url,
        biography: f.biography,
        is_verified: f.isVerified || f.is_verified,
      })).filter((f) => f.username);
      res.json({ followers, count: followers.length });
    } else {
      res.status(400).json({ error: `engine ${IG_CONFIG.engine} not yet implemented for followers` });
    }
  } catch (e) {
    res.status(500).json({ error: String(e.message || e) });
  }
});

// Scrape public profile / post
app.get("/api/ig/scrape", async (req, res) => {
  const { url } = req.query;
  if (!IG_CONFIG.token) return res.status(400).json({ error: "Instagram not configured" });
  if (!url) return res.status(400).json({ error: "url required" });

  try {
    if (IG_CONFIG.engine === "apify") {
      const runRes = await fetch(`https://api.apify.com/v2/acts/apify~instagram-scraper/run-sync-get-dataset-items?token=${encodeURIComponent(IG_CONFIG.token)}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ directUrls: [url], resultsLimit: 10 }),
      });
      if (!runRes.ok) throw new Error(`Apify ${runRes.status}: ${(await runRes.text()).slice(0, 300)}`);
      const data = await runRes.json();
      res.json(data);
    } else {
      res.status(400).json({ error: "scrape requires apify engine" });
    }
  } catch (e) {
    res.status(500).json({ error: String(e.message || e) });
  }
});

// Export IG followers to Excel
app.post("/api/ig/export", async (req, res) => {
  const { followers } = req.body;
  if (!followers?.length) return res.status(400).json({ error: "no followers" });
  const wb = new ExcelJS.Workbook();
  const ws = wb.addWorksheet("Followers");
  ws.columns = [
    { header: "#", key: "i", width: 6 },
    { header: "Username", key: "u", width: 24 },
    { header: "שם מלא", key: "n", width: 28 },
    { header: "תמונה", key: "p", width: 48 },
    { header: "Bio", key: "b", width: 50 },
    { header: "Verified", key: "v", width: 10 },
  ];
  ws.getRow(1).font = { bold: true, color: { argb: "FFEC4899" } };
  followers.forEach((f, i) => ws.addRow({ i: i + 1, u: "@" + (f.username || ""), n: f.full_name || "", p: f.profile_pic_url || "", b: f.biography || "", v: f.is_verified ? "✓" : "" }));
  await fs.mkdir(path.join(DATA_DIR, "exports"), { recursive: true });
  const filename = `ig_followers_${Date.now()}.xlsx`;
  await wb.xlsx.writeFile(path.join(DATA_DIR, "exports", filename));
  res.json({ url: `/exports/${filename}`, filename, rows: followers.length });
});

// DM broadcast
app.post("/api/ig/dm-broadcast", async (req, res) => {
  const { users, text, delayMs } = req.body;
  if (!IG_CONFIG.token) return res.status(400).json({ error: "not configured" });
  res.json({ total: users.length, sent: 0, failed: users.length, error: "DM broadcast requires Instagrapi/MCP — not implemented in Apify path" });
});

// Agent stub
app.post("/api/ig/agent/start", (req, res) => {
  res.json({ ok: false, error: "Agent pipeline not yet wired — needs webhook + LLM. Save goal/rules for now." });
});

// ========== SERVE UI ==========
app.get("/", (_, res) => res.sendFile(path.join(__dirname, "ui.html")));

const PORT = Number(process.env.PORT) || 3500;
app.listen(PORT, () => {
  console.log(`📱 WhatsApp Groups: http://localhost:${PORT}`);
  if (!KEY) console.log("⚠️  WASENDER_API_KEY not set — add it to .env");
});

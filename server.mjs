import express from "express";
import path from "path";
import fs from "fs/promises";
import crypto from "crypto";
import { fileURLToPath } from "url";
import ExcelJS from "exceljs";
import { readJson, writeJson, listKeys, deleteKey, writeBinary } from "./storage.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
// DATA_DIR for persistent storage on Railway volume (defaults to __dirname for local)
const DATA_DIR = process.env.DATA_DIR || __dirname;
await fs.mkdir(DATA_DIR, { recursive: true }).catch(() => {});

const app = express();
app.use(express.json({ limit: "2mb" }));

// ========== COOKIE AUTH (persistent login) ==========
const AUTH_USER = process.env.AUTH_USER || "oren";
const AUTH_PASS = process.env.AUTH_PASS || "WhatsApp2026";
const COOKIE_SECRET = process.env.COOKIE_SECRET || (AUTH_USER + ":" + AUTH_PASS + ":mayo");
const COOKIE_NAME = "mayo_sess";
const COOKIE_MAX_AGE = 60 * 60 * 24 * 365; // 1 year

function signCookie(user) {
  const payload = Buffer.from(JSON.stringify({ u: user, t: Date.now() })).toString("base64url");
  const sig = crypto.createHmac("sha256", COOKIE_SECRET).update(payload).digest("base64url");
  return `${payload}.${sig}`;
}
function verifyCookie(val) {
  if (!val || typeof val !== "string") return null;
  const [payload, sig] = val.split(".");
  if (!payload || !sig) return null;
  const expected = crypto.createHmac("sha256", COOKIE_SECRET).update(payload).digest("base64url");
  if (sig !== expected) return null;
  try {
    const obj = JSON.parse(Buffer.from(payload, "base64url").toString("utf8"));
    return obj.u === AUTH_USER ? obj : null;
  } catch { return null; }
}

function parseCookies(header) {
  const out = {};
  if (!header) return out;
  for (const part of header.split(";")) {
    const [k, ...v] = part.trim().split("=");
    if (k) out[k] = decodeURIComponent(v.join("="));
  }
  return out;
}

app.post("/api/login", express.urlencoded({ extended: true }), (req, res) => {
  const { user, pass } = req.body || {};
  if (user === AUTH_USER && pass === AUTH_PASS) {
    const cookie = `${COOKIE_NAME}=${signCookie(user)}; Max-Age=${COOKIE_MAX_AGE}; Path=/; SameSite=Lax; HttpOnly${req.secure || req.headers["x-forwarded-proto"] === "https" ? "; Secure" : ""}`;
    res.set("Set-Cookie", cookie);
    return res.redirect("/");
  }
  res.redirect("/login?err=1");
});

app.post("/api/logout", (req, res) => {
  res.set("Set-Cookie", `${COOKIE_NAME}=; Max-Age=0; Path=/; SameSite=Lax; HttpOnly`);
  res.redirect("/login");
});

app.get("/login", (req, res) => {
  const err = req.query.err ? '<div style="color:#ef4444;margin-bottom:10px">סיסמה שגויה</div>' : "";
  res.send(`<!doctype html><html lang="he" dir="rtl"><head><meta charset="UTF-8"><title>התחברות</title>
<style>body{font-family:system-ui,sans-serif;background:#0a0a12;color:#e8e8f0;display:flex;align-items:center;justify-content:center;min-height:100vh;margin:0}
.box{background:#12121c;border:1px solid #22223a;border-radius:14px;padding:32px;width:320px}
h1{margin:0 0 20px;font-size:22px;text-align:center}
input{width:100%;padding:12px;background:#1a1a28;border:1px solid #22223a;border-radius:8px;color:#e8e8f0;font-size:14px;margin-bottom:10px;box-sizing:border-box}
button{width:100%;padding:12px;background:#25d366;color:white;border:none;border-radius:8px;font-weight:700;font-size:14px;cursor:pointer}
button:hover{background:#128c7e}
.remember{display:flex;align-items:center;gap:8px;font-size:13px;color:#9a9ab0;margin:6px 0 14px}
.remember input{width:auto;margin:0}
.hint{color:#6a6a80;font-size:12px;text-align:center;margin-top:14px}
</style></head><body>
<div class="box">
  <h1>📱 WhatsApp Groups</h1>
  ${err}
  <form method="POST" action="/api/login" id="lf">
    <input id="lu" name="user" placeholder="שם משתמש" autocomplete="username" required />
    <input id="lp" name="pass" type="password" placeholder="סיסמה" autocomplete="current-password" required />
    <label class="remember"><input type="checkbox" id="rm" checked /> זכור אותי (אוטומטי בפעם הבאה)</label>
    <button type="submit">התחבר</button>
  </form>
  <div class="hint">חיבור נשמר לשנה. סימון "זכור אותי" ימלא אוטומטית בפעם הבאה</div>
</div>
<script>
// Load saved creds
try {
  const saved = JSON.parse(localStorage.getItem("mayo_creds") || "null");
  if (saved && saved.u && saved.p) {
    document.getElementById("lu").value = saved.u;
    document.getElementById("lp").value = saved.p;
    // Auto-submit only if no error (first time user landed here)
    if (!${req.query.err ? "true" : "false"}) {
      setTimeout(() => document.getElementById("lf").submit(), 200);
    }
  }
} catch {}

// Save creds on submit if remember checked
document.getElementById("lf").addEventListener("submit", (e) => {
  const u = document.getElementById("lu").value;
  const p = document.getElementById("lp").value;
  if (document.getElementById("rm").checked) {
    localStorage.setItem("mayo_creds", JSON.stringify({ u, p }));
  } else {
    localStorage.removeItem("mayo_creds");
  }
});
</script>
</body></html>`);
});

app.use((req, res, next) => {
  // Skip auth for login page/endpoint AND public webhook
  if (req.path === "/login" || req.path === "/api/login") return next();
  if (req.path.startsWith("/api/webhook/")) return next();
  const cookies = parseCookies(req.headers.cookie);
  const session = verifyCookie(cookies[COOKIE_NAME]);
  if (session) return next();
  if (req.path.startsWith("/api/")) return res.status(401).json({ error: "not authenticated" });
  res.redirect("/login");
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

// ========== WaSender Rate Limiter (60 req/min token bucket) ==========
let WA_TOKENS = 60;
let WA_LAST_REFILL = Date.now();
const WA_MAX_TOKENS = 60;
const WA_REFILL_MS = 60 * 1000; // full refill per minute

async function waRateLimit() {
  const elapsed = Date.now() - WA_LAST_REFILL;
  if (elapsed >= WA_REFILL_MS) {
    WA_TOKENS = WA_MAX_TOKENS;
    WA_LAST_REFILL = Date.now();
  } else if (elapsed > 0) {
    const add = Math.floor((elapsed / WA_REFILL_MS) * WA_MAX_TOKENS);
    if (add > 0) {
      WA_TOKENS = Math.min(WA_MAX_TOKENS, WA_TOKENS + add);
      WA_LAST_REFILL += (add / WA_MAX_TOKENS) * WA_REFILL_MS;
    }
  }
  if (WA_TOKENS <= 0) {
    const waitMs = WA_REFILL_MS - (Date.now() - WA_LAST_REFILL);
    await new Promise(r => setTimeout(r, Math.max(waitMs, 500)));
    WA_TOKENS = WA_MAX_TOKENS;
    WA_LAST_REFILL = Date.now();
  }
  WA_TOKENS--;
}

// Error log for observability
async function logError(context, err) {
  try {
    const key = `error-log.json`;
    const log = (await readJson(key)) || [];
    log.unshift({ at: new Date().toISOString(), context, error: String(err).slice(0, 500) });
    await writeJson(key, log.slice(0, 200));
  } catch {}
}

async function wa(method, path, body, sessionKey) {
  if (!KEY && !sessionKey) return { ok: false, error: "WASENDER_API_KEY not configured" };
  await waRateLimit();
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
    if (!res.ok) {
      // On 429 — return tokens to bucket and signal rate-limit upstream
      if (res.status === 429) WA_TOKENS = 0;
      return { ok: false, status: res.status, error: data?.message || data?.error || `HTTP ${res.status}` };
    }
    return { ok: true, data };
  } catch (e) {
    logError(`wa ${method} ${path}`, e.message);
    return { ok: false, error: e.message };
  }
}

// ========== GLOBAL SEND QUEUE (serializes all WhatsApp write ops to avoid ban) ==========
let WRITE_QUEUE = Promise.resolve();
let LAST_WRITE_AT = 0;
const MIN_WRITE_GAP_MS = 10000; // 10 seconds between any two WhatsApp write ops

function throttledWaWrite(method, path, body, sessionKey) {
  WRITE_QUEUE = WRITE_QUEUE.then(async () => {
    const sinceLast = Date.now() - LAST_WRITE_AT;
    const wait = Math.max(0, MIN_WRITE_GAP_MS - sinceLast);
    if (wait > 0) await new Promise(r => setTimeout(r, wait));
    const result = await wa(method, path, body, sessionKey);
    LAST_WRITE_AT = Date.now();
    return result;
  });
  return WRITE_QUEUE;
}

// ========== SESSION MANAGEMENT (with hourly cache + log) ==========
let SESSIONS_CACHE = null;
let SESSIONS_UPDATED_AT = 0;
const SESSIONS_TTL = 60 * 60 * 1000; // 1 hour

async function refreshSessions() {
  const r = await wa("GET", "/whatsapp-sessions");
  if (!r.ok) throw new Error(r.error || "fetch failed");
  const sessions = Array.isArray(r.data) ? r.data : r.data?.data || [];
  SESSIONS_CACHE = sessions;
  SESSIONS_UPDATED_AT = Date.now();
  // Append to connection log
  const logKey = "connection-log.json";
  const log = (await readJson(logKey)) || [];
  log.unshift({
    at: new Date().toISOString(),
    total: sessions.length,
    connected: sessions.filter(s => ["connected","ready"].includes((s.status||"").toLowerCase())).length,
    sessions: sessions.map(s => ({ id: s.id, name: s.name, phone: s.phone_number, status: s.status })),
  });
  await writeJson(logKey, log.slice(0, 200));
  return sessions;
}

app.get("/api/sessions", async (req, res) => {
  const force = req.query.refresh === "1";
  try {
    if (!SESSIONS_CACHE || force || Date.now() - SESSIONS_UPDATED_AT > SESSIONS_TTL) {
      await refreshSessions();
    }
    res.json({ sessions: SESSIONS_CACHE, updatedAt: SESSIONS_UPDATED_AT, cached: true });
  } catch (e) {
    if (SESSIONS_CACHE) return res.json({ sessions: SESSIONS_CACHE, updatedAt: SESSIONS_UPDATED_AT, stale: true, error: e.message });
    res.status(500).json({ error: e.message });
  }
});

app.get("/api/connection-log", async (_, res) => {
  const log = (await readJson("connection-log.json")) || [];
  res.json({ log });
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

// ========== GROUPS + MEMBERS CACHE ==========
const GROUPS_CACHE = new Map();  // sessionId → { groups, sessionKey, connected, status, at, refreshing }
const MEMBERS_CACHE = new Map(); // groupId → { participants, at }
const GROUPS_TTL = 60 * 60 * 1000; // 1 hour

// Core fetcher: hit WASENDER + enrich + check saved-groups. Expensive — always serve from cache when possible.
async function fetchGroupsForSession(sid) {
  const sessionR = await wa("GET", `/whatsapp-sessions/${sid}`);
  if (!sessionR.ok) throw new Error(sessionR.error || "session fetch failed");
  const sessData = sessionR.data?.data || sessionR.data;
  const sessionKey = sessData?.api_key;
  const myPhone = (sessData?.phone_number || "").replace(/\D/g, "");
  if (!sessionKey) throw new Error("session API key not found");
  const status = (sessData?.status || "").toLowerCase();
  const connected = status === "connected" || status === "ready";

  const r = await wa("GET", "/groups", null, sessionKey);
  if (!r.ok) throw new Error(r.error || "groups fetch failed");
  const groups = Array.isArray(r.data) ? r.data : r.data?.data || r.data?.groups || [];

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
              // Check admin status of connected phone in this group
              let iAmAdmin = false;
              if (myPhone && Array.isArray(md?.participants)) {
                const me = md.participants.find(p => {
                  const pn = (p.pn || p.jid || p.id || "").replace(/\D/g, "");
                  return pn === myPhone || (p.jid || "").startsWith(myPhone + "@");
                });
                iAmAdmin = !!(me && (me.isAdmin || me.isSuperAdmin || me.admin));
              }
              enriched[idx] = {
                ...g,
                size,
                desc: md?.desc || md?.description,
                subject: md?.subject || g.name,
                creation: md?.creation,
                iAmAdmin,
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
  const savedFiles = (await listKeys("saved-groups")).filter(k => k.endsWith(".json"));
  const savedMap = new Map();
  for (const f of savedFiles) {
    const data = await readJson(f);
    if (data) savedMap.set(data.groupId, { file: f.replace(/^saved-groups\//, ""), memberCount: data.memberCount, savedAt: data.savedAt });
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

  return { groups: withStatus, sessionKey, connected, status };
}

// Serve from cache; stale-while-revalidate when >1h old. Force with ?refresh=1.
app.get("/api/sessions/:id/groups", async (req, res) => {
  const sid = req.params.id;
  const force = req.query.refresh === "1";
  const cached = GROUPS_CACHE.get(sid);
  const fresh = cached && Date.now() - cached.at < GROUPS_TTL;

  if (cached && !force) {
    // Stale — kick off a background refresh, but serve the cached payload immediately
    if (!fresh && !cached.refreshing) {
      cached.refreshing = true;
      fetchGroupsForSession(sid)
        .then((r) => GROUPS_CACHE.set(sid, { ...r, at: Date.now(), refreshing: false }))
        .catch((e) => { console.error(`bg groups refresh (${sid}) failed:`, e.message); if (cached) cached.refreshing = false; });
    }
    return res.json({
      groups: cached.groups, sessionKey: cached.sessionKey,
      connected: cached.connected, status: cached.status,
      updatedAt: cached.at, cached: true, stale: !fresh,
    });
  }

  // No cache or forced refresh — fetch synchronously
  try {
    const r = await fetchGroupsForSession(sid);
    GROUPS_CACHE.set(sid, { ...r, at: Date.now(), refreshing: false });
    res.json({ ...r, updatedAt: Date.now(), cached: false });
  } catch (e) {
    if (cached) {
      return res.json({
        groups: cached.groups, sessionKey: cached.sessionKey,
        connected: cached.connected, status: cached.status,
        updatedAt: cached.at, cached: true, stale: true, error: e.message,
      });
    }
    res.status(500).json({ error: e.message });
  }
});

// Save delta — only new members (dedup against existing saved group)
app.post("/api/save-group-delta", async (req, res) => {
  const { groupId, groupName, members, metadata } = req.body;
  if (!groupId || !members) return res.status(400).json({ error: "groupId + members required" });
  const safeId = groupId.replace(/[^a-zA-Z0-9]/g, "_");
  const fp = path.join(GROUPS_DIR, `${safeId}.json`);

  const storageKey = `saved-groups/${safeId}.json`;
  const existing = await readJson(storageKey);

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
  await writeJson(storageKey, payload);

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

// Get group participants/members (cached 1 hour, optional pagination)
app.get("/api/sessions/:id/groups/:groupId/participants", async (req, res) => {
  const groupId = req.params.groupId;
  const force = req.query.refresh === "1";
  const page = Number(req.query.page) || 0;
  const limit = Math.min(Number(req.query.limit) || 0, 2000);

  let participants;
  let updatedAt;
  let fromCache = false;
  const cached = MEMBERS_CACHE.get(groupId);
  if (cached && !force && Date.now() - cached.at < GROUPS_TTL) {
    participants = cached.participants;
    updatedAt = cached.at;
    fromCache = true;
  } else {
    const sessionApiKey = req.query.sessionKey || null;
    const r = await wa("GET", `/groups/${groupId}/participants`, null, sessionApiKey);
    if (!r.ok) {
      const r2 = await wa("GET", `/whatsapp-sessions/${req.params.id}/groups/${groupId}/participants`);
      if (!r2.ok) return res.status(500).json(r2);
      participants = Array.isArray(r2.data) ? r2.data : r2.data?.data || r2.data?.participants || [];
    } else {
      participants = Array.isArray(r.data) ? r.data : r.data?.data || [];
    }
    updatedAt = Date.now();
    MEMBERS_CACHE.set(groupId, { participants, at: updatedAt });
  }

  // Server-side pagination if requested
  if (limit > 0) {
    const total = participants.length;
    const start = page * limit;
    const pageSlice = participants.slice(start, start + limit);
    return res.json({ participants: pageSlice, total, page, limit, updatedAt, cached: fromCache });
  }
  res.json({ participants, updatedAt, cached: fromCache });
});

// Error log endpoint
app.get("/api/error-log", async (_, res) => {
  const log = (await readJson("error-log.json")) || [];
  res.json({ log });
});

// ========== WaSender WEBHOOK — live event receiver ==========
// No auth middleware on this (WaSender calls it directly); validate via secret
app.post("/api/webhook/wasender", express.raw({ type: "application/json", limit: "10mb" }), async (req, res) => {
  const secret = process.env.WASENDER_WEBHOOK_SECRET || "";
  // simple secret check via query param (?secret=...) or header
  const provided = req.query.secret || req.headers["x-webhook-secret"] || "";
  if (secret && provided !== secret) {
    return res.status(401).json({ error: "invalid secret" });
  }

  let payload;
  try { payload = JSON.parse(req.body.toString("utf8")); } catch { payload = {}; }

  const event = payload.event || payload.type;
  const data = payload.data || payload;
  try {
    // Capture pushnames from messages.upsert
    if (event === "messages.upsert" && data?.messages?.length) {
      const names = (await readJson("pushnames.json")) || {};
      let changed = false;
      for (const m of data.messages) {
        const pn = (m.key?.participant || m.key?.remoteJid || "").split("@")[0];
        const name = m.pushName || m.pushname;
        if (pn && name && names[pn] !== name) {
          names[pn] = name;
          changed = true;
        }
      }
      if (changed) await writeJson("pushnames.json", names);
    }
    // Group participants update → invalidate cache
    if (event === "group-participants.update" && data?.id) {
      MEMBERS_CACHE.delete(data.id);
      const logKey = "webhook-log.json";
      const log = (await readJson(logKey)) || [];
      log.unshift({ at: new Date().toISOString(), event, gid: data.id, action: data.action });
      await writeJson(logKey, log.slice(0, 300));
    }
  } catch (e) {
    await logError("webhook", e.message);
  }
  res.json({ ok: true });
});

app.get("/api/pushnames", async (_, res) => {
  const names = (await readJson("pushnames.json")) || {};
  res.json({ count: Object.keys(names).length, names });
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
    await ensureLocalContacts();
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
await fs.mkdir(GROUPS_DIR, { recursive: true }).catch(() => {});

// ========== BRANDS ==========
const BRANDS_FILE = path.join(DATA_DIR, "brands.json");
const BRAND_LOGS_DIR = path.join(DATA_DIR, "brand-logs");
await fs.mkdir(BRAND_LOGS_DIR, { recursive: true }).catch(() => {});
let BRANDS = [];
async function ensureBrands() {
  // Always reload — on Vercel each function call may be a different instance
  BRANDS = (await readJson("brands.json")) || [];
}
async function saveBrands() {
  await writeJson("brands.json", BRANDS);
}

async function appendBrandLog(brandId, entry) {
  const key = `brand-logs/${brandId}.json`;
  const log = (await readJson(key)) || [];
  log.unshift({ at: new Date().toISOString(), ...entry });
  await writeJson(key, log.slice(0, 500));
}

async function loadSavedGroupsMap() {
  const keys = (await listKeys("saved-groups")).filter(k => k.endsWith(".json"));
  const map = new Map();
  for (const key of keys) {
    const d = await readJson(key);
    if (d) map.set(d.groupId, { ...d, file: key.replace(/^saved-groups\//, "") });
  }
  return map;
}

app.get("/api/brands", async (_, res) => {
  await ensureBrands();
  const savedMap = await loadSavedGroupsMap();
  const brands = await Promise.all(BRANDS.map(async b => {
    let latestSavedAt = null;
    for (const gid of b.groupIds) {
      const sa = savedMap.get(gid)?.savedAt;
      if (sa && (!latestSavedAt || sa > latestSavedAt)) latestSavedAt = sa;
    }
    let statsUpdatedAt = null;
    try {
      const c = await loadBrandStatsCache(b.id);
      if (c?.at) statsUpdatedAt = c.at;
    } catch {}
    return {
      ...b,
      group_count: b.groupIds.length,
      total_members: b.groupIds.reduce((a, gid) => a + (savedMap.get(gid)?.memberCount || 0), 0),
      latest_saved_at: latestSavedAt,
      stats_updated_at: statsUpdatedAt,
    };
  }));
  res.json({ brands });
});

app.post("/api/brands", async (req, res) => {
  await ensureBrands();
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
  await ensureBrands();
  const b = BRANDS.find(x => x.id === req.params.id);
  if (!b) return res.status(404).json({ error: "not found" });
  if (req.body.name) b.name = req.body.name;
  if (req.body.color) b.color = req.body.color;
  await saveBrands();
  await appendBrandLog(b.id, { type: "brand_updated", changes: req.body });
  res.json(b);
});

app.delete("/api/brands/:id", async (req, res) => {
  await ensureBrands();
  const idx = BRANDS.findIndex(x => x.id === req.params.id);
  if (idx < 0) return res.status(404).json({ error: "not found" });
  BRANDS.splice(idx, 1);
  await saveBrands();
  res.json({ ok: true });
});

app.post("/api/brands/:id/groups", async (req, res) => {
  await ensureBrands();
  const b = BRANDS.find(x => x.id === req.params.id);
  if (!b) return res.status(404).json({ error: "not found" });
  const { groupId, groupName } = req.body;
  if (!groupId) return res.status(400).json({ error: "groupId required" });
  if (!b.groupIds.includes(groupId)) {
    b.groupIds.push(groupId);
    await saveBrands();
    await appendBrandLog(b.id, { type: "group_added", groupId, groupName });
    invalidateBrandStatsCache(b.id);
  }
  res.json(b);
});

app.delete("/api/brands/:id/groups/:gid", async (req, res) => {
  await ensureBrands();
  const b = BRANDS.find(x => x.id === req.params.id);
  if (!b) return res.status(404).json({ error: "not found" });
  const gid = decodeURIComponent(req.params.gid);
  b.groupIds = b.groupIds.filter(x => x !== gid);
  await saveBrands();
  await appendBrandLog(b.id, { type: "group_removed", groupId: gid });
  invalidateBrandStatsCache(b.id);
  res.json(b);
});

// Brand stats cache — in-memory + Blob persistence (survives cold starts)
const BRAND_STATS_CACHE = new Map();
const BRAND_STATS_TTL = 60 * 60 * 1000; // 1 hour
const BRAND_STATS_STALE = 24 * 60 * 60 * 1000; // serve up to 24h stale

async function loadBrandStatsCache(brandId) {
  if (BRAND_STATS_CACHE.has(brandId)) return BRAND_STATS_CACHE.get(brandId);
  const blob = await readJson(`brand-stats-cache/${brandId}.json`);
  if (blob?.data) {
    BRAND_STATS_CACHE.set(brandId, blob);
    return blob;
  }
  return null;
}

async function saveBrandStatsCache(brandId, data) {
  const entry = { data, at: Date.now() };
  BRAND_STATS_CACHE.set(brandId, entry);
  await writeJson(`brand-stats-cache/${brandId}.json`, entry);
}

function invalidateBrandStatsCache(brandId) {
  BRAND_STATS_CACHE.delete(brandId);
  // Blob not deleted — next request will overwrite
}

app.get("/api/brands/:id/stats", async (req, res) => {
  await ensureBrands();
  const b = BRANDS.find(x => x.id === req.params.id);
  if (!b) return res.status(404).json({ error: "not found" });

  const force = req.query.refresh === "1";
  const cached = await loadBrandStatsCache(req.params.id);
  const age = cached ? Date.now() - cached.at : Infinity;

  // Serve fresh cache immediately
  if (cached && !force && age < BRAND_STATS_TTL) {
    return res.json({ ...cached.data, updatedAt: cached.at, cached: true, age });
  }

  // Serve stale cache immediately + refresh in background (stale-while-revalidate)
  if (cached && !force && age < BRAND_STATS_STALE) {
    res.json({ ...cached.data, updatedAt: cached.at, cached: true, stale: true, age });
    // Background refresh — fire-and-forget
    computeBrandStatsAndCache(b).catch(e => logError(`bg stats ${req.params.id}`, e.message));
    return;
  }

  // No cache or force refresh — compute synchronously
  try {
    const payload = await computeBrandStatsAndCache(b);
    res.json({ ...payload, updatedAt: Date.now(), cached: false });
  } catch (e) {
    if (cached) return res.json({ ...cached.data, updatedAt: cached.at, cached: true, error: e.message });
    res.status(500).json({ error: e.message });
  }
});

// Refactored: compute stats + save to cache
async function computeBrandStatsAndCache(b) {
  let adminMap;
  try { adminMap = await detectAdminOnBrandGroups(b); } catch { adminMap = new Map(); }
  // Resolve connected session's own phone so we never count ourselves as duplicate
  let myPhone = "";
  try {
    if (!SESSIONS_CACHE?.length) { try { await refreshSessions(); } catch {} }
    const s = (SESSIONS_CACHE || []).find(s => ["connected","ready"].includes((s.status||"").toLowerCase()));
    myPhone = (s?.phone_number || "").replace(/\D/g, "");
  } catch {}

  // Log admin status changes
  const prevAdmin = (await readJson(`brand-admin-state/${b.id}.json`)) || {};
  const currAdmin = {};
  for (const gid of b.groupIds) currAdmin[gid] = !!adminMap.get(gid);
  const changes = [];
  for (const gid of b.groupIds) {
    if (prevAdmin[gid] === undefined) continue;
    if (prevAdmin[gid] !== currAdmin[gid]) {
      changes.push({ gid, from: prevAdmin[gid], to: currAdmin[gid] });
    }
  }
  for (const ch of changes) {
    await appendBrandLog(b.id, {
      type: ch.to ? "admin_granted" : "admin_revoked",
      groupId: ch.gid,
    });
  }
  if (Object.keys(prevAdmin).length === 0 || changes.length) {
    await writeJson(`brand-admin-state/${b.id}.json`, currAdmin);
  }
  const savedMap = await loadSavedGroupsMap();

  const phoneGroups = new Map();
  const phoneGroupIds = new Map();
  const phoneIsAdminIn = new Map(); // phone → Set of groupIds where this phone is admin
  const groupDetails = [];
  const groupSizes = new Map();
  let latestSavedAt = null;

  for (const gid of b.groupIds) {
    const d = savedMap.get(gid);
    if (!d) { groupDetails.push({ groupId: gid, missing: true, iAmAdmin: adminMap.get(gid) || false }); continue; }
    if (!latestSavedAt || d.savedAt > latestSavedAt) latestSavedAt = d.savedAt;
    groupSizes.set(gid, d.memberCount || 0);
    groupDetails.push({
      groupId: gid,
      groupName: d.groupName,
      memberCount: d.memberCount,
      savedAt: d.savedAt,
      previousSavedAt: d.previousSavedAt,
      iAmAdmin: adminMap.get(gid) || false,
    });
    for (const m of d.members) {
      const ph = m._phone || (m.pn || m.jid || m.id || "").replace(/@.*/, "");
      if (!ph) continue;
      if (!phoneGroups.has(ph)) phoneGroups.set(ph, new Set());
      phoneGroups.get(ph).add(d.groupName || gid);
      if (!phoneGroupIds.has(ph)) phoneGroupIds.set(ph, new Set());
      phoneGroupIds.get(ph).add(gid);
      if (m.isAdmin || m.isSuperAdmin || m.admin) {
        if (!phoneIsAdminIn.has(ph)) phoneIsAdminIn.set(ph, new Set());
        phoneIsAdminIn.get(ph).add(gid);
      }
    }
  }

  // Skip admins AND self from dedup calculations — immune
  const skipPhones = new Set(phoneIsAdminIn.keys());
  if (myPhone) skipPhones.add(myPhone);

  const totalSum = groupDetails.reduce((a, g) => a + (g.memberCount || 0), 0);
  const uniqueMembers = phoneGroups.size;
  // Count duplicates EXCLUDING admins — they're immune
  let duplicates = 0;
  for (const [phone, gids] of phoneGroupIds) {
    if (skipPhones.has(phone)) continue; // admin — skip
    if (gids.size > 1) duplicates += gids.size - 1; // extra appearances
  }

  // Calculate how many duplicates are REMOVABLE — exclude admins
  let fullyRemovable = 0;
  let partiallyRemovable = 0;
  let blocked = 0;
  let removalOpsTotal = 0;
  let removalOpsPossible = 0;
  let skippedAdmins = 0;
  for (const [phone, gids] of phoneGroupIds) {
    if (gids.size <= 1) continue;
    if (skipPhones.has(phone)) { skippedAdmins++; continue; }
    const sorted = [...gids].sort((a, b) => (groupSizes.get(b)||0) - (groupSizes.get(a)||0));
    const removeFrom = sorted.slice(0, -1);
    removalOpsTotal += removeFrom.length;
    const adminsAvailable = removeFrom.filter(g => adminMap.get(g));
    removalOpsPossible += adminsAvailable.length;
    if (adminsAvailable.length === removeFrom.length) fullyRemovable++;
    else if (adminsAvailable.length > 0) partiallyRemovable++;
    else blocked++;
  }
  const inMultiple = [...phoneGroups.entries()].filter(([ph, set]) => set.size > 1 && !skipPhones.has(ph));
  const topOverlaps = inMultiple.slice(0, 20).map(([phone, set]) => ({ phone, groups: [...set] }));

  // Recent leavers from log
  let recentLeavers = [];
  try {
    const log = (await readJson(`brand-logs/${b.id}.json`)) || [];
    recentLeavers = log.filter(l => l.type === "member_left").slice(0, 10);
  } catch {}

  const payload = {
    brand: b,
    total_sum: totalSum,
    unique_members: uniqueMembers,
    duplicates,
    members_in_multiple_groups: inMultiple.length,
    top_overlaps: topOverlaps,
    groups: groupDetails,
    latest_saved_at: latestSavedAt,
    recent_leavers: recentLeavers,
    removable: {
      fully: fullyRemovable,
      partial: partiallyRemovable,
      blocked,
      ops_total: removalOpsTotal,
      ops_possible: removalOpsPossible,
      admin_groups: groupDetails.filter(g => g.iAmAdmin).length,
      non_admin_groups: groupDetails.filter(g => !g.iAmAdmin).length,
    },
  };
  await saveBrandStatsCache(b.id, payload);
  return payload;
}

// Get members list filtered by type (all/unique/duplicates/leavers) — JSON or Excel
async function computeBrandMembers(brandId, type = "unique") {
  await ensureBrands();
  const b = BRANDS.find(x => x.id === brandId);
  if (!b) return null;
  const savedMap = await loadSavedGroupsMap();
  // Resolve connected session's own phone — never list self as a duplicate
  let myPhone = "";
  try {
    if (!SESSIONS_CACHE?.length) { try { await refreshSessions(); } catch {} }
    const s = (SESSIONS_CACHE || []).find(s => ["connected","ready"].includes((s.status||"").toLowerCase()));
    myPhone = (s?.phone_number || "").replace(/\D/g, "");
  } catch {}
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
    const log = (await readJson(`brand-logs/${brandId}.json`)) || [];
    for (const l of log) if (l.type === "member_left" && l.phone) leaverPhones.add(l.phone);
  } catch {}

  const all = [...phoneMap.values()];
  const unique = all.map(m => ({ ...m, groups: [...new Set(m.groups)] }));
  // Exclude admins AND self from duplicates
  const duplicates = unique.filter(m => m.groups.length > 1 && !m.isAdmin && m.phone !== myPhone);
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
  // exports dir ensured by storage module
  const safe = data.brand.name.replace(/[^a-zA-Z0-9\u0590-\u05FF]/g, "_");
  const filename = `${safe}_${type}_${Date.now()}.xlsx`;
  const __xlsxBuf = await wb.xlsx.writeBuffer();
  const __xlsxUrl = await writeBinary(`exports/${filename}`, __xlsxBuf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
  res.json({ url: __xlsxUrl, filename, rows: data.members.length });
});

// Helper: for each saved-group, determine whether the connected session is admin.
// Uses the first connected session's cached metadata. If uncached, fetches metadata cheaply.
async function detectAdminOnBrandGroups(brand) {
  const adminMap = new Map(); // groupId → boolean
  if (!SESSIONS_CACHE?.length) {
    try { await refreshSessions(); } catch {}
  }
  const session = (SESSIONS_CACHE || []).find(s => ["connected","ready"].includes((s.status||"").toLowerCase()));
  if (!session) return adminMap;
  const sessionKey = session.api_key;
  const myPhone = (session.phone_number || "").replace(/\D/g, "");
  if (!sessionKey || !myPhone) return adminMap;

  // Try cache first
  const cached = GROUPS_CACHE.get(String(session.id));
  if (cached?.groups) {
    for (const g of cached.groups) {
      const gid = g.id || g.jid || g.groupId;
      if (gid && typeof g.iAmAdmin === "boolean") adminMap.set(gid, g.iAmAdmin);
    }
  }

  // For any brand group missing admin info, fetch metadata
  for (const gid of brand.groupIds) {
    if (adminMap.has(gid)) continue;
    try {
      const m = await wa("GET", `/groups/${gid}/metadata`, null, sessionKey);
      if (m.ok) {
        const md = m.data?.data || m.data;
        const me = (md?.participants || []).find(p => {
          const pn = (p.pn || p.jid || p.id || "").replace(/\D/g, "");
          return pn === myPhone;
        });
        adminMap.set(gid, !!(me && (me.isAdmin || me.isSuperAdmin || me.admin)));
      }
    } catch {}
  }
  return adminMap;
}

// Preview duplicates in brand (who's in multiple groups)
app.get("/api/brands/:id/duplicates", async (req, res) => {
  await ensureBrands();
  const b = BRANDS.find(x => x.id === req.params.id);
  if (!b) return res.status(404).json({ error: "not found" });
  const savedMap = await loadSavedGroupsMap();

  const phoneMap = new Map(); // phone → [{ groupId, groupName, memberCount }, ...]
  const groupSizes = new Map();
  for (const gid of b.groupIds) {
    const d = savedMap.get(gid);
    if (!d) continue;
    groupSizes.set(gid, { name: d.groupName || gid, count: d.memberCount || 0 });
    for (const m of d.members) {
      const ph = m._phone || (m.pn || m.jid || m.id || "").replace(/@.*/, "");
      if (!ph) continue;
      if (!phoneMap.has(ph)) phoneMap.set(ph, []);
      phoneMap.get(ph).push({ groupId: gid, groupName: d.groupName || gid, name: m.verifiedName || m.name || m.notify || "" });
    }
  }

  // Only duplicates (in >1 group)
  const duplicates = [];
  for (const [phone, entries] of phoneMap) {
    if (entries.length <= 1) continue;
    // Sort by group size (largest first) — that's the one to remove from by default
    const withSize = entries.map(e => ({ ...e, size: groupSizes.get(e.groupId)?.count || 0 }));
    withSize.sort((a, b) => b.size - a.size);
    duplicates.push({
      phone,
      name: entries[0].name || "",
      groups: withSize,
      keepIn: withSize[withSize.length - 1].groupId,
      removeFrom: withSize.slice(0, -1).map(g => g.groupId),
    });
  }
  res.json({ total: duplicates.length, duplicates });
});

// Actually perform removal — runs the remove via WaSender API
app.post("/api/brands/:id/remove-duplicates", async (req, res) => {
  await ensureBrands();
  const b = BRANDS.find(x => x.id === req.params.id);
  if (!b) return res.status(404).json({ error: "not found" });
  const { sessionKey, mode } = req.body;
  const savedMap = await loadSavedGroupsMap();

  // Get the connected account's own phone — NEVER remove this
  let myPhone = null;
  try {
    if (!SESSIONS_CACHE?.length) await refreshSessions();
    const session = (SESSIONS_CACHE || []).find(s => ["connected","ready"].includes((s.status||"").toLowerCase()));
    myPhone = (session?.phone_number || "").replace(/\D/g, "");
  } catch {}

  const phoneMap = new Map();
  const adminPhones = new Set();
  const groupSizes = new Map();
  const groupNames = new Map();
  for (const gid of b.groupIds) {
    const d = savedMap.get(gid);
    if (!d) continue;
    groupSizes.set(gid, d.memberCount || 0);
    groupNames.set(gid, d.groupName || gid);
    for (const m of d.members) {
      const ph = m._phone || (m.pn || m.jid || m.id || "").replace(/@.*/, "");
      if (!ph) continue;
      if (!phoneMap.has(ph)) phoneMap.set(ph, new Set());
      phoneMap.get(ph).add(gid);
      if (m.isAdmin || m.isSuperAdmin || m.admin) adminPhones.add(ph);
    }
  }

  // Plan removals: exclude self + admins
  const plan = new Map();
  let protectedSkipped = 0;
  for (const [phone, gids] of phoneMap) {
    if (gids.size <= 1) continue;
    if (phone === myPhone) { protectedSkipped++; continue; } // never remove self
    if (adminPhones.has(phone)) { protectedSkipped++; continue; } // never remove admins
    const sorted = [...gids].sort((a, b) => (groupSizes.get(b)||0) - (groupSizes.get(a)||0));
    const removeFrom = sorted.slice(0, -1);
    for (const gid of removeFrom) {
      if (!plan.has(gid)) plan.set(gid, []);
      plan.get(gid).push(phone);
    }
  }

  if (mode !== "execute") {
    const summary = [...plan.entries()].map(([gid, phones]) => ({
      groupId: gid, groupName: groupNames.get(gid), size: groupSizes.get(gid), toRemove: phones.length,
    }));
    return res.json({ preview: true, totalOps: [...plan.values()].reduce((a,p)=>a+p.length,0), groups: summary });
  }

  if (!sessionKey) return res.status(400).json({ error: "sessionKey required for execute" });

  const results = [];
  const removalEvents = []; // individual phone removals for the log
  for (const [gid, phones] of plan) {
    const groupName = groupNames.get(gid) || gid;
    const participants = phones.map(p => `${p}@s.whatsapp.net`);
    const CHUNK = 20;
    for (let i = 0; i < participants.length; i += CHUNK) {
      const batch = participants.slice(i, i + CHUNK);
      const batchPhones = phones.slice(i, i + CHUNK);
      const r = await throttledWaWrite("POST", `/groups/${gid}/participants/remove`, { participants: batch }, sessionKey);
      results.push({ groupId: gid, groupName, batch: i / CHUNK + 1, count: batch.length, ok: r.ok, error: r.error });
      // Log each phone removal (success or fail)
      for (const phone of batchPhones) {
        removalEvents.push({ phone, groupId: gid, groupName, ok: r.ok, error: r.error || null, at: new Date().toISOString() });
      }
    }
  }

  // Write brand log entries
  const groupedByGroup = new Map();
  for (const ev of removalEvents) {
    if (!groupedByGroup.has(ev.groupId)) groupedByGroup.set(ev.groupId, { ok: 0, failed: 0, phones: [] });
    const g = groupedByGroup.get(ev.groupId);
    if (ev.ok) g.ok++; else g.failed++;
    g.phones.push(ev.phone);
  }
  for (const [gid, info] of groupedByGroup) {
    await appendBrandLog(b.id, {
      type: "duplicates_removed",
      groupId: gid,
      groupName: groupNames.get(gid),
      removed: info.ok,
      failed: info.failed,
      phones: info.phones,
    });
  }

  // Append to dedicated removals log
  const removalsKey = `removals-log/${b.id}.json`;
  const existingRemovals = (await readJson(removalsKey)) || [];
  const runEntry = {
    runId: `run_${Date.now()}`,
    at: new Date().toISOString(),
    operations: removalEvents.length,
    successful: removalEvents.filter(e => e.ok).length,
    failed: removalEvents.filter(e => !e.ok).length,
    events: removalEvents,
  };
  existingRemovals.unshift(runEntry);
  await writeJson(removalsKey, existingRemovals.slice(0, 50));

  invalidateBrandStatsCache(b.id);
  res.json({ executed: true, operations: results.length, results, runId: runEntry.runId });
});

// Removals log endpoint
app.get("/api/brands/:id/removals-log", async (req, res) => {
  const log = (await readJson(`removals-log/${req.params.id}.json`)) || [];
  res.json({ log });
});

// Streaming remove-duplicates — returns ndjson progress
app.post("/api/brands/:id/remove-duplicates-stream", async (req, res) => {
  await ensureBrands();
  const b = BRANDS.find(x => x.id === req.params.id);
  if (!b) return res.status(404).json({ error: "not found" });
  const { sessionKey } = req.body;
  if (!sessionKey) return res.status(400).json({ error: "sessionKey required" });

  // Get the connected account's own phone — NEVER remove self
  let myPhone = null;
  try {
    if (!SESSIONS_CACHE?.length) await refreshSessions();
    const session = (SESSIONS_CACHE || []).find(s => ["connected","ready"].includes((s.status||"").toLowerCase()));
    myPhone = (session?.phone_number || "").replace(/\D/g, "");
  } catch {}

  const savedMap = await loadSavedGroupsMap();
  const phoneMap = new Map();
  const adminPhones = new Set();
  const groupSizes = new Map();
  const groupNames = new Map();
  for (const gid of b.groupIds) {
    const d = savedMap.get(gid);
    if (!d) continue;
    groupSizes.set(gid, d.memberCount || 0);
    groupNames.set(gid, d.groupName || gid);
    for (const m of d.members) {
      const ph = m._phone || (m.pn || m.jid || m.id || "").replace(/@.*/, "");
      if (!ph) continue;
      if (!phoneMap.has(ph)) phoneMap.set(ph, new Set());
      phoneMap.get(ph).add(gid);
      if (m.isAdmin || m.isSuperAdmin || m.admin) adminPhones.add(ph);
    }
  }

  const plan = new Map();
  let protectedSkipped = 0;
  for (const [phone, gids] of phoneMap) {
    if (gids.size <= 1) continue;
    if (phone === myPhone) { protectedSkipped++; continue; }
    if (adminPhones.has(phone)) { protectedSkipped++; continue; }
    const sorted = [...gids].sort((a, b) => (groupSizes.get(b)||0) - (groupSizes.get(a)||0));
    const removeFrom = sorted.slice(0, -1);
    for (const gid of removeFrom) {
      if (!plan.has(gid)) plan.set(gid, []);
      plan.get(gid).push(phone);
    }
  }

  res.setHeader("Content-Type", "application/x-ndjson; charset=utf-8");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("X-Accel-Buffering", "no");
  const write = (obj) => { res.write(JSON.stringify(obj) + "\n"); res.flush?.(); };

  const totalOps = [...plan.values()].reduce((a, p) => a + p.length, 0);
  write({ type: "start", totalOps, groups: plan.size });

  let processed = 0;
  const results = [];
  const removalEvents = [];
  for (const [gid, phones] of plan) {
    const groupName = groupNames.get(gid) || gid;
    write({ type: "group_start", groupId: gid, groupName, count: phones.length });
    const CHUNK = 20;
    for (let i = 0; i < phones.length; i += CHUNK) {
      const batchPhones = phones.slice(i, i + CHUNK);
      const participants = batchPhones.map(p => `${p}@s.whatsapp.net`);
      const r = await wa("POST", `/groups/${gid}/participants/remove`, { participants }, sessionKey);
      for (const phone of batchPhones) {
        processed++;
        removalEvents.push({ phone, groupId: gid, groupName, ok: r.ok, error: r.error || null, at: new Date().toISOString() });
        write({
          type: r.ok ? "removed" : "failed",
          processed, totalOps,
          groupId: gid, groupName, phone,
          error: r.error || null,
        });
      }
      results.push({ groupId: gid, batch: i / CHUNK + 1, ok: r.ok });
      // Rate limit cushion between batches (throttledWaWrite already enforces 10s)
      if (i + CHUNK < phones.length) await new Promise(rr => setTimeout(rr, 1000));
    }
    write({ type: "group_done", groupId: gid, groupName });
  }

  // Persist logs
  const groupedByGroup = new Map();
  for (const ev of removalEvents) {
    if (!groupedByGroup.has(ev.groupId)) groupedByGroup.set(ev.groupId, { ok: 0, failed: 0, phones: [] });
    const g = groupedByGroup.get(ev.groupId);
    if (ev.ok) g.ok++; else g.failed++;
    g.phones.push(ev.phone);
  }
  for (const [gid, info] of groupedByGroup) {
    await appendBrandLog(b.id, {
      type: "duplicates_removed", groupId: gid,
      groupName: groupNames.get(gid), removed: info.ok, failed: info.failed, phones: info.phones,
    });
  }
  const removalsKey = `removals-log/${b.id}.json`;
  const existing = (await readJson(removalsKey)) || [];
  existing.unshift({
    runId: `run_${Date.now()}`, at: new Date().toISOString(),
    operations: removalEvents.length,
    successful: removalEvents.filter(e => e.ok).length,
    failed: removalEvents.filter(e => !e.ok).length,
    events: removalEvents,
  });
  await writeJson(removalsKey, existing.slice(0, 50));
  invalidateBrandStatsCache(b.id);

  write({
    type: "done",
    total: removalEvents.length,
    successful: removalEvents.filter(e => e.ok).length,
    failed: removalEvents.filter(e => !e.ok).length,
  });
  res.end();
});

// Broadcast message to selected groups in the brand (with media + custom delay)
app.post("/api/brands/:id/broadcast-groups", async (req, res) => {
  await ensureBrands();
  const b = BRANDS.find(x => x.id === req.params.id);
  if (!b) return res.status(404).json({ error: "not found" });
  const { text, sessionKey, groupIds, mediaUrl, mediaType, delayMs } = req.body;
  if (!sessionKey) return res.status(400).json({ error: "sessionKey required" });
  if (!text && !mediaUrl) return res.status(400).json({ error: "text or media required" });

  const targets = (groupIds?.length ? groupIds : b.groupIds).filter(g => b.groupIds.includes(g));
  if (!targets.length) return res.status(400).json({ error: "no valid target groups" });

  // Override global delay for this call (min 3s, max 60s)
  const customDelay = Math.max(3000, Math.min(60000, delayMs || 10000));
  const results = [];
  let lastSentAt = 0;
  for (const gid of targets) {
    const sinceLast = Date.now() - lastSentAt;
    if (lastSentAt > 0 && sinceLast < customDelay) await new Promise(r => setTimeout(r, customDelay - sinceLast));
    const payload = { to: gid };
    if (text) payload.text = text;
    if (mediaUrl) {
      if (mediaType === "video") payload.videoUrl = mediaUrl;
      else if (mediaType === "document") payload.documentUrl = mediaUrl;
      else payload.imageUrl = mediaUrl; // default image
      if (text) payload.caption = text;
    }
    const r = await wa("POST", "/send-message", payload, sessionKey);
    results.push({ groupId: gid, ok: r.ok, msgId: r.data?.data?.msgId || r.data?.msgId, error: r.error });
    lastSentAt = Date.now();
  }
  const sent = results.filter(r => r.ok).length;
  await appendBrandLog(b.id, {
    type: "broadcast_groups", count: sent, failed: results.length - sent,
    text: (text || "").slice(0, 100), hasMedia: !!mediaUrl, mediaType: mediaType || null,
    targetCount: targets.length,
  });
  res.json({ total: results.length, sent, failed: results.length - sent, results });
});

// Streaming broadcast — returns ndjson progress events
app.post("/api/brands/:id/broadcast-stream", async (req, res) => {
  await ensureBrands();
  const b = BRANDS.find(x => x.id === req.params.id);
  if (!b) return res.status(404).json({ error: "not found" });
  const { text, sessionKey, groupIds, mediaUrl, mediaType, delayMs } = req.body;
  if (!sessionKey) return res.status(400).json({ error: "sessionKey required" });
  if (!text && !mediaUrl) return res.status(400).json({ error: "text or media required" });

  const targets = (groupIds?.length ? groupIds : b.groupIds).filter(g => b.groupIds.includes(g));
  if (!targets.length) return res.status(400).json({ error: "no valid target groups" });

  // Resolve group names
  const savedMap = await loadSavedGroupsMap();
  const names = new Map();
  for (const gid of targets) {
    const d = savedMap.get(gid);
    names.set(gid, d?.groupName || gid);
  }

  res.setHeader("Content-Type", "application/x-ndjson; charset=utf-8");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("X-Accel-Buffering", "no");
  const write = (obj) => { res.write(JSON.stringify(obj) + "\n"); res.flush?.(); };

  write({ type: "start", total: targets.length });

  const customDelay = Math.max(3000, Math.min(60000, delayMs || 10000));
  const results = [];
  let lastSentAt = 0;
  for (let i = 0; i < targets.length; i++) {
    const gid = targets[i];
    const gname = names.get(gid);
    const sinceLast = Date.now() - lastSentAt;
    if (lastSentAt > 0 && sinceLast < customDelay) {
      write({ type: "waiting", waitMs: customDelay - sinceLast });
      await new Promise(r => setTimeout(r, customDelay - sinceLast));
    }
    write({ type: "sending", index: i + 1, total: targets.length, groupId: gid, groupName: gname });
    const payload = { to: gid };
    if (text) payload.text = text;
    if (mediaUrl) {
      if (mediaType === "video") payload.videoUrl = mediaUrl;
      else if (mediaType === "document") payload.documentUrl = mediaUrl;
      else payload.imageUrl = mediaUrl;
      if (text) payload.caption = text;
    }
    const r = await wa("POST", "/send-message", payload, sessionKey);
    results.push({ groupId: gid, groupName: gname, ok: r.ok, error: r.error });
    write({ type: r.ok ? "sent" : "failed", index: i + 1, total: targets.length, groupId: gid, groupName: gname, error: r.error });
    lastSentAt = Date.now();
  }

  const sent = results.filter(r => r.ok).length;
  const failed = results.length - sent;
  await appendBrandLog(b.id, {
    type: "broadcast_groups", count: sent, failed,
    text: (text || "").slice(0, 100), hasMedia: !!mediaUrl, mediaType: mediaType || null,
    targetCount: targets.length, report: results,
  });
  write({ type: "done", total: results.length, sent, failed, results });
  res.end();
});

// Upload media file to Blob and get URL (for broadcast attachments)
app.post("/api/upload-media", express.raw({ type: "*/*", limit: "50mb" }), async (req, res) => {
  try {
    const ct = req.headers["content-type"] || "application/octet-stream";
    const ext = ct.split("/")[1]?.split(";")[0] || "bin";
    const filename = `broadcast-media/${Date.now()}.${ext}`;
    const url = await writeBinary(filename, req.body, ct);
    res.json({ url, mediaType: ct.startsWith("video") ? "video" : ct.startsWith("image") ? "image" : "document" });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get("/api/brands/:id/log", async (req, res) => {
  const log = (await readJson(`brand-logs/${req.params.id}.json`)) || [];
  res.json({ log });
});

// Rewrite save-group-delta to also emit brand log entries for leavers/new members
async function emitMemberChangesToBrands(groupId, groupName, previousMembers, newMembers) {
  await ensureBrands();
  // Invalidate stats cache for any brand containing this group
  for (const b of BRANDS) {
    if (b.groupIds.includes(groupId)) invalidateBrandStatsCache(b.id);
  }
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
async function ensureLocalContacts() {
  LOCAL_CONTACTS = (await readJson("local-contacts.json")) || {};
}
async function saveLocalContacts() {
  await writeJson("local-contacts.json", LOCAL_CONTACTS);
}

app.get("/api/local-contacts", async (_, res) => { await ensureLocalContacts(); res.json({ contacts: LOCAL_CONTACTS, count: Object.keys(LOCAL_CONTACTS).length }); });

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
  await writeJson(`saved-groups/${safeId}.json`, payload);
  res.json({ ok: true, file: `${safeId}.json`, rows: members.length });
});

app.get("/api/saved-groups", async (_, res) => {
  try {
    const files = (await listKeys("saved-groups")).filter(k => k.endsWith(".json"));
    const groups = [];
    for (const key of files) {
      const data = await readJson(key);
      if (data) groups.push({ file: key.replace(/^saved-groups\//, ""), groupId: data.groupId, groupName: data.groupName, memberCount: data.memberCount, savedAt: data.savedAt });
    }
    groups.sort((a, b) => new Date(b.savedAt) - new Date(a.savedAt));
    res.json({ groups });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get("/api/saved-groups/:file", async (req, res) => {
  const data = await readJson(`saved-groups/${req.params.file}`);
  if (!data) return res.status(404).json({ error: "not found" });
  res.json(data);
});

// Delete is now ARCHIVE — moves to archived-groups/
app.post("/api/saved-groups/:file/archive", async (req, res) => {
  const src = `saved-groups/${req.params.file}`;
  const data = await readJson(src);
  if (!data) return res.status(404).json({ error: "not found" });
  data.archivedAt = new Date().toISOString();
  await writeJson(`archived-groups/${req.params.file}`, data);
  await deleteKey(src);
  // Remove from any brand
  await ensureBrands();
  let changed = false;
  for (const b of BRANDS) {
    if (b.groupIds.includes(data.groupId)) {
      b.groupIds = b.groupIds.filter(g => g !== data.groupId);
      changed = true;
      await appendBrandLog(b.id, { type: "group_archived", groupId: data.groupId, groupName: data.groupName });
    }
  }
  if (changed) await saveBrands();
  res.json({ ok: true });
});

app.post("/api/archived-groups/:file/restore", async (req, res) => {
  const src = `archived-groups/${req.params.file}`;
  const data = await readJson(src);
  if (!data) return res.status(404).json({ error: "not found" });
  delete data.archivedAt;
  await writeJson(`saved-groups/${req.params.file}`, data);
  await deleteKey(src);
  res.json({ ok: true });
});

app.get("/api/archived-groups", async (_, res) => {
  try {
    const files = (await listKeys("archived-groups")).filter(k => k.endsWith(".json"));
    const groups = [];
    for (const key of files) {
      const data = await readJson(key);
      if (data) groups.push({
        file: key.replace(/^archived-groups\//, ""),
        groupId: data.groupId,
        groupName: data.groupName,
        memberCount: data.memberCount,
        savedAt: data.savedAt,
        archivedAt: data.archivedAt,
      });
    }
    groups.sort((a, b) => new Date(b.archivedAt) - new Date(a.archivedAt));
    res.json({ groups });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Kept for backward compat but disabled — returns 403
app.delete("/api/saved-groups/:file", (_, res) => {
  res.status(403).json({ error: "deletion disabled — use /archive instead" });
});

// Merge all saved groups + dedup → export Excel
app.post("/api/saved-groups/merge-export", async (req, res) => {
  const { files } = req.body;
  try {
    const allKeys = (await listKeys("saved-groups")).filter(k => k.endsWith(".json"));
    const allFiles = allKeys.map(k => k.replace(/^saved-groups\//, ""));
    const useFiles = files?.length ? files.filter(f => allFiles.includes(f)) : allFiles;
    if (!useFiles.length) return res.status(400).json({ error: "no saved groups" });

    const seen = new Map();
    let totalBeforeDedup = 0;
    for (const f of useFiles) {
      const data = await readJson(`saved-groups/${f}`);
      if (!data) continue;
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
    // exports dir ensured by storage module
    const __xlsxBuf = await wb.xlsx.writeBuffer();
  const __xlsxUrl = await writeBinary(`exports/${filename}`, __xlsxBuf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");

    res.json({
      url: __xlsxUrl,
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

// ========== SEND MESSAGE (throttled via global queue) ==========
app.post("/api/send", async (req, res) => {
  const { to, text, sessionApiKey } = req.body;
  if (!to || !text) return res.status(400).json({ error: "to + text required" });
  const r = await throttledWaWrite("POST", "/send-message", { to, text }, sessionApiKey);
  res.json(r);
});

// Broadcast to multiple numbers — uses global throttle (10s per send cross-requests)
app.post("/api/broadcast", async (req, res) => {
  const { numbers, text, sessionApiKey } = req.body;
  if (!numbers?.length || !text) return res.status(400).json({ error: "numbers[] + text required" });
  const results = [];
  for (const n of numbers) {
    const phone = normalizePhone(n);
    const r = await throttledWaWrite("POST", "/send-message", { to: phone, text }, sessionApiKey);
    results.push({ phone, ok: r.ok, error: r.error || null });
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

  // exports dir ensured by storage module
  const filename = `${(groupName || "group").replace(/[^a-zA-Z0-9\u0590-\u05FF]/g, "_")}_${Date.now()}.xlsx`;
  const __xlsxBuf = await wb.xlsx.writeBuffer();
  const __xlsxUrl = await writeBinary(`exports/${filename}`, __xlsxBuf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");

  res.json({ url: __xlsxUrl, filename, rows: participants.length });
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
  IG_CONFIG = (await readJson("ig-config.json")) || IG_CONFIG;
} catch {}

async function saveIgConfig() {
  await writeJson("ig-config.json", IG_CONFIG);
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
  // exports dir ensured by storage module
  const filename = `ig_followers_${Date.now()}.xlsx`;
  const __xlsxBuf = await wb.xlsx.writeBuffer();
  const __xlsxUrl = await writeBinary(`exports/${filename}`, __xlsxBuf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
  res.json({ url: __xlsxUrl, filename, rows: followers.length });
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
const IS_VERCEL = !!process.env.VERCEL;

if (!IS_VERCEL) {
  app.listen(PORT, () => {
    console.log(`📱 WhatsApp Groups: http://localhost:${PORT}`);
    if (!KEY) console.log("⚠️  WASENDER_API_KEY not set — add it to .env");
  });
}

export default app;

// Storage abstraction: Vercel Blob in production, local fs in dev
import fs from "fs/promises";
import path from "path";
import { put, list, del, head } from "@vercel/blob";

const USE_BLOB = !!process.env.BLOB_READ_WRITE_TOKEN;
const LOCAL_DATA = process.env.DATA_DIR || path.join(process.cwd());

// Capture recent storage errors so we can see why reads are silently failing
export const STORAGE_ERRORS = [];
function recordErr(op, key, err) {
  const entry = { at: new Date().toISOString(), op, key, err: String(err?.message || err).slice(0, 300) };
  STORAGE_ERRORS.unshift(entry);
  if (STORAGE_ERRORS.length > 50) STORAGE_ERRORS.length = 50;
  console.error(`[storage] ${op} ${key}: ${entry.err}`);
}

function resolveLocalPath(key) {
  return path.join(LOCAL_DATA, key);
}

async function tryFetch(url, opts) {
  try { return await fetch(url, { cache: "no-store", ...opts }); }
  catch { return null; }
}

export async function readJson(key) {
  if (USE_BLOB) {
    let h;
    try { h = await head(key); }
    catch (e) {
      if (e?.name === "BlobNotFoundError" || /not found/i.test(e?.message || "")) return null;
      recordErr("head", key, e);
      return null;
    }
    if (!h?.url) return null;
    const tok = process.env.BLOB_READ_WRITE_TOKEN;
    // Build a list of URL/header combos to try. Vercel serverless sometimes 403s
    // the public CDN URL — we fall back to the downloadUrl variant and Bearer.
    const attempts = [
      { url: h.url },
      { url: h.downloadUrl || h.url + "?download=1" },
      { url: h.url, headers: { Authorization: `Bearer ${tok}` } },
      { url: h.downloadUrl || h.url + "?download=1", headers: { Authorization: `Bearer ${tok}` } },
    ];
    // Retry each attempt up to 2 times with small backoff — intermittent 403
    for (let pass = 0; pass < 2; pass++) {
      for (const a of attempts) {
        const res = await tryFetch(a.url, { headers: a.headers });
        if (res?.ok) {
          try { return await res.json(); }
          catch (e) { recordErr("parse", key, e); return null; }
        }
      }
      if (pass === 0) await new Promise(r => setTimeout(r, 250));
    }
    recordErr("fetch", key, `all 4 attempts ×2 failed — size=${h.size}`);
    return null;
  }
  try { return JSON.parse(await fs.readFile(resolveLocalPath(key), "utf-8")); }
  catch { return null; }
}

export async function writeJson(key, data) {
  const body = JSON.stringify(data, null, 2);
  if (USE_BLOB) {
    await put(key, body, { access: "public", contentType: "application/json", addRandomSuffix: false, allowOverwrite: true });
    return;
  }
  const fp = resolveLocalPath(key);
  await fs.mkdir(path.dirname(fp), { recursive: true });
  await fs.writeFile(fp, body);
}

export async function listKeys(prefix) {
  if (USE_BLOB) {
    try {
      const { blobs } = await list({ prefix, limit: 1000 });
      return blobs.map(b => b.pathname);
    } catch (e) { recordErr("listKeys", prefix, e); return []; }
  }
  const dir = resolveLocalPath(prefix);
  try {
    const items = await fs.readdir(dir);
    return items.map(name => path.join(prefix, name).replace(/\\/g, "/"));
  } catch { return []; }
}

export async function deleteKey(key) {
  if (USE_BLOB) {
    const { blobs } = await list({ prefix: key, limit: 1 });
    const match = blobs.find(b => b.pathname === key);
    if (match) await del(match.url);
    return;
  }
  try { await fs.unlink(resolveLocalPath(key)); } catch {}
}

// For binary (Excel exports) — in Blob mode returns public URL; in local mode writes file
export async function writeBinary(key, buffer, contentType = "application/octet-stream") {
  if (USE_BLOB) {
    const r = await put(key, buffer, { access: "public", contentType, addRandomSuffix: false, allowOverwrite: true });
    return r.url;
  }
  const fp = resolveLocalPath(key);
  await fs.mkdir(path.dirname(fp), { recursive: true });
  await fs.writeFile(fp, buffer);
  return `/${key.replace(/\\/g, "/")}`;
}

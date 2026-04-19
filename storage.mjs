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

export async function readJson(key) {
  if (USE_BLOB) {
    try {
      const h = await head(key);
      if (!h?.url) return null;
      // Try the downloadUrl first (auth'd), then public url, then add token header
      const candidates = [h.downloadUrl, h.url].filter(Boolean);
      for (const u of candidates) {
        try {
          const res = await fetch(u, { cache: "no-store" });
          if (res.ok) return await res.json();
          if (res.status !== 403) { recordErr("fetch", key, `${res.status} ${res.statusText}`); return null; }
        } catch (e) { recordErr("fetch", key, e); }
      }
      // Last-resort: authenticated fetch with token
      const tok = process.env.BLOB_READ_WRITE_TOKEN;
      if (tok && h.url) {
        try {
          const res = await fetch(h.url, { cache: "no-store", headers: { Authorization: `Bearer ${tok}` } });
          if (res.ok) return await res.json();
          recordErr("fetch-auth", key, `${res.status} ${res.statusText}`);
        } catch (e) { recordErr("fetch-auth", key, e); }
      }
      return null;
    } catch (e) {
      if (e?.name === "BlobNotFoundError" || /not found/i.test(e?.message || "")) return null;
      recordErr("readJson", key, e);
      return null;
    }
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

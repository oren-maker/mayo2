// Storage abstraction: Vercel Blob in production, local fs in dev
import fs from "fs/promises";
import path from "path";
import { put, list, del, head } from "@vercel/blob";

const USE_BLOB = !!process.env.BLOB_READ_WRITE_TOKEN;
const LOCAL_DATA = process.env.DATA_DIR || path.join(process.cwd());

function resolveLocalPath(key) {
  return path.join(LOCAL_DATA, key);
}

export async function readJson(key) {
  if (USE_BLOB) {
    try {
      const { blobs } = await list({ prefix: key, limit: 1 });
      const match = blobs.find(b => b.pathname === key);
      if (!match) return null;
      const res = await fetch(match.url);
      if (!res.ok) return null;
      return await res.json();
    } catch { return null; }
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
    const { blobs } = await list({ prefix, limit: 1000 });
    return blobs.map(b => b.pathname);
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

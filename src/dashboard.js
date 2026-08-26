// Serves the built-in web dashboard (GET /) from a single static HTML file.
// Kept separate from server.js routing to keep both files small.
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DASHBOARD_PATH = path.join(__dirname, 'dashboard.html');

let cache = null;
let cachedAt = 0;

export function dashboardHtml() {
  // Re-read when the file changes on disk so live edits show up on refresh.
  try {
    const mtime = fs.statSync(DASHBOARD_PATH).mtimeMs;
    if (!cache || mtime !== cachedAt) {
      cache = fs.readFileSync(DASHBOARD_PATH, 'utf8');
      cachedAt = mtime;
    }
    return cache;
  } catch {
    return '<h1>dashboard.html missing</h1>';
  }
}

export function sendDashboard(res) {
  const html = dashboardHtml();
  res.writeHead(200, {
    'content-type': 'text/html; charset=utf-8',
    'cache-control': 'no-cache',
  });
  res.end(html);
}

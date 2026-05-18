#!/usr/bin/env python3
"""
Patch live n8n workflow "Socio - processing file" (PUT /process brand_name + Extract Drive).

Requires N8N_BASE_URL and N8N_API_KEY in the environment (same as n8n MCP).

Usage:
  set N8N_BASE_URL=https://your-instance.app.n8n.cloud
  set N8N_API_KEY=...
  python scripts/patch_n8n_socio_processing_brand.py
"""
from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request

WORKFLOW_ID = os.getenv("N8N_SOCIO_PROCESSING_WORKFLOW_ID", "OWq4pKSbFhCwcYmF")

EXTRACT_DRIVE_JS = r"""const base = $('Process One At A Time2').item.json;
const row = { ...base, ...$input.item.json };

function canonBrand(v) {
  const k = String(v || '').trim().toLowerCase().replace(/^@/, '').replace(/\s+/g, '').replace(/\./g, '');
  const map = { finzarc: 'Finzarc', finzarcai: 'Finzarc', deepfried: 'Deepfried', deepfriedai: 'Deepfried' };
  if (map[k]) return map[k];
  const id = String(v || row.ID || '').trim();
  const m = id.match(/IG-?0*(\d+)/i);
  if (m) {
    const n = parseInt(m[1], 10);
    if (n >= 7) return 'Deepfried';
    if (n >= 1) return 'Finzarc';
  }
  return String(v || '').trim();
}

const url = String(row.raw_gdrive_url || '').trim();
if (!url) {
  throw new Error(`raw_gdrive_url missing for ID=${row.ID ?? 'unknown'}`);
}

let fileId = null;
let m = url.match(/\/file\/d\/([a-zA-Z0-9_-]+)/);
if (m) fileId = m[1];

if (!fileId) {
  m = url.match(/[?&]id=([a-zA-Z0-9_-]+)/);
  if (m) fileId = m[1];
}

if (!fileId) {
  throw new Error(`Could not extract Drive file id from URL: ${url}`);
}

const brand_name = canonBrand(row.brand_name || base.brand_name || row.ID);

return { json: { ...row, brand_name, file_id: fileId } };"""

PROCESS_BRAND_EXPR = "={{ $('Process One At A Time2').item.json.brand_name || $('Extract Drive File ID2').item.json.brand_name }}"


def _request(method: str, url: str, body: dict | None = None) -> dict:
    api_key = (os.getenv("N8N_API_KEY") or "").strip()
    if not api_key:
        raise SystemExit("Set N8N_API_KEY (n8n Settings → API)")

    data = None
    headers = {"X-N8N-API-KEY": api_key, "Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        err = e.read().decode("utf-8", errors="replace")
        raise SystemExit(f"HTTP {e.code} {url}\n{err}") from e


def main() -> int:
    base = (os.getenv("N8N_BASE_URL") or "https://finzarc.app.n8n.cloud").rstrip("/")
    wf = _request("GET", f"{base}/api/v1/workflows/{WORKFLOW_ID}")
    nodes = wf.get("nodes") or []
    changed = 0

    for node in nodes:
        name = node.get("name", "")
        if name == "Extract Drive File ID2":
            node.setdefault("parameters", {})["jsCode"] = EXTRACT_DRIVE_JS
            changed += 1
        if name == "POST /process":
            params = node.setdefault("parameters", {})
            body = params.setdefault("bodyParameters", {}).setdefault("parameters", [])
            # Remove formBinaryData file row if present
            params["bodyParameters"]["parameters"] = [
                p
                for p in body
                if p.get("parameterType") != "formBinaryData" and p.get("name") != "file"
            ]
            body = params["bodyParameters"]["parameters"]
            for p in body:
                if p.get("name") == "brand_name":
                    p["value"] = PROCESS_BRAND_EXPR
                    changed += 1
            if not any(p.get("name") == "brand_name" for p in body):
                body.append({"name": "brand_name", "value": PROCESS_BRAND_EXPR})

    if not changed:
        print("No nodes patched — check workflow id and node names.", file=sys.stderr)
        return 1

    payload = {
        "name": wf.get("name"),
        "nodes": nodes,
        "connections": wf.get("connections"),
        "settings": wf.get("settings"),
    }
    _request("PUT", f"{base}/api/v1/workflows/{WORKFLOW_ID}", payload)
    print(f"Patched workflow {WORKFLOW_ID} ({changed} updates). Publish in n8n if you use published versions.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

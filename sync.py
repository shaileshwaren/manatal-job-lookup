"""
Manatal → Airtable sync (standalone).

Run from terminal:
  python sync.py

Avoiding 429 rate limits (Manatal allows 100 requests/60s):
  - This script paces requests (~1.5s between Manatal pages) to stay under the limit.
  - We use the documented jobs API query params: status=active, open_at__gte=<cutoff>,
    page_size=100, page=N so the API filters server-side and returns fewer pages.
  - Alternatives: use Manatal's Data Export (Admin > Data Management > Data Export);
    or ask Manatal for a higher rate limit or a filtered jobs endpoint.

Included jobs: open_at/created_at in last 3 months, not archived, status == "active".
JD is stripped of HTML/CSS to plain text (code-only by default). Optionally set
USE_AI_JD_EXTRACT=1 and OPENAI_API_KEY to use OpenAI to extract plain text from HTML.
Manatal jobs return organization (int ID) only; client_name from GET /organizations/.

FIELD MAPPING (Manatal API → internal → Airtable table)
──────────────────────────────────────────────────────
job_id (text)    raw["id"]
job_name         raw["position_name"]
jd (plain text)  raw["description"] then HTML/CSS stripped
client_id (text) raw["organization"] (integer ID)
client_name      from GET /organizations/ (id -> name)
word_cnt (int)   len(jd.split())
published date   raw["open_at"] or raw["created_at"] for filtering
"""
import asyncio
import html
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import httpx
from dotenv import load_dotenv

load_dotenv()

MANATAL_API_TOKEN = os.getenv("MANATAL_API_TOKEN")
AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_ID = os.getenv("AIRTABLE_TABLE_ID")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
USE_AI_JD_EXTRACT = os.getenv("USE_AI_JD_EXTRACT", "").strip().lower() in ("1", "true", "yes")


def _require_env(name: str, value: Optional[str]) -> None:
    if not value:
        raise RuntimeError(f"Missing required env: {name}")


def compute_cutoff_date() -> datetime:
    return datetime.now(timezone.utc) - timedelta(days=90)


def compute_jd_word_count(jd: str) -> int:
    return len(jd.split())


def _load_manatal_jobs_config() -> Dict[str, Any]:
    """
    Load jobs API query config from manatal_jobs_config.json, with sane defaults.

    File shape (all keys optional):
    {
      "params": {
        "status": "active",
        "is_published": "true",
        "city": "",
        "state": "",
        "is_remote": null,
        ...
      },
      "page_size": 100
    }

    - Any param with value null/"" is skipped.
    - We always add open_at__gte (based on cutoff) and page/page_size in code.
    """
    global _MANATAL_JOBS_CONFIG_CACHE
    if _MANATAL_JOBS_CONFIG_CACHE is not None:
        return _MANATAL_JOBS_CONFIG_CACHE

    cfg: Dict[str, Any] = {
        "params": {
            "status": "active",
            "is_published": "true",
        },
        "page_size": 100,
    }
    if MANATAL_JOBS_CONFIG_FILE.exists():
        try:
            raw = json.loads(MANATAL_JOBS_CONFIG_FILE.read_text())
            if isinstance(raw, dict):
                user_params = raw.get("params")
                if isinstance(user_params, dict):
                    cfg["params"].update(user_params)
                for k, v in raw.items():
                    if k != "params":
                        cfg[k] = v
        except Exception:
            # On any error, fall back to defaults
            pass
    _MANATAL_JOBS_CONFIG_CACHE = cfg
    return cfg


def _jd_to_plain_text(html_content: str) -> str:
    """Strip HTML/CSS with regex and return plain text (code-only, no AI)."""
    if not html_content or not isinstance(html_content, str):
        return ""
    text = re.sub(r"<script[^>]*>[\s\S]*?</script>", " ", html_content, flags=re.IGNORECASE)
    text = re.sub(r"<style[^>]*>[\s\S]*?</style>", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"</(p|div|br|tr|li|h[1-6])[^>]*>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n\s*\n", "\n\n", text)
    return text.strip()


# Max chars to send to OpenAI for JD extraction (to stay within context and cost)
JD_AI_MAX_INPUT_CHARS = 12000


def _jd_to_plain_text_with_ai(html_content: str) -> str:
    """Use OpenAI to extract plain text from HTML job description. Falls back to code strip on error."""
    if not html_content or not isinstance(html_content, str):
        return ""
    if not OPENAI_API_KEY:
        return _jd_to_plain_text(html_content)
    truncated = html_content[:JD_AI_MAX_INPUT_CHARS] if len(html_content) > JD_AI_MAX_INPUT_CHARS else html_content
    try:
        with httpx.Client(timeout=30.0) as client:
            resp = client.post(
                "https://api.openai.com/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {OPENAI_API_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": "gpt-4o-mini",
                    "messages": [
                        {
                            "role": "system",
                            "content": "Extract only the plain text from the user's job description. Remove all HTML tags, links, and formatting. Keep paragraphs and list structure as readable text. Output nothing but the plain text, no explanation.",
                        },
                        {"role": "user", "content": truncated},
                    ],
                    "max_tokens": 4096,
                },
            )
            if resp.status_code != 200:
                return _jd_to_plain_text(html_content)
            data = resp.json()
            choice = (data.get("choices") or [None])[0]
            if not choice:
                return _jd_to_plain_text(html_content)
            text = (choice.get("message") or {}).get("content") or ""
            return text.strip() or _jd_to_plain_text(html_content)
    except Exception:
        return _jd_to_plain_text(html_content)


def _jd_to_plain_text_choose(html_content: str) -> str:
    """Use AI extraction if enabled and key set, else code-only strip."""
    if USE_AI_JD_EXTRACT and OPENAI_API_KEY:
        return _jd_to_plain_text_with_ai(html_content)
    return _jd_to_plain_text(html_content)


def _is_job_included(raw: Dict[str, Any], cutoff: datetime, published_at_parsed: Optional[datetime]) -> bool:
    """Only include jobs published in last 3 months, not archived, status active."""
    if published_at_parsed is None or published_at_parsed < cutoff:
        return False
    # Exclude archived (common field names)
    if raw.get("is_archived") is True or raw.get("archived") is True:
        return False
    if raw.get("is_archived") in (1, "1", "true") or raw.get("archived") in (1, "1", "true"):
        return False
    # Include only when status is active/open (if API sends status)
    status = raw.get("status") or raw.get("state") or raw.get("job_status")
    if status is not None:
        if isinstance(status, str):
            status = status.lower().strip()
        else:
            status = str(status).lower()
        active_values = ("active", "open", "published", "live")
        if status not in active_values:
            return False
    return True


def normalize_manatal_job(raw: Dict[str, Any]) -> Dict[str, Any]:
    # Manatal API: id (int), position_name, description (HTML), organization (int), status, open_at/created_at
    job_id = str(raw.get("id") or raw.get("uuid") or "")
    job_name = (
        raw.get("position_name")
        or raw.get("job_title")
        or raw.get("title")
        or ""
    )
    jd_raw = (
        raw.get("description")
        or raw.get("job_description")
        or raw.get("details")
        or ""
    )
    jd = _jd_to_plain_text_choose(jd_raw)
    # Manatal returns organization as integer ID only; client_name filled from /organizations/ later
    org_val = raw.get("organization")
    if isinstance(org_val, dict):
        client_id = str(org_val.get("id") or org_val.get("uuid") or "")
    elif org_val is not None:
        client_id = str(org_val)
    else:
        client = raw.get("client") or raw.get("company")
        client_id = str((client.get("id") or client.get("uuid") or "")) if isinstance(client, dict) else ""
    client_name = ""  # filled from organizations API in run_sync_async
    jd_word_cnt = compute_jd_word_count(jd)
    # Prefer open_at (when job opened) then created_at for "published" date
    published_at = (
        raw.get("open_at")
        or raw.get("published_at")
        or raw.get("created_at")
        or raw.get("updated_at")
    )
    return {
        "job_id": job_id,
        "job_name": job_name,
        "jd": jd,
        "client_id": client_id,
        "client_name": client_name,
        "jd_word_cnt": jd_word_cnt,
        "published_at": published_at,
    }


def _parse_iso_datetime(value: Any) -> Optional[datetime]:
    if not value or not isinstance(value, str):
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


# Safeguards so the script never runs forever
# Manatal allows 100 requests per 60 seconds. We stay under by waiting between each request.
MAX_MANATAL_PAGES = 100
MAX_429_RETRIES = 3
PAGE_DELAY_SEC = 1.5   # ~40 requests/min, under 100/min limit
MIN_SYNC_INTERVAL_SEC = 60  # refuse to run if last sync was less than this ago
LAST_RUN_FILE = Path(__file__).resolve().parent / ".last_sync_time"
MANATAL_JOBS_CONFIG_FILE = Path(__file__).resolve().parent / "manatal_jobs_config.json"

_MANATAL_JOBS_CONFIG_CACHE: Optional[Dict[str, Any]] = None


def _guard_recent_run() -> None:
    """Refuse to run if we already ran recently (avoids hammering Manatal)."""
    if LAST_RUN_FILE.exists():
        try:
            last = float(LAST_RUN_FILE.read_text().strip() or "0")
        except (ValueError, OSError):
            last = 0
        elapsed = time.time() - last
        if elapsed < MIN_SYNC_INTERVAL_SEC:
            raise RuntimeError(
                f"Sync was run {elapsed:.0f}s ago. Wait at least {MIN_SYNC_INTERVAL_SEC}s between runs."
            )
    LAST_RUN_FILE.write_text(str(time.time()))


MANATAL_ORG_PAGE_SIZE = 100
MAX_ORG_PAGES = 500
ORG_CONCURRENCY = 3  # max parallel org detail requests


async def fetch_organizations() -> Dict[str, str]:
    """Fetch all organizations from Manatal; return map org_id -> name for filling client_name."""
    _require_env("MANATAL_API_TOKEN", MANATAL_API_TOKEN)
    url = "https://api.manatal.com/open/v3/organizations/"
    headers = {
        "Authorization": f"Token {MANATAL_API_TOKEN}",
        "Accept": "application/json",
    }
    result: Dict[str, str] = {}
    params: Dict[str, Any] = {"page_size": MANATAL_ORG_PAGE_SIZE}
    page = 0
    async with httpx.AsyncClient(timeout=30.0) as client:
        while page < MAX_ORG_PAGES:
            page += 1
            resp = await client.get(url, headers=headers, params=params)
            if resp.status_code != 200:
                break
            data = resp.json()
            for rec in data.get("results") or (data if isinstance(data, list) else []):
                if isinstance(rec, dict):
                    oid = rec.get("id")
                    name = rec.get("name") or ""
                    if oid is not None:
                        result[str(oid)] = (name or "").strip()
            next_url = data.get("next") if isinstance(data, dict) else None
            if not next_url:
                break
            url = next_url
            params = {"page_size": MANATAL_ORG_PAGE_SIZE}
            await asyncio.sleep(PAGE_DELAY_SEC)
    return result


async def fetch_organizations_for_ids(org_ids: Set[str]) -> Dict[str, str]:
    """Fetch only the organizations whose IDs we saw on jobs."""
    if not org_ids:
        return {}
    _require_env("MANATAL_API_TOKEN", MANATAL_API_TOKEN)
    base_url = "https://api.manatal.com/open/v3/organizations/"
    headers = {
        "Authorization": f"Token {MANATAL_API_TOKEN}",
        "Accept": "application/json",
    }
    result: Dict[str, str] = {}
    sem = asyncio.Semaphore(ORG_CONCURRENCY)

    async def _fetch_one(oid: str, client: httpx.AsyncClient) -> None:
        async with sem:
            params = {"id": oid}
            resp = await client.get(base_url, headers=headers, params=params)
            if resp.status_code != 200:
                return
            data = resp.json()
            records = data.get("results") if isinstance(data, dict) else data
            if isinstance(records, dict):
                recs_list = [records]
            elif isinstance(records, list):
                recs_list = records
            else:
                return
            for rec in recs_list:
                if not isinstance(rec, dict):
                    continue
                rid = rec.get("id")
                name = rec.get("name") or ""
                if rid is not None:
                    result[str(rid)] = name.strip()

    async with httpx.AsyncClient(timeout=30.0) as client:
        tasks = [_fetch_one(oid, client) for oid in sorted(org_ids)]
        await asyncio.gather(*tasks)
    return result

# Documented Manatal jobs API query params: status, is_published, open_at__gte, page_size, page
MANATAL_PAGE_SIZE = 100


def _manatal_filter_params(cutoff: datetime, page_num: int = 1) -> Dict[str, Any]:
    """
    Query params for GET /open/v3/jobs/ so the API filters server-side.
    Base behavior (overridable via manatal_jobs_config.json):
      - Load static filters from config["params"] (skipping null/"").
      - Always add open_at__gte=YYYY-MM-DD based on cutoff (last 90 days by default).
      - Use page_size from config or default 100.
      - Set page=N based on pagination.

    To change filters, edit manatal_jobs_config.json instead of code.
    """
    cfg = _load_manatal_jobs_config()
    base_params = cfg.get("params") or {}
    params: Dict[str, Any] = {}
    if isinstance(base_params, dict):
        for k, v in base_params.items():
            if v is None:
                continue
            if isinstance(v, str) and not v.strip():
                continue
            params[k] = v
    params["open_at__gte"] = cutoff.strftime("%Y-%m-%d")
    page_size = cfg.get("page_size") or MANATAL_PAGE_SIZE
    params["page_size"] = int(page_size)
    params["page"] = page_num
    return params


def _parse_next_page(next_url: Optional[str]) -> Optional[int]:
    """Extract page number from Manatal next URL (e.g. ...?page=2 or ...&page=2)."""
    if not next_url:
        return None
    match = re.search(r"[?&]page=(\d+)", next_url, re.I)
    return int(match.group(1)) if match else None


async def fetch_recent_jobs_from_manatal(cutoff: datetime) -> List[Dict[str, Any]]:
    _require_env("MANATAL_API_TOKEN", MANATAL_API_TOKEN)
    base_url = "https://api.manatal.com/open/v3/jobs/"
    headers = {
        "Authorization": f"Token {MANATAL_API_TOKEN}",
        "Accept": "application/json",
    }
    jobs: List[Dict[str, Any]] = []
    page_num = 1
    retries_429 = 0

    async with httpx.AsyncClient(timeout=30.0) as client:
        while True:
            if page_num > MAX_MANATAL_PAGES:
                print(f"  (stopped after {MAX_MANATAL_PAGES} pages to avoid runaway)", flush=True)
                break
            print(f"  Manatal page {page_num}...", flush=True)
            params = _manatal_filter_params(cutoff, page_num)

            resp = await client.get(base_url, headers=headers, params=params)
            if resp.status_code == 429:
                retries_429 += 1
                if retries_429 > MAX_429_RETRIES:
                    raise RuntimeError(
                        "Manatal rate limit (429) hit too many times. Wait a minute and run again."
                    )
                try:
                    body = resp.json()
                    msg = body.get("detail", "") if isinstance(body, dict) else resp.text
                    match = re.search(r"available in (\d+)\s*seconds", msg, re.I)
                    wait_sec = min(int(match.group(1)), 60) if match else 30
                except Exception:
                    wait_sec = 30
                print(f"  Rate limited; waiting {wait_sec}s (retry {retries_429}/{MAX_429_RETRIES})...", flush=True)
                await asyncio.sleep(wait_sec)
                continue
            retries_429 = 0  # reset after success

            if resp.status_code != 200:
                raise RuntimeError(
                    f"Manatal error: {resp.status_code} {resp.text}"
                )
            data = resp.json()
            raw_jobs = data.get("results") if isinstance(data, dict) else None
            if raw_jobs is None:
                raw_jobs = data if isinstance(data, list) else []
            for raw_job in raw_jobs:
                normalized = normalize_manatal_job(raw_job)
                published_at_parsed = _parse_iso_datetime(normalized.get("published_at"))
                if not _is_job_included(raw_job, cutoff, published_at_parsed):
                    continue
                jobs.append(normalized)
            next_url = data.get("next") if isinstance(data, dict) else None
            next_page = _parse_next_page(next_url)
            if next_page is None:
                break
            page_num = next_page
            await asyncio.sleep(PAGE_DELAY_SEC)  # stay under Manatal 100 req/min
    # Ensure one row per job_id (API should not repeat across pages; guard against it)
    seen_ids: Dict[str, Dict[str, Any]] = {}
    for job in jobs:
        jid = str(job.get("job_id") or "").strip()
        if jid and jid not in seen_ids:
            seen_ids[jid] = job
    jobs = list(seen_ids.values())
    print(f"  Fetched {len(jobs)} jobs from Manatal (last 3 months, status=active).", flush=True)
    return jobs


# Sync loads existing rows to decide create vs update. If we cap pages too low, we never
# see some job_ids and create a second row (dupe). Dedupe loads up to 500 pages to find
# dupes; sync uses the same cap so we don't create new dupes after dedupe.
MAX_AIRTABLE_PAGES = 500
MAX_AIRTABLE_PAGES_DEDUPE = 500
AIRTABLE_DELETE_DELAY_SEC = 0.25  # stay under 5 req/s per base

async def delete_airtable_record(record_id: str) -> None:
    """Delete a single record from the Airtable table."""
    _require_env("AIRTABLE_TOKEN", AIRTABLE_TOKEN)
    _require_env("AIRTABLE_BASE_ID", AIRTABLE_BASE_ID)
    _require_env("AIRTABLE_TABLE_ID", AIRTABLE_TABLE_ID)
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_ID}/{record_id}"
    headers = {"Authorization": f"Bearer {AIRTABLE_TOKEN}"}
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.delete(url, headers=headers)
        if resp.status_code not in (200, 204):
            raise RuntimeError(
                f"Airtable delete error: {resp.status_code} {resp.text}"
            )


async def dedupe_airtable_by_job_id() -> int:
    """Load all records, group by job_id; for each job_id with multiple rows, keep one and delete the rest. Returns count deleted."""
    _require_env("AIRTABLE_TOKEN", AIRTABLE_TOKEN)
    _require_env("AIRTABLE_BASE_ID", AIRTABLE_BASE_ID)
    _require_env("AIRTABLE_TABLE_ID", AIRTABLE_TABLE_ID)
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_ID}"
    headers = {"Authorization": f"Bearer {AIRTABLE_TOKEN}", "Accept": "application/json"}
    params: Dict[str, Any] = {"pageSize": 100}
    rows: List[Dict[str, str]] = []
    page = 0

    async with httpx.AsyncClient(timeout=30.0) as client:
        while True:
            if page >= MAX_AIRTABLE_PAGES_DEDUPE:
                print(f"  (dedupe: stopped loading after {MAX_AIRTABLE_PAGES_DEDUPE} pages)", flush=True)
                break
            page += 1
            resp = await client.get(url, headers=headers, params=params)
            if resp.status_code != 200:
                raise RuntimeError(f"Airtable list error: {resp.status_code} {resp.text}")
            data = resp.json()
            for rec in data.get("records", []):
                job_id_key = str(rec.get("fields", {}).get("job_id") or "").strip()
                if not job_id_key:
                    continue
                rows.append({"record_id": rec["id"], "job_id": job_id_key})
            offset = data.get("offset")
            if not offset:
                break
            params["offset"] = offset

    by_job_id: Dict[str, List[str]] = {}
    for r in rows:
        by_job_id.setdefault(r["job_id"], []).append(r["record_id"])
    to_delete: List[str] = []
    for job_id, record_ids in by_job_id.items():
        if len(record_ids) > 1:
            to_delete.extend(record_ids[1:])
    if not to_delete:
        return 0
    print(f"  Removing {len(to_delete)} duplicate row(s) (same job_id)...", flush=True)
    for i, record_id in enumerate(to_delete):
        await delete_airtable_record(record_id)
        if (i + 1) % 50 == 0 or i + 1 == len(to_delete):
            print(f"  Deleted {i + 1}/{len(to_delete)}...", flush=True)
        await asyncio.sleep(AIRTABLE_DELETE_DELAY_SEC)
    return len(to_delete)


async def fetch_all_airtable_records() -> Dict[str, Dict[str, Any]]:
    _require_env("AIRTABLE_TOKEN", AIRTABLE_TOKEN)
    _require_env("AIRTABLE_BASE_ID", AIRTABLE_BASE_ID)
    _require_env("AIRTABLE_TABLE_ID", AIRTABLE_TABLE_ID)
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_ID}"
    headers = {
        "Authorization": f"Bearer {AIRTABLE_TOKEN}",
        "Accept": "application/json",
    }
    records_by_job_id: Dict[str, Dict[str, Any]] = {}
    params: Dict[str, Any] = {"pageSize": 100}
    page = 0

    async with httpx.AsyncClient(timeout=30.0) as client:
        while True:
            if page >= MAX_AIRTABLE_PAGES:
                print(f"  (stopped after {MAX_AIRTABLE_PAGES} Airtable pages)", flush=True)
                break
            page += 1
            print(f"  Airtable page {page}...", flush=True)
            resp = await client.get(url, headers=headers, params=params)
            if resp.status_code != 200:
                raise RuntimeError(
                    f"Airtable list error: {resp.status_code} {resp.text}"
                )
            data = resp.json()
            for rec in data.get("records", []):
                fields = rec.get("fields", {})
                job_id_key = str(fields.get("job_id") or "").strip()
                if not job_id_key:
                    continue
                records_by_job_id[job_id_key] = {
                    "record_id": rec.get("id"),
                    "fields": fields,
                }
            offset = data.get("offset")
            if not offset:
                break
            params["offset"] = offset
    print(f"  Loaded {len(records_by_job_id)} existing records from Airtable.", flush=True)
    return records_by_job_id


async def create_airtable_record(job: Dict[str, Any]) -> None:
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_ID}"
    headers = {
        "Authorization": f"Bearer {AIRTABLE_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {
        "fields": {
            "job_id": str(job["job_id"] or ""),
            "job_name": job["job_name"],
            "jd": job["jd"],
            "client_id": str(job["client_id"] or ""),
            "client_name": job["client_name"],
            "word_cnt": job["jd_word_cnt"],
        }
    }
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(url, headers=headers, json=payload)
        if resp.status_code not in (200, 201):
            raise RuntimeError(
                f"Airtable create error: {resp.status_code} {resp.text}"
            )


async def create_airtable_records_batch(jobs: List[Dict[str, Any]]) -> None:
    """Create up to 10 Airtable records in a single request."""
    if not jobs:
        return
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_ID}"
    headers = {
        "Authorization": f"Bearer {AIRTABLE_TOKEN}",
        "Content-Type": "application/json",
    }
    records = []
    for job in jobs:
        records.append({
            "fields": {
                "job_id": str(job["job_id"] or ""),
                "job_name": job["job_name"],
                "jd": job["jd"],
                "client_id": str(job["client_id"] or ""),
                "client_name": job["client_name"],
                "word_cnt": job["jd_word_cnt"],
            }
        })
    payload = {"records": records}
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(url, headers=headers, json=payload)
        if resp.status_code not in (200, 201):
            raise RuntimeError(
                f"Airtable batch create error: {resp.status_code} {resp.text}"
            )


async def update_airtable_record(record_id: str, job: Dict[str, Any]) -> None:
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_ID}/{record_id}"
    headers = {
        "Authorization": f"Bearer {AIRTABLE_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {
        "fields": {
            "job_name": job["job_name"],
            "jd": job["jd"],
            "word_cnt": job["jd_word_cnt"],
        }
    }
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.patch(url, headers=headers, json=payload)
        if resp.status_code not in (200, 201):
            raise RuntimeError(
                f"Airtable update error: {resp.status_code} {resp.text}"
            )


async def update_airtable_records_batch(records: List[Dict[str, Any]]) -> None:
    """
    Batch update up to 10 Airtable records. Each item in records must have:
      - record_id
      - job (normalized Manatal job dict)
    """
    if not records:
        return
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_ID}"
    headers = {
        "Authorization": f"Bearer {AIRTABLE_TOKEN}",
        "Content-Type": "application/json",
    }
    payload_records = []
    for item in records:
        record_id = item["record_id"]
        job = item["job"]
        payload_records.append({
            "id": record_id,
            "fields": {
                "job_name": job["job_name"],
                "jd": job["jd"],
                "word_cnt": job["jd_word_cnt"],
            },
        })
    payload = {"records": payload_records}
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.patch(url, headers=headers, json=payload)
        if resp.status_code not in (200, 201):
            raise RuntimeError(
                f"Airtable batch update error: {resp.status_code} {resp.text}"
            )


async def run_sync_async() -> Dict[str, Any]:
    """Fetch Manatal jobs (last 3 months), sync to Airtable; return updatedCount, updatedJobs, airtableUrl."""
    cutoff = compute_cutoff_date()
    print("Fetching jobs from Manatal...", flush=True)
    manatal_jobs = await fetch_recent_jobs_from_manatal(cutoff)
    # Build set of organization IDs we actually need names for
    org_ids: Set[str] = set()
    for job in manatal_jobs:
        cid = str(job.get("client_id") or "").strip()
        if cid:
            org_ids.add(cid)
    print(f"Fetching organizations from Manatal for {len(org_ids)} organization(s)...", flush=True)
    org_map = await fetch_organizations_for_ids(org_ids)
    # If the id-filtered fetch didn't return names (API may not support ?id=),
    # fall back to paging through organizations and fill what we can.
    if org_ids and len(org_map) < len(org_ids):
        print("  Fallback: paging through organizations to resolve missing names...", flush=True)
        all_orgs = await fetch_organizations()
        for oid in org_ids:
            if oid in all_orgs and oid not in org_map:
                org_map[oid] = all_orgs[oid]
    print(f"  Loaded {len(org_map)} organizations.", flush=True)
    for job in manatal_jobs:
        job["client_name"] = org_map.get(str(job["client_id"]), "") or job.get("client_name") or ""
    print("Fetching existing records from Airtable...", flush=True)
    airtable_records = await fetch_all_airtable_records()
    updated_jobs: List[Dict[str, Any]] = []
    total = len(manatal_jobs)
    print(f"Syncing {total} jobs to Airtable...", flush=True)

    # Collect creates and updates, then send to Airtable in small batches
    creates_batch: List[Dict[str, Any]] = []
    updates_batch: List[Dict[str, Any]] = []

    async def _flush_creates() -> None:
        nonlocal creates_batch
        if creates_batch:
            await create_airtable_records_batch(creates_batch)
            creates_batch = []

    async def _flush_updates() -> None:
        nonlocal updates_batch
        if updates_batch:
            await update_airtable_records_batch(updates_batch)
            updates_batch = []

    for i, job in enumerate(manatal_jobs, 1):
        job_id_key = str(job["job_id"] or "").strip()
        if not job_id_key:
            continue
        if i % 50 == 0 or i == total:
            print(f"  Progress {i}/{total}...", flush=True)
        existing = airtable_records.get(job_id_key)
        if not existing:
            creates_batch.append(job)
            if len(creates_batch) >= 10:
                await _flush_creates()
            continue
        existing_fields = existing.get("fields", {})
        try:
            existing_word_cnt = int(existing_fields.get("word_cnt") or 0)
        except (TypeError, ValueError):
            existing_word_cnt = 0
        needs_jd_update = existing_word_cnt != job["jd_word_cnt"]
        existing_client_name = (existing_fields.get("client_name") or "").strip()
        desired_client_name = (job.get("client_name") or "").strip()
        needs_client_name_update = bool(desired_client_name) and (existing_client_name != desired_client_name)
        if needs_jd_update or needs_client_name_update:
            record_id = existing["record_id"]
            updates_batch.append({"record_id": record_id, "job": job})
            if needs_jd_update:
                updated_jobs.append({
                    "job_id": job_id_key,
                    "job_name": job["job_name"],
                    "client_name": job["client_name"],
                    "old_jd_word_cnt": existing_word_cnt,
                    "new_jd_word_cnt": job["jd_word_cnt"],
                })
            if len(updates_batch) >= 10:
                await _flush_updates()

    # Flush any remaining batched writes
    await _flush_creates()
    await _flush_updates()

    airtable_url = f"https://airtable.com/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_ID}"
    return {
        "updatedCount": len(updated_jobs),
        "updatedJobs": updated_jobs,
        "airtableUrl": airtable_url,
    }


def run_sync() -> Dict[str, Any]:
    """Synchronous wrapper: run_sync_async() via asyncio.run()."""
    return asyncio.run(run_sync_async())


if __name__ == "__main__":
    dedupe_only = "--dedupe-only" in sys.argv
    if dedupe_only:
        print("Running Airtable dedupe only (remove duplicate job_id rows)...", flush=True)
        try:
            deleted = asyncio.run(dedupe_airtable_by_job_id())
            print(f"Done. Removed {deleted} duplicate(s).")
        except Exception as e:
            print("Error:", e, file=sys.stderr)
            sys.exit(1)
        sys.exit(0)
    print("Running Manatal -> Airtable sync (last 3 months)...", flush=True)
    try:
        _guard_recent_run()
        result = run_sync()
        n = result["updatedCount"]
        print(f"Done. Updated {n} job(s).")
        if n > 0:
            for j in result["updatedJobs"]:
                print(
                    f"  - {j['job_id']} | {j['job_name']} | {j['client_name']} | "
                    f"{j['old_jd_word_cnt']} -> {j['new_jd_word_cnt']} words"
                )
        print("Airtable:", result["airtableUrl"])
    except Exception as e:
        print("Error:", e, file=sys.stderr)
        sys.exit(1)

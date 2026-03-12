import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi import Request


load_dotenv()

MANATAL_API_TOKEN = os.getenv("MANATAL_API_TOKEN")
AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_ID = os.getenv("AIRTABLE_TABLE_ID")


app = FastAPI(title="Manatal → Airtable Job Sync")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))


@app.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    """
    Serve the main UI.
    """
    return templates.TemplateResponse("index.html", {"request": request})


def _require_env_var(name: str, value: Optional[str]) -> None:
    if not value:
        raise HTTPException(
            status_code=500,
            detail=f"Missing required environment variable: {name}",
        )


def compute_cutoff_date() -> datetime:
    """
    Compute cutoff date for the last ~3 months (90 days).
    """
    return datetime.now(timezone.utc) - timedelta(days=90)


def compute_jd_word_count(jd: str) -> int:
    return len(jd.split())


def normalize_manatal_job(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize a Manatal job payload into the shape we need.

    This function is defensive because Manatal fields can vary slightly.
    """
    job_id = str(raw.get("id") or raw.get("uuid") or "")

    # Job name: try a few likely fields.
    job_name = (
        raw.get("position_name")
        or raw.get("job_title")
        or raw.get("title")
        or ""
    )

    # Job description: try a few likely fields.
    jd = (
        raw.get("description")
        or raw.get("job_description")
        or raw.get("details")
        or ""
    )

    client = raw.get("client") or raw.get("company") or {}
    if not isinstance(client, dict):
        client = {}

    client_id = str(client.get("id") or client.get("uuid") or "")
    client_name = client.get("name") or client.get("company_name") or ""

    jd_word_cnt = compute_jd_word_count(jd)

    # Published / created date for filtering – try a few common fields.
    published_at = (
        raw.get("published_at")
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
        # Manatal usually returns ISO 8601 timestamps.
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


async def fetch_recent_jobs_from_manatal(cutoff: datetime) -> List[Dict[str, Any]]:
    """
    Fetch jobs from Manatal and filter them client-side to only keep those
    published/created within the last 3 months.
    """
    _require_env_var("MANATAL_API_TOKEN", MANATAL_API_TOKEN)

    url = "https://api.manatal.com/open/v3/jobs/"
    headers = {
        "Authorization": f"Token {MANATAL_API_TOKEN}",
        "Accept": "application/json",
    }

    jobs: List[Dict[str, Any]] = []
    params: Dict[str, Any] = {}

    async with httpx.AsyncClient(timeout=30.0) as client:
        while True:
            resp = await client.get(url, headers=headers, params=params)
            if resp.status_code != 200:
                raise HTTPException(
                    status_code=502,
                    detail=f"Error fetching jobs from Manatal: {resp.status_code} {resp.text}",
                )

            data = resp.json()

            # Manatal's API returns a `results` list in many endpoints; fall back to the
            # whole payload if that isn't present.
            raw_jobs = data.get("results") if isinstance(data, dict) else None
            if raw_jobs is None:
                if isinstance(data, list):
                    raw_jobs = data
                else:
                    raw_jobs = []

            for raw_job in raw_jobs:
                normalized = normalize_manatal_job(raw_job)
                published_at = _parse_iso_datetime(normalized.get("published_at"))
                if published_at is None or published_at < cutoff:
                    continue
                jobs.append(normalized)

            # Handle pagination if present.
            next_url = None
            if isinstance(data, dict):
                next_url = data.get("next")

            if not next_url:
                break

            url = next_url
            params = {}

    return jobs


async def fetch_all_airtable_records() -> Dict[str, Dict[str, Any]]:
    """
    Fetch all records from the Airtable table and index them by job_id.
    """
    _require_env_var("AIRTABLE_TOKEN", AIRTABLE_TOKEN)
    _require_env_var("AIRTABLE_BASE_ID", AIRTABLE_BASE_ID)
    _require_env_var("AIRTABLE_TABLE_ID", AIRTABLE_TABLE_ID)

    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_ID}"
    headers = {
        "Authorization": f"Bearer {AIRTABLE_TOKEN}",
        "Accept": "application/json",
    }

    records_by_job_id: Dict[str, Dict[str, Any]] = {}
    params: Dict[str, Any] = {"pageSize": 100}

    async with httpx.AsyncClient(timeout=30.0) as client:
        while True:
            resp = await client.get(url, headers=headers, params=params)
            if resp.status_code != 200:
                raise HTTPException(
                    status_code=502,
                    detail=f"Error fetching records from Airtable: {resp.status_code} {resp.text}",
                )

            data = resp.json()
            records = data.get("records", [])
            for rec in records:
                fields = rec.get("fields", {})
                job_id = str(fields.get("job_id") or "")
                if not job_id:
                    continue
                records_by_job_id[job_id] = {
                    "record_id": rec.get("id"),
                    "fields": fields,
                }

            offset = data.get("offset")
            if not offset:
                break
            params["offset"] = offset

    return records_by_job_id


async def create_airtable_record(job: Dict[str, Any]) -> None:
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_ID}"
    headers = {
        "Authorization": f"Bearer {AIRTABLE_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {
        "fields": {
            "job_id": job["job_id"],
            "job_name": job["job_name"],
            "jd": job["jd"],
            "client_id": job["client_id"],
            "client_name": job["client_name"],
            "jd_word_cnt": job["jd_word_cnt"],
        }
    }

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(url, headers=headers, json=payload)
        if resp.status_code not in (200, 201):
            raise HTTPException(
                status_code=502,
                detail=f"Error creating Airtable record: {resp.status_code} {resp.text}",
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
            "jd_word_cnt": job["jd_word_cnt"],
        }
    }

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.patch(url, headers=headers, json=payload)
        if resp.status_code not in (200, 201):
            raise HTTPException(
                status_code=502,
                detail=f"Error updating Airtable record: {resp.status_code} {resp.text}",
            )


@app.post("/update-jobs")
async def update_jobs() -> Dict[str, Any]:
    """
    Fetch recent jobs from Manatal and sync them into Airtable.
    """
    cutoff = compute_cutoff_date()

    # Fetch data
    manatal_jobs = await fetch_recent_jobs_from_manatal(cutoff)
    airtable_records = await fetch_all_airtable_records()

    updated_jobs: List[Dict[str, Any]] = []

    for job in manatal_jobs:
        job_id = job["job_id"]
        if not job_id:
            continue

        existing = airtable_records.get(job_id)
        if not existing:
            # New job – create in Airtable but do not treat as an \"updated\" job.
            await create_airtable_record(job)
            continue

        existing_fields = existing.get("fields", {})

        try:
            existing_jd_word_cnt = int(existing_fields.get("jd_word_cnt") or 0)
        except (TypeError, ValueError):
            existing_jd_word_cnt = 0

        if existing_jd_word_cnt != job["jd_word_cnt"]:
            record_id = existing["record_id"]
            await update_airtable_record(record_id, job)
            updated_jobs.append(
                {
                    "job_id": job_id,
                    "job_name": job["job_name"],
                    "client_name": job["client_name"],
                    "old_jd_word_cnt": existing_jd_word_cnt,
                    "new_jd_word_cnt": job["jd_word_cnt"],
                }
            )

    airtable_url = f"https://airtable.com/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_ID}"

    return {
        "updatedCount": len(updated_jobs),
        "updatedJobs": updated_jobs,
        "airtableUrl": airtable_url,
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")), reload=True)


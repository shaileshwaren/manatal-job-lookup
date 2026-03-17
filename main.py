"""
Manatal -> Airtable Job Sync — Render web app.

Serves the UI and POST /update-jobs; sync logic lives in sync.py (run locally with: python sync.py).
GET /api/refresh-jobs?api_key=... is for Airtable button: returns HTML immediately and runs sync in background.
"""
import asyncio
import os

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi import Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from sync import run_sync_async

load_dotenv()

app = FastAPI(title="Manatal → Airtable Job Sync")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))

WEBHOOK_API_KEY = os.getenv("WEBHOOK_API_KEY", "")
LAST_SYNC_STATUS = {
    "status": "idle",  # idle | running | done | error
    "created": 0,
    "updated": 0,
    "error": "",
}


@app.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/api/refresh-status")
async def refresh_status():
    """Return last sync status for polling from the refresh page."""
    return LAST_SYNC_STATUS


@app.get("/api/refresh-jobs", response_class=HTMLResponse)
async def refresh_jobs(api_key: str = Query(..., description="Must match WEBHOOK_API_KEY")):
    """
    Trigger sync from Airtable button (GET). Validates api_key, returns an HTML page
    that shows 'Processing…' and polls /api/refresh-status until done.
    """
    if not WEBHOOK_API_KEY or api_key != WEBHOOK_API_KEY:
        return HTMLResponse(
            content="<html><body><p>Forbidden. Invalid or missing api_key.</p></body></html>",
            status_code=403,
        )

    async def _run_and_record():
        global LAST_SYNC_STATUS
        LAST_SYNC_STATUS = {"status": "running", "created": 0, "updated": 0, "error": ""}
        try:
            result = await run_sync_async()
            created = result.get("createdCount", 0)
            updated = result.get("updatedCount", 0)
            LAST_SYNC_STATUS = {
                "status": "done",
                "created": created,
                "updated": updated,
                "error": "",
            }
        except Exception as e:  # catch all so status stays consistent
            LAST_SYNC_STATUS = {
                "status": "error",
                "created": 0,
                "updated": 0,
                "error": str(e),
            }

    # Start background task; page will poll status.
    asyncio.create_task(_run_and_record())

    html = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Refreshing jobs</title>
  <style>
    body { font-family: system-ui, sans-serif; max-width: 520px; margin: 2rem auto; padding: 0 1rem; }
    h1 { font-size: 1.25rem; color: #111; }
    p { color: #444; line-height: 1.5; }
    .status-running { color: #92400e; }
    .status-done { color: #15803d; }
    .status-error { color: #b91c1c; }
  </style>
</head>
<body>
  <h1>Refreshing jobs from Manatal</h1>
  <p id="statusText" class="status-running">
    Processing. You can close this tab; updates will continue in the background regardless.
  </p>
  <script>
    async function pollStatus() {
      try {
        const res = await fetch('/api/refresh-status');
        if (!res.ok) {
          setTimeout(pollStatus, 3000);
          return;
        }
        const data = await res.json();
        const el = document.getElementById('statusText');
        if (data.status === 'done') {
          const created = data.created ?? 0;
          const updated = data.updated ?? 0;
          const parts = [];
          if (created) parts.push(created + ' new');
          if (updated) parts.push(updated + ' updated');
          const summary = parts.length ? parts.join(', ') : 'no changes';
          el.textContent = 'Done. Sync complete: ' + summary + '. You can close this tab; updates to Airtable are already applied.';
          el.className = 'status-done';
        } else if (data.status === 'error') {
          el.textContent = 'Error while syncing: ' + (data.error || 'Unknown error') + '.';
          el.className = 'status-error';
        } else {
          // still running or idle
          setTimeout(pollStatus, 3000);
        }
      } catch (e) {
        setTimeout(pollStatus, 3000);
      }
    }
    // Start polling shortly after load
    setTimeout(pollStatus, 2000);
  </script>
</body>
</html>
"""
    return HTMLResponse(content=html)


@app.post("/update-jobs")
async def update_jobs():
    """Run Manatal -> Airtable sync and return updatedCount, updatedJobs, airtableUrl."""
    try:
        return await run_sync_async()
    except RuntimeError as e:
        raise HTTPException(status_code=502, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8000")),
        reload=True,
    )

"""
Manatal → Airtable Job Sync — Render web app.

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


@app.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/api/refresh-jobs", response_class=HTMLResponse)
async def refresh_jobs(api_key: str = Query(..., description="Must match WEBHOOK_API_KEY")):
    """
    Trigger sync from Airtable button (GET). Validates api_key, returns HTML immediately,
    runs sync in background. Use query: ?api_key=joblookup-2026 (or WEBHOOK_API_KEY env).
    """
    if not WEBHOOK_API_KEY or api_key != WEBHOOK_API_KEY:
        return HTMLResponse(
            content="<html><body><p>Forbidden. Invalid or missing api_key.</p></body></html>",
            status_code=403,
        )
    asyncio.create_task(run_sync_async())
    html = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
      <meta charset="UTF-8" />
      <meta name="viewport" content="width=device-width, initial-scale=1.0" />
      <title>Refreshing jobs</title>
      <style>
        body { font-family: system-ui, sans-serif; max-width: 480px; margin: 2rem auto; padding: 0 1rem; }
        h1 { font-size: 1.25rem; color: #111; }
        p { color: #444; line-height: 1.5; }
      </style>
    </head>
    <body>
      <h1>Refreshing jobs from Manatal</h1>
      <p>Processing. This may take a minute or two.</p>
      <p><strong>You can close this tab; updates will continue in the background regardless.</strong></p>
    </body>
    </html>
    """
    return HTMLResponse(content=html)


@app.post("/update-jobs")
async def update_jobs():
    """Run Manatal → Airtable sync and return updatedCount, updatedJobs, airtableUrl."""
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

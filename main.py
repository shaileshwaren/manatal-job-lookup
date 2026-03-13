"""
Manatal → Airtable Job Sync — Render web app.

Serves the UI and POST /update-jobs; sync logic lives in sync.py (run locally with: python sync.py).
"""
import os

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi import Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from sync import run_sync_async

load_dotenv()

app = FastAPI(title="Manatal → Airtable Job Sync")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))


@app.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    return templates.TemplateResponse("index.html", {"request": request})


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

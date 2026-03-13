# Manatal → Airtable Job Sync

Sync jobs from Manatal (last 3 months) into an Airtable table. Only updates Airtable when the JD word count changes.

## Deploy to Render

1. **Sign in**  
   Go to [render.com](https://render.com) and sign in (or sign up). Connect your GitHub account if you haven’t.

2. **New Web Service**  
   - Click **New +** → **Web Service**.  
   - Connect the repo: **shaileshwaren/manatal-job-lookup** (or choose it from “Connect a repository”).  
   - Click **Connect**.

3. **Settings**
   - **Name**: e.g. `manatal-job-lookup` (or any name you like).  
   - **Region**: pick the one closest to you.  
   - **Branch**: `main`.  
   - **Runtime**: **Python 3**.  
   - **Build Command**:
     ```bash
     pip install -r requirements.txt
     ```
   - **Start Command**:
     ```bash
     uvicorn main:app --host 0.0.0.0 --port $PORT
     ```
   - **Instance type**: Free (or paid if you prefer).

4. **Environment variables**  
   In the **Environment** section, add:

   | Key                 | Value (your real values) |
   |---------------------|---------------------------|
   | `MANATAL_API_TOKEN` | Your Manatal API token    |
   | `AIRTABLE_TOKEN`    | Your Airtable PAT         |
   | `AIRTABLE_BASE_ID`  | `app285aKVVr7JYL43`       |
   | `AIRTABLE_TABLE_ID` | `tblCV6w4fGex9VgzK`        |

   Render will inject `PORT`; the start command uses it automatically.

   **Optional – AI for JD plain text:** To use OpenAI to extract plain text from HTML job descriptions (instead of regex-only), add `OPENAI_API_KEY` and `USE_AI_JD_EXTRACT=1`. Uses `gpt-4o-mini`; falls back to code-only strip on error or if unset.

5. **Deploy**  
   Click **Create Web Service**. Render will build and deploy. When it’s live, open the service URL (e.g. `https://manatal-job-lookup.onrender.com`), click **Update jobs**, then use **Open in Airtable** to check the table.

## Run locally

**Web app (same as Render):**
```bash
pip install -r requirements.txt
# Set MANATAL_API_TOKEN, AIRTABLE_TOKEN, AIRTABLE_BASE_ID, AIRTABLE_TABLE_ID in .env
uvicorn main:app --host 0.0.0.0 --port 8000
```
Open [http://localhost:8000](http://localhost:8000).

**Sync only (terminal, no server):**
```bash
# Same .env as above
python sync.py
```
Runs the Manatal → Airtable sync and prints updated count and job list (or errors) to the terminal.

### Avoiding Manatal 429 (rate limit) errors

- **In this repo:** The sync script is paced to stay under Manatal’s **100 requests per 60 seconds** (about 1.5s between job-list pages). You also can’t run `python sync.py` more than once per 60 seconds.
- **Other options:**
  1. **Use Manatal’s Data Export:** In Manatal go to **Administration → Data Management → Data Export**, choose Jobs, pick fields, and generate a CSV/XLS. You can then import or script that file into Airtable instead of using the API (no rate limit on exports).
  2. **Ask for a higher limit:** Contact [Manatal support](https://support.manatal.com/docs/manatal-api) (support@manatal.com or Open API settings) to request a higher rate limit for your account.

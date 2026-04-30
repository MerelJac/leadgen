# Lead Gen — Mission-Driven Orgs

Daily-runnable pipeline that finds nonprofits / B Corps / civic orgs / community foundations, grades their websites, finds decision-makers, drafts outreach, and appends to your Google Sheet.

## Commands to run
- load .env
```
set -a; source .env; set +a  
```
- run script
```
python lead_gen.py   
```

## What the script does

1. **Searches** with DuckDuckGo (free, no API key) using your category keywords.
2. **Filters partisan/political** orgs out twice — once on the search snippet, once on real site content.
3. **Fetches each site** and runs technical checks: mobile viewport, load time, modern framework markers, copyright year, table-based layout, etc.
4. **Tiers** each site:
   - **Tier 1** = outdated / not mobile / slow / stale → website rebuild
   - **Tier 2** = modern, fast, responsive → software automation or branded mini-game
   - **Disqualify** = recently rebuilt, dead DNS, or doesn't load
5. **Finds decision maker** (Executive Director / CEO) name + LinkedIn via DDG site search.
6. **Calls Claude Haiku 4.5** ONCE per qualified lead to confirm the tier and write a personalized outreach message.
7. **Appends to your Google Sheet**, deduped by domain.

## Cost control

- Hard daily budget cap (default **$1.50/day**) tracked in `state/budget.json`.
- One Claude call per qualified lead, capped at 600 output tokens.
- Pre-LLM heuristics drop disqualified sites for free.
- Run aborts mid-pipeline if budget hit. State persists across runs; resets at midnight local.

Typical Haiku 4.5 cost per lead: roughly $0.002–0.005. **$1.50/day = ~300–700 leads of LLM work**, but the bottleneck will actually be search variety and site fetching, so realistically you'll process 25–50 leads/day with the default settings.

Tune `MAX_CANDIDATES_PER_RUN` in the config block of `lead_gen.py` if you want more or fewer per run.

## Editing the sheet

The script ONLY writes to these columns:

`Domain`, `Organization name`, `Website URL`, `Decision maker name`, `LinkedIn URL`, `Tier`, `Reason for grade`, `Suggested offer`, `Outreach message`, `Date added`

**Anything else you add is yours.** Add columns like `Status`, `Contacted on`, `Reply received`, `Notes`, whatever you need — the script will never overwrite them. Dedupe is by `Domain`, so the script also won't re-add a lead you've already worked.

> **Important:** Don't change the `Domain` column header or its values. That's the dedupe key.

## Setup — one-time

### 1. Install Python deps

```bash
cd lead_gen
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Get a Google Sheets service account (free)

The script writes to your sheet via a Google service account. This avoids OAuth pop-ups entirely — perfect for cron / GitHub Actions.

1. Go to https://console.cloud.google.com/
2. Create a new project (or pick an existing one).
3. Enable the **Google Sheets API** and **Google Drive API** for that project.
4. Go to **IAM & Admin → Service Accounts → Create Service Account**. Name it whatever (`leadgen-bot` is fine). Skip the optional role steps.
5. Open the new service account, go to **Keys → Add Key → Create new key → JSON**. Download the file.
6. Save it as `service_account.json` in this folder. (Or set `GOOGLE_SERVICE_ACCOUNT_FILE` to its path.)
7. Open the JSON file, find `"client_email"` (looks like `leadgen-bot@yourproject.iam.gserviceaccount.com`).
8. Open your Google Sheet → **Share** → paste that email → give it **Editor** access.

### 3. Prep the sheet headers

In row 1 of the `Leads` tab (or whatever you set `WORKSHEET_NAME` to — defaults to `Leads`), make sure these column headers exist, in any order:

```
Domain | Organization name | Website URL | Decision maker name | LinkedIn URL | Tier | Reason for grade | Suggested offer | Outreach message | Date added
```

You can add your own columns before, between, or after — the script matches by header name, not position.

If the worksheet doesn't exist, the script creates it on first run with the right headers.

### 4. Set environment variables

```bash
export ANTHROPIC_API_KEY=sk-ant-...
export DAILY_BUDGET_USD=1.50          # optional, default $1.50
export GOOGLE_SERVICE_ACCOUNT_FILE=./service_account.json   # optional
```

## Running

### Manual (now)

```bash
# Dry run first — see what it would do, no sheet writes
python lead_gen.py --dry-run

# Real run
python lead_gen.py
```

### Daily via cron (Mac/Linux)

```bash
crontab -e
# Run at 8:30am every day:
30 8 * * * cd /full/path/to/lead_gen && /full/path/to/venv/bin/python lead_gen.py >> run.log 2>&1
```

### Daily via GitHub Actions (free)

1. Push this folder to a private GitHub repo.
2. Repo **Settings → Secrets and variables → Actions → New repository secret**:
   - `ANTHROPIC_API_KEY` — your key
   - `GOOGLE_SERVICE_ACCOUNT_JSON` — the **entire contents** of `service_account.json`, pasted in
3. The included `.github/workflows/daily.yml` runs daily at 13:30 UTC. Adjust the cron line to whatever you want.

## State files

`state/budget.json` — today's spend. Gets reset at midnight local. Safe to delete to reset.

## Tuning later

In `lead_gen.py` near the top:
- `RESULTS_PER_QUERY` — how deep each search goes (default 8)
- `MAX_CANDIDATES_PER_RUN` — hard cap on leads processed per run (default 25)
- `SEARCH_QUERIES` — your search categories. Add/remove freely.
- `PARTISAN_KEYWORDS` — pre-LLM partisan filter

## What it doesn't do (yet)

- WHOIS lookups for true site age (some "recently built" sites slip through; the LLM catches most of these as a backstop).
- Lighthouse-grade performance scoring (uses load time + responsive markers as a proxy).
- Email finding — only LinkedIn URL. If you want emails, Hunter.io or Apollo are the typical adds, but both have paid tiers.

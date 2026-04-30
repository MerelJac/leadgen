#!/usr/bin/env python3
"""
Lead generation pipeline for mission-driven organizations.

Pipeline:
  1. DuckDuckGo search (free) for orgs by category keywords
  2. Visit each site, capture HTML + headers + perf signals
  3. Filter out partisan/political orgs (keyword + LLM check)
  4. Score Tier 1 (rebuild) vs Tier 2 (automation/mini-game) vs Disqualify
  5. Find Executive Director name + LinkedIn (DDG site search)
  6. Draft personalized outreach with Claude Haiku
  7. Append to Google Sheet, dedupe by domain (so you can edit other columns freely)

Budget: hard daily cap on Claude API spend. Tracked in state file.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import socket
import sys
import time
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone, date
from pathlib import Path
from typing import Optional, Iterable
from urllib.parse import urlparse, urljoin

import requests
from bs4 import BeautifulSoup
from ddgs import DDGS
from anthropic import Anthropic
import gspread
from google.oauth2.service_account import Credentials

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

ROOT = Path(__file__).parent
STATE_DIR = ROOT / "state"
STATE_DIR.mkdir(exist_ok=True)

CONFIG = {
    # Hard daily spend cap in USD. Script aborts mid-run if hit.
    "DAILY_BUDGET_USD": float(os.environ.get("DAILY_BUDGET_USD", "0.25")),

    # Claude Haiku 4.5 pricing per million tokens (verified Apr 2026)
    "PRICE_INPUT_PER_M": 1.00,
    "PRICE_OUTPUT_PER_M": 2.00,
    "MODEL": "claude-haiku-4-5",

    # How many search queries to run, and results per query
    "RESULTS_PER_QUERY": 8,

    # Per-run cap on how many new candidate orgs to fully evaluate
    # (keeps cost predictable). Tune up once you confirm spend.
    "MAX_CANDIDATES_PER_RUN": 25,

    # Site fetch settings
    "REQUEST_TIMEOUT": 12,
    "USER_AGENT": "Mozilla/5.0 (compatible; LeadGenBot/1.0; +outreach research)",

    # Sheet
    "SHEET_URL": "https://docs.google.com/spreadsheets/d/1Eje7iG-EJzVIsEkhCzwi_0xSicl6_3m-BvVqWe1ZbR8/edit",
    "WORKSHEET_NAME": "Leads",
    "SERVICE_ACCOUNT_FILE": os.environ.get("GOOGLE_SERVICE_ACCOUNT_FILE",
                                            str(ROOT / "service_account.json")),
}

# These columns are owned by the script. Editing them in the sheet may be
# overwritten on re-run if you change the dedupe key. Anything OUTSIDE this
# list is yours — add columns like "Status", "Contacted on", "Notes",
# "Reply received", whatever — the script never touches those.
SCRIPT_OWNED_COLUMNS = [
    "Domain",                          # dedupe key (hidden-ish, technical)
    "Organization name",
    "Website URL",
    "Decision maker name",
    "LinkedIn URL",
    "Tier",
    "Reason for grade",
    "Suggested offer",
    "Outreach message",
    "Date added",
]

SEARCH_QUERIES = [
    "community foundation",
    "social enterprise nonprofit",
    "civic nonprofit organization",
    "B Corp certified",
    "youth development organization",
    "community advocacy group",
    "faith-based community organization",
    "neighborhood foundation",
    "social impact nonprofit",
]

# Hard exclusions — partisan / political. Anything matching is dropped before
# we spend a token on it. The LLM gets a second pass too.
PARTISAN_KEYWORDS = [
    "republican", "democrat", "democratic party", "gop", "rnc", "dnc",
    "campaign committee", "political action committee", " pac ",
    "for congress", "for senate", "for governor", "for president",
    "vote for", "elect ", "re-elect", "reelect",
    "conservative party", "liberal party", "progressive party",
    "tea party", "libertarian party", "green party",
    "trump", "biden", "harris", "desantis", "newsom",  # candidate-name signals
    "political campaign", "campaign 20",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("leadgen")


# ---------------------------------------------------------------------------
# Budget tracking — survives across runs, resets daily
# ---------------------------------------------------------------------------

class BudgetExceeded(Exception):
    pass


class FatalAPIError(Exception):
    """Errors that mean we should stop the whole run, not just skip a lead."""
    pass


# Substrings in API error messages that mean "stop, don't retry"
FATAL_API_ERROR_SIGNALS = (
    "credit balance is too low",
    "insufficient credits",
    "billing",
    "invalid x-api-key",
    "authentication_error",
    "permission_error",
    "account is not authorized",
)


@dataclass
class Budget:
    date_iso: str
    spent_usd: float = 0.0
    input_tokens: int = 0
    output_tokens: int = 0

    @classmethod
    def load(cls, path: Path, cap_usd: float) -> "Budget":
        today = date.today().isoformat()
        if path.exists():
            try:
                data = json.loads(path.read_text())
                if data.get("date_iso") == today:
                    return cls(**data)
            except Exception:
                pass
        return cls(date_iso=today)

    def save(self, path: Path) -> None:
        path.write_text(json.dumps(asdict(self), indent=2))

    def remaining(self, cap_usd: float) -> float:
        return max(0.0, cap_usd - self.spent_usd)

    def add(self, input_tok: int, output_tok: int) -> None:
        self.input_tokens += input_tok
        self.output_tokens += output_tok
        cost = (input_tok / 1_000_000) * CONFIG["PRICE_INPUT_PER_M"] + \
               (output_tok / 1_000_000) * CONFIG["PRICE_OUTPUT_PER_M"]
        self.spent_usd += cost

    def check(self, cap_usd: float) -> None:
        if self.spent_usd >= cap_usd:
            raise BudgetExceeded(
                f"Daily budget ${cap_usd:.2f} hit (spent ${self.spent_usd:.4f}). "
                f"Stopping. Resets at midnight local."
            )


# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------

@dataclass
class Lead:
    organization_name: str
    website_url: str
    domain: str
    decision_maker_name: str = ""
    linkedin_url: str = ""
    tier: str = ""           # "1", "2", or "" if disqualified
    reason: str = ""
    suggested_offer: str = ""
    outreach_message: str = ""
    date_added: str = ""

    def to_row(self) -> list[str]:
        return [
            self.domain,
            self.organization_name,
            self.website_url,
            self.decision_maker_name,
            self.linkedin_url,
            self.tier,
            self.reason,
            self.suggested_offer,
            self.outreach_message,
            self.date_added,
        ]


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------

def search_organizations(queries: list[str], per_query: int) -> list[dict]:
    """Run DDG searches, return de-duplicated candidates by domain."""
    seen: set[str] = set()
    candidates: list[dict] = []
    with DDGS() as ddgs:
        for q in queries:
            log.info("Searching: %s", q)
            try:
                results = list(ddgs.text(q, max_results=per_query, region="us-en"))
            except Exception as e:
                log.warning("Search failed for %r: %s", q, e)
                continue
            for r in results:
                url = r.get("href") or r.get("url") or ""
                title = r.get("title") or ""
                snippet = r.get("body") or ""
                if not url:
                    continue
                domain = canonical_domain(url)
                if not domain or domain in seen:
                    continue
                if is_obviously_skippable_domain(domain):
                    continue
                seen.add(domain)
                candidates.append({
                    "url": url,
                    "title": title,
                    "snippet": snippet,
                    "domain": domain,
                    "source_query": q,
                })
            time.sleep(1)  # be polite to DDG
    log.info("Total unique candidates from search: %d", len(candidates))
    return candidates


def canonical_domain(url: str) -> str:
    try:
        host = urlparse(url).netloc.lower()
        if host.startswith("www."):
            host = host[4:]
        return host
    except Exception:
        return ""


SKIP_DOMAINS = {
    "wikipedia.org", "en.wikipedia.org", "linkedin.com", "facebook.com",
    "twitter.com", "x.com", "instagram.com", "youtube.com", "tiktok.com",
    "reddit.com", "amazon.com", "bcorporation.net",  # directory not org
    "guidestar.org", "candid.org", "charitynavigator.org",
    "indeed.com", "glassdoor.com", "ziprecruiter.com",
    "irs.gov", "sba.gov", "yelp.com", "bbb.org",
    "medium.com", "substack.com",
}


def is_obviously_skippable_domain(domain: str) -> bool:
    if not domain:
        return True
    parts = domain.split(".")
    base = ".".join(parts[-2:]) if len(parts) >= 2 else domain
    return base in SKIP_DOMAINS


# ---------------------------------------------------------------------------
# Partisan filter (cheap pre-LLM screen)
# ---------------------------------------------------------------------------

def looks_partisan(text: str) -> bool:
    t = f" {text.lower()} "
    return any(kw in t for kw in PARTISAN_KEYWORDS)


# ---------------------------------------------------------------------------
# Site fetch + technical signals
# ---------------------------------------------------------------------------

@dataclass
class SiteSignals:
    url: str
    final_url: str = ""
    status: int = 0
    load_time_s: float = 0.0
    content_length: int = 0
    has_viewport_meta: bool = False
    uses_responsive_css: bool = False
    last_modified: str = ""
    copyright_year: Optional[int] = None
    title: str = ""
    meta_description: str = ""
    body_text_excerpt: str = ""
    looks_dead: bool = False
    error: str = ""
    # Heuristic age signals — when the site was likely built/last redesigned
    has_modern_framework: bool = False  # React/Vue/Next markers
    has_tailwind: bool = False
    has_old_table_layout: bool = False
    has_flash_or_iframe_old: bool = False


def fetch_site(url: str) -> SiteSignals:
    sig = SiteSignals(url=url)
    try:
        # DNS first — fast fail
        host = urlparse(url).netloc
        try:
            socket.gethostbyname(host)
        except socket.gaierror:
            sig.looks_dead = True
            sig.error = "DNS resolution failed"
            return sig

        t0 = time.time()
        r = requests.get(
            url,
            timeout=CONFIG["REQUEST_TIMEOUT"],
            headers={"User-Agent": CONFIG["USER_AGENT"]},
            allow_redirects=True,
        )
        sig.load_time_s = round(time.time() - t0, 2)
        sig.status = r.status_code
        sig.final_url = r.url
        sig.content_length = len(r.content)
        sig.last_modified = r.headers.get("Last-Modified", "")

        if r.status_code >= 400:
            sig.error = f"HTTP {r.status_code}"
            return sig

        html = r.text
        soup = BeautifulSoup(html, "html.parser")

        # Viewport meta = baseline mobile-friendliness
        viewport = soup.find("meta", attrs={"name": "viewport"})
        sig.has_viewport_meta = viewport is not None

        # Title + description
        if soup.title and soup.title.string:
            sig.title = soup.title.string.strip()[:200]
        desc = soup.find("meta", attrs={"name": "description"})
        if desc and desc.get("content"):
            sig.meta_description = desc["content"].strip()[:300]

        # Body text excerpt (for partisan check + LLM context)
        text = " ".join(soup.get_text(separator=" ").split())
        sig.body_text_excerpt = text[:1500]

        # Modern framework markers
        html_lower = html.lower()
        sig.has_modern_framework = any(m in html_lower for m in [
            "__next", "_nuxt", "data-reactroot", "ng-version",
            "data-vue", "/_app/", "data-sveltekit",
        ])
        sig.has_tailwind = "tailwind" in html_lower or \
                          re.search(r'class="[^"]*\b(flex|grid|md:|lg:)\b', html_lower) is not None
        sig.uses_responsive_css = "@media" in html_lower or sig.has_tailwind

        # Old-school markers
        # table-based layout (ignore data tables — heuristic: many <table> at top of body)
        tables = soup.find_all("table")
        if tables:
            # If there's a <table> ancestor wrapping major nav/content — old layout.
            for t in tables[:3]:
                if t.find("table") or len(t.find_all("td")) > 20:
                    sig.has_old_table_layout = True
                    break
        sig.has_flash_or_iframe_old = "<applet" in html_lower or "shockwave" in html_lower

        # Copyright year
        m = re.search(r"©\s*(\d{4})|copyright\s*(?:&copy;)?\s*(\d{4})", html_lower)
        if m:
            year = int(m.group(1) or m.group(2))
            if 1990 <= year <= 2100:
                sig.copyright_year = year

    except requests.exceptions.SSLError as e:
        sig.error = f"SSL error: {e}"
        sig.looks_dead = True
    except requests.exceptions.ConnectionError as e:
        sig.error = f"Connection error: {e}"
        sig.looks_dead = True
    except requests.exceptions.Timeout:
        sig.error = "Timeout"
        sig.load_time_s = CONFIG["REQUEST_TIMEOUT"]
    except Exception as e:
        sig.error = f"Fetch error: {e}"

    return sig


def extract_org_name(sig: SiteSignals, domain: str) -> str:
    """
    Pull a clean org name from the homepage. Strategy:
      1. og:site_name (most reliable when present)
      2. <title>, with taglines and common suffixes stripped
      3. Fall back to a humanized version of the domain
    """
    # Try og:site_name from the body excerpt is hard since we already parsed.
    # Re-pull from the title field with smart trimming.
    title = (sig.title or "").strip()

    if title:
        # Split on common separators and pick the part most likely to be the org
        # name. Page titles typically look like:
        #   "About Us | Hamilton Community Foundation"
        #   "Hamilton Community Foundation - Empowering communities since 1954"
        #   "Hamilton Community Foundation"
        parts = re.split(r"\s[|–—\-:]\s", title)
        parts = [p.strip() for p in parts if p.strip()]

        # Filter out obvious page-name parts ("About Us", "Home", "Donate", etc.)
        page_words = {
            "home", "about", "about us", "contact", "contact us", "donate",
            "give", "giving", "support us", "our work", "our mission",
            "our story", "team", "our team", "blog", "news", "events",
            "programs", "what we do", "who we are", "meet our donors",
            "meet the team",
        }
        filtered = [p for p in parts if p.lower() not in page_words]

        # Of remaining parts, prefer the one that looks most like an org name:
        # contains a key org word, or is the longest reasonable candidate.
        org_keywords = (
            "foundation", "nonprofit", "society", "association", "alliance",
            "council", "institute", "coalition", "network", "center", "centre",
            "trust", "fund", "project", "initiative", "ministries", "church",
            "community", "group", "organization", "league", "corps",
        )
        for p in filtered:
            if any(kw in p.lower() for kw in org_keywords) and 3 <= len(p) <= 80:
                return p[:80]

        # Otherwise take the first reasonable filtered part
        for p in filtered:
            if 3 <= len(p) <= 80:
                return p[:80]

    # Fallback: humanize the domain
    base = domain.split(".")[0].replace("-", " ").replace("_", " ")
    return base.title()[:80]


# ---------------------------------------------------------------------------
# Tiering — heuristic first, LLM only for borderline + outreach
# ---------------------------------------------------------------------------

def heuristic_tier(sig: SiteSignals) -> tuple[str, str]:
    """
    Returns (tier, reason). Tier is "DISQUALIFY", "1", "2", or "BORDERLINE".
    """
    if sig.looks_dead or sig.error and sig.status == 0:
        return "DISQUALIFY", f"Site doesn't load: {sig.error}"
    if sig.status >= 400:
        return "DISQUALIFY", f"HTTP {sig.status}"

    # "Built/refreshed in last 12 months" disqualifier:
    # If copyright year is current and modern framework is present, treat
    # as recently built. We can't perfectly detect this without WHOIS/Wayback;
    # this is a reasonable proxy.
    current_year = datetime.now().year
    if (sig.copyright_year == current_year
        and sig.has_modern_framework
        and sig.has_viewport_meta
        and sig.uses_responsive_css):
        return "DISQUALIFY", "Site appears recently built/redesigned (modern stack + current copyright)"

    # Tier 1 signals: outdated / not mobile / slow
    tier1_reasons = []
    if not sig.has_viewport_meta:
        tier1_reasons.append("no mobile viewport tag")
    if sig.load_time_s > 5.0:
        tier1_reasons.append(f"slow load ({sig.load_time_s}s)")
    if sig.has_old_table_layout:
        tier1_reasons.append("table-based layout")
    if sig.has_flash_or_iframe_old:
        tier1_reasons.append("legacy embed tech")
    if sig.copyright_year and sig.copyright_year < current_year - 2:
        tier1_reasons.append(f"copyright {sig.copyright_year} (stale)")
    if not sig.uses_responsive_css and not sig.has_modern_framework:
        tier1_reasons.append("no responsive CSS")

    # Strong Tier 2 signals: modern stack, mobile-friendly, fast
    tier2_signals = (
        sig.has_modern_framework
        and sig.has_viewport_meta
        and sig.uses_responsive_css
        and sig.load_time_s < 3.0
    )

    if len(tier1_reasons) >= 2:
        return "1", "; ".join(tier1_reasons)
    if tier2_signals:
        return "2", "Modern responsive site, fast load — good candidate for automation/engagement tooling"
    if len(tier1_reasons) == 1:
        return "BORDERLINE", "; ".join(tier1_reasons)
    return "BORDERLINE", "Mixed signals — needs LLM judgment"


# ---------------------------------------------------------------------------
# Decision maker discovery
# ---------------------------------------------------------------------------

def find_decision_maker(domain: str, org_name: str) -> tuple[str, str]:
    """
    Returns (name, linkedin_url). Best-effort via DDG site search.
    Tries both the org's own site (about/team page) and LinkedIn.
    """
    name, linkedin = "", ""

    # 1. LinkedIn search via DDG
    queries = [
        f'site:linkedin.com/in "{org_name}" "executive director"',
        f'site:linkedin.com/in "{org_name}" "CEO"',
        f'"{org_name}" executive director',
    ]
    with DDGS() as ddgs:
        for q in queries:
            try:
                results = list(ddgs.text(q, max_results=5, region="us-en"))
            except Exception as e:
                log.debug("DDG decision-maker search failed: %s", e)
                continue
            for r in results:
                url = r.get("href") or r.get("url") or ""
                title = r.get("title") or ""
                if "linkedin.com/in/" in url and not linkedin:
                    linkedin = url.split("?")[0]
                    # title is typically "Jane Smith - Executive Director - Org Name | LinkedIn"
                    parts = re.split(r"\s[-–|]\s", title)
                    if parts:
                        candidate = parts[0].strip()
                        # Sanity check — looks like a person's name (2+ words, no @)
                        if 2 <= len(candidate.split()) <= 5 and "@" not in candidate:
                            name = candidate
                if name and linkedin:
                    return name, linkedin
            if name and linkedin:
                break
            time.sleep(0.5)
    return name, linkedin


# ---------------------------------------------------------------------------
# Claude — borderline tiering + outreach drafting
# ---------------------------------------------------------------------------

def claude_classify_and_draft(
    client: Anthropic,
    budget: Budget,
    cap_usd: float,
    org_name: str,
    site_text: str,
    title: str,
    description: str,
    heuristic_result: tuple[str, str],
    decision_maker: str,
) -> dict:
    """
    Single Claude call that does three things:
      1. Confirm/correct partisan flag
      2. Confirm/refine tier (or accept heuristic)
      3. Draft personalized outreach

    Returns dict with: partisan(bool), tier("1"|"2"|"DISQUALIFY"),
                       reason, suggested_offer, outreach_message
    """
    budget.check(cap_usd)

    h_tier, h_reason = heuristic_result

    system = (
        "You evaluate websites of mission-driven organizations for a B2B "
        "outreach pipeline. You output ONLY valid JSON with these keys: "
        "partisan (bool), tier ('1', '2', or 'DISQUALIFY'), reason (string, "
        "one sentence), suggested_offer (string, one short phrase), "
        "outreach_message (string, 80-130 words, warm and specific to the "
        "org, no fake compliments, no generic intros, no emojis). "
        "Tier 1 = website rebuild (outdated, not mobile, slow, stale). "
        "Tier 2 = modern site, offer software/automation OR a branded mini-game "
        "engagement tool. DISQUALIFY = partisan/political affiliation, "
        "or extremely recently rebuilt site. "
        "Be skeptical: only mark partisan if there's clear evidence of party "
        "affiliation, candidate endorsement, or campaign work. Civic engagement, "
        "voter education, and policy advocacy alone are NOT partisan."
    )

    user = f"""Organization: {org_name}
Title tag: {title}
Meta description: {description}
Decision maker (if found): {decision_maker or "(unknown)"}

Site excerpt (first 1500 chars):
{site_text}

Heuristic tier guess: {h_tier}
Heuristic reason: {h_reason}

Output JSON only. The outreach message should:
- Address the decision maker by first name if known, else "Hi there"
- Reference something specific from the org's mission (from the excerpt)
- Make the suggested offer naturally, not as a pitch dump
- End with a soft call to action (15-min chat)
- Sound like a human, not a template"""

    try:
        resp = client.messages.create(
            model=CONFIG["MODEL"],
            max_tokens=600,
            system=system,
            messages=[{"role": "user", "content": user}],
        )
    except Exception as e:
        msg = str(e).lower()
        if any(sig in msg for sig in FATAL_API_ERROR_SIGNALS):
            # Don't keep hammering the API or filling the sheet with
            # heuristic-only entries — stop the whole run.
            raise FatalAPIError(str(e)) from e
        log.warning("Claude API error (skipping this lead): %s", e)
        return {
            "partisan": False,
            "tier": h_tier if h_tier in ("1", "2") else "DISQUALIFY",
            "reason": h_reason,
            "suggested_offer": "Website rebuild" if h_tier == "1" else "Software automation",
            "outreach_message": "",
            "_skip_due_to_api_error": True,
        }

    budget.add(resp.usage.input_tokens, resp.usage.output_tokens)
    log.info("Claude call: %d in / %d out tok | day spend: $%.4f",
             resp.usage.input_tokens, resp.usage.output_tokens, budget.spent_usd)

    raw = resp.content[0].text.strip()
    # Strip code fences if present
    raw = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw, flags=re.MULTILINE).strip()
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        log.warning("Could not parse Claude JSON: %s", raw[:200])
        return {
            "partisan": False,
            "tier": h_tier if h_tier in ("1", "2") else "DISQUALIFY",
            "reason": h_reason,
            "suggested_offer": "Website rebuild" if h_tier == "1" else "Software automation",
            "outreach_message": "",
        }


# ---------------------------------------------------------------------------
# Google Sheets — append-only, dedupe by domain, never touches user columns
# ---------------------------------------------------------------------------

class SheetClient:
    def __init__(self, sheet_url: str, worksheet_name: str, creds_path: str):
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
        self.gc = gspread.authorize(creds)
        self.sh = self.gc.open_by_url(sheet_url)
        try:
            self.ws = self.sh.worksheet(worksheet_name)
        except gspread.WorksheetNotFound:
            self.ws = self.sh.add_worksheet(title=worksheet_name, rows=1000, cols=20)
            self.ws.append_row(SCRIPT_OWNED_COLUMNS)

        # Ensure header row exists & matches
        existing_headers = self.ws.row_values(1)
        if not existing_headers:
            self.ws.append_row(SCRIPT_OWNED_COLUMNS)
            existing_headers = SCRIPT_OWNED_COLUMNS
        self.headers = existing_headers
        # Find column indices for script-owned fields
        self.col_index = {h: i for i, h in enumerate(self.headers)}
        # Verify Domain column exists
        if "Domain" not in self.col_index:
            raise RuntimeError(
                "Sheet header row is missing 'Domain' column. "
                "Add 'Domain' as the first column header so dedupe works."
            )

    def existing_domains(self) -> set[str]:
        col = self.col_index["Domain"] + 1  # gspread is 1-indexed
        values = self.ws.col_values(col)[1:]  # skip header
        return {v.strip().lower() for v in values if v.strip()}

    def append_lead(self, lead: Lead) -> None:
        # Build row matching THIS sheet's header order, leaving user columns blank
        row = [""] * len(self.headers)
        mapping = {
            "Domain": lead.domain,
            "Organization name": lead.organization_name,
            "Website URL": lead.website_url,
            "Decision maker name": lead.decision_maker_name,
            "LinkedIn URL": lead.linkedin_url,
            "Tier": lead.tier,
            "Reason for grade": lead.reason,
            "Suggested offer": lead.suggested_offer,
            "Outreach message": lead.outreach_message,
            "Date added": lead.date_added,
        }
        for key, val in mapping.items():
            if key in self.col_index:
                row[self.col_index[key]] = val
        self.ws.append_row(row, value_input_option="RAW")


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def run(dry_run: bool = False) -> None:
    cap = CONFIG["DAILY_BUDGET_USD"]
    budget_path = STATE_DIR / "budget.json"
    budget = Budget.load(budget_path, cap)
    log.info("Daily budget cap: $%.2f | already spent today: $%.4f",
             cap, budget.spent_usd)

    if budget.spent_usd >= cap:
        log.error("Already over budget. Exiting.")
        return

    # Anthropic client
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        log.error("ANTHROPIC_API_KEY not set.")
        sys.exit(1)
    client = Anthropic(api_key=api_key)

    # Sheet (skip in dry-run)
    sheet: Optional[SheetClient] = None
    existing: set[str] = set()
    if not dry_run:
        try:
            sheet = SheetClient(
                CONFIG["SHEET_URL"],
                CONFIG["WORKSHEET_NAME"],
                CONFIG["SERVICE_ACCOUNT_FILE"],
            )
            existing = sheet.existing_domains()
            log.info("Sheet loaded. %d existing domains (will skip).", len(existing))
        except FileNotFoundError:
            log.error(
                "Service account file not found at %s. "
                "See README for setup steps.",
                CONFIG["SERVICE_ACCOUNT_FILE"],
            )
            sys.exit(1)
    else:
        log.info("DRY RUN — sheet writes disabled.")

    # 1. Search
    candidates = search_organizations(
        SEARCH_QUERIES, CONFIG["RESULTS_PER_QUERY"]
    )

    # 2. Filter already-in-sheet + obvious partisan from snippet
    fresh = []
    for c in candidates:
        if c["domain"] in existing:
            continue
        snippet_blob = f"{c['title']} {c['snippet']}"
        if looks_partisan(snippet_blob):
            log.info("Skip partisan (snippet): %s", c["domain"])
            continue
        fresh.append(c)

    log.info("After dedupe + partisan snippet filter: %d candidates", len(fresh))
    fresh = fresh[: CONFIG["MAX_CANDIDATES_PER_RUN"]]
    log.info("Processing up to %d this run.", len(fresh))

    # 3-6. Per-candidate pipeline
    added = 0
    today_iso = date.today().isoformat()

    for i, c in enumerate(fresh, 1):
        try:
            budget.check(cap)
        except BudgetExceeded as e:
            log.warning(str(e))
            break

        log.info("[%d/%d] %s", i, len(fresh), c["domain"])

        # Always evaluate the HOMEPAGE, not whatever deep page DDG returned.
        # Deep pages give bad org names ("Meet our donors", "About us")
        # and aren't representative of the site's design quality.
        homepage_url = f"https://{c['domain']}/"
        sig = fetch_site(homepage_url)

        # If the homepage failed but the deep page from search exists,
        # fall back to the deep page so we at least try.
        if sig.looks_dead or sig.status >= 400:
            log.info("  homepage failed (%s), trying deep URL", sig.error or sig.status)
            sig = fetch_site(c["url"])

        # Partisan check on actual site text
        if looks_partisan(sig.body_text_excerpt + " " + sig.title):
            log.info("  → skip (partisan content)")
            continue

        h_tier, h_reason = heuristic_tier(sig)
        if h_tier == "DISQUALIFY":
            log.info("  → disqualified: %s", h_reason)
            continue

        # Org name extraction — homepage <title> is way more reliable than
        # deep-page titles. Strip common suffixes and tagline separators.
        org_name = extract_org_name(sig, c["domain"])
        dm_name, dm_linkedin = find_decision_maker(c["domain"], org_name)
        log.info("  decision maker: %s", dm_name or "(not found)")

        # LLM call (this is the costly bit — and it's gated by budget.check above)
        try:
            llm_out = claude_classify_and_draft(
                client, budget, cap,
                org_name=org_name,
                site_text=sig.body_text_excerpt,
                title=sig.title,
                description=sig.meta_description,
                heuristic_result=(h_tier, h_reason),
                decision_maker=dm_name,
            )
        except BudgetExceeded as e:
            log.warning(str(e))
            break
        except FatalAPIError as e:
            log.error("=" * 60)
            log.error("FATAL API ERROR — stopping run.")
            log.error("%s", e)
            log.error("Fix the underlying issue (billing, key, etc.) then re-run.")
            log.error("=" * 60)
            break

        # If the API errored transiently, skip this lead — don't write a
        # heuristic-only row with no outreach message.
        if llm_out.get("_skip_due_to_api_error"):
            log.info("  → skipped (transient API error)")
            continue

        if llm_out.get("partisan"):
            log.info("  → skip (LLM flagged partisan)")
            continue
        tier = llm_out.get("tier", h_tier)
        if tier == "DISQUALIFY":
            log.info("  → disqualified by LLM: %s", llm_out.get("reason"))
            continue
        if tier not in ("1", "2"):
            log.info("  → unknown tier %r, skipping", tier)
            continue

        lead = Lead(
            organization_name=org_name,
            # Save the canonical homepage URL, not the deep page DDG returned.
            website_url=f"https://{c['domain']}/",
            domain=c["domain"],
            decision_maker_name=dm_name,
            linkedin_url=dm_linkedin,
            tier=tier,
            reason=llm_out.get("reason", h_reason),
            suggested_offer=llm_out.get("suggested_offer", ""),
            outreach_message=llm_out.get("outreach_message", ""),
            date_added=today_iso,
        )

        if dry_run:
            log.info("  [DRY RUN] would add: %s", lead.organization_name)
            print(json.dumps(asdict(lead), indent=2))
        else:
            try:
                sheet.append_lead(lead)
                added += 1
                log.info("  ✓ added Tier %s lead: %s", tier, lead.organization_name)
            except Exception as e:
                log.error("  Sheet write failed: %s", e)

        # Persist budget after each LLM call so a crash doesn't lose state
        budget.save(budget_path)

    budget.save(budget_path)
    log.info("=" * 60)
    log.info("Run complete. Added: %d | Spent today: $%.4f / $%.2f",
             added, budget.spent_usd, cap)
    log.info("Tokens today — input: %d | output: %d",
             budget.input_tokens, budget.output_tokens)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true",
                   help="Skip sheet writes, print leads to stdout.")
    args = p.parse_args()
    try:
        run(dry_run=args.dry_run)
    except KeyboardInterrupt:
        log.warning("Interrupted.")


if __name__ == "__main__":
    main()
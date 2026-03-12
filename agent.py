"""
Company Deduplication Agent
- Pages through all HubSpot companies
- Groups by domain
- For each duplicate group, picks primary (most contacts + deals, tiebreak: oldest)
- Calls HubSpot merge API to do true merges (preserves all notes/activity/deals/contacts)
"""

import os
import time
import json
import requests
from collections import defaultdict

HUBSPOT_TOKEN = os.environ["HUBSPOT_TOKEN"]
BASE    = "https://api.hubapi.com"
HEADERS = {"Authorization": f"Bearer {HUBSPOT_TOKEN}", "Content-Type": "application/json"}

def hs_get(path, params=None):
    r = requests.get(f"{BASE}{path}", headers=HEADERS, params=params)
    r.raise_for_status()
    return r.json()

def hs_post(path, payload):
    r = requests.post(f"{BASE}{path}", headers=HEADERS, json=payload)
    r.raise_for_status()
    return r.json()

# ── Fetch all companies ────────────────────────────────────────────────────────

def fetch_all_companies():
    """Page through ALL companies using GET endpoint (no 10k search limit)."""
    print("📥 Fetching all companies...")
    companies = []
    after = None
    page = 0
    props = "name,domain,num_associated_contacts,num_associated_deals,createdate"

    while True:
        params = {"limit": 100, "properties": props}
        if after:
            params["after"] = after

        data = hs_get("/crm/v3/objects/companies", params=params)
        results = data.get("results", [])
        companies.extend(results)
        page += 1

        if page % 10 == 0:
            print(f"  ...fetched {len(companies)} companies so far")

        after = data.get("paging", {}).get("next", {}).get("after")
        if not after:
            break

        time.sleep(0.05)

    print(f"✅ Fetched {len(companies)} total companies")
    return companies

# ── Find duplicates ────────────────────────────────────────────────────────────

SKIP_DOMAINS = {
    "", "gmail.com", "yahoo.com", "hotmail.com", "outlook.com",
    "aol.com", "icloud.com", "mail.com", "protonmail.com",
}

def find_duplicates(companies):
    """Group companies by domain, return groups with 2+ companies."""
    by_domain = defaultdict(list)

    for c in companies:
        domain = (c["properties"].get("domain") or "").strip().lower()
        # Strip protocol/www
        domain = domain.replace("https://", "").replace("http://", "").lstrip("www.").rstrip("/")
        if not domain or domain in SKIP_DOMAINS:
            continue
        by_domain[domain].append(c)

    duplicates = {d: cs for d, cs in by_domain.items() if len(cs) > 1}
    print(f"🔍 Found {len(duplicates)} domains with duplicate companies")
    return duplicates

# ── Pick primary record ────────────────────────────────────────────────────────

def pick_primary(companies):
    """
    Primary = most contacts + deals.
    Tiebreak: oldest record (lowest createdate).
    """
    def score(c):
        contacts = int(c["properties"].get("num_associated_contacts") or 0)
        deals    = int(c["properties"].get("num_associated_deals") or 0)
        created  = c["properties"].get("createdate") or "9999"
        return (contacts + deals, -int(created[:10].replace("-", "") or 0))

    return max(companies, key=score)

# ── Merge ──────────────────────────────────────────────────────────────────────

def merge_companies(primary_id, secondary_id):
    """
    Call HubSpot's native merge endpoint.
    All notes, activity, deals, contacts roll up to primary_id.
    secondary_id is archived after merge.
    """
    r = requests.post(
        f"{BASE}/crm/v3/objects/companies/merge",
        headers=HEADERS,
        json={"primaryObjectId": str(primary_id), "objectIdToMerge": str(secondary_id)},
    )
    r.raise_for_status()
    return r.json()

# ── Main ───────────────────────────────────────────────────────────────────────

def run_dedup(dry_run=False):
    stats = {
        "total_companies": 0,
        "duplicate_domains": 0,
        "merges_performed": 0,
        "merges_failed": 0,
        "skipped_dry_run": 0,
        "details": [],
    }

    companies = fetch_all_companies()
    stats["total_companies"] = len(companies)

    duplicates = find_duplicates(companies)
    stats["duplicate_domains"] = len(duplicates)

    print(f"\n{'🔍 DRY RUN — no changes will be made' if dry_run else '🔀 Starting merges...'}\n")

    for domain, group in duplicates.items():
        primary   = pick_primary(group)
        secondaries = [c for c in group if c["id"] != primary["id"]]

        primary_name = primary["properties"].get("name", "Unnamed")
        primary_contacts = int(primary["properties"].get("num_associated_contacts") or 0)
        primary_deals    = int(primary["properties"].get("num_associated_deals") or 0)

        print(f"\n📎 {domain} ({len(group)} records)")
        print(f"  PRIMARY: {primary_name} (id={primary['id']}, contacts={primary_contacts}, deals={primary_deals})")

        for sec in secondaries:
            sec_name = sec["properties"].get("name", "Unnamed")
            sec_contacts = int(sec["properties"].get("num_associated_contacts") or 0)
            sec_deals    = int(sec["properties"].get("num_associated_deals") or 0)
            print(f"  MERGE ←  {sec_name} (id={sec['id']}, contacts={sec_contacts}, deals={sec_deals})")

            detail = {
                "domain": domain,
                "primary_id": primary["id"],
                "primary_name": primary_name,
                "secondary_id": sec["id"],
                "secondary_name": sec_name,
                "status": None,
            }

            if dry_run:
                detail["status"] = "dry_run"
                stats["skipped_dry_run"] += 1
            else:
                try:
                    merge_companies(primary["id"], sec["id"])
                    detail["status"] = "merged"
                    stats["merges_performed"] += 1
                    time.sleep(0.2)  # be gentle with the API
                except Exception as e:
                    detail["status"] = f"error: {e}"
                    stats["merges_failed"] += 1
                    print(f"  ⚠️  Merge failed: {e}")

            stats["details"].append(detail)

    print(f"\n{'='*50}")
    print(f"✅ Done!")
    print(f"  Companies scanned:   {stats['total_companies']}")
    print(f"  Duplicate domains:   {stats['duplicate_domains']}")
    if dry_run:
        print(f"  Would merge:         {stats['skipped_dry_run']}")
    else:
        print(f"  Merges performed:    {stats['merges_performed']}")
        print(f"  Merges failed:       {stats['merges_failed']}")

    return stats

if __name__ == "__main__":
    import sys
    dry = "--dry-run" in sys.argv
    run_dedup(dry_run=dry)

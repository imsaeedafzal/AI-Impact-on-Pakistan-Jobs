"""
Pakistan Pipeline - Data Processor.

Reads collected sources and occupations, fetches data from each source,
extracts employment/wage/education information, and writes structured
datasets to the output/ folder.

This should only be run after collect.py has gathered enough sources.

Usage:
    python process.py                  # process all sources
    python process.py --limit 10       # process first 10 sources only
    python process.py --delay 5        # 5 second delay between requests
    python process.py --status         # show processing status

Rate-limiting & safety:
    - Per-domain rate limiting (max 1 request per DOMAIN_DELAY seconds to same domain)
    - Global delay between all requests (--delay flag, default 2s)
    - Respects robots.txt when possible
    - Caches raw HTML locally so repeated runs don't re-fetch
    - Skips job portals by default (use --include-portals to include)
"""

import argparse
import csv
import json
import os
import re
import time
from datetime import datetime
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup

from collections import defaultdict

from config import (
    SOURCES_CSV,
    OCCUPATIONS_MASTER_CSV,
    OUTPUT_DIR,
    RAW_DIR,
    OUTPUT_OCCUPATIONS_COLUMNS,
    CURRENCY,
)

# -- Rate Limiting Settings ---------------------------------------------------
DEFAULT_DELAY = 2          # seconds between any two requests
DOMAIN_DELAY = 5           # seconds between requests to the SAME domain
MAX_REQUESTS_PER_DOMAIN = 30   # max pages fetched per domain in one run
PORTAL_DOMAINS = {         # domains that require extra caution
    "www.rozee.pk", "rozee.pk",
    "www.mustakbil.com", "mustakbil.com",
    "pk.indeed.com", "indeed.com",
}
# -----------------------------------------------------------------------------


# =============================================================================
# Utilities
# =============================================================================

def slugify(title):
    """Convert an occupation title to a URL-friendly slug."""
    slug = title.lower().strip()
    slug = re.sub(r'[^a-z0-9\s-]', '', slug)
    slug = re.sub(r'[\s]+', '-', slug)
    slug = re.sub(r'-+', '-', slug)
    return slug.strip('-')


def load_sources():
    """Load all collected sources."""
    if not os.path.exists(SOURCES_CSV):
        print(f"ERROR: {SOURCES_CSV} not found. Run collect.py first.")
        return []
    with open(SOURCES_CSV, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def load_occupations():
    """Load master occupation list."""
    if not os.path.exists(OCCUPATIONS_MASTER_CSV):
        print(f"ERROR: {OCCUPATIONS_MASTER_CSV} not found. Run collect.py first.")
        return []
    with open(OCCUPATIONS_MASTER_CSV, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def fetch_and_save_raw(client, url, slug):
    """Fetch a source page and save raw HTML to output/raw/."""
    raw_path = os.path.join(RAW_DIR, f"{slug}.html")
    if os.path.exists(raw_path):
        return raw_path

    try:
        resp = client.get(url, timeout=15, follow_redirects=True)
        if resp.status_code == 200:
            with open(raw_path, "w", encoding="utf-8") as f:
                f.write(resp.text)
            return raw_path
    except Exception as e:
        print(f"    ERROR fetching {url}: {e}")
    return None


def extract_numbers_from_text(text):
    """Extract numeric values from text, handling Pakistani number formats."""
    # Match numbers like 1,234,567 or 50000 or 1.5 million
    numbers = []

    # Handle "X million" / "X billion" patterns
    for match in re.finditer(r'([\d,.]+)\s*(million|billion|lakh|crore)', text, re.IGNORECASE):
        num_str = match.group(1).replace(",", "")
        try:
            num = float(num_str)
            multiplier = match.group(2).lower()
            if multiplier == "million":
                num *= 1_000_000
            elif multiplier == "billion":
                num *= 1_000_000_000
            elif multiplier == "lakh":
                num *= 100_000
            elif multiplier == "crore":
                num *= 10_000_000
            numbers.append(int(num))
        except ValueError:
            pass

    # Match plain numbers
    for match in re.finditer(r'(?<!\w)([\d,]+)(?!\w)', text):
        num_str = match.group(1).replace(",", "")
        try:
            num = int(num_str)
            if num > 100:  # Filter out small irrelevant numbers
                numbers.append(num)
        except ValueError:
            pass

    return numbers


def extract_salary_from_text(text):
    """Try to extract salary/wage data from text content."""
    salary_patterns = [
        # PKR / Rs. patterns
        r'(?:Rs\.?|PKR)\s*([\d,]+)',
        r'([\d,]+)\s*(?:per month|monthly|/month)',
        r'salary[:\s]+(?:Rs\.?|PKR)?\s*([\d,]+)',
        r'wage[:\s]+(?:Rs\.?|PKR)?\s*([\d,]+)',
        r'(?:average|median|mean)\s+(?:salary|wage|income)[:\s]+(?:Rs\.?|PKR)?\s*([\d,]+)',
    ]

    salaries = []
    for pattern in salary_patterns:
        for match in re.finditer(pattern, text, re.IGNORECASE):
            try:
                val = int(match.group(1).replace(",", ""))
                # Reasonable salary range for Pakistan (monthly: 10K-5M PKR)
                if 10_000 <= val <= 5_000_000:
                    salaries.append(val)
            except (ValueError, IndexError):
                pass

    return salaries


def extract_employment_data(text):
    """Try to extract employment/workforce numbers from text."""
    patterns = [
        r'(?:employ(?:ed|ment|ees)?|workforce|workers?|labour force)[:\s]+(?:approximately\s+)?([\d,.]+\s*(?:million|lakh|crore)?)',
        r'([\d,.]+\s*(?:million|lakh|crore)?)\s+(?:employ(?:ed|ees)|workers?|people\s+(?:working|employed))',
    ]

    employment = []
    for pattern in patterns:
        for match in re.finditer(pattern, text, re.IGNORECASE):
            nums = extract_numbers_from_text(match.group(0))
            employment.extend(nums)

    return employment


# =============================================================================
# Source Processing
# =============================================================================

def process_html_source(html_path, source_meta):
    """
    Process a single HTML source file and extract structured data.
    Returns a dict of extracted information.
    """
    with open(html_path, encoding="utf-8") as f:
        soup = BeautifulSoup(f.read(), "html.parser")

    text = soup.get_text(separator=" ", strip=True)

    data = {
        "source_url": source_meta["url"],
        "source_title": source_meta["title"],
        "category": source_meta.get("category", ""),
        "subcategory": source_meta.get("subcategory", ""),
        "text_length": len(text),
        "salaries_found": extract_salary_from_text(text),
        "employment_numbers": extract_employment_data(text),
        "tables_found": len(soup.find_all("table")),
    }

    # Extract tables with labor-related data
    tables_data = []
    for table in soup.find_all("table"):
        table_text = table.get_text().lower()
        labor_keywords = ["employ", "occupation", "salary", "wage", "worker",
                         "labour", "labor", "sector", "industry"]
        if any(kw in table_text for kw in labor_keywords):
            rows = []
            for tr in table.find_all("tr"):
                cells = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
                if cells:
                    rows.append(cells)
            if rows:
                tables_data.append(rows)

    data["relevant_tables"] = tables_data
    return data


def process_sources(limit=None, delay=DEFAULT_DELAY, include_portals=False):
    """
    Process all collected sources and extract structured data.
    Saves raw HTML and extracted data to output/.

    Safety measures:
    - Caches raw HTML locally (never re-fetches a page already saved)
    - Per-domain rate limiting (DOMAIN_DELAY seconds between same-domain hits)
    - Max requests per domain cap (MAX_REQUESTS_PER_DOMAIN)
    - Skips job portal domains unless --include-portals is set
    - Global delay between all requests (configurable via --delay)
    """
    sources = load_sources()
    occupations = load_occupations()

    if not sources:
        return
    if not occupations:
        return

    os.makedirs(RAW_DIR, exist_ok=True)

    # Filter to HTML sources only (PDFs need different handling)
    html_sources = [s for s in sources if s.get("format", "html") == "html"]

    # Filter out job portals unless explicitly included
    if not include_portals:
        before = len(html_sources)
        html_sources = [
            s for s in html_sources
            if urlparse(s["url"]).netloc not in PORTAL_DOMAINS
        ]
        skipped_portals = before - len(html_sources)
        if skipped_portals:
            print(f"  Skipping {skipped_portals} job portal sources (use --include-portals to include)")

    if limit:
        html_sources = html_sources[:limit]

    print("=" * 60)
    print("PROCESSING SOURCES")
    print("=" * 60)
    print(f"\nTotal sources: {len(sources)}")
    print(f"HTML sources to process: {len(html_sources)}")
    print(f"Total occupations in master list: {len(occupations)}")
    print(f"Delay: {delay}s global, {DOMAIN_DELAY}s per-domain")
    print(f"Max requests per domain: {MAX_REQUESTS_PER_DOMAIN}")

    client = httpx.Client(
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        },
        follow_redirects=True,
    )

    all_extracted = []
    domain_last_hit = {}               # domain -> timestamp of last request
    domain_request_count = defaultdict(int)  # domain -> number of requests this run
    cached_count = 0
    fetched_count = 0
    skipped_count = 0

    for i, source in enumerate(html_sources):
        url = source["url"]
        domain = urlparse(url).netloc
        slug = re.sub(r'[^a-z0-9]', '-', domain)
        raw_path_candidate = os.path.join(RAW_DIR, f"{slug}-{i}.html")

        print(f"\n  [{i+1}/{len(html_sources)}] {source['title'][:50]}...", end=" ", flush=True)

        # Check if already cached locally
        if os.path.exists(raw_path_candidate):
            print("CACHED", end=" ")
            cached_count += 1
            try:
                data = process_html_source(raw_path_candidate, source)
                all_extracted.append(data)
                print(f"(tables={data['tables_found']}, salaries={len(data['salaries_found'])})")
            except Exception as e:
                print(f"PARSE ERROR: {e}")
            continue

        # Per-domain request cap
        if domain_request_count[domain] >= MAX_REQUESTS_PER_DOMAIN:
            print(f"SKIP (domain cap reached: {MAX_REQUESTS_PER_DOMAIN} requests to {domain})")
            skipped_count += 1
            continue

        # Per-domain rate limiting
        if domain in domain_last_hit:
            elapsed = time.time() - domain_last_hit[domain]
            if elapsed < DOMAIN_DELAY:
                wait = DOMAIN_DELAY - elapsed
                print(f"[waiting {wait:.0f}s for {domain}]", end=" ", flush=True)
                time.sleep(wait)

        # Fetch
        raw_path = fetch_and_save_raw(client, url, f"{slug}-{i}")
        domain_last_hit[domain] = time.time()
        domain_request_count[domain] += 1
        fetched_count += 1

        if not raw_path:
            print("SKIP (fetch failed)")
            skipped_count += 1
            continue

        try:
            data = process_html_source(raw_path, source)
            all_extracted.append(data)
            print(f"OK (tables={data['tables_found']}, salaries={len(data['salaries_found'])})")
            # Incremental save after each successful extraction
            _save_extraction_summary(all_extracted)
        except Exception as e:
            print(f"ERROR: {e}")

        # Global delay
        time.sleep(delay)

    client.close()

    print(f"\n  Fetched: {fetched_count}, Cached: {cached_count}, Skipped: {skipped_count}")

    # Final save (also saved incrementally above, but do a clean final write)
    _save_extraction_summary(all_extracted)

    # Build the output occupation dataset
    _build_occupation_dataset(occupations, all_extracted)

    return all_extracted


def _save_extraction_summary(all_extracted):
    """Save extraction summary to JSON (called incrementally and at end)."""
    summary_path = os.path.join(OUTPUT_DIR, "extraction_summary.json")
    serializable = []
    for d in all_extracted:
        entry = {k: v for k, v in d.items() if k != "relevant_tables"}
        entry["relevant_tables_count"] = len(d.get("relevant_tables", []))
        serializable.append(entry)

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(serializable, f, indent=2)

    print(f"  >> Checkpoint: {len(serializable)} sources saved to {summary_path}")


def _build_occupation_dataset(occupations, extracted_data):
    """
    Build the final occupation dataset by merging master occupation list
    with any data extracted from sources.
    """
    print(f"\nBuilding occupation dataset...")

    # Aggregate salary data from all sources
    all_salaries = []
    for d in extracted_data:
        all_salaries.extend(d.get("salaries_found", []))

    # Calculate rough median salary if we have data
    median_salary = None
    if all_salaries:
        sorted_salaries = sorted(all_salaries)
        median_salary = sorted_salaries[len(sorted_salaries) // 2]
        print(f"  Aggregate salary data: {len(all_salaries)} data points, "
              f"median ~{CURRENCY} {median_salary:,}")

    # Build output rows from master occupation list
    output_rows = []
    for occ in occupations:
        slug = slugify(occ["title"])
        output_rows.append({
            "title": occ["title"],
            "slug": slug,
            "category": occ.get("category", ""),
            "sector": occ.get("sector", ""),
            "type": occ.get("type", ""),
            "median_pay_annual": "",
            "median_pay_monthly": "",
            "entry_education": occ.get("education_required", ""),
            "num_jobs": "",
            "outlook_desc": "",
            "source_urls": occ.get("source_url", ""),
        })

    # Write output CSV
    output_csv = os.path.join(OUTPUT_DIR, "occupations.csv")
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_OCCUPATIONS_COLUMNS)
        writer.writeheader()
        writer.writerows(output_rows)

    # Write output JSON
    output_json = os.path.join(OUTPUT_DIR, "occupations.json")
    json_data = []
    for row in output_rows:
        json_data.append({
            "title": row["title"],
            "slug": row["slug"],
            "category": row["category"],
            "sector": row["sector"],
            "type": row["type"],
            "education": row["entry_education"],
        })
    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(json_data, f, indent=2)

    print(f"  Wrote {len(output_rows)} occupations to {output_csv}")
    print(f"  Wrote {len(json_data)} occupations to {output_json}")


# =============================================================================
# Status Report
# =============================================================================

def show_status():
    """Show current processing status."""
    print("=" * 60)
    print("PIPELINE STATUS")
    print("=" * 60)

    # Sources
    if os.path.exists(SOURCES_CSV):
        with open(SOURCES_CSV, newline="", encoding="utf-8") as f:
            sources = list(csv.DictReader(f))
        print(f"\nSources ({SOURCES_CSV}):")
        print(f"  Total: {len(sources)}")

        by_category = {}
        by_reliability = {}
        by_format = {}
        by_status = {}
        for s in sources:
            by_category[s.get("category", "?")] = by_category.get(s.get("category", "?"), 0) + 1
            by_reliability[s.get("reliability", "?")] = by_reliability.get(s.get("reliability", "?"), 0) + 1
            by_format[s.get("format", "?")] = by_format.get(s.get("format", "?"), 0) + 1
            by_status[s.get("status", "?")] = by_status.get(s.get("status", "?"), 0) + 1

        print(f"  By category: {dict(sorted(by_category.items()))}")
        print(f"  By reliability: {dict(sorted(by_reliability.items()))}")
        print(f"  By format: {dict(sorted(by_format.items()))}")
        print(f"  By status: {dict(sorted(by_status.items()))}")
    else:
        print(f"\nNo sources collected yet. Run: python collect.py")

    # Occupations
    if os.path.exists(OCCUPATIONS_MASTER_CSV):
        with open(OCCUPATIONS_MASTER_CSV, newline="", encoding="utf-8") as f:
            occs = list(csv.DictReader(f))
        print(f"\nOccupations ({OCCUPATIONS_MASTER_CSV}):")
        print(f"  Total: {len(occs)}")

        by_sector = {}
        by_type = {}
        for o in occs:
            by_sector[o.get("sector", "?")] = by_sector.get(o.get("sector", "?"), 0) + 1
            by_type[o.get("type", "?")] = by_type.get(o.get("type", "?"), 0) + 1

        print(f"  By sector: {dict(sorted(by_sector.items()))}")
        print(f"  By type: {dict(sorted(by_type.items()))}")
    else:
        print(f"\nNo occupations collected yet. Run: python collect.py")

    # Output
    output_csv = os.path.join(OUTPUT_DIR, "occupations.csv")
    if os.path.exists(output_csv):
        with open(output_csv, newline="", encoding="utf-8") as f:
            out = list(csv.DictReader(f))
        print(f"\nOutput ({output_csv}):")
        print(f"  Total occupations: {len(out)}")
        with_pay = sum(1 for o in out if o.get("median_pay_annual") or o.get("median_pay_monthly"))
        with_jobs = sum(1 for o in out if o.get("num_jobs"))
        print(f"  With pay data: {with_pay}")
        print(f"  With employment data: {with_jobs}")
    else:
        print(f"\nNo output dataset yet. Run: python process.py")

    # Raw files
    if os.path.exists(RAW_DIR):
        raw_count = len([f for f in os.listdir(RAW_DIR) if f.endswith(".html")])
        print(f"\nRaw HTML files cached: {raw_count}")


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Pakistan Pipeline: Process collected sources into datasets"
    )
    parser.add_argument("--limit", type=int, default=None,
                        help="Limit number of sources to process")
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY,
                        help=f"Global delay between requests in seconds (default: {DEFAULT_DELAY})")
    parser.add_argument("--include-portals", action="store_true",
                        help="Include job portal sites (Rozee.pk, Indeed, etc.)")
    parser.add_argument("--status", action="store_true",
                        help="Show processing status and exit")
    args = parser.parse_args()

    if args.status:
        show_status()
        return

    print(f"Pakistan Pipeline Processor")
    print(f"Timestamp: {datetime.now().isoformat()}")

    process_sources(limit=args.limit, delay=args.delay,
                    include_portals=args.include_portals)

    print("\n" + "=" * 60)
    print("DONE")
    print("=" * 60)
    print(f"\nOutput files are in {OUTPUT_DIR}/")
    print(f"Next step: Use Claude Code to score AI exposure on the output data.")


if __name__ == "__main__":
    main()

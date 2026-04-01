"""
Pakistan Pipeline - Source & Occupation Collector.

Discovers trustworthy data sources related to Pakistan's labor market
and collects all possible occupations. Results are stored incrementally
in CSV files that grow over time (never deleted, only appended).

Usage:
    python collect.py                        # full run: sources + occupations
    python collect.py --sources-only         # only discover sources
    python collect.py --occupations-only     # only discover occupations
    python collect.py --verify               # verify existing sources are alive

Each run gets a unique run_id for audit tracking.
"""

import argparse
import csv
import os
import re
import time
import uuid
from datetime import datetime
from urllib.parse import urlparse, urljoin

import httpx
from bs4 import BeautifulSoup

from config import (
    SEED_SOURCES,
    SEARCH_KEYWORDS,
    SOURCES_CSV,
    OCCUPATIONS_MASTER_CSV,
    SOURCE_LOGS_CSV,
    SOURCES_COLUMNS,
    OCCUPATIONS_MASTER_COLUMNS,
    DATA_DIR,
    OCCUPATION_SECTORS,
    OCCUPATION_TYPES,
)


# =============================================================================
# Utilities
# =============================================================================

def get_run_id():
    """Generate a short unique run ID."""
    return datetime.now().strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:6]


def ensure_dirs():
    """Create data directories if they don't exist."""
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs("output/raw", exist_ok=True)


def load_existing_sources():
    """Load existing sources from CSV. Returns set of URLs for dedup."""
    urls = set()
    if not os.path.exists(SOURCES_CSV):
        return urls
    with open(SOURCES_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            urls.add(row["url"].strip().rstrip("/"))
    return urls


def load_existing_occupations():
    """Load existing occupations from CSV. Returns set of normalized titles."""
    titles = set()
    if not os.path.exists(OCCUPATIONS_MASTER_CSV):
        return titles
    with open(OCCUPATIONS_MASTER_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            titles.add(row["title"].strip().lower())
    return titles


def append_to_csv(filepath, columns, rows):
    """Append rows to a CSV file. Creates with header if file doesn't exist."""
    file_exists = os.path.exists(filepath) and os.path.getsize(filepath) > 0
    with open(filepath, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)


def log_run(run_id, action, details):
    """Log a pipeline run to source_logs.csv."""
    columns = ["run_id", "timestamp", "action", "details"]
    row = {
        "run_id": run_id,
        "timestamp": datetime.now().isoformat(),
        "action": action,
        "details": details,
    }
    append_to_csv(SOURCE_LOGS_CSV, columns, [row])


def normalize_url(url):
    """Normalize a URL for deduplication."""
    return url.strip().rstrip("/")


def detect_format(url, content_type=""):
    """Detect the format of a source based on URL and content type."""
    url_lower = url.lower()
    if url_lower.endswith(".pdf"):
        return "pdf"
    if url_lower.endswith(".csv"):
        return "csv"
    if url_lower.endswith(".xlsx") or url_lower.endswith(".xls"):
        return "excel"
    if "application/pdf" in content_type:
        return "pdf"
    if "text/csv" in content_type:
        return "csv"
    return "html"


def fetch_page(client, url, timeout=15):
    """Fetch a page and return (response, soup) or (None, None) on failure."""
    try:
        resp = client.get(url, timeout=timeout, follow_redirects=True)
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, "html.parser")
            return resp, soup
        else:
            print(f"    HTTP {resp.status_code}: {url}")
            return resp, None
    except Exception as e:
        print(f"    ERROR fetching {url}: {e}")
        return None, None


# =============================================================================
# Source Discovery
# =============================================================================

def discover_seed_sources(run_id, existing_urls):
    """Register seed sources from config that aren't already in the CSV."""
    new_sources = []
    skipped = 0

    for seed in SEED_SOURCES:
        normalized = normalize_url(seed["url"])
        if normalized in existing_urls:
            skipped += 1
            continue

        new_sources.append({
            "url": seed["url"],
            "title": seed["title"],
            "domain": urlparse(seed["url"]).netloc,
            "category": seed["category"],
            "subcategory": seed["subcategory"],
            "reliability": seed["reliability"],
            "format": "html",
            "description": seed["description"],
            "date_found": datetime.now().strftime("%Y-%m-%d"),
            "status": "unverified",
            "run_id": run_id,
        })
        existing_urls.add(normalized)

    return new_sources, skipped


def crawl_source_for_subpages(client, base_url, existing_urls, run_id,
                               keywords=None, max_links=20):
    """
    Crawl a source's main page to find sub-pages with labor/employment data.
    Returns list of new source rows found.
    """
    if keywords is None:
        keywords = [
            "labour", "labor", "employment", "occupation", "workforce",
            "salary", "wage", "job", "survey", "census", "statistics",
            "economic", "manpower", "skill", "vocational", "training",
            "lfs", "pslm", "hies",
        ]

    resp, soup = fetch_page(client, base_url)
    if not soup:
        return []

    domain = urlparse(base_url).netloc
    found = []
    seen_on_page = set()

    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        text = a_tag.get_text(strip=True).lower()
        full_url = urljoin(base_url, href)
        normalized = normalize_url(full_url)

        # Only follow links on the same domain
        if urlparse(full_url).netloc != domain:
            continue

        # Skip if already known or seen in this crawl
        if normalized in existing_urls or normalized in seen_on_page:
            continue

        # Check if the link text or URL contains relevant keywords
        combined = text + " " + href.lower()
        if not any(kw in combined for kw in keywords):
            continue

        seen_on_page.add(normalized)
        link_title = a_tag.get_text(strip=True) or href

        # Detect format
        fmt = detect_format(full_url)

        found.append({
            "url": full_url,
            "title": link_title[:200],
            "domain": domain,
            "category": "government",  # inherited from parent
            "subcategory": "discovered-subpage",
            "reliability": "high",
            "format": fmt,
            "description": f"Sub-page discovered from {base_url}",
            "date_found": datetime.now().strftime("%Y-%m-%d"),
            "status": "unverified",
            "run_id": run_id,
        })

        if len(found) >= max_links:
            break

    return found


def discover_sources(run_id, verify=False):
    """
    Main source discovery routine.

    1. Register seed sources from config (skip existing).
    2. Crawl each seed source for sub-pages with labor data.
    3. Optionally verify existing sources are still alive.
    """
    ensure_dirs()
    existing_urls = load_existing_sources()
    all_new = []

    print("=" * 60)
    print("PHASE 1: SOURCE DISCOVERY")
    print("=" * 60)

    # Step 1: Seed sources
    print(f"\n[1/3] Registering seed sources...")
    seed_new, seed_skipped = discover_seed_sources(run_id, existing_urls)
    all_new.extend(seed_new)
    print(f"  Added: {len(seed_new)}, Already existed: {seed_skipped}")

    # Step 2: Crawl seed sources for sub-pages
    print(f"\n[2/3] Crawling seed sources for sub-pages...")
    client = httpx.Client(
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
        },
        follow_redirects=True,
    )

    crawl_targets = SEED_SOURCES[:15]  # Crawl top 15 seed sources
    for i, seed in enumerate(crawl_targets):
        print(f"  [{i+1}/{len(crawl_targets)}] Crawling {seed['title']}...", end=" ", flush=True)
        try:
            sub_pages = crawl_source_for_subpages(
                client, seed["url"], existing_urls, run_id
            )
            all_new.extend(sub_pages)
            for sp in sub_pages:
                existing_urls.add(normalize_url(sp["url"]))
            print(f"found {len(sub_pages)} sub-pages")
        except Exception as e:
            print(f"ERROR: {e}")
        time.sleep(1)  # Be polite

    # Step 3: Verify existing sources (optional)
    if verify:
        print(f"\n[3/3] Verifying existing sources...")
        _verify_existing_sources(client)
    else:
        print(f"\n[3/3] Skipping verification (use --verify to enable)")

    client.close()

    # Save all new sources
    if all_new:
        append_to_csv(SOURCES_CSV, SOURCES_COLUMNS, all_new)
        print(f"\n>> Appended {len(all_new)} new sources to {SOURCES_CSV}")
    else:
        print(f"\n>> No new sources found this run.")

    log_run(run_id, "discover_sources", f"new={len(all_new)}")
    return all_new


def _verify_existing_sources(client):
    """Check if existing sources are still reachable and update status."""
    if not os.path.exists(SOURCES_CSV):
        print("  No sources to verify.")
        return

    rows = []
    with open(SOURCES_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    updated = 0
    for row in rows:
        url = row["url"]
        try:
            resp = client.head(url, timeout=10, follow_redirects=True)
            new_status = "active" if resp.status_code == 200 else f"http-{resp.status_code}"
        except Exception:
            new_status = "dead"

        if row["status"] != new_status:
            row["status"] = new_status
            updated += 1
        time.sleep(0.5)

    # Rewrite the entire file with updated statuses
    with open(SOURCES_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=SOURCES_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)

    print(f"  Verified {len(rows)} sources, updated {updated} statuses.")


# =============================================================================
# Occupation Discovery
# =============================================================================

# Pakistan Standard Classification of Occupations (PSCO) based on ISCO-08
# This is the core taxonomy of Pakistani occupations.
PAKISTAN_OCCUPATIONS = [
    # --- Agriculture & Livestock ---
    ("Farmer (Crop)", "", "Agriculture & Livestock", "Agriculture & Livestock", "agricultural", "No formal education"),
    ("Livestock Farmer", "", "Agriculture & Livestock", "Agriculture & Livestock", "agricultural", "No formal education"),
    ("Agricultural Laborer", "", "Agriculture & Livestock", "Agriculture & Livestock", "unskilled-labor", "No formal education"),
    ("Tractor Operator", "", "Agriculture & Livestock", "Agriculture & Livestock", "skilled-labor", "Primary (Grade 5)"),
    ("Irrigation Worker", "", "Agriculture & Livestock", "Agriculture & Livestock", "skilled-labor", "Primary (Grade 5)"),
    ("Fisherman", "", "Agriculture & Livestock", "Agriculture & Livestock", "skilled-labor", "No formal education"),
    ("Poultry Farmer", "", "Agriculture & Livestock", "Agriculture & Livestock", "agricultural", "Middle (Grade 8)"),
    ("Dairy Farmer", "", "Agriculture & Livestock", "Agriculture & Livestock", "agricultural", "Primary (Grade 5)"),
    ("Agricultural Extension Officer", "", "Agriculture & Livestock", "Agriculture & Livestock", "professional", "Bachelor's degree"),
    ("Veterinarian", "", "Agriculture & Livestock", "Healthcare & Medical", "medical", "Professional degree (MBBS, LLB, CA, etc.)"),
    ("Floriculturist / Nursery Worker", "", "Agriculture & Livestock", "Agriculture & Livestock", "skilled-labor", "Middle (Grade 8)"),

    # --- Textiles & Garments ---
    ("Textile Mill Worker", "", "Textiles & Garments", "Textiles & Garments", "skilled-labor", "Primary (Grade 5)"),
    ("Power Loom Operator", "", "Textiles & Garments", "Textiles & Garments", "skilled-labor", "Primary (Grade 5)"),
    ("Garment Stitcher / Tailor", "", "Textiles & Garments", "Textiles & Garments", "trades", "Middle (Grade 8)"),
    ("Textile Designer", "", "Textiles & Garments", "Textiles & Garments", "technical", "Bachelor's degree"),
    ("Quality Inspector (Textiles)", "", "Textiles & Garments", "Textiles & Garments", "technical", "Intermediate (Grade 12)"),
    ("Dyeing & Finishing Operator", "", "Textiles & Garments", "Textiles & Garments", "skilled-labor", "Primary (Grade 5)"),
    ("Embroidery Worker", "", "Textiles & Garments", "Textiles & Garments", "trades", "No formal education"),
    ("Spinning Machine Operator", "", "Textiles & Garments", "Textiles & Garments", "skilled-labor", "Primary (Grade 5)"),

    # --- Construction & Real Estate ---
    ("Mason / Bricklayer", "", "Construction & Real Estate", "Construction & Real Estate", "trades", "No formal education"),
    ("Carpenter", "", "Construction & Real Estate", "Construction & Real Estate", "trades", "Primary (Grade 5)"),
    ("Plumber", "", "Construction & Real Estate", "Construction & Real Estate", "trades", "Middle (Grade 8)"),
    ("Electrician", "", "Construction & Real Estate", "Construction & Real Estate", "trades", "Middle (Grade 8)"),
    ("Painter (Building)", "", "Construction & Real Estate", "Construction & Real Estate", "trades", "No formal education"),
    ("Welder", "", "Construction & Real Estate", "Construction & Real Estate", "trades", "Middle (Grade 8)"),
    ("Steel Fixer / Iron Worker", "", "Construction & Real Estate", "Construction & Real Estate", "trades", "No formal education"),
    ("Civil Engineer", "", "Construction & Real Estate", "Construction & Real Estate", "professional", "Bachelor's degree"),
    ("Architect", "", "Construction & Real Estate", "Construction & Real Estate", "professional", "Bachelor's degree"),
    ("Construction Laborer", "", "Construction & Real Estate", "Construction & Real Estate", "unskilled-labor", "No formal education"),
    ("Crane Operator", "", "Construction & Real Estate", "Construction & Real Estate", "skilled-labor", "Middle (Grade 8)"),
    ("Real Estate Agent", "", "Construction & Real Estate", "Construction & Real Estate", "services", "Intermediate (Grade 12)"),
    ("Surveyor", "", "Construction & Real Estate", "Construction & Real Estate", "technical", "Bachelor's degree"),

    # --- Manufacturing & Industry ---
    ("Factory Worker (General)", "", "Manufacturing & Industry", "Manufacturing & Industry", "unskilled-labor", "Primary (Grade 5)"),
    ("Machine Operator", "", "Manufacturing & Industry", "Manufacturing & Industry", "skilled-labor", "Middle (Grade 8)"),
    ("Production Supervisor", "", "Manufacturing & Industry", "Manufacturing & Industry", "managerial", "Intermediate (Grade 12)"),
    ("Quality Control Inspector", "", "Manufacturing & Industry", "Manufacturing & Industry", "technical", "Intermediate (Grade 12)"),
    ("Industrial Engineer", "", "Manufacturing & Industry", "Manufacturing & Industry", "professional", "Bachelor's degree"),
    ("Mechanical Engineer", "", "Manufacturing & Industry", "Manufacturing & Industry", "professional", "Bachelor's degree"),
    ("Chemical Engineer", "", "Manufacturing & Industry", "Manufacturing & Industry", "professional", "Bachelor's degree"),
    ("Electrical Engineer", "", "Manufacturing & Industry", "Manufacturing & Industry", "professional", "Bachelor's degree"),
    ("Electronics Technician", "", "Manufacturing & Industry", "Manufacturing & Industry", "technical", "Diploma / Certificate"),
    ("CNC Machine Operator", "", "Manufacturing & Industry", "Manufacturing & Industry", "technical", "Diploma / Certificate"),
    ("Packaging Worker", "", "Manufacturing & Industry", "Manufacturing & Industry", "unskilled-labor", "Primary (Grade 5)"),

    # --- Transport & Logistics ---
    ("Truck Driver", "", "Transport & Logistics", "Transport & Logistics", "skilled-labor", "Primary (Grade 5)"),
    ("Bus Driver", "", "Transport & Logistics", "Transport & Logistics", "skilled-labor", "Middle (Grade 8)"),
    ("Rickshaw Driver", "", "Transport & Logistics", "Transport & Logistics", "services", "No formal education"),
    ("Taxi / Ride-hail Driver", "", "Transport & Logistics", "Transport & Logistics", "services", "Middle (Grade 8)"),
    ("Delivery Rider", "", "Transport & Logistics", "Transport & Logistics", "services", "Middle (Grade 8)"),
    ("Warehouse Worker", "", "Transport & Logistics", "Transport & Logistics", "unskilled-labor", "Primary (Grade 5)"),
    ("Logistics Coordinator", "", "Transport & Logistics", "Transport & Logistics", "clerical", "Bachelor's degree"),
    ("Customs Clearing Agent", "", "Transport & Logistics", "Transport & Logistics", "clerical", "Intermediate (Grade 12)"),
    ("Ship/Port Worker", "", "Transport & Logistics", "Transport & Logistics", "skilled-labor", "Middle (Grade 8)"),
    ("Airline Pilot", "", "Transport & Logistics", "Transport & Logistics", "professional", "Bachelor's degree"),
    ("Train Driver", "", "Transport & Logistics", "Transport & Logistics", "skilled-labor", "Matric (Grade 10)"),
    ("Air Traffic Controller", "", "Transport & Logistics", "Transport & Logistics", "professional", "Bachelor's degree"),

    # --- Information Technology ---
    ("Software Developer", "", "Information Technology", "Information Technology", "technical", "Bachelor's degree"),
    ("Web Developer", "", "Information Technology", "Information Technology", "technical", "Bachelor's degree"),
    ("Mobile App Developer", "", "Information Technology", "Information Technology", "technical", "Bachelor's degree"),
    ("Data Analyst", "", "Information Technology", "Information Technology", "technical", "Bachelor's degree"),
    ("Data Scientist", "", "Information Technology", "Information Technology", "professional", "Master's degree"),
    ("Database Administrator", "", "Information Technology", "Information Technology", "technical", "Bachelor's degree"),
    ("Network Engineer", "", "Information Technology", "Information Technology", "technical", "Bachelor's degree"),
    ("Cybersecurity Analyst", "", "Information Technology", "Information Technology", "technical", "Bachelor's degree"),
    ("IT Support Technician", "", "Information Technology", "Information Technology", "technical", "Diploma / Certificate"),
    ("System Administrator", "", "Information Technology", "Information Technology", "technical", "Bachelor's degree"),
    ("UI/UX Designer", "", "Information Technology", "Information Technology", "technical", "Bachelor's degree"),
    ("DevOps Engineer", "", "Information Technology", "Information Technology", "technical", "Bachelor's degree"),
    ("AI / Machine Learning Engineer", "", "Information Technology", "Information Technology", "professional", "Master's degree"),
    ("Cloud Engineer", "", "Information Technology", "Information Technology", "technical", "Bachelor's degree"),
    ("QA / Test Engineer", "", "Information Technology", "Information Technology", "technical", "Bachelor's degree"),
    ("Computer Hardware Technician", "", "Information Technology", "Information Technology", "trades", "Diploma / Certificate"),
    ("SEO / Digital Marketing Specialist", "", "Information Technology", "Information Technology", "technical", "Bachelor's degree"),
    ("Graphic Designer", "", "Information Technology", "Information Technology", "technical", "Bachelor's degree"),
    ("Freelance IT Worker", "", "Information Technology", "Information Technology", "technical", "Diploma / Certificate"),
    ("E-commerce Specialist", "", "Information Technology", "Information Technology", "technical", "Bachelor's degree"),

    # --- Healthcare & Medical ---
    ("Doctor (MBBS)", "", "Healthcare & Medical", "Healthcare & Medical", "medical", "Professional degree (MBBS, LLB, CA, etc.)"),
    ("Surgeon", "", "Healthcare & Medical", "Healthcare & Medical", "medical", "Professional degree (MBBS, LLB, CA, etc.)"),
    ("Dentist", "", "Healthcare & Medical", "Healthcare & Medical", "medical", "Professional degree (MBBS, LLB, CA, etc.)"),
    ("Nurse", "", "Healthcare & Medical", "Healthcare & Medical", "medical", "Diploma / Certificate"),
    ("Pharmacist", "", "Healthcare & Medical", "Healthcare & Medical", "medical", "Professional degree (MBBS, LLB, CA, etc.)"),
    ("Lab Technician", "", "Healthcare & Medical", "Healthcare & Medical", "technical", "Diploma / Certificate"),
    ("Paramedic / EMT", "", "Healthcare & Medical", "Healthcare & Medical", "medical", "Diploma / Certificate"),
    ("Lady Health Worker", "", "Healthcare & Medical", "Healthcare & Medical", "medical", "Matric (Grade 10)"),
    ("Physiotherapist", "", "Healthcare & Medical", "Healthcare & Medical", "medical", "Bachelor's degree"),
    ("Radiologist / Imaging Technician", "", "Healthcare & Medical", "Healthcare & Medical", "technical", "Diploma / Certificate"),
    ("Psychologist / Counselor", "", "Healthcare & Medical", "Healthcare & Medical", "professional", "Master's degree"),
    ("Homeopathic / Hakeem Practitioner", "", "Healthcare & Medical", "Healthcare & Medical", "medical", "Diploma / Certificate"),
    ("Midwife / Dai", "", "Healthcare & Medical", "Healthcare & Medical", "medical", "No formal education"),
    ("Optician / Optometrist", "", "Healthcare & Medical", "Healthcare & Medical", "medical", "Bachelor's degree"),
    ("Medical Transcriptionist", "", "Healthcare & Medical", "Healthcare & Medical", "clerical", "Intermediate (Grade 12)"),
    ("Hospital Administrator", "", "Healthcare & Medical", "Healthcare & Medical", "managerial", "Master's degree"),

    # --- Education & Training ---
    ("Primary School Teacher", "", "Education & Training", "Education & Training", "educational", "Intermediate (Grade 12)"),
    ("Secondary School Teacher", "", "Education & Training", "Education & Training", "educational", "Bachelor's degree"),
    ("College / University Lecturer", "", "Education & Training", "Education & Training", "educational", "Master's degree"),
    ("University Professor", "", "Education & Training", "Education & Training", "educational", "Doctorate / PhD"),
    ("Madrasa Teacher", "", "Education & Training", "Education & Training", "educational", "No formal education"),
    ("Tuition / Coaching Teacher", "", "Education & Training", "Education & Training", "educational", "Bachelor's degree"),
    ("Montessori / Early Childhood Teacher", "", "Education & Training", "Education & Training", "educational", "Diploma / Certificate"),
    ("Special Education Teacher", "", "Education & Training", "Education & Training", "educational", "Bachelor's degree"),
    ("Vocational Trainer", "", "Education & Training", "Education & Training", "educational", "Diploma / Certificate"),
    ("School Principal / Administrator", "", "Education & Training", "Education & Training", "managerial", "Master's degree"),
    ("Research Assistant", "", "Education & Training", "Education & Training", "educational", "Master's degree"),
    ("Librarian", "", "Education & Training", "Education & Training", "clerical", "Bachelor's degree"),

    # --- Banking & Finance ---
    ("Bank Teller / Officer", "", "Banking & Finance", "Banking & Finance", "clerical", "Bachelor's degree"),
    ("Accountant", "", "Banking & Finance", "Banking & Finance", "professional", "Bachelor's degree"),
    ("Chartered Accountant (CA)", "", "Banking & Finance", "Banking & Finance", "professional", "Professional degree (MBBS, LLB, CA, etc.)"),
    ("Financial Analyst", "", "Banking & Finance", "Banking & Finance", "professional", "Bachelor's degree"),
    ("Insurance Agent", "", "Banking & Finance", "Banking & Finance", "services", "Intermediate (Grade 12)"),
    ("Microfinance Officer", "", "Banking & Finance", "Banking & Finance", "clerical", "Bachelor's degree"),
    ("Tax Consultant", "", "Banking & Finance", "Banking & Finance", "professional", "Bachelor's degree"),
    ("Auditor", "", "Banking & Finance", "Banking & Finance", "professional", "Bachelor's degree"),
    ("Stock Broker / Trader", "", "Banking & Finance", "Banking & Finance", "professional", "Bachelor's degree"),
    ("Loan Officer", "", "Banking & Finance", "Banking & Finance", "clerical", "Bachelor's degree"),
    ("Investment Banker", "", "Banking & Finance", "Banking & Finance", "professional", "Master's degree"),

    # --- Government & Public Administration ---
    ("Civil Servant (BPS Officer)", "", "Government & Public Administration", "Government & Public Administration", "professional", "Bachelor's degree"),
    ("Police Officer", "", "Government & Public Administration", "Government & Public Administration", "services", "Intermediate (Grade 12)"),
    ("Police Constable", "", "Government & Public Administration", "Government & Public Administration", "services", "Matric (Grade 10)"),
    ("Military Officer", "", "Government & Public Administration", "Defence & Security", "professional", "Bachelor's degree"),
    ("Soldier / Jawan", "", "Government & Public Administration", "Defence & Security", "services", "Matric (Grade 10)"),
    ("Patwari / Revenue Officer", "", "Government & Public Administration", "Government & Public Administration", "clerical", "Intermediate (Grade 12)"),
    ("Postman", "", "Government & Public Administration", "Government & Public Administration", "services", "Matric (Grade 10)"),
    ("Court Clerk / Stenographer", "", "Government & Public Administration", "Government & Public Administration", "clerical", "Intermediate (Grade 12)"),
    ("Railway Worker", "", "Government & Public Administration", "Transport & Logistics", "skilled-labor", "Middle (Grade 8)"),
    ("WAPDA / Electricity Line Worker", "", "Government & Public Administration", "Energy & Utilities", "skilled-labor", "Middle (Grade 8)"),
    ("Sui Gas Technician", "", "Government & Public Administration", "Energy & Utilities", "technical", "Diploma / Certificate"),

    # --- Retail & Wholesale Trade ---
    ("Shopkeeper / Small Retailer", "", "Retail & Wholesale Trade", "Retail & Wholesale Trade", "services", "Primary (Grade 5)"),
    ("Wholesale Dealer", "", "Retail & Wholesale Trade", "Retail & Wholesale Trade", "services", "Middle (Grade 8)"),
    ("Sales Representative", "", "Retail & Wholesale Trade", "Retail & Wholesale Trade", "services", "Intermediate (Grade 12)"),
    ("Cashier", "", "Retail & Wholesale Trade", "Retail & Wholesale Trade", "clerical", "Matric (Grade 10)"),
    ("Supermarket Worker", "", "Retail & Wholesale Trade", "Retail & Wholesale Trade", "services", "Matric (Grade 10)"),
    ("Street Vendor / Hawker", "", "Retail & Wholesale Trade", "Retail & Wholesale Trade", "unskilled-labor", "No formal education"),
    ("Procurement Officer", "", "Retail & Wholesale Trade", "Retail & Wholesale Trade", "clerical", "Bachelor's degree"),

    # --- Hospitality & Tourism ---
    ("Hotel Receptionist", "", "Hospitality & Tourism", "Hospitality & Tourism", "services", "Intermediate (Grade 12)"),
    ("Chef / Cook", "", "Hospitality & Tourism", "Hospitality & Tourism", "trades", "Middle (Grade 8)"),
    ("Waiter", "", "Hospitality & Tourism", "Hospitality & Tourism", "services", "Middle (Grade 8)"),
    ("Tour Guide", "", "Hospitality & Tourism", "Hospitality & Tourism", "services", "Bachelor's degree"),
    ("Hotel Manager", "", "Hospitality & Tourism", "Hospitality & Tourism", "managerial", "Bachelor's degree"),
    ("Housekeeper (Hotel)", "", "Hospitality & Tourism", "Hospitality & Tourism", "services", "Primary (Grade 5)"),
    ("Travel Agent", "", "Hospitality & Tourism", "Hospitality & Tourism", "services", "Intermediate (Grade 12)"),
    ("Fast Food Worker", "", "Hospitality & Tourism", "Hospitality & Tourism", "services", "Matric (Grade 10)"),
    ("Baker", "", "Hospitality & Tourism", "Hospitality & Tourism", "trades", "Middle (Grade 8)"),

    # --- Media & Communications ---
    ("Journalist / Reporter", "", "Media & Communications", "Media & Communications", "professional", "Bachelor's degree"),
    ("TV Anchor / News Presenter", "", "Media & Communications", "Media & Communications", "professional", "Bachelor's degree"),
    ("Cameraman / Videographer", "", "Media & Communications", "Media & Communications", "technical", "Diploma / Certificate"),
    ("Radio Host / RJ", "", "Media & Communications", "Media & Communications", "professional", "Bachelor's degree"),
    ("Content Writer / Copywriter", "", "Media & Communications", "Media & Communications", "professional", "Bachelor's degree"),
    ("Social Media Manager", "", "Media & Communications", "Media & Communications", "technical", "Bachelor's degree"),
    ("Video Editor", "", "Media & Communications", "Media & Communications", "technical", "Diploma / Certificate"),
    ("Translator / Interpreter", "", "Media & Communications", "Media & Communications", "professional", "Bachelor's degree"),
    ("Public Relations Officer", "", "Media & Communications", "Media & Communications", "professional", "Bachelor's degree"),
    ("Advertising Executive", "", "Media & Communications", "Media & Communications", "professional", "Bachelor's degree"),

    # --- Legal & Judiciary ---
    ("Lawyer / Advocate", "", "Legal & Judiciary", "Legal & Judiciary", "professional", "Professional degree (MBBS, LLB, CA, etc.)"),
    ("Judge", "", "Legal & Judiciary", "Legal & Judiciary", "professional", "Professional degree (MBBS, LLB, CA, etc.)"),
    ("Legal Advisor / Corporate Counsel", "", "Legal & Judiciary", "Legal & Judiciary", "professional", "Professional degree (MBBS, LLB, CA, etc.)"),
    ("Paralegal / Legal Assistant", "", "Legal & Judiciary", "Legal & Judiciary", "clerical", "Bachelor's degree"),
    ("Notary Public / Deed Writer", "", "Legal & Judiciary", "Legal & Judiciary", "services", "Intermediate (Grade 12)"),

    # --- Energy & Utilities ---
    ("Power Plant Operator", "", "Energy & Utilities", "Energy & Utilities", "technical", "Diploma / Certificate"),
    ("Solar Panel Installer / Technician", "", "Energy & Utilities", "Energy & Utilities", "trades", "Middle (Grade 8)"),
    ("Petroleum Engineer", "", "Energy & Utilities", "Energy & Utilities", "professional", "Bachelor's degree"),
    ("Gas Fitter", "", "Energy & Utilities", "Energy & Utilities", "trades", "Middle (Grade 8)"),
    ("Meter Reader", "", "Energy & Utilities", "Energy & Utilities", "services", "Matric (Grade 10)"),

    # --- Mining & Quarrying ---
    ("Coal Miner", "", "Mining & Quarrying", "Mining & Quarrying", "skilled-labor", "No formal education"),
    ("Quarry Worker", "", "Mining & Quarrying", "Mining & Quarrying", "unskilled-labor", "No formal education"),
    ("Mining Engineer", "", "Mining & Quarrying", "Mining & Quarrying", "professional", "Bachelor's degree"),
    ("Gem Cutter / Stone Worker", "", "Mining & Quarrying", "Mining & Quarrying", "trades", "Primary (Grade 5)"),

    # --- Domestic & Personal Services ---
    ("Domestic Worker / Maid", "", "Domestic & Personal Services", "Domestic & Personal Services", "unskilled-labor", "No formal education"),
    ("Guard / Chowkidar", "", "Domestic & Personal Services", "Domestic & Personal Services", "services", "Primary (Grade 5)"),
    ("Security Guard (Private)", "", "Domestic & Personal Services", "Domestic & Personal Services", "services", "Middle (Grade 8)"),
    ("Driver (Private)", "", "Domestic & Personal Services", "Domestic & Personal Services", "services", "Middle (Grade 8)"),
    ("Barber / Hairdresser", "", "Domestic & Personal Services", "Domestic & Personal Services", "trades", "Primary (Grade 5)"),
    ("Beautician", "", "Domestic & Personal Services", "Domestic & Personal Services", "trades", "Middle (Grade 8)"),
    ("Laundry Worker / Dhobi", "", "Domestic & Personal Services", "Domestic & Personal Services", "services", "No formal education"),
    ("Sweeper / Sanitary Worker", "", "Domestic & Personal Services", "Domestic & Personal Services", "unskilled-labor", "No formal education"),
    ("Gardener / Mali", "", "Domestic & Personal Services", "Domestic & Personal Services", "services", "No formal education"),
    ("Nanny / Childcare Worker", "", "Domestic & Personal Services", "Domestic & Personal Services", "services", "Primary (Grade 5)"),
    ("Cobbler / Shoe Repairer", "", "Domestic & Personal Services", "Domestic & Personal Services", "trades", "No formal education"),
    ("Tailor (Local)", "", "Domestic & Personal Services", "Domestic & Personal Services", "trades", "Primary (Grade 5)"),
    ("Mechanic (Auto)", "", "Domestic & Personal Services", "Domestic & Personal Services", "trades", "Middle (Grade 8)"),
    ("Mechanic (Motorcycle)", "", "Domestic & Personal Services", "Domestic & Personal Services", "trades", "Primary (Grade 5)"),
    ("AC / Refrigeration Technician", "", "Domestic & Personal Services", "Domestic & Personal Services", "trades", "Middle (Grade 8)"),
    ("Mobile Phone Repair Technician", "", "Domestic & Personal Services", "Domestic & Personal Services", "trades", "Middle (Grade 8)"),

    # --- Arts, Culture & Sports ---
    ("Musician / Singer", "", "Arts, Culture & Sports", "Arts, Culture & Sports", "professional", "No formal education"),
    ("Actor / Performer", "", "Arts, Culture & Sports", "Arts, Culture & Sports", "professional", "No formal education"),
    ("Painter / Fine Artist", "", "Arts, Culture & Sports", "Arts, Culture & Sports", "professional", "Bachelor's degree"),
    ("Calligrapher", "", "Arts, Culture & Sports", "Arts, Culture & Sports", "trades", "Middle (Grade 8)"),
    ("Cricket Player (Professional)", "", "Arts, Culture & Sports", "Arts, Culture & Sports", "professional", "No formal education"),
    ("Sports Coach", "", "Arts, Culture & Sports", "Arts, Culture & Sports", "professional", "Bachelor's degree"),
    ("Photographer", "", "Arts, Culture & Sports", "Arts, Culture & Sports", "technical", "Diploma / Certificate"),
    ("Event Planner", "", "Arts, Culture & Sports", "Arts, Culture & Sports", "services", "Bachelor's degree"),

    # --- NGO & Development Sector ---
    ("NGO Field Worker", "", "NGO & Development Sector", "NGO & Development Sector", "services", "Intermediate (Grade 12)"),
    ("Social Worker", "", "NGO & Development Sector", "NGO & Development Sector", "professional", "Bachelor's degree"),
    ("Community Health Worker", "", "NGO & Development Sector", "NGO & Development Sector", "services", "Matric (Grade 10)"),
    ("Development Program Manager", "", "NGO & Development Sector", "NGO & Development Sector", "managerial", "Master's degree"),
    ("Monitoring & Evaluation Officer", "", "NGO & Development Sector", "NGO & Development Sector", "professional", "Master's degree"),
    ("Human Rights Activist", "", "NGO & Development Sector", "NGO & Development Sector", "professional", "Bachelor's degree"),

    # --- Cross-cutting / General ---
    ("HR Manager / Officer", "", "General", "Banking & Finance", "managerial", "Bachelor's degree"),
    ("Marketing Manager", "", "General", "Retail & Wholesale Trade", "managerial", "Bachelor's degree"),
    ("Office Clerk / Assistant", "", "General", "Government & Public Administration", "clerical", "Intermediate (Grade 12)"),
    ("Data Entry Operator", "", "General", "Information Technology", "clerical", "Matric (Grade 10)"),
    ("Receptionist", "", "General", "Domestic & Personal Services", "clerical", "Intermediate (Grade 12)"),
    ("Call Center Agent", "", "General", "Information Technology", "services", "Intermediate (Grade 12)"),
    ("Project Manager", "", "General", "Information Technology", "managerial", "Bachelor's degree"),
    ("Supply Chain Manager", "", "General", "Transport & Logistics", "managerial", "Bachelor's degree"),
    ("Import / Export Agent", "", "General", "Retail & Wholesale Trade", "services", "Intermediate (Grade 12)"),
    ("Entrepreneur / Small Business Owner", "", "General", "Retail & Wholesale Trade", "managerial", "No formal education"),
    ("Freelancer (General)", "", "General", "Information Technology", "technical", "Diploma / Certificate"),
]


def discover_occupations(run_id):
    """
    Collect all known Pakistani occupations and append new ones
    to occupations_master.csv.
    """
    ensure_dirs()
    existing_titles = load_existing_occupations()
    new_occupations = []

    print("\n" + "=" * 60)
    print("PHASE 2: OCCUPATION DISCOVERY")
    print("=" * 60)

    # Step 1: Register built-in occupation list
    print(f"\n[1/2] Registering Pakistan occupation taxonomy ({len(PAKISTAN_OCCUPATIONS)} occupations)...")
    skipped = 0
    for title, title_urdu, category, sector, occ_type, education in PAKISTAN_OCCUPATIONS:
        if title.strip().lower() in existing_titles:
            skipped += 1
            continue

        new_occupations.append({
            "title": title,
            "title_urdu": title_urdu,
            "category": category,
            "sector": sector,
            "type": occ_type,
            "education_required": education,
            "source_url": "built-in-psco-taxonomy",
            "date_found": datetime.now().strftime("%Y-%m-%d"),
            "run_id": run_id,
        })
        existing_titles.add(title.strip().lower())

    print(f"  Added: {len(new_occupations)}, Already existed: {skipped}")

    # Step 2: Try to discover more occupations from collected sources
    print(f"\n[2/2] Scanning collected sources for additional occupations...")
    extra = _scan_sources_for_occupations(existing_titles, run_id)
    new_occupations.extend(extra)
    print(f"  Found {len(extra)} additional occupations from sources")

    # Save
    if new_occupations:
        append_to_csv(OCCUPATIONS_MASTER_CSV, OCCUPATIONS_MASTER_COLUMNS, new_occupations)
        print(f"\n>> Appended {len(new_occupations)} new occupations to {OCCUPATIONS_MASTER_CSV}")
    else:
        print(f"\n>> No new occupations found this run.")

    log_run(run_id, "discover_occupations", f"new={len(new_occupations)}")
    return new_occupations


def _scan_sources_for_occupations(existing_titles, run_id):
    """
    Scan collected source pages for occupation-related keywords
    and extract any new occupation titles found.
    """
    if not os.path.exists(SOURCES_CSV):
        return []

    # Occupation-related patterns to look for in page content
    occupation_patterns = [
        r"(?:occupation|profession|job title|designation)[:\s]+([A-Z][a-zA-Z\s/&]+)",
    ]

    new_occs = []
    client = httpx.Client(
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        },
        follow_redirects=True,
    )

    with open(SOURCES_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        sources = [r for r in reader if r.get("format") == "html"]

    # Only scan a limited number to be respectful
    scan_limit = min(10, len(sources))
    for source in sources[:scan_limit]:
        try:
            resp, soup = fetch_page(client, source["url"], timeout=10)
            if not soup:
                continue

            text = soup.get_text()
            for pattern in occupation_patterns:
                matches = re.findall(pattern, text)
                for match in matches:
                    title = match.strip()
                    if (
                        len(title) > 3
                        and len(title) < 60
                        and title.lower() not in existing_titles
                    ):
                        new_occs.append({
                            "title": title,
                            "title_urdu": "",
                            "category": "Discovered",
                            "sector": "Unknown",
                            "type": "unknown",
                            "education_required": "",
                            "source_url": source["url"],
                            "date_found": datetime.now().strftime("%Y-%m-%d"),
                            "run_id": run_id,
                        })
                        existing_titles.add(title.lower())
        except Exception:
            continue
        time.sleep(0.5)

    client.close()
    return new_occs


# =============================================================================
# Main Entry Point
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Pakistan Pipeline: Discover sources and occupations"
    )
    parser.add_argument("--sources-only", action="store_true",
                        help="Only discover sources (skip occupations)")
    parser.add_argument("--occupations-only", action="store_true",
                        help="Only discover occupations (skip sources)")
    parser.add_argument("--verify", action="store_true",
                        help="Verify existing sources are still alive")
    args = parser.parse_args()

    run_id = get_run_id()
    print(f"Pakistan Pipeline Collector")
    print(f"Run ID: {run_id}")
    print(f"Timestamp: {datetime.now().isoformat()}")

    if not args.occupations_only:
        discover_sources(run_id, verify=args.verify)

    if not args.sources_only:
        discover_occupations(run_id)

    # Final summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)

    if os.path.exists(SOURCES_CSV):
        with open(SOURCES_CSV, newline="", encoding="utf-8") as f:
            source_count = sum(1 for _ in csv.DictReader(f))
        print(f"  Total sources in {SOURCES_CSV}: {source_count}")

    if os.path.exists(OCCUPATIONS_MASTER_CSV):
        with open(OCCUPATIONS_MASTER_CSV, newline="", encoding="utf-8") as f:
            occ_count = sum(1 for _ in csv.DictReader(f))
        print(f"  Total occupations in {OCCUPATIONS_MASTER_CSV}: {occ_count}")

    print(f"\nRun again to discover more sources and occupations.")
    print(f"Use --verify to check if existing sources are still alive.")


if __name__ == "__main__":
    main()

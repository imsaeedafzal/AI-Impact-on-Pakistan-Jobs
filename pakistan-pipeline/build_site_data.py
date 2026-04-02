"""
Build site/data.json for Pakistan by merging occupation data with AI exposure scores.

Data Sources (all verified from official publications):

1. PBS Labour Force Survey 2024-25:
   - Total employed: 77.2 million
   - Sector distribution: Agriculture 33.1%, Industry 25.7%, Services 41.2%
   Source: https://www.pbs.gov.pk/labour-force-statistics

2. PBS Labour Force Survey 2020-21 (Table 12.12 in Economic Survey 2024-25):
   - Employment by 21 major industry divisions (percentages)
   Source: Pakistan Economic Survey 2024-25, Chapter 12

3. Pakistan Medical & Dental Council / Pakistan Nursing Council (2024):
   - Registered Doctors: 319,572
   - Registered Dentists: 39,088
   - Registered Nurses: 138,391
   - Midwives: 46,801
   - Lady Health Workers: 29,163
   Source: Pakistan Economic Survey 2024-25, Table 11.3

4. Pakistan Software Export Board / SECP (FY2025 Jul-Mar):
   - IT & ITeS companies: 30,000+
   - IT exports: US$ 2.825 billion
   - Freelancer remittances: US$ 400 million
   - Startups generated 185,000 jobs
   Source: Pakistan Economic Survey 2024-25, Chapter 15

5. AI exposure scores: Model name is read from scores.json metadata

6. PSCO mapping: Based on ISCO-08 standard

Usage:
    python build_site_data.py
"""

import csv
import json
import os
import sys

PIPELINE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(PIPELINE_DIR)
OUTPUT_DIR = os.path.join(PIPELINE_DIR, "output")
SITE_DIR = os.path.join(PROJECT_DIR, "site")

OCCUPATIONS_CSV = os.path.join(OUTPUT_DIR, "occupations.csv")
SCORES_JSON = os.path.join(OUTPUT_DIR, "scores.json")
SITE_DATA_JSON = os.path.join(SITE_DIR, "data.json")

# =============================================================================
# TOTAL EMPLOYED: PBS Labour Force Survey 2024-25
# Total employed persons: 77.2 million
# =============================================================================
TOTAL_EMPLOYED = 77_200_000

# =============================================================================
# EMPLOYMENT BY MAJOR INDUSTRY DIVISION
# Source: PBS LFS 2020-21 (Table 12.12 in Pakistan Economic Survey 2024-25)
# Percentages of total employed (we apply these to 77.2M total from LFS 2024-25)
#
# Note: The industry percentages are from LFS 2020-21 (most recent detailed
# breakdown published). The total employed (77.2M) is from LFS 2024-25.
# This is transparently documented as a limitation.
# =============================================================================
INDUSTRY_EMPLOYMENT_PCT = {
    "agriculture_forestry_fishing": 37.4,
    "mining_quarrying": 0.3,
    "manufacturing": 14.9,
    "electricity_gas": 0.2,
    "water_sewerage": 0.4,
    "construction": 9.5,
    "wholesale_retail_trade": 14.4,
    "transport_storage": 5.8,
    "accommodation_food": 1.9,
    "information_communication": 0.5,
    "financial_insurance": 0.5,
    "real_estate": 0.5,
    "professional_scientific": 0.6,
    "admin_support": 0.8,
    "public_admin_defence": 2.9,
    "education": 3.8,
    "health_social_work": 1.3,
    "arts_entertainment": 0.2,
    "other_services": 2.5,
    "household_activities": 1.6,
    "extraterritorial": 0.0,
}

# Convert to absolute numbers
INDUSTRY_EMPLOYMENT = {
    k: int(TOTAL_EMPLOYED * v / 100)
    for k, v in INDUSTRY_EMPLOYMENT_PCT.items()
}

# =============================================================================
# SECTOR-SPECIFIC VERIFIED DATA
# From Pakistan Economic Survey 2024-25 and sector regulators
# =============================================================================

# Source URLs for Pakistan Economic Survey 2024-25 chapters
SOURCE_URLS = {
    "health": "https://www.finance.gov.pk/survey/chapter_25/11_Health_and_Nutrition.pdf",
    "education": "https://www.finance.gov.pk/survey/chapter_25/10_Education.pdf",
    "it": "https://www.finance.gov.pk/survey/chapter_25/15_Information_Technology.pdf",
    "employment": "https://www.finance.gov.pk/survey/chapter_25/12_Population.pdf",
    "pbs_lfs": "https://www.pbs.gov.pk/labour-force-statistics",
}

# Healthcare workforce (Table 11.3, PMC/PNC data, calendar year 2024)
VERIFIED_DATA = {
    "doctor-mbbs": {"jobs": 319_572, "source": "PMC registered doctors 2024 (Econ Survey Table 11.3)", "source_url": SOURCE_URLS["health"]},
    "dentist": {"jobs": 39_088, "source": "PMC registered dentists 2024 (Econ Survey Table 11.3)", "source_url": SOURCE_URLS["health"]},
    "nurse": {"jobs": 138_391, "source": "PNC registered nurses 2024 (Econ Survey Table 11.3)", "source_url": SOURCE_URLS["health"]},
    "midwife-dai": {"jobs": 46_801, "source": "PNC registered midwives 2024 (Econ Survey Table 11.3)", "source_url": SOURCE_URLS["health"]},
    "lady-health-worker": {"jobs": 29_163, "source": "PBS registered LHWs 2024 (Econ Survey Table 11.3)", "source_url": SOURCE_URLS["health"]},
}

# Education workforce (Table 10.3 & 10.7, Ministry of Education / HEC, FY 2023-24)
EDUCATION_VERIFIED = {
    "primary-school-teacher": {"jobs": 508_290, "source": "Ministry of Education, Primary teachers FY2024 (Econ Survey Table 10.3)", "source_url": SOURCE_URLS["education"]},
    "secondary-school-teacher": {"jobs": 776_300, "source": "Ministry of Education, High school teachers FY2024 (Econ Survey Table 10.3)", "source_url": SOURCE_URLS["education"]},
    "college-university-lecturer": {"jobs": 59_843, "source": "Ministry of Education, Degree college teachers FY2024 (Econ Survey Table 10.3)", "source_url": SOURCE_URLS["education"]},
    "university-professor": {"jobs": 22_334, "source": "HEC, PhD faculty in universities FY2024 (Econ Survey Table 10.7)", "source_url": SOURCE_URLS["education"]},
    "vocational-trainer": {"jobs": 51_077, "source": "Ministry of Education, Technical & vocational teachers FY2024 (Econ Survey Table 10.3)", "source_url": SOURCE_URLS["education"]},
}

# Combined verified data lookup
VERIFIED_DATA = {**VERIFIED_DATA, **EDUCATION_VERIFIED}

# IT sector (Chapter 15, PSEB/SECP data FY2025)
# Total IT employment estimated from: 0.5% of 77.2M = 386,000 (Info & Communication)
# Plus startup jobs: 185,000
# IT companies registered: 30,000+
IT_SECTOR_NOTE = "IT & ITeS: 30,000+ companies (SECP), US$ 2.825B exports FY25 (PSEB)"

# =============================================================================
# OCCUPATION → INDUSTRY MAPPING
# Maps each occupation to the PBS industry division it falls under
# =============================================================================
OCCUPATION_TO_INDUSTRY = {
    # Agriculture & Livestock → agriculture_forestry_fishing
    "farmer-crop": "agriculture_forestry_fishing",
    "livestock-farmer": "agriculture_forestry_fishing",
    "agricultural-laborer": "agriculture_forestry_fishing",
    "tractor-operator": "agriculture_forestry_fishing",
    "irrigation-worker": "agriculture_forestry_fishing",
    "fisherman": "agriculture_forestry_fishing",
    "poultry-farmer": "agriculture_forestry_fishing",
    "dairy-farmer": "agriculture_forestry_fishing",
    "agricultural-extension-officer": "agriculture_forestry_fishing",
    "veterinarian": "agriculture_forestry_fishing",
    "floriculturist-nursery-worker": "agriculture_forestry_fishing",

    # Textiles & Garments → manufacturing
    "textile-mill-worker": "manufacturing",
    "power-loom-operator": "manufacturing",
    "garment-stitcher-tailor": "manufacturing",
    "textile-designer": "manufacturing",
    "quality-inspector-textiles": "manufacturing",
    "dyeing-finishing-operator": "manufacturing",
    "embroidery-worker": "manufacturing",
    "spinning-machine-operator": "manufacturing",

    # Construction → construction
    "mason-bricklayer": "construction",
    "carpenter": "construction",
    "plumber": "construction",
    "electrician": "construction",
    "painter-building": "construction",
    "welder": "construction",
    "steel-fixer-iron-worker": "construction",
    "civil-engineer": "construction",
    "architect": "construction",
    "construction-laborer": "construction",
    "crane-operator": "construction",
    "real-estate-agent": "real_estate",
    "surveyor": "construction",

    # Manufacturing & Industry → manufacturing
    "factory-worker-general": "manufacturing",
    "machine-operator": "manufacturing",
    "production-supervisor": "manufacturing",
    "quality-control-inspector": "manufacturing",
    "industrial-engineer": "manufacturing",
    "mechanical-engineer": "manufacturing",
    "chemical-engineer": "manufacturing",
    "electrical-engineer": "manufacturing",
    "electronics-technician": "manufacturing",
    "cnc-machine-operator": "manufacturing",
    "packaging-worker": "manufacturing",

    # Transport → transport_storage
    "truck-driver": "transport_storage",
    "bus-driver": "transport_storage",
    "rickshaw-driver": "transport_storage",
    "taxi-ride-hail-driver": "transport_storage",
    "delivery-rider": "transport_storage",
    "warehouse-worker": "transport_storage",
    "logistics-coordinator": "transport_storage",
    "customs-clearing-agent": "transport_storage",
    "shipport-worker": "transport_storage",
    "airline-pilot": "transport_storage",
    "train-driver": "transport_storage",
    "air-traffic-controller": "transport_storage",

    # IT → information_communication
    "software-developer": "information_communication",
    "web-developer": "information_communication",
    "mobile-app-developer": "information_communication",
    "data-analyst": "information_communication",
    "data-scientist": "information_communication",
    "database-administrator": "information_communication",
    "network-engineer": "information_communication",
    "cybersecurity-analyst": "information_communication",
    "it-support-technician": "information_communication",
    "system-administrator": "information_communication",
    "uiux-designer": "information_communication",
    "devops-engineer": "information_communication",
    "ai-machine-learning-engineer": "information_communication",
    "cloud-engineer": "information_communication",
    "qa-test-engineer": "information_communication",
    "computer-hardware-technician": "information_communication",
    "seo-digital-marketing-specialist": "information_communication",
    "graphic-designer": "information_communication",
    "freelance-it-worker": "information_communication",
    "e-commerce-specialist": "information_communication",

    # Healthcare → health_social_work
    "doctor-mbbs": "health_social_work",
    "surgeon": "health_social_work",
    "dentist": "health_social_work",
    "nurse": "health_social_work",
    "pharmacist": "health_social_work",
    "lab-technician": "health_social_work",
    "paramedic-emt": "health_social_work",
    "lady-health-worker": "health_social_work",
    "physiotherapist": "health_social_work",
    "radiologist-imaging-technician": "health_social_work",
    "psychologist-counselor": "health_social_work",
    "homeopathic-hakeem-practitioner": "health_social_work",
    "midwife-dai": "health_social_work",
    "optician-optometrist": "health_social_work",
    "medical-transcriptionist": "health_social_work",
    "hospital-administrator": "health_social_work",

    # Education → education
    "primary-school-teacher": "education",
    "secondary-school-teacher": "education",
    "college-university-lecturer": "education",
    "university-professor": "education",
    "madrasa-teacher": "education",
    "tuition-coaching-teacher": "education",
    "montessori-early-childhood-teacher": "education",
    "special-education-teacher": "education",
    "vocational-trainer": "education",
    "school-principal-administrator": "education",
    "research-assistant": "education",
    "librarian": "education",

    # Banking → financial_insurance
    "bank-teller-officer": "financial_insurance",
    "accountant": "financial_insurance",
    "chartered-accountant-ca": "financial_insurance",
    "financial-analyst": "financial_insurance",
    "insurance-agent": "financial_insurance",
    "microfinance-officer": "financial_insurance",
    "tax-consultant": "financial_insurance",
    "auditor": "financial_insurance",
    "stock-broker-trader": "financial_insurance",
    "loan-officer": "financial_insurance",
    "investment-banker": "financial_insurance",

    # Government → public_admin_defence
    "civil-servant-bps-officer": "public_admin_defence",
    "police-officer": "public_admin_defence",
    "police-constable": "public_admin_defence",
    "military-officer": "public_admin_defence",
    "soldier-jawan": "public_admin_defence",
    "patwari-revenue-officer": "public_admin_defence",
    "postman": "public_admin_defence",
    "court-clerk-stenographer": "public_admin_defence",
    "railway-worker": "transport_storage",
    "wapda-electricity-line-worker": "electricity_gas",
    "sui-gas-technician": "electricity_gas",

    # Retail → wholesale_retail_trade
    "shopkeeper-small-retailer": "wholesale_retail_trade",
    "wholesale-dealer": "wholesale_retail_trade",
    "sales-representative": "wholesale_retail_trade",
    "cashier": "wholesale_retail_trade",
    "supermarket-worker": "wholesale_retail_trade",
    "street-vendor-hawker": "wholesale_retail_trade",
    "procurement-officer": "wholesale_retail_trade",

    # Hospitality → accommodation_food
    "hotel-receptionist": "accommodation_food",
    "chef-cook": "accommodation_food",
    "waiter": "accommodation_food",
    "tour-guide": "accommodation_food",
    "hotel-manager": "accommodation_food",
    "housekeeper-hotel": "accommodation_food",
    "travel-agent": "accommodation_food",
    "fast-food-worker": "accommodation_food",
    "baker": "accommodation_food",

    # Media → information_communication
    "journalist-reporter": "information_communication",
    "tv-anchor-news-presenter": "information_communication",
    "cameraman-videographer": "information_communication",
    "radio-host-rj": "information_communication",
    "content-writer-copywriter": "information_communication",
    "social-media-manager": "information_communication",
    "video-editor": "information_communication",
    "translator-interpreter": "information_communication",
    "public-relations-officer": "information_communication",
    "advertising-executive": "information_communication",

    # Legal → professional_scientific
    "lawyer-advocate": "professional_scientific",
    "judge": "public_admin_defence",
    "legal-advisor-corporate-counsel": "professional_scientific",
    "paralegal-legal-assistant": "professional_scientific",
    "notary-public-deed-writer": "professional_scientific",

    # Energy → electricity_gas
    "power-plant-operator": "electricity_gas",
    "solar-panel-installer-technician": "construction",
    "petroleum-engineer": "mining_quarrying",
    "gas-fitter": "construction",
    "meter-reader": "electricity_gas",

    # Mining → mining_quarrying
    "coal-miner": "mining_quarrying",
    "quarry-worker": "mining_quarrying",
    "mining-engineer": "mining_quarrying",
    "gem-cutter-stone-worker": "mining_quarrying",

    # Domestic & Personal → household_activities + other_services
    "domestic-worker-maid": "household_activities",
    "guard-chowkidar": "admin_support",
    "security-guard-private": "admin_support",
    "driver-private": "household_activities",
    "barber-hairdresser": "other_services",
    "beautician": "other_services",
    "laundry-worker-dhobi": "other_services",
    "sweeper-sanitary-worker": "water_sewerage",
    "gardener-mali": "household_activities",
    "nanny-childcare-worker": "household_activities",
    "cobbler-shoe-repairer": "other_services",
    "tailor-local": "other_services",
    "mechanic-auto": "wholesale_retail_trade",  # repair of motor vehicles
    "mechanic-motorcycle": "wholesale_retail_trade",
    "ac-refrigeration-technician": "other_services",
    "mobile-phone-repair-technician": "other_services",

    # Arts → arts_entertainment
    "musician-singer": "arts_entertainment",
    "actor-performer": "arts_entertainment",
    "painter-fine-artist": "arts_entertainment",
    "calligrapher": "arts_entertainment",
    "cricket-player-professional": "arts_entertainment",
    "sports-coach": "arts_entertainment",
    "photographer": "arts_entertainment",
    "event-planner": "arts_entertainment",

    # NGO → other_services
    "ngo-field-worker": "other_services",
    "social-worker": "health_social_work",
    "community-health-worker": "health_social_work",
    "development-program-manager": "other_services",
    "monitoring-evaluation-officer": "other_services",
    "human-rights-activist": "other_services",

    # General / Cross-cutting
    "hr-manager-officer": "admin_support",
    "marketing-manager": "wholesale_retail_trade",
    "office-clerk-assistant": "admin_support",
    "data-entry-operator": "admin_support",
    "receptionist": "admin_support",
    "call-center-agent": "information_communication",
    "project-manager": "professional_scientific",
    "supply-chain-manager": "transport_storage",
    "import-export-agent": "wholesale_retail_trade",
    "entrepreneur-small-business-owner": "wholesale_retail_trade",
    "freelancer-general": "information_communication",
}

# PSCO mapping (kept for tooltip display)
OCCUPATION_TO_PSCO = {
    "farmer-crop": "6", "livestock-farmer": "6", "agricultural-laborer": "9",
    "tractor-operator": "8", "irrigation-worker": "6", "fisherman": "6",
    "poultry-farmer": "6", "dairy-farmer": "6", "agricultural-extension-officer": "2",
    "veterinarian": "2", "floriculturist-nursery-worker": "6",
    "textile-mill-worker": "8", "power-loom-operator": "8",
    "garment-stitcher-tailor": "7", "textile-designer": "2",
    "quality-inspector-textiles": "3", "dyeing-finishing-operator": "8",
    "embroidery-worker": "7", "spinning-machine-operator": "8",
    "mason-bricklayer": "7", "carpenter": "7", "plumber": "7",
    "electrician": "7", "painter-building": "7", "welder": "7",
    "steel-fixer-iron-worker": "7", "civil-engineer": "2", "architect": "2",
    "construction-laborer": "9", "crane-operator": "8", "real-estate-agent": "5",
    "surveyor": "3", "factory-worker-general": "9", "machine-operator": "8",
    "production-supervisor": "1", "quality-control-inspector": "3",
    "industrial-engineer": "2", "mechanical-engineer": "2",
    "chemical-engineer": "2", "electrical-engineer": "2",
    "electronics-technician": "3", "cnc-machine-operator": "8",
    "packaging-worker": "9", "truck-driver": "8", "bus-driver": "8",
    "rickshaw-driver": "8", "taxi-ride-hail-driver": "8",
    "delivery-rider": "9", "warehouse-worker": "9",
    "logistics-coordinator": "4", "customs-clearing-agent": "4",
    "shipport-worker": "9", "airline-pilot": "2", "train-driver": "8",
    "air-traffic-controller": "3", "software-developer": "2",
    "web-developer": "2", "mobile-app-developer": "2", "data-analyst": "2",
    "data-scientist": "2", "database-administrator": "2",
    "network-engineer": "2", "cybersecurity-analyst": "2",
    "it-support-technician": "3", "system-administrator": "3",
    "uiux-designer": "2", "devops-engineer": "2",
    "ai-machine-learning-engineer": "2", "cloud-engineer": "2",
    "qa-test-engineer": "2", "computer-hardware-technician": "3",
    "seo-digital-marketing-specialist": "2", "graphic-designer": "2",
    "freelance-it-worker": "2", "e-commerce-specialist": "2",
    "doctor-mbbs": "2", "surgeon": "2", "dentist": "2", "nurse": "3",
    "pharmacist": "2", "lab-technician": "3", "paramedic-emt": "3",
    "lady-health-worker": "3", "physiotherapist": "2",
    "radiologist-imaging-technician": "3", "psychologist-counselor": "2",
    "homeopathic-hakeem-practitioner": "2", "midwife-dai": "3",
    "optician-optometrist": "2", "medical-transcriptionist": "4",
    "hospital-administrator": "1", "primary-school-teacher": "2",
    "secondary-school-teacher": "2", "college-university-lecturer": "2",
    "university-professor": "2", "madrasa-teacher": "2",
    "tuition-coaching-teacher": "2", "montessori-early-childhood-teacher": "2",
    "special-education-teacher": "2", "vocational-trainer": "2",
    "school-principal-administrator": "1", "research-assistant": "2",
    "librarian": "2", "bank-teller-officer": "4", "accountant": "2",
    "chartered-accountant-ca": "2", "financial-analyst": "2",
    "insurance-agent": "5", "microfinance-officer": "4",
    "tax-consultant": "2", "auditor": "2", "stock-broker-trader": "2",
    "loan-officer": "4", "investment-banker": "2",
    "civil-servant-bps-officer": "1", "police-officer": "5",
    "police-constable": "5", "military-officer": "1", "soldier-jawan": "5",
    "patwari-revenue-officer": "4", "postman": "4",
    "court-clerk-stenographer": "4", "railway-worker": "9",
    "wapda-electricity-line-worker": "7", "sui-gas-technician": "3",
    "shopkeeper-small-retailer": "5", "wholesale-dealer": "5",
    "sales-representative": "5", "cashier": "4", "supermarket-worker": "5",
    "street-vendor-hawker": "5", "procurement-officer": "4",
    "hotel-receptionist": "4", "chef-cook": "5", "waiter": "5",
    "tour-guide": "5", "hotel-manager": "1", "housekeeper-hotel": "9",
    "travel-agent": "4", "fast-food-worker": "5", "baker": "7",
    "journalist-reporter": "2", "tv-anchor-news-presenter": "2",
    "cameraman-videographer": "3", "radio-host-rj": "2",
    "content-writer-copywriter": "2", "social-media-manager": "2",
    "video-editor": "3", "translator-interpreter": "2",
    "public-relations-officer": "2", "advertising-executive": "1",
    "lawyer-advocate": "2", "judge": "2",
    "legal-advisor-corporate-counsel": "2", "paralegal-legal-assistant": "4",
    "notary-public-deed-writer": "4", "power-plant-operator": "8",
    "solar-panel-installer-technician": "7", "petroleum-engineer": "2",
    "gas-fitter": "7", "meter-reader": "4", "coal-miner": "8",
    "quarry-worker": "9", "mining-engineer": "2", "gem-cutter-stone-worker": "7",
    "domestic-worker-maid": "9", "guard-chowkidar": "5",
    "security-guard-private": "5", "driver-private": "8",
    "barber-hairdresser": "5", "beautician": "5",
    "laundry-worker-dhobi": "9", "sweeper-sanitary-worker": "9",
    "gardener-mali": "9", "nanny-childcare-worker": "5",
    "cobbler-shoe-repairer": "7", "tailor-local": "7",
    "mechanic-auto": "7", "mechanic-motorcycle": "7",
    "ac-refrigeration-technician": "7", "mobile-phone-repair-technician": "7",
    "musician-singer": "2", "actor-performer": "2",
    "painter-fine-artist": "2", "calligrapher": "7",
    "cricket-player-professional": "2", "sports-coach": "2",
    "photographer": "3", "event-planner": "5",
    "ngo-field-worker": "5", "social-worker": "2",
    "community-health-worker": "3", "development-program-manager": "1",
    "monitoring-evaluation-officer": "2", "human-rights-activist": "2",
    "hr-manager-officer": "1", "marketing-manager": "1",
    "office-clerk-assistant": "4", "data-entry-operator": "4",
    "receptionist": "4", "call-center-agent": "4",
    "project-manager": "1", "supply-chain-manager": "1",
    "import-export-agent": "5", "entrepreneur-small-business-owner": "1",
    "freelancer-general": "2",
}

PSCO_NAMES = {
    "1": "Managers",
    "2": "Professionals",
    "3": "Technicians and Associate Professionals",
    "4": "Clerical Support Workers",
    "5": "Service and Sales Workers",
    "6": "Skilled Agricultural, Forestry and Fishery Workers",
    "7": "Craft and Related Trades Workers",
    "8": "Plant and Machine Operators and Assemblers",
    "9": "Elementary Occupations",
}

INDUSTRY_NAMES = {
    "agriculture_forestry_fishing": "Agriculture, Forestry & Fishing",
    "mining_quarrying": "Mining & Quarrying",
    "manufacturing": "Manufacturing",
    "electricity_gas": "Electricity & Gas",
    "water_sewerage": "Water & Sewerage",
    "construction": "Construction",
    "wholesale_retail_trade": "Wholesale & Retail Trade",
    "transport_storage": "Transport & Storage",
    "accommodation_food": "Accommodation & Food",
    "information_communication": "Information & Communication",
    "financial_insurance": "Financial & Insurance",
    "real_estate": "Real Estate",
    "professional_scientific": "Professional & Scientific",
    "admin_support": "Admin & Support Services",
    "public_admin_defence": "Public Admin & Defence",
    "education": "Education",
    "health_social_work": "Health & Social Work",
    "arts_entertainment": "Arts & Entertainment",
    "other_services": "Other Services",
    "household_activities": "Household Activities",
}


def main():
    sys.stdout.reconfigure(encoding='utf-8')

    with open(SCORES_JSON, encoding="utf-8") as f:
        scores_raw = json.load(f)

    # Support both metadata format and flat array
    if isinstance(scores_raw, dict) and "scores" in scores_raw:
        scores_metadata = scores_raw.get("metadata", {})
        scores = {s["slug"]: s for s in scores_raw["scores"]}
    else:
        scores_metadata = {}
        scores = {s["slug"]: s for s in scores_raw}

    scoring_model = scores_metadata.get("model", "Unknown")
    scoring_date = scores_metadata.get("scored_at", "Unknown")

    with open(OCCUPATIONS_CSV, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    # Load types and Urdu names from master CSV
    occ_types = {}
    occ_urdu = {}
    master_csv = os.path.join(PIPELINE_DIR, "data", "occupations_master.csv")
    if os.path.exists(master_csv):
        with open(master_csv, encoding="utf-8") as f:
            for row_m in csv.DictReader(f):
                slug_key = row_m.get("title", "").lower().strip()
                occ_types[slug_key] = row_m.get("type", "")
                occ_urdu[slug_key] = row_m.get("title_urdu", "")

    # Also load from occupations.json for slug-based lookup
    occ_types_by_slug = {}
    occ_json_path = os.path.join(OUTPUT_DIR, "occupations.json")
    if os.path.exists(occ_json_path):
        with open(occ_json_path, encoding="utf-8") as f:
            for o in json.load(f):
                occ_types_by_slug[o["slug"]] = o.get("type", "")

    # PBS LFS PSCO major group employment (from LFS 2020-21, Table 12.9 context)
    # Used as WEIGHTS within each industry to proportionally size occupations
    PSCO_WEIGHT = {
        "1": 2_670_000,   # Managers
        "2": 4_540_000,   # Professionals
        "3": 2_140_000,   # Technicians
        "4": 1_290_000,   # Clerks
        "5": 8_860_000,   # Service & Sales
        "6": 19_920_000,  # Agriculture
        "7": 12_560_000,  # Craft & Trades
        "8": 6_170_000,   # Operators
        "9": 13_610_000,  # Elementary
    }

    # Step 1: Group occupations by (industry, psco) for proportional weighting
    # For each industry, we distribute its total employment proportionally
    # based on PSCO weights of the occupations within that industry.
    industry_psco_occs = {}  # {industry: {psco: [slugs]}}
    for row in rows:
        slug = row["slug"]
        ind = OCCUPATION_TO_INDUSTRY.get(slug)
        psco = OCCUPATION_TO_PSCO.get(slug, "")
        if ind:
            if ind not in industry_psco_occs:
                industry_psco_occs[ind] = {}
            if psco not in industry_psco_occs[ind]:
                industry_psco_occs[ind][psco] = []
            industry_psco_occs[ind][psco].append(slug)

    # Step 2: Calculate per-occupation job share using PSCO weighting
    occ_jobs = {}
    occ_sources = {}
    for ind, psco_map in industry_psco_occs.items():
        ind_total = INDUSTRY_EMPLOYMENT.get(ind, 0)

        # Calculate total PSCO weight for this industry
        total_weight = 0
        for psco, slugs in psco_map.items():
            w = PSCO_WEIGHT.get(psco, 1_000_000)
            total_weight += w * len(slugs)

        if total_weight == 0:
            total_weight = 1

        # Distribute proportionally
        for psco, slugs in psco_map.items():
            w = PSCO_WEIGHT.get(psco, 1_000_000)
            # Each occupation in this PSCO group gets: (psco_weight / total_weight) * industry_total
            per_occ = int(ind_total * w / total_weight)
            for slug in slugs:
                occ_jobs[slug] = per_occ
                occ_sources[slug] = (
                    f"PBS LFS: {INDUSTRY_NAMES.get(ind, ind)} "
                    f"({INDUSTRY_EMPLOYMENT_PCT.get(ind, 0)}% = {ind_total:,} jobs), "
                    f"weighted by PSCO {psco} ({PSCO_NAMES.get(psco, '')})"
                )

    # Build data
    data = []
    unmapped = []
    for row in rows:
        slug = row["slug"]
        score = scores.get(slug, {})
        industry = OCCUPATION_TO_INDUSTRY.get(slug)
        psco = OCCUPATION_TO_PSCO.get(slug, "")

        if not industry:
            unmapped.append(slug)
            continue

        # Priority: 1) Verified per-occupation 2) PSCO-weighted industry share
        verified = VERIFIED_DATA.get(slug)
        if verified:
            jobs = verified["jobs"]
            data_source = verified["source"]
            source_url = verified.get("source_url", SOURCE_URLS["employment"])
            is_verified = True
        else:
            jobs = occ_jobs.get(slug, 0)
            data_source = occ_sources.get(slug, "Unknown")
            source_url = SOURCE_URLS["pbs_lfs"]
            is_verified = False

        title_key = row["title"].lower().strip()
        data.append({
            "title": row["title"],
            "title_urdu": occ_urdu.get(title_key, ""),
            "slug": slug,
            "category": row["category"],
            "type": occ_types.get(title_key, "") or occ_types_by_slug.get(slug, ""),
            "industry": industry,
            "industry_name": INDUSTRY_NAMES.get(industry, industry),
            "industry_employment": INDUSTRY_EMPLOYMENT.get(industry, 0),
            "psco_group": psco,
            "psco_group_name": PSCO_NAMES.get(psco, ""),
            "jobs": jobs,
            "data_source": data_source,
            "source_url": source_url,
            "is_verified": is_verified,
            "education": row.get("entry_education", ""),
            "exposure": score.get("exposure"),
            "exposure_rationale": score.get("rationale"),
        })

    # Write data.json with metadata so the frontend knows which model scored
    site_output = {
        "metadata": {
            "scoring_model": scoring_model,
            "scoring_date": scoring_date,
            "total_employed": TOTAL_EMPLOYED,
            "total_occupations": len(data),
            "data_sources": [
                "PBS Labour Force Survey 2024-25",
                "Pakistan Economic Survey 2024-25",
                "Pakistan Medical & Dental Council (2024)",
                "Pakistan Nursing Council (2024)",
                "PSEB / SECP (FY2025)",
            ],
            "built_at": __import__("datetime").datetime.now().strftime("%Y-%m-%d"),
        },
        "occupations": data,
    }

    os.makedirs(SITE_DIR, exist_ok=True)
    with open(SITE_DATA_JSON, "w", encoding="utf-8") as f:
        json.dump(site_output, f)

    # Summary
    print(f"Wrote {len(data)} occupations to {SITE_DATA_JSON}")
    if unmapped:
        print(f"WARNING: {len(unmapped)} unmapped: {unmapped}")

    total_jobs = sum(d["jobs"] for d in data)
    print(f"Total jobs represented: {total_jobs:,}")

    verified_count = sum(1 for d in data if d["slug"] in VERIFIED_DATA)
    print(f"Per-occupation verified data: {verified_count} occupations (healthcare)")

    avg_exp = sum(d["exposure"] for d in data if d["exposure"] is not None) / len([d for d in data if d["exposure"] is not None])
    print(f"Average AI exposure: {avg_exp:.1f}")

    print(f"\nBy PBS industry division:")
    ind_stats = {}
    for d in data:
        ind = d["industry"]
        if ind not in ind_stats:
            ind_stats[ind] = {"count": 0, "jobs": 0, "official": d["industry_employment"]}
        ind_stats[ind]["count"] += 1
        ind_stats[ind]["jobs"] += d["jobs"]

    for ind in sorted(ind_stats, key=lambda x: -ind_stats[x]["official"]):
        s = ind_stats[ind]
        name = INDUSTRY_NAMES.get(ind, ind)
        print(f"  {name}: {s['official']:>12,} (PBS) → {s['count']} occupations = {s['jobs']:>12,} allocated")


if __name__ == "__main__":
    main()

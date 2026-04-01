"""
Pakistan Pipeline Configuration.

Central configuration for source discovery, occupation collection,
and data processing for Pakistan's labor market.
"""

# -- Country Settings ----------------------------------------------------------
COUNTRY = "Pakistan"
CURRENCY = "PKR"
CURRENCY_SYMBOL = "Rs."

# -- Education Tiers (Pakistan system) ----------------------------------------
EDUCATION_TIERS = [
    "No formal education",
    "Primary (Grade 5)",
    "Middle (Grade 8)",
    "Matric (Grade 10)",
    "Intermediate (Grade 12)",
    "Diploma / Certificate",
    "Bachelor's degree",
    "Master's degree",
    "M.Phil / MS",
    "Doctorate / PhD",
    "Professional degree (MBBS, LLB, CA, etc.)",
]

# -- Occupation Sectors (Pakistan economy) ------------------------------------
OCCUPATION_SECTORS = [
    "Agriculture & Livestock",
    "Textiles & Garments",
    "Construction & Real Estate",
    "Manufacturing & Industry",
    "Transport & Logistics",
    "Information Technology",
    "Healthcare & Medical",
    "Education & Training",
    "Banking & Finance",
    "Government & Public Administration",
    "Retail & Wholesale Trade",
    "Hospitality & Tourism",
    "Media & Communications",
    "Legal & Judiciary",
    "Energy & Utilities",
    "Mining & Quarrying",
    "Defence & Security",
    "Domestic & Personal Services",
    "Arts, Culture & Sports",
    "NGO & Development Sector",
]

# -- Occupation Types ---------------------------------------------------------
OCCUPATION_TYPES = [
    "technical",
    "non-technical",
    "skilled-labor",
    "unskilled-labor",
    "professional",
    "managerial",
    "clerical",
    "educational",
    "medical",
    "agricultural",
    "trades",
    "services",
]

# -- Source Reliability Levels ------------------------------------------------
RELIABILITY_LEVELS = {
    "high": "Government body, international organization, or central bank",
    "medium": "Established private institution, job portal, or industry body",
    "low": "Blog, news article, or unverified source",
}

# -- Source Categories --------------------------------------------------------
SOURCE_CATEGORIES = [
    "government",
    "international-organization",
    "central-bank",
    "regulatory-body",
    "academic-institution",
    "job-portal",
    "industry-body",
    "research-organization",
    "news-media",
    "private-sector",
]

# -- Known Government & Institutional Domains ---------------------------------
# These are the seed domains the collector will crawl for Pakistan labor data.
SEED_SOURCES = [
    # --- Government (Federal) ---
    {
        "url": "https://www.pbs.gov.pk",
        "title": "Pakistan Bureau of Statistics",
        "category": "government",
        "subcategory": "labour-statistics",
        "reliability": "high",
        "description": "Official statistics body. Publishes Labour Force Survey, PSLM, Census data.",
    },
    {
        "url": "https://www.finance.gov.pk",
        "title": "Ministry of Finance - Pakistan",
        "category": "government",
        "subcategory": "economic-data",
        "reliability": "high",
        "description": "Annual Economic Survey with employment, wages, and sector-wise data.",
    },
    {
        "url": "https://www.sbp.org.pk",
        "title": "State Bank of Pakistan",
        "category": "central-bank",
        "subcategory": "wages-economic",
        "reliability": "high",
        "description": "Monetary policy reports, wage data, economic indicators.",
    },
    {
        "url": "https://www.pc.gov.pk",
        "title": "Planning Commission of Pakistan",
        "category": "government",
        "subcategory": "development-planning",
        "reliability": "high",
        "description": "Five-year plans, PSDP, employment projections.",
    },
    {
        "url": "https://www.moip.gov.pk",
        "title": "Ministry of Industries and Production",
        "category": "government",
        "subcategory": "industry-employment",
        "reliability": "high",
        "description": "Industrial sector employment and production data.",
    },
    {
        "url": "https://www.navttc.gov.pk",
        "title": "NAVTTC - National Vocational & Technical Training Commission",
        "category": "government",
        "subcategory": "skills-training",
        "reliability": "high",
        "description": "Vocational training, skills classification, trades data.",
    },
    {
        "url": "https://www.hec.gov.pk",
        "title": "Higher Education Commission",
        "category": "government",
        "subcategory": "education-employment",
        "reliability": "high",
        "description": "Graduate employment data, education statistics.",
    },
    {
        "url": "https://www.mohr.gov.pk",
        "title": "Ministry of Human Rights",
        "category": "government",
        "subcategory": "labour-rights",
        "reliability": "high",
        "description": "Labour laws, worker rights, employment conditions.",
    },
    {
        "url": "https://www.ophrd.gov.pk",
        "title": "Ministry of Overseas Pakistanis & HRD",
        "category": "government",
        "subcategory": "overseas-employment",
        "reliability": "high",
        "description": "Overseas employment data, remittances, migrant worker stats.",
    },
    {
        "url": "https://www.eobi.gov.pk",
        "title": "Employees Old-Age Benefits Institution",
        "category": "government",
        "subcategory": "employment-registration",
        "reliability": "high",
        "description": "Registered employers/employees data.",
    },

    # --- Provincial ---
    {
        "url": "https://bos.gop.pk",
        "title": "Bureau of Statistics - Punjab",
        "category": "government",
        "subcategory": "provincial-statistics",
        "reliability": "high",
        "description": "Punjab province employment and economic statistics.",
    },
    {
        "url": "https://sindh.gov.pk/bos",
        "title": "Bureau of Statistics - Sindh",
        "category": "government",
        "subcategory": "provincial-statistics",
        "reliability": "high",
        "description": "Sindh province employment and economic statistics.",
    },

    # --- International Organizations ---
    {
        "url": "https://ilostat.ilo.org/data/country/PAK",
        "title": "ILO ILOSTAT - Pakistan",
        "category": "international-organization",
        "subcategory": "labour-statistics",
        "reliability": "high",
        "description": "International Labour Organization Pakistan data. Employment by occupation, wages, working conditions.",
    },
    {
        "url": "https://data.worldbank.org/country/pakistan",
        "title": "World Bank - Pakistan",
        "category": "international-organization",
        "subcategory": "economic-indicators",
        "reliability": "high",
        "description": "GDP, employment ratio, labor force participation, sectoral data.",
    },
    {
        "url": "https://www.adb.org/countries/pakistan/main",
        "title": "Asian Development Bank - Pakistan",
        "category": "international-organization",
        "subcategory": "economic-development",
        "reliability": "high",
        "description": "Economic reports, employment projections, sector analysis.",
    },
    {
        "url": "https://data.undp.org",
        "title": "UNDP Data - Pakistan",
        "category": "international-organization",
        "subcategory": "human-development",
        "reliability": "high",
        "description": "Human Development Index, education-employment linkages.",
    },

    # --- Job Portals & Private Sector ---
    {
        "url": "https://www.rozee.pk",
        "title": "Rozee.pk",
        "category": "job-portal",
        "subcategory": "job-listings",
        "reliability": "medium",
        "description": "Largest Pakistan job portal. Salary surveys, occupation listings.",
    },
    {
        "url": "https://www.mustakbil.com",
        "title": "Mustakbil.com",
        "category": "job-portal",
        "subcategory": "job-listings",
        "reliability": "medium",
        "description": "Job portal with salary and occupation data.",
    },
    {
        "url": "https://pk.indeed.com",
        "title": "Indeed Pakistan",
        "category": "job-portal",
        "subcategory": "job-listings",
        "reliability": "medium",
        "description": "International job portal with Pakistan salary and job data.",
    },

    # --- Research & Industry ---
    {
        "url": "https://www.pide.org.pk",
        "title": "Pakistan Institute of Development Economics",
        "category": "research-organization",
        "subcategory": "economic-research",
        "reliability": "high",
        "description": "Labour market research, employment studies, economic analysis.",
    },
    {
        "url": "https://www.fpcci.org.pk",
        "title": "Federation of Pakistan Chambers of Commerce & Industry",
        "category": "industry-body",
        "subcategory": "industry-data",
        "reliability": "medium",
        "description": "Industry employment data, trade statistics.",
    },
    {
        "url": "https://www.pseb.org.pk",
        "title": "Pakistan Software Export Board",
        "category": "government",
        "subcategory": "it-sector",
        "reliability": "high",
        "description": "IT industry employment, software exports, tech workforce data.",
    },
    {
        "url": "https://www.psx.com.pk",
        "title": "Pakistan Stock Exchange",
        "category": "private-sector",
        "subcategory": "corporate-data",
        "reliability": "medium",
        "description": "Listed company employment data, sector-wise corporate stats.",
    },
]

# -- Search Keywords for Discovering New Sources ------------------------------
SEARCH_KEYWORDS = [
    "Pakistan labour force survey",
    "Pakistan employment statistics",
    "Pakistan occupation classification",
    "Pakistan salary survey",
    "Pakistan workforce data",
    "Pakistan Bureau of Statistics employment",
    "PSCO Pakistan Standard Classification of Occupations",
    "Pakistan economic survey employment chapter",
    "Pakistan wages by occupation",
    "Pakistan job market report",
    "Pakistan sectoral employment",
    "Pakistan skills gap analysis",
    "Pakistan vocational training occupations",
    "Pakistan IT workforce statistics",
    "Pakistan agriculture employment data",
    "Pakistan textile industry employment",
    "Pakistan construction sector jobs",
    "Pakistan healthcare workforce",
    "Pakistan education sector employment",
]

# -- File Paths ---------------------------------------------------------------
DATA_DIR = "data"
OUTPUT_DIR = "output"
RAW_DIR = "output/raw"
SOURCES_CSV = "data/sources.csv"
OCCUPATIONS_MASTER_CSV = "data/occupations_master.csv"
SOURCE_LOGS_CSV = "data/source_logs.csv"

# -- CSV Column Definitions ---------------------------------------------------
SOURCES_COLUMNS = [
    "url", "title", "domain", "category", "subcategory",
    "reliability", "format", "description",
    "date_found", "status", "run_id",
]

OCCUPATIONS_MASTER_COLUMNS = [
    "title", "title_urdu", "category", "sector", "type",
    "education_required", "source_url", "date_found", "run_id",
]

OUTPUT_OCCUPATIONS_COLUMNS = [
    "title", "slug", "category", "sector", "type",
    "median_pay_annual", "median_pay_monthly",
    "entry_education", "num_jobs", "outlook_desc",
    "source_urls",
]

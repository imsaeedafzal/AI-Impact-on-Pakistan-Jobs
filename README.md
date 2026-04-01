# AI Exposure of Pakistan's Job Market

Interactive treemap visualization analyzing how AI will reshape **206 Pakistani occupations** covering **77.2 million jobs**.

## What This Shows

Each rectangle represents a Pakistani occupation. **Size** reflects estimated employment (from PBS data, PSCO-weighted). **Color** reflects AI exposure: green = low exposure (physical/manual work), red = high exposure (digital/knowledge work).

- **206 occupations** across 20 PBS industry divisions
- **77.2 million jobs** represented (PBS Labour Force Survey 2024-25)
- **Average AI exposure: 3.8/10** (Pakistan's economy is heavily physical/agricultural)
- **Most exposed:** Data Entry Operator (10), Medical Transcriptionist (10), Content Writer (9)
- **Least exposed:** Rickshaw Driver (1), Barber (1), Agricultural Laborer (1)

## Data Sources

All employment data comes from official Pakistani government publications:

| Source | Data | Reference |
|--------|------|-----------|
| **PBS Labour Force Survey 2024-25** | Total employed: 77.2M, sector distribution | [pbs.gov.pk](https://www.pbs.gov.pk/labour-force-statistics) |
| **Pakistan Economic Survey 2024-25** | Employment by 21 industry divisions (Table 12.12) | [finance.gov.pk](https://www.finance.gov.pk/survey_2025.html) |
| **Pakistan Medical & Dental Council** | 319,572 registered doctors, 39,088 dentists (2024) | Econ Survey Table 11.3 |
| **Pakistan Nursing Council** | 138,391 nurses, 46,801 midwives, 29,163 LHWs (2024) | Econ Survey Table 11.3 |
| **PSEB / SECP** | 30,000+ IT companies, $2.825B IT exports (FY25) | Econ Survey Chapter 15 |
| **104 institutional sources** | Collected via automated pipeline | See `pakistan-pipeline/data/sources.csv` |

**AI exposure scores** (0-10) were analyzed by an Anthropic Claude model (see `scores.json` metadata for exact model used) for each occupation individually.

**What is NOT included:** Per-occupation salary data is not shown because no verified official source exists for this in Pakistan.

## Project Structure

```
AI-Impact-on-Pakistan/
├── site/                              # The visualization (static website)
│   ├── index.html                     # Interactive treemap with filters
│   └── data.json                      # 206 occupations with all data
│
├── pakistan-pipeline/                  # Data collection & processing
│   ├── config.py                      # Configuration, seed sources, sectors
│   ├── collect.py                     # Source & occupation discovery
│   ├── process.py                     # Extract data from collected sources
│   ├── build_site_data.py             # Merge all data into site/data.json
│   ├── requirements.txt               # Python dependencies
│   ├── score.py                        # AI exposure scoring algorithm (LLM-based)
│   ├── data/
│   │   ├── sources.csv                # 104 discovered data sources
│   │   └── occupations_master.csv     # 206 Pakistani occupations
│   └── output/
│       ├── scores.json                # AI exposure scores (0-10) + rationales
│       ├── occupations.json           # Occupation list with metadata
│       └── occupations.csv            # Occupation data for processing
│
├── .github/workflows/static.yml       # GitHub Pages deployment
└── README.md
```

## How It Works

### 1. Source Collection
```bash
cd pakistan-pipeline
pip install -r requirements.txt
python collect.py
```
Crawls Pakistani government websites (PBS, Finance Ministry, SBP, etc.) to discover data sources. Appends to `data/sources.csv`. Run multiple times to grow the source list. Existing sources are never deleted.

### 2. Data Processing
```bash
python process.py
```
Fetches data from collected sources with rate limiting (2s global, 5s per-domain). Extracts tables, salary references, and employment data. Caches raw HTML locally so re-runs are instant.

### 3. AI Exposure Scoring
```bash
# Option A: Re-score using an LLM API (requires API key)
echo "OPENROUTER_API_KEY=your_key" > .env
python score.py                          # score all 206 occupations
python score.py --start 0 --end 10       # test on first 10
python score.py --model google/gemini-2.5-flash
python score.py --dry-run                # preview without API calls

# Option B: Score interactively using Claude Code (no API key needed)
# Just ask Claude Code: "Score these occupations for AI exposure"

# Option C: Edit scores manually
# Directly edit output/scores.json with your own scores and rationales
```
The model used is automatically recorded in `scores.json` metadata. The scoring rubric is in `score.py` -- contributors can modify it to improve accuracy, add Pakistan-specific factors, or use different models.

### 4. Build Site Data
```bash
python build_site_data.py
```
Merges occupation data, PBS employment figures, PMC/PNC healthcare registrations, and AI exposure scores into `site/data.json`.

### 5. View Locally
```bash
cd ../site
python -m http.server 8000
# Open http://localhost:8000
```

## Features

- **Interactive treemap** with hover tooltips showing exposure rationale and data sources
- **Filters** by PBS industry, occupation type, education level, exposure range
- **Size toggle** to view by Employment (PBS data), AI Exposure Score, or Equal
- **Sort options** by employment, exposure (high/low), industry, alphabetical
- **Sidebar stats** with job-weighted averages, histogram, breakdown by tier/education/industry
- **Mobile responsive** layout for phones and tablets
- **Full methodology panel** with source attribution for every data point
- **Disclaimer banner** transparent about data limitations

## How to Contribute

Contributions that improve data accuracy are especially welcome:

- **Add more occupations** -- edit `pakistan-pipeline/data/occupations_master.csv`
- **Add verified data sources** -- add to `pakistan-pipeline/config.py` SEED_SOURCES
- **Improve AI exposure scores** -- edit `pakistan-pipeline/output/scores.json` with rationale
- **Integrate new sector data** -- PEC (engineers), PBC (lawyers), HEC (faculty), SBP (banking) registrations are not yet integrated
- **Add per-occupation employment** -- if you find verified data for specific occupations
- **Add Urdu translations** -- `occupations_master.csv` has a `title_urdu` column (currently empty)
- **Fix bugs or improve the visualization** -- `site/index.html` is self-contained vanilla JS

### Data Accuracy Guidelines

- Only add data from **verifiable official sources** (government bodies, regulatory councils, international organizations)
- Always cite the **exact source** (report name, table number, year)
- If data is estimated, **label it as estimated** with methodology
- Never fabricate per-occupation numbers -- show "No data" rather than a guess

## AI Exposure Scale

| Score | Level | Description | Examples |
|-------|-------|-------------|----------|
| 1 | Minimal | Physical/manual work, AI has no impact | Rickshaw Driver, Mason, Agricultural Laborer |
| 2-3 | Low | Mostly physical, AI helps with minor tasks | Electrician, Nurse, Plumber |
| 4-5 | Moderate | Mix of physical and knowledge work | Doctor, Police Officer, Veterinarian |
| 6-7 | High | Predominantly knowledge work | Accountant, Civil Engineer, Journalist |
| 8-9 | Very High | Almost entirely digital work | Software Developer, Graphic Designer, Data Analyst |
| 10 | Maximum | Routine digital processing | Data Entry Operator |

## Limitations

- **Industry percentages** are from PBS LFS 2020-21 (most recent detailed breakdown published); total employed (77.2M) is from LFS 2024-25
- **Per-occupation employment** is estimated using PSCO-weighted distribution within each PBS industry, not actual per-occupation counts
- **~70% of Pakistan's workforce** is in the informal sector; PBS LFS covers both but occupation-level data may skew toward formal employment
- **Per-occupation salary** data is not available from any verified official Pakistani source
- **AI exposure scores** are analytical assessments by an AI model, not empirical measurements

## Credits

- **Author**: Saeed Afzal
- **Data**: Pakistan Bureau of Statistics, Pakistan Economic Survey 2024-25, Pakistan Medical & Dental Council, Pakistan Nursing Council, Pakistan Software Export Board, SECP
- **Analysis**: Anthropic Claude (model recorded in `scores.json` metadata)
- **PSCO**: Pakistan Standard Classification of Occupations (based on ISCO-08)

## License

MIT

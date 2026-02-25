# Philippines Data Extraction Tools

## Overview

Dual-tool platform for extracting and analyzing Philippine disaster response data from NDRRMC (National Disaster Risk Reduction and Management Council) reports and DSWD DROMIC (Disaster Response Operations Monitoring and Information Center) reports.

**Purpose:** Convert 50-600 page disaster response PDFs into structured, analysis-ready datasets with proper administrative geography hierarchy, P-codes, and advanced vulnerability analytics.

**Live Demo:** https://ifrc-philippines-data-convert.streamlit.app/
---

## Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Installation](#installation)
- [File Structure](#file-structure)
- [Tool 1: NDRRMC Sitrep Extractor](#tool-1-ndrrmc-sitrep-extractor)
- [Tool 2: DROMIC Report Extractor](#tool-2-dromic-report-extractor)
- [P-Code Matching](#p-code-matching)
- [Configuration](#configuration)
- [Deployment](#deployment)
- [Known Issues](#known-issues)
- [Contributing](#contributing)

---

## Features

### NDRRMC Sitrep Extractor
- **PDF Extraction**: Tabula-based extraction of 13 table types from NDRRMC situational reports
- **Location Hierarchy**: Automatic parsing of Region → Province → Municipality → Barangay structure
- **Data Transformation**: Standardization, validation, and cleaning with P-code matching
- **Interactive Dashboards**: Summary and detailed analytics with 5 vulnerability assessment modules
- **CSV Export**: Clean, analysis-ready downloads with dynamic filtering

### DROMIC Report Extractor
- **Custom Pattern Matching**: User-defined text patterns for flexible table detection
- **Admin Level Detection**: Counter-based algorithm to identify Region/Province/Municipality hierarchy
- **P-code Integration**: Automatic P-code matching at ADM0-ADM3 levels
- **Single-Table Focus**: Optimized for quick extraction of specific DROMIC tables
- **Direct Download**: Immediate CSV export without dashboard complexity

### Shared Features
- **P-code Matching**: Philippine Standard Geographic Codes (PSGC) at all admin levels
- **PDF Loading**: File upload or direct URL fetching
- **Error Handling**: Graceful degradation for missing data or malformed tables

---

## Architecture
```
┌─────────────────┐
│   Streamlit UI  │
│  (Dual Tools)   │
└────────┬────────┘
         │
    ┌────▼─────────────────────────────────┐
    │                                      │
┌───▼──────────┐              ┌───────────▼────┐
│ NDRRMC Tool  │              │  DROMIC Tool   │
│              │              │                │
│ pdf_extractor│              │dromic_extractor│
│transformations│              │                │
└───┬──────────┘              └───────────┬────┘
    │                                     │
    │        ┌────────────────────────────┘
    │        │
┌───▼────────▼───┐
│  P-code Module │
│   (pcodes.py)  │
└───┬────────────┘
    │
┌───▼──────────────┐
│  Session State   │
│  (In-memory)     │
└──────────────────┘
```

---

## Installation

### Prerequisites
- Python 3.9+
- Java Runtime Environment (JRE) - required for tabula-py
- Git

### Local Setup
```bash
# Clone repository
git clone https://github.com/YOUR-USERNAME/ifrc-philippines-data-convert/.git
cd ifrc-ndrrmc-convert

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run locally
streamlit run streamlit_app.py
```

### Requirements
```txt
streamlit
pdfplumber
pandas
openpyxl
numpy
tabula-py
jpype1
PyPDF2
python-docx
scikit-learn
requests
fuzzywuzzy
python-Levenshtein
unidecode
```

---

## File Structure
```
ifrc-ndrrmc-convert/
├── streamlit_app.py           # Main Streamlit application (dual tools)
├── pdf_extractor.py           # NDRRMC extraction functions
├── transformations.py         # NDRRMC data transformation pipeline
├── dromic_extractor.py        # DROMIC extraction and admin detection
├── pcodes.py                  # P-code matching (shared)
├── config.py                  # Configuration and mappings
├── requirements.txt           # Python dependencies
├── packages.txt               # System dependencies (Java)
├── data/
│   ├── phl_adminareas_fixed.csv    # P-code reference
│   └── hucs_and_pcodes.csv         # Highly Urbanized Cities
├── assets/
│   └── Logo-Horizontal-RGB.svg
└── README.md
```

---

## Tool 1: NDRRMC Sitrep Extractor

### Supported Tables (13 types)
- **Demographics**: Affected Population, Casualties, Damaged Houses
- **Infrastructure**: Roads & Bridges, Power Supply, Water Supply, Communications
- **Sectoral**: Agriculture Damage, Infrastructure Damage
- **Assistance**: Family Assistance, LGU Assistance, Pre-emptive Evacuation
- **Incidents**: Related Incidents (flooding, landslides)

### Extraction Pipeline

**Summary Tables:**
```python
extract_summary_tables(pdf_path)
→ Returns: dict of DataFrames (regional aggregates)
```

**Detailed Tables:**
```python
extract_detailed_tables(
    pdf_path, 
    selected_tables=['AFFECTED POPULATION', ...],
    summaries=summary_dict,
    progress_callback=update_function
)
→ Returns: dict of raw DataFrames (municipality/barangay level)
```

### Transformation Logic

Each table type has dedicated transformation:
```python
transform_affected_population(df)
transform_casualties(df)
transform_damaged_houses(df)
# ... 13 total functions
```

**Standard transformations:**
1. Header standardization
2. Location hierarchy parsing
3. Numeric cleaning
4. Level tagging (Region/Province/Municipality/Barangay)
5. P-code matching
6. Validation

**Special handling:**
- **Metro Manila districts** - Treated as municipalities
- **Highly Urbanized Cities** - Function as both province and municipality
- **Casualties** - PII removal (names redacted)

### Dashboard Analytics

**5 Analytical Modules:**

1. **Assistance Gap Analysis**
```python
   Gap_Score = Families_Requiring_Assistance / Families_Assisted
   Weighted_Gap_Score = Gap_Score × (1 + Displacement_Magnitude/100)
```

2. **Access & Isolation Risk**
```python
   Isolation_Score = Road_Blockage + Still_Flooded + High_Displacement
```

3. **Lifelines Compound Failure**
```python
   Lifelines_Failed = Water_Down + Power_Down + Comms_Down
```

4. **Recovery Progress Tracking**
```python
   Recovery_Rate = Restored / (Interrupted + Restored) × 100%
```

5. **Vulnerability Hotspots**
```python
   Vulnerability_Index = (Displacement_Score × 0.4) + (Housing_Score × 0.3) + (Lifeline_Score × 0.3)
   Weighted_Vulnerability = Vulnerability_Index × (1 + Impact_Magnitude/100)
```

---

## Tool 2: DROMIC Report Extractor

### Methodology

Based on empirically-tested Jupyter notebook workflow optimized for DSWD DROMIC reports.

### Admin Level Detection Algorithm

**Counter-Based Hierarchy Detection:**

1. **Identify numeric column** with all non-zero values (used for validation)
2. **Detect admin levels:**
   - `GRAND TOTAL` → ADM0 (country)
   - UPPERCASE (not GRAND TOTAL) → ADM1 (region)
   - Match HUC list → HUC (Highly Urbanized City)
   - **Counter logic** for ADM2/ADM3:
     - Start with first row as ADM2 (province)
     - Accumulate totals from subsequent rows
     - When accumulated total ≈ ADM2 value → those rows are ADM3 (municipalities)
     - Next row becomes new ADM2

**Example:**
```
REGION V                    [ADM1]
  Albay         500          [ADM2] ← Start counter
    Legazpi     300          [ADM3] ← Counter: 300
    Daraga      200          [ADM3] ← Counter: 500 (matches ADM2, reset)
  Camarines Sur 800          [ADM2] ← New province
    ...
```

### Custom Text Patterns

Users can specify:
- **Page Text Pattern**: Text that must appear on page to extract tables (default: "NO. OF DAMAGED HOUSES")
- **Table Text Pattern**: Comma-separated text that must appear in table header (default: "NO. OF DAMAGED HOUSES, Total")

### P-code Matching

Uses fuzzy string matching with cleaned location names:
1. Match ADM2 (province) first
2. Derive ADM1 (region) from province P-code
3. Match ADM3 (municipality) within province context
4. Forward-fill and back-fill for hierarchy consistency

---

## P-Code Matching

All transformed tables include Philippine Standard Geographic Code (PSGC) P-codes for integration with other datasets.

### P-code Columns

**NDRRMC Tool:**
- `ADM0_EN`, `ADM0_PCODE`: Country level
- `ADM1_EN`, `ADM1_PCODE`: Region level
- `ADM2_EN`, `ADM2_PCODE`: Province level
- `ADM3_EN`, `ADM3_PCODE`: Municipality/City level
- `ADM4_EN`, `ADM4_PCODE`: Barangay level (when available)

**DROMIC Tool:**
- `ADM0_new`: Country level (PH)
- `ADM1_new`: Region level
- `ADM2_new`: Province level
- `ADM3_new`: Municipality/City level

### Matching Algorithm

1. **Name Normalization:**
   - Convert to lowercase
   - Remove diacritics (ñ → n, á → a)
   - Strip "City of", "Municipality of", parentheticals
   - Convert Roman numerals to Arabic (Region III → Region 3)
   - Standardize abbreviations (St. → San, Sta. → Santa)

2. **Fuzzy String Matching:**
   - Uses Levenshtein distance algorithm via fuzzywuzzy
   - 80% similarity threshold
   - Hierarchical matching (province context narrows municipality search)

3. **Special Cases:**
   - **Metro Manila Districts**: Treated as municipalities (ADM3 level)
   - **Highly Urbanized Cities (HUCs)**: Match at both province and municipality levels
   - **Independent Component Cities**: Match at province level
   - **NIR (Negros Island Region)**: Provinces mapped back to original regions (created post-2020 census)

### Match Quality

- **Region (ADM1)**: ~98% match rate
- **Province (ADM2)**: ~95% match rate  
- **Municipality (ADM3)**: ~90% match rate
- **Barangay (ADM4)**: ~85% match rate

**Common reasons for non-match:**
- Non-standard naming in PDF (e.g., "City Proper" instead of municipality name)
- Newly created barangays not yet in P-code reference (post-2023)
- OCR errors in PDF extraction
- Administrative boundary changes

### Reference Data

P-code matching uses `data/phl_adminareas_fixed.csv`:
- 42,046 barangays
- 1,488 municipalities/cities
- 81 provinces
- 17 regions
- Data as of 2023 PSGC

---

## Configuration

### `config.py`

**Key mappings:**
```python
REGION_PROVINCE_MAP = {
    'REGION I': ['ILOCOS NORTE', 'ILOCOS SUR', ...],
    'REGION II': ['BATANES', 'CAGAYAN', ...],
    # ... full Philippine administrative map
}

PROVINCE_ALIASES = {
    'METRO MANILA': 'NCR',
    'NATIONAL CAPITAL REGION': 'NCR',
    # ... alternative names
}
```

**Usage:**
- Validate location parsing (NDRRMC tool)
- Standardize region/province names
- Handle administrative boundary changes

---

## Deployment

### Streamlit Cloud

**Required files:**
- `requirements.txt` - Python packages
- `packages.txt` - System packages (contains: `default-jre` for Java)

**Deployment steps:**
1. Push to GitHub
2. Connect repository at share.streamlit.io
3. Set main file: `streamlit_app.py`
4. Deploy (auto-rebuilds on git push)

**Resource considerations:**
- NDRRMC extraction: 15-20 minutes for 500+ page PDFs
- DROMIC extraction: ~30 seconds typical
- Streamlit Cloud free tier has timeout limits
- Java installation adds ~30 seconds to cold start

### Environment Variables

None required - tools are self-contained.

---

## Known Issues

### Critical
- **Deprecation warnings**: `fillna(method='ffill')` → need to migrate to `.ffill()`
- **Casualties Level column**: Missing in some transformed outputs, causes KeyError in dashboard

### Moderate
- **Font warnings**: PDFBox font substitution warnings (cosmetic, can be suppressed)
- **Extraction failures**: Some very old reports (pre-2020) have incompatible formatting
- **Lifelines data**: Roads/bridges/power/water tables often unpopulated in NDRRMC reports

### Minor
- **NIR province mapping**: Negros Island Region created after 2020 census, manual mapping required
- **Metro Manila barangay counts**: Districts treated as municipalities, may inflate counts
- **DROMIC HUC detection**: Requires `data/hucs_and_pcodes.csv` file; gracefully handles missing file

---

## Development Notes

### Testing Strategy
- Local testing with sample PDFs before deployment
- No automated test suite (manual QA on representative reports)
- User acceptance testing in Philippines field office

### Code Style
- Functional programming approach (transformation functions are pure where possible)
- Minimal OOP (Streamlit session state for UI state management)
- Inline comments for complex location parsing logic

### Performance
- **NDRRMC Bottleneck**: Tabula PDF extraction (~1.5-2 sec/page)
- **DROMIC Bottleneck**: pdfplumber table extraction (~0.5-1 sec/page)
- **Memory**: Large reports (1000+ pages) can spike to 2GB RAM during extraction

---

## Contributing

### Reporting Issues
Open GitHub issue with:
1. PDF file (or link to public report)
2. Steps to reproduce
3. Expected vs actual behavior
4. Screenshots if UI issue
5. Specify which tool (NDRRMC or DROMIC)

### Pull Requests
1. Fork repository
2. Create feature branch
3. Test locally with multiple reports (both tools)
4. Submit PR with description of changes

### Priority Fixes Needed
1. Migrate pandas `.fillna(method='ffill')` → `.ffill()` 
2. Add Level column check in casualties dashboard section
3. Suppress Java PDFBox font warnings
4. Add automated tests for transformation functions
5. Improve DROMIC error messages for malformed tables

---

[Lee Revis](https://www.linkedin.com/in/leerevis/)

[Ella Blom](https://www.linkedin.com/in/ella-blom/)

---

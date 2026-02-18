# NDRRMC PDF Extraction & Analytics Tool

## Overview

Automated extraction and transformation pipeline for Philippine NDRRMC (National Disaster Risk Reduction and Management Council) DROMIC (Disaster Response Operations Monitoring and Information Center) situational reports.

**Purpose:** Convert 100-600 page disaster response PDFs into structured, analysis-ready datasets with proper administrative geography hierarchy and advanced vulnerability analytics.

**Live Demo:** [[Streamlit Tool]](https://ifrc-ndrrmc-convert.streamlit.app/)

---

## Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Installation](#installation)
- [File Structure](#file-structure)
- [Data Pipeline](#data-pipeline)
- [Transformation Logic](#transformation-logic)
- [Dashboard Analytics](#dashboard-analytics)
- [Configuration](#configuration)
- [Deployment](#deployment)
- [Known Issues](#known-issues)
- [Contributing](#contributing)

---

## Features

### Core Functionality
- **PDF Extraction**: Tabula-based extraction of 13 table types from NDRRMC reports
- **Location Hierarchy**: Automatic parsing of Region → Province → Municipality → Barangay structure
- **Data Transformation**: Standardization, validation, and cleaning of extracted tables
- **Interactive Dashboards**: Summary and detailed analytics with 5 vulnerability assessment modules
- **CSV Export**: Clean, analysis-ready downloads with dynamic filtering

### Supported Tables
- **Demographics**: Affected Population, Casualties, Damaged Houses
- **Infrastructure**: Roads & Bridges, Power Supply, Water Supply, Communications
- **Sectoral**: Agriculture Damage, Infrastructure Damage
- **Assistance**: Family Assistance, LGU Assistance, Pre-emptive Evacuation
- **Incidents**: Related Incidents (flooding, landslides)

---

## Architecture
```
┌─────────────────┐
│   Streamlit UI  │
│   (Frontend)    │
└────────┬────────┘
         │
    ┌────▼─────────────────────────────────┐
    │                                      │
┌───▼──────────┐              ┌───────────▼────┐
│ pdf_extractor│              │ transformations│
│              │              │                │
│ - Tabula     │              │ - Standardize  │
│ - PyPDF2     │              │ - Validate     │
│ - Parsing    │              │ - Hierarchy    │
└───┬──────────┘              └───────────┬────┘
    │                                     │
    │        ┌────────────────────────────┘
    │        │
┌───▼────────▼───┐
│  Session State │
│  (In-memory)   │
└───┬────────────┘
    │
┌───▼──────────────┐
│ Dashboard Analytics│
│                    │
│ - Assistance Gaps  │
│ - Vulnerability    │
│ - Recovery Track   │
└────────────────────┘
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
git clone https://github.com/YOUR-USERNAME/ifrc-ndrrmc-convert.git
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
```

---

## File Structure
```
ifrc-ndrrmc-convert/
├── streamlit_app.py          # Main Streamlit application
├── pdf_extractor.py           # PDF extraction functions
├── transformations.py         # Data transformation pipeline
├── config.py                  # Configuration and mappings
├── requirements.txt           # Python dependencies
├── packages.txt               # System dependencies (Java)
├── assets/                    # Logo and static files
│   └── Logo-Horizontal-RGB.svg
├── pdfs_to_analyze/          # (Optional) Analysis folder
└── README.md                  # This file
```

---

## Data Pipeline

### 1. Extraction (`pdf_extractor.py`)

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

**Key Functions:**
- `tabula.read_pdf()` - Extract tables from PDF pages
- `detect_table_type()` - Identify table category from header patterns
- `parse_location_hierarchy()` - Extract Region/Province/Municipality/Barangay
- `clean_numeric_columns()` - Strip formatting, convert to float

### 2. Transformation (`transformations.py`)

Each table type has dedicated transformation:
```python
transform_affected_population(df_raw)
transform_casualties(df_raw)
transform_damaged_houses(df_raw)
# ... etc
```

**Standard transformations:**
1. **Header standardization** - Remove special characters, normalize names
2. **Location parsing** - Extract hierarchy from merged cells
3. **Numeric cleaning** - Remove commas, convert strings to numbers
4. **Level tagging** - Add 'Level' column (Region/Province/Municipality/Barangay)
5. **Validation** - Check for missing critical fields

**Special handling:**
- **Metro Manila districts** - Treated as municipalities
- **Highly Urbanized Cities** - Function as both province and municipality
- **Casualties** - PII removal (names redacted for privacy)

### 3. Analytics (`streamlit_app.py` - Dashboard section)

**5 Analytical Modules:**

#### 1. Assistance Gap Analysis
```python
Gap_Score = Families_Requiring_Assistance / Families_Assisted
Weighted_Gap_Score = Gap_Score × (1 + Displacement_Magnitude/100)
```

#### 2. Access & Isolation Risk
```python
Isolation_Score = Road_Blockage + Still_Flooded + High_Displacement
```

#### 3. Lifelines Compound Failure
```python
Lifelines_Failed = Water_Down + Power_Down + Comms_Down
```

#### 4. Recovery Progress Tracking
```python
Recovery_Rate = Restored / (Interrupted + Restored) × 100%
Stagnation_Score = No_Water_Recovery + No_Power_Recovery + Still_Flooded
```

#### 5. Vulnerability Hotspots
```python
# Housing Score Components
Displacement_Rate = (Total_Displaced / Affected_Persons) × 100
Housing_Damage_Rate = (Total_Damaged / Affected_Families) × 100
Housing_Severity = ((Totally_Damaged × 2 + Partially × 1) / Affected_Families) × 100

Housing_Score = (Displacement_Rate × 0.4) + (Housing_Damage_Rate × 0.3) + (Housing_Severity × 0.3)

# Combined Index
Vulnerability_Index = (Displacement_Score × 0.4) + (Housing_Score × 0.3) + (Lifeline_Score × 0.3)
Weighted_Vulnerability = Vulnerability_Index × (1 + Impact_Magnitude/100)
```

**Magnitude Weighting:**
Uses `MinMaxScaler` to normalize absolute numbers (affected population, displaced persons) to 0-100 scale, then weights scores to prioritize larger-scale impacts.

---

## Transformation Logic

### Location Hierarchy Parsing

Philippine administrative structure:
```
Region (17 regions)
  ├── Province
  │     ├── Municipality / City
  │     │     └── Barangay
```

**Special Cases:**
- **NCR (Metro Manila)**: 16 districts treated as municipalities
- **Independent Cities**: Highly Urbanized Cities (HUCs) are province-level but also have barangays
- **NIR (Negros Island Region)**: Created post-2020 census, provinces mapped back to original regions

**Parsing Strategy:**
1. Detect indentation level from PDF spacing
2. Use `config.REGION_PROVINCE_MAP` for validation
3. Forward-fill location columns for nested entries
4. Tag each row with administrative `Level`

### Data Quality Issues

**Common PDF problems handled:**
- Merged cells spanning multiple rows
- Inconsistent spacing/indentation
- Mixed numeric formats (1,234 vs 1234.0)
- Subtotal/total rows mixed with data
- Header rows repeated mid-table

**Validation checks:**
- Non-negative values for counts
- Logical hierarchy (barangays within municipalities)
- Required columns present
- Numeric columns actually numeric

---

## Dashboard Analytics

### Data Consolidation

Dashboard creates `df_insights` by merging:
```python
df_insights = (
    AFFECTED_POPULATION +
    DAMAGED_HOUSES (left join) +
    ASSISTANCE_TO_FAMILIES (left join) +
    RELATED_INCIDENTS (left join)
)
```

**Aggregation level:** Municipality (filters `Level == 'Municipality'`)

**Graceful degradation:** Sections adapt if data tables missing (e.g., no assistance data → skip gap analysis)

### Normalization

Uses `sklearn.preprocessing.MinMaxScaler` for:
- Lifeline failures (water/power/comms interruptions)
- Displacement magnitude
- Impact magnitude for vulnerability weighting

**Scale:** 0-100 for interpretability

### Known Data Issues

**Displacement Rate Unreliability:**
Analysis of 1,538 municipalities across 5 reports shows:
- 41.4% report Affected Persons = Total Displaced (data quality issue)
- When differentiated, average gap is 35.1%
- Vulnerability scoring weights this accordingly

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
- Validate location parsing
- Standardize region/province names
- Handle administrative boundary changes

---

## Deployment

### Streamlit Cloud

**Files required:**
- `requirements.txt` - Python packages
- `packages.txt` - System packages (contains: `default-jre` for Java)

**Deployment steps:**
1. Push to GitHub
2. Connect repository at share.streamlit.io
3. Set main file: `streamlit_app.py`
4. Deploy (auto-rebuilds on git push)

**Resource considerations:**
- Extraction can take 15-20 minutes for 500+ page PDFs
- Streamlit Cloud free tier has timeout limits
- Java installation adds ~30 seconds to cold start

### Environment Variables

None required - tool is self-contained.

---

## Known Issues

### Critical
- **Deprecation warnings**: `fillna(method='ffill')` → need to migrate to `.ffill()`
- **Casualties Level column**: Missing in some transformed outputs, causes KeyError in dashboard

### Moderate
- **Font warnings**: PDFBox font substitution warnings (cosmetic, can be suppressed)
- **Extraction failures**: Some very old reports (pre-2020) have incompatible formatting
- **Lifelines data**: Roads/bridges/power/water tables often unpopulated (depends on report completeness)

### Minor
- **NIR province mapping**: Negros Island Region created after 2020 census, manual mapping required
- **Metro Manila barangay counts**: Districts treated as municipalities, may inflate barangay counts

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
- **Bottleneck**: Tabula PDF extraction (~1.5-2 sec/page)
- **Optimization**: Selective page extraction, parallel processing not implemented
- **Memory**: Large reports (1000+ pages) can spike to 2GB RAM during extraction

---

## Contributing


### Pull Requests
1. Fork repository
2. Create feature branch
3. Test locally with multiple reports
4. Submit PR with description of changes

### Priority Fixes Needed
1. Migrate pandas `.fillna(method='ffill')` → `.ffill()` 
2. Add Level column check in casualties dashboard section
3. Suppress Java PDFBox font warnings
4. Add automated tests for transformation functions


## Version History

- **v1.0-beta** (2025-01-07) - Initial beta release with 5 analytics modules

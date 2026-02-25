import streamlit as st
import pandas as pd
import numpy as np
from pdf_extractor import extract_summary_tables, extract_detailed_tables
import transformations
import tempfile
import os
import PyPDF2
import tabula
from sklearn.preprocessing import MinMaxScaler

# =============================================================================
# STREAMLIT CONFIG
# =============================================================================

# Page configuration
st.set_page_config(
    page_title="NDRRMC Report Extractor",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state
if 'current_page' not in st.session_state:
    st.session_state.current_page = "Home"

if 'current_tool' not in st.session_state:
    st.session_state.current_tool = None

def format_dataframe_for_display(df):
    """
    Format dataframe for display:
    - Remove underscores from column names
    - Add comma formatting to numeric columns
    """
    df_display = df.copy()
    
    # Remove underscores from column names
    df_display.columns = [col.replace('_', ' ') for col in df_display.columns]
    
    # Format numeric columns with commas
    for col in df_display.columns:
        # Check if numeric (int or float, any bit size)
        if pd.api.types.is_numeric_dtype(df_display[col]):
            # Convert to string with comma formatting
            df_display[col] = df_display[col].apply(
                lambda x: f"{int(x):,}" if pd.notna(x) and x == int(x) else (f"{x:,.2f}" if pd.notna(x) else "")
            )
    
    return df_display

def create_dynamic_filters(df, table_name):
    """
    Create smart dynamic filters for dataframes:
    - Location hierarchy (Region, Province, Municipality, Barangay) with cascading
    - Other text columns with <30 unique values
    - Skip numeric columns and 'Location' column
    
    Returns: filtered dataframe
    """
    df_filtered = df.copy()
    
    # Define location hierarchy columns
    location_cols = ['Region', 'Province', 'Municipality', 'Barangay']
    
    # Find which location columns exist in this dataframe
    existing_location_cols = [col for col in location_cols if col in df.columns]
    
    # Find other filterable text columns (not location, not numeric, <30 unique values)
    other_filterable_cols = []
    for col in df.columns:
        if col in location_cols or col == 'Location':
            continue  # Skip location hierarchy and 'Location' column
        if not pd.api.types.is_numeric_dtype(df[col]):
            unique_count = df[col].nunique()
            if unique_count < 30 and unique_count > 1:
                other_filterable_cols.append(col)
    
    # Create cascading location filters
    if existing_location_cols:
        st.markdown("**Location Filters:**")
        location_filter_cols = st.columns(len(existing_location_cols))
        
        for idx, col in enumerate(existing_location_cols):
            with location_filter_cols[idx]:
                unique_values = ['All'] + sorted(df_filtered[col].dropna().astype(str).unique().tolist())
                
                selected_value = st.selectbox(
                    col,
                    unique_values,
                    key=f"filter_{table_name}_{col}"
                )
                
                if selected_value != 'All':
                    df_filtered = df_filtered[df_filtered[col].astype(str) == selected_value]
    
    # Create other categorical filters
    if other_filterable_cols:
        st.markdown("**Other Filters:**")
        
        # Layout in rows of 4
        num_other_cols = len(other_filterable_cols)
        rows_needed = (num_other_cols + 3) // 4
        
        for row_idx in range(rows_needed):
            cols = st.columns(4)
            
            for col_idx in range(4):
                overall_idx = row_idx * 4 + col_idx
                
                if overall_idx >= num_other_cols:
                    break
                
                column = other_filterable_cols[overall_idx]
                
                with cols[col_idx]:
                    unique_values = ['All'] + sorted(df_filtered[column].dropna().astype(str).unique().tolist())
                    
                    selected_value = st.selectbox(
                        column.replace('_', ' '),
                        unique_values,
                        key=f"filter_{table_name}_{column}"
                    )
                    
                    if selected_value != 'All':
                        df_filtered = df_filtered[df_filtered[column].astype(str) == selected_value]
    
    return df_filtered

# =============================================================================
# SIDEBAR NAVIGATION
# =============================================================================
with st.sidebar:
    # IFRC Logo
    col1, col2, col3 = st.columns([0.5, 2, 0.5])
    with col2:
        st.image("assets/Logo-Horizontal-RGB.svg", width='stretch')
    
    st.markdown("<h1 style='text-align: center;'>Philippines Data Conversion Tools</h1>", unsafe_allow_html=True)
    st.markdown("<p style='text-align: center;'><a href='https://monitoring-dashboard.ndrrmc.gov.ph/page/situations' target='_blank'>NDRRMC Sitreps</a></p>", unsafe_allow_html=True)
    st.markdown("<p style='text-align: center;'><a href='https://dromic.dswd.gov.ph/category/situation-reports/' target='_blank'>DWSD DROMIC Sitreps</a></p>", unsafe_allow_html=True)
    st.markdown("---")
    
    # Show current tool
    if st.session_state.current_tool:
        tool_emoji = "🔷" if st.session_state.current_tool == "NDRRMC" else "🔶"
        st.info(f"{tool_emoji} **Active Tool:** {st.session_state.current_tool}")
        st.markdown("---")
    
    # Home button (always visible)
    if st.button("🏠 Home", width='stretch', 
                 type="primary" if st.session_state.current_page == "Home" else "secondary",
                 key="nav_home"):
        st.session_state.current_page = "Home"
        st.session_state.current_tool = None
        st.rerun()
    
    # NDRRMC Tool Navigation
    if st.session_state.current_tool == "NDRRMC":
        st.markdown("**📄 NDRRMC Sitrep**")
        
        if st.button("📁 Load PDF", width='stretch', 
                     type="primary" if st.session_state.current_page == "Load PDF" else "secondary",
                     key="nav_load"):
            st.session_state.current_page = "Load PDF"
            st.rerun()
        
        if st.button("📊 Summary Dashboard", width='stretch', 
                     type="primary" if st.session_state.current_page == "Summary" else "secondary",
                     key="nav_summary"):
            st.session_state.current_page = "Summary"
            st.rerun()
        
        if st.button("🔍 Extract Detailed Data", width='stretch', 
                     type="primary" if st.session_state.current_page == "Extract" else "secondary",
                     key="nav_extract"):
            st.session_state.current_page = "Extract"
            st.rerun()
        
        if st.button("📊 Detailed Dashboard", width='stretch', 
                     type="primary" if st.session_state.current_page == "Dashboard" else "secondary",
                     key="nav_dashboard"):
            st.session_state.current_page = "Dashboard"
            st.rerun()

        if st.button("⬇️ Downloads", width='stretch', 
                     type="primary" if st.session_state.current_page == "Downloads" else "secondary",
                     key="nav_downloads"):
            st.session_state.current_page = "Downloads"
            st.rerun()
    
    # DROMIC Tool Navigation
    elif st.session_state.current_tool == "DROMIC":
        st.markdown("**📋 DROMIC Tool**")
        
        if st.button("📁 Load PDF", width='stretch', 
                     type="primary" if st.session_state.current_page == "Load PDF" else "secondary",
                     key="nav_dromic_load"):
            st.session_state.current_page = "Load PDF"
            st.rerun()
        
        if st.button("🔍 Extract & Transform", width='stretch', 
                     type="primary" if st.session_state.current_page == "DROMIC Extract" else "secondary",
                     key="nav_dromic_extract"):
            st.session_state.current_page = "DROMIC Extract"
            st.rerun()
    
    
    # Help (always visible)
    st.markdown("---")
    if st.button("❓ Help & FAQ", width='stretch', 
                 type="primary" if st.session_state.current_page == "Help" else "secondary",
                 key="nav_help"):
        st.session_state.current_page = "Help"
        st.rerun()
    
    st.markdown("---")
    st.caption("IFRC Philippines 2025")

page = st.session_state.current_page


# =============================================================================
# HOME PAGE
# =============================================================================
if page == "Home":
    st.title("📊 NDRRMC Data Extraction Tools")
    st.markdown("### Choose your extraction tool")
    
    # Hero section
    col1, col2 = st.columns(2)
    
    with col1:
        with st.container(border=True):
            st.subheader("🔷 NDRRMC Sitrep Extractor")
            st.markdown("""
            **Best for:** Multi-table extraction with detailed analytics
            
            **Features:**
            - 13 table types supported
            - Summary + detailed dashboards
            - 5 vulnerability assessment modules
            - Interactive filters and downloads
            
            **Typical reports:** 100-600 pages
            """)
            
            if st.button("📄 Launch NDRRMC Sitrep Tool", type="primary", use_container_width=True):
                st.session_state.current_tool = "NDRRMC"
                st.session_state.current_page = "Load PDF"
                st.rerun()
    
    with col2:
        with st.container(border=True):
            st.subheader("🔶 DROMIC Report Extractor")
            st.markdown("""
            **Best for:** Quick single-table extraction
            
            **Features:**
            - Custom text pattern detection
            - Automatic admin level detection
            - P-code matching
            - Single CSV output
            
            **Typical reports:** DSWD DROMIC reports
            """)
            
            if st.button("📋 Launch DROMIC Tool", type="primary", use_container_width=True):
                st.session_state.current_tool = "DROMIC"
                st.session_state.current_page = "Load PDF"
                st.rerun()
    
    st.markdown("---")
 

# =============================================================================
# LOAD PDF PAGE
# =============================================================================
elif page == "Load PDF":
    st.title("📁 Load PDF Report")
    st.markdown("---")
    
    # Center the upload interface
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        tab1, tab2 = st.tabs(["📤 Upload File", "🔗 Load from URL"])
        
        with tab1:
            st.markdown("**Upload PDF from your computer**")
            uploaded_file = st.file_uploader("Choose a PDF file", type="pdf", key="pdf_uploader")
            
            if uploaded_file is not None:
                st.success(f"✅ Loaded: {uploaded_file.name}")
                
                # Save to temp file
                with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as tmp_file:
                    tmp_file.write(uploaded_file.getvalue())
                    temp_pdf_path = tmp_file.name
                
                st.session_state['temp_pdf_path'] = temp_pdf_path
                st.session_state['pdf_name'] = uploaded_file.name
                st.session_state['pdf_loaded'] = True
                
                # Extract page count
                import PyPDF2
                try:
                    with open(temp_pdf_path, 'rb') as f:
                        pdf_reader = PyPDF2.PdfReader(f)
                        page_count = len(pdf_reader.pages)
                        st.session_state['page_count'] = page_count
                        st.info(f"📄 {page_count} pages detected")
                except Exception as e:
                    st.error(f"Error reading PDF: {str(e)}")
                    st.session_state['pdf_loaded'] = False
                
                # Auto-extract summaries (ONLY for NDRRMC tool)
                if st.session_state.current_tool == "NDRRMC":
                    if st.session_state.get('pdf_loaded') and 'summaries' not in st.session_state:
                        st.markdown("")
                        with st.spinner("📊 Extracting summary tables (~2 seconds)..."):
                            try:
                                summaries = extract_summary_tables(temp_pdf_path)
                                st.session_state['summaries'] = summaries
                                st.session_state['summary_extracted'] = True
                                st.success(f"✅ Found {len(summaries)} tables with summary data")
                            except Exception as e:
                                st.error(f"Error extracting summaries: {str(e)}")
                                st.session_state['summary_extracted'] = False
                    
                    # Navigation buttons for NDRRMC
                    if st.session_state.get('summary_extracted'):
                        st.markdown("")
                        col_a, col_b = st.columns(2)
                        with col_a:
                            if st.button("📊 View Summary Dashboard", type="primary", width='stretch', key="goto_summary"):
                                st.session_state.current_page = "Summary"
                                st.rerun()
                        with col_b:
                            if st.button("🔍 Extract Detailed Tables", width='stretch', key="goto_extract"):
                                st.session_state.current_page = "Extract"
                                st.rerun()
        
        with tab2:
            st.markdown("**Load PDF from a web address**")
            pdf_url = st.text_input("Enter PDF URL:", placeholder="https://example.com/report.pdf", key="pdf_url_input")
            
            if pdf_url:
                if st.button("Load PDF from URL", type="primary", key="load_url_btn"):
                    import requests
                    
                    with st.spinner("📥 Downloading PDF from URL..."):
                        try:
                            response = requests.get(pdf_url, timeout=30)
                            response.raise_for_status()
                            
                            # Save to temp file
                            with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as tmp_file:
                                tmp_file.write(response.content)
                                temp_pdf_path = tmp_file.name
                            
                            # Extract filename from URL
                            import urllib.parse
                            parsed_url = urllib.parse.urlparse(pdf_url)
                            filename = os.path.basename(parsed_url.path) or "downloaded_report.pdf"
                            
                            st.session_state['temp_pdf_path'] = temp_pdf_path
                            st.session_state['pdf_name'] = filename
                            st.session_state['pdf_loaded'] = True
                            
                            st.success(f"✅ Downloaded: {filename}")
                            
                            # Extract page count
                            import PyPDF2
                            try:
                                with open(temp_pdf_path, 'rb') as f:
                                    pdf_reader = PyPDF2.PdfReader(f)
                                    page_count = len(pdf_reader.pages)
                                    st.session_state['page_count'] = page_count
                                    st.info(f"📄 {page_count} pages detected")
                            except Exception as e:
                                st.error(f"Error reading PDF: {str(e)}")
                                st.session_state['pdf_loaded'] = False
                            
                            # Auto-extract summaries (ONLY for NDRRMC tool)
                            if st.session_state.current_tool == "NDRRMC":
                                if st.session_state.get('pdf_loaded'):
                                    with st.spinner("📊 Extracting summary tables..."):
                                        try:
                                            summaries = extract_summary_tables(temp_pdf_path)
                                            st.session_state['summaries'] = summaries
                                            st.session_state['summary_extracted'] = True
                                            st.success(f"✅ Found {len(summaries)} tables with summary data")
                                            st.rerun()
                                        except Exception as e:
                                            st.error(f"Error extracting summaries: {str(e)}")
                                            st.session_state['summary_extracted'] = False
                        
                        except requests.exceptions.RequestException as e:
                            st.error(f"Error downloading PDF: {str(e)}")
                        except Exception as e:
                            st.error(f"Unexpected error: {str(e)}")
    
    # Navigation for DROMIC tool (OUTSIDE tabs)
    if st.session_state.current_tool == "DROMIC" and st.session_state.get('pdf_loaded'):
        st.markdown("---")
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            if st.button("🔍 Go to Extract & Transform", type="primary", use_container_width=True, key="goto_dromic_extract_main"):
                st.session_state.current_page = "DROMIC Extract"
                st.rerun()

# =============================================================================
# SUMMARY DASHBOARD
# =============================================================================
elif page == "Summary":
    st.title("📊 Summary Dashboard")
    st.caption("Automatically extracted summary tables")
    st.markdown("---")
    
    # Check if summaries exist
    if 'summaries' not in st.session_state or not st.session_state.get('summary_extracted'):
        st.warning("⚠️ Please load a PDF first")
        if st.button("📁 Go to Load PDF", type="primary"):
            st.session_state.current_page = "Load PDF"
            st.rerun()
    else:
        summaries = st.session_state['summaries']
    

        # Display disaster info if available
        if 'report_metadata' in st.session_state:
            metadata = st.session_state['report_metadata']
            if metadata.get('disaster_name'):
                st.info(f"🌪️ **Disaster:** {metadata['disaster_name']} ({metadata.get('disaster_year', '')})")
        
        st.markdown("")
        
        # Key metrics section
        st.subheader("Overview Metrics")
        
        col1, col2, col3, col4 = st.columns(4)
        
        # Affected Population metrics
        if 'AFFECTED POPULATION' in summaries:
            df_ap = summaries['AFFECTED POPULATION']
            df_ap_calc = df_ap[df_ap['Region'] != '**TOTAL**'].copy()
            
            with col1:
                total_families = df_ap_calc['Families'].sum()
                st.metric("👨‍👩‍👧‍👦 Families Affected", f"{total_families:,}")
            
            with col2:
                total_persons = df_ap_calc['Persons'].sum()
                st.metric("👥 Persons Affected", f"{total_persons:,}")
            
            with col3:
                total_displaced = df_ap_calc['Inside Persons'].sum() + df_ap_calc['Outside Persons'].sum()
                st.metric("🏕️ Total Displaced", f"{total_displaced:,}")
        
        # Casualties
        if 'CASUALTIES' in summaries:
            df_cas = summaries['CASUALTIES']
            df_cas_calc = df_cas[df_cas['Region'] != '**TOTAL**'].copy()
            
            with col4:
                total_casualties = (df_cas_calc['Validated_dead'].sum() + 
                                  df_cas_calc['Validated_injured'].sum() + 
                                  df_cas_calc['Validated_missing'].sum())
                st.metric("⚠️ Total Casualties", f"{int(total_casualties):,}")
        
        # Damaged Houses
        if 'DAMAGED HOUSES' in summaries:
            df_dh = summaries['DAMAGED HOUSES']
            df_dh_calc = df_dh[df_dh['Region'] != '**GRAND TOTAL**'].copy()
            
            # Add to existing columns or create new row
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                total_damaged = df_dh_calc['Total'].sum()
                st.metric("🏠 Damaged Houses", f"{int(total_damaged):,}")
        
        st.markdown("---")
        
        # Detailed tables in tabs
        st.subheader("Detailed Summary Tables")
        
        # Group tables by category
        demographics_tables = []
        damages_tables = []
        lifelines_tables = []
        assistance_tables = []
        
        for table_name in summaries.keys():
            if table_name in ['AFFECTED POPULATION', 'CASUALTIES']:
                demographics_tables.append(table_name)
            elif table_name in ['DAMAGED HOUSES', 'DAMAGE TO INFRASTRUCTURE', 'DAMAGE TO AGRICULTURE', 'RELATED INCIDENTS']:  # ADD HERE
                damages_tables.append(table_name)
            elif table_name in ['ROADS AND BRIDGES', 'POWER', 'WATER SUPPLY', 'COMMUNICATION LINES']:
                lifelines_tables.append(table_name)
            elif table_name in ['ASSISTANCE TO FAMILIES', 'ASSISTANCE TO LGUS', 'PRE-EMPTIVE EVACUATION']:
                assistance_tables.append(table_name)
        
        # Create tabs
        tab_names = []
        if demographics_tables: tab_names.append("👥 Demographics")
        if damages_tables: tab_names.append("🏚️ Damages")
        if lifelines_tables: tab_names.append("⚡ Lifelines")
        if assistance_tables: tab_names.append("🚑 Assistance")
        
        if tab_names:
            tabs = st.tabs(tab_names)
            tab_idx = 0
            
            # Demographics tab
            if demographics_tables:
                with tabs[tab_idx]:
                    for table_name in demographics_tables:
                        df = summaries[table_name]
                        st.markdown(f"**{table_name}**")
                        
                        # Remove TOTAL rows for display
                        if table_name == 'AFFECTED POPULATION':
                            df_display = df[df['Region'] != '**TOTAL**'].copy()
                        elif table_name == 'CASUALTIES':
                            df_display = df[df['Region'] != '**TOTAL**'].copy()
                        else:
                            df_display = df.copy()
                        
                        df_formatted = format_dataframe_for_display(df_display)
                        st.dataframe(df_formatted, width='stretch', hide_index=True)
                        st.markdown("")
                tab_idx += 1
            
            # Damages tab
            if damages_tables:
                with tabs[tab_idx]:
                    for table_name in damages_tables:
                        df = summaries[table_name]
                        st.markdown(f"**{table_name}**")
                        
                        if table_name == 'DAMAGED HOUSES':
                            df_display = df[df['Region'] != '**GRAND TOTAL**'].copy()
                        else:
                            df_display = df.copy()
                        
                        df_formatted = format_dataframe_for_display(df_display)
                        st.dataframe(df_formatted, width='stretch', hide_index=True)
                        st.markdown("")
                tab_idx += 1
            
            # Lifelines tab
            if lifelines_tables:
                with tabs[tab_idx]:
                    for table_name in lifelines_tables:
                        df = summaries[table_name]
                        st.markdown(f"**{table_name}**")
                        df_formatted = format_dataframe_for_display(df)
                        st.dataframe(df_formatted, width='stretch', hide_index=True)
                        st.markdown("")
                tab_idx += 1
            
            # Assistance tab
            if assistance_tables:
                with tabs[tab_idx]:
                    for table_name in assistance_tables:
                        df = summaries[table_name]
                        st.markdown(f"**{table_name}**")
                        
                        # Remove TOTAL rows for display
                        if table_name == 'PRE-EMPTIVE EVACUATION':
                            df_display = df[df['Region'] != '**TOTAL**'].copy()
                        else:
                            df_display = df.copy()
                        
                        df_formatted = format_dataframe_for_display(df_display)
                        st.dataframe(df_formatted, width='stretch', hide_index=True)
                        st.markdown("")
        
        # Navigation buttons
        st.markdown("---")
        col1, col2 = st.columns(2)
        with col1:
            if st.button("⬅️ Back to Upload", width='stretch', key="back_to_load"):
                st.session_state.current_page = "Load PDF"
                st.rerun()
        with col2:
            if st.button("🔍 Extract Detailed Tables ➡️", type="primary", width='stretch', key="summary_to_extract"):
                st.session_state.current_page = "Extract"
                st.rerun()

# =============================================================================
# EXTRACTION
# =============================================================================
elif page == "Extract":
    st.title("🔍 Extract Detailed Tables")
    st.caption("Select tables to extract with full location hierarchy")
    st.markdown("---")
    
    # Check if PDF loaded
    if 'summaries' not in st.session_state or not st.session_state.get('summary_extracted'):
        st.warning("⚠️ Please load a PDF first")
        if st.button("📁 Go to Load PDF", type="primary"):
            st.session_state.current_page = "Load PDF"
            st.rerun()
    else:
        # Define transformable tables
        transformable_tables = [
            'AFFECTED POPULATION',
            'CASUALTIES',
            'DAMAGED HOUSES',
            'ROADS AND BRIDGES',
            'POWER',
            'WATER SUPPLY',
            'COMMUNICATION LINES',
            'DAMAGE TO AGRICULTURE',
            'DAMAGE TO INFRASTRUCTURE',
            'ASSISTANCE TO FAMILIES',
            'ASSISTANCE TO LGUS',
            'PRE-EMPTIVE EVACUATION',
            'RELATED INCIDENTS'
        ]
        
        # Available tables dictionary
        available_tables = {
            'AFFECTED POPULATION': 'Detailed breakdown by municipality/barangay',
            'CASUALTIES': 'Casualty details (names removed for privacy)',
            'DAMAGED HOUSES': 'Houses damaged by location',
            'ROADS AND BRIDGES': 'Infrastructure status details',
            'POWER': 'Power interruption details',
            'WATER SUPPLY': 'Water supply interruption details',
            'COMMUNICATION LINES': 'Communication line status',
            'DAMAGE TO AGRICULTURE': 'Agricultural damage details',
            'DAMAGE TO INFRASTRUCTURE': 'Infrastructure damage details',
            'ASSISTANCE TO FAMILIES': 'Family assistance details',
            'ASSISTANCE TO LGUS': 'LGU assistance details',
            'PRE-EMPTIVE EVACUATION': 'Pre-emptive evacuation details',
            'RELATED INCIDENTS': 'Flooded areas, landslides, and other incidents'  # ADD THIS
        }
        
        # Filter to only show tables present in PDF
        summary_tables = set(st.session_state['summaries'].keys())
        available_tables_filtered = {
            name: desc for name, desc in available_tables.items() 
            if name in summary_tables
        }
        
        # Table selection
        st.subheader("🔍 Select Detailed Tables to Extract")
        st.caption("Choose which tables you need to reduce extraction time")
        st.markdown(f"**Found {len(available_tables_filtered)} tables with detailed data available**")
        
        # Select All / Deselect All toggle
        col_toggle1, col_toggle2, col_toggle3 = st.columns([1, 1, 4])
        with col_toggle1:
            if st.button("✅ Select All", key="select_all_btn"):
                for table in available_tables_filtered.keys():
                    st.session_state[f'chk_{table}'] = True
                st.rerun()
        with col_toggle2:
            if st.button("❌ Deselect All", key="deselect_all_btn"):
                for table in available_tables_filtered.keys():
                    st.session_state[f'chk_{table}'] = False
                st.rerun()
        
        st.markdown("")
        
        # Group tables by category
        col1, col2, col3, col4 = st.columns(4)
        
        selected_tables = []
        
        with col1:
            st.markdown("**👥 Demographics**")
            if 'AFFECTED POPULATION' in available_tables_filtered:
                if st.checkbox("Affected Population", key="chk_AFFECTED POPULATION"):
                    selected_tables.append('AFFECTED POPULATION')
            if 'CASUALTIES' in available_tables_filtered:
                if st.checkbox("Casualties", key="chk_CASUALTIES"):
                    selected_tables.append('CASUALTIES')
        
        with col2:
            st.markdown("**🏚️ Damages**")
            if 'DAMAGED HOUSES' in available_tables_filtered:
                if st.checkbox("Damaged Houses", key="chk_DAMAGED HOUSES"):
                    selected_tables.append('DAMAGED HOUSES')
            if 'DAMAGE TO INFRASTRUCTURE' in available_tables_filtered:
                if st.checkbox("Infrastructure Damage", key="chk_DAMAGE TO INFRASTRUCTURE"):
                    selected_tables.append('DAMAGE TO INFRASTRUCTURE')
            if 'DAMAGE TO AGRICULTURE' in available_tables_filtered:
                if st.checkbox("Agriculture Damage", key="chk_DAMAGE TO AGRICULTURE"):
                    selected_tables.append('DAMAGE TO AGRICULTURE')
            if 'RELATED INCIDENTS' in available_tables_filtered:
                if st.checkbox("Related Incidents", key="chk_RELATED INCIDENTS"):
                    selected_tables.append('RELATED INCIDENTS')
        
        with col3:
            st.markdown("**⚡ Lifelines**")
            if 'ROADS AND BRIDGES' in available_tables_filtered:
                if st.checkbox("Roads and Bridges", key="chk_ROADS AND BRIDGES"):
                    selected_tables.append('ROADS AND BRIDGES')
            if 'POWER' in available_tables_filtered:
                if st.checkbox("Power", key="chk_POWER"):
                    selected_tables.append('POWER')
            if 'WATER SUPPLY' in available_tables_filtered:
                if st.checkbox("Water Supply", key="chk_WATER SUPPLY"):
                    selected_tables.append('WATER SUPPLY')
            if 'COMMUNICATION LINES' in available_tables_filtered:
                if st.checkbox("Communications", key="chk_COMMUNICATION LINES"):
                    selected_tables.append('COMMUNICATION LINES')
        
        with col4:
            st.markdown("**🚑 Assistance**")
            if 'ASSISTANCE TO FAMILIES' in available_tables_filtered:
                if st.checkbox("Family Assistance", key="chk_ASSISTANCE TO FAMILIES"):
                    selected_tables.append('ASSISTANCE TO FAMILIES')
            if 'ASSISTANCE TO LGUS' in available_tables_filtered:
                if st.checkbox("LGU Assistance", key="chk_ASSISTANCE TO LGUS"):
                    selected_tables.append('ASSISTANCE TO LGUS')
            if 'PRE-EMPTIVE EVACUATION' in available_tables_filtered:
                if st.checkbox("Pre-Emptive Evacuation", key="chk_PRE-EMPTIVE EVACUATION"):
                    selected_tables.append('PRE-EMPTIVE EVACUATION')
        
        st.markdown("---")
        
        # Show extraction button if tables selected
        if not selected_tables:
            st.warning("⚠️ Please select at least one table to extract")
        else:
            # Calculate estimated time
            page_count = st.session_state.get('page_count', 0)
            table_count = len(st.session_state['summaries'])
            
            # Conservative base rate - scales with document size (adjusted for Streamlit Cloud)
            if page_count < 100:
                base_rate = 1.5
            elif page_count < 300:
                base_rate = 1.8
            else:
                base_rate = 2.1
            
            # Add complexity from table count
            complexity_factor = table_count * 0.05
            per_page = base_rate + complexity_factor
            
            estimated_seconds = page_count * per_page + 15
            estimated_minutes = estimated_seconds / 60
            
            time_str = f"{estimated_minutes:.1f} minutes" if estimated_minutes >= 1 else f"{estimated_seconds:.0f} seconds"
            
            st.info(f"⏱️ Estimated extraction time: ~{time_str} (~{page_count} pages, {len(selected_tables)} tables selected)")
            
            # Extract button
            col1, col2, col3 = st.columns([1, 2, 1])
            
            with col2:
                if st.button("🔍 Extract Selected Tables", type="primary", width='stretch', key="extract_btn"):
                    import time
                    
                    pdf_path = st.session_state['temp_pdf_path']
                    
                    with st.spinner(f"🔄 Extracting {len(selected_tables)} table(s)..."):
                        # Progress tracking
                        progress_bar = st.progress(0)
                        status_text = st.empty()
                        time_text = st.empty()
                        
                        start_time = time.time()
                        
                        def update_progress(current, total, message):
                            progress = int((current / total) * 100)
                            progress_bar.progress(progress)
                            status_text.text(f"📄 {message}")
                            
                            elapsed = time.time() - start_time
                            minutes = int(elapsed // 60)
                            seconds = int(elapsed % 60)
                            time_text.text(f"⏱️ Time elapsed: {minutes}m {seconds}s")
                        
                        # Extract detailed tables
                        combined_sections = extract_detailed_tables(
                            pdf_path, 
                            selected_tables=selected_tables,
                            summaries=st.session_state['summaries'],
                            progress_callback=update_progress
                        )
                        
                        # Extract disaster name from filename
                        filename = st.session_state.get('pdf_name', '')
                        
                        report_metadata = {'disaster_name': 'Unknown', 'disaster_year': ''}
                        
                        import re
                        disaster_match = re.search(r'_Effects_[Oo]f_(.+?)_(\d{4})', filename)
                        if disaster_match:
                            report_metadata['disaster_name'] = disaster_match.group(1).replace('_', ' ')
                            report_metadata['disaster_year'] = disaster_match.group(2)
                        
                        # Save raw data
                        st.session_state['combined_sections'] = combined_sections
                        st.session_state['report_metadata'] = report_metadata
                        st.session_state['detailed_extracted'] = True
                        
                        # Auto-transform the extracted tables
                        st.session_state['transformed_tables'] = {}
                        
                        for table_name in combined_sections.keys():
                            try:
                                if table_name in transformable_tables:
                                    # Call the appropriate transformation function
                                    function_name = f"transform_{table_name.lower().replace(' ', '_').replace('-', '_')}"
                                    transform_func = getattr(transformations, function_name)
                                    df_transformed = transform_func(combined_sections[table_name])
                                    st.session_state['transformed_tables'][table_name] = df_transformed
                                else:
                                    # No transformation available, store raw data
                                    st.session_state['transformed_tables'][table_name] = combined_sections[table_name]
                            except Exception as e:
                                st.error(f"⚠️ Could not transform {table_name}: {str(e)}")
                                # Store raw data as fallback
                                st.session_state['transformed_tables'][table_name] = combined_sections[table_name]
                        
                        # Show completion message
                        total_time = time.time() - start_time
                        minutes = int(total_time // 60)
                        seconds = int(total_time % 60)
                    
                    # MOVED OUTSIDE THE SPINNER BLOCK:
                    # MOVED OUTSIDE THE SPINNER BLOCK:
                    st.success(f"✅ Extracted and transformed {len(combined_sections)} tables in {minutes}m {seconds}s!")

                    if report_metadata.get('disaster_name'):
                        st.info(f"🌪️ Disaster: {report_metadata['disaster_name']} ({report_metadata.get('disaster_year', '')})")

                    # Set flag and rerun to show buttons
                    st.session_state['extraction_complete'] = True
                    st.rerun()
        
        # Show navigation after extraction completes (OUTSIDE the button block)
        if st.session_state.get('extraction_complete'):
            st.markdown("---")
            st.markdown("**✅ Extraction Complete! Where would you like to go?**")
            col1, col2 = st.columns(2)

            with col1:
                if st.button("📊 View Dashboard", type="primary", use_container_width=True, key="goto_dash2"):
                    st.session_state['extraction_complete'] = False
                    st.session_state.current_page = "Dashboard"
                    st.rerun()

            with col2:
                if st.button("⬇️ View Downloads", use_container_width=True, key="goto_dl2"):
                    st.session_state['extraction_complete'] = False
                    st.session_state.current_page = "Downloads"
                    st.rerun()


# =============================================================================
# DOWNLOADS PAGE
# =============================================================================
elif page == "Downloads":
    st.title("⬇️ Download Transformed Tables")
    st.caption("Clean CSV files with Region/Province/Municipality/Barangay hierarchy")
    st.markdown("---")
    
    # Check if data exists
    if 'transformed_tables' not in st.session_state or not st.session_state.get('detailed_extracted'):
        st.warning("⚠️ Please extract data first")
        col1, col2 = st.columns(2)
        with col1:
            if st.button("📁 Load PDF", width='stretch'):
                st.session_state.current_page = "Load PDF"
                st.rerun()
        with col2:
            if st.button("🔍 Extract Data", type="primary", width='stretch'):
                st.session_state.current_page = "Extract"
                st.rerun()
    else:
        transformed_tables = st.session_state['transformed_tables']
        report_metadata = st.session_state.get('report_metadata', {})
        
        # Display disaster info
        if report_metadata.get('disaster_name'):
            st.info(f"🌪️ **Disaster:** {report_metadata['disaster_name']} ({report_metadata.get('disaster_year', '')})")
        
        st.markdown("")
        
        # Build filename components
        # Extract disaster name from metadata
        disaster_name = report_metadata.get('disaster_name', 'Unknown').replace(' ', '_')
        
        # Extract sitrep number from filename
        import re
        filename = st.session_state.get('pdf_name', '')
        sitrep_match = re.search(r'Report_No[._]+(\d+)', filename, re.IGNORECASE)
        sitrep_number = f"Sitrep{sitrep_match.group(1)}" if sitrep_match else "SitrepUnknown"
        
        # Date extracted (today)
        from datetime import datetime
        date_extracted = datetime.now().strftime("%Y%m%d")
        
        # Group tables by category for organized display
        demographics_tables = []
        damages_tables = []
        lifelines_tables = []
        assistance_tables = []
        
        for table_name in transformed_tables.keys():
            if table_name in ['AFFECTED POPULATION', 'CASUALTIES']:
                demographics_tables.append(table_name)
            elif table_name in ['DAMAGED HOUSES', 'DAMAGE TO INFRASTRUCTURE', 'DAMAGE TO AGRICULTURE', 'RELATED INCIDENTS']:
                damages_tables.append(table_name)
            elif table_name in ['ROADS AND BRIDGES', 'POWER', 'WATER SUPPLY', 'COMMUNICATION LINES']:
                lifelines_tables.append(table_name)
            elif table_name in ['ASSISTANCE TO FAMILIES', 'ASSISTANCE TO LGUS', 'PRE-EMPTIVE EVACUATION']:
                assistance_tables.append(table_name)
        

        # Display tables by category
        if demographics_tables:
            st.subheader("👥 Demographics")
            for table_name in demographics_tables:
                df = transformed_tables[table_name]
                
                with st.expander(f"**{table_name}** ({len(df):,} rows)", expanded=False):
                    # Add filters section
                    st.markdown("**Filters:**")
                    
                    # Call the dynamic filter function
                    df_filtered = create_dynamic_filters(df, table_name.replace(' ', '_'))
                    
                    # Show filtered row count
                    st.caption(f"Showing {len(df_filtered):,} of {len(df):,} rows")
                    
                    st.markdown("---")
                    
                    # Format for display
                    df_formatted = format_dataframe_for_display(df_filtered)
                    
                    # Show filtered dataframe
                    st.dataframe(
                        df_formatted,
                        width='stretch',
                        hide_index=True,
                        height=400,
                        use_container_width=True,
                        column_config=None,
                        key=f"view_{table_name.replace(' ', '_')}"
                    )
                    
                    # Build filename
                    table_name_clean = table_name.replace(' ', '_').replace('/', '_')
                    csv_filename = f"{disaster_name}_{sitrep_number}_{table_name_clean}_{date_extracted}.csv"
                    
                    # Two download buttons
                    col_dl1, col_dl2 = st.columns(2)
                    
                    with col_dl1:
                        # Download FULL data
                        csv_full = df.to_csv(index=False).encode('utf-8')
                        st.download_button(
                            f"📥 Download Full Data ({len(df):,} rows)",
                            data=csv_full,
                            file_name=csv_filename,
                            mime="text/csv",
                            key=f"dl_full_{table_name_clean}"
                        )
                    
                    with col_dl2:
                        # Download FILTERED data
                        csv_filtered = df_filtered.to_csv(index=False).encode('utf-8')
                        st.download_button(
                            f"📥 Download Filtered Data ({len(df_filtered):,} rows)",
                            data=csv_filtered,
                            file_name=csv_filename.replace('.csv', '_filtered.csv'),
                            mime="text/csv",
                            key=f"dl_filtered_{table_name_clean}"
                        )
            
            st.markdown("---")
        
        if damages_tables:
            st.subheader("🏚️ Damages")
            for table_name in damages_tables:
                df = transformed_tables[table_name]
                
                with st.expander(f"**{table_name}** ({len(df):,} rows)", expanded=False):
                    # Add filters section
                    st.markdown("**Filters:**")
                    
                    # Call the dynamic filter function
                    df_filtered = create_dynamic_filters(df, table_name.replace(' ', '_'))
                    
                    # Show filtered row count
                    st.caption(f"Showing {len(df_filtered):,} of {len(df):,} rows")
                    
                    st.markdown("---")
                    
                    # Format for display
                    df_formatted = format_dataframe_for_display(df_filtered)
                    
                    # Show filtered dataframe
                    st.dataframe(
                        df_formatted,
                        width='stretch',
                        hide_index=True,
                        height=400,
                        use_container_width=True,
                        column_config=None,
                        key=f"view_{table_name.replace(' ', '_')}"
                    )
                    
                    # Build filename
                    table_name_clean = table_name.replace(' ', '_').replace('/', '_')
                    csv_filename = f"{disaster_name}_{sitrep_number}_{table_name_clean}_{date_extracted}.csv"
                    
                    # Two download buttons
                    col_dl1, col_dl2 = st.columns(2)
                    
                    with col_dl1:
                        # Download FULL data
                        csv_full = df.to_csv(index=False).encode('utf-8')
                        st.download_button(
                            f"📥 Download Full Data ({len(df):,} rows)",
                            data=csv_full,
                            file_name=csv_filename,
                            mime="text/csv",
                            key=f"dl_full_{table_name_clean}"
                        )
                    
                    with col_dl2:
                        # Download FILTERED data
                        csv_filtered = df_filtered.to_csv(index=False).encode('utf-8')
                        st.download_button(
                            f"📥 Download Filtered Data ({len(df_filtered):,} rows)",
                            data=csv_filtered,
                            file_name=csv_filename.replace('.csv', '_filtered.csv'),
                            mime="text/csv",
                            key=f"dl_filtered_{table_name_clean}"
                        )
            
            st.markdown("---")
        
        if lifelines_tables:
            st.subheader("⚡ Lifelines")
            for table_name in lifelines_tables:
                df = transformed_tables[table_name]
                
                with st.expander(f"**{table_name}** ({len(df):,} rows)", expanded=False):
                    # Add filters section
                    st.markdown("**Filters:**")
                    
                    # Call the dynamic filter function
                    df_filtered = create_dynamic_filters(df, table_name.replace(' ', '_'))
                    
                    # Show filtered row count
                    st.caption(f"Showing {len(df_filtered):,} of {len(df):,} rows")
                    
                    st.markdown("---")
                    
                    # Format for display
                    df_formatted = format_dataframe_for_display(df_filtered)
                    
                    # Show filtered dataframe
                    st.dataframe(
                        df_formatted,
                        width='stretch',
                        hide_index=True,
                        height=400,
                        use_container_width=True,
                        column_config=None,
                        key=f"view_{table_name.replace(' ', '_')}"
                    )
                    
                    # Build filename
                    table_name_clean = table_name.replace(' ', '_').replace('/', '_')
                    csv_filename = f"{disaster_name}_{sitrep_number}_{table_name_clean}_{date_extracted}.csv"
                    
                    # Two download buttons
                    col_dl1, col_dl2 = st.columns(2)
                    
                    with col_dl1:
                        # Download FULL data
                        csv_full = df.to_csv(index=False).encode('utf-8')
                        st.download_button(
                            f"📥 Download Full Data ({len(df):,} rows)",
                            data=csv_full,
                            file_name=csv_filename,
                            mime="text/csv",
                            key=f"dl_full_{table_name_clean}"
                        )
                    
                    with col_dl2:
                        # Download FILTERED data
                        csv_filtered = df_filtered.to_csv(index=False).encode('utf-8')
                        st.download_button(
                            f"📥 Download Filtered Data ({len(df_filtered):,} rows)",
                            data=csv_filtered,
                            file_name=csv_filename.replace('.csv', '_filtered.csv'),
                            mime="text/csv",
                            key=f"dl_filtered_{table_name_clean}"
                        )
            
            st.markdown("---")
        
        if assistance_tables:
            st.subheader("🚑 Assistance Provided")
            for table_name in assistance_tables:
                df = transformed_tables[table_name]
                
                with st.expander(f"**{table_name}** ({len(df):,} rows)", expanded=False):
                    # Add filters section
                    st.markdown("**Filters:**")
                    
                    # Call the dynamic filter function
                    df_filtered = create_dynamic_filters(df, table_name.replace(' ', '_'))
                    
                    # Show filtered row count
                    st.caption(f"Showing {len(df_filtered):,} of {len(df):,} rows")
                    
                    st.markdown("---")
                    
                    # Format for display
                    df_formatted = format_dataframe_for_display(df_filtered)
                    
                    # Show filtered dataframe
                    st.dataframe(
                        df_formatted,
                        width='stretch',
                        hide_index=True,
                        height=400,
                        use_container_width=True,
                        column_config=None,
                        key=f"view_{table_name.replace(' ', '_')}"
                    )
                    
                    # Build filename
                    table_name_clean = table_name.replace(' ', '_').replace('/', '_')
                    csv_filename = f"{disaster_name}_{sitrep_number}_{table_name_clean}_{date_extracted}.csv"
                    
                    # Two download buttons
                    col_dl1, col_dl2 = st.columns(2)
                    
                    with col_dl1:
                        # Download FULL data
                        csv_full = df.to_csv(index=False).encode('utf-8')
                        st.download_button(
                            f"📥 Download Full Data ({len(df):,} rows)",
                            data=csv_full,
                            file_name=csv_filename,
                            mime="text/csv",
                            key=f"dl_full_{table_name_clean}"
                        )
                    
                    with col_dl2:
                        # Download FILTERED data
                        csv_filtered = df_filtered.to_csv(index=False).encode('utf-8')
                        st.download_button(
                            f"📥 Download Filtered Data ({len(df_filtered):,} rows)",
                            data=csv_filtered,
                            file_name=csv_filename.replace('.csv', '_filtered.csv'),
                            mime="text/csv",
                            key=f"dl_filtered_{table_name_clean}"
                        )

        
        # Navigation
        st.markdown("---")
        col1, col2 = st.columns(2)
        with col1:
            if st.button("⬅️ Back to Extraction", width='stretch', key="back_to_extract"):
                st.session_state.current_page = "Extract"
                st.rerun()
        with col2:
            if st.button("🏠 Back to Home", width='stretch', key="back_to_home"):
                st.session_state.current_page = "Home"
                st.rerun()



# =============================================================================
# DETAILED DASHBOARD
# =============================================================================
elif page == "Dashboard":
    st.title("📊 Detailed Impact Dashboard")
    st.caption("Advanced analytics and vulnerability assessment")
    st.warning("⚠️ **BETA:** Some sections require roads/lifelines data which may not be available in all reports")
    st.markdown("---")
    
    # Check if data exists
    if 'transformed_tables' not in st.session_state or not st.session_state.get('detailed_extracted'):
        st.warning("⚠️ Please extract detailed data first")
        if st.button("🔍 Go to Extract Data", type="primary"):
            st.session_state.current_page = "Extract"
            st.rerun()
    else:
        transformed_tables = st.session_state['transformed_tables']
        
        # Check which tables are available
        has_affected_pop = 'AFFECTED POPULATION' in transformed_tables
        has_damaged_houses = 'DAMAGED HOUSES' in transformed_tables
        has_casualties = 'CASUALTIES' in transformed_tables
        has_assistance = 'ASSISTANCE TO FAMILIES' in transformed_tables
        has_related_incidents = 'RELATED INCIDENTS' in transformed_tables
        
        if not has_affected_pop:
            st.warning("⚠️ Affected Population data needed for dashboard. Please extract it first.")
            if st.button("🔍 Go to Extract Data", type="primary"):
                st.session_state.current_page = "Extract"
                st.rerun()
        else:
            # Get the affected population data
            df_affected = transformed_tables['AFFECTED POPULATION'].copy()
            
            # Filter to municipality level only for aggregation
            df_muni = df_affected[df_affected['Level'] == 'Municipality'].copy()
            
            # Calculate overview metrics (always cumulative for overview)
            total_persons = df_muni['Affected_Persons'].sum()
            total_families = df_muni['Affected_Families'].sum()
            total_inside = df_muni['Inside_Persons_CUM'].sum()
            total_outside = df_muni['Outside_Persons_CUM'].sum()
            total_displaced = total_inside + total_outside
            total_ecs = df_muni['ECs_CUM'].sum()
            
            # Overview Metrics Section
            st.subheader("📈 Overview Metrics")
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("👥 Persons Affected", f"{total_persons:,.0f}")
                st.caption(f"{total_families:,.0f} families")
            
            with col2:
                st.metric("🏕️ Total Displaced", f"{total_displaced:,.0f}")
                proportion_displaced = (total_displaced / total_persons * 100) if total_persons > 0 else 0
                st.caption(f"{proportion_displaced:.1f}% of affected")
            
            with col3:
                st.metric("🏠 Persons in ECs", f"{total_inside:,.0f}")
                proportion_inside = (total_inside / total_displaced * 100) if total_displaced > 0 else 0
                st.caption(f"{proportion_inside:.1f}% of displaced")
            
            with col4:
                st.metric("🏕️ Number of ECs", f"{total_ecs:,.0f}")
                avg_per_ec = total_inside / total_ecs if total_ecs > 0 else 0
                st.caption(f"{avg_per_ec:.0f} persons/EC avg")
            
            # Second row of metrics
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                if has_damaged_houses:
                    df_houses = transformed_tables['DAMAGED HOUSES']
                    df_houses_muni = df_houses[df_houses['Level'] == 'Municipality']
                    total_damaged = df_houses_muni['Grand_Total_Damaged'].sum()
                    st.metric("🏚️ Houses Damaged", f"{total_damaged:,.0f}")
                else:
                    st.metric("🏚️ Houses Damaged", "N/A")
                    st.caption("Extract data to view")
            
            with col2:
                if has_casualties:
                    df_cas = transformed_tables['CASUALTIES']
                    # Casualties table doesn't have Level column - just use it as-is
                    df_cas_muni = df_cas.copy()
                    
                    if 'Casualty_Type' in df_cas_muni.columns and 'QTY' in df_cas_muni.columns:
                        total_casualties = df_cas_muni['QTY'].sum()
                        st.metric("⚠️ Total Casualties", f"{int(total_casualties):,}")
                        
                        dead = df_cas_muni[df_cas_muni['Casualty_Type'].str.upper().str.contains('DEAD', na=False)]['QTY'].sum()
                        injured = df_cas_muni[df_cas_muni['Casualty_Type'].str.upper().str.contains('INJURED', na=False)]['QTY'].sum()
                        missing = df_cas_muni[df_cas_muni['Casualty_Type'].str.upper().str.contains('MISSING', na=False)]['QTY'].sum()
                        st.caption(f"Dead: {int(dead)}, Injured: {int(injured)}, Missing: {int(missing)}")
                    else:
                        st.metric("⚠️ Total Casualties", "N/A")
                else:
                    st.metric("⚠️ Total Casualties", "N/A")
                    st.caption("Extract data to view")
            
            with col3:
                proportion_outside = (total_outside / total_displaced * 100) if total_displaced > 0 else 0
                st.metric("🌳 Outside ECs", f"{total_outside:,.0f}")
                st.caption(f"{proportion_outside:.1f}% of displaced")
            
            with col4:
                if has_assistance:
                    df_assist = transformed_tables['ASSISTANCE TO FAMILIES']
                    df_assist_muni = df_assist[df_assist['Level'] == 'Municipality']
                    total_assisted = df_assist_muni['Families_Assisted'].sum()
                    st.metric("🤝 Families Assisted", f"{total_assisted:,.0f}")
                else:
                    st.metric("🤝 Families Assisted", "N/A")
                    st.caption("Extract data to view")
            
            st.markdown("---")
            
            # ====================================================================
            # PREPARE CONSOLIDATED DATASET FOR INSIGHTS
            # ====================================================================
            
            # Start with affected population
            df_insights = df_muni.copy()
            
            # Merge damaged houses if available
            if has_damaged_houses:
                df_houses = transformed_tables['DAMAGED HOUSES']
                df_houses_muni = df_houses[df_houses['Level'] == 'Municipality'][
                    ['Region', 'Province', 'Municipality', 'Totally_Damaged', 'Partially_Damaged', 'Grand_Total_Damaged']
                ].copy()
                df_insights = df_insights.merge(
                    df_houses_muni,
                    on=['Region', 'Province', 'Municipality'],
                    how='left'
                )
                df_insights['Totally_Damaged'] = df_insights['Totally_Damaged'].fillna(0)
                df_insights['Partially_Damaged'] = df_insights['Partially_Damaged'].fillna(0)
                df_insights['Grand_Total_Damaged'] = df_insights['Grand_Total_Damaged'].fillna(0)
            else:
                df_insights['Totally_Damaged'] = 0
                df_insights['Partially_Damaged'] = 0
                df_insights['Grand_Total_Damaged'] = 0
            
            # Merge assistance if available
            if has_assistance:
                df_assist = transformed_tables['ASSISTANCE TO FAMILIES']
                df_assist_muni = df_assist[df_assist['Level'] == 'Municipality'][
                    ['Region', 'Province', 'Municipality', 'Families_Requiring_Assistance', 'Families_Assisted']
                ].copy()
                df_insights = df_insights.merge(
                    df_assist_muni,
                    on=['Region', 'Province', 'Municipality'],
                    how='left'
                )
                df_insights['Families_Requiring_Assistance'] = df_insights['Families_Requiring_Assistance'].fillna(0)
                df_insights['Families_Assisted'] = df_insights['Families_Assisted'].fillna(0)
            else:
                df_insights['Families_Requiring_Assistance'] = 0
                df_insights['Families_Assisted'] = 0
            
            # Calculate percentage assisted
            df_insights['Percent_Assisted'] = np.where(
                df_insights['Families_Requiring_Assistance'] > 0,
                (df_insights['Families_Assisted'] / df_insights['Families_Requiring_Assistance'] * 100),
                0
            )
            
            # Add flooding status if related incidents available
            if has_related_incidents:
                df_incidents = transformed_tables['RELATED INCIDENTS']
                df_incidents_muni = df_incidents[df_incidents['Level'] == 'Municipality']
                
                # Check if municipality has flooding incidents
                flooded_munis = df_incidents_muni[
                    df_incidents_muni['Type_of_Incident'].str.contains('flood', case=False, na=False)
                ]['Municipality'].unique()
                
                df_insights['Still_Flooded'] = df_insights['Municipality'].isin(flooded_munis)
            else:
                df_insights['Still_Flooded'] = False
            
            # Add placeholder data for roads/lifelines (these would come from other tables if available)
            df_insights['Roads_Not_Passable'] = 0  # Placeholder
            df_insights['Roads_Passable'] = 0  # Placeholder
            df_insights['Water_Interrupted'] = 0  # Placeholder
            df_insights['Water_Restored'] = 0  # Placeholder
            df_insights['Power_Interrupted'] = 0  # Placeholder
            df_insights['Power_Restored'] = 0  # Placeholder
            df_insights['Comms_Down'] = 0  # Placeholder
            df_insights['Comms_Restored'] = 0  # Placeholder
            
            # ====================================================================
            # SECTION 1: ASSISTANCE GAP ANALYSIS
            # ====================================================================
            if has_assistance:
                with st.container(border=True):
                    st.subheader("🎯 Assistance Gap Analysis")
                    st.caption("Identifies municipalities with high displacement but low assistance coverage")
                    
                    # Calculate gap metrics
                    df_gaps = df_insights.copy()
                    df_gaps['Total_Displaced'] = df_gaps['Inside_Persons_CUM'] + df_gaps['Outside_Persons_CUM']

                    # Gap score: families needing assistance vs families assisted
                    df_gaps['Gap_Score'] = np.where(
                        df_gaps['Families_Requiring_Assistance'] == 0,
                        0,
                        np.where(
                            df_gaps['Families_Assisted'] > 0,
                            df_gaps['Families_Requiring_Assistance'] / df_gaps['Families_Assisted'],
                            999
                        )
                    )

                    # ADD MAGNITUDE WEIGHTING:
                    # Normalize Total_Displaced to 0-100 scale
                    scaler = MinMaxScaler(feature_range=(0, 100))
                    df_gaps['Displacement_Magnitude'] = scaler.fit_transform(
                        df_gaps[['Total_Displaced']]
                    )

                    # Weighted Gap Score = Gap_Score * Displacement_Magnitude weight
                    # Higher displacement = higher priority even with same gap ratio
                    df_gaps['Weighted_Gap_Score'] = df_gaps['Gap_Score'] * (1 + df_gaps['Displacement_Magnitude'] / 100)

                    # Filter to only municipalities with displaced people
                    df_gaps = df_gaps[df_gaps['Total_Displaced'] > 0].copy()
                    
                    # Overview metrics
                    col1, col2, col3, col4 = st.columns(4)
                    
                    with col1:
                        critical_gaps = len(df_gaps[df_gaps['Gap_Score'] >= 10])
                        st.metric("🚨 Critical Gaps", f"{critical_gaps}")
                        st.caption("Gap Score ≥ 10")
                    
                    with col2:
                        avg_coverage = df_gaps['Percent_Assisted'].mean()
                        st.metric("📊 Avg Coverage", f"{avg_coverage:.1f}%")
                    
                    with col3:
                        total_gap = df_gaps['Families_Requiring_Assistance'].sum() - df_gaps['Families_Assisted'].sum()
                        st.metric("⚠️ Total Gap", f"{total_gap:,.0f}")
                        st.caption("Families still needing aid")
                    
                    with col4:
                        unassisted = len(df_gaps[df_gaps['Families_Assisted'] == 0])
                        st.metric("🔴 Zero Assistance", f"{unassisted}")
                        st.caption("Municipalities")
                    
                    st.markdown("---")
                    
                    with st.expander("ℹ️ How Gap Score Works", expanded=False):
                        st.markdown("""
                        **Gap Score = Total Displaced / Families Assisted**
                        - Higher score = worse gap (more people displaced per family assisted)
                        - Score of 999 = No assistance provided yet (critical priority)
                        - Lower scores indicate better coverage relative to displacement
                        """)
                    
                    # Top municipalities table
                    st.markdown("**🚨 Priority Municipalities - Highest Assistance Gaps**")
                    
                    with st.expander("📋 View Top 10 Gaps", expanded=True):
                        top_gaps = df_gaps.nlargest(10, 'Weighted_Gap_Score')[
                            ['Region', 'Province', 'Municipality', 'Total_Displaced', 
                            'Families_Requiring_Assistance', 'Families_Assisted', 
                            'Percent_Assisted', 'Gap_Score', 'Weighted_Gap_Score']
                        ].copy()
                        
                        top_gaps_formatted = format_dataframe_for_display(top_gaps)
                        st.dataframe(top_gaps_formatted, use_container_width=True, hide_index=True)
            
            # ====================================================================
            # SECTION 2: ACCESS & ISOLATION RISK
            # ====================================================================
            # Note: This section requires roads/bridges data which may not be available
            # Keeping the structure but it will show zeros until that data is extracted
            
            with st.container(border=True):
                st.subheader("🚧 Access & Isolation Risk")
                st.caption("Municipalities cut off from aid routes by impassable roads and flooding")
                
                # Calculate isolation score
                df_isolation = df_insights.copy()
                df_isolation['Road_Blockage'] = (df_isolation['Roads_Not_Passable'] > 0).astype(int)
                df_isolation['High_Displacement'] = (df_isolation['Inside_Persons_CUM'] + df_isolation['Outside_Persons_CUM'] > 100).astype(int)
                df_isolation['Isolation_Score'] = (
                    df_isolation['Road_Blockage'] + 
                    df_isolation['Still_Flooded'].astype(int) + 
                    df_isolation['High_Displacement']
                )
                
                # Filter to isolated areas
                df_isolated = df_isolation[df_isolation['Isolation_Score'] >= 2].copy()
                
                # Overview metrics
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    isolated_count = len(df_isolated)
                    st.metric("🚧 Isolated Municipalities", f"{isolated_count}")
                    st.caption("2+ isolation factors")
                
                with col2:
                    road_blocks = df_isolation['Roads_Not_Passable'].sum()
                    st.metric("🛑 Road Blockages", f"{int(road_blocks)}")
                
                with col3:
                    flooded_count = df_isolation['Still_Flooded'].sum()
                    st.metric("🌊 Still Flooded", f"{int(flooded_count)}")
                
                with col4:
                    people_isolated = df_isolated['Affected_Persons'].sum()
                    st.metric("👥 People in Isolated Areas", f"{people_isolated:,.0f}")
                
                st.markdown("---")
                
                with st.expander("ℹ️ How Isolation Score Works", expanded=False):
                    st.markdown("""
                    **Isolation Score = Road Blockage + Still Flooded + High Displacement**
                    - 3 = Critical isolation (all three factors present)
                    - 2 = High isolation risk
                    - 1 = One isolation factor
                    - 0 = No isolation issues
                    """)
                
                if len(df_isolated) > 0:
                    st.markdown("**🚨 Most Isolated Municipalities**")
                    
                    with st.expander("📋 View Rankings", expanded=True):
                        top_isolated = df_isolated.nlargest(15, 'Isolation_Score')[
                            ['Region', 'Province', 'Municipality', 'Affected_Persons',
                             'Road_Blockage', 'Still_Flooded', 'High_Displacement', 'Isolation_Score']
                        ].copy()
                        
                        top_isolated_formatted = format_dataframe_for_display(top_isolated)
                        st.dataframe(top_isolated_formatted, use_container_width=True, hide_index=True)
                else:
                    st.info("No municipalities currently meeting isolation criteria (2+ factors)")
            
            # ====================================================================
            # SECTION 3: LIFELINES COMPOUND FAILURE
            # ====================================================================
            # Note: Requires lifelines data
            
            with st.container(border=True):
                st.subheader("⚡ Lifelines Compound Failure")
                st.caption("Municipalities with multiple utility failures (Water + Power + Communications)")
                
                # Calculate lifelines failures
                df_lifelines = df_insights.copy()
                df_lifelines['Water_Down'] = (df_lifelines['Water_Interrupted'] > 0).astype(int)
                df_lifelines['Power_Down'] = (df_lifelines['Power_Interrupted'] > 0).astype(int)
                df_lifelines['Comms_Down_Flag'] = (df_lifelines['Comms_Down'] > 0).astype(int)
                df_lifelines['Lifelines_Failed'] = (
                    df_lifelines['Water_Down'] + 
                    df_lifelines['Power_Down'] + 
                    df_lifelines['Comms_Down_Flag']
                )
                
                # Filter to compound failures
                df_compound = df_lifelines[df_lifelines['Lifelines_Failed'] >= 2].copy()
                
                # Overview metrics
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    three_failures = len(df_lifelines[df_lifelines['Lifelines_Failed'] == 3])
                    st.metric("🔴 All Utilities Failed", f"{three_failures}")
                    st.caption("Water + Power + Comms")
                
                with col2:
                    two_failures = len(df_lifelines[df_lifelines['Lifelines_Failed'] == 2])
                    st.metric("🟠 Two Utilities Failed", f"{two_failures}")
                
                with col3:
                    water_down = df_lifelines['Water_Down'].sum()
                    st.metric("💧 Water Interrupted", f"{int(water_down)}")
                    st.caption("Municipalities")
                
                with col4:
                    people_affected = df_compound['Affected_Persons'].sum()
                    st.metric("👥 People in 2+ Failures", f"{people_affected:,.0f}")
                
                st.markdown("---")
                
                with st.expander("ℹ️ How Lifelines Failure Score Works", expanded=False):
                    st.markdown("""
                    **Lifelines Failed = Water Down + Power Down + Comms Down**
                    - 3 = All utilities failed (most critical)
                    - 2 = Two utilities failed (compound failure)
                    - 1 = One utility failed
                    - **Water is weighted as primary concern**, as it's essential for health and survival
                    """)
                
                if len(df_compound) > 0:
                    st.markdown("**🚨 Municipalities with Compound Failures**")
                    
                    with st.expander("📋 View Details", expanded=True):
                        top_lifelines = df_compound.nlargest(15, 'Lifelines_Failed')[
                            ['Region', 'Province', 'Municipality', 'Affected_Persons',
                             'Water_Down', 'Power_Down', 'Comms_Down_Flag', 'Lifelines_Failed']
                        ].copy()
                        
                        # Rename for display
                        top_lifelines = top_lifelines.rename(columns={
                            'Water_Down': 'Water Failed',
                            'Power_Down': 'Power Failed',
                            'Comms_Down_Flag': 'Comms Failed'
                        })
                        
                        top_lifelines_formatted = format_dataframe_for_display(top_lifelines)
                        st.dataframe(top_lifelines_formatted, use_container_width=True, hide_index=True)
                else:
                    st.info("No municipalities currently experiencing compound utility failures")
            
            # ====================================================================
            # SECTION 4: RECOVERY PROGRESS TRACKING
            # ====================================================================
            
            with st.container(border=True):
                st.subheader("📈 Recovery Progress Tracking")
                st.caption("Tracking restoration of services and flood recession")
                
                # Calculate recovery rates
                df_recovery = df_insights.copy()
                
                # Water recovery rate
                df_recovery['Water_Total'] = df_recovery['Water_Interrupted'] + df_recovery['Water_Restored']
                df_recovery['Water_Recovery_Rate'] = np.where(
                    df_recovery['Water_Total'] > 0,
                    (df_recovery['Water_Restored'] / df_recovery['Water_Total'] * 100),
                    100  # If no interruption, 100% "recovered"
                )
                
                # Power recovery rate
                df_recovery['Power_Total'] = df_recovery['Power_Interrupted'] + df_recovery['Power_Restored']
                df_recovery['Power_Recovery_Rate'] = np.where(
                    df_recovery['Power_Total'] > 0,
                    (df_recovery['Power_Restored'] / df_recovery['Power_Total'] * 100),
                    100
                )
                
                # Comms recovery rate
                df_recovery['Comms_Total'] = df_recovery['Comms_Down'] + df_recovery['Comms_Restored']
                df_recovery['Comms_Recovery_Rate'] = np.where(
                    df_recovery['Comms_Total'] > 0,
                    (df_recovery['Comms_Restored'] / df_recovery['Comms_Total'] * 100),
                    100
                )
                
                # Flood recession (inverse of still flooded)
                flooded_now = df_recovery['Still_Flooded'].sum()
                total_munis = len(df_recovery)
                flood_recovery_rate = ((total_munis - flooded_now) / total_munis * 100) if total_munis > 0 else 100
                
                # Stagnation: no recovery progress
                df_recovery['No_Water_Recovery'] = ((df_recovery['Water_Interrupted'] > 0) & (df_recovery['Water_Restored'] == 0)).astype(int)
                df_recovery['No_Power_Recovery'] = ((df_recovery['Power_Interrupted'] > 0) & (df_recovery['Power_Restored'] == 0)).astype(int)
                df_recovery['Stagnation_Score'] = (
                    df_recovery['No_Water_Recovery'] + 
                    df_recovery['No_Power_Recovery'] + 
                    df_recovery['Still_Flooded'].astype(int)
                )
                
                # Overview metrics - Progress bars
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown("**💧 Water Recovery**")
                    avg_water = df_recovery['Water_Recovery_Rate'].mean()
                    st.progress(avg_water / 100)
                    st.caption(f"{avg_water:.1f}% restored")
                    
                    st.markdown("**⚡ Power Recovery**")
                    avg_power = df_recovery['Power_Recovery_Rate'].mean()
                    st.progress(avg_power / 100)
                    st.caption(f"{avg_power:.1f}% restored")
                
                with col2:
                    st.markdown("**📡 Communications Recovery**")
                    avg_comms = df_recovery['Comms_Recovery_Rate'].mean()
                    st.progress(avg_comms / 100)
                    st.caption(f"{avg_comms:.1f}% restored")
                    
                    st.markdown("**🌊 Flood Recession**")
                    st.progress(flood_recovery_rate / 100)
                    st.caption(f"{flood_recovery_rate:.1f}% areas cleared")
                
                st.markdown("---")
                
                with st.expander("ℹ️ How Recovery Rates Work", expanded=False):
                    st.markdown("""
                    **Recovery Rate = (Restored / (Interrupted + Restored)) × 100%**
                    - 100% = Full recovery (all services restored)
                    - 50-99% = Partial recovery in progress
                    - 0-49% = Early recovery stage
                    - **Stagnation** = No restoration progress (interrupted but nothing restored)
                    """)
                
                # Stagnation alert
                stagnant = df_recovery[df_recovery['Stagnation_Score'] >= 2].copy()
                if len(stagnant) > 0:
                    st.markdown("**⚠️ Stagnation Alert - No Recovery Progress**")
                    
                    with st.expander(f"📋 View {len(stagnant)} Stagnant Municipalities", expanded=True):
                        stagnant_display = stagnant.nlargest(15, 'Stagnation_Score')[
                            ['Region', 'Province', 'Municipality', 'Affected_Persons',
                             'No_Water_Recovery', 'No_Power_Recovery', 'Still_Flooded', 'Stagnation_Score']
                        ].copy()
                        
                        # Rename for display
                        stagnant_display = stagnant_display.rename(columns={
                            'No_Water_Recovery': 'Water Stagnant',
                            'No_Power_Recovery': 'Power Stagnant'
                        })
                        
                        stagnant_formatted = format_dataframe_for_display(stagnant_display)
                        st.dataframe(stagnant_formatted, use_container_width=True, hide_index=True)
            
            # ====================================================================
            # SECTION 5: VULNERABILITY HOTSPOTS
            # ====================================================================
            
            with st.container(border=True):
                st.subheader("🎯 Vulnerability Hotspots")
                st.caption("Multi-dimensional compound impact scoring")
                
                df_vuln = df_insights.copy()
                
                # Calculate Housing Vulnerability Components
                
                # Metric 1: Displacement Rate (% of affected persons who are displaced)
                df_vuln['Total_Displaced'] = df_vuln['Inside_Persons_CUM'] + df_vuln['Outside_Persons_CUM']
                df_vuln['Displacement_Rate'] = np.where(
                    df_vuln['Affected_Persons'] > 0,
                    (df_vuln['Total_Displaced'] / df_vuln['Affected_Persons'] * 100),
                    0
                )
                
                # Metric 2: Housing Damage Rate (damaged houses per affected family)
                df_vuln['Housing_Damage_Rate'] = np.where(
                    df_vuln['Affected_Families'] > 0,
                    (df_vuln['Grand_Total_Damaged'] / df_vuln['Affected_Families'] * 100),
                    0
                )
                
                # Metric 3: Housing Severity (weighted damage per affected family)
                df_vuln['Housing_Severity'] = np.where(
                    df_vuln['Affected_Families'] > 0,
                    ((df_vuln['Totally_Damaged'] * 2 + df_vuln['Partially_Damaged'] * 1) / df_vuln['Affected_Families'] * 100),
                    0
                )
                
                # Combined Housing Score
                df_vuln['Housing_Score'] = (
                    df_vuln['Displacement_Rate'] * 0.4 +
                    df_vuln['Housing_Damage_Rate'] * 0.3 +
                    df_vuln['Housing_Severity'] * 0.3
                ).clip(0, 100)
                
                # Normalize scores to 0-100 scale for consistency
                from sklearn.preprocessing import MinMaxScaler
                scaler = MinMaxScaler(feature_range=(0, 100))
                
                # Displacement Score (already 0-100 percentage)
                df_vuln['Displacement_Score'] = df_vuln['Displacement_Rate'].clip(0, 100)
                
                # Lifeline Score
                lifeline_sum = (df_vuln['Water_Interrupted'].fillna(0) + 
                               df_vuln['Power_Interrupted'].fillna(0) + 
                               df_vuln['Comms_Down'].fillna(0))
                
                if lifeline_sum.max() > 0:
                    df_vuln['Lifeline_Score'] = scaler.fit_transform(
                        lifeline_sum.values.reshape(-1, 1)
                    ).flatten()
                else:
                    df_vuln['Lifeline_Score'] = 0
                
                # Compound Vulnerability Index (weighted average)
                df_vuln['Vulnerability_Index'] = (
                    df_vuln['Displacement_Score'] * 0.4 +
                    df_vuln['Housing_Score'] * 0.3 +
                    df_vuln['Lifeline_Score'] * 0.3
                ).round(1)

                # Normalize Affected_Persons to 0-100 scale
                scaler_vuln = MinMaxScaler(feature_range=(0, 100))
                df_vuln['Impact_Magnitude'] = scaler_vuln.fit_transform(
                    df_vuln[['Affected_Persons']]
                )

                # Weighted Vulnerability = Vulnerability_Index * magnitude weight
                # Larger affected population = higher priority
                df_vuln['Weighted_Vulnerability'] = df_vuln['Vulnerability_Index'] * (1 + df_vuln['Impact_Magnitude'] / 100)
                
                # Overview metrics
                col1, col2, col3, col4 = st.columns(4)

                with col1:
                    critical_count = len(df_vuln[df_vuln['Weighted_Vulnerability'] >= 75])  # Changed
                    st.metric("🔴 Critical Hotspots", f"{critical_count}")
                    st.caption("Weighted Vuln ≥ 75")
                
                with col2:
                    high_count = len(df_vuln[(df_vuln['Vulnerability_Index'] >= 50) & (df_vuln['Vulnerability_Index'] < 75)])
                    st.metric("🟠 High Vulnerability", f"{high_count}")
                    st.caption("Vulnerability 50-74")
                
                with col3:
                    avg_vulnerability = df_vuln['Vulnerability_Index'].mean()
                    st.metric("📊 Average Index", f"{avg_vulnerability:.1f}")
                
                with col4:
                    avg_displacement = df_vuln['Displacement_Rate'].mean()
                    st.metric("👥 Avg Displacement Rate", f"{avg_displacement:.1f}%")
                    st.caption("% of affected persons")
                
                st.markdown("---")
                
                with st.expander("ℹ️ How Vulnerability Index Works", expanded=False):
                    st.markdown("""
                    **Vulnerability Index = Weighted Average of:**
                    - **Displacement Score (40%)**: % of affected persons who are displaced
                    - **Housing Score (30%)**: Combines three metrics:
                      - Displacement Rate (40%)
                      - Housing Damage Rate (30%): Damaged houses / Affected families
                      - Housing Severity (30%): (Totally × 2 + Partially × 1) / Affected families
                    - **Lifeline Score (30%)**: Normalized utility failures (Water + Power + Comms)
                    
                    **Index Scale:**
                    - 75-100 = Critical vulnerability (red flag)
                    - 50-74 = High vulnerability (needs attention)
                    - 0-49 = Moderate/low vulnerability
                    
                    **Housing metrics use affected families as denominator, not total population.**
                    """)
                
                # Top vulnerability hotspots
                st.markdown("**🚨 Top 15 Vulnerability Hotspots**")

                with st.expander("📋 View Rankings", expanded=True):
                    hotspots = df_vuln.nlargest(15, 'Weighted_Vulnerability')[  # Changed
                        ['Region', 'Province', 'Municipality', 'Affected_Persons', 'Total_Displaced',
                        'Displacement_Score', 'Housing_Score', 'Lifeline_Score', 
                        'Vulnerability_Index', 'Weighted_Vulnerability']  # Added both scores
                    ].copy()
                    
                    hotspots_formatted = format_dataframe_for_display(hotspots)
                    st.dataframe(hotspots_formatted, use_container_width=True, hide_index=True)

# =============================================================================
# HELP
# =============================================================================
elif page == "Help":
    st.title("❓ Help & Frequently Asked Questions")
    st.markdown("---")
    
    st.markdown("### Getting Started")
    
    with st.expander("How do I upload a PDF?", expanded=True):
        st.markdown("""
        1. Click **Load PDF** in the sidebar menu
        2. Choose either:
           - **Upload**: Select a file from your computer
           - **URL**: Paste a link to a PDF online
        3. Wait ~2 seconds for automatic summary extraction
        4. View summaries or extract detailed tables
        """)
    
    with st.expander("What does a compatible report look like?"):
        # Placeholder for report example image
        # st.image("assets/report_example.png", caption="Example: NDRRMC DROMIC report format")
        st.markdown("""
        **Compatible format:**
        - NDRRMC DROMIC Situational Reports
        - Landscape-oriented table pages
        - Clear headers with Region/Province/Municipality hierarchy
        - Tabular data (not narrative text)
        - Machine-readable PDF (not scanned images)
        """)
    
    with st.expander("How long does extraction take?"):
        st.markdown("""
        - **Summary tables:** ~2 seconds (automatic)
        - **Detailed tables:** ~1-2 seconds per page
        
        **Examples:**
        - 100-page report: ~2-3 minutes
        - 300-page report: ~6-10 minutes
        - 600-page report: ~12-20 minutes
        """)
    
    with st.expander("What tables are supported?"):
        st.markdown("""
        **Demographics (3 tables):**
        - Affected Population
        - Casualties  
        - Damaged Houses
        
        **Infrastructure (4 tables):**
        - Roads & Bridges
        - Power Supply
        - Water Supply
        - Communication Lines
        
        **Sectoral (4 tables):**
        - Damage to Agriculture
        - Damage to Infrastructure
        - Assistance to Families
        - Assistance to LGUs
        """)
    
    with st.expander("What format are the downloads?"):
        st.markdown("""
        All tables are provided as CSV files with:
        - **Location hierarchy:** Region, Province, Municipality, Barangay columns
        - **Clean data:** Standardized formatting, no totals/subtotals
        - **Ready for analysis:** Import directly into Excel, Power BI, Python, R
        """)
    
    st.markdown("---")
    st.markdown("### Troubleshooting")
    
    with st.expander("My PDF isn't working"):
        st.markdown("""
        **Requirements:**
        - NDRRMC DROMIC format
        - Landscape-oriented tables
        - Machine-readable (not scanned images)
        - From 2024-2025
        
        **Common issues:**
        - Portrait-only PDFs won't extract properly
        - Very old reports may have different formatting
        - Scanned/image PDFs require OCR (not supported)
        """)
    
    with st.expander("Data looks incomplete or wrong"):
        st.markdown("""
        - Compare with source PDF to verify
        - Some reports have non-standard table formatting
        - "No breakdown" means municipality-level data only
        - Check the Summary Dashboard first to verify basic extraction worked
        """)
    
    with st.expander("App is slow or timing out"):
        st.markdown("""
        - Large PDFs (500+ pages) take 15-25 minutes
        - Streamlit free tier has limited resources
        - Try extracting fewer tables at once
        - Refresh the page and try again if stuck
        """)

# =============================================================================
# DROMIC EXTRACT PAGE
# =============================================================================
elif page == "DROMIC Extract":
    st.title("🔍 DROMIC Data Extraction")
    st.caption("Extract and transform DROMIC tables with custom text patterns")
    st.markdown("---")
    
    # Check if PDF loaded
    if 'pdf_loaded' not in st.session_state or not st.session_state.get('pdf_loaded'):
        st.warning("⚠️ Please load a PDF first")
        if st.button("📁 Go to Load PDF", type="primary"):
            st.session_state.current_page = "Load PDF"
            st.rerun()
    else:
        st.info(f"📄 **Loaded:** {st.session_state.get('pdf_name', 'Unknown')}")
        
        st.markdown("### Configure Extraction")
        
        # Text pattern inputs
        col1, col2 = st.columns(2)
        
        with col1:
            page_text = st.text_input(
                "Page Text Pattern",
                value="NO. OF DAMAGED HOUSES",
                help="Text that must appear on the page to extract tables"
            )
        
        with col2:
            table_text_input = st.text_input(
                "Table Text Pattern (comma-separated)",
                value="NO. OF DAMAGED HOUSES, Total",
                help="Text patterns that must appear in the table header"
            )
        
        # Convert table text to set
        table_text = set([t.strip() for t in table_text_input.split(",")])
        
        st.markdown("---")
        
        # Extract button
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            if st.button("🔍 Extract & Transform", type="primary", use_container_width=True):
                pdf_path = st.session_state['temp_pdf_path']
                
                with st.spinner("🔄 Extracting DROMIC data..."):
                    try:
                        from dromic_extractor import extract_dromic_table
                        
                        # Extract with custom patterns
                        df_dromic = extract_dromic_table(
                            pdf_path,
                            page_text=page_text,
                            table_text=table_text
                        )
                        
                        # Save to session state
                        st.session_state['dromic_data'] = df_dromic
                        st.session_state['dromic_extracted'] = True
                        
                        st.success(f"✅ Extracted {len(df_dromic)} rows!")
                        
                    except Exception as e:
                        st.error(f"❌ Extraction failed: {str(e)}")
                        st.exception(e)
        
        # MOVED OUTSIDE: Show preview and download if data exists
        st.markdown("---")
        if 'dromic_data' in st.session_state and st.session_state.get('dromic_extracted'):
            df_dromic = st.session_state['dromic_data']
            
            st.markdown("### Preview (first 20 rows)")
            st.dataframe(df_dromic.head(20), use_container_width=True)
            
            # Download button
            st.markdown("---")
            from datetime import datetime
            filename = st.session_state.get('pdf_name', 'DROMIC_Extract')
            filename_clean = filename.replace('.pdf', '').replace(' ', '_')
            date_str = datetime.now().strftime("%Y%m%d")
            csv_filename = f"{filename_clean}_{date_str}.csv"

            col1, col2, col3 = st.columns([1, 2, 1])
            with col2:
                csv_data = df_dromic.to_csv(index=False).encode('utf-8')
                st.download_button(
                    "📥 Download CSV",
                    data=csv_data,
                    file_name=csv_filename,
                    mime="text/csv",
                    type="primary",
                    use_container_width=True
                )
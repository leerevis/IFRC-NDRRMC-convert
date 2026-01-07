# pdf_extractor.py - NDRRMC PDF Table Extractor using Tabula (TWO-STAGE with LATTICE MODE)
# Stage 1: Quick summaries from portrait pages (7 seconds)
# Stage 2: Detailed tables from landscape pages (11 minutes)

import tabula
import pandas as pd
import PyPDF2

# =============================================================================
# STAGE 1: SUMMARY EXTRACTION (Portrait Pages)
# =============================================================================

def extract_summary_tables(pdf_source):
    """
    Stage 1: Quick summary extraction using pdfplumber (more reliable for summaries)
    ONLY extracts from PORTRAIT pages to avoid misidentifying detailed tables
    """
    import pdfplumber
    
    summaries = {}
    
    with pdfplumber.open(pdf_source) as pdf:
        # Extract from first 10 pages (portrait summary pages)
        for page_num in range(min(10, len(pdf.pages))):
            page = pdf.pages[page_num]
            
            # Step 1: Check if page is portrait (width <= height)
            width = float(page.width)
            height = float(page.height)
            is_portrait = width <= height
            
            if not is_portrait:
                continue  # Skip landscape pages
            
            # Step 2: Extract tables from portrait page
            tables = page.extract_tables()
            
            for table in tables:
                if not table or len(table) < 2:
                    continue
                
                # Convert to DataFrame
                df = pd.DataFrame(table[1:], columns=table[0])
                
                # Step 3: Check for each summary table type
                columns_text = ' '.join([str(col).lower() for col in df.columns if col])
                
                # Check for Affected Population
                if 'affected' in columns_text and 'inside' in columns_text and 'outside' in columns_text:
                    summaries['AFFECTED POPULATION'] = df
                
                # Check for Damaged Houses
                if (('partially' in columns_text and 'totally' in columns_text and 'amount' in columns_text) and
                    'agriculture' not in columns_text and 'farmer' not in columns_text and 'crop' not in columns_text):
                    summaries['DAMAGED HOUSES'] = df
                
                # Check for Casualties
                if 'validated' in columns_text and 'validation' in columns_text:
                    summaries['CASUALTIES'] = df
                
                # Check first row for sub-headers (for Power/Water detection)
                if len(df) > 0:
                    first_row_text = ' '.join([str(val).lower() for val in df.iloc[0].tolist() if val and not pd.isna(val)])
                else:
                    first_row_text = ""

                # Check for Roads and Bridges summary
                if 'passable' in columns_text and 'ROADS AND BRIDGES' not in summaries:
                    summaries['ROADS AND BRIDGES'] = df

                # Check for Power/Water summaries (check first row for INTERRUPTED/RESTORED)
                if 'interrupted' in first_row_text and 'restored' in first_row_text:
                    if 'POWER' not in summaries:
                        summaries['POWER'] = df
                    elif 'WATER SUPPLY' not in summaries:
                        summaries['WATER SUPPLY'] = df

                # Check for Communications summary (has 'communication' and checks first row)
                if 'communication' in columns_text or 'area' in columns_text:
                    if len(df) > 0:
                        first_row_text = ' '.join([str(val).lower() for val in df.iloc[0].tolist() if val and not pd.isna(val)])
                        
                        if 'without communication' in first_row_text or 'restored communication' in first_row_text:
                            summaries['COMMUNICATION LINES'] = df

                # Check for Agriculture summary
                if (('agriculture' in columns_text or 'farmer' in columns_text or 'fisherfolk' in columns_text) and
                    'families' not in columns_text and 'persons' not in columns_text):
                    summaries['DAMAGE TO AGRICULTURE'] = df

                # Check for Infrastructure summary
                if (('infrastructure' in columns_text and 'damage' in columns_text and 'cost' in columns_text) and
                    'families' not in columns_text and 'persons' not in columns_text):
                    summaries['DAMAGE TO INFRASTRUCTURE'] = df

                # Check for Assistance to Families summary
                if (('families' in columns_text and 'assistance' in columns_text and 'requiring' in columns_text) and
                    'affected' not in columns_text and 'evacuation' not in columns_text):
                    summaries['ASSISTANCE TO FAMILIES'] = df

                # Check for Assistance to LGUs summary
                if (('lgus' in columns_text or ('cluster' in columns_text and 'assistance' in columns_text)) and
                    'families' not in columns_text and 'persons' not in columns_text):
                    summaries['ASSISTANCE TO LGUS'] = df

                # Check for Related Incidents summary
                if 'RELATED INCIDENTS' not in summaries:
                    first_col = str(df.columns[0]).strip().upper()
                    if first_col == 'REGION' and len(df.columns) >= 2:
                        # Check for incident keywords in other columns
                        incident_keywords = ['flooded', 'flood', 'fallen', 'debris', 'tree', 'landslide', 
                                           'maritime', 'storm surge', 'surge', 'drowning', 'wave swell', 
                                           'swell', 'overflow', 'structural fire', 'fire', 'oil leak', 
                                           'chemical leak', 'collapsed structure', 'collapse']
                        other_cols = ' '.join([str(col).lower() for col in df.columns[1:]])
                        
                        has_incident_keyword = any(keyword in other_cols for keyword in incident_keywords)
                        
                        if has_incident_keyword:
                            # Process the Related Incidents table
                            df_incidents = df.copy()
                            
                            # Check if row 0 contains sub-column names
                            first_data_row = df_incidents.iloc[0] if len(df_incidents) > 0 else None
                            has_subheaders = False
                            if first_data_row is not None:
                                first_cell = str(first_data_row.iloc[0]).strip().upper()
                                if first_cell == '' or first_cell == 'NAN' or first_cell == 'NONE':
                                    has_subheaders = True
                            
                            # Handle multi-level headers
                            if has_subheaders and len(df_incidents) > 0:
                                sub_headers = df_incidents.iloc[0].tolist()
                                new_columns = ['Region']
                                current_main_header = None
                                
                                for i, col in enumerate(df_incidents.columns[1:], 1):
                                    main_header = str(col).strip()
                                    sub_header = str(sub_headers[i]).strip()
                                    
                                    if main_header and main_header.upper() not in ['', 'NAN', 'NONE', 'UNNAMED']:
                                        current_main_header = main_header
                                    
                                    if sub_header and sub_header.upper() not in ['', 'NAN', 'NONE']:
                                        new_columns.append(f"{current_main_header} - {sub_header}")
                                    else:
                                        new_columns.append(current_main_header if current_main_header else main_header)
                                
                                df_incidents.columns = new_columns
                                df_incidents = df_incidents.iloc[1:].reset_index(drop=True)
                            else:
                                df_incidents.columns = ['Region'] + [str(col).strip() for col in df_incidents.columns[1:]]
                            
                            summaries['RELATED INCIDENTS'] = df_incidents

                # Check for Pre-Emptive Evacuation summary (process of elimination)
                # Only 3 columns: Region, Families, Persons
                if len(df.columns) == 3:
                    col0 = str(df.columns[0]).lower() if len(df.columns) > 0 else ''
                    col1 = str(df.columns[1]).lower() if len(df.columns) > 1 else ''
                    col2 = str(df.columns[2]).lower() if len(df.columns) > 2 else ''
                    
                    # Check: Column 0 has "region", Column 1 has "families", Column 2 has "persons"
                    if ('region' in col0 and 
                        'families' in col1 and 
                        'persons' in col2):
                        # Make sure it's not already identified as another type
                        if 'PRE-EMPTIVE EVACUATION' not in summaries:
                            summaries['PRE-EMPTIVE EVACUATION'] = df

    # Step 4: Clean summaries (OUTSIDE the loop!)
    cleaned_summaries = {}
    for table_type, df in summaries.items():
        cleaned_summaries[table_type] = clean_summary_table(df, table_type)
    
    return cleaned_summaries

def clean_summary_table(df, table_type):
    """
    Clean up summary tables for display
    WHY: Raw extraction has messy headers and unnamed columns
    """
    df = df.copy()
    
    if table_type == 'AFFECTED POPULATION':
        # Remove first TWO rows
        df = df.iloc[2:].reset_index(drop=True)
        
        # Set clean column names
        df.columns = [
            'Region',
            'Brgys',
            'Families',
            'Persons',
            'No. of ECs',
            'Inside Families',
            'Inside Persons',
            'Outside Families',
            'Outside Persons'
        ]
        
        # Remove TOTAL row (we'll calculate our own)
        df = df[df['Region'] != 'TOTAL'].reset_index(drop=True)
        
        # Convert numeric columns to integers
        numeric_cols = ['Brgys', 'Families', 'Persons', 'No. of ECs', 
                       'Inside Families', 'Inside Persons', 'Outside Families', 'Outside Persons']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0).astype(int)
        
        # Calculate totals
        total_row = pd.DataFrame([{
            'Region': '**TOTAL**',
            'Brgys': df['Brgys'].sum(),
            'Families': df['Families'].sum(),
            'Persons': df['Persons'].sum(),
            'No. of ECs': df['No. of ECs'].sum(),
            'Inside Families': df['Inside Families'].sum(),
            'Inside Persons': df['Inside Persons'].sum(),
            'Outside Families': df['Outside Families'].sum(),
            'Outside Persons': df['Outside Persons'].sum()
        }])
        
        # Append total at bottom
        df = pd.concat([df, total_row], ignore_index=True)
        
    elif table_type == 'DAMAGED HOUSES':
        df.columns = ['Region', 'Partially', 'Totally', 'Total', 'Amount (PHP)']
        
        # Remove GRAND TOTAL row
        df = df[df['Region'] != 'GRAND TOTAL'].reset_index(drop=True)
        
        # Convert to numeric
        for col in ['Partially', 'Totally', 'Total', 'Amount (PHP)']:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0).astype(int)
        
        # Calculate totals
        total_row = pd.DataFrame([{
            'Region': '**GRAND TOTAL**',
            'Partially': df['Partially'].sum(),
            'Totally': df['Totally'].sum(),
            'Total': df['Total'].sum(),
            'Amount (PHP)': df['Amount (PHP)'].sum()
        }])
        
        df = pd.concat([df, total_row], ignore_index=True)

    elif table_type == 'CASUALTIES':
        # Step 1: The first row contains sub-headers (dead, injured, missing)
        # We need to combine top-level headers with sub-headers
        
        # Step 2: Build proper column names
        new_columns = []
        sub_headers = df.iloc[0].tolist()  # First row has: dead, injured, missing, etc.
        
        for i, (col, sub) in enumerate(zip(df.columns, sub_headers)):
            if col == 'REGION' or pd.isna(col):
                if sub and not pd.isna(sub) and sub.lower() != 'none':
                    # This is under a category (VALIDATED, FOR VALIDATION, TOTAL REPORTED)
                    # Find which category by looking backwards
                    category = None
                    for j in range(i, -1, -1):
                        if df.columns[j] and not pd.isna(df.columns[j]) and df.columns[j] not in ['REGION']:
                            category = df.columns[j]
                            break
                    
                    if category:
                        new_columns.append(f"{category}_{sub}".replace(' ', '_'))
                    else:
                        new_columns.append(str(sub))
                else:
                    new_columns.append('Region' if i == 0 else f'Col_{i}')
            else:
                new_columns.append(col)
        
        # Step 3: Remove first row (sub-headers) and apply new column names
        df = df.iloc[1:].reset_index(drop=True)
        df.columns = new_columns[:len(df.columns)]
        
        # Step 4: Clean up column names
        df.columns = [
            'Region',
            'Validated_dead', 'Validated_injured', 'Validated_missing',
            'For_Validation_dead', 'For_Validation_injured', 'For_Validation_missing',
            'Total_dead', 'Total_injured', 'Total_missing'
        ]
        
        # Step 5: Remove TOTAL row
        df = df[~df['Region'].str.upper().str.contains('TOTAL', na=False)].reset_index(drop=True)
        
        # Step 6: Convert to numeric
        numeric_cols = [col for col in df.columns if col != 'Region']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0).astype(int)
        
        # Step 7: Calculate totals
        total_row = pd.DataFrame([{
            'Region': '**TOTAL**',
            'Validated_dead': df['Validated_dead'].sum(),
            'Validated_injured': df['Validated_injured'].sum(),
            'Validated_missing': df['Validated_missing'].sum(),
            'For_Validation_dead': df['For_Validation_dead'].sum(),
            'For_Validation_injured': df['For_Validation_injured'].sum(),
            'For_Validation_missing': df['For_Validation_missing'].sum(),
            'Total_dead': df['Total_dead'].sum(),
            'Total_injured': df['Total_injured'].sum(),
            'Total_missing': df['Total_missing'].sum()
        }])
        
        df = pd.concat([df, total_row], ignore_index=True)

    elif table_type == 'ROADS AND BRIDGES':
        # Step 1: Remove first row (sub-headers: ROADS, BRIDGES under NOT PASSABLE/PASSABLE)
        df = df.iloc[1:].reset_index(drop=True)
        
        # Step 2: Set column names
        df.columns = ['Region', 'Roads_Not_Passable', 'Bridges_Not_Passable', 'Roads_Passable', 'Bridges_Passable']
        
        # Step 3: Remove TOTAL row
        df = df[~df['Region'].str.upper().str.contains('TOTAL', na=False)].reset_index(drop=True)
        
        # Step 4: Convert to numeric
        numeric_cols = ['Roads_Not_Passable', 'Bridges_Not_Passable', 'Roads_Passable', 'Bridges_Passable']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0).astype(int)
        
        # Step 5: Calculate totals
        total_row = pd.DataFrame([{
            'Region': '**TOTAL**',
            'Roads_Not_Passable': df['Roads_Not_Passable'].sum(),
            'Bridges_Not_Passable': df['Bridges_Not_Passable'].sum(),
            'Roads_Passable': df['Roads_Passable'].sum(),
            'Bridges_Passable': df['Bridges_Passable'].sum()
        }])
        
        df = pd.concat([df, total_row], ignore_index=True)

    elif table_type == 'POWER':
        # Step 1: Remove first row (sub-headers: INTERRUPTED, RESTORED)
        df = df.iloc[1:].reset_index(drop=True)
        
        # Step 2: Set column names
        df.columns = ['Region', 'Interrupted', 'Restored']
        
        # Step 3: Remove TOTAL row
        df = df[~df['Region'].str.upper().str.contains('TOTAL', na=False)].reset_index(drop=True)
        
        # Step 4: Convert to numeric
        df['Interrupted'] = pd.to_numeric(df['Interrupted'].astype(str).str.replace(',', ''), errors='coerce').fillna(0).astype(int)
        df['Restored'] = pd.to_numeric(df['Restored'].astype(str).str.replace(',', ''), errors='coerce').fillna(0).astype(int)
        
        # Step 5: Calculate totals
        total_row = pd.DataFrame([{
            'Region': '**TOTAL**',
            'Interrupted': df['Interrupted'].sum(),
            'Restored': df['Restored'].sum()
        }])
        
        df = pd.concat([df, total_row], ignore_index=True)

    elif table_type == 'WATER SUPPLY':
        # Step 1: Remove first row (sub-headers: INTERRUPTED, RESTORED)
        df = df.iloc[1:].reset_index(drop=True)
        
        # Step 2: Set column names
        df.columns = ['Region', 'Interrupted', 'Restored']
        
        # Step 3: Remove TOTAL row
        df = df[~df['Region'].str.upper().str.contains('TOTAL', na=False)].reset_index(drop=True)
        
        # Step 4: Convert to numeric
        df['Interrupted'] = pd.to_numeric(df['Interrupted'].astype(str).str.replace(',', ''), errors='coerce').fillna(0).astype(int)
        df['Restored'] = pd.to_numeric(df['Restored'].astype(str).str.replace(',', ''), errors='coerce').fillna(0).astype(int)
        
        # Step 5: Calculate totals
        total_row = pd.DataFrame([{
            'Region': '**TOTAL**',
            'Interrupted': df['Interrupted'].sum(),
            'Restored': df['Restored'].sum()
        }])
        
        df = pd.concat([df, total_row], ignore_index=True)

    elif table_type == 'COMMUNICATION LINES':
        # Step 1: Remove first row (sub-headers)
        df = df.iloc[1:].reset_index(drop=True)
        
        # Step 2: Set column names
        df.columns = ['Region', 'Without_Communication', 'Restored_Communication']
        
        # Step 3: Remove TOTAL row
        df = df[~df['Region'].str.upper().str.contains('TOTAL', na=False)].reset_index(drop=True)
        
        # Step 4: Convert to numeric
        df['Without_Communication'] = pd.to_numeric(df['Without_Communication'].astype(str).str.replace(',', ''), errors='coerce').fillna(0).astype(int)
        df['Restored_Communication'] = pd.to_numeric(df['Restored_Communication'].astype(str).str.replace(',', ''), errors='coerce').fillna(0).astype(int)
        
        # Step 5: Calculate totals
        total_row = pd.DataFrame([{
            'Region': '**TOTAL**',
            'Without_Communication': df['Without_Communication'].sum(),
            'Restored_Communication': df['Restored_Communication'].sum()
        }])
        
        df = pd.concat([df, total_row], ignore_index=True)
    
    elif table_type == 'DAMAGE TO AGRICULTURE':
        # Step 1: Remove first row (sub-headers)
        df = df.iloc[1:].reset_index(drop=True)
        
        # Step 2: Set column names
        df.columns = [
            'Region',
            'Farmers_Affected',
            'Crop_Area_Totally_Damaged',
            'Crop_Area_Partially_Damaged',
            'Crop_Area_Total',
            'Infrastructure_Totally_Damaged',
            'Infrastructure_Partially_Damaged',
            'Infrastructure_Total',
            'Production_Volume_Lost_MT',
            'Production_Loss_Cost_PHP'
        ]
        
        # Step 3: Remove TOTAL row
        df = df[~df['Region'].str.upper().str.contains('TOTAL', na=False)].reset_index(drop=True)
        
        # Step 4: Convert to numeric
        numeric_cols = [col for col in df.columns if col != 'Region']
        for col in numeric_cols:
            # Handle both comma-separated numbers and decimals
            df[col] = df[col].astype(str).str.replace(',', '').str.replace('\n', '')
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        # Step 5: Calculate totals
        total_row = pd.DataFrame([{
            'Region': '**TOTAL**',
            'Farmers_Affected': int(df['Farmers_Affected'].sum()),
            'Crop_Area_Totally_Damaged': df['Crop_Area_Totally_Damaged'].sum(),
            'Crop_Area_Partially_Damaged': df['Crop_Area_Partially_Damaged'].sum(),
            'Crop_Area_Total': df['Crop_Area_Total'].sum(),
            'Infrastructure_Totally_Damaged': int(df['Infrastructure_Totally_Damaged'].sum()),
            'Infrastructure_Partially_Damaged': int(df['Infrastructure_Partially_Damaged'].sum()),
            'Infrastructure_Total': int(df['Infrastructure_Total'].sum()),
            'Production_Volume_Lost_MT': df['Production_Volume_Lost_MT'].sum(),
            'Production_Loss_Cost_PHP': df['Production_Loss_Cost_PHP'].sum()
        }])
        
        df = pd.concat([df, total_row], ignore_index=True)
    
    elif table_type == 'DAMAGE TO INFRASTRUCTURE':
        # Step 1: Set column names
        df.columns = ['Region', 'Number_of_Damaged_Infrastructure', 'Cost_of_Damage_PHP']
        
        # Step 2: Remove TOTAL row
        df = df[~df['Region'].str.upper().str.contains('TOTAL', na=False)].reset_index(drop=True)
        
        # Step 3: Convert to numeric
        df['Number_of_Damaged_Infrastructure'] = pd.to_numeric(df['Number_of_Damaged_Infrastructure'].astype(str).str.replace(',', ''), errors='coerce').fillna(0).astype(int)
        df['Cost_of_Damage_PHP'] = pd.to_numeric(df['Cost_of_Damage_PHP'].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
        
        # Step 4: Calculate totals
        total_row = pd.DataFrame([{
            'Region': '**TOTAL**',
            'Number_of_Damaged_Infrastructure': int(df['Number_of_Damaged_Infrastructure'].sum()),
            'Cost_of_Damage_PHP': df['Cost_of_Damage_PHP'].sum()
        }])
        
        df = pd.concat([df, total_row], ignore_index=True)
    
    elif table_type == 'ASSISTANCE TO FAMILIES':
        # Step 1: Set column names
        df.columns = ['Region', 'Families_Requiring_Assistance', 'Cost_of_Assistance', 'Families_Assisted', 'Percent_Assisted']
        
        # Step 2: Remove TOTAL row
        df = df[~df['Region'].str.upper().str.contains('TOTAL', na=False)].reset_index(drop=True)
        
        # Step 3: Convert to numeric
        df['Families_Requiring_Assistance'] = pd.to_numeric(df['Families_Requiring_Assistance'].astype(str).str.replace(',', ''), errors='coerce').fillna(0).astype(int)
        df['Cost_of_Assistance'] = pd.to_numeric(df['Cost_of_Assistance'].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
        df['Families_Assisted'] = pd.to_numeric(df['Families_Assisted'].astype(str).str.replace(',', ''), errors='coerce').fillna(0).astype(int)
        df['Percent_Assisted'] = pd.to_numeric(df['Percent_Assisted'].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
        
        # Step 4: Calculate totals
        total_row = pd.DataFrame([{
            'Region': '**TOTAL**',
            'Families_Requiring_Assistance': int(df['Families_Requiring_Assistance'].sum()),
            'Cost_of_Assistance': df['Cost_of_Assistance'].sum(),
            'Families_Assisted': int(df['Families_Assisted'].sum()),
            'Percent_Assisted': (df['Families_Assisted'].sum() / df['Families_Requiring_Assistance'].sum() * 100) if df['Families_Requiring_Assistance'].sum() > 0 else 0
        }])
        
        df = pd.concat([df, total_row], ignore_index=True)
    
    elif table_type == 'ASSISTANCE TO LGUS':
        # Step 1: Set column names
        df.columns = ['Region', 'Cluster', 'Cost_of_Assistance']
        
        # Step 2: Remove TOTAL row
        df = df[~df['Region'].str.upper().str.contains('TOTAL', na=False)].reset_index(drop=True)
        
        # Step 3: Convert cost to numeric
        df['Cost_of_Assistance'] = pd.to_numeric(df['Cost_of_Assistance'].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
        
        # Step 4: Calculate totals
        total_row = pd.DataFrame([{
            'Region': '**TOTAL**',
            'Cluster': '',
            'Cost_of_Assistance': df['Cost_of_Assistance'].sum()
        }])
        
        df = pd.concat([df, total_row], ignore_index=True)
    
    elif table_type == 'PRE-EMPTIVE EVACUATION':
        # Step 1: Set column names
        df.columns = ['Region', 'Families', 'Persons']
        
        # Step 2: Remove GRAND TOTAL / TOTAL row
        df = df[~df['Region'].str.upper().str.contains('TOTAL', na=False)].reset_index(drop=True)
        
        # Step 3: Convert to numeric
        df['Families'] = pd.to_numeric(df['Families'].astype(str).str.replace(',', ''), errors='coerce').fillna(0).astype(int)
        df['Persons'] = pd.to_numeric(df['Persons'].astype(str).str.replace(',', ''), errors='coerce').fillna(0).astype(int)
        
        # Step 4: Calculate totals
        total_row = pd.DataFrame([{
            'Region': '**TOTAL**',
            'Families': df['Families'].sum(),
            'Persons': df['Persons'].sum()
        }])
        
        df = pd.concat([df, total_row], ignore_index=True)
    
    elif table_type == 'RELATED INCIDENTS':
        # Step 1: Remove TOTAL/GRAND TOTAL rows
        df = df[~df['Region'].astype(str).str.upper().str.contains('TOTAL|GRAND', na=False)].reset_index(drop=True)
        
        # Step 2: Convert numeric columns
        for col in df.columns[1:]:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0).astype(int)
        
        # Step 3: Add TOTAL row
        total_row = {'Region': '**TOTAL**'}
        for col in df.columns[1:]:
            total_row[col] = df[col].sum()
        
        df = pd.concat([df, pd.DataFrame([total_row])], ignore_index=True)

    return df


# =============================================================================
# STAGE 2: DETAILED EXTRACTION (Landscape Pages)
# =============================================================================

def extract_detailed_tables(pdf_source, selected_tables=None, summaries=None, progress_callback=None):
    """
    Stage 2: Detailed extraction with page context tracking
    summaries parameter added to enable Power/Water cross-referencing
    """
    if selected_tables is None:
        selected_tables = ['AFFECTED POPULATION', 'DAMAGED HOUSES', 'RELATED INCIDENTS']
    
    # Get landscape pages
    with open(pdf_source, 'rb') as f:
        pdf_reader = PyPDF2.PdfReader(f)
        landscape_pages = []
        
        for page_num in range(len(pdf_reader.pages)):
            page = pdf_reader.pages[page_num]
            width = float(page.mediabox.width)
            height = float(page.mediabox.height)
            
            if width > height:
                landscape_pages.append(page_num + 1)
    
    # Extract all tables
    all_tables = []
    for i, page_num in enumerate(landscape_pages):
        if progress_callback and i % 10 == 0:
            progress_callback(i, len(landscape_pages), f"Extracting page {page_num}...")
        
        try:
            page_tables = tabula.read_pdf(
                pdf_source,
                pages=str(page_num),
                multiple_tables=True,
                encoding='latin-1',
                lattice=True
            )
            all_tables.extend(page_tables)
        except Exception as e:
            print(f"Warning: Error on page {page_num}: {e}")
    
    # Extract selected tables
    combined_sections = {}
    
    if 'AFFECTED POPULATION' in selected_tables:
        affected_pop = extract_affected_population_table(all_tables)
        if affected_pop is not None:
            combined_sections['AFFECTED POPULATION'] = affected_pop
    
    if 'DAMAGED HOUSES' in selected_tables:
        damaged_houses = extract_damaged_houses_table(all_tables)
        if damaged_houses is not None:
            combined_sections['DAMAGED HOUSES'] = damaged_houses
    
    if 'RELATED INCIDENTS' in selected_tables:
        related_incidents = extract_related_incidents_table(all_tables)
        if related_incidents is not None:
            combined_sections['RELATED INCIDENTS'] = related_incidents
    
    if 'ROADS AND BRIDGES' in selected_tables:
        roads_bridges = extract_roads_bridges_table(all_tables)
        if roads_bridges is not None:
            combined_sections['ROADS AND BRIDGES'] = roads_bridges
    
    if 'POWER' in selected_tables and summaries and 'POWER' in summaries:
        power = extract_power_table(all_tables, summaries['POWER'])
        if power is not None:
            combined_sections['POWER'] = power
    
    if 'WATER SUPPLY' in selected_tables and summaries and 'WATER SUPPLY' in summaries:
        water = extract_water_table(all_tables, summaries['WATER SUPPLY'])
        if water is not None:
            combined_sections['WATER SUPPLY'] = water
    
    if 'COMMUNICATION LINES' in selected_tables:
        communications = extract_communications_table(all_tables)
        if communications is not None:
            combined_sections['COMMUNICATION LINES'] = communications

    if 'CASUALTIES' in selected_tables:
        casualties = extract_casualties_detailed_table(all_tables)
        if casualties is not None:
            combined_sections['CASUALTIES'] = casualties

    if 'DAMAGE TO AGRICULTURE' in selected_tables:
        agriculture = extract_agriculture_table(all_tables)
        if agriculture is not None:
            combined_sections['DAMAGE TO AGRICULTURE'] = agriculture

    if 'DAMAGE TO INFRASTRUCTURE' in selected_tables:
        infrastructure = extract_infrastructure_table(all_tables)
        if infrastructure is not None:
            combined_sections['DAMAGE TO INFRASTRUCTURE'] = infrastructure

    if 'ASSISTANCE TO FAMILIES' in selected_tables:
        families = extract_families_assistance_table(all_tables)
        if families is not None:
            combined_sections['ASSISTANCE TO FAMILIES'] = families

    if 'ASSISTANCE TO LGUS' in selected_tables:
        lgus = extract_lgus_assistance_table(all_tables)
        if lgus is not None:
            combined_sections['ASSISTANCE TO LGUS'] = lgus

    if 'PRE-EMPTIVE EVACUATION' in selected_tables:
        preemptive_evac = extract_preemptive_evacuation_table(all_tables)
        if preemptive_evac is not None:
            combined_sections['PRE-EMPTIVE EVACUATION'] = preemptive_evac

    return combined_sections


# =============================================================================
# BACKWARDS COMPATIBLE WRAPPER
# =============================================================================

def extract_tables_from_pdf(pdf_source, progress_callback=None):
    """
    Main entry point: Extract all tables from PDF
    
    This is a backwards-compatible wrapper that does Stage 2 only.
    For two-stage extraction, use extract_summary_tables() then extract_detailed_tables()
    
    Args:
        pdf_source: Path to PDF file
        progress_callback: Optional function(current, total, message)
    
    Returns:
        tuple: (combined_sections dict, report_metadata dict)
    """
    combined_sections = extract_detailed_tables(pdf_source, progress_callback)
    
    report_metadata = {
        'date': None,
        'title': None
    }
    
    return combined_sections, report_metadata


# =============================================================================
# AFFECTED POPULATION TABLE EXTRACTOR
# =============================================================================

def is_affected_population_detailed(df):
    """
    Identify DETAILED Affected Population tables
    """
    columns_text = ' '.join([str(col).lower() for col in df.columns])
    has_keywords = ('affected' in columns_text and 
                   'evacuation' in columns_text and 
                   ('inside' in columns_text or 'outside' in columns_text))
    
    if not has_keywords:
        return False
    
    first_col_name = str(df.columns[0]).lower()
    has_location_hierarchy = ('province' in first_col_name or 
                             'municipality' in first_col_name or 
                             'barangay' in first_col_name)
    
    return has_keywords and has_location_hierarchy


def expand_affected_population_columns_lattice(df):
    """
    Map lattice-extracted columns to standard names
    
    NO SPLITTING NEEDED - lattice gives us 19 separate columns!
    """
    column_names = [
        'Location',           # Col 0
        'Sub-total',          # Col 1
        'Affected_Brgys',     # Col 2
        'Affected_Families',  # Col 3
        'Affected_Persons',   # Col 4
        'ECs_CUM',            # Col 5
        'ECs_NOW',            # Col 6
        'Inside_Families_CUM',    # Col 7
        'Inside_Families_NOW',    # Col 8
        'Inside_Persons_CUM',     # Col 9
        'Inside_Persons_NOW',     # Col 10
        'Outside_Families_CUM',   # Col 11
        'Outside_Families_NOW',   # Col 12
        'Outside_Persons_CUM',    # Col 13
        'Outside_Persons_NOW',    # Col 14
        'Total_Families_CUM',     # Col 15
        'Total_Families_NOW',     # Col 16
        'Total_Persons_CUM',      # Col 17
        'Total_Persons_NOW'       # Col 18
    ]
    
    df_renamed = df.copy()
    df_renamed.columns = column_names
    
    return df_renamed


def combine_table_pieces(all_tables, table_indices):
    """
    Combine multiple table pieces into one dataframe
    """
    pieces = [all_tables[i] for i in table_indices]
    combined = pd.concat(pieces, ignore_index=True)
    return combined


def extract_affected_population_table(all_tables):
    """
    Main extraction function for Affected Population table
    """
    affected_indices = []
    for i, df in enumerate(all_tables):
        if is_affected_population_detailed(df):
            affected_indices.append(i)
    
    if not affected_indices:
        return None
    
    combined = combine_table_pieces(all_tables, affected_indices)
    expanded = expand_affected_population_columns_lattice(combined)
    
    return expanded


# =============================================================================
# DAMAGED HOUSES TABLE EXTRACTOR
# =============================================================================

def is_damaged_houses_detailed(df):
    """
    Identify DETAILED Damaged Houses tables
    """
    columns_text = ' '.join([str(col).lower() for col in df.columns])
    has_keywords = 'damaged' in columns_text and 'houses' in columns_text
    
    if not has_keywords:
        return False
    
    first_col_name = str(df.columns[0]).lower()
    has_location_hierarchy = ('province' in first_col_name or 
                             'municipality' in first_col_name or 
                             'barangay' in first_col_name)
    
    return has_keywords and has_location_hierarchy


def expand_damaged_houses_columns_lattice(df):
    """
    Map lattice-extracted Damaged Houses columns
    
    Lattice extracts 6 columns but names them wrong due to vertical merge.
    """
    column_names = [
        'Location',
        'Totally_Damaged',
        'Partially_Damaged', 
        'Grand_Total_Damaged',
        'Amount_PHP',
        'Remarks'
    ]
    
    df_renamed = df.copy()
    if len(df.columns) >= 6:
        df_renamed = df.iloc[:, :6].copy()
        df_renamed.columns = column_names
    
    return df_renamed


def extract_damaged_houses_table(all_tables):
    """
    Main extraction function for Damaged Houses table
    """
    damaged_indices = []
    for i, df in enumerate(all_tables):
        if is_damaged_houses_detailed(df):
            damaged_indices.append(i)
    
    if not damaged_indices:
        return None
    
    combined = combine_table_pieces(all_tables, damaged_indices)
    expanded = expand_damaged_houses_columns_lattice(combined)
    
    return expanded


# =============================================================================
# RELATED INCIDENTS TABLE EXTRACTOR
# =============================================================================

def is_related_incidents_detailed(df):
    """
    Identify DETAILED Related Incidents tables
    """
    columns_text = ' '.join([str(col).lower() for col in df.columns])
    
    # Step 1: Check for incident-specific keywords
    has_keywords = ('incident' in columns_text or 
                   ('type' in columns_text and 'occurrence' in columns_text))
    
    if not has_keywords:
        return False
    
    # Step 2: Check for location hierarchy
    first_col_name = str(df.columns[0]).lower()
    has_location_hierarchy = ('province' in first_col_name or 
                             'municipality' in first_col_name or 
                             'barangay' in first_col_name or
                             'region' in first_col_name)
    
    return has_keywords and has_location_hierarchy


def expand_related_incidents_columns_lattice(df):
    """
    Map lattice-extracted columns for Related Incidents
    Expected: 9 columns based on PDF structure
    """
    # Step 1: Define expected column names
    column_names = [
        'Location',
        'Count',
        'Type_of_Incident',
        'Date_of_Occurrence',
        'Time_of_Occurrence',
        'Description',
        'Actions_Taken',
        'Remarks',
        'Status'
    ]
    
    df_renamed = df.copy()
    
    # Step 2: Handle variable column counts
    if len(df.columns) >= 9:
        df_renamed = df.iloc[:, :9].copy()
        df_renamed.columns = column_names
    elif len(df.columns) == 8:
        # Sometimes Status column might be missing
        df_renamed.columns = column_names[:8]
    else:
        # Fallback - use what we have
        df_renamed.columns = column_names[:len(df.columns)]
    
    return df_renamed


def extract_related_incidents_table(all_tables):
    """
    Main extraction function for Related Incidents table
    """
    # Step 1: Find all incident table pieces
    incidents_indices = []
    for i, df in enumerate(all_tables):
        if is_related_incidents_detailed(df):
            incidents_indices.append(i)
    
    if not incidents_indices:
        return None
    
    # Step 2: Combine pieces
    combined = combine_table_pieces(all_tables, incidents_indices)
    
    # Step 3: Expand columns
    expanded = expand_related_incidents_columns_lattice(combined)
    
    return expanded

# =============================================================================
# ROADS AND BRIDGES TABLE EXTRACTOR
# =============================================================================

def is_roads_bridges_detailed(df):
    """
    Identify DETAILED Roads and Bridges tables
    """
    columns_text = ' '.join([str(col).lower() for col in df.columns])
    
    # Step 1: Check for roads/bridges keywords
    has_keywords = 'road' in columns_text or 'bridge' in columns_text
    
    if not has_keywords:
        return False
    
    # Step 2: Check for location hierarchy
    first_col_name = str(df.columns[0]).lower()
    has_location_hierarchy = ('province' in first_col_name or 
                             'municipality' in first_col_name or 
                             'barangay' in first_col_name or
                             'region' in first_col_name)
    
    return has_keywords and has_location_hierarchy


def expand_roads_bridges_columns_lattice(df):
    """
    Map lattice-extracted columns for Roads and Bridges
    Expected: 11 columns
    """
    # Step 1: Define expected column names
    column_names = [
        'Location',
        'Count',
        'Type',
        'Classification',
        'Road_Section_Bridge',
        'Status',
        'Date_Passable',
        'Time_Passable',
        'Date_Not_Passable',
        'Time_Not_Passable',
        'Remarks'
    ]
    
    df_renamed = df.copy()
    
    # Step 2: Handle variable column counts
    if len(df.columns) >= 11:
        df_renamed = df.iloc[:, :11].copy()
        df_renamed.columns = column_names
    else:
        df_renamed.columns = column_names[:len(df.columns)]
    
    return df_renamed


def extract_roads_bridges_table(all_tables):
    """
    Main extraction function for Roads and Bridges table
    """
    # Step 1: Find all roads/bridges table pieces
    roads_indices = []
    for i, df in enumerate(all_tables):
        if is_roads_bridges_detailed(df):
            roads_indices.append(i)
    
    if not roads_indices:
        return None
    
    # Step 2: Combine pieces
    combined = combine_table_pieces(all_tables, roads_indices)
    
    # Step 3: Expand columns
    expanded = expand_roads_bridges_columns_lattice(combined)
    
    return expanded

# =============================================================================
# UTILITY TABLE HELPER FUNCTIONS
# =============================================================================

def is_utility_detailed(df):
    """Identify utility tables (Power/Water) - 9 columns with SERVICE PROVIDER"""
    if len(df.columns) < 9:
        return False
    
    columns_text = ' '.join([str(col).lower() for col in df.columns])
    
    # Check for utility keywords
    has_keywords = 'service provider' in columns_text and 'interruption' in columns_text
    
    if not has_keywords:
        return False
    
    # Check for location hierarchy
    first_col_name = str(df.columns[0]).lower()
    has_location_hierarchy = ('province' in first_col_name or 
                             'municipality' in first_col_name or 
                             'region' in first_col_name)
    
    return has_keywords and has_location_hierarchy


def match_utility_by_grand_total(all_tables, utility_indices, target_total):
    """
    Match utility tables by comparing GRAND TOTAL against target
    Returns list of table indices that match
    """
    matched_indices = []
    
    for i in utility_indices:
        df = all_tables[i]
        
        # Check first row for GRAND TOTAL
        first_cell = str(df.iloc[0, 0]).strip().upper()
        
        if 'TOTAL' in first_cell:
            try:
                count = int(float(str(df.iloc[0, 1]).replace(',', '')))
                
                if count == target_total:
                    matched_indices.append(i)
            except:
                pass
    
    return matched_indices


def expand_utility_columns_lattice(df):
    """Map lattice-extracted columns for utilities (Power/Water) - 9 columns"""
    column_names = [
        'Location',
        'Count',
        'Type',
        'Service_Provider',
        'Date_Interruption',
        'Time_Interruption',
        'Date_Restored',
        'Time_Restored',
        'Remarks'
    ]
    
    df_renamed = df.copy()
    
    if len(df.columns) >= 9:
        df_renamed = df.iloc[:, :9].copy()
        df_renamed.columns = column_names
    else:
        df_renamed.columns = column_names[:len(df.columns)]
    
    return df_renamed


# =============================================================================
# POWER TABLE EXTRACTOR
# =============================================================================

def extract_power_table(all_tables, power_summary):
    """Extract Power tables using summary cross-reference"""
    # Step 1: Find all utility tables
    utility_indices = []
    for i, df in enumerate(all_tables):
        if is_utility_detailed(df):
            utility_indices.append(i)
    
    if not utility_indices:
        return None
    
    # Step 2: Calculate target GRAND TOTAL (Interrupted + Restored)
    total_row = power_summary[power_summary['Region'] == '**TOTAL**']
    if total_row.empty:
        return None
    
    target_total = total_row['Interrupted'].iloc[0] + total_row['Restored'].iloc[0]
    
    # Step 3: Match by GRAND TOTAL
    power_indices = match_utility_by_grand_total(all_tables, utility_indices, target_total)
    
    if not power_indices:
        return None
    
    # Step 4: Combine and expand
    combined = combine_table_pieces(all_tables, power_indices)
    expanded = expand_utility_columns_lattice(combined)
    
    return expanded


# =============================================================================
# WATER SUPPLY TABLE EXTRACTOR
# =============================================================================

def extract_water_table(all_tables, water_summary):
    """Extract Water Supply tables using summary cross-reference"""
    # Step 1: Find all utility tables
    utility_indices = []
    for i, df in enumerate(all_tables):
        if is_utility_detailed(df):
            utility_indices.append(i)
    
    if not utility_indices:
        return None
    
    # Step 2: Calculate target GRAND TOTAL (Interrupted + Restored)
    total_row = water_summary[water_summary['Region'] == '**TOTAL**']
    if total_row.empty:
        return None
    
    target_total = total_row['Interrupted'].iloc[0] + total_row['Restored'].iloc[0]
    
    # Step 3: Match by GRAND TOTAL
    water_indices = match_utility_by_grand_total(all_tables, utility_indices, target_total)
    
    if not water_indices:
        return None
    
    # Step 4: Combine and expand
    combined = combine_table_pieces(all_tables, water_indices)
    expanded = expand_utility_columns_lattice(combined)
    
    return expanded

# =============================================================================
# COMMUNICATIONS TABLE EXTRACTOR
# =============================================================================

def is_communications_detailed(df):
    """Identify DETAILED Communications tables"""
    if len(df.columns) < 18:
        return False
    
    columns_text = ' '.join([str(col).lower() for col in df.columns])
    
    # Check for communications keywords
    has_keywords = ('telecom' in columns_text or 
                   'communication' in columns_text or 
                   '2g' in columns_text or 
                   '3g' in columns_text or 
                   '4g' in columns_text)
    
    if not has_keywords:
        return False
    
    # Check for location hierarchy
    first_col_name = str(df.columns[0]).lower()
    has_location_hierarchy = ('province' in first_col_name or 
                             'municipality' in first_col_name or 
                             'region' in first_col_name)
    
    return has_keywords and has_location_hierarchy


def expand_communications_columns_lattice(df):
    """Map lattice-extracted columns for Communications - 18 columns"""
    column_names = [
        'Location',
        'Count',
        'Telecom_Company',
        'Status_of_Communication',
        'Date_Interruption',
        'Time_Interruption',
        'Date_Restoration',
        'Time_Restoration',
        '2G_Site_Count',
        '2G_With_Coverage',
        '2G_Percent_Coverage',
        '3G_Site_Count',
        '3G_With_Coverage',
        '3G_Percent_Coverage',
        '4G_Site_Count',
        '4G_With_Coverage',
        '4G_Percent_Coverage',
        'Remarks'
    ]
    
    df_renamed = df.copy()
    
    if len(df.columns) >= 18:
        df_renamed = df.iloc[:, :18].copy()
        df_renamed.columns = column_names
    else:
        df_renamed.columns = column_names[:len(df.columns)]
    
    return df_renamed


def extract_communications_table(all_tables):
    """Main extraction function for Communications table"""
    # Step 1: Find all communications table pieces
    comms_indices = []
    for i, df in enumerate(all_tables):
        if is_communications_detailed(df):
            comms_indices.append(i)
    
    if not comms_indices:
        return None
    
    # Step 2: Combine pieces
    combined = combine_table_pieces(all_tables, comms_indices)
    
    # Step 3: Expand columns
    expanded = expand_communications_columns_lattice(combined)
    
    return expanded

# =============================================================================
# CASUALTIES DETAILED TABLE EXTRACTOR
# =============================================================================

def is_casualties_detailed(df):
    """Identify DETAILED Casualties tables"""
    columns_text = ' '.join([str(col).lower() for col in df.columns])
    
    # Step 1: Check for casualties-specific keywords (SURNAME is unique to casualties)
    has_keywords = 'surname' in columns_text and 'validated' in columns_text
    
    if not has_keywords:
        return False
    
    # Step 2: Check for location hierarchy
    first_col_name = str(df.columns[0]).lower()
    has_location_hierarchy = ('province' in first_col_name or 
                             'municipality' in first_col_name or 
                             'barangay' in first_col_name)
    
    return has_keywords and has_location_hierarchy


def expand_casualties_columns_lattice(df):
    """
    Map lattice-extracted columns for Casualties - 12 columns
    Keep only: Location, QTY, Age, Sex, Source_of_Data, Validated
    DROP: Surname, First Name, Middle Name, Address, Cause, Remarks
    """
    # Step 1: Keep only the columns we want (by index)
    # Columns: 0=Location, 1=QTY, 5=Age, 6=Sex, 10=Source, 11=Validated
    columns_to_keep = [0, 1, 5, 6, 10, 11]
    
    df_filtered = df.iloc[:, columns_to_keep].copy()
    
    # Step 2: Set column names
    df_filtered.columns = ['Location', 'QTY', 'Age', 'Sex', 'Source_of_Data', 'Validated']
    
    return df_filtered


def extract_casualties_detailed_table(all_tables):
    """
    Main extraction function for Casualties detailed table
    Filters out PII columns
    """
    # Step 1: Find all casualties table pieces
    casualties_indices = []
    for i, df in enumerate(all_tables):
        if is_casualties_detailed(df):
            casualties_indices.append(i)
    
    if not casualties_indices:
        return None
    
    # Step 2: Combine pieces
    combined = combine_table_pieces(all_tables, casualties_indices)
    
    # Step 3: Filter columns (remove PII)
    filtered = expand_casualties_columns_lattice(combined)
    
    return filtered

# =============================================================================
# DAMAGE TO AGRICULTURE TABLE EXTRACTOR
# =============================================================================

def is_agriculture_detailed(df):
    """Identify DETAILED Agriculture tables (not summary)"""
    columns_text = ' '.join([str(col).lower() for col in df.columns])
    
    # Step 1: Check for agriculture keywords
    has_keywords = ('farmer' in columns_text or 
                   'fisherfolk' in columns_text or 
                   'agriculture' in columns_text or
                   ('crop' in columns_text and 'area' in columns_text))
    
    if not has_keywords:
        return False
    
    # Step 2: Check for location hierarchy (detailed tables have this)
    first_col_name = str(df.columns[0]).lower()
    has_location_hierarchy = ('region' in first_col_name and 
                             'province' in first_col_name and 
                             'municipality' in first_col_name)
    
    # Step 3: Exclude summary table (has 10 columns, detailed has 13)
    is_detailed = len(df.columns) >= 13
    
    return has_keywords and has_location_hierarchy and is_detailed


def expand_agriculture_columns_lattice(df):
    """Map lattice-extracted columns for Agriculture - 13 columns"""
    # Step 1: Remove first row (sub-headers: TOTALLY DAMAGED, PARTIALLY DAMAGED, TOTAL)
    df = df.iloc[1:].reset_index(drop=True)
    
    # Step 2: Set column names for 13 columns
    column_names = [
        'Location',
        'Count',
        'Classification',
        'Type',
        'Farmers_Fisherfolk_Affected',
        'Crop_Area_Totally_Damaged',
        'Crop_Area_Partially_Damaged',
        'Crop_Area_Total',
        'Infrastructure_Totally_Damaged',
        'Infrastructure_Partially_Damaged',
        'Infrastructure_Total',
        'Production_Volume_Lost_MT',
        'Production_Loss_Cost_PHP'
    ]
    
    df_renamed = df.copy()
    
    if len(df.columns) >= 13:
        df_renamed = df.iloc[:, :13].copy()
        df_renamed.columns = column_names
    else:
        df_renamed.columns = column_names[:len(df.columns)]
    
    return df_renamed


def extract_agriculture_table(all_tables):
    """Main extraction function for Agriculture table"""
    # Step 1: Find all agriculture table pieces
    ag_indices = []
    for i, df in enumerate(all_tables):
        if is_agriculture_detailed(df):
            ag_indices.append(i)
    
    if not ag_indices:
        return None
    
    # Step 2: Combine pieces
    combined = combine_table_pieces(all_tables, ag_indices)
    
    # Step 3: Expand columns
    expanded = expand_agriculture_columns_lattice(combined)
    
    return expanded

# =============================================================================
# DAMAGE TO INFRASTRUCTURE TABLE EXTRACTOR
# =============================================================================

def is_infrastructure_detailed(df):
    """Identify DETAILED Infrastructure tables"""
    columns_text = ' '.join([str(col).lower() for col in df.columns])
    
    # Check for infrastructure-specific keywords
    has_keywords = (('type' in columns_text and 'classification' in columns_text and 'unit' in columns_text) or
                   ('infrastructure' in columns_text and 'type' in columns_text and 'quantity' in columns_text))
    
    if not has_keywords:
        return False
    
    # Check for location hierarchy
    first_col_name = str(df.columns[0]).lower()
    has_location_hierarchy = ('region' in first_col_name or 
                             'province' in first_col_name or 
                             'municipality' in first_col_name)
    
    return has_keywords and has_location_hierarchy


def expand_infrastructure_columns_lattice(df):
    """Map lattice-extracted columns for Infrastructure - 11 columns"""
    column_names = [
        'Location',
        'Count',
        'Type',
        'Classification',
        'Infrastructure',
        'Number_of_Damaged',
        'Unit',
        'Quantity',
        'Status',
        'Cost_PHP',
        'Remarks'
    ]
    
    df_renamed = df.copy()
    
    if len(df.columns) >= 11:
        df_renamed = df.iloc[:, :11].copy()
        df_renamed.columns = column_names
    else:
        df_renamed.columns = column_names[:len(df.columns)]
    
    return df_renamed


def extract_infrastructure_table(all_tables):
    """Main extraction function for Infrastructure table"""
    # Step 1: Find all infrastructure table pieces
    infra_indices = []
    for i, df in enumerate(all_tables):
        if is_infrastructure_detailed(df):
            infra_indices.append(i)
    
    if not infra_indices:
        return None
    
    # Step 2: Combine pieces
    combined = combine_table_pieces(all_tables, infra_indices)
    
    # Step 3: Expand columns
    expanded = expand_infrastructure_columns_lattice(combined)
    
    return expanded

# =============================================================================
# ASSISTANCE TO FAMILIES TABLE EXTRACTOR
# =============================================================================

def is_families_assistance_detailed(df):
    """Identify DETAILED Assistance to Families tables"""
    columns_text = ' '.join([str(col).lower() for col in df.columns])
    
    # Check for families assistance keywords
    has_keywords = ('families' in columns_text and 
                   'needs' in columns_text and 
                   ('nfis provided' in columns_text or 'provided' in columns_text))
    
    if not has_keywords:
        return False
    
    # Check for location hierarchy
    first_col_name = str(df.columns[0]).lower()
    has_location_hierarchy = ('region' in first_col_name or 
                             'province' in first_col_name or 
                             'municipality' in first_col_name)
    
    return has_keywords and has_location_hierarchy


def expand_families_assistance_columns_lattice(df):
    """Map lattice-extracted columns for Families Assistance - 13 columns"""
    # Step 1: Remove first row (sub-headers: QTY, UNIT, COST PER UNIT, AMOUNT, SOURCE)
    df = df.iloc[1:].reset_index(drop=True)
    
    # Step 2: Set column names (CORRECTED ORDER)
    column_names = [
        'Location',                    # Col 0
        'Count',                       # Col 1
        'Families_Affected',           # Col 2
        'Needs',                       # Col 3
        'Families_Requiring_Assistance', # Col 4
        'NFIs_QTY',                    # Col 5 (was F/NFIs PROVIDED)
        'NFIs_Unit',                   # Col 6 (was NO. OF FAMILIES ASSISTED)
        'NFIs_Cost_Per_Unit',          # Col 7 (was % OF FAMILIES ASSISTED)
        'NFIs_Amount',                 # Col 8 (was REMARKS)
        'NFIs_Source',                 # Col 9 (was Unnamed: 1)
        'Families_Assisted',           # Col 10 (was Unnamed: 2)
        'Percent_Assisted',            # Col 11 (was Unnamed: 3)
        'Remarks'                      # Col 12 (was Unnamed: 4)
    ]
    
    df_renamed = df.copy()
    
    if len(df.columns) >= 13:
        df_renamed = df.iloc[:, :13].copy()
        df_renamed.columns = column_names
    else:
        df_renamed.columns = column_names[:len(df.columns)]
    
    return df_renamed


def extract_families_assistance_table(all_tables):
    """Main extraction function for Families Assistance table"""
    # Step 1: Find all families assistance table pieces
    fam_indices = []
    for i, df in enumerate(all_tables):
        if is_families_assistance_detailed(df):
            fam_indices.append(i)
    
    if not fam_indices:
        return None
    
    # Step 2: Combine pieces
    combined = combine_table_pieces(all_tables, fam_indices)
    
    # Step 3: Expand columns
    expanded = expand_families_assistance_columns_lattice(combined)
    
    return expanded


# =============================================================================
# ASSISTANCE TO LGUS TABLE EXTRACTOR
# =============================================================================

def is_lgus_assistance_detailed(df):
    """Identify DETAILED Assistance to LGUs tables"""
    columns_text = ' '.join([str(col).lower() for col in df.columns])
    
    # Check for LGUs assistance keywords (CLUSTER is unique to LGUs)
    has_keywords = ('cluster' in columns_text and 
                   'nfis' in columns_text and 
                   'services provided' in columns_text)
    
    if not has_keywords:
        return False
    
    # Check for location hierarchy
    first_col_name = str(df.columns[0]).lower()
    has_location_hierarchy = ('region' in first_col_name or 
                             'province' in first_col_name or 
                             'municipality' in first_col_name)
    
    return has_keywords and has_location_hierarchy


def expand_lgus_assistance_columns_lattice(df):
    """Map lattice-extracted columns for LGUs Assistance - 12 columns"""
    # Step 1: Remove first row (sub-headers: TYPE, QTY, UNIT, COST PER UNIT, AMOUNT)
    df = df.iloc[1:].reset_index(drop=True)
    
    # Step 2: Set column names (CORRECTED ORDER)
    column_names = [
        'Location',
        'Count',
        'Families_Affected',
        'Families_Assisted',
        'Cluster',
        'NFIs_Type',           # Col 5: was showing "QTY" in header
        'NFIs_QTY',            # Col 6: was showing "UNIT" in header
        'NFIs_Unit',           # Col 7: was showing "COST PER UNIT" in header
        'NFIs_Cost_Per_Unit',  # Col 8: was showing "AMOUNT" in header
        'NFIs_Amount',         # Col 9: Unnamed
        'NFIs_Source',         # Col 10: Unnamed
        'Remarks'              # Col 11: Unnamed
    ]
    
    df_renamed = df.copy()
    
    if len(df.columns) >= 12:
        df_renamed = df.iloc[:, :12].copy()
        df_renamed.columns = column_names
    else:
        df_renamed.columns = column_names[:len(df.columns)]
    
    return df_renamed


def extract_lgus_assistance_table(all_tables):
    """Main extraction function for LGUs Assistance table"""
    # Step 1: Find all lgus assistance table pieces
    lgu_indices = []
    for i, df in enumerate(all_tables):
        if is_lgus_assistance_detailed(df):
            lgu_indices.append(i)
    
    if not lgu_indices:
        return None
    
    # Step 2: Combine pieces
    combined = combine_table_pieces(all_tables, lgu_indices)
    
    # Step 3: Expand columns
    expanded = expand_lgus_assistance_columns_lattice(combined)
    
    return expanded

# =============================================================================
# PRE-EMPTIVE EVACUATION
# =============================================================================

def is_preemptive_evacuation_detailed(df):
    """
    Identify DETAILED Pre-Emptive Evacuation tables
    Has 7 columns with Male/Female/Families/Remarks
    """
    if len(df.columns) != 7:
        return False
    
    columns_text = ' '.join([str(col).lower() for col in df.columns])
    
    # Check for key identifying columns - must have all
    has_location = ('province' in columns_text or 'municipality' in columns_text or 'barangay' in columns_text)
    has_gender = ('male' in columns_text and 'female' in columns_text)
    has_families = 'families' in columns_text
    has_remarks = 'remarks' in columns_text
    
    return has_location and has_gender and has_families and has_remarks


def extract_preemptive_evacuation_table(all_tables):
    """
    Extract Pre-Emptive Evacuation detailed table
    7 columns: Location | Blank | Families | Male | Female | Total | Remarks
    """
    matching_indices = []
    
    for i, df in enumerate(all_tables):
        if df is None or df.empty:
            continue
        
        if is_preemptive_evacuation_detailed(df):
            matching_indices.append(i)
    
    if not matching_indices:
        return None
    
    # Combine all matching pieces
    pieces = [all_tables[i] for i in matching_indices]
    combined = pd.concat(pieces, ignore_index=True)
    
    # Set standard column names (7 columns)
    combined.columns = [
        'Region_Province_Municipality_Barangay',
        'Blank',
        'Families',
        'Male',
        'Female',
        'Total',
        'Remarks'
    ]
    
    # Clean up
    combined = combined[combined['Region_Province_Municipality_Barangay'].notna()]
    combined = combined[~combined['Region_Province_Municipality_Barangay'].astype(str).str.upper().str.contains('TOTAL|GRAND', na=False)]
    
    return combined

# =============================================================================
# FILE NAME
# =============================================================================

def extract_report_name(filename):
    """Extract report name from filename"""
    import re
    
    if not filename:
        return "NDRRMC Disaster Report"
    
    # Remove file extension
    name = filename.rsplit('.', 1)[0]
    
    # Replace underscores with spaces
    name = name.replace('_', ' ')
    
    # Look for "Situational Report No. XX" pattern
    match = re.search(r'(Situational Report No\.?\s+\d+)', name, re.IGNORECASE)
    if match:
        return match.group(1)
    
    # If no match, return cleaned filename (first 50 chars)
    return name[:50] if len(name) > 50 else name

# =============================================================================
# TESTING
# =============================================================================

if __name__ == "__main__":
    pass
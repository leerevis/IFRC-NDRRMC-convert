import pandas as pd
from config import REGION_IDENTIFIERS, REGION_PROVINCE_MAP, HUCS

def extract_location_hierarchy(df, location_col='Location', subtotal_col='Sub-total'):
    """
    Extracts Region, Province, Municipality, and Barangay from a hierarchical location column.
    
    Parameters:
    - df: DataFrame with location data
    - location_col: Name of the column containing location hierarchy
    - subtotal_col: Name of the column used to identify municipality rows (has values)
    
    Returns:
    - DataFrame with Region, Province, Municipality, Barangay columns added
    """
    
    # STEP 1: Make a copy to avoid modifying original
    df = df.copy()

    # STEP 0: Remove GRAND TOTAL rows
    df = df[~df[location_col].str.contains('GRAND TOTAL', case=False, na=False)].reset_index(drop=True)
    
    # STEP 2: Skip blank removal - let hierarchy logic handle it
    
    # STEP 3: Create and identify Region column
    df['Region'] = None
    
    for idx, row in df.iterrows():
        location = str(row[location_col]).strip().upper()
        if location in REGION_IDENTIFIERS:
            df.at[idx, 'Region'] = row[location_col]
    
    # STEP 4: Forward-fill Region down the dataframe
    df['Region'] = df['Region'].ffill()
    
    # STEP 5: Create and identify Province column (including HUCs)
    df['Province'] = None
    df['Is_Province_Header'] = False  # NEW: Track which rows are province headers
    
    for idx, row in df.iterrows():
        if pd.notna(row['Region']) and pd.isna(row['Province']):
            region = str(row['Region']).strip().upper()
            location = str(row['Location']).strip().upper()
            original_location = str(row['Location']).strip()  # Keep original case
            
            # HUC SPECIAL CASE - Highly Urbanized Cities act as provinces (region-aware)
            if location in HUCS and HUCS.get(location) == region:
                df.at[idx, 'Province'] = row['Location']
                df.at[idx, 'Is_Province_Header'] = True  # Mark as header
            # Normal province processing
            elif region in REGION_PROVINCE_MAP:
                if location in REGION_PROVINCE_MAP[region] and original_location.isupper():
                    df.at[idx, 'Province'] = row['Location']
                    df.at[idx, 'Is_Province_Header'] = True  # Mark as header
    
    # STEP 5d: Second pass - identify sentence-case provinces by sum matching
    if 'Affected_Persons' in df.columns:
        #print("Step 5d: Identifying sentence-case provinces by sum matching...")
        provinces_identified = 0
        
        for region in df['Region'].unique():
            if pd.isna(region):
                continue
            
            region_upper = str(region).strip().upper()
            if region_upper not in REGION_PROVINCE_MAP:
                continue
            
            province_list = REGION_PROVINCE_MAP[region_upper]
            
            for province_name in province_list:
                potential_province_rows = df[
                    (df['Region'] == region) & 
                    (df['Location'].str.upper() == province_name.upper()) &
                    (df['Province'].isna())
                ]
                
                for test_idx in potential_province_rows.index:
                    test_row = df.loc[test_idx]
                    
                    try:
                        province_total = test_row['Affected_Persons']
                        if pd.isna(province_total) or province_total <= 0:
                            continue
                    except:
                        continue
                    
                    cumsum = 0
                    matched = False
                    rows_below = df.loc[test_idx+1:]
                    
                    for below_idx in rows_below.index:
                        below_row = df.loc[below_idx]
                        
                        if below_row['Region'] != region:
                            break
                        
                        below_location = str(below_row['Location']).strip()
                        
                        if not below_location or below_location == '' or below_location == 'nan' or below_location == 'None':
                            continue
                        
                        below_affected = below_row['Affected_Persons']
                        if pd.isna(below_affected):
                            below_affected = 0
                        
                        if below_affected > 0:
                            below_upper = below_location.upper()
                            if below_upper in province_list:
                                if below_affected >= province_total * 0.5:
                                    break
                            
                            cumsum += below_affected
                            
                            if cumsum == province_total:
                                df.at[test_idx, 'Province'] = test_row['Location']
                                df.at[test_idx, 'Is_Province_Header'] = True  # Mark as header
                                provinces_identified += 1
                                matched = True
                                break
                            
                            if cumsum > province_total:
                                break
        
        #print(f"Step 5d: Identified {provinces_identified} additional sentence-case provinces")
    
    # STEP 5a: Mark HUC rows so they don't forward-fill to other rows
    df['Is_HUC'] = df.apply(
        lambda row: row['Province'].strip().upper() in HUCS and 
                    HUCS.get(row['Province'].strip().upper()) == row['Region'].strip().upper()
        if pd.notna(row['Province']) and pd.notna(row['Region']) else False, 
        axis=1
    )
    
    # STEP 5b: Save HUC province values with their location identifier
    huc_backup = df[df['Is_HUC']][['Region', 'Location', 'Province']].copy()
    
    # Temporarily set HUC provinces to NaN so they don't forward-fill
    df.loc[df['Is_HUC'], 'Province'] = pd.NA
    
    # STEP 6: Remove Region header rows
    df = df[~((df['Region'] == df[location_col]) & (df['Province'].isna()) & (~df['Is_HUC']))].reset_index(drop=True)
    
    # STEP 7: Forward-fill Province within each Region
    df['Province'] = df.groupby('Region', group_keys=False)['Province'].ffill()
    
    # STEP 7a: Restore HUC provinces by matching Region + Location
    for _, huc_row in huc_backup.iterrows():
        mask = (df['Region'] == huc_row['Region']) & (df['Location'] == huc_row['Location'])
        df.loc[mask, 'Province'] = huc_row['Province']
    
    # STEP 8: Create and identify Municipality column
    df['Municipality'] = None
    
    for idx, row in df.iterrows():
        region = str(row['Region']).strip().upper() if pd.notna(row['Region']) else ''
        location = str(row['Location']).strip().upper() if pd.notna(row['Location']) else ''

        if location in HUCS and HUCS.get(location) == region:
            df.at[idx, 'Municipality'] = row['Location']
        else:
            subtotal = row[subtotal_col]
            if pd.notna(subtotal) and str(subtotal).strip() != '':
                df.at[idx, 'Municipality'] = row[location_col]
    
    # STEP 9: Forward-fill Municipality
    df['Municipality'] = df.groupby(['Region', 'Province'], group_keys=False)['Municipality'].ffill()
    
    # STEP 10: Create and identify Barangay column
    df['Barangay'] = None
    
    for idx, row in df.iterrows():
        region = str(row['Region']).strip().upper() if pd.notna(row['Region']) else ''
        location = str(row['Location']).strip().upper() if pd.notna(row['Location']) else ''
        subtotal = row[subtotal_col]
        
        if location in HUCS and HUCS.get(location) == region:
            df.at[idx, 'Municipality'] = row[location_col]
            df.at[idx, 'Barangay'] = None
        elif pd.notna(subtotal) and str(subtotal).strip() != '':
            df.at[idx, 'Municipality'] = row[location_col]
            df.at[idx, 'Barangay'] = None
        else:
            df.at[idx, 'Barangay'] = row[location_col]
    
    # STEP 11: Remove Province header rows (but keep HUCs even if marked as headers)
    # Reconstruct Is_HUC check since we dropped the column
    is_huc_check = df.apply(
        lambda row: row['Province'].strip().upper() in HUCS and 
                    HUCS.get(row['Province'].strip().upper()) == row['Region'].strip().upper()
        if pd.notna(row['Province']) and pd.notna(row['Region']) else False, 
        axis=1
    )
    df = df[~(df['Is_Province_Header'] & ~is_huc_check)].reset_index(drop=True)
    
    # STEP 12: Remove page break "None" rows
    df = df[df[location_col] != 'None'].reset_index(drop=True)
    # STEP 12a: Remove blank Location rows
    df = df[(df['Location'].notna()) & (df['Location'].astype(str).str.strip() != '')].reset_index(drop=True)
    #print(f"After remove blank locations: {len(df)} rows")
    
    # STEP 13: Remove the Is_HUC helper column
    df = df.drop(columns=['Is_HUC'])
    
    # STEP 14: Reorder columns
    location_cols = ['Region', 'Province', 'Municipality', 'Barangay']
    other_cols = [col for col in df.columns if col not in location_cols]
    df = df[location_cols + other_cols]
    
    # STEP 15: Remove exact duplicate rows
    df = df.drop_duplicates(keep='first').reset_index(drop=True)
    
    # STEP 16: Add Level column to identify data granularity
    df['Level'] = None

    # Barangay level: has Barangay filled
    df.loc[(df['Barangay'].notna()) & (df['Barangay'].astype(str).str.strip() != ''), 'Level'] = 'Barangay'

    # Municipality level: has Municipality filled, Barangay blank, AND Location is not blank
    location_str = df['Location'].astype(str).str.strip().str.upper()
    has_location = df['Location'].notna() & (location_str != '') & (location_str != 'NONE')
    df.loc[(df['Municipality'].notna()) & 
        ((df['Barangay'].isna()) | (df['Barangay'].astype(str).str.strip() == '')) & 
        has_location, 'Level'] = 'Municipality'

    #print(f"FINAL: {len(df)} rows - Municipality: {len(df[df['Level']=='Municipality'])}, Barangay: {len(df[df['Level']=='Barangay'])}")
    
    return df

def transform_affected_population(df):
    """
    Transform the AFFECTED POPULATION table to extract hierarchical location data.
    """
    # Make a copy to avoid modifying the original
    df = df.copy()

    # Step 1: Remove first 3 header rows
    df = df.iloc[3:].reset_index(drop=True)
    
    # Step 2: Rename columns with standardized names
    new_columns = ['Location', 'Sub-total', 'Affected_Brgys', 'Affected_Families', 'Affected_Persons', 
                   'ECs_CUM', 'ECs_NOW', 'Inside_Families_CUM', 'Inside_Families_NOW', 
                   'Inside_Persons_CUM', 'Inside_Persons_NOW', 'Outside_Families_CUM', 
                   'Outside_Families_NOW', 'Outside_Persons_CUM', 'Outside_Persons_NOW', 
                   'Total_Families_CUM', 'Total_Families_NOW', 'Total_Persons_CUM', 'Total_Persons_NOW']
    df.columns = new_columns
    
    # Step 2a: Convert numeric columns to numeric type (MOVED HERE - before hierarchy extraction)
    numeric_columns = [
        'Sub-total', 'Affected_Brgys', 'Affected_Families', 'Affected_Persons',
        'ECs_CUM', 'ECs_NOW', 'Inside_Families_CUM', 'Inside_Families_NOW',
        'Inside_Persons_CUM', 'Inside_Persons_NOW', 'Outside_Families_CUM',
        'Outside_Families_NOW', 'Outside_Persons_CUM', 'Outside_Persons_NOW',
        'Total_Families_CUM', 'Total_Families_NOW', 'Total_Persons_CUM', 'Total_Persons_NOW'
    ]
    
    for col in numeric_columns:
        df[col] = df[col].astype(str).str.replace(',', '')
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    #print(f"After numeric conversion: {len(df)} rows, Affected: {df['Affected_Persons'].sum():,.0f}")
    
    # Step 3: Extract location hierarchy (now has numeric data available)
    df = extract_location_hierarchy(df, location_col='Location', subtotal_col='Sub-total')

    # Step 3a: Remove rows where Sub-total is blank EXCEPT for HUCs
    from config import HUCS

    is_huc = (df['Province'].astype(str).str.upper().isin(HUCS)) | (df['Location'].astype(str).str.upper().isin(HUCS))
    df = df[(df['Sub-total'].notna() & (df['Sub-total'].astype(str).str.strip() != '')) | is_huc].reset_index(drop=True)

    # Step 3b: Remove duplicate HUC/ICC entries
    df['_location_upper'] = df['Location'].str.upper()
    df['_is_huc'] = df['_location_upper'].isin(HUCS.keys())

    huc_mask = df['_is_huc']
    non_huc_rows = df[~huc_mask].copy()
    huc_rows = df[huc_mask].copy()

    huc_rows = huc_rows.drop_duplicates(subset=['Region', '_location_upper'], keep='first')

    df = pd.concat([non_huc_rows, huc_rows], ignore_index=True).sort_index(kind='stable')
    df = df.drop(columns=['_location_upper', '_is_huc']).reset_index(drop=True)

    # Step 4: Remove blank header rows where all numeric columns are NaN
    df = df.dropna(subset=numeric_columns, how='all').reset_index(drop=True)
    
    # Step 7: Remove flag columns before returning
    columns_to_drop = ['Is_Province_Header', 'Is_Municipality_Header', '_location_upper', '_is_huc']
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])

    return df

def transform_related_incidents(df):
    """
    Transform the RELATED INCIDENTS table to extract hierarchical location data.
    """

    # Make a copy to avoid modifying the original
    df = df.copy()

    # Step 1: Rename columns with standardized names
    new_columns = ['Location', 'Sub-total', 'Type_of_Incident', 'Date_of_Occurrence', 
                   'Time_of_Occurrence', 'Description', 'Actions_Taken', 'Remarks', 'Status']
    df.columns = new_columns
    
    # Step 2: Extract location hierarchy
    df = extract_location_hierarchy(df, location_col='Location', subtotal_col='Sub-total')
    
    # Step 3: Convert Sub-total to numeric
    df['Sub-total'] = df['Sub-total'].astype(str).str.replace(',', '')
    df['Sub-total'] = pd.to_numeric(df['Sub-total'], errors='coerce')
    
    # Step 4: Remove blank rows where Sub-total is NaN and all text columns are empty
    text_columns = ['Type_of_Incident', 'Date_of_Occurrence', 'Description']
    df = df.dropna(subset=['Sub-total'] + text_columns, how='all').reset_index(drop=True)
    
    # Step X: Remove flag columns before returning
    columns_to_drop = ['Is_Province_Header', 'Is_Municipality_Header', '_location_upper', '_is_huc']
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])

    return df

def transform_roads_and_bridges(df):
    """
    Transform the ROADS AND BRIDGES table to extract hierarchical location data.
    """
    # Make a copy to avoid modifying the original
    df = df.copy()
    
    # Step 1: Rename columns with standardized names
    new_columns = ['Location', 'Sub-total', 'Type', 'Classification', 'Road_Section_Bridge', 
                   'Status', 'Date_Reported_Passable', 'Time_Reported_Passable', 
                   'Date_Reported_Not_Passable', 'Time_Reported_Not_Passable', 'Remarks']
    df.columns = new_columns
    
    # Step 2: Extract location hierarchy
    df = extract_location_hierarchy(df, location_col='Location', subtotal_col='Sub-total')
    
    # Step 3: Convert Sub-total to numeric
    df['Sub-total'] = df['Sub-total'].astype(str).str.replace(',', '')
    df['Sub-total'] = pd.to_numeric(df['Sub-total'], errors='coerce')
    
    # Step 4: Standardize Status values
    status_mapping = {
        'OLP - ONE LANE\nPASSABLE': 'OLP - One Lane Passable',
        'PATV -\nPASSABLE ALL\nTYPES\nVEHICLES': 'PATV - Passable to All Types Vehicles',
        'OLPLV - One-\nLane Passable to\nLight Vehicles': 'OLPLV - One Lane Passable to Light Vehicles',
        'NPATV - NOT\nPASSABLE ALL\nTYPES\nVEHICLES': 'NPATV - Not Passable to All Types Vehicles',
        'PTLV -\nPASSABLE TO\nLIGHT VEHICLES': 'PTLV - Passable to Light Vehicles',
        'NPLV - NOT\nPASSABLE LIGHT\nVEHICLES': 'NPLV - Not Passable to Light Vehicles'
    }
    
    df['Status'] = df['Status'].replace(status_mapping)
    
    # Step 5: Remove blank rows where Sub-total is NaN and all text columns are empty
    text_columns = ['Type', 'Classification', 'Status']
    df = df.dropna(subset=['Sub-total'] + text_columns, how='all').reset_index(drop=True)
    
    return df

def transform_power(df):
    """
    Transform the POWER table to extract hierarchical location data.
    """
    # Make a copy to avoid modifying the original
    df = df.copy()
    
    # Step 1: Rename columns with standardized names
    new_columns = ['Location', 'Sub-total', 'Type', 'Service_Provider', 
                   'Date_of_Interruption', 'Time_of_Interruption', 
                   'Date_Restored', 'Time_Restored', 'Remarks']
    df.columns = new_columns
    
    # Step 2: Extract location hierarchy
    df = extract_location_hierarchy(df, location_col='Location', subtotal_col='Sub-total')
    
    # Step 3: Convert Sub-total to numeric
    df['Sub-total'] = df['Sub-total'].astype(str).str.replace(',', '')
    df['Sub-total'] = pd.to_numeric(df['Sub-total'], errors='coerce')
    
    # Step 4: Remove blank rows where Sub-total is NaN and all text columns are empty
    text_columns = ['Type', 'Service_Provider']
    df = df.dropna(subset=['Sub-total'] + text_columns, how='all').reset_index(drop=True)
    
    return df

def transform_water_supply(df):
    """
    Transform the WATER SUPPLY table to extract hierarchical location data.
    """
    # Make a copy to avoid modifying the original
    df = df.copy()
    
    # Step 1: Rename columns with standardized names
    new_columns = ['Location', 'Sub-total', 'Type', 'Service_Provider', 
                   'Date_of_Interruption', 'Time_of_Interruption', 
                   'Date_Restored', 'Time_Restored', 'Remarks']
    df.columns = new_columns
    
    # Step 2: Extract location hierarchy
    df = extract_location_hierarchy(df, location_col='Location', subtotal_col='Sub-total')
    
    # Step 3: Convert Sub-total to numeric
    df['Sub-total'] = df['Sub-total'].astype(str).str.replace(',', '')
    df['Sub-total'] = pd.to_numeric(df['Sub-total'], errors='coerce')
    
    # Step 4: Remove blank rows where Sub-total is NaN and all text columns are empty
    text_columns = ['Type', 'Service_Provider']
    df = df.dropna(subset=['Sub-total'] + text_columns, how='all').reset_index(drop=True)
    
    return df

def transform_communication_lines(df):
    """Transform Communication Lines - manual hierarchy mapping"""
    import pandas as pd
    from transformations import extract_location_hierarchy
    from config import HUCS
    
    df = df.copy()
    
    # Step 1: Identify header rows (have Count)
    header_mask = df['Count'].notna()
    
    # Step 2: Extract hierarchy from ONLY header rows
    headers_df = df[header_mask].copy()
    headers_processed = extract_location_hierarchy(headers_df, location_col='Location', subtotal_col='Count')
    
    # Step 3: Create lookup map: Municipality -> (Region, Province)
    location_map = {}
    for _, row in headers_processed.iterrows():
        if pd.notna(row['Municipality']):
            location_map[row['Municipality']] = {
                'Region': row['Region'],
                'Province': row['Province']
            }
    
    # Step 4: Forward-fill Municipality from headers to all rows
    df['Municipality_Filled'] = None
    df.loc[header_mask, 'Municipality_Filled'] = df.loc[header_mask, 'Location']
    df['Municipality_Filled'] = df['Municipality_Filled'].ffill()
    
    # Step 5: Map Region/Province to all rows based on Municipality
    df['Region'] = df['Municipality_Filled'].map(lambda x: location_map.get(x, {}).get('Region'))
    df['Province'] = df['Municipality_Filled'].map(lambda x: location_map.get(x, {}).get('Province'))
    df['Municipality'] = df['Municipality_Filled']
    
    # Step 6: Assign Barangay for detail rows (non-headers with Status filled)
    df['Barangay'] = None
    detail_mask = ~header_mask & df['Status_of_Communication'].notna()
    
    for idx in df[detail_mask].index:
        location_val = df.at[idx, 'Location']
        if pd.notna(location_val) and str(location_val).strip() not in ['', 'nan', 'None']:
            df.at[idx, 'Barangay'] = location_val
    
    # Step 7: Keep only detail rows (have Status)
    df = df[df['Status_of_Communication'].notna()].reset_index(drop=True)
    
    # Step 8: Assign Level
    df['Level'] = df['Barangay'].apply(lambda x: 'Barangay' if pd.notna(x) and str(x).strip() not in ['', 'None', 'nan'] else 'Municipality')
    
    # Step 9: Fix "No breakdown" under HUCs - AFTER filtering to detail rows
    for idx in df.index:
        muni = str(df.at[idx, 'Municipality']).strip()
        province = df.at[idx, 'Province']
        region = df.at[idx, 'Region']
        barangay = df.at[idx, 'Barangay']
        
        if muni.lower() == 'no breakdown' and pd.notna(province) and pd.notna(region):
            province_upper = str(province).strip().upper()
            region_upper = str(region).strip().upper()
            
            # Check if Province is a HUC (try exact match first, then with "CITY" appended)
            is_huc = False
            if province_upper in HUCS and HUCS[province_upper] == region_upper:
                is_huc = True
            elif f"{province_upper} CITY" in HUCS and HUCS[f"{province_upper} CITY"] == region_upper:
                is_huc = True
            
            if is_huc:
                # Copy Province to Municipality
                df.at[idx, 'Municipality'] = province
                # Set Barangay to "No breakdown" if it's blank
                if pd.isna(barangay) or str(barangay).strip() in ['', 'None', 'nan']:
                    df.at[idx, 'Barangay'] = 'No breakdown'
                    df.at[idx, 'Level'] = 'Barangay'
    
    # Step 10: Fix Level - "No breakdown" and "All Barangays" should be Municipality level
    municipality_barangays = ['No breakdown', 'All Barangays']
    for idx in df.index:
        barangay = str(df.at[idx, 'Barangay']).strip()
        if barangay in municipality_barangays:
            df.at[idx, 'Level'] = 'Municipality'

    # Step 11: Clean text columns
    text_cols = ['Telecom_Company', 'Status_of_Communication', 'Remarks', 'Barangay']
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace('\r', ' ').str.strip()
            df[col] = df[col].replace('nan', None)
    
    if 'Status_of_Communication' in df.columns:
        df['Status_of_Communication'] = df['Status_of_Communication'].str.replace(' ', '')
    
    # Step 12: Remove temp columns
    columns_to_drop = ['Municipality_Filled', 'Count', 'Location']
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])
    
    # Step 13: Reorder columns - location hierarchy first
    location_cols = ['Region', 'Province', 'Municipality', 'Barangay', 'Level']
    other_cols = [col for col in df.columns if col not in location_cols]
    df = df[location_cols + other_cols]

    return df

def transform_damaged_houses(df):
    """
    Transform the DAMAGED HOUSES table.
    """
    # Make a copy
    df = df.copy()
    
    # Step 1: Rename columns
    df.columns = ['Location', 'Totally_Damaged', 'Partially_Damaged', 'Grand_Total_Damaged', 'Amount_PHP', 'Remarks']
    
    # Step 2: Remove repeating header rows
    df = df[df['Totally_Damaged'] != 'TOTALLY'].reset_index(drop=True)

    # Step 2.5: Remove GRAND TOTAL row  ← ADD THIS LINE
    df = df[~df['Location'].str.contains('GRAND TOTAL', case=False, na=False)].reset_index(drop=True)

    # Step 3: Import region and province mappings
    from config import REGION_IDENTIFIERS, REGION_PROVINCE_MAP
    
    # Step 4: Create Region column
    df['Region'] = None
    
    # Step 5: Identify Regions
    for idx, row in df.iterrows():
        location = str(row['Location']).strip().upper()
        if location in REGION_IDENTIFIERS:
            df.at[idx, 'Region'] = row['Location']

    # Step 6: Move Region column to the beginning
    df = df[['Region'] + [col for col in df.columns if col != 'Region']]

    # Step 7: Forward-fill Region
    df['Region'] = df['Region'].ffill()

    # Step 8: Remove rows where Region equals Location
    df = df[df['Region'] != df['Location']].reset_index(drop=True)

    # Step 9: Create Province column
    df['Province'] = None

    # Step 10: Identify Provinces
    for idx, row in df.iterrows():
        if pd.notna(row['Region']) and pd.isna(row['Province']):
            region = str(row['Region']).strip().upper()
            location = str(row['Location']).strip()  # Keep original case
            
            if region in REGION_PROVINCE_MAP:
                if location.upper() in REGION_PROVINCE_MAP[region] and location.isupper():
                    # Only match if location is ALL CAPS
                    df.at[idx, 'Province'] = row['Location']
    
    # Step 11: Move Province column
    cols = df.columns.tolist()
    cols.remove('Province')
    region_idx = cols.index('Region')
    cols.insert(region_idx + 1, 'Province')
    df = df[cols]

    # Step 12: Forward-fill Province
    df['Province'] = df['Province'].ffill()

    # Step 13: Remove rows where Province equals Location (case-sensitive)
    df = df[df['Province'] != df['Location']].reset_index(drop=True)

    # Step 14: Convert numeric columns to numeric type
    numeric_cols = ['Totally_Damaged', 'Partially_Damaged', 'Grand_Total_Damaged', 'Amount_PHP']
    for col in numeric_cols:
        df[col] = df[col].astype(str).str.replace(',', '')
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Step 15: Create Municipality and Barangay columns
    df['Municipality'] = None
    df['Barangay'] = None
    
    # Step 16: Identify Municipalities and Barangays using cumulative totals
    current_municipality_idx = None
    municipality_total = 0
    cumulative_total = 0
    previous_province = None
    
    for idx, row in df.iterrows():
        location = str(row['Location']).strip()
        
        # Check if Province changed - reset if so
        if row['Province'] != previous_province:
            current_municipality_idx = None
            municipality_total = 0
            cumulative_total = 0
            previous_province = row['Province']
        
        # If blank location - reset municipality tracking
        if location == '' or location == 'nan':
            current_municipality_idx = None
            municipality_total = 0
            cumulative_total = 0
            continue
        
        # If we're tracking a municipality
        if current_municipality_idx is not None:
            # Add this row to cumulative
            cumulative_total += row['Grand_Total_Damaged'] if pd.notna(row['Grand_Total_Damaged']) else 0
            
            # Mark as barangay
            df.at[idx, 'Barangay'] = location
            
            # Check if we've reached the municipality total
            if cumulative_total >= municipality_total:
                # Reset for next municipality
                current_municipality_idx = None
                municipality_total = 0
                cumulative_total = 0
        else:
            # This is a new municipality
            df.at[idx, 'Municipality'] = location
            current_municipality_idx = idx
            municipality_total = row['Grand_Total_Damaged'] if pd.notna(row['Grand_Total_Damaged']) else 0
            cumulative_total = 0
    
    # Step 17: Forward-fill Municipality
    df['Municipality'] = df['Municipality'].ffill()

    # Step 17.5: Add a Level column to identify data granularity
    df['Level'] = None
    df.loc[df['Barangay'].notna(), 'Level'] = 'Barangay'
    df.loc[(df['Municipality'].notna()) & (df['Barangay'].isna()), 'Level'] = 'Municipality'
    
    # Step 18: Remove blank location rows
    df = df[(df['Location'].notna()) & (df['Location'].str.strip() != '') & (df['Location'] != 'nan')].reset_index(drop=True)
    
    # Step 19: Move Municipality and Barangay columns after Province
    cols = df.columns.tolist()
    cols.remove('Municipality')
    cols.remove('Barangay')
    province_idx = cols.index('Province')
    cols.insert(province_idx + 1, 'Municipality')
    cols.insert(province_idx + 2, 'Barangay')
    df = df[cols]
    
    return df

def transform_casualties(df):
    """
    Transform Casualties detailed table
    - Extract casualty type from Location column
    - Split location hierarchy
    - Add age grouping
    - Remove PII and unnecessary columns
    """
    df = df.copy()
    
    # Step 1: Extract casualty types from Location column
    casualty_types = ['DEAD', 'INJURED', 'MISSING']
    
    df['Casualty_Type'] = None
    current_type = None
    
    for idx, row in df.iterrows():
        location = str(row['Location']).strip().upper()
        
        # Check if this row is a casualty type header
        is_type_header = False
        for ctype in casualty_types:
            if ctype in location and ('REGION' not in location and 'PROVINCE' not in location):
                is_type_header = True
                if 'INJURED' in location:
                    current_type = 'INJURED/ILL'
                elif 'DEAD' in location:
                    current_type = 'DEAD'
                elif 'MISSING' in location:
                    current_type = 'MISSING'
                break
        
        if is_type_header:
            df.at[idx, 'Casualty_Type'] = current_type
            df.at[idx, 'Location'] = None  # Mark for removal
        else:
            df.at[idx, 'Casualty_Type'] = current_type
    
    # Step 2: Remove casualty type header rows
    df = df[df['Location'].notna()].reset_index(drop=True)
    
    # Step 3: Extract location hierarchy
    df = extract_location_hierarchy(df, location_col='Location', subtotal_col='QTY')
    
    # Step 4: Add granular age grouping
    def assign_age_group(age):
        if pd.isna(age):
            return 'Unknown'
        try:
            age = int(float(age))
            if age <= 1:
                return '0-1 (Infants)'
            elif age <= 4:
                return '2-4 (Toddlers)'
            elif age <= 12:
                return '5-12 (Children)'
            elif age <= 17:
                return '13-17 (Adolescents)'
            elif age <= 64:
                return '18-64 (Adults)'
            else:
                return '65+ (Elderly)'
        except:
            return 'Unknown'
    
    df['Age_Group'] = df['Age'].apply(assign_age_group)
    
    # Step 5: Drop Age column (we only need Age_Group)
    df = df.drop(columns=['Age'])
    
    # Step 6: Remove flag columns
    columns_to_drop = ['Is_Province_Header', 'Is_Municipality_Header', '_location_upper', '_is_huc']
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])
    
    return df

def transform_damage_to_agriculture(df):
    """Transform the DAMAGE TO AGRICULTURE table to extract hierarchical location data."""
    import pandas as pd
    from config import HUCS
    
    df = df.copy()
    
    # Step 1: Rename columns to match current structure (13 columns)
    new_columns = [
        'Location', 'Count', 'Classification', 'Type', 
        'Farmers_Fisherfolk_Affected',
        'Crop_Area_Totally_Damaged', 'Crop_Area_Partially_Damaged', 'Crop_Area_Total',
        'Infrastructure_Totally_Damaged', 'Infrastructure_Partially_Damaged', 'Infrastructure_Total',
        'Production_Volume_Lost_MT', 'Production_Loss_Cost_PHP'
    ]
    df.columns = new_columns
    
    # Step 2: Identify header rows (have Count)
    header_mask = df['Count'].notna()
    
    # Step 3: Extract hierarchy from ONLY header rows
    headers_df = df[header_mask].copy()
    headers_processed = extract_location_hierarchy(headers_df, location_col='Location', subtotal_col='Count')
    
    # Step 4: Create lookup map: Location name -> (Region, Province, Municipality)
    location_map = {}
    for _, row in headers_processed.iterrows():
        location_name = row.get('Location')
        if pd.notna(location_name):
            location_map[location_name] = {
                'Region': row.get('Region'),
                'Province': row.get('Province'),
                'Municipality': row.get('Municipality')
            }
    
    # Step 5: Forward-fill Location from headers to all rows
    df['Location_Filled'] = None
    df.loc[header_mask, 'Location_Filled'] = df.loc[header_mask, 'Location']
    df['Location_Filled'] = df['Location_Filled'].ffill()
    
    # Step 6: Map Region/Province/Municipality to all rows based on Location
    df['Region'] = df['Location_Filled'].map(lambda x: location_map.get(x, {}).get('Region'))
    df['Province'] = df['Location_Filled'].map(lambda x: location_map.get(x, {}).get('Province'))
    df['Municipality'] = df['Location_Filled'].map(lambda x: location_map.get(x, {}).get('Municipality'))
    
    # Step 7: Assign Barangay for detail rows (non-headers with data)
    df['Barangay'] = None
    detail_mask = ~header_mask & (df['Classification'].notna() | df['Type'].notna())
    
    for idx in df[detail_mask].index:
        location_val = df.at[idx, 'Location']
        if pd.notna(location_val) and str(location_val).strip() not in ['', 'nan', 'None']:
            df.at[idx, 'Barangay'] = location_val
    
    # Step 8: Keep only detail rows (have Classification or Type filled)
    df = df[df['Classification'].notna() | df['Type'].notna()].reset_index(drop=True)
    
    # Step 9: Assign Level based on what data exists
    def assign_level(row):
        muni = str(row['Municipality']).strip() if pd.notna(row['Municipality']) else ''
        barangay = str(row['Barangay']).strip() if pd.notna(row['Barangay']) else ''
        
        # If Municipality is "No breakdown", this is province-level data
        if muni.lower() == 'no breakdown':
            return 'Province'
        
        # If has barangay data
        if barangay and barangay not in ['', 'None', 'nan']:
            if barangay in ['No breakdown', 'All Barangays']:
                return 'Municipality'
            else:
                return 'Barangay'
        
        # If has municipality but no barangay
        if muni and muni not in ['', 'None', 'nan']:
            return 'Municipality'
        
        # Otherwise province level
        return 'Province'

    df['Level'] = df.apply(assign_level, axis=1)
    
    # Step 10: Clean numeric columns (remove commas, convert to float)
    numeric_cols = [
        'Farmers_Fisherfolk_Affected',
        'Crop_Area_Totally_Damaged', 'Crop_Area_Partially_Damaged', 'Crop_Area_Total',
        'Infrastructure_Totally_Damaged', 'Infrastructure_Partially_Damaged', 'Infrastructure_Total',
        'Production_Volume_Lost_MT', 'Production_Loss_Cost_PHP'
    ]
    
    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace(',', '')
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Step 12: Remove temp columns
    columns_to_drop = ['Location_Filled', 'Count', 'Location']
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])
    
    # Step 13: Reorder columns - location hierarchy first
    location_cols = ['Region', 'Province', 'Municipality', 'Barangay', 'Level']
    other_cols = [col for col in df.columns if col not in location_cols]
    df = df[location_cols + other_cols]
    
    return df

def transform_damage_to_infrastructure(df):
    """Transform Infrastructure detailed table"""
    import pandas as pd
    
    df = df.copy()
    
    # Step 1: Remove total rows
    total_keywords = ['GRAND TOTAL']
    df = df[~df['Location'].str.upper().str.contains('|'.join(total_keywords), na=False)].reset_index(drop=True)
    
    # Step 2: Forward-fill Location for detail rows
    df['Location'] = df['Location'].fillna(method='ffill')
    
    # Step 3: Assign Level based on Count
    # If Count exists (not NaN) → Municipality, otherwise → Barangay
    df['Level'] = df['Count'].apply(lambda x: 'Municipality' if pd.notna(x) else 'Barangay')
    
    # Step 4: Extract location hierarchy
    df = extract_location_hierarchy(df, location_col='Location', subtotal_col='Count')
    
    # Step 5: Clean text columns (remove \r)
    text_cols = ['Type', 'Classification', 'Infrastructure', 'Status', 'Remarks']
    for col in text_cols:
        df[col] = df[col].astype(str).str.replace('\r', ' ').str.strip()
        df[col] = df[col].replace('nan', None)
    
    # Step 6: Remove rows where Type, Classification, and Infrastructure are all empty
    df = df[~((df['Type'].isna() | (df['Type'] == 'None')) & 
              (df['Classification'].isna() | (df['Classification'] == 'None')) &
              (df['Infrastructure'].isna() | (df['Infrastructure'] == 'None')))].reset_index(drop=True)
    
    # Step 7: Clean numeric columns BEFORE grouping
    numeric_cols = ['Number_of_Damaged', 'Quantity', 'Cost_PHP']
    for col in numeric_cols:
        df[col] = df[col].astype(str).str.replace(',', '').str.replace('\r', '')
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    # Step 8: Group by unique infrastructure (ignore Unit/Quantity/Barangay - same infrastructure = one row)
    group_cols = ['Region', 'Province', 'Municipality', 'Level', 'Type', 'Classification', 'Infrastructure']
        
    agg_dict = {
        'Number_of_Damaged': 'first',  # Should be consistent (always 1 for same infrastructure)
        'Cost_PHP': 'sum',  # Total cost of all damage
        'Status': lambda x: x.mode()[0] if len(x.mode()) > 0 else 'Multiple statuses',
        'Remarks': 'count',  # Number of damage records
        'Unit': lambda x: ', '.join(x.dropna().astype(str).unique()) if x.notna().any() else None,
        'Quantity': 'sum'  # Total quantity across all damage
    }

    df_grouped = df.groupby(group_cols, dropna=False).agg(agg_dict).reset_index()
    df_grouped = df_grouped.rename(columns={'Remarks': 'Damage_Record_Count'})
    
    # TODO: Get actual document name and page numbers from extraction metadata
    df_grouped['Remarks'] = df_grouped['Damage_Record_Count'].apply(
        lambda x: f"See source document for {x} damage record(s)" if x > 1 else None
    )
    
    # Step 9: Remove flag columns
    columns_to_drop = ['Is_Province_Header', 'Is_Municipality_Header', '_location_upper', '_is_huc', 'Count']
    df_grouped = df_grouped.drop(columns=[col for col in columns_to_drop if col in df_grouped.columns])
    
    return df_grouped

def transform_assistance_to_families(df):
    """Transform Assistance to Families detailed table"""
    import pandas as pd
    
    df = df.copy()
    
    # Step 1: Remove total rows
    total_keywords = ['GRAND TOTAL']
    df = df[~df['Location'].str.upper().str.contains('|'.join(total_keywords), na=False)].reset_index(drop=True)
    
    # Step 2: Forward-fill Location and Count for detail rows
    df['Location'] = df['Location'].fillna(method='ffill')
    df['Count'] = df['Count'].fillna(method='ffill')
    
    # Step 3: Assign Level - all municipalities (have Count)
    df['Level'] = 'Municipality'
    
    # Step 4: Extract location hierarchy
    df = extract_location_hierarchy(df, location_col='Location', subtotal_col='Count')
    
    # Step 5: Clean text columns (remove \r)
    text_cols = ['Needs', 'NFIs_Unit', 'Remarks', 'NFIs_Source']
    for col in text_cols:
        df[col] = df[col].astype(str).str.replace('\r', ' ').str.strip()
        df[col] = df[col].replace('nan', None)
    
    # Step 6: Remove rows where Needs is empty (header rows)
    df = df[~(df['Needs'].isna() | (df['Needs'] == 'None'))].reset_index(drop=True)
    
    # Step 7: Clean numeric columns
    numeric_cols = ['Families_Affected', 'Families_Requiring_Assistance', 'NFIs_QTY', 
                   'NFIs_Cost_Per_Unit', 'NFIs_Amount', 'Families_Assisted', 'Percent_Assisted']
    
    for col in numeric_cols:
        df[col] = df[col].astype(str).str.replace(',', '').str.replace('\r', '')
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    # Step 8: Remove flag columns
    columns_to_drop = ['Is_Province_Header', 'Is_Municipality_Header', '_location_upper', '_is_huc', 'Count']
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])
    
    return df

def transform_assistance_to_lgus(df):
    """Transform Assistance to LGUs detailed table"""
    import pandas as pd
    
    df = df.copy()
    
    # Step 1: Remove total rows
    total_keywords = ['GRAND TOTAL']
    df = df[~df['Location'].str.upper().str.contains('|'.join(total_keywords), na=False)].reset_index(drop=True)
    
    # Step 2: Forward-fill Location and Count for detail rows
    df['Location'] = df['Location'].fillna(method='ffill')
    df['Count'] = df['Count'].fillna(method='ffill')
    
    # Step 3: Assign Level based on Count (has Count = Municipality, no Count = detail row)
    df['Level'] = df['Count'].apply(lambda x: 'Municipality' if pd.notna(x) else 'Detail')
    
    # Step 4: Extract location hierarchy
    df = extract_location_hierarchy(df, location_col='Location', subtotal_col='Count')
    
    # Step 5: Clean text columns (remove \r)
    text_cols = ['Cluster', 'NFIs_Type', 'NFIs_Unit', 'NFIs_Source', 'Remarks']
    for col in text_cols:
        df[col] = df[col].astype(str).str.replace('\r', ' ').str.strip()
        df[col] = df[col].replace('nan', None)
    
    # Step 6: Remove header rows - multiple conditions
    # Remove if Cluster is "TYPE"
    # Remove if NFIs_Type is "TYPE" or "QTY"
    # Remove if NFIs_Unit is "COST PER UNIT"
    df = df[~((df['Cluster'] == 'TYPE') | 
            (df['NFIs_Type'] == 'TYPE') | 
            (df['NFIs_Type'] == 'QTY') |
            (df['NFIs_Unit'] == 'COST PER UNIT'))].reset_index(drop=True)

    # Also remove rows where both Cluster and NFIs_Type are empty
    df = df[~((df['Cluster'].isna() | (df['Cluster'] == 'None')) & 
            (df['NFIs_Type'].isna() | (df['NFIs_Type'] == 'None')))].reset_index(drop=True)
     # Also remove any row where NFIs_Type is exactly "TYPE"
    df = df[df['NFIs_Type'] != 'TYPE'].reset_index(drop=True)
    
    # Step 7: Fix Level - all should be Municipality since detail rows got removed
    df['Level'] = 'Municipality'
    
    # Step 8: Clean numeric columns
    numeric_cols = ['Families_Affected', 'Families_Assisted', 'NFIs_QTY', 
                   'NFIs_Cost_Per_Unit', 'NFIs_Amount']
    
    for col in numeric_cols:
        df[col] = df[col].astype(str).str.replace(',', '').str.replace('\r', '')
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    # Step 9: Remove flag columns
    columns_to_drop = ['Is_Province_Header', 'Is_Municipality_Header', '_location_upper', '_is_huc', 'Count']
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])
    
    return df

def transform_pre_emptive_evacuation(df):
    """
    Transform Pre-Emptive Evacuation detailed table
    """
    df = df.copy()
    
    # Step 1: Rename columns for consistency
    df = df.rename(columns={
        'Region_Province_Municipality_Barangay': 'Location',
        'Blank': 'Sub-total'  # This column acts as subtotal indicator!
    })
    
    # Step 2: Convert numeric columns
    # IMPORTANT: Sub-total should keep NaN (not convert to 0) so hierarchy logic works
    df['Sub-total'] = pd.to_numeric(df['Sub-total'].astype(str).str.replace(',', ''), errors='coerce')
    
    # Other numeric columns can be 0-filled
    numeric_cols = ['Families', 'Male', 'Female', 'Total']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0).astype(int)
    
    # Step 3: Extract location hierarchy (now has Sub-total column!)
    df = extract_location_hierarchy(df, location_col='Location', subtotal_col='Sub-total')
    
    # Step 4: Add Level column
    df['Level'] = None
    df.loc[df['Barangay'].notna() & (df['Barangay'].astype(str).str.strip() != ''), 'Level'] = 'Barangay'
    
    location_str = df['Location'].astype(str).str.strip().str.upper()
    has_location = df['Location'].notna() & (location_str != '') & (location_str != 'NONE')
    df.loc[(df['Municipality'].notna()) & 
           ((df['Barangay'].isna()) | (df['Barangay'].astype(str).str.strip() == '')) & 
           has_location, 'Level'] = 'Municipality'
    
    # Step 5: Reorder columns
    location_cols = ['Region', 'Province', 'Municipality', 'Barangay', 'Location']
    other_cols = [col for col in df.columns if col not in location_cols]
    df = df[location_cols + other_cols]
    
    return df
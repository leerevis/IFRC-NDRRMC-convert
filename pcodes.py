import pandas as pd
from unidecode import unidecode
from thefuzz import process
import os

# Load PCode reference data
script_dir = os.path.dirname(__file__)
pcode_path = os.path.join(script_dir, 'data', 'phl_adminareas_fixed.csv')
pcode_df = pd.read_csv(pcode_path)

def get_clean_names(origin_column):
    """Clean location names for fuzzy matching."""
    new_column = origin_column.str.casefold()
    strings_to_remove = [
        r"\(.*\)", "city of", "city", "brgy.", "barangay", "region", 
        "province of", "municipality of", "-", r"\*", ","
    ]
    for string in strings_to_remove:
        new_column = new_column.str.replace(string, "", regex=True)
    
    new_column = new_column.str.replace(".", "", regex=False)
    new_column = new_column.apply(unidecode)
    
    # Roman numerals to Arabic
    numeral_dict = {
        r"\si($|\s)": " 1", r"\sii($|\s)": " 2", r"\siii($|\s)": " 3",
        r"\siv($|\s)": " 4", r"\sv($|\s)": " 5", r"\svi($|\s)": " 6",
        r"\svii($|\s)": " 7", r"\sviii($|\s)": " 8", r"\six($|\s)": " 9",
        r"\sx($|\s)": " 10", r"\sxi($|\s)": " 11", r"\sxii($|\s)": " 12",
        r"\sxiii($|\s)": " 13"
    }
    for roman, arabic in numeral_dict.items():
        new_column = new_column.str.replace(roman, arabic, regex=True)
    
    # st. and sta.
    new_column = new_column.str.replace("st.", "san", regex=False)
    new_column = new_column.str.replace("sta.", "santa", regex=False)
    
    new_column = new_column.str.strip()
    return new_column

def add_pcodes(df):
    """
    Add Philippine P-codes and English names (ADM0-ADM4) to transformed dataframe.
    
    Parameters:
    - df: DataFrame with Region, Province, Municipality, Barangay columns
    
    Returns:
    - DataFrame with added P-code and name columns
    """
    output_df = df.copy()
    
    # ADM0 - Country level
    output_df['ADM0_EN'] = 'Philippines'
    output_df['ADM0_PCODE'] = 'PH'
    
    # Check if we have the required columns
    if 'Region' not in output_df.columns:
        return output_df
    
    # Clean names for matching
    output_df['region_clean'] = get_clean_names(output_df['Region'].fillna(''))
    
    has_province = 'Province' in output_df.columns and output_df['Province'].notna().any()
    has_municipality = 'Municipality' in output_df.columns and output_df['Municipality'].notna().any()
    has_barangay = 'Barangay' in output_df.columns and output_df['Barangay'].notna().any()
    
    if has_province:
        output_df['province_clean'] = get_clean_names(output_df['Province'].fillna(''))
    if has_municipality:
        output_df['mun_clean'] = get_clean_names(output_df['Municipality'].fillna(''))
    if has_barangay:
        output_df['brgy_clean'] = get_clean_names(output_df['Barangay'].fillna(''))
    
    # ADM1 - Region level
    region_list = pcode_df['adm1_clean'].dropna().unique().tolist()
    
    def match_region(clean_name):
        if not clean_name or clean_name == '':
            return pd.Series([None, None])
        match = process.extractOne(clean_name, region_list, score_cutoff=80)
        if match:
            matched_name = match[0]
            pcode_row = pcode_df[pcode_df['adm1_clean'] == matched_name].iloc[0]
            return pd.Series([pcode_row['ADM1_EN'], pcode_row['ADM1_new']])
        return pd.Series([None, None])
    
    output_df[['ADM1_EN', 'ADM1_PCODE']] = output_df['region_clean'].apply(match_region)
    
    # ADM2 - Province level
    if has_province:
        province_list = pcode_df['adm2_clean'].dropna().unique().tolist()
        
        def match_province(row):
            if not row['province_clean'] or row['province_clean'] == '':
                return pd.Series([None, None, None, None])
            
            match = process.extractOne(row['province_clean'], province_list, score_cutoff=80)
            if match:
                matched_name = match[0]
                pcode_row = pcode_df[pcode_df['adm2_clean'] == matched_name].iloc[0]
                return pd.Series([
                    pcode_row['ADM2_EN'], 
                    pcode_row['ADM2_new'],
                    pcode_row['ADM1_EN'],
                    pcode_row['ADM1_new']
                ])
            return pd.Series([None, None, None, None])
        
        output_df[['ADM2_EN', 'ADM2_PCODE', 'ADM1_EN_check', 'ADM1_PCODE_check']] = output_df.apply(match_province, axis=1)
        
        # Use province-derived ADM1 if region match failed
        output_df['ADM1_EN'] = output_df['ADM1_EN'].fillna(output_df['ADM1_EN_check'])
        output_df['ADM1_PCODE'] = output_df['ADM1_PCODE'].fillna(output_df['ADM1_PCODE_check'])
        output_df = output_df.drop(['ADM1_EN_check', 'ADM1_PCODE_check'], axis=1)
    
    # ADM3 - Municipality level
    if has_municipality and has_province:
        def match_municipality(row):
            if not row['mun_clean'] or pd.isna(row.get('ADM2_PCODE')):
                return pd.Series([None, None])
            
            # Filter to municipalities within this province
            mun_list = pcode_df[pcode_df['ADM2_new'] == row['ADM2_PCODE']]['adm3_clean'].dropna().unique().tolist()
            if not mun_list:
                return pd.Series([None, None])
                
            match = process.extractOne(row['mun_clean'], mun_list, score_cutoff=80)
            
            if match:
                matched_name = match[0]
                pcode_row = pcode_df[(pcode_df['ADM2_new'] == row['ADM2_PCODE']) & 
                                    (pcode_df['adm3_clean'] == matched_name)]
                if not pcode_row.empty:
                    return pd.Series([pcode_row.iloc[0]['ADM3_EN'], pcode_row.iloc[0]['ADM3_new']])
            return pd.Series([None, None])
        
        output_df[['ADM3_EN', 'ADM3_PCODE']] = output_df.apply(match_municipality, axis=1)
    
    # ADM4 - Barangay level
    if has_barangay and has_municipality:
        def match_barangay(row):
            if not row.get('brgy_clean') or pd.isna(row.get('ADM3_PCODE')):
                return pd.Series([None, None])
            
            # Filter to barangays within this municipality
            brgy_list = pcode_df[pcode_df['ADM3_new'] == row['ADM3_PCODE']]['adm4_clean'].dropna().unique().tolist()
            if not brgy_list:
                return pd.Series([None, None])
                
            match = process.extractOne(row['brgy_clean'], brgy_list, score_cutoff=80)
            
            if match:
                matched_name = match[0]
                pcode_row = pcode_df[(pcode_df['ADM3_new'] == row['ADM3_PCODE']) & 
                                    (pcode_df['adm4_clean'] == matched_name)]
                if not pcode_row.empty:
                    return pd.Series([pcode_row.iloc[0]['ADM4_EN'], pcode_row.iloc[0]['ADM4_new']])
            return pd.Series([None, None])
        
        output_df[['ADM4_EN', 'ADM4_PCODE']] = output_df.apply(match_barangay, axis=1)
    
    # Clean up temporary columns
    cols_to_drop = ['region_clean', 'province_clean', 'mun_clean', 'brgy_clean']
    output_df = output_df.drop([c for c in cols_to_drop if c in output_df.columns], axis=1, errors='ignore')
    
    return output_df
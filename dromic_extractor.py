# dromic_extractor.py

import requests
import pdfplumber
import io
import pandas as pd
import numpy as np
from unidecode import unidecode

def get_clean_names(origin_column):
    """Clean location names for matching (from notebook)."""
    new_column = origin_column.str.casefold()
    strings_to_remove = [
        r"\(.*\)", "city of", "city", "brgy.", "barangay", "region", "-", r"\*"
    ]
    for string in strings_to_remove:
        new_column = new_column.str.replace(string, "", regex=True)
    
    new_column = new_column.apply(unidecode)
    
    numeral_dict = {
        r"\si($|\s)": " 1", r"\sii($|\s)": " 2", r"\siii($|\s)": " 3",
        r"\siv($|\s)": " 4", r"\sv($|\s)": " 5", r"\svi($|\s)": " 6",
        r"\svii($|\s)": " 7", r"\sviii($|\s)": " 8", r"\six($|\s)": " 9",
        r"\sx($|\s)": " 10", r"\sxi($|\s)": " 11", r"\sxii($|\s)": " 12",
        r"\sxiii($|\s)": " 13"
    }
    for roman, arabic in numeral_dict.items():
        new_column = new_column.str.replace(roman, arabic, regex=True)
    
    new_column = new_column.str.replace("st.", "san", regex=False)
    new_column = new_column.str.replace("sta.", "santa", regex=False)
    new_column = new_column.str.strip()
    return new_column


def extract_dromic_table(pdf_path, page_text="NO. OF DAMAGED HOUSES", table_text=None):
    """
    Extract DROMIC table from PDF (adapted from notebook).
    """
    if table_text is None:
        table_text = set([page_text, "Total"])
    
    # Load PDF
    if pdf_path.startswith('http'):
        req_pdf = requests.get(pdf_path)
        pdf = pdfplumber.open(io.BytesIO(req_pdf.content))
    else:
        pdf = pdfplumber.open(pdf_path)
    
    # Find pages with right text and extract tables
    temp_tables = []
    for page in range(len(pdf.pages)):
        if page_text in pdf.pages[page].extract_text():
            for table in pdf.pages[page].find_tables():
                temp_tables.append(pd.DataFrame(table.extract()))
    
    if not temp_tables:
        raise ValueError(f"No tables found with text pattern: {page_text}")
    
    # Find all tables that have the right text
    right_tables = []
    for table in temp_tables:
        for column, values in table.items():
            if table_text.issubset(list(values)):
                right_tables.append(table)
                break
    
    if not right_tables:
        raise ValueError(f"No tables found matching criteria: {table_text}")
    
    # Get correct column names from first table
    first_row = right_tables[0].index[right_tables[0][0] == "GRAND TOTAL"][0]
    header_input = right_tables[0].copy().iloc[0:first_row]
    
    # Extend labels rightward
    for index, row in header_input.iterrows():
        temp_value = ""
        for i in range(len(header_input.columns)):
            if str(header_input.iloc[index, i]) != "None":
                temp_value = str(header_input.iloc[index, i])
            if str(header_input.iloc[index, i]) == "None":
                header_input.iloc[index, i] = temp_value
    
    # Generate new headers
    new_headers = []
    for col in header_input.columns:
        new_headers.append("_".join(header_input[col]))
    
    # Set headers for each table
    for i in range(len(right_tables)):
        right_tables[i] = right_tables[i].set_axis(new_headers, axis=1).drop(range(0, first_row), axis=0)
    
    # Merge into one table
    full_table = pd.concat(right_tables).reset_index(drop=True)
    
    # Data cleaning
    cleaned_table = full_table.copy()
    cleaned_table = cleaned_table.replace(",", "", regex=True)
    cleaned_table = cleaned_table.replace(r"^-$", 0, regex=True)
    
    # Convert numeric columns
    test_df = cleaned_table.copy()
    test_df = test_df.apply(pd.to_numeric, errors="coerce")
    
    for column in test_df:
        if (test_df[column].isna().sum() / len(test_df)) < 0.1:
            cleaned_table[column] = pd.to_numeric(cleaned_table[column])
        else:
            cleaned_table[column] = cleaned_table[column].astype(str)
    
    cleaned_table = cleaned_table.replace("PLGU", "Provincial LGU")
    cleaned_table["clean_name"] = get_clean_names(cleaned_table[cleaned_table.columns[0]])
    
    # Detect admin levels
    admin_table = detect_admin_levels(cleaned_table)
    
    # Add P-codes
    pcoded_table = add_dromic_pcodes(admin_table)
    
    return pcoded_table


def detect_admin_levels(df):
    """Detect administrative levels using counter logic (from notebook)."""
    admin_table = df.copy()
    
    # Identify first numeric column with all non-zero values
    no_0s = admin_table.select_dtypes(include=np.number).all()
    if no_0s.sum() == 0:
        raise ValueError("No numeric column found with all non-zero values")
    first_numcol = no_0s[no_0s == True].index[0]
    
    # GRAND TOTAL -> admin 0
    admin_table["adm0"] = admin_table[admin_table.columns[0]].str.contains("GRAND TOTAL", na=False)
    
    # Upper case but not GRAND TOTAL -> admin 1
    admin_table["adm1"] = (
        (admin_table[admin_table.columns[0]].str.isupper()) & 
        (admin_table["adm0"] != True)
    )
    
    # Load HUC list
    try:
        huc_df = pd.read_csv("data/hucs_and_pcodes.csv")
        admin_table["huc"] = (
            admin_table[admin_table.columns[0]].str.strip().isin(huc_df["city_first"]) | 
            admin_table[admin_table.columns[0]].str.strip().isin(huc_df["city_last"])
        )
        admin_table.loc[admin_table[admin_table.columns[0]] == "Ormoc City", "huc"] = True
        admin_table.loc[admin_table[admin_table.columns[0]] == "City of Manila", "huc"] = False
    except FileNotFoundError:
        admin_table["huc"] = False
    
    # Determine admins 2 and 3 using counter
    admin2_3 = admin_table[
        (admin_table["adm0"] == False) & 
        (admin_table["adm1"] == False) & 
        (admin_table["huc"] == False)
    ].copy().reset_index()
    
    admin2_3["adm2"] = False
    admin2_3["adm3"] = False
    admin2_3["adm3_counter"] = 0
    
    adm2_index = 0
    adm2_value = admin2_3.iloc[adm2_index][first_numcol]
    
    for index, row in admin2_3.iterrows():
        if index == adm2_index:
            admin2_3.loc[admin2_3.index[index], "adm2"] = True
        
        if admin2_3.loc[admin2_3.index[index], "adm2"] == False:
            admin2_3.loc[admin2_3.index[index], "adm3_counter"] = (
                admin2_3.loc[admin2_3.index[index - 1], "adm3_counter"] + 
                admin2_3.iloc[index][first_numcol]
            )
        
        if ((admin2_3.loc[admin2_3.index[index], "adm3_counter"] <= adm2_value) & 
            (admin2_3.loc[admin2_3.index[index], "adm3_counter"] > 0)):
            admin2_3.loc[admin2_3.index[index], "adm3"] = True
        
        if round(admin2_3.loc[admin2_3.index[index], "adm3_counter"]) == round(adm2_value):
            admin2_3.loc[admin2_3.index[index], "adm3_counter"] = 0
            if index + 1 in admin2_3.index:
                admin2_3.loc[admin2_3.index[index + 1], "adm2"] = True
                adm2_index = index + 1
                adm2_value = admin2_3.iloc[index + 1][first_numcol]
    
    # Merge back
    admin_table = admin_table.reset_index()
    admin_table_levels = pd.merge(
        admin_table, 
        admin2_3[["index", "adm2", "adm3"]], 
        on="index", 
        how="outer"
    )
    
    return admin_table_levels


def add_dromic_pcodes(df):
    """Add P-codes using admin level flags (from notebook)."""
    import os
    
    # Load pcode reference
    pcode_path = os.path.join("data", "phl_adminareas_fixed.csv")
    pcode_df = pd.read_csv(pcode_path)
    
    dromic_pcoded = df.copy()
    
    # Admin 0
    dromic_pcoded["ADM0_new"] = "PH"
    
    # Admin 2
    admin2 = dromic_pcoded[dromic_pcoded["adm2"] == True].copy()
    pcode_level = pcode_df[["ADM2_new", "adm2_clean"]].drop_duplicates()
    admin2 = pd.merge(admin2, pcode_level, left_on="clean_name", right_on="adm2_clean", how="left").drop("adm2_clean", axis=1)
    dromic_pcoded = pd.merge(dromic_pcoded, admin2[["index", "ADM2_new"]], on="index", how="outer")
    
    # Admin 1
    dromic_pcoded["ADM1_new"] = dromic_pcoded["ADM2_new"].str[:-2]
    
    # Fill admin 1 up and admin 3 down
    for index, row in dromic_pcoded.iterrows():
        if row["adm1"] == True:
            dromic_pcoded.loc[dromic_pcoded.index[index], "ADM1_new"] = dromic_pcoded.loc[dromic_pcoded.index[index + 1], "ADM1_new"]
        if (row["adm3"] == True) | (row["huc"] == True):
            dromic_pcoded.loc[dromic_pcoded.index[index], "ADM2_new"] = dromic_pcoded.loc[dromic_pcoded.index[index - 1], "ADM2_new"]
            dromic_pcoded.loc[dromic_pcoded.index[index], "ADM1_new"] = dromic_pcoded.loc[dromic_pcoded.index[index - 1], "ADM1_new"]
    
    # Admin 3
    admin3_df = pd.DataFrame()
    for admin2_code in dromic_pcoded["ADM2_new"].dropna().unique():
        pcode_sel = pcode_df[pcode_df["ADM2_new"] == admin2_code]
        pcode_sel = pcode_sel.drop_duplicates(subset = ["adm3_clean", "ADM3_new"])
        dromic_sel = dromic_pcoded[(dromic_pcoded["ADM2_new"] == admin2_code) & (dromic_pcoded["adm3"] == True)]
        joined_sel = pd.merge(dromic_sel, pcode_sel[["adm3_clean", "ADM3_new"]], left_on="clean_name", right_on="adm3_clean", how="left").drop("adm3_clean", axis=1)
        admin3_df = pd.concat([admin3_df, joined_sel], axis=0)
    
    dromic_pcoded = pd.merge(dromic_pcoded, admin3_df[["index", "ADM3_new"]], on="index", how="left")
    
    return dromic_pcoded
import pdfplumber
import pandas as pd
import os
import re

# ---------------- PDF TO EXCEL ------------------RUN THIS FIRST

ELECTRIC_KEYWORDS = ['electric', 'light', 'power', 'energy']
GAS_KEYWORDS = ['gas', 'works']

def categorize_utility(name):
    name_lower = name.lower()
    if any(k in name_lower for k in ELECTRIC_KEYWORDS):
        return "Electric"
    elif any(k in name_lower for k in GAS_KEYWORDS):
        return "Gas"
    return "Unknown"

def extract_company_tables(pdf_path, year):
    electric_tables = []
    gas_tables = []

    with pdfplumber.open(pdf_path) as pdf:
        for page_num, page in enumerate(pdf.pages, start=1):
            text = page.extract_text()
            tables = page.extract_tables()

            if not tables or not text:
                continue

            company_name = text.strip().split('\n')[0].strip()
            utility_type = categorize_utility(company_name)

            for table in tables:
                if not table or len(table) < 2:
                    continue

                header = table[0]
                rows = table[1:]

                seen = {}
                clean_header = []
                for col in header:
                    col = col.replace('\n', ' ').strip() if isinstance(col, str) else col
                    if col in seen:
                        seen[col] += 1
                        col = f"{col}_{seen[col]}"
                    else:
                        seen[col] = 0
                    clean_header.append(col)

                try:
                    df = pd.DataFrame(rows, columns=clean_header)
                    df.dropna(axis=0, how='all', inplace=True)
                    df.dropna(axis=1, how='all', inplace=True)

                    df.insert(0, 'Company', company_name)
                    df.insert(1, 'Page', page_num)
                    df["Year"] = int(year)

                    flat_text = " ".join(text.split()).lower()
                    match = re.search(r'(residential(?: heating)?)\D*(\d{3,5})\s*(kwh|mcf)', flat_text)
                    if match:
                        df["Rate Classification"] = match.group(1).title()
                        df["Usage Level"] = int(match.group(2))
                        df["Unit"] = match.group(3).lower()

                    if utility_type == "Electric":
                        electric_tables.append(df)
                    elif utility_type == "Gas":
                        gas_tables.append(df)

                except Exception as e:
                    print(f" Skipping malformed table on page {page_num} in {os.path.basename(pdf_path)}: {e}")
                    continue

    return (
        pd.concat(electric_tables, ignore_index=True) if electric_tables else None,
        pd.concat(gas_tables, ignore_index=True) if gas_tables else None
    )

def batch_process_pdfs(pdf_folder, output_folder):
    os.makedirs(output_folder, exist_ok=True)

    for filename in os.listdir(pdf_folder):
        if filename.lower().endswith(".pdf"):
            pdf_path = os.path.join(pdf_folder, filename)
            year = ''.join(filter(str.isdigit, filename))[:4]

            print(f" Processing {filename}")
            electric_df, gas_df = extract_company_tables(pdf_path, year)

            if electric_df is not None:
                electric_out = os.path.join(output_folder, f"cleaned_electric_rates_{year}.xlsx")
                electric_df.to_excel(electric_out, index=False)
                print(f" Electric → {electric_out}")
            if gas_df is not None:
                gas_out = os.path.join(output_folder, f"cleaned_gas_rates_{year}.xlsx")
                gas_df.to_excel(gas_out, index=False)
                print(f" Gas → {gas_out}")
            if electric_df is None and gas_df is None:
                print(f"️ No usable data in {filename}")

# ---------------- CLEANING LOGIC ------------------

def clean_excel_file(file_path, output_path):
    try:
        df = pd.read_excel(file_path)
        is_gas = "gas" in file_path.lower()

        # Trim extra header/summary
        if is_gas:
            df = df.drop(df.index[1:41], errors='ignore')
            df = df.drop(df.columns[1:18], axis=1, errors='ignore')
        else:
            df = df.drop(df.index[1:25], errors='ignore')

        # Strip whitespace
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

        # Drop fully empty columns
        df.dropna(axis=1, how='all', inplace=True)

        # Drop rows that are mostly empty (>75%)
        df = df[df.isnull().mean(axis=1) <= 0.75]

        # Reorder core columns
        priority = ['Company', 'Rate Classification', 'Usage Level', 'Unit', 'Year']
        existing = [col for col in priority if col in df.columns]
        remaining = [col for col in df.columns if col not in existing]
        df = df[existing + remaining] if existing else df

        df.reset_index(drop=True, inplace=True)
        df.to_excel(output_path, index=False)
        print(f" Cleaned: {output_path}")

    except Exception as e:
        print(f"Error on {file_path}: {e}")

def batch_clean_excel_files(input_folder, output_folder):
    os.makedirs(output_folder, exist_ok=True)

    for filename in os.listdir(input_folder):
        if filename.endswith(".xlsx"):
            input_path = os.path.join(input_folder, filename)
            output_path = os.path.join(output_folder, filename)
            clean_excel_file(input_path, output_path)

# ---------------- RUN ------------------

pdf_folder = "/Users/srutha/Documents/Research Project/PDFs"
extracted_output = "/Users/srutha/Documents/Research Project/ExcelOutputs"
final_output = "/Users/srutha/Documents/Research Project/ExcelOutputs/FinalCleaned"

# Step 1: Extract from PDFs
batch_process_pdfs(pdf_folder, extracted_output)

# Step 2: Clean Excel files (trim + reorder + remove sparse)
batch_clean_excel_files(extracted_output, final_output)



run this after above part

import pandas as pd
import os

def drop_summary_rows(file_path, output_path):
    try:
        df = pd.read_excel(file_path)

        # Drop rows with summary marker in 'Company' column
        df = df[df['Company'] != "Gas Utilities Comparison - GCR Companies"]

        # Optional: reset index and clean whitespace
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        df.reset_index(drop=True, inplace=True)

        df.to_excel(output_path, index=False)
        print(f" Summary rows removed and saved: {output_path}")

    except Exception as e:
        print(f" Error processing {file_path}: {e}")

def batch_drop_summaries(input_folder, output_folder):
    os.makedirs(output_folder, exist_ok=True)

    for file in os.listdir(input_folder):
        if file.endswith(".xlsx"):
            input_path = os.path.join(input_folder, file)
            output_path = os.path.join(output_folder, file)
            drop_summary_rows(input_path, output_path)

#CHANGE THE PATH TO MATCH YOUR FILES
input_folder = "/Users/srutha/Documents/Research Project/ExcelOutputs/FinalCleaned"
output_folder = "/Users/srutha/Documents/Research Project/ExcelOutputs/NoSummary"


batch_drop_summaries(input_folder, output_folder)

###############################################################################################
# run above part then this below

import pandas as pd
import os

def merge_excel_files(folder_path, output_path, file_type="xlsx"):
    merged_data = []

    for file in os.listdir(folder_path):
        if file.endswith(f".{file_type}"):
            file_path = os.path.join(folder_path, file)
            try:
                df = pd.read_excel(file_path)
                df["Source File"] = file  # Add filename tag
                merged_data.append(df)
            except Exception as e:
                print(f" Failed to read {file}: {e}")

    if merged_data:
        final_df = pd.concat(merged_data, ignore_index=True)
        final_df.to_excel(output_path, index=False)
        print(f" Merged and saved to: {output_path}")
    else:
        print(" No files merged — folder may be empty or files invalid.")
# PUT THE PATH WITH ALL THE PDFS IN ONE FILE
input_folder = "/Users/srutha/Documents/Research Project/ExcelOutputs/FinalCleaned"
output_file = "/Users/srutha/Documents/Research Project/ExcelOutputs/merged_rates.xlsx"

merge_excel_files(input_folder, output_file)

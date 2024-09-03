import pandas as pd
import json
from tqdm import tqdm
import re
from slugify import slugify

# File paths
# excel_file = './Documents/state_data.xlsx'
excel_file = "C:/python/py_projects/Documents/US_STATES_01.xlsx"
output_file = 'Documents/State Index.json'

def fill_order_values(df, order_column):
    """ Fills missing order values by continuing the sequence from the previous number """
    last_order = 0
    for i, row in df.iterrows():
        if pd.isna(row[order_column]):
            last_order += 1
            df.at[i, order_column] = last_order
        else:
            last_order = int(row[order_column])
    return df

def is_seo_friendly(slug):
    """ Check if a slug is SEO friendly using a simple regex """
    return bool(re.match(r'^[a-z0-9]+(?:-[a-z0-9]+)*$', slug))

def convert_to_slug(value):
    """ Convert a given value to a slug, ensuring it's treated as a string """
    if pd.notna(value):
        return slugify(str(value))
    return ""

def convert_excel_to_json(state_df, related_dfs):
    json_data = []

    try:
        # Group data by state and process each state row with a progress bar
        for _, state_row in tqdm(state_df.iterrows(), total=len(state_df), desc="Processing States"):
            state_name = state_row[state_df.columns[0]]  # Assuming the first column is the state name
            state_obj = {}
            for col in state_df.columns:
                state_obj[col] = state_row[col] if pd.notna(state_row[col]) else ""

            for tab_name, df in related_dfs.items():
                state_obj[tab_name] = []
                state_related = df[df[df.columns[0]] == state_row[state_df.columns[0]]]
                state_related = fill_order_values(state_related, 'Order')
                
                for _, related_row in tqdm(state_related.iterrows(), total=len(state_related), desc=f"Processing {tab_name} for {state_name}"):
                    related_obj = {}
                    for col in df.columns:
                        if col == 'PostalCodeName':
                            related_obj[col] = str(int(related_row[col])) if pd.notna(related_row[col]) else ""
                        elif col == 'Slug':
                            # slug_value = str(related_row[col]).rstrip('.0') if pd.notna(related_row[col]) else ""
                            slug_value = str(related_row[col]) if pd.notna(related_row[col]) else ""
                            if not is_seo_friendly(slug_value):
                                # Find the column with 'Name' in its title and generate a slug from it
                                name_column = next((c for c in df.columns if 'Name' in c), None)
                                if name_column:
                                    slug_value = convert_to_slug(related_row[name_column])
                            related_obj[col] = slug_value
                        elif col == 'Order':
                            related_obj[col] = int(related_row[col]) if pd.notna(related_row[col]) else 0
                        elif col == 'Active':
                            related_obj[col] = related_row[col] if pd.notna(related_row[col]) else 1
                        else:
                            related_obj[col] = related_row[col] if pd.notna(related_row[col]) else ""
                    if 'Active' not in df.columns:
                        related_obj['Active'] = 1  # Mark as 1 if Active column is missing
                    state_obj[tab_name].append(related_obj)

            json_data.append(state_obj)

        print("JSON conversion completed successfully!")
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

    return json_data

def save_json_to_file(json_result, output_file):
    try:
        with open(output_file, 'w', encoding='utf-8') as json_file:
            json.dump(json_result, json_file, ensure_ascii=False, indent=4)
        print(f"JSON saved successfully to {output_file}")
    except Exception as e:
        print(f"Failed to save JSON file: {e}")

# Main function
def main():
    try:
        # Load the main State sheet
        state_df = pd.read_excel(excel_file, sheet_name='State')

        # Load the related sheets dynamically
        related_dfs = {}
        sheet_names = pd.ExcelFile(excel_file).sheet_names
        for sheet_name in tqdm(sheet_names, desc="Loading Sheets"):
            if sheet_name != 'State':
                df = pd.read_excel(excel_file, sheet_name=sheet_name)
                related_dfs[sheet_name] = df

        # Convert Excel to JSON
        json_result = convert_excel_to_json(state_df, related_dfs)

        # Save JSON to a file if conversion was successful
        if json_result:
            save_json_to_file(json_result, output_file)
    except Exception as e:
        print(f"An error occurred while processing the Excel file: {e}")

if __name__ == "__main__":
    main()

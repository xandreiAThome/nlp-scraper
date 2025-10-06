import pandas as pd
from pathlib import Path
import os

def create_parallel_corpus():
    # Define the language pairs to create
    language_pairs = [
        ("English", "Bikolano"),
        ("English", "Cebuano"), 
        ("English", "Spanish"),
        ("English", "Ilokano"),
        ("Cebuano", "Bikolano"),
        ("Cebuano", "Spanish"),
        ("Cebuano", "Ilokano")
    ]
    
    language_data = {}
    
    # Read TSV files
    pathlist = Path("Verses").glob('**/*.tsv')
    for path in pathlist:
        path_in_string = str(path)
        print("Reading file:", path_in_string)
        
        # Extract language
        filename = os.path.basename(path_in_string)
        language = filename.replace("Bible_", "").replace(".tsv", "")
        
        # Read the TSV file
        with open(path_in_string, "r", encoding="utf-8") as language_file:
            df = pd.read_csv(language_file, sep="\t")
            
            # Convert all merge columns to string
            df['Book'] = df['Book'].astype(str)
            df['Chapter'] = df['Chapter'].astype(str)
            df['Verse'] = df['Verse'].astype(str)
            
            language_data[language] = df
            print(f"Loaded {len(df)} verses for {language}")
            print(f"Sample data types - Book: {df['Book'].dtype}, Chapter: {df['Chapter'].dtype}, Verse: {df['Verse'].dtype}")
    
    output_dir = "Parallel_Corpus"
    os.makedirs(output_dir, exist_ok=True)
    
    # Create parallel corpora
    for lang1, lang2 in language_pairs:
        if lang1 in language_data and lang2 in language_data:
            print(f"\nCreating parallel corpus: {lang1} - {lang2}")
            
            # Get the dataframes
            df1 = language_data[lang1]
            df2 = language_data[lang2]
            
            verse_col1 = df1.columns[-1]
            verse_col2 = df2.columns[-1]
            
            print(f"Using column '{verse_col1}' for {lang1} and '{verse_col2}' for {lang2}")
            
            # Merge
            merged_df = pd.merge(
                df1, 
                df2, 
                on=['Book', 'Chapter', 'Verse'], 
                how='outer',
                suffixes=(f'_{lang1}', f'_{lang2}')
            )
            
            # Create the final output format
            result_df = pd.DataFrame()
            result_df['Book'] = merged_df['Book']
            result_df['Chapter'] = merged_df['Chapter']
            result_df['Verse'] = merged_df['Verse']
            
            # Handle missing verses
            lang1_verse_col = f'{verse_col1}_{lang1}'
            lang2_verse_col = f'{verse_col2}_{lang2}'
            
            result_df[lang1] = merged_df[lang1_verse_col].fillna('<no verse>')
            result_df[lang2] = merged_df[lang2_verse_col].fillna('<no verse>')
            
            # Convert Chapter and Verse to numeric for sorting
            result_df['Chapter_num'] = pd.to_numeric(result_df['Chapter'], errors='coerce')
            result_df['Verse_num'] = pd.to_numeric(result_df['Verse'], errors='coerce')
            
            result_df = result_df.sort_values(['Book', 'Chapter_num', 'Verse_num']).reset_index(drop=True)
            result_df = result_df.drop(['Chapter_num', 'Verse_num'], axis=1)
            
            # Save to Excel
            excel_filename = f"{output_dir}/{lang1}_{lang2}_Parallel.xlsx"
            result_df.to_excel(excel_filename, index=False)
            print(f"Saved: {excel_filename}")
            
        else:
            missing_langs = []
            if lang1 not in language_data:
                missing_langs.append(lang1)
            if lang2 not in language_data:
                missing_langs.append(lang2)
            print(f"Warning: Missing data for {', '.join(missing_langs)}. Skipping {lang1}-{lang2} pair.")
            
# Helper function to debug the file structure and column names
def debug_file_structure():
    pathlist = Path("Verses").glob('**/*.tsv')
    for path in pathlist:
        path_in_string = str(path)
        print(f"\nFile: {path_in_string}")
        
        try:
            with open(path_in_string, "r", encoding="utf-8") as language_file:
                df = pd.read_csv(language_file, sep="\t", nrows=5)
                print(f"Columns: {list(df.columns)}")
                print(f"Data types: {df.dtypes.to_dict()}")
                print("First few rows:")
                print(df.head())
        except Exception as e:
            print(f"Error reading file: {e}")

if __name__ == "__main__":
    debug_file_structure()
    
    create_parallel_corpus()
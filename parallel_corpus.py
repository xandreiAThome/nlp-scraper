import pandas as pd
from pathlib import Path
import os
import re

def parse_verse_range(verse_str):
    """
    Parse a verse string and return a set of individual verse numbers it covers.
    Examples: "1" -> {1}, "1-4" -> {1,2,3,4}
    """
    verse_str = str(verse_str).strip()
    
    if '-' in verse_str:
        # It's a range like "1-4"
        try:
            start, end = verse_str.split('-', 1)
            start_num = int(start.strip())
            end_num = int(end.strip())
            return set(range(start_num, end_num + 1))
        except ValueError:
            # If parsing fails, treat as single verse
            try:
                return {int(verse_str)}
            except ValueError:
                return set()
    else:
        # Single verse number
        try:
            return {int(verse_str)}
        except ValueError:
            return set()

def consolidate_verses(df):
    """
    For each unique (Book, Chapter, Verse) where Verse is a range,
    find all individual verses that should be concatenated.
    Returns a mapping: (Book, Chapter, verse_range) -> list of verse numbers to concatenate
    """
    verse_map = {}
    
    # Group by Book and Chapter
    for (book, chapter), group in df.groupby(['Book', 'Chapter']):
        verses_in_chapter = group['Verse'].unique()
        
        # Find verse ranges
        for verse_str in verses_in_chapter:
            verse_nums = parse_verse_range(verse_str)
            if len(verse_nums) > 1:  # It's a range
                verse_map[(book, chapter, verse_str)] = sorted(verse_nums)
    
    return verse_map

def align_verses_for_merge(df1, df2):
    """
    Align two dataframes by handling verse ranges.
    If df1 has "1-4" and df2 has individual verses 1,2,3,4,
    concatenate df2's verses to create a "1-4" row.
    """
    # Find verse ranges in each dataframe
    ranges1 = consolidate_verses(df1)
    ranges2 = consolidate_verses(df2)
    
    df1_aligned = df1.copy()
    df2_aligned = df2.copy()
    
    # Process df1's ranges: concatenate matching verses from df2
    for (book, chapter, verse_range), verse_nums in ranges1.items():
        text_col = df2_aligned.columns[-1]  
        texts = []
        verses_to_remove = []
        
        # Check each verse in the range individually
        for v in verse_nums:
            matching_verse = df2_aligned[
                (df2_aligned['Book'] == book) & 
                (df2_aligned['Chapter'] == chapter) & 
                (df2_aligned['Verse'] == str(v))
            ]
            
            if len(matching_verse) > 0:
                text = str(matching_verse[text_col].iloc[0])
                # Handle empty, NaN, or missing content
                if text and text.lower() not in ['nan', 'none', '']:
                    texts.append(text.strip())
                else:
                    texts.append('<missing verse>')
                verses_to_remove.append(str(v))
            else:
                texts.append('<missing verse>')
        
        if verses_to_remove:
            concatenated_text = ' '.join(texts)
            
            # Remove individual verse rows from df2
            df2_aligned = df2_aligned[~(
                (df2_aligned['Book'] == book) & 
                (df2_aligned['Chapter'] == chapter) & 
                (df2_aligned['Verse'].astype(str).isin(verses_to_remove))
            )]
            
            # Add the concatenated row
            new_row = {
                'Book': book,
                'Chapter': chapter,
                'Verse': verse_range,
                text_col: concatenated_text
            }
            df2_aligned = pd.concat([df2_aligned, pd.DataFrame([new_row])], ignore_index=True)
    
    # Process df2's ranges: concatenate matching verses from df1
    for (book, chapter, verse_range), verse_nums in ranges2.items():
        text_col = df1_aligned.columns[-1]  # Last column is the text
        texts = []
        verses_to_remove = []
        
        # Check each verse in the range individually
        for v in verse_nums:
            matching_verse = df1_aligned[
                (df1_aligned['Book'] == book) & 
                (df1_aligned['Chapter'] == chapter) & 
                (df1_aligned['Verse'] == str(v))
            ]
            
            if len(matching_verse) > 0:
                text = str(matching_verse[text_col].iloc[0])
                # Handle empty, NaN, or missing content
                if text and text.lower() not in ['nan', 'none', '']:
                    texts.append(text.strip())
                else:
                    texts.append('<missing verse>')
                verses_to_remove.append(str(v))
            else:
                texts.append('<missing verse>')
        
        if verses_to_remove:
            concatenated_text = ' '.join(texts)
            
            # Remove individual verse rows from df1
            df1_aligned = df1_aligned[~(
                (df1_aligned['Book'] == book) & 
                (df1_aligned['Chapter'] == chapter) & 
                (df1_aligned['Verse'].astype(str).isin(verses_to_remove))
            )]
            
            # Add the concatenated row
            new_row = {
                'Book': book,
                'Chapter': chapter,
                'Verse': verse_range,
                text_col: concatenated_text
            }
            df1_aligned = pd.concat([df1_aligned, pd.DataFrame([new_row])], ignore_index=True)
    
    return df1_aligned, df2_aligned

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
            
            # Align verses (handle merged verses by concatenating individual ones)
            print(f"Aligning verses between {lang1} and {lang2}...")
            df1, df2 = align_verses_for_merge(df1, df2 )
            
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
            
            # Save to TSV
            out_filename = f"{output_dir}/{lang1}_{lang2}_Parallel.tsv"
            result_df.to_csv(out_filename, sep='\t', index=False)
            print(f"Saved: {out_filename}")
            
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
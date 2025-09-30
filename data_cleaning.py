import pcre2
import os
import time
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor

VERSE_RULES = [
    (r"(</span>|</div>)", r"\1\n"),  # TOKENIZE closing span/div
    (r".*(<span class=\"ChapterContent_label__R2PLt\">((\d|,|-)+)</span>)", r"@\2"),  # EXTRACT VERSE NUMBER
    (r".*(<span class=\"ChapterContent_content__RrUqA\">([^<]+)</span>)", r"\2"),  # EXTRACT VERSE CONTENT
    # (r"^.*[<>].*$\R?", r""),  # Get rid of unnecessary HTML
    (r"\r\n?", r"\n"),  # UNIFY NEWLINES
    (r" +", r" "),  # NORMALIZE WHITESPACE
    (r"\[|\]", r""),  # REMOVE SQUARE BRACKETS
    (r"Profetizada por Jesús Anunciada por el ángel Sentido cristiano Véanse la tabla y en la", r""),
    # REMOVE SPANISH REF
    (r"@((\d|,|-)+)([^@]+)", r"\n\\Verse \1:\3"),  # SEGMENT BY VERSE
    (r"^\s*$\R?", r""),  # REMOVE EMPTY LINES
]

compiled_rules = [(pcre2.compile(p, pcre2.MULTILINE), r) for p, r in VERSE_RULES]

def apply_rules(text: str, rules) -> str:
    for pattern_str, repl in rules:
        pattern = pcre2.compile(pattern_str, pcre2.MULTILINE)
        text = pattern.sub(repl, text)
    return text.strip()

def process_file(file, input_path, verses_path, sentences_path):
    with file.open("r", encoding="utf-8") as f:
        text = f.read()

    print("Processing: " + file.name)
    verse_segment = apply_rules(text, VERSE_RULES)

    relative = file.relative_to(input_path).with_suffix(".txt")
    verse_file = verses_path / relative
    sentence_file = sentences_path / relative

    verse_file.parent.mkdir(parents=True, exist_ok=True)
    sentence_file.parent.mkdir(parents=True, exist_ok=True)

    with verse_file.open("w", encoding="utf-8") as f:
        f.write(verse_segment)

    return str(relative)

def choose_workers(files):
    total_size = sum(f.stat().st_size for f in files)
    avg_size = total_size / len(files)

    if len(files) > 500 and avg_size < 50_000:  # many small files (<50 KB)
        print("Using single process")
        return 1
    else:
        max_workers = min(os.cpu_count() or 4, 4)  # cap at 4
        print(f"Using {max_workers} workers")
        return max_workers

def segment_verses(input_folder, verses_folder, sentence_folder, workers=None):
    input_path = Path(input_folder).resolve()
    verses_path = Path(verses_folder).resolve()
    sentences_path = Path(sentence_folder).resolve()

    files = list(input_path.rglob("*.html"))
    print(f"{len(files)} HTML files to process under {input_path}")

    if not files:
        print("No HTML files found. Check path/extension.")
        return

    if workers is None:
        workers = choose_workers(files)

    start = time.time()

    files = files[:1]

    if workers == 1:
        for f in files:
            result = process_file(f, input_path, verses_path, sentences_path)
            print("Processed:", result)
    else:
        with ProcessPoolExecutor(max_workers=workers) as executor:
            for result in executor.map(
                process_file,
                files,
                [input_path] * len(files),
                [verses_path] * len(files),
                [sentences_path] * len(files)
            ):
                print("Processed:", result)

    print("Finished in", round(time.time() - start, 2), "seconds")

if __name__ == "__main__":
    segment_verses("Original Text", "Verses", "Sentences")



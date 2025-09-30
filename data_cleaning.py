import regex as re   # <-- use regex instead of re
import os
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor

VERSE_RULES = [
    (re.compile(r"(</span>|</div>)"), r"\1\r\n"),
    (re.compile(r'<span class="ChapterContent_label__R2PLt">((\d|,|-)+)</span>'), r"@\1 "),
    (re.compile(r'<span class="ChapterContent_content__RrUqA">([^<]+)</span>'), r"\1"),
    (re.compile(r"<[^>]+>"), r""),
    (re.compile(r"[\[\]]"), r""),
    (re.compile(r"  +"), r" "),
    (re.compile(r"Profetizada por Jesús Anunciada por el ángel Sentido cristiano Véanse la tabla y en la"), r""),
]

SENTENCE_RULES = [
    (re.compile(r'<span class="ChapterContent_content__RrUqA">([^<]+)</span>'), r"\1"),
    (re.compile(r"<[^>]+>"), r""),
    (re.compile(r"[\[\]]"), r""),
    (re.compile(r"  +"), r" "),
    (re.compile(r"Profetizada por Jesús Anunciada por el ángel Sentido cristiano Véanse la tabla y en la"), r""),
    (re.compile(r'("?[^.!?]+[.!?]"?)(?=\s+[A-ZÁÉÍÓÚÑ0-9])'), r"\1\r\n"),
]

def apply_rules(text: str, rules) -> str:
    for pattern, repl in rules:
        text = pattern.sub(repl, text)
    return text.strip()

def process_file(file, input_path, verses_path, sentences_path):
    with file.open("r", encoding="utf-8") as f:
        text = f.read()

    verse_segment = apply_rules(text, VERSE_RULES)
    sentence_segment = apply_rules(text, SENTENCE_RULES)

    # keep relative path but use .txt extension
    relative = file.relative_to(input_path).with_suffix(".txt")
    verse_file = verses_path / relative
    sentence_file = sentences_path / relative

    verse_file.parent.mkdir(parents=True, exist_ok=True)
    sentence_file.parent.mkdir(parents=True, exist_ok=True)

    with verse_file.open("w", encoding="utf-8") as f:
        f.write(verse_segment)
    with sentence_file.open("w", encoding="utf-8") as f:
        f.write(sentence_segment)

    return str(relative)

def segment_verses(input_folder, verses_folder, sentence_folder, workers=None):
    input_path = Path(input_folder).resolve()
    verses_path = Path(verses_folder).resolve()
    sentences_path = Path(sentence_folder).resolve()

    files = list(input_path.rglob("*.html"))
    print(f"Found {len(files)} HTML files to process under {input_path}")

    if not files:
        print("⚠️ No HTML files found. Check path/extension.")
        return

    if workers is None:
        workers = os.cpu_count() or 4

    with ProcessPoolExecutor(max_workers=workers) as executor:
        for result in executor.map(
            lambda f: process_file(f, input_path, verses_path, sentences_path),
            files
        ):
            print(f"Processed: {result}")

if __name__ == "__main__":
    segment_verses("Original Text", "Verses", "Sentences")

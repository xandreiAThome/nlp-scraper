import pcre2
import csv
import os
import time
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor


VERSE_RULES = [
    (
        r"<span class=\"ChapterContent_label__R2PLt\">([\d,-]+)</span>",
        "\r\n@$1\r\n",
    ),  # Extract Verse Number
    (
        r"<span class=\"ChapterContent_content__RrUqA\">([^<]+)<\/span>",
        "\r\n%$1\r\n",
    ),  # Extract Verse Content
    (r"^[^@%].*$\R?", r""),  # Clean-up of unnecessary HTML
    (
        r"%|\R|\[|\]|Profetizada por Jesús Anunciada por el ángel Sentido cristiano Véanse la tabla y en la",
        r" ",
    ),  # Get rid of unnecessary elements
    (r" +", r" "),  # Clean-up of unnecessary WHITESPACE
    (r"@([\d,-]+)([^@]+)", "\nVerse $1:$2"),  # Segment by Verse
]

SENTENCE_RULES = [
    (
        r"<span class=\"ChapterContent_content__RrUqA\">([^<]+)<\/span>",
        "\r\n%$1\r\n",
    ),  # Extract Verse Content
    (r"^[^%].*$\R?", r""),  # Clean-up of unnecessary HTML
    (
        r"%|\R|\[|\]|Profetizada por Jesús Anunciada por el ángel Sentido cristiano Véanse la tabla y en la ",
        r" ",
    ),  # Get rid of unnecessary elements
    (r" +", r" "),  # Clean-up of unnecessary WHITESPACE
    (r" ((?-i)[^\.\!\?]+[\.\!\?]”?(?=\s+[A-Z]))", "$1\n"),  # Segment by Sentence
    (r"^\s|(?<=“) ", ""),  # Clean-up of generated WHITESPACE
]

SENTENCE_ONLY_RULE = [
    (r" ((?-i)[^\.\!\?]+[\.\!\?]”?(?=\s+[A-Z]))", "$1\n"),  # Segment by Sentence
    (r"^\s|(?<=“) ", ""),  # Clean-up of generated WHITESPACE
]


def apply_rules(text: str, rules) -> str:
    for pattern_str, repl in rules:
        pattern = pcre2.compile(pattern_str, pcre2.MULTILINE)
        text = pattern.sub(repl, text)
    return text.strip()


def process_file(file, verses_path, sentences_path):
    with file.open("r", encoding="utf-8") as f:
        text = f.read()

    print("Processing: " + file.name)
    verse_segment = apply_rules(text, VERSE_RULES)
    # sentence_segment = apply_rules(text, SENTENCE_RULES)

    filename = os.path.basename(file)
    name_no_ext = filename.rsplit(".", 1)[0]
    name_parts = name_no_ext.split(".")

    if len(name_parts) != 4:
        print(f"Skipping file with unexpected name format: {filename} -> {name_parts}")
        return "N/A"

    book, chapter, ver, lang = name_parts

    verse_file = (verses_path / lang).with_suffix(".tsv")
    sentence_file = (sentences_path / lang).with_suffix(".tsv")

    verse_file.parent.mkdir(parents=True, exist_ok=True)
    sentence_file.parent.mkdir(parents=True, exist_ok=True)

    # Only write header if file does not exist or is empty
    write_header_verse = not verse_file.exists() or verse_file.stat().st_size == 0
    write_header_sentence = (
        not sentence_file.exists() or sentence_file.stat().st_size == 0
    )
    with verse_file.open("a", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter="\t")
        if write_header_verse:
            writer.writerow(["Book", "Chapter", "Verse", "Text"])

        for row in verse_segment.splitlines():
            if row.startswith("Verse "):
                verse_num, verse_text = row[6:].split(":", 1)
                writer.writerow([book, chapter, verse_num, verse_text.strip()])
                sentence_segment = apply_rules(verse_text.strip(), SENTENCE_ONLY_RULE)
                with sentence_file.open("a", encoding="utf-8") as sf:
                    writer_s = csv.writer(sf, delimiter="\t")
                    if write_header_sentence:
                        writer_s.writerow(
                            ["Book", "Chapter", "Verse", "Sentence", "Text"]
                        )
                        write_header_sentence = (
                            False  # Ensure header is written only once
                        )
                    for idx, sentence in enumerate(sentence_segment.splitlines()):
                        writer_s.writerow(
                            [book, chapter, verse_num, idx + 1, sentence.strip()]
                        )

    # with sentence_file.open("a", encoding="utf-8") as f:
    #     f.write(sentence_segment)

    return str(filename)


def choose_workers(files):
    total_size = sum(f.stat().st_size for f in files)
    avg_size = total_size / len(files)

    if len(files) > 500 and avg_size < 50_000:
        print("Using single process")
        return 1
    else:
        max_workers = min(os.cpu_count() or 4, 4)
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

    # Made it 1 worker for now because of race conditions on writing to same file
    if workers:
        for f in files:
            result = process_file(f, verses_path, sentences_path)
            print("Processed:", result)
    else:
        with ProcessPoolExecutor(max_workers=workers) as executor:
            for result in executor.map(
                process_file,
                files,
                [verses_path] * len(files),
                [sentences_path] * len(files),
            ):
                print("Processed:", result)

    print("Finished in", round(time.time() - start, 2), "seconds")


if __name__ == "__main__":
    segment_verses("Original-Text", "Verses", "Sentences")

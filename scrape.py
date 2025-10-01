from playwright.sync_api import sync_playwright
import json, os, urllib.parse, time
import smtplib
from email.message import EmailMessage
from dotenv import load_dotenv

# Load environment variables from .env in project root
load_dotenv()

DELAY = 1  # seconds

with open("scrape-info/1/data.json", "r", encoding="utf-8") as f:
    data = json.load(f)

custom_ua = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:142.0) Gecko/20100101 Firefox/142.0"
)

with sync_playwright() as pw:
    browser = pw.chromium.launch(headless=True)
    context = browser.new_context(user_agent=custom_ua)
    page = context.new_page()

    print("Starting scraping...")
    errors = []
    saved_count = 0
    for i in data:
        save_dir = f"Original-Text/{i['book']}/{i['lang']}"
        os.makedirs(save_dir, exist_ok=True)
        for ch in range(1, i["chapter_length"] + 1):
            url = f"https://www.bible.com/bible/{i['ver']}/{i['book']}.{ch}.{i['bible_ver']}"

            filename = f"{i['book']}.{ch}.{i['bible_ver']}.{i['lang']}.html"
            save_path = os.path.join(save_dir, filename)

            resp = page.goto(url, wait_until="load")

            html = page.content()
            try:
                with open(save_path, "w", encoding="utf-8") as f:
                    f.write(html)
                saved_count += 1
                # inline progress — update the same terminal line without crowding
                print(f"\rSaved: {saved_count}", end="", flush=True)

            except Exception as e:
                errors.append(
                    f"Error saving HTML {i['book']}.{ch}.{i['bible_ver']}.{i['lang']}: {e}"
                )
            print(f"\rErrors encountered: {len(errors)}", end="", flush=True)

            time.sleep(DELAY)
    if errors:
        print("Errors encountered:")
        for error in errors:
            print(error)
    else:
        print("Scrape completed with no errors.")

    # --- email notification (minimal, configured via environment variables) ---
    try:
        # Default to Gmail's SMTP if not specified in .env
        smtp_server = os.environ.get("SMTP_SERVER", "smtp.gmail.com")
        smtp_port = int(os.environ.get("SMTP_PORT", "587"))
        smtp_user = os.environ.get("SMTP_USER")
        smtp_pass = os.environ.get("SMTP_PASS")
        # If SCRAPE_NOTIFY_TO isn't set, fall back to the SMTP_USER (send to self)
        notify_to = os.environ.get("SCRAPE_NOTIFY_TO") or smtp_user

        if smtp_server and smtp_user and smtp_pass and notify_to:
            msg = EmailMessage()
            msg["From"] = smtp_user
            msg["To"] = notify_to
            msg["Subject"] = "Scrape finished: NLP1K"
            body = "Scraping finished.\n\n"
            if errors:
                body += "Errors:\n" + "\n".join(errors)
            else:
                body += "No errors encountered."
            msg.set_content(body)

            with smtplib.SMTP(smtp_server, smtp_port, timeout=10) as s:
                s.starttls()
                s.login(smtp_user, smtp_pass)
                s.send_message(msg)
            print("Notification email sent to", notify_to)
        else:
            print("SMTP environment not fully configured; skipping email.")
    except Exception as e:
        print("Failed to send notification email:", e)

    browser.close()
    # After browser closes, print a final newline so following prints start on a fresh line
    print()

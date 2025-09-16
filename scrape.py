from playwright.sync_api import sync_playwright
import json, os, urllib.parse, time


with open("data.json", "r", encoding="utf-8") as f:
    data = json.load(f)

custom_ua = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:142.0) Gecko/20100101 Firefox/142.0"
)
print(type(data))  # could be dict or list depending on file
print(data)
with sync_playwright() as pw:
    browser = pw.chromium.launch(headless=True)
    context = browser.new_context(user_agent=custom_ua)
    page = context.new_page()

    for i in data:
        save_dir = f"{i["book"]}/{i["lang"]}"
        os.makedirs(save_dir, exist_ok=True)
        for ch in range(1, i["chapter_length"] + 1): 
            url = f"https://www.bible.com/bible/{i["ver"]}/{i["book"]}.{ch}.{i["bible_ver"]}"

            filename = f"{i["book"]}.{ch}.{i["bible_ver"]}.{i["lang"]}.html"
            save_path = os.path.join(save_dir, filename)

            print(f"Fetching {url} with UA={custom_ua!r}")
            resp = page.goto(url, wait_until="load")

            html = page.content()
            try:
                with open(save_path, "w", encoding="utf-8") as f:
                    f.write(html)
                print(f"Saved html to {save_path}")
            except:
                print("Error saving HTML")
            time.sleep(2)

    browser.close()

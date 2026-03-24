import asyncio
import csv
import json
import os
from datetime import datetime
from playwright.async_api import async_playwright

TARGET_URL = "https://nepalstock.com/floor-sheet"
API_URL = "https://nepalstock.com/api/nots/nepse-data/floorsheet"

OUTPUT_DIR = "data"   # folder to store files

# Maximum retries for page load
MAX_RETRIES = 3
PAGE_LOAD_TIMEOUT = 60000
INITIAL_WAIT_DELAY = 5  # seconds to wait after page load for XHR requests


def extract_trades(data):
    if isinstance(data, list):
        return data

    if isinstance(data, dict):
        if "content" in data and isinstance(data["content"], list):
            return data["content"]

        for key in data.values():
            result = extract_trades(key)
            if result:
                return result

    return None


def extract_market_date(trades):
    """
    Extract date from first trade row
    Adjust key if needed (check your data structure)
    """
    sample = trades[0]

    # Try common keys
    for key in ["tradeDate", "businessDate", "timestamp"]:
        if key in sample:
            raw = sample[key]
            try:
                dt = datetime.fromisoformat(raw.replace("Z", ""))
                return dt.strftime("%m-%d-%Y")
            except:
                pass

    # fallback: today (not ideal)
    return datetime.now().strftime("%m-%d-%Y")


async def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)  # IMPORTANT
        context = await browser.new_context(
            ignore_https_errors=True,
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36"
        )
        page = await context.new_page()

        session = {"auth": None, "id": None}

        # -----------------------------
        # TOKEN SNIFFER
        # -----------------------------
        def sniff(request):
            if "floorsheet" in request.url and request.method == "POST":
                auth = request.headers.get("authorization")
                post_data = request.post_data
                if auth and post_data and not session["auth"]:
                    try:
                        payload = json.loads(post_data)
                        session["auth"] = auth
                        session["id"] = payload.get("id")
                        print("[✓] Token captured successfully")
                    except json.JSONDecodeError as e:
                        print(f"[!] Failed to parse POST data: {e}")
                    except Exception as e:
                        print(f"[!] Error in sniff function: {e}")

        page.on("request", sniff)

        print(f"Opening {TARGET_URL}")
        for attempt in range(MAX_RETRIES):
            try:
                # Wait for network to be idle to ensure all XHR requests complete
                await page.goto(TARGET_URL, wait_until="networkidle", timeout=PAGE_LOAD_TIMEOUT)
                print("[✓] Page loaded successfully")
                break
            except Exception as e:
                print(f"[!] Attempt {attempt+1} failed: {e}")
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(5)
        else:
            print("❌ Failed to load page after retries")
            await browser.close()
            return

        # Wait for XHR requests to be captured
        print(f"Waiting {INITIAL_WAIT_DELAY} seconds for token to be captured...")
        await asyncio.sleep(INITIAL_WAIT_DELAY)

        if not session["auth"]:
            print("❌ Token capture failed - Authorization header not found")
            print("[!] Debugging: Check if the website structure has changed")
            print("[!] Try manually visiting the website to verify API requests are being made")
            await browser.close()
            return

        print(f"[✓] Using ID: {session['id']}")

        all_trades = []
        page_num = 0
        page_size = 500

        # -----------------------------
        # FETCH LOOP
        # -----------------------------
        while True:
            print(f"Fetching page {page_num}...")

            js_fetch = f"""
            async () => {{
                const res = await fetch("{API_URL}?page={page_num}&size={page_size}&sort=contractId,desc", {{
                    method: "POST",
                    headers: {{
                        "accept": "application/json, text/plain, */*",
                        "authorization": "{session['auth']}",
                        "content-type": "application/json",
                        "referer": "{TARGET_URL}"
                    }},
                    body: JSON.stringify({{ "id": {session['id']} }}),
                    credentials: "include"
                }});

                let body = null;
                try {{
                    body = await res.json();
                }} catch (e) {{
                    console.error("Failed to parse response:", e);
                }}

                return {{ status: res.status, body }};
            }}
            """

            result = await page.evaluate(js_fetch)

            if result["status"] == 401:
                print("🔄 Token expired. Reloading...")
                session["auth"] = None
                session["id"] = None
                await page.reload(wait_until="networkidle")
                await asyncio.sleep(6)
                continue

            if result["status"] != 200:
                print(f"[!] API returned status {result['status']}")
                break

            data = result["body"]
            trades = extract_trades(data)

            if not trades:
                print("✓ No more data.")
                break

            all_trades.extend(trades)
            print(f"  → {len(trades)} rows (Total: {len(all_trades)})")

            page_num += 1
            await asyncio.sleep(1.2)

        await browser.close()

        # -----------------------------
        # VALIDATION
        # -----------------------------
        if not all_trades:
            print("❌ No trades found. Likely market closed.")
            return

        if len(all_trades) < 500:
            print("⚠️ Too few rows. Skipping (likely holiday).")
            return

        # Remove duplicates
        unique = {t.get("contractId"): t for t in all_trades if t.get("contractId")}
        rows = list(unique.values())

        # -----------------------------
        # MARKET DATE
        # -----------------------------
        market_date = extract_market_date(rows)
        filename = f"{market_date}.csv"
        filepath = os.path.join(OUTPUT_DIR, filename)

        # -----------------------------
        # DUPLICATE CHECK
        # -----------------------------
        if os.path.exists(filepath):
            print(f"⚠️ File already exists for {market_date}. Skipping.")
            return

        # -----------------------------
        # SAVE FILE
        # -----------------------------
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)

        print(f"✅ Saved {len(rows)} rows → {filepath}")


if __name__ == "__main__":
    asyncio.run(main())

import asyncio
import csv
import json
import os
from datetime import datetime
from playwright.async_api import async_playwright

TARGET_URL = "https://nepalstock.com/floor-sheet"
API_URL = "https://nepalstock.com/api/nots/nepse-data/floorsheet"
OUTPUT_DIR = "data"

MAX_RETRIES = 3
PAGE_LOAD_TIMEOUT = 60000

# ONLY THESE COLUMNS
KEEP_COLUMNS = [
    "contractId",
    "businessDate",
    "symbol",
    "contractQuantity",
    "contractRate",
    "contractAmount",
    "sellerMemberId",
    "buyerMemberId"
]


def extract_trades(data):
    """Extract trades list from API response"""
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
    """Extract market date from trades data"""
    sample = trades[0]
    for key in ["tradeDate", "businessDate", "timestamp"]:
        if key in sample:
            raw = sample[key]
            try:
                dt = datetime.fromisoformat(raw.replace("Z", ""))
                return dt.strftime("%m-%d-%Y")
            except:
                pass
    return None


def filter_columns(rows):
    """Keep only KEEP_COLUMNS from the data"""
    filtered_rows = []
    for row in rows:
        filtered_row = {col: row.get(col, "") for col in KEEP_COLUMNS}
        filtered_rows.append(filtered_row)
    return filtered_rows


async def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            ignore_https_errors=True,
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36"
        )
        page = await context.new_page()

        session = {"auth": None, "id": None}

        # TOKEN SNIFFER
        def sniff(request):
            if "floorsheet" in request.url and request.method == "POST":
                auth = request.headers.get("authorization")
                post_data = request.post_data
                if auth and post_data and not session["auth"]:
                    try:
                        payload = json.loads(post_data)
                        session["auth"] = auth
                        session["id"] = payload.get("id")
                        print("[✓] Token captured")
                    except:
                        pass

        page.on("request", sniff)

        # STEP 1: LOAD PAGE
        print(f"Opening {TARGET_URL}")
        for attempt in range(MAX_RETRIES):
            try:
                await page.goto(TARGET_URL, wait_until="domcontentloaded", timeout=PAGE_LOAD_TIMEOUT)
                break
            except Exception as e:
                print(f"Retry {attempt+1} failed: {e}")
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(5)
        else:
            print("❌ Failed to load page after retries")
            await browser.close()
            return

        # Wait for token to be captured
        await asyncio.sleep(3)

        if not session["auth"]:
            print("❌ Token capture failed")
            await browser.close()
            return

        # STEP 2: FETCH ALL TRADES
        all_trades = []
        page_num = 0
        page_size = 500

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
                }} catch (e) {{}}

                return {{ status: res.status, body }};
            }}
            """

            result = await page.evaluate(js_fetch)

            if result["status"] == 401:
                print("🔄 Token expired. Reloading...")
                session["auth"] = None
                session["id"] = None
                await page.reload(wait_until="domcontentloaded")
                await asyncio.sleep(3)
                continue

            if result["status"] != 200:
                break

            data = result["body"]
            trades = extract_trades(data)

            if not trades:
                break

            all_trades.extend(trades)
            print(f"  → {len(trades)} rows (Total: {len(all_trades)})")

            page_num += 1
            await asyncio.sleep(1)

        await browser.close()

        # STEP 3: CHECK IF MARKET WAS OPEN
        if not all_trades:
            print("❌ No trades found - Market was closed (holiday/weekend)")
            return

        print(f"✅ Found {len(all_trades)} trades")

        # STEP 4: REMOVE DUPLICATES
        unique = {t.get("contractId"): t for t in all_trades if t.get("contractId")}
        rows = list(unique.values())

        # STEP 5: FILTER TO KEEP ONLY WANTED COLUMNS
        filtered_rows = filter_columns(rows)
        print(f"✅ Filtered to {len(KEEP_COLUMNS)} columns")

        # STEP 6: EXTRACT DATE
        market_date = extract_market_date(rows)
        if not market_date:
            print("❌ Could not extract market date")
            return

        print(f"Market date: {market_date}")

        # STEP 7: CHECK IF FILE ALREADY EXISTS
        filename = f"{market_date}.csv"
        filepath = os.path.join(OUTPUT_DIR, filename)

        if os.path.exists(filepath):
            print(f"⚠️ File already exists: {filepath}")
            return

        # STEP 8: SAVE CSV FILE WITH ONLY KEEP_COLUMNS
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=KEEP_COLUMNS)
            writer.writeheader()
            writer.writerows(filtered_rows)

        print(f"✅ Saved {len(filtered_rows)} rows to {filepath}")


if __name__ == "__main__":
    asyncio.run(main())

import json
import os
import csv
import requests
import time
import re
from bs4 import BeautifulSoup

def get_config():
    try:
        with open('secrets.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print("Error: secrets.json not found. Please ensure it exists with 'sheet_url'.")
        exit(1)
    except json.JSONDecodeError:
        print("Error: secrets.json contains invalid JSON.")
        exit(1)

def load_cache():
    if os.path.exists('cache.json'):
        try:
            with open('cache.json', 'r') as f:
                data = json.load(f)
                return {k: v for k, v in data.items() if isinstance(v, str) and "<!DOCTYPE" not in v}
        except (json.JSONDecodeError, Exception):
            return {}
    return {}

def save_cache(cache):
    with open('cache.json', 'w') as f:
        json.dump(cache, f, indent=2)

def clean_username(name_str):
    if not name_str:
        return "Unknown Resident"

    match = re.search(r'\((.*?)\)', name_str)
    if match:
        username = match.group(1).strip()
    else:
        username = name_str.split('|')[0].strip()

    return username.replace('.', ' ')

def format_evidence(text):
    if not text:
        return "N/A"

    if text.startswith('http://') or text.startswith('https://'):
        return text

    formatted = re.sub(r'(?<!^)(?<!\n)\s*(\[)', r'\n\1', text)
    return formatted.strip()

def get_sl_username(session, uuid, cache):
    if uuid in cache:
        sanitized = clean_username(cache[uuid])
        if sanitized != cache[uuid]:
            cache[uuid] = sanitized
        return sanitized

    url = f"https://world.secondlife.com/resident/{uuid}"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }

    try:
        response = session.get(url, headers=headers, timeout=15, allow_redirects=True)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            if soup.title and soup.title.string:
                username = clean_username(soup.title.string)
                if username.lower() not in ["second life", "not found", "resident profile", "internal server error"]:
                    cache[uuid] = username
                    return username
    except requests.exceptions.RequestException as e:
        print(f"Network error fetching {uuid}: {e}")

    return "Unknown Resident"

def build_database():
    config = get_config()
    sheet_url = config.get("sheet_url")
    cache = load_cache()

    if not os.path.exists('api'):
        os.makedirs('api')

    cache_buster = f"cache_bt={int(time.time())}"
    if "?" in sheet_url:
        sheet_url += f"&{cache_buster}"
    else:
        sheet_url += f"?{cache_buster}"

    with requests.Session() as session:
        print(f"Downloading data from Google Sheets (Cache Buster: {cache_buster})...")
        try:
            headers = {
                'Cache-Control': 'no-cache, no-store, must-revalidate',
                'Pragma': 'no-cache',
                'Expires': '0'
            }
            response = session.get(sheet_url, headers=headers)
            response.encoding = 'utf-8'
            response.raise_for_status()
        except Exception as e:
            print(f"Failed to download Google Sheet: {e}")
            return

        reader = csv.DictReader(response.text.splitlines())
        users = list(reader)

        table_data = []
        current_uuids = set()

        for user in users:
            uuid = user.get("uuid", "").strip()
            if not uuid:
                continue

            current_uuids.add(uuid)
            print(f"Processing {uuid}...")
            username = get_sl_username(session, uuid, cache)

            if uuid not in cache:
                time.sleep(0.3)

            reason = user.get("reason", "N/A")

            evidence_raw = user.get("evidence_link", "N/A")
            evidence_formatted = format_evidence(evidence_raw)

            table_data.append({"uuid": uuid, "username": username, "reason": reason})

            detailed_data = {
                "uuid": uuid,
                "username": username,
                "reason": reason,
                "details": {
                    "date": user.get("date", "N/A"),
                    "evidence_link": evidence_formatted,
                    "notes": user.get("notes", "")
                }
            }

            with open(f"api/{uuid}.json", 'w') as f:
                json.dump(detailed_data, f, indent=2)

        print("Cleaning up stale entries...")
        for filename in os.listdir('api'):
            if filename.endswith('.json'):
                uuid_from_file = filename.replace('.json', '')
                # Delete if it's not in the current sheet OR if it's the old usernames.json
                if uuid_from_file not in current_uuids or filename == 'usernames.json':
                    print(f"Removing stale file: {filename}")
                    os.remove(os.path.join('api', filename))

        for cached_uuid in list(cache.keys()):
            if cached_uuid not in current_uuids:
                del cache[cached_uuid]

        with open('data.json', 'w') as f:
            json.dump(table_data, f, indent=2)

        save_cache(cache)
        print(f"Sync Complete: {len(table_data)} users active. Stale entries removed.")

if __name__ == "__main__":
    build_database()
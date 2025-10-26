import os
import json
import time
import requests
import subprocess
import hashlib
import threading
from urllib.parse import urlparse, unquote

# === Environment variables ===
TELEGRAM_BOT_TOKEN = os.environ["bot_token"]
TELEGRAM_CHAT_ID = os.environ["channel_id"]
M3U8_URL = os.environ["m3u8_url"]

# === Constants ===
SENT_JSON_FILE = "sent.json"
MERGE_GROUP_SIZE = 5
CHECK_INTERVAL = 5  # seconds between M3U8 polls

# Shared data for background thread
downloaded_ts = set()
stop_flag = False
lock = threading.Lock()


# === Utility functions ===

def safe_ts_filename(ts_url: str) -> str:
    """Generate safe filename from .ts URL."""
    parsed = urlparse(ts_url)
    filename = os.path.basename(parsed.path)
    filename = unquote(filename)
    if not filename.endswith(".ts"):
        filename += ".ts"
    if len(filename) > 80:
        hashed = hashlib.md5(ts_url.encode()).hexdigest()[:8]
        filename = f"segment_{hashed}.ts"
    return filename


def download_new_segments():
    """Check M3U8 and download new .ts segments."""
    try:
        r = requests.get(M3U8_URL, timeout=10)
        r.raise_for_status()
    except Exception as e:
        print(f"⚠️ Failed to fetch playlist: {e}")
        return False

    lines = r.text.splitlines()
    ts_urls = [line.strip() for line in lines if line and not line.startswith("#")]
    base_url = M3U8_URL.rsplit("/", 1)[0]

    new_files = 0

    for ts_name in ts_urls:
        ts_url = ts_name if ts_name.startswith("http") else f"{base_url}/{ts_name}"
        ts_file = safe_ts_filename(ts_url)

        with lock:
            if ts_file in downloaded_ts:
                continue
            downloaded_ts.add(ts_file)

        if os.path.exists(ts_file):
            continue

        try:
            res = requests.get(ts_url, timeout=10)
            res.raise_for_status()
            with open(ts_file, "wb") as f:
                f.write(res.content)
            new_files += 1
            print(f"⬇️ Downloaded: {ts_file}")
        except Exception as e:
            print(f"❌ Failed: {ts_file}: {e}")
            with lock:
                downloaded_ts.discard(ts_file)
            time.sleep(1)

    return new_files > 0


def download_worker():
    """Background thread: continuously fetch new segments."""
    while not stop_flag:
        new = download_new_segments()
        if not new:
            time.sleep(CHECK_INTERVAL)
        else:
            time.sleep(1)


def merge_ts_to_mp4():
    """Merge every 5 .ts → one .mp4."""
    ts_files = sorted([f for f in os.listdir() if f.endswith(".ts")])
    groups = [ts_files[i:i + MERGE_GROUP_SIZE] for i in range(0, len(ts_files), MERGE_GROUP_SIZE)]

    for group in groups:
        if not group:
            continue
        first_ts = group[0]
        mp4_name = first_ts.rsplit(".", 1)[0] + ".mp4"
        if os.path.exists(mp4_name):
            continue

        list_file = "concat_list.txt"
        with open(list_file, "w") as f:
            for ts in group:
                f.write(f"file '{ts}'\n")

        print(f"🎞️ Merging {len(group)} segments → {mp4_name}")
        cmd = ["ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", list_file, "-c", "copy", mp4_name]
        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)

        os.remove(list_file)


def load_sent_status():
    if os.path.exists(SENT_JSON_FILE):
        with open(SENT_JSON_FILE, "r") as f:
            return json.load(f)
    return {}


def save_sent_status(status_dict):
    with open(SENT_JSON_FILE, "w") as f:
        json.dump(status_dict, f, indent=4)


def send_to_telegram(file_path):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
    while True:
        try:
            with open(file_path, "rb") as f:
                response = requests.post(
                    url,
                    data={"chat_id": TELEGRAM_CHAT_ID, "caption": file_path},
                    files={"document": f},
                    timeout=60,
                )
            if response.status_code == 200:
                return True
        except Exception as e:
            print(f"⚠️ Telegram send error for {file_path}: {e}")
        print(f"Retrying {file_path}...")
        time.sleep(5)


def process_files():
    status = load_sent_status()
    now = time.time()

    all_files = sorted([f for f in os.listdir() if f.endswith(".mp4")])

    for f in all_files:
        if f not in status:
            status[f] = {"first_seen": now, "sent": False}

    unsent = [f for f in all_files if not status[f]["sent"]]

    if len(unsent) > 5:
        base_files = unsent[:-5]
        tail_files = unsent[-5:]
    else:
        base_files, tail_files = [], unsent

    files_to_send = list(base_files)
    for f in tail_files:
        if now - status[f]["first_seen"] > 180:
            files_to_send.append(f)

    for f in files_to_send:
        if send_to_telegram(f):
            print(f"✅ Sent: {f}")
            status[f]["sent"] = True

    save_sent_status(status)


# === Main loop ===

if __name__ == "__main__":
    start_time = time.time()
    last_new_file_time = start_time

    print("🚀 Starting background download thread...")
    t = threading.Thread(target=download_worker, daemon=True)
    t.start()

    try:
        while True:
            before = set(os.listdir())

            merge_ts_to_mp4()
            process_files()

            after = set(os.listdir())
            if after != before:
                last_new_file_time = time.time()

            elapsed = time.time() - start_time
            idle_time = time.time() - last_new_file_time

            if elapsed > 9000:  # 2.5 hrs
                print("⏱️ 2.5 hours elapsed — stopping.")
                break
            if idle_time > 900:  # 15 min
                print("🕒 Idle 15 minutes — stopping.")
                break

            time.sleep(20)

    finally:
        stop_flag = True
        t.join(timeout=5)
        for f in os.listdir():
            if f.endswith(".ts"):
                os.remove(f)
        print("🧹 Cleaned .ts files. ✅ Done.")

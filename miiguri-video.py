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
MERGE_IDLE_LIMIT = 30  # seconds since last modification of group before merging smaller group

# Shared data for background thread
downloaded_ts = set()
lock = threading.Lock()
stop_event = threading.Event()


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
    # sanitize slightly (remove problematic characters)
    filename = filename.replace("..", "_").replace("/", "_")
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
            # already present on disk
            continue

        try:
            res = requests.get(ts_url, timeout=20)
            res.raise_for_status()
            # write to a temp file then atomically rename to avoid partially-written files being visible
            tmp_name = ts_file + ".part"
            with open(tmp_name, "wb") as f:
                f.write(res.content)
            os.replace(tmp_name, ts_file)
            new_files += 1
            print(f"⬇️ Downloaded: {ts_file}")
        except Exception as e:
            print(f"❌ Failed to download {ts_file}: {e}")
            with lock:
                downloaded_ts.discard(ts_file)
            # if partial file exists, remove it
            try:
                if os.path.exists(tmp_name):
                    os.remove(tmp_name)
            except Exception:
                pass
            time.sleep(1)

    return new_files > 0


def download_worker():
    """Background thread: continuously fetch new segments."""
    while not stop_event.is_set():
        try:
            new = download_new_segments()
            if not new:
                # no new files found: wait full interval
                stop_event.wait(CHECK_INTERVAL)
            else:
                # got new files recently, poll again sooner
                stop_event.wait(1)
        except Exception as e:
            print("Download worker error:", e)
            stop_event.wait(2)


def merge_ts_to_mp4():
    """
    Merge .ts → .mp4 dynamically:
    - Prefer merging groups of MERGE_GROUP_SIZE.
    - If a group is smaller than MERGE_GROUP_SIZE, only merge it if the newest file in that group
      has not been modified for at least MERGE_IDLE_LIMIT seconds.
    """
    ts_files = sorted([f for f in os.listdir() if f.endswith(".ts")])
    if not ts_files:
        return

    # split into groups of MERGE_GROUP_SIZE
    groups = [ts_files[i:i + MERGE_GROUP_SIZE] for i in range(0, len(ts_files), MERGE_GROUP_SIZE)]

    now = time.time()
    for group in groups:
        if not group:
            continue

        # skip tiny groups unless they've been idle for MERGE_IDLE_LIMIT
        if len(group) < MERGE_GROUP_SIZE:
            # compute newest modification time in this group
            try:
                newest_mtime = max(os.path.getmtime(f) for f in group)
            except FileNotFoundError:
                # some file disappeared, skip this group for now
                continue
            group_idle = now - newest_mtime
            if group_idle < MERGE_IDLE_LIMIT:
                # still being updated recently -> skip
                # print debug
                print(f"⏳ Group of {len(group)} not idle yet (idle {group_idle:.1f}s) -> skip")
                continue

        # additional safety: make sure files are non-zero and exist
        ready = True
        for ts in group:
            try:
                if not os.path.exists(ts) or os.path.getsize(ts) == 0:
                    ready = False
                    break
            except Exception:
                ready = False
                break
        if not ready:
            print("⚠️ Some files in group are missing or zero-sized -> skip merging this group.")
            continue

        first_ts = group[0]
        mp4_name = first_ts.rsplit(".", 1)[0] + ".mp4"
        if os.path.exists(mp4_name):
            # already merged
            continue

        # create a unique concat list file for this merge
        list_file = f"{mp4_name}.concat.txt"
        try:
            with open(list_file, "w", encoding="utf-8") as f:
                for ts in group:
                    # ffmpeg concat demuxer expects paths; wrap in single quotes and escape single quotes inside
                    safe_path = ts.replace("'", "'\\''")
                    f.write(f"file '{safe_path}'\n")

            print(f"🎞️ Merging {len(group)} segments → {mp4_name}")
            cmd = [
                "ffmpeg", "-y", "-f", "concat", "-safe", "0",
                "-i", list_file, "-c", "copy", mp4_name
            ]
            proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if proc.returncode != 0:
                print(f"❌ ffmpeg failed for {mp4_name}. stderr:\n{proc.stderr.decode(errors='ignore')}")
                # keep ts files for retry
            else:
                print(f"✅ Merged to {mp4_name}")
                # remove merged .ts files only on success
                for ts in group:
                    try:
                        if os.path.exists(ts):
                            os.remove(ts)
                    except Exception as e:
                        print(f"⚠️ Could not remove {ts}: {e}")
        finally:
            try:
                if os.path.exists(list_file):
                    os.remove(list_file)
            except Exception:
                pass


def load_sent_status():
    if os.path.exists(SENT_JSON_FILE):
        try:
            with open(SENT_JSON_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_sent_status(status_dict):
    with open(SENT_JSON_FILE, "w", encoding="utf-8") as f:
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
                    timeout=120,
                )
            if response.status_code == 200:
                return True
            else:
                print(f"Telegram responded {response.status_code}: {response.text}")
        except Exception as e:
            print(f"⚠️ Telegram send error for {file_path}: {e}")
        print(f"Retrying {file_path} in 5s...")
        time.sleep(5)


def process_files():
    status = load_sent_status()
    now = time.time()

    all_files = sorted([f for f in os.listdir() if f.endswith(".mp4")])

    for f in all_files:
        if f not in status:
            status[f] = {"first_seen": now, "sent": False}

    unsent = [f for f in all_files if not status.get(f, {}).get("sent", False)]

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

            if elapsed > 9000:
                print("⏱️ 2.5 hours elapsed — stopping.")
                break
            if idle_time > 9000:
                print("🕒 Idle 2.5 hours — stopping.")
                break

            # sleep a bit so loop is not tight
            time.sleep(10)

    finally:
        stop_event.set()
        t.join(timeout=5)
        # cleanup partial files and .ts files
        for f in os.listdir():
            if f.endswith(".ts") or f.endswith(".part"):
                try:
                    os.remove(f)
                except Exception:
                    pass
        print("🧹 Cleaned .ts files. ✅ Done.")

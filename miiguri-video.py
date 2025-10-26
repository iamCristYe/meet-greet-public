import os
import json
import time
import requests
import subprocess

# === Environment variables ===
TELEGRAM_BOT_TOKEN = os.environ["bot_token"]
TELEGRAM_CHAT_ID = os.environ["channel_id"]
M3U8_URL = os.environ["m3u8_url"]

# === Constants ===
SENT_JSON_FILE = "sent.json"


# === Core functions ===

def download_segments():
    """
    从 M3U8_URL 下载所有 .ts 片段，但不合并。
    如果本地已存在同名文件则跳过。
    """
    print("Fetching playlist:", M3U8_URL)
    r = requests.get(M3U8_URL)
    r.raise_for_status()

    lines = r.text.splitlines()
    ts_urls = [line.strip() for line in lines if line and not line.startswith("#")]

    base_url = M3U8_URL.rsplit("/", 1)[0]

    for ts_name in ts_urls:
        ts_url = ts_name if ts_name.startswith("http") else f"{base_url}/{ts_name}"
        ts_file = os.path.basename(ts_name)

        if os.path.exists(ts_file):
            continue

        print(f"Downloading segment: {ts_file}")
        try:
            res = requests.get(ts_url, timeout=10)
            res.raise_for_status()
            with open(ts_file, "wb") as f:
                f.write(res.content)
        except Exception as e:
            print(f"Failed to download {ts_file}: {e}")
            time.sleep(2)


def convert_ts_to_mp4():
    """
    将所有未转换的 .ts 文件转为 .mp4
    """
    for ts_file in sorted([f for f in os.listdir() if f.endswith(".ts")]):
        mp4_file = ts_file.rsplit(".", 1)[0] + ".mp4"
        if os.path.exists(mp4_file):
            continue

        print(f"Converting {ts_file} → {mp4_file}")
        cmd = [
            "ffmpeg",
            "-y",
            "-i", ts_file,
            "-c", "copy",
            mp4_file
        ]
        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)


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
        with open(file_path, "rb") as f:
            response = requests.post(
                url,
                data={"chat_id": TELEGRAM_CHAT_ID, "caption": file_path},
                files={"document": f},
            )
        if response.status_code == 200:
            return True

        print(f"Failed to send {file_path}, retrying...")
        time.sleep(5)


def process_files():
    status = load_sent_status()
    now = time.time()

    all_files = sorted([f for f in os.listdir() if f.endswith(".mp4")])

    # 初始化新文件状态
    for f in all_files:
        if f not in status:
            status[f] = {"first_seen": now, "sent": False}

    unsent_files = [f for f in all_files if not status[f]["sent"]]

    # 分批逻辑
    if len(unsent_files) > 5:
        base_files = unsent_files[:-5]
        tail_files = unsent_files[-5:]
    else:
        base_files, tail_files = [], unsent_files

    files_to_send = list(base_files)
    for f in tail_files:
        if now - status[f]["first_seen"] > 180:
            files_to_send.append(f)

    # 发送文件
    for f in files_to_send:
        if send_to_telegram(f):
            print(f"Sent: {f}")
            status[f]["sent"] = True

    save_sent_status(status)


# === Main loop ===

if __name__ == "__main__":
    start_time = time.time()
    last_new_file_time = start_time

    print("Started recording loop...")

    try:
        while True:
            before_files = set(os.listdir())

            download_segments()
            convert_ts_to_mp4()
            process_files()

            # detect new files since last loop
            after_files = set(os.listdir())
            if after_files != before_files:
                last_new_file_time = time.time()

            elapsed = time.time() - start_time
            idle_time = time.time() - last_new_file_time

            # Stop after 2.5 hours (9000 sec) or idle for 15 min (900 sec)
            if elapsed > 9000:
                print("⏱️ 2.5 hours elapsed, exiting.")
                break

            if idle_time > 900:
                print("🕒 No new files for 15 minutes, exiting.")
                break

            time.sleep(30)

    finally:
        # Clean up leftover .ts files
        for f in os.listdir():
            if f.endswith(".ts"):
                os.remove(f)
        print("🧹 Cleaned up leftover .ts files.")
        print("✅ Finished session.")

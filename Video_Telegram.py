    # if "output00000800.mp4" in all_files:
    #     print("Found output00000800.mp4, exiting.")
    #     exit(0)


import os
import json
import time
import subprocess
import requests
import subprocess

# time.sleep(20)
# https://www.showroom-live.com/api/live/streaming_url?room_id=190685&abr_available=1

# Telegram 配置
TELEGRAM_BOT_TOKEN = os.environ["bot_token"]
TELEGRAM_CHAT_ID = os.environ["channel_id"]

# JSON 文件存储每个文件的状态（首次出现时间 + 是否已发送）
SENT_JSON_FILE = "sent.json"


# FFmpeg 命令
FFMPEG_COMMAND = [
    "ffmpeg",
    # "-decryption_key",
    # "7f257214bc48de644c9b25eebda5fa62",
    "-i",
    "chunklist.ts",
    "-map",
    "0:v",
    "-map",
    "0:a",
    "-c",
    "copy",
    "-segment_time",
    "10",
    "-f",
    "segment",
    "-reset_timestamps",
    "1",
    "output%08d.mp4",
]


def run_ffmpeg():
    """运行 FFmpeg 命令并覆盖已有的 mp4 文件"""
    try:
        subprocess.run(
            FFMPEG_COMMAND,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            check=True,
        )
    except subprocess.CalledProcessError as e:
        print("FFmpeg error output:")
        print(e.stderr.decode())
        time.sleep(10)


def load_sent_status():
    """加载 sent.json 数据结构"""
    if os.path.exists(SENT_JSON_FILE):
        with open(SENT_JSON_FILE, "r") as f:
            return json.load(f)
    return {}


def save_sent_status(status_dict):
    """保存文件状态到 sent.json"""
    with open(SENT_JSON_FILE, "w") as f:
        json.dump(status_dict, f, indent=4)


def send_to_telegram(file_path):
    """发送文件到 Telegram，直到成功为止"""
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
    """处理并发送符合条件的 MP4 文件"""
    status = load_sent_status()
    now = time.time()

    # 查找所有还存在于文件夹的 output mp4
    all_files = sorted(
        [f for f in os.listdir() if f.startswith("output") and f.endswith(".mp4")]
    )

    if "output00000800.mp4" in all_files:
        print("Found output00000800.mp4, exiting.")
        exit(0)

    # 更新状态字典中未记录的文件，记录首次发现时间和是否已发送
    for f in all_files:
        if f not in status:
            status[f] = {"first_seen": now, "sent": False}

    # 找出未发送的文件
    unsent_files = [f for f in all_files if not status.get(f, {}).get("sent", False)]

    # 分离最后五个
    if len(unsent_files) > 5:
        base_files = unsent_files[:-5]
        tail_files = unsent_files[-5:]
    else:
        base_files = []
        tail_files = unsent_files

    files_to_send = list(base_files)  # 确保复制

    # 添加尾部文件：如果 first_seen 时间超过 180 秒
    for f in tail_files:
        first_seen = status[f]["first_seen"]
        if now - first_seen > 180:
            files_to_send.append(f)

    # 发送文件
    for file_name in files_to_send:
        if not status[file_name]["sent"]:
            if send_to_telegram(file_name):
                print(f"Sent: {file_name}")
                status[file_name]["sent"] = True

    save_sent_status(status)


if __name__ == "__main__":
    while True:
        run_ffmpeg()
        process_files()
        # time.sleep(30)
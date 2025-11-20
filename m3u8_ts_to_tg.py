import os
import json
import time
import requests
import subprocess
import hashlib
import threading
from urllib.parse import urlparse, unquote


class M3U8TSToTG:
    """
    Handles M3U8 video stream processing:
    - Downloads .ts segments from M3U8 playlist
    - Merges segments into MP4 files
    - Sends MP4 files to Telegram
    """

    def __init__(
        self,
        m3u8_url,
        telegram_bot_token,
        telegram_chat_id,
        caption_prefix="",
        work_dir=".",
    ):
        """
        Initialize M3U8TSToTG.

        Args:
            m3u8_url: URL to the M3U8 playlist
            telegram_bot_token: Telegram bot token for sending files
            telegram_chat_id: Telegram channel/chat ID to send files to
            work_dir: Working directory for storing files (default: current directory)
        """
        self.m3u8_url = m3u8_url
        self.telegram_bot_token = telegram_bot_token
        self.telegram_chat_id = telegram_chat_id
        self.caption_prefix = caption_prefix
        self.work_dir = work_dir

        # Constants
        self.sent_json_file = os.path.join(work_dir, "sent.json")
        self.merge_group_size = 5
        self.check_interval = 5  # seconds between M3U8 polls
        self.merge_idle_limit = 30  # seconds since last modification before merging

        # Shared data for background thread
        self.downloaded_ts = set()
        self.lock = threading.Lock()
        self.stop_event = threading.Event()

    def safe_ts_filename(self, ts_url: str) -> str:
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
        return os.path.join(self.work_dir, filename)

    def download_new_segments(self) -> bool:
        """Check M3U8 and download new .ts segments."""
        try:
            r = requests.get(self.m3u8_url, timeout=10)
            r.raise_for_status()
        except Exception as e:
            print(f"⚠️ Failed to fetch playlist: {e}")
            return False

        lines = r.text.splitlines()
        ts_urls = [line.strip() for line in lines if line and not line.startswith("#")]
        base_url = self.m3u8_url.rsplit("/", 1)[0]

        new_files = 0

        for ts_name in ts_urls:
            ts_url = ts_name if ts_name.startswith("http") else f"{base_url}/{ts_name}"
            ts_file = self.safe_ts_filename(ts_url)

            with self.lock:
                if ts_file in self.downloaded_ts:
                    continue
                self.downloaded_ts.add(ts_file)

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
                print(f"⬇️ Downloaded: {os.path.basename(ts_file)}")
            except Exception as e:
                print(f"❌ Failed to download {ts_file}: {e}")
                with self.lock:
                    self.downloaded_ts.discard(ts_file)
                # if partial file exists, remove it
                try:
                    if os.path.exists(tmp_name):
                        os.remove(tmp_name)
                except Exception:
                    pass
                time.sleep(1)

        return new_files > 0

    def download_worker(self):
        """Background thread: continuously fetch new segments."""
        while not self.stop_event.is_set():
            try:
                new = self.download_new_segments()
                if not new:
                    # no new files found: wait full interval
                    self.stop_event.wait(self.check_interval)
                else:
                    # got new files recently, poll again sooner
                    self.stop_event.wait(1)
            except Exception as e:
                print("Download worker error:", e)
                self.stop_event.wait(2)

    def merge_ts_to_mp4(self):
        """
        Merge .ts → .mp4 dynamically:
        - Prefer merging groups of MERGE_GROUP_SIZE.
        - If a group is smaller than MERGE_GROUP_SIZE, only merge it if the newest file in that group
          has not been modified for at least MERGE_IDLE_LIMIT seconds.
        """
        ts_files = sorted([f for f in os.listdir(self.work_dir) if f.endswith(".ts")])
        if not ts_files:
            return

        # Get full paths
        ts_files = [os.path.join(self.work_dir, f) for f in ts_files]

        # split into groups of MERGE_GROUP_SIZE
        groups = [
            ts_files[i : i + self.merge_group_size]
            for i in range(0, len(ts_files), self.merge_group_size)
        ]

        now = time.time()
        for group in groups:
            if not group:
                continue

            # skip tiny groups unless they've been idle for MERGE_IDLE_LIMIT
            if len(group) < self.merge_group_size:
                # compute newest modification time in this group
                try:
                    newest_mtime = max(os.path.getmtime(f) for f in group)
                except FileNotFoundError:
                    # some file disappeared, skip this group for now
                    continue
                group_idle = now - newest_mtime
                if group_idle < self.merge_idle_limit:
                    # still being updated recently -> skip
                    print(
                        f"⏳ Group of {len(group)} not idle yet (idle {group_idle:.1f}s) -> skip"
                    )
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
                print(
                    "⚠️ Some files in group are missing or zero-sized -> skip merging this group."
                )
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

                print(f"🎞️ Merging {len(group)} segments → {os.path.basename(mp4_name)}")
                cmd = [
                    "ffmpeg",
                    "-y",
                    "-f",
                    "concat",
                    "-safe",
                    "0",
                    "-i",
                    list_file,
                    "-c",
                    "copy",
                    mp4_name,
                ]
                proc = subprocess.run(
                    cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
                )
                if proc.returncode != 0:
                    print(
                        f"❌ ffmpeg failed for {mp4_name}. stderr:\n{proc.stderr.decode(errors='ignore')}"
                    )
                    # keep ts files for retry
                else:
                    print(f"✅ Merged to {os.path.basename(mp4_name)}")
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

    def load_sent_status(self) -> dict:
        """Load the status of files sent to Telegram."""
        if os.path.exists(self.sent_json_file):
            try:
                with open(self.sent_json_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                return {}
        return {}

    def save_sent_status(self, status_dict: dict):
        """Save the status of files sent to Telegram."""
        with open(self.sent_json_file, "w", encoding="utf-8") as f:
            json.dump(status_dict, f, indent=4)

    def send_to_telegram(self, file_path: str) -> bool:
        """Send a file to Telegram chat."""
        url = f"https://api.telegram.org/bot{self.telegram_bot_token}/sendDocument"
        while True:
            try:
                with open(file_path, "rb") as f:
                    to_send_caption = (
                        file_path
                        if not self.caption_prefix
                        else f"{self.caption_prefix}_{file_path}"
                    )
                    response = requests.post(
                        url,
                        data={
                            "chat_id": self.telegram_chat_id,
                            "caption": to_send_caption,
                        },
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

    def process_files(self):
        """Process and send MP4 files to Telegram."""
        status = self.load_sent_status()
        now = time.time()

        all_files = sorted([f for f in os.listdir(self.work_dir) if f.endswith(".mp4")])

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
            file_path = os.path.join(self.work_dir, f)
            if self.send_to_telegram(file_path):
                print(f"✅ Sent: {f}")
                status[f]["sent"] = True

        self.save_sent_status(status)

    def cleanup(self):
        """Clean up temporary and .ts files."""
        for f in os.listdir(self.work_dir):
            if f.endswith(".ts") or f.endswith(".part"):
                try:
                    os.remove(os.path.join(self.work_dir, f))
                except Exception:
                    pass

    def run(self, timeout_hours=2.5):
        """
        Main loop: download segments, merge to MP4, and send to Telegram.

        Args:
            timeout_hours: Stop after this many hours of inactivity or total runtime
        """
        timeout_seconds = timeout_hours * 3600
        start_time = time.time()
        last_new_file_time = start_time

        print("🚀 Starting background download thread...")
        t = threading.Thread(target=self.download_worker, daemon=True)
        t.start()

        try:
            while True:
                before = set(os.listdir(self.work_dir))

                self.merge_ts_to_mp4()
                self.process_files()

                after = set(os.listdir(self.work_dir))
                if after != before:
                    last_new_file_time = time.time()

                elapsed = time.time() - start_time
                idle_time = time.time() - last_new_file_time

                if elapsed > timeout_seconds:
                    print(f"⏱️ {timeout_hours} hours elapsed — stopping.")
                    break
                if idle_time > timeout_seconds:
                    print(f"🕒 Idle {timeout_hours} hours — stopping.")
                    break

                # sleep a bit so loop is not tight
                time.sleep(10)

        finally:
            self.stop_event.set()
            t.join(timeout=5)
            self.cleanup()
            print("🧹 Cleaned .ts files. ✅ Done.")

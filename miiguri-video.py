"""
Main entry point for video processor.

Usage:
    Set environment variables:
    - bot_token: Telegram bot token
    - channel_id: Telegram channel/chat ID
    - m3u8_url: URL to M3U8 playlist

    Then run: python main.py
"""

import os
from m3u8_ts_to_tg import M3U8TSToTG


def main():
    """Initialize and run the video processor with environment variables."""
    # Get environment variables
    bot_token = os.environ.get("bot_token")
    channel_id = os.environ.get("channel_id")
    m3u8_url = os.environ.get("m3u8_url")

    # Validate required environment variables
    if not bot_token:
        raise ValueError("Environment variable 'bot_token' is required")
    if not channel_id:
        raise ValueError("Environment variable 'channel_id' is required")
    if not m3u8_url:
        raise ValueError("Environment variable 'm3u8_url' is required")

    print("🎬 Video Processor Starting")
    print(f"   M3U8 URL: {m3u8_url}")
    print(f"   Telegram Channel: {channel_id}")

    # Create processor instance
    processor = M3U8TSToTG(
        m3u8_url=m3u8_url,
        telegram_bot_token=bot_token,
        telegram_chat_id=channel_id,
        work_dir=".",
    )

    # Run the processor
    processor.run(timeout_hours=2.5)


if __name__ == "__main__":
    main()

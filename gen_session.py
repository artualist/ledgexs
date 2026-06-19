"""
bot/gen_session.py
==================
One-time helper to generate a Telethon StringSession string.

Run this ONCE from your local machine (or a Replit Shell):

    python bot/gen_session.py

You will be prompted for your phone number and the OTP Telegram sends you.
The script prints a session string — copy it and save it as the Replit Secret
named  TELETHON_SESSION.

You do NOT need to run this again unless you revoke the session on
https://my.telegram.org/auth → Active Sessions.

Required env vars (set in shell before running, or enter when prompted):
  TELEGRAM_API_ID
  TELEGRAM_API_HASH
"""

import os
import sys

try:
    from telethon.sync import TelegramClient
    from telethon.sessions import StringSession
except ImportError:
    print("ERROR: telethon is not installed.  Run:  pip install telethon")
    sys.exit(1)

api_id_str = os.environ.get("TELEGRAM_API_ID") or input("Enter TELEGRAM_API_ID: ").strip()
api_hash = os.environ.get("TELEGRAM_API_HASH") or input("Enter TELEGRAM_API_HASH: ").strip()

if not api_id_str or not api_hash:
    print("ERROR: TELEGRAM_API_ID and TELEGRAM_API_HASH are required.")
    sys.exit(1)

try:
    api_id = int(api_id_str)
except ValueError:
    print("ERROR: TELEGRAM_API_ID must be an integer.")
    sys.exit(1)

print("\nConnecting to Telegram — you will receive an OTP on your account…\n")

with TelegramClient(StringSession(), api_id, api_hash) as client:
    session_string = client.session.save()

print("\n" + "=" * 60)
print("SUCCESS — copy the string below and save it as the")
print("Replit Secret named:  TELETHON_SESSION")
print("=" * 60)
print(session_string)
print("=" * 60 + "\n")

#!/usr/bin/env python3
"""
Create a Pyrogram user session and export a session string.

Run this script LOCALLY (or on any machine with a working Telegram login)
to generate a session string. Then set the PYROGRAM_SESSION env var on
the server to use it as a fallback userbot session.

⚠️  SECURITY WARNING:
    The session string exported by this script is a full login token.
    Anyone with this string can control your Telegram account.
    - NEVER commit the generated *.txt files to Git.
    - Treat the session string like a password.
    - The script saves {session_name}.txt locally for convenience —
      delete it after you've set the env var on your server.

Usage:
    # Set API credentials
    export API_ID=12345
    export API_HASH=your_api_hash

    # Run the script (interactive - will prompt for phone/code)
    python scripts/create_pyrogram_session.py

    # Or export an existing .session file as a session string
    python scripts/create_pyrogram_session.py --from-file my_session.session

Requirements:
    - Pyrogram: pip install pyrogram tgcrypto
    - API_ID and API_HASH from https://my.telegram.org/apps
"""

import asyncio
import os
import pathlib
import sys

# Ensure project root is on sys.path
_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def get_credentials() -> tuple:
    """Return (api_id, api_hash) from env or prompt."""
    api_id = os.getenv("API_ID") or os.getenv("PYROGRAM_API_ID") or os.getenv("TELEGRAM_APP_ID")
    api_hash = os.getenv("API_HASH") or os.getenv("PYROGRAM_API_HASH") or os.getenv("TELEGRAM_API_HASH")

    if not api_id:
        api_id = input("Enter your API_ID: ").strip()
    if not api_hash:
        api_hash = input("Enter your API_HASH: ").strip()

    try:
        api_id = int(api_id)
    except (TypeError, ValueError):
        print("ERROR: API_ID must be an integer. Get yours from https://my.telegram.org/apps")
        sys.exit(1)

    if not api_hash:
        print("ERROR: API_HASH is required. Get yours from https://my.telegram.org/apps")
        sys.exit(1)

    return api_id, api_hash


async def create_session(api_id: int, api_hash: str, session_name: str = "pyrogram_session"):
    """Create a new Pyrogram session interactively and export the session string."""
    try:
        from pyrogram import Client
    except ImportError:
        print(
            "Pyrogram is not installed. Install it with:\n"
            "  pip install pyrogram tgcrypto"
        )
        sys.exit(1)

    print(f"\n{'='*60}")
    print("Pyrogram Session Creator")
    print(f"{'='*60}")
    print(f"\nCreating session: {session_name}")
    print(f"API_ID: {api_id}")
    print("\nYou will be prompted to enter:")
    print("  1. Your phone number (international format, e.g. +1234567890)")
    print("  2. The login code sent to your Telegram app or SMS")
    print("  3. Your 2FA password (if enabled)")
    print(f"\n{'='*60}\n")

    client = Client(session_name, api_id=api_id, api_hash=api_hash, in_memory=True)

    try:
        await client.start()
        print(f"\n{'='*60}")
        print("✅ Login successful!")
        print(f"{'='*60}")

        # Get the session string
        session_string = await client.export_session_string()
        
        me = await client.get_me()
        print(f"\nUser: {me.first_name or ''} {me.last_name or ''}".strip())
        print(f"User ID: {me.id}")
        print(f"Phone: +{me.phone_number if hasattr(me, 'phone_number') else 'unknown'}")

        print(f"\n{'='*60}")
        print("📋 SESSION STRING (copy this):")
        print(f"{'='*60}")
        print(session_string)
        print(f"{'='*60}")
        print("\nSet this as an environment variable on your server:")
        print(f"  PYROGRAM_SESSION='{session_string[:50]}...'")
        print("\nOr if using Render, add it as a secret environment variable.")
        print(f"\n{'='*60}")
        print("⚠️  SECURITY: The session string is a full login token.")
        print("   Never commit it to Git. Delete the .txt file after use.")
        print(f"{'='*60}")

        # Also save to a text file for convenience
        with open(f"{session_name}.txt", "w") as f:
            f.write(session_string)
        print(f"✅ Saved to {session_name}.txt — DELETE THIS FILE after you've set the env var!")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)
    finally:
        await client.stop()


async def export_from_file(api_id: int, api_hash: str, session_file: str):
    """Export a session string from an existing .session file."""
    try:
        from pyrogram import Client
    except ImportError:
        print("Pyrogram is not installed. Install it with: pip install pyrogram tgcrypto")
        sys.exit(1)

    if not os.path.exists(session_file):
        # Try with .session extension
        if not session_file.endswith(".session"):
            session_file += ".session"
        if not os.path.exists(session_file):
            print(f"ERROR: Session file not found: {session_file}")
            sys.exit(1)

    session_name = os.path.splitext(os.path.basename(session_file))[0]
    print(f"Exporting session string from: {session_file}")

    client = Client(session_name, api_id=api_id, api_hash=api_hash)
    try:
        await client.start()
        session_string = await client.export_session_string()
        
        print(f"\n{'='*60}")
        print("📋 SESSION STRING:")
        print(f"{'='*60}")
        print(session_string)
        print(f"{'='*60}")
        print("\n⚠️  SECURITY: Treat this like a password. Delete the file after use.")
        print(f"{'='*60}")

        with open(f"{session_name}_exported.txt", "w") as f:
            f.write(session_string)
        print(f"\n✅ Saved to {session_name}_exported.txt — DELETE THIS FILE after use!")
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)
    finally:
        await client.stop()


async def main():
    args = sys.argv[1:]
    
    # Parse --from-file flag
    from_file = None
    for i, arg in enumerate(args):
        if arg in ("--from-file", "--export") and i + 1 < len(args):
            from_file = args[i + 1]
    
    api_id, api_hash = get_credentials()

    if from_file:
        await export_from_file(api_id, api_hash, from_file)
    else:
        session_name = os.getenv("PYROGRAM_SESSION_NAME", "pyrogram_session")
        await create_session(api_id, api_hash, session_name)


if __name__ == "__main__":
    asyncio.run(main())
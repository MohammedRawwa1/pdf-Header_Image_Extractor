"""Helper script to set bot commands (async)."""

import asyncio
import os

from telegram import Bot, BotCommand

from utils.i18n import command_menu

#: One list, shared with bot.py and /setcommands (was a stale 5-entry copy).
DEFAULT_COMMANDS = command_menu()


async def set_commands(token: str, commands=None):
    bot = Bot(token=token)
    cmds = commands or DEFAULT_COMMANDS
    bot_commands = [BotCommand(cmd, desc) for cmd, desc in cmds]
    await bot.set_my_commands(bot_commands)
    await bot.close()


if __name__ == "__main__":
    token = os.getenv("BOT_TOKEN")
    if not token:
        print("Missing BOT_TOKEN environment variable")
        raise SystemExit(1)
    asyncio.run(set_commands(token))

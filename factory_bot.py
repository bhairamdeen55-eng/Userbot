# ────────────────────────────────────────────────
#   FUN BOT — CLONE FACTORY (management Bot API bot)
#
#   WHAT THIS DOES:
#   - Runs as a normal Telegram Bot (via @BotFather token), NOT a userbot.
#   - Lets anyone generate their own fun_bot clone by giving API_ID,
#     API_HASH, and OWNER_ID through chat.
#   - Deliberately does NOT ask for phone number or OTP anywhere in this
#     bot. Login for each clone happens in the owner's own terminal by
#     running funbot_core.py --login-only, where Telethon's own prompt
#     asks for phone/code directly — this bot never sees or stores it.
#   - Once a clone has logged in (its session file exists), this factory
#     auto-starts and monitors it as a background subprocess.
#   - Provides a settings menu so each owner can tweak their own clone's
#     cooldown / reactions without touching a terminal again.
#
#   SETUP:
#   1. Get a bot token from @BotFather → set BOT_TOKEN below.
#   2. Get your own API_ID / API_HASH from my.telegram.org → set below
#      (this is for the FACTORY bot's own connection, separate from any
#      clone's credentials).
#   3. Make sure funbot_core.py sits in the same folder as this script.
#   4. python factory_bot.py
# ────────────────────────────────────────────────

import asyncio
import json
import logging
import os
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

import psutil
from aiohttp import web
from telethon import TelegramClient, events, Button
from telethon.errors import RPCError

try:
    from motor.motor_asyncio import AsyncIOMotorClient
except ImportError:
    AsyncIOMotorClient = None

# ────────────────────────────────────────────────
#   CONFIG — reads from environment variables (set these in Koyeb's
#   dashboard under your app's Environment Variables, NOT in this file).
#   Local testing: you can still hardcode fallback values below, but
#   never commit real secrets to a public/shared repo.
# ────────────────────────────────────────────────
FACTORY_API_ID = int(os.environ.get("FACTORY_API_ID", "12345678"))
FACTORY_API_HASH = os.environ.get("FACTORY_API_HASH", "your_api_hash_here")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "123456789:your-botfather-token-here")

# Numeric Telegram user ID of YOU, the person running this factory bot.
# Only this ID can open /admin and see the owner panel below.
FACTORY_OWNER_ID = int(os.environ.get("FACTORY_OWNER_ID", "0"))

# MongoDB — set MONGODB_URI to your Atlas connection string as an env var.
# If left empty, the bot falls back to local-files-only mode (fine for a
# real VPS with persistent disk; NOT safe on Koyeb/Render where the
# filesystem resets on every redeploy).
MONGODB_URI = os.environ.get("MONGODB_URI", "")
MONGODB_DB_NAME = os.environ.get("MONGODB_DB_NAME", "funbot_factory")

# ── Public/restricted Mongo URI (used ONLY in the Colab login snippet we
# hand to end-users, so they never see your main admin-level MONGODB_URI).
# Create a SEPARATE MongoDB Atlas database user with readWrite access
# scoped to MONGODB_DB_NAME only (not admin/dbAdmin) and put its
# connection string here. Falls back to MONGODB_URI if not set — but
# that means every user who runs the Colab step sees your full-access
# connection string, so only leave this unset if you fully trust every
# person who will ever use this bot.
PUBLIC_MONGODB_URI = os.environ.get("PUBLIC_MONGODB_URI", MONGODB_URI)

# Raw GitHub URL to funbot_core.py — the Colab snippet downloads it from
# here so end-users don't need git installed on their own device.
CORE_SCRIPT_RAW_URL = os.environ.get(
    "CORE_SCRIPT_RAW_URL",
    "https://raw.githubusercontent.com/bhairamdeen55-eng/Userbot/main/funbot_core.py",
)

# How often (seconds) the factory re-checks MongoDB for freshly-logged-in
# clones (session_string appeared) without needing a manual redeploy.
MONGO_SYNC_INTERVAL_SECONDS = int(os.environ.get("MONGO_SYNC_INTERVAL_SECONDS", "60"))

# Resource Usage Guard thresholds (feature 8) — a clone restarts itself if
# it crosses either of these, sustained, to protect the whole host.
MAX_CPU_PERCENT = float(os.environ.get("MAX_CPU_PERCENT", "80"))
MAX_MEM_MB = float(os.environ.get("MAX_MEM_MB", "300"))

BASE_DIR = Path(__file__).resolve().parent
CLONES_DIR = BASE_DIR / "clones"
CORE_SCRIPT = BASE_DIR / "funbot_core.py"
CLONES_DIR.mkdir(exist_ok=True)

WATCH_INTERVAL_SECONDS = 15   # how often the factory checks/restarts clones
AUTO_BACKUP_INTERVAL_SECONDS = 24 * 60 * 60   # send a backup to the owner every 24h

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.FileHandler("factory_bot.log", encoding="utf-8"), logging.StreamHandler()],
)
log = logging.getLogger("factory")

factory = TelegramClient("factory_bot_session", FACTORY_API_ID, FACTORY_API_HASH)

running_procs: Dict[str, subprocess.Popen] = {}   # owner_id(str) -> process handle

# ── MongoDB (feature 9) — optional. If MONGODB_URI is unset, mongo_clones
# stays None and every mongo_* helper below becomes a safe no-op, so the
# bot still works purely off local files (e.g. a real VPS). ──
mongo_client = AsyncIOMotorClient(MONGODB_URI) if (MONGODB_URI and AsyncIOMotorClient) else None
mongo_db = mongo_client[MONGODB_DB_NAME] if mongo_client is not None else None
mongo_clones = mongo_db["clones"] if mongo_db is not None else None

MAIN_MENU = [
    [Button.inline("🧬 Make My Own Clone", b"make_clone")],
    [Button.inline("📋 My Clone Status", b"my_status")],
    [Button.inline("⚙️ My Clone Settings", b"my_settings")],
    [Button.inline("🗑️ Delete My Clone", b"delete_clone")],
]

BACK_BUTTON = [[Button.inline("⬅️ Back", b"back_main")]]


# ────────────────────────────────────────────────
#   PER-OWNER FILE HELPERS
# ────────────────────────────────────────────────
def clone_dir(owner_id: int) -> Path:
    d = CLONES_DIR / str(owner_id)
    d.mkdir(parents=True, exist_ok=True)
    return d


def config_path(owner_id: int) -> Path:
    return clone_dir(owner_id) / "config.json"


def settings_path(owner_id: int) -> Path:
    return clone_dir(owner_id) / "settings.json"


def session_path(owner_id: int) -> Path:
    return clone_dir(owner_id) / "funbot_session.txt"


def load_json(path: Path) -> Optional[dict]:
    if path.is_file():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception as e:
            log.error(f"Could not read {path}: {e}")
    return None


def save_json(path: Path, obj: dict):
    path.write_text(json.dumps(obj, indent=2), encoding="utf-8")


def default_settings() -> dict:
    return {"default_cooldown": 3, "react_enabled_global": True}


def is_factory_owner(uid: int) -> bool:
    return FACTORY_OWNER_ID != 0 and uid == FACTORY_OWNER_ID


def all_owner_dirs():
    """Yield (owner_key, owner_dir) for every registered clone folder."""
    for d in CLONES_DIR.iterdir():
        if d.is_dir():
            yield d.name, d


# ────────────────────────────────────────────────
#   MONGODB HELPERS (feature 9 — Database Integration)
#   config.json / settings.json / funbot_session.txt stay as the LOCAL
#   working copies (funbot_core.py subprocess reads them directly), but
#   every change is mirrored to Mongo, and mongo_hydrate_all() rebuilds
#   those local files from Mongo on startup — so a redeploy on a
#   no-persistent-disk host (Koyeb, Render) doesn't lose anything.
# ────────────────────────────────────────────────
async def mongo_upsert_clone(owner_id: int, cfg: Optional[dict] = None, settings: Optional[dict] = None):
    if mongo_clones is None:
        return
    doc = {}
    if cfg is not None:
        doc["config"] = cfg
    if settings is not None:
        doc["settings"] = settings
    if not doc:
        return
    try:
        await mongo_clones.update_one({"owner_id": owner_id}, {"$set": doc}, upsert=True)
    except Exception as e:
        log.error(f"Mongo upsert failed for {owner_id}: {e}")


async def mongo_delete_clone(owner_id: int):
    if mongo_clones is None:
        return
    try:
        await mongo_clones.delete_one({"owner_id": owner_id})
    except Exception as e:
        log.error(f"Mongo delete failed for {owner_id}: {e}")


async def mongo_hydrate_all():
    """Runs once at startup. Rebuilds every clones/<owner_id>/ local file
    (config, settings, session string) from what's stored in MongoDB."""
    if mongo_clones is None:
        log.info("MONGODB_URI not set — running in local-files-only mode.")
        return
    try:
        count = 0
        async for doc in mongo_clones.find({}):
            owner_id = doc.get("owner_id")
            if owner_id is None:
                continue
            cfg = doc.get("config")
            settings = doc.get("settings")
            session_str = doc.get("session_string")
            if cfg:
                save_json(config_path(owner_id), cfg)
            if settings:
                save_json(settings_path(owner_id), settings)
            if session_str:
                session_path(owner_id).write_text(session_str, encoding="utf-8")
            count += 1
        log.info(f"✅ Hydrated {count} clone(s) from MongoDB.")
    except Exception as e:
        log.error(f"Mongo hydrate failed: {e}")


async def mongo_sync_new_sessions():
    """Runs every MONGO_SYNC_INTERVAL_SECONDS in the background (not just at
    startup). If someone just finished the Colab login step, their
    session_string appears in Mongo — this pulls it down to the local
    session file so process_watcher can auto-start their clone within one
    watch cycle, with no manual redeploy needed."""
    if mongo_clones is None:
        return
    while True:
        await asyncio.sleep(MONGO_SYNC_INTERVAL_SECONDS)
        try:
            async for doc in mongo_clones.find({"session_string": {"$exists": True, "$ne": ""}}):
                owner_id = doc.get("owner_id")
                if owner_id is None:
                    continue
                sess_file = session_path(owner_id)
                mongo_sess = doc.get("session_string", "")
                local_sess = sess_file.read_text(encoding="utf-8").strip() if sess_file.is_file() else ""
                if mongo_sess and mongo_sess != local_sess:
                    # config/settings may also exist only in Mongo if this
                    # device never had them locally (fresh container)
                    cfg = doc.get("config")
                    settings = doc.get("settings")
                    if cfg and not config_path(owner_id).is_file():
                        save_json(config_path(owner_id), cfg)
                    if settings and not settings_path(owner_id).is_file():
                        save_json(settings_path(owner_id), settings)
                    sess_file.write_text(mongo_sess, encoding="utf-8")
                    log.info(f"🔄 Synced new session for owner {owner_id} from MongoDB (no redeploy needed).")
        except Exception as e:
            log.error(f"mongo_sync_new_sessions error: {e}")


# ────────────────────────────────────────────────
#   /start AND MAIN MENU
# ────────────────────────────────────────────────
@factory.on(events.NewMessage(pattern=r"^/start$"))
async def start(event):
    await event.reply(
        "👋 **Fun Bot — Clone Factory**\n\n"
        "Apna khud ka fun-reaction userbot banao. Login kabhi bhi is bot ke "
        "through nahi hota — phone number/OTP hamesha tumhare apne terminal "
        "me, sirf tumhare control me.\n\n"
        "Neeche se option chuno:",
        buttons=MAIN_MENU,
    )


@factory.on(events.CallbackQuery(data=b"back_main"))
async def back_main(event):
    await event.edit(
        "👋 **Fun Bot — Clone Factory**\n\nNeeche se option chuno:",
        buttons=MAIN_MENU,
    )


# ────────────────────────────────────────────────
#   MAKE MY OWN CLONE (wizard — API_ID / API_HASH / OWNER_ID only)
# ────────────────────────────────────────────────
@factory.on(events.CallbackQuery(data=b"make_clone"))
async def make_clone(event):
    owner_id = event.sender_id
    if config_path(owner_id).is_file():
        await event.answer("Aapka clone already exist karta hai — Status/Settings se manage karo.", alert=True)
        return
    await event.answer()
    await run_clone_wizard(owner_id, event.chat_id)


async def run_clone_wizard(owner_id: int, chat_id: int):
    try:
        async with factory.conversation(chat_id, timeout=300) as conv:
            await conv.send_message(
                "🧬 **Naya Clone Setup — Step 1/3**\n\n"
                "Apna **API_ID** bhejo (my.telegram.org se, sirf number)."
            )
            r = await conv.get_response()
            api_id_text = r.raw_text.strip()
            if not api_id_text.isdigit():
                await conv.send_message("⚠️ API_ID sirf number hona chahiye. **Make My Own Clone** se dobara try karo.")
                return
            api_id = int(api_id_text)

            await conv.send_message("**Step 2/3** — apna **API_HASH** bhejo.")
            r = await conv.get_response()
            api_hash = r.raw_text.strip()
            if len(api_hash) < 10:
                await conv.send_message("⚠️ Ye API_HASH sahi nahi lag raha. **Make My Own Clone** se dobara try karo.")
                return

            await conv.send_message(
                "**Step 3/3** — apna **numeric Telegram user ID** (OWNER_ID) bhejo.\n"
                "Pata nahi to @userinfobot ko message karo."
            )
            r = await conv.get_response()
            owner_id_text = r.raw_text.strip()
            if not owner_id_text.isdigit():
                await conv.send_message("⚠️ OWNER_ID sirf number hona chahiye. **Make My Own Clone** se dobara try karo.")
                return

            cfg = {
                "api_id": api_id,
                "api_hash": api_hash,
                "owner_id": int(owner_id_text),
                "session_file": str(session_path(owner_id)),
                "data_file": str(clone_dir(owner_id) / "funbot_data.json"),
                "settings_file": str(settings_path(owner_id)),
                "log_file": str(clone_dir(owner_id) / "funbot.log"),
                "factory_owner_telegram_id": owner_id,
                "mongodb_uri": MONGODB_URI,
                "mongodb_db_name": MONGODB_DB_NAME,
            }
            save_json(config_path(owner_id), cfg)
            save_json(settings_path(owner_id), default_settings())
            await mongo_upsert_clone(owner_id, cfg=cfg, settings=default_settings())

            colab_snippet = (
                "!pip install -q telethon pymongo\n\n"
                "import json, urllib.request\n\n"
                "config = {\n"
                f'    "api_id": {api_id},\n'
                f'    "api_hash": "{api_hash}",\n'
                f'    "owner_id": {int(owner_id_text)},\n'
                f'    "factory_owner_telegram_id": {owner_id},\n'
                '    "session_file": "funbot_session.txt",\n'
                f'    "mongodb_uri": "{PUBLIC_MONGODB_URI}",\n'
                f'    "mongodb_db_name": "{MONGODB_DB_NAME}",\n'
                "}\n"
                "with open(\"config.json\", \"w\") as f:\n"
                "    json.dump(config, f)\n\n"
                f'urllib.request.urlretrieve("{CORE_SCRIPT_RAW_URL}", "funbot_core.py")\n\n'
                "!python funbot_core.py --config config.json --login-only"
            )
            await conv.send_message(
                "✅ **Config ban gaya!**\n\n"
                "⚠️ Security ki wajah se login is bot ke through kabhi nahi hota — "
                "phone number ya OTP main kabhi nahi maangta. Login sirf ek baar, "
                "khud tumhare control me hoga — Google Colab ke through (free, "
                "kuch install nahi karna):\n\n"
                "**Kaise karna hai:**\n"
                "1️⃣ [colab.research.google.com](https://colab.research.google.com) kholo, "
                "**New Notebook** banao\n"
                "2️⃣ Neeche diya poora code ek cell me paste karo\n"
                "3️⃣ ▶️ Run dabao\n"
                "4️⃣ Cell ke neeche phone number aur OTP maangega — wahi type karke Enter dabao "
                "(ye seedha Telegram ko jata hai, mujhe kabhi nahi dikhta)\n\n"
                f"```\n{colab_snippet}\n```\n\n"
                "Login ho jaane ke ~1 minute baad (main Mongo automatically check karta rehta "
                "hoon) tumhara clone khud start ho jayega. **📋 My Clone Status** dabake confirm "
                "kar sakte ho.",
                buttons=MAIN_MENU,
            )
    except asyncio.TimeoutError:
        await factory.send_message(chat_id, "⏳ Time out ho gaya. **Make My Own Clone** se dobara try karo.", buttons=MAIN_MENU)
    except Exception as e:
        log.error(f"Wizard error for {owner_id}: {e}")
        await factory.send_message(chat_id, f"❌ Kuch galat ho gaya: {e}", buttons=MAIN_MENU)


# ────────────────────────────────────────────────
#   STATUS
# ────────────────────────────────────────────────
@factory.on(events.CallbackQuery(data=b"my_status"))
async def my_status(event):
    owner_id = event.sender_id
    if not config_path(owner_id).is_file():
        await event.answer("Aapka koi clone nahi bana hai.", alert=True)
        return

    if not session_path(owner_id).is_file():
        await event.edit(
            "⏳ **Login baaki hai**\n\n"
            "Config ban chuka hai lekin login abhi complete nahi hua. "
            "Wizard ke message me diya gaya command apne terminal me chalao.",
            buttons=MAIN_MENU,
        )
        return

    proc = running_procs.get(str(owner_id))
    alive = proc is not None and proc.poll() is None
    status_txt = "🟢 Running" if alive else "🟡 Login ho chuka hai, agle check me (≤15s) auto-start hoga"
    await event.edit(f"📋 **Clone Status**\n{status_txt}", buttons=MAIN_MENU)


# ────────────────────────────────────────────────
#   SETTINGS
# ────────────────────────────────────────────────
@factory.on(events.CallbackQuery(data=b"my_settings"))
async def my_settings(event):
    owner_id = event.sender_id
    if not config_path(owner_id).is_file():
        await event.answer("Pehle apna clone banao.", alert=True)
        return

    s = load_json(settings_path(owner_id)) or default_settings()
    react_state = "ON ✅" if s.get("react_enabled_global", True) else "OFF ❌"
    await event.edit(
        "⚙️ **My Clone Settings**\n\n"
        f"⏱️ Default cooldown: **{s.get('default_cooldown', 3)}s**\n"
        f"😀 Reactions (all groups): **{react_state}**\n\n"
        "Change karne ke liye neeche se chuno (~20s me clone pe apply ho jayega, restart ki zaroorat nahi):",
        buttons=[
            [Button.inline("⏱️ 3s", b"cd_3"), Button.inline("⏱️ 30s", b"cd_30"), Button.inline("⏱️ 60s", b"cd_60")],
            [Button.inline("😀 Reactions ON", b"react_on"), Button.inline("🙅 Reactions OFF", b"react_off")],
            [Button.inline("⬅️ Back", b"back_main")],
        ],
    )


@factory.on(events.CallbackQuery(pattern=b"^cd_(\\d+)$"))
async def set_cooldown(event):
    owner_id = event.sender_id
    if not config_path(owner_id).is_file():
        await event.answer("Pehle apna clone banao.", alert=True)
        return
    seconds = int(event.pattern_match.group(1))
    s = load_json(settings_path(owner_id)) or default_settings()
    s["default_cooldown"] = seconds
    save_json(settings_path(owner_id), s)
    await mongo_upsert_clone(owner_id, settings=s)
    await event.answer(f"Cooldown {seconds}s set ho gaya ✅")
    await my_settings(event)


@factory.on(events.CallbackQuery(data=b"react_on"))
async def react_on(event):
    owner_id = event.sender_id
    if not config_path(owner_id).is_file():
        await event.answer("Pehle apna clone banao.", alert=True)
        return
    s = load_json(settings_path(owner_id)) or default_settings()
    s["react_enabled_global"] = True
    save_json(settings_path(owner_id), s)
    await mongo_upsert_clone(owner_id, settings=s)
    await event.answer("Reactions ON ✅")
    await my_settings(event)


@factory.on(events.CallbackQuery(data=b"react_off"))
async def react_off(event):
    owner_id = event.sender_id
    if not config_path(owner_id).is_file():
        await event.answer("Pehle apna clone banao.", alert=True)
        return
    s = load_json(settings_path(owner_id)) or default_settings()
    s["react_enabled_global"] = False
    save_json(settings_path(owner_id), s)
    await mongo_upsert_clone(owner_id, settings=s)
    await event.answer("Reactions OFF ❌")
    await my_settings(event)


# ────────────────────────────────────────────────
#   DELETE CLONE
# ────────────────────────────────────────────────
@factory.on(events.CallbackQuery(data=b"delete_clone"))
async def delete_clone(event):
    owner_id = event.sender_id
    if not config_path(owner_id).is_file():
        await event.answer("Aapka koi clone nahi hai.", alert=True)
        return
    await event.edit(
        "🗑️ **Pakka delete karna hai?**\n\n"
        "Ye tumhara clone stop kar dega aur uska config/session/data sab delete kar dega. "
        "Ye undo nahi ho sakta.",
        buttons=[
            [Button.inline("✅ Haan, delete karo", b"confirm_delete")],
            [Button.inline("❌ Nahi, cancel", b"back_main")],
        ],
    )


@factory.on(events.CallbackQuery(data=b"confirm_delete"))
async def confirm_delete(event):
    owner_id = event.sender_id
    owner_key = str(owner_id)
    proc = running_procs.pop(owner_key, None)
    if proc and proc.poll() is None:
        proc.terminate()
        log.info(f"Terminated clone process for {owner_id}")

    d = clone_dir(owner_id)
    try:
        for f in d.iterdir():
            f.unlink()
        d.rmdir()
    except Exception as e:
        log.error(f"Delete error for {owner_id}: {e}")

    await mongo_delete_clone(owner_id)
    await event.edit("🗑️ Clone delete ho gaya.", buttons=MAIN_MENU)


# ────────────────────────────────────────────────
#   OWNER PANEL (only you — FACTORY_OWNER_ID — can open this)
# ────────────────────────────────────────────────
ADMIN_MENU = [
    [Button.inline("📢 Broadcast Message", b"adm_broadcast")],
    [Button.inline("👥 View Users", b"adm_users")],
    [Button.inline("🐞 View Errors", b"adm_errors")],
    [Button.inline("📊 Stats", b"adm_stats")],
    [Button.inline("📦 Backup Now", b"adm_backup")],
]


def make_backup_zip() -> Path:
    """Zips the whole clones/ folder (configs + sessions + data) into a
    timestamped .zip in a temp dir and returns its path. Caller should
    delete it after sending."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_base = Path(tempfile.gettempdir()) / f"funbot_backup_{ts}"
    archive_path = shutil.make_archive(str(out_base), "zip", root_dir=str(CLONES_DIR))
    return Path(archive_path)


async def send_backup_to_owner(reason: str = "manual"):
    if FACTORY_OWNER_ID == 0:
        # FACTORY_OWNER_ID not configured — nothing to send to
        return
    try:
        zip_path = make_backup_zip()
        size_mb = zip_path.stat().st_size / (1024 * 1024)
        await factory.send_file(
            FACTORY_OWNER_ID,
            str(zip_path),
            caption=f"📦 **Backup** ({reason})\n🗓️ {datetime.now():%Y-%m-%d %H:%M}\n💾 {size_mb:.1f} MB\n\n"
                    "⚠️ Isme sabhi clones ke config + login session hain — kisi ko forward mat karo, "
                    "jiske paas ye file jaayegi wo un clones ko poora control kar sakta hai.",
        )
        zip_path.unlink(missing_ok=True)
        log.info(f"Backup sent to owner ({reason})")
    except Exception as e:
        log.error(f"Backup send failed: {e}")


async def backup_scheduler():
    while True:
        await asyncio.sleep(AUTO_BACKUP_INTERVAL_SECONDS)
        await send_backup_to_owner(reason="daily auto-backup")


async def restore_latest_backup_from_telegram():
    """Runs once at startup. Looks through the factory bot's own DM history
    with the owner for the most recent backup .zip it ever sent, downloads
    it, and unpacks it into clones/. This is what makes the setup survive
    a redeploy/restart on a host with no persistent disk (Koyeb, Render,
    etc.) — Telegram itself becomes the storage."""
    if FACTORY_OWNER_ID == 0:
        log.info("FACTORY_OWNER_ID not set — skipping backup restore.")
        return
    if any(CLONES_DIR.iterdir()):
        log.info("clones/ already has data — skipping restore (not a fresh container).")
        return
    try:
        async for msg in factory.iter_messages(FACTORY_OWNER_ID, limit=200):
            fname = getattr(msg.file, "name", None) if msg.file else None
            if fname and fname.startswith("funbot_backup_") and fname.endswith(".zip"):
                tmp_zip = Path(tempfile.gettempdir()) / fname
                await msg.download_media(file=str(tmp_zip))
                shutil.unpack_archive(str(tmp_zip), str(CLONES_DIR))
                tmp_zip.unlink(missing_ok=True)
                log.info(f"✅ Restored clones/ from backup message id={msg.id} ({fname})")
                return
        log.info("No previous backup found in chat history — starting fresh.")
    except Exception as e:
        log.error(f"Backup restore failed: {e}")


# ────────────────────────────────────────────────
#   TINY HTTP HEALTH SERVER — required by platforms like Koyeb/Render
#   that expect a process to listen on $PORT. Does nothing else.
# ────────────────────────────────────────────────
async def start_health_server():
    port = int(os.environ.get("PORT", 8080))
    app = web.Application()
    app.router.add_get("/", lambda request: web.Response(text="funbot factory is alive"))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    log.info(f"Health server listening on port {port}")


@factory.on(events.NewMessage(pattern=r"^/admin$"))
async def admin_panel(event):
    if not is_factory_owner(event.sender_id):
        return  # silently ignore — don't reveal the panel exists to others
    await event.reply("👑 **Owner Panel**\n\nChuno:", buttons=ADMIN_MENU)


@factory.on(events.CallbackQuery(data=b"adm_back"))
async def admin_back(event):
    if not is_factory_owner(event.sender_id):
        await event.answer("⛔ Ye sirf owner ke liye hai.", alert=True)
        return
    await event.edit("👑 **Owner Panel**\n\nChuno:", buttons=ADMIN_MENU)


# ── Broadcast: send a message to every registered clone owner ──
@factory.on(events.CallbackQuery(data=b"adm_broadcast"))
async def adm_broadcast(event):
    if not is_factory_owner(event.sender_id):
        await event.answer("⛔ Ye sirf owner ke liye hai.", alert=True)
        return
    await event.answer()
    chat_id = event.chat_id
    try:
        async with factory.conversation(chat_id, timeout=300) as conv:
            await conv.send_message(
                "📢 **Broadcast — Step 1/2**\n\n"
                "Jo message bhejna hai wo yaha type karo. Markdown chalta hai "
                "(**bold**, _italic_, [link text](https://example.com)). "
                "Cancel ke liye /cancel."
            )
            r = await conv.get_response()
            if r.raw_text.strip().lower() == "/cancel":
                await conv.send_message("❌ Cancel kar diya.", buttons=ADMIN_MENU)
                return
            msg_text = r.raw_text

            await conv.send_message(
                "**Step 2/2** — Message ke neeche ek button bhi laga sakte ho.\n"
                "Format: `Button Label | https://example.com`\n"
                "Nahi chahiye to /skip bhejo."
            )
            r2 = await conv.get_response()
            btn_input = r2.raw_text.strip()
            buttons = None
            if btn_input.lower() != "/skip" and "|" in btn_input:
                label, url = btn_input.split("|", 1)
                label, url = label.strip(), url.strip()
                if label and url.startswith(("http://", "https://", "tg://")):
                    buttons = [[Button.url(label, url)]]
                else:
                    await conv.send_message("⚠️ Button URL http(s):// se shuru honi chahiye — button skip kar diya.")

            sent, failed = 0, 0
            for owner_key, _ in all_owner_dirs():
                try:
                    target_id = int(owner_key)
                except ValueError:
                    continue
                try:
                    await factory.send_message(
                        target_id,
                        f"📢 **Announcement**\n\n{msg_text}",
                        buttons=buttons,
                        parse_mode="markdown",
                        link_preview=False,
                    )
                    sent += 1
                except Exception as e:
                    failed += 1
                    log.warning(f"Broadcast failed for {target_id}: {e}")
                await asyncio.sleep(0.3)  # gentle pacing, avoid flood limits

            await conv.send_message(
                f"✅ Broadcast bhej diya.\n📨 Sent: {sent}  ❌ Failed: {failed}\n\n"
                "(Failed usually matlab wo user ne is bot ko kabhi /start nahi kiya, "
                "bot API bots sirf unhi ko DM kar sakte hain jinhone pehle bot ko start kiya ho.)",
                buttons=ADMIN_MENU,
            )
    except asyncio.TimeoutError:
        await factory.send_message(chat_id, "⏳ Time out ho gaya.", buttons=ADMIN_MENU)


# ── View Users: list every registered clone with status ──
@factory.on(events.CallbackQuery(data=b"adm_users"))
async def adm_users(event):
    if not is_factory_owner(event.sender_id):
        await event.answer("⛔ Ye sirf owner ke liye hai.", alert=True)
        return
    await event.answer()

    rows = []
    for owner_key, owner_dir in all_owner_dirs():
        cfg = load_json(owner_dir / "config.json")
        sess_exists = (owner_dir / "funbot_session.txt").is_file()
        proc = running_procs.get(owner_key)
        alive = proc is not None and proc.poll() is None
        if not cfg:
            state = "⚠️ config missing"
        elif not sess_exists:
            state = "⏳ not logged in"
        elif alive:
            state = "🟢 running"
        else:
            state = "🟡 stopped"
        rows.append(f"• `{owner_key}` — {state}")

    if not rows:
        text = "👥 **Users**\n\nAbhi tak koi clone register nahi hua."
    else:
        text = "👥 **Users** (" + str(len(rows)) + ")\n\n" + "\n".join(rows)

    # Telegram messages cap out around 4096 chars — trim if the list is huge
    if len(text) > 3900:
        text = text[:3900] + "\n\n… (list truncated)"

    await event.edit(text, buttons=[[Button.inline("⬅️ Back", b"adm_back")]])


# ── View Errors: tail the factory log + each clone's recent error lines ──
@factory.on(events.CallbackQuery(data=b"adm_errors"))
async def adm_errors(event):
    if not is_factory_owner(event.sender_id):
        await event.answer("⛔ Ye sirf owner ke liye hai.", alert=True)
        return
    await event.answer()

    chunks = []

    # factory bot's own log
    factory_log = BASE_DIR / "factory_bot.log"
    if factory_log.is_file():
        lines = factory_log.read_text(encoding="utf-8", errors="ignore").splitlines()
        err_lines = [l for l in lines if "ERROR" in l or "WARNING" in l][-10:]
        if err_lines:
            chunks.append("**Factory log:**\n```\n" + "\n".join(err_lines) + "\n```")

    # each clone's own log / process output
    for owner_key, owner_dir in all_owner_dirs():
        for fname in ("funbot.log", "clone_output.log"):
            fpath = owner_dir / fname
            if not fpath.is_file():
                continue
            lines = fpath.read_text(encoding="utf-8", errors="ignore").splitlines()
            err_lines = [l for l in lines if "ERROR" in l] [-5:]
            if err_lines:
                chunks.append(f"**Clone {owner_key} ({fname}):**\n```\n" + "\n".join(err_lines) + "\n```")

    if not chunks:
        text = "🐞 **Errors**\n\nAbhi tak koi error log nahi mila. Sab theek lag raha hai ✅"
    else:
        text = "🐞 **Recent Errors**\n\n" + "\n\n".join(chunks)

    if len(text) > 3900:
        text = text[:3900] + "\n\n… (truncated, poore logs ke liye server pe log files check karo)"

    await event.edit(text, buttons=[[Button.inline("⬅️ Back", b"adm_back")]])


# ── Stats: quick numeric summary ──
@factory.on(events.CallbackQuery(data=b"adm_stats"))
async def adm_stats(event):
    if not is_factory_owner(event.sender_id):
        await event.answer("⛔ Ye sirf owner ke liye hai.", alert=True)
        return
    await event.answer()

    total = logged_in = running = 0
    for owner_key, owner_dir in all_owner_dirs():
        total += 1
        if (owner_dir / "funbot_session.txt").is_file():
            logged_in += 1
        proc = running_procs.get(owner_key)
        if proc is not None and proc.poll() is None:
            running += 1

    text = (
        "📊 **Stats**\n\n"
        f"👥 Total registered clones: **{total}**\n"
        f"🔑 Logged in: **{logged_in}**\n"
        f"🟢 Currently running: **{running}**"
    )
    await event.edit(text, buttons=[[Button.inline("⬅️ Back", b"adm_back")]])


# ── Backup Now: on-demand zip of all clone data, sent to owner's DM ──
@factory.on(events.CallbackQuery(data=b"adm_backup"))
async def adm_backup(event):
    if not is_factory_owner(event.sender_id):
        await event.answer("⛔ Ye sirf owner ke liye hai.", alert=True)
        return
    await event.answer("📦 Backup bana raha hoon…")
    await send_backup_to_owner(reason="manual (owner panel)")
    await event.edit(
        "✅ Backup tumhari DM me bhej diya (isi chat me upar dekho).",
        buttons=[[Button.inline("⬅️ Back", b"adm_back")]],
    )


# ────────────────────────────────────────────────
#   BACKGROUND WATCHER — auto-starts/monitors every logged-in clone
#   Now also detects crashes vs a clean exit, DMs the clone owner (and
#   you) with a summary, and specifically flags an invalid/expired
#   session so the owner knows to re-login instead of the watcher
#   silently retrying forever (features 1 & 2).
# ────────────────────────────────────────────────
def start_clone_process(owner_key: str, owner_dir: Path, cfg_file: Path):
    log_path = owner_dir / "clone_output.log"
    logf = open(log_path, "a", encoding="utf-8")
    p = subprocess.Popen(
        [sys.executable, str(CORE_SCRIPT), "--config", str(cfg_file)],
        stdout=logf, stderr=subprocess.STDOUT,
        cwd=str(owner_dir),
    )
    running_procs[owner_key] = p
    log.info(f"(Re)started clone for owner {owner_key} (pid {p.pid})")


async def handle_clone_crash(owner_key: str, owner_dir: Path, exit_code: Optional[int]) -> bool:
    """Returns True if the watcher should restart this clone, False if it
    should wait for the owner to fix something (e.g. re-login)."""
    log_path = owner_dir / "clone_output.log"
    tail = ""
    if log_path.is_file():
        lines = log_path.read_text(encoding="utf-8", errors="ignore").splitlines()
        tail = "\n".join(lines[-15:])

    session_invalid = any(
        marker in tail for marker in
        ("AuthKeyInvalidError", "AuthKeyUnregisteredError", "SessionRevokedError", "UserDeactivatedError", "SessionPasswordNeededError")
    )

    try:
        owner_id = int(owner_key)
    except ValueError:
        owner_id = None

    if session_invalid:
        text = (
            "🔑 **Session Expired / Invalid**\n\n"
            "Tumhara clone ka login session invalid ho gaya hai (kahi aur se logout "
            "hua ho sakta hai, ya session revoke ho gayi). Jab tak dobara login nahi "
            "karte, bot start nahi hoga.\n\n"
            "Apne terminal me ye chalao:\n"
            f"```\ncd {owner_dir}\npython {CORE_SCRIPT} --config config.json --login-only\n```"
        )
        try:
            (owner_dir / "funbot_session.txt").unlink(missing_ok=True)
            if mongo_clones is not None and owner_id is not None:
                await mongo_clones.update_one({"owner_id": owner_id}, {"$unset": {"session_string": ""}})
        except Exception:
            pass
        should_restart = False
    else:
        text = (
            "⚠️ **Clone Crash — Restarting**\n\n"
            f"Tumhara clone crash ho gaya (exit code: {exit_code}). Main ise turant "
            "restart kar raha hoon.\n\n"
            f"**Last log lines:**\n```\n{(tail[-800:] if tail else 'log khali hai')}\n```"
        )
        should_restart = True

    if owner_id:
        try:
            await factory.send_message(owner_id, text)
        except Exception as e:
            log.warning(f"Could not DM clone owner {owner_id}: {e}")
    if FACTORY_OWNER_ID and owner_id != FACTORY_OWNER_ID:
        try:
            await factory.send_message(FACTORY_OWNER_ID, f"🐞 Clone `{owner_key}` crashed.\n\n{text}")
        except Exception:
            pass

    return should_restart


async def process_watcher():
    while True:
        try:
            for owner_dir in CLONES_DIR.iterdir():
                if not owner_dir.is_dir():
                    continue
                owner_key = owner_dir.name
                cfg_file = owner_dir / "config.json"
                sess_file = owner_dir / "funbot_session.txt"
                if not cfg_file.is_file() or not sess_file.is_file():
                    continue  # not configured yet, or login not done yet

                proc = running_procs.get(owner_key)
                if proc is None:
                    start_clone_process(owner_key, owner_dir, cfg_file)
                elif proc.poll() is not None:
                    exit_code = proc.returncode
                    running_procs.pop(owner_key, None)
                    log.warning(f"Clone {owner_key} exited (code {exit_code})")
                    should_restart = await handle_clone_crash(owner_key, owner_dir, exit_code)
                    if should_restart and sess_file.is_file():
                        start_clone_process(owner_key, owner_dir, cfg_file)
        except Exception as e:
            log.error(f"process_watcher error: {e}")
        await asyncio.sleep(WATCH_INTERVAL_SECONDS)


# ────────────────────────────────────────────────
#   RESOURCE USAGE GUARD (feature 8 — psutil)
#   Kills + restarts any clone that sustains high CPU/RAM, to protect
#   the whole host (important on small free-tier instances).
# ────────────────────────────────────────────────
async def resource_guard():
    while True:
        await asyncio.sleep(30)
        for owner_key, proc in list(running_procs.items()):
            if proc.poll() is not None:
                continue
            try:
                ps_proc = psutil.Process(proc.pid)
                cpu = ps_proc.cpu_percent(interval=0.5)
                mem_mb = ps_proc.memory_info().rss / (1024 * 1024)
                if cpu > MAX_CPU_PERCENT or mem_mb > MAX_MEM_MB:
                    log.warning(f"Clone {owner_key} over limits (cpu={cpu:.0f}%, mem={mem_mb:.0f}MB) — restarting")
                    proc.terminate()
                    try:
                        proc.wait(timeout=5)
                    except Exception:
                        proc.kill()
                    running_procs.pop(owner_key, None)
                    try:
                        owner_id = int(owner_key)
                        await factory.send_message(
                            owner_id,
                            "🛡️ **Resource Guard**\n\nTumhara clone high resource use kar raha "
                            f"tha (CPU {cpu:.0f}%, RAM {mem_mb:.0f}MB) — safety ke liye restart "
                            "kar diya. Agle check (≤15s) me wapas start ho jayega.",
                        )
                    except Exception:
                        pass
            except psutil.NoSuchProcess:
                continue
            except Exception as e:
                log.error(f"resource_guard error for {owner_key}: {e}")


# ────────────────────────────────────────────────
#   START
# ────────────────────────────────────────────────
async def main():
    await factory.start(bot_token=BOT_TOKEN)
    me = await factory.get_me()
    log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    log.info("🏭 CLONE FACTORY BOT STARTED")
    log.info(f"🤖 @{me.username}")
    log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    await start_health_server()   # needed on Koyeb/Render — must bind $PORT
    if mongo_clones is not None:
        await mongo_hydrate_all()               # primary: rebuild clones/ from MongoDB
    else:
        await restore_latest_backup_from_telegram()  # fallback: no DB configured
    asyncio.create_task(process_watcher())
    asyncio.create_task(backup_scheduler())
    asyncio.create_task(resource_guard())
    asyncio.create_task(mongo_sync_new_sessions())
    await factory.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())

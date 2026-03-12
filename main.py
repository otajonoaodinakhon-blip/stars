import os
import logging
import asyncio
import uuid
from datetime import datetime
from typing import Optional, Dict, List, Tuple
from collections import defaultdict

import asyncpg
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
    CallbackQuery,
    Message,
    Update
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from aiohttp import web
from dotenv import load_dotenv

load_dotenv()

# ------------------- KONFIGURATSIYA -------------------
MAIN_BOT_TOKEN = os.getenv("BOT_TOKEN")  # Asosiy platforma boti tokeni
if not MAIN_BOT_TOKEN:
    raise ValueError("BOT_TOKEN topilmadi!")

DATABASE1_URL = os.getenv("DATABASE1_URL")
DATABASE2_URL = os.getenv("DATABASE2_URL")
if not DATABASE1_URL or not DATABASE2_URL:
    raise ValueError("DATABASE1_URL va DATABASE2_URL berilishi kerak!")

ADMIN_IDS_STR = os.getenv("ADMIN_IDS")
ADMIN_IDS = [int(x) for x in ADMIN_IDS_STR.split(",")] if ADMIN_IDS_STR else []
CHANNEL_ID_STR = os.getenv("CHANNEL_ID")   # Hisobot kanali (ixtiyoriy)
CHANNEL_ID = int(CHANNEL_ID_STR) if CHANNEL_ID_STR else None

RENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL")
if not RENDER_EXTERNAL_URL:
    raise ValueError("RENDER_EXTERNAL_URL topilmadi!")
WEBHOOK_PATH = "/webhook/{token}"  # tokenni URL dan olish uchun
WEBAPP_HOST = "0.0.0.0"
WEBAPP_PORT = int(os.getenv("PORT", 8080))

# ------------------- DATABASE POOLLAR -------------------
class Database:
    pool1: asyncpg.Pool = None
    pool2: asyncpg.Pool = None

db = Database()

def get_shard(user_id: int) -> asyncpg.Pool:
    return db.pool1 if user_id % 2 == 0 else db.pool2

async def create_tables(conn: asyncpg.Connection):
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            telegram_id BIGINT UNIQUE NOT NULL,
            username TEXT,
            first_name TEXT,
            joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            bots_count INTEGER DEFAULT 0,
            is_admin BOOLEAN DEFAULT FALSE,
            balance INTEGER DEFAULT 0
        )
    """)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS bots (
            id SERIAL PRIMARY KEY,
            bot_token TEXT UNIQUE NOT NULL,
            bot_username TEXT NOT NULL,
            owner_id BIGINT NOT NULL,
            users_count INTEGER DEFAULT 0,
            status TEXT DEFAULT 'active',
            connected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            type TEXT DEFAULT 'user_bot',  -- 'platform' yoki 'user_bot'
            required_channel TEXT,          -- bot uchun majburiy kanal (username)
            FOREIGN KEY (owner_id) REFERENCES users(telegram_id) ON DELETE CASCADE
        )
    """)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS bot_users (
            id SERIAL PRIMARY KEY,
            bot_id TEXT NOT NULL,
            user_id BIGINT NOT NULL,
            joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(bot_id, user_id)
        )
    """)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            id SERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL,
            amount INTEGER NOT NULL,
            admin_id BIGINT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            note TEXT
        )
    """)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS config (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_bots_owner ON bots(owner_id)")
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_bot_users_bot ON bot_users(bot_id)")

async def init_db():
    db.pool1 = await asyncpg.create_pool(DATABASE1_URL, ssl="require")
    db.pool2 = await asyncpg.create_pool(DATABASE2_URL, ssl="require")
    async with db.pool1.acquire() as conn:
        await create_tables(conn)
    async with db.pool2.acquire() as conn:
        await create_tables(conn)
    print("✅ Ikkala PostgreSQL ga ulandi va jadvallar yaratildi")

async def close_db():
    if db.pool1:
        await db.pool1.close()
    if db.pool2:
        await db.pool2.close()

# ------------------- BOT MA'LUMOTLARI FUNKSIYALARI -------------------
async def get_bot_info(bot_token: str) -> Optional[Dict]:
    """Berilgan token haqida ma'lumot qaytaradi (qaysi shardda bo'lsa)."""
    # token qaysi user'ga tegishli? owner_id ni bilmaymiz, ikkala shardda qidiramiz
    async def search(pool):
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM bots WHERE bot_token = $1", bot_token)
            return dict(row) if row else None
    results = await asyncio.gather(search(db.pool1), search(db.pool2), return_exceptions=True)
    for res in results:
        if isinstance(res, dict) and res:
            return res
    return None

async def update_bot_channel(bot_token: str, channel: str, owner_id: int) -> bool:
    """Botning majburiy kanalini yangilaydi (faqat egasi qila oladi)."""
    pool = get_shard(owner_id)
    async with pool.acquire() as conn:
        result = await conn.execute(
            "UPDATE bots SET required_channel = $1 WHERE bot_token = $2 AND owner_id = $3",
            channel, bot_token, owner_id
        )
        return result == "UPDATE 1"

async def get_bots_by_owner(owner_id: int) -> List[Dict]:
    pool = get_shard(owner_id)
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM bots WHERE owner_id = $1 ORDER BY connected_at DESC", owner_id)
        return [dict(row) for row in rows]

async def add_bot(bot_token: str, bot_username: str, owner_id: int):
    pool = get_shard(owner_id)
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO bots (bot_token, bot_username, owner_id, connected_at, type)
            VALUES ($1, $2, $3, CURRENT_TIMESTAMP, 'user_bot')
        """, bot_token, bot_username, owner_id)
        await conn.execute("UPDATE users SET bots_count = bots_count + 1 WHERE telegram_id = $1", owner_id)

async def remove_bot(bot_token: str, owner_id: int) -> bool:
    pool = get_shard( owner_id)
    async with pool.acquire() as conn:
        result = await conn.execute("DELETE FROM bots WHERE bot_token = $1 AND owner_id = $2", bot_token, owner_id)
        if result == "DELETE 1":
            await conn.execute("UPDATE users SET bots_count = bots_count - 1 WHERE telegram_id = $1", owner_id)
            await conn.execute("DELETE FROM bot_users WHERE bot_id = $1", bot_token)
            return True
        return False

# ------------------- USER FUNKSIYALARI -------------------
async def get_user(telegram_id: int) -> Optional[Dict]:
    pool = get_shard(telegram_id)
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM users WHERE telegram_id = $1", telegram_id)
        return dict(row) if row else None

async def update_user(telegram_id: int, username: str, first_name: str, is_admin: bool = False):
    pool = get_shard(telegram_id)
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO users (telegram_id, username, first_name, joined_at, is_admin)
            VALUES ($1, $2, $3, CURRENT_TIMESTAMP, $4)
            ON CONFLICT (telegram_id) DO UPDATE SET
                username = EXCLUDED.username,
                first_name = EXCLUDED.first_name
        """, telegram_id, username, first_name, is_admin)

async def add_bot_user(bot_token: str, user_id: int):
    bot_info = await get_bot_info(bot_token)
    if not bot_info:
        return
    pool = get_shard(bot_info['owner_id'])
    async with pool.acquire() as conn:
        try:
            await conn.execute("INSERT INTO bot_users (bot_id, user_id) VALUES ($1, $2)", bot_token, user_id)
            await conn.execute("UPDATE bots SET users_count = users_count + 1 WHERE bot_token = $1", bot_token)
        except asyncpg.UniqueViolationError:
            pass

async def get_bot_users_count(bot_token: str) -> int:
    bot_info = await get_bot_info(bot_token)
    if not bot_info:
        return 0
    pool = get_shard(bot_info['owner_id'])
    async with pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM bot_users WHERE bot_id = $1", bot_token)
        return count or 0

# ------------------- BALANS -------------------
async def get_balance(user_id: int) -> int:
    pool = get_shard(user_id)
    async with pool.acquire() as conn:
        bal = await conn.fetchval("SELECT balance FROM users WHERE telegram_id = $1", user_id)
        return bal or 0

async def add_balance(user_id: int, amount: int, admin_id: int = None, note: str = ""):
    pool = get_shard(user_id)
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("UPDATE users SET balance = balance + $1 WHERE telegram_id = $2", amount, user_id)
            await conn.execute(
                "INSERT INTO transactions (user_id, amount, admin_id, note) VALUES ($1, $2, $3, $4)",
                user_id, amount, admin_id, note
            )

async def deduct_balance(user_id: int, amount: int) -> bool:
    pool = get_shard(user_id)
    async with pool.acquire() as conn:
        async with conn.transaction():
            current = await conn.fetchval("SELECT balance FROM users WHERE telegram_id = $1 FOR UPDATE", user_id)
            if current >= amount:
                await conn.execute("UPDATE users SET balance = balance - $1 WHERE telegram_id = $2", amount, user_id)
                return True
            return False

# ------------------- GLOBAL KONFIG -------------------
async def get_payment_link() -> Optional[str]:
    async def fetch(pool):
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT value FROM config WHERE key = 'payment_link'")
            return row['value'] if row else None
    val = await fetch(db.pool1)
    if val is None:
        val = await fetch(db.pool2)
    return val

async def set_payment_link(link: str):
    async def update(pool):
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO config (key, value) VALUES ('payment_link', $1)
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
            """, link)
    await asyncio.gather(update(db.pool1), update(db.pool2), return_exceptions=True)

# ------------------- STATISTIKA -------------------
async def get_total_users() -> int:
    async def count(pool):
        async with pool.acquire() as conn:
            return await conn.fetchval("SELECT COUNT(*) FROM users")
    c1, c2 = await asyncio.gather(count(db.pool1), count(db.pool2))
    return (c1 or 0) + (c2 or 0)

async def get_total_bots() -> int:
    async def count(pool):
        async with pool.acquire() as conn:
            return await conn.fetchval("SELECT COUNT(*) FROM bots")
    c1, c2 = await asyncio.gather(count(db.pool1), count(db.pool2))
    return (c1 or 0) + (c2 or 0)

async def get_total_balance() -> int:
    async def sum_bal(pool):
        async with pool.acquire() as conn:
            return await conn.fetchval("SELECT COALESCE(SUM(balance), 0) FROM users")
    s1, s2 = await asyncio.gather(sum_bal(db.pool1), sum_bal(db.pool2))
    return (s1 or 0) + (s2 or 0)

# ------------------- BOT INSTANCE CACHE -------------------
# token -> Bot object
bot_instances: Dict[str, Bot] = {}

def get_bot_instance(token: str) -> Bot:
    if token not in bot_instances:
        bot_instances[token] = Bot(token=token)
    return bot_instances[token]

# ------------------- DISPATCHER (umumiy) -------------------
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# ------------------- MAJBURIY OBUNA TEKSHIRISH (umumiy) -------------------
async def check_subscription(user_id: int, bot_token: str) -> Tuple[bool, Optional[str]]:
    """Foydalanuvchi botning majburiy kanaliga a'zomi? (True/False, kanal nomi)"""
    bot_info = await get_bot_info(bot_token)
    if not bot_info or not bot_info.get('required_channel'):
        return True, None
    channel = bot_info['required_channel']
    try:
        bot = get_bot_instance(bot_token)  # shu botning o'zidan foydalanamiz
        chat = await bot.get_chat(channel)
        member = await bot.get_chat_member(chat_id=chat.id, user_id=user_id)
        return member.status in ["member", "administrator", "creator"], channel
    except Exception as e:
        logging.error(f"Obuna tekshirishda xatolik: {e}")
        return False, channel

# ------------------- PLATFORMA BOTI UCHUN KLAVIATURALAR -------------------
def platform_main_menu(user_id: int = None):
    buttons = [
        [KeyboardButton(text="🤖 Bot ulash")],
        [KeyboardButton(text="⭐ Stars sotib olish")],
        [KeyboardButton(text="📊 Mening botlarim")],
        [KeyboardButton(text="👤 Mening balansim")],
        [KeyboardButton(text="ℹ️ Yordam")]
    ]
    if user_id and user_id in ADMIN_IDS:
        buttons.append([KeyboardButton(text="📢 Reklama yuborish")])
        buttons.append([KeyboardButton(text="⚙️ Admin sozlamalari")])
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)

def cancel_menu():
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="❌ Bekor qilish")]], resize_keyboard=True)

def inline_unlink_button(bot_token: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.add(InlineKeyboardButton(text="❌ Botni uzish", callback_data=f"unlink:{bot_token}"))
    builder.add(InlineKeyboardButton(text="⚙️ Sozlamalar", callback_data=f"settings:{bot_token}"))
    builder.adjust(1)
    return builder.as_markup()

def bot_settings_menu(bot_token: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.add(InlineKeyboardButton(text="🔗 Majburiy kanalni sozlash", callback_data=f"set_channel:{bot_token}"))
    builder.add(InlineKeyboardButton(text="📊 Statistika", callback_data=f"bot_stats:{bot_token}"))
    builder.add(InlineKeyboardButton(text="◀️ Orqaga", callback_data="back_to_my_bots"))
    builder.adjust(1)
    return builder.as_markup()

# ------------------- TOKEN MASKI -------------------
def mask_token(token: str) -> str:
    """Tokenning faqat oxirgi 6 belgisini ko'rsatadi, qolganini # bilan almashtiradi."""
    if len(token) <= 6:
        return token
    return "#" * (len(token) - 6) + token[-6:]

# ------------------- UMUMIY HANDLERLAR (barcha botlar uchun) -------------------
@dp.message(Command("start"))
async def cmd_start(message: Message, bot: Bot):
    token = bot.token
    user_id = message.from_user.id
    await update_user(user_id, message.from_user.username or "", message.from_user.first_name or "", user_id in ADMIN_IDS)

    # Majburiy obuna tekshiruvi
    ok, channel = await check_subscription(user_id, token)
    if not ok:
        await message.answer(
            f"❌ Botdan foydalanish uchun avval {channel} kanaliga a'zo bo'ling!\n"
            f"Obuna bo'lgach, /start ni qayta bosing.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔗 Kanalga o'tish", url=f"https://t.me/{channel.lstrip('@')}")]
            ])
        )
        return

    # Agar bu platforma boti bo'lsa, to'liq menyu chiqaramiz
    if token == MAIN_BOT_TOKEN:
        await message.answer(
            "🤖 *BOT PLATFORM*\n\nQuyidagilardan birini tanlang:",
            reply_markup=platform_main_menu(user_id),
            parse_mode="Markdown"
        )
    else:
        # User boti uchun sodda xabar
        bot_info = await get_bot_info(token)
        owner_name = bot_info.get('owner_name', 'Nomaʼlum') if bot_info else 'Nomaʼlum'
        await message.answer(
            f"👋 Xush kelibsiz! Bu @{bot_info['bot_username']} boti.\n"
            f"👤 Egasi: @{owner_name}\n"
            f"💬 Yordam uchun /help"
        )

@dp.message(Command("help"))
async def cmd_help(message: Message, bot: Bot):
    token = bot.token
    user_id = message.from_user.id
    ok, channel = await check_subscription(user_id, token)
    if not ok:
        await message.answer(f"❌ Avval {channel} kanaliga a'zo bo'ling!")
        return

    if token == MAIN_BOT_TOKEN:
        text = ("📚 *Yordam*\n\n"
                "• Bot ulash – o‘z botingizni platformaga ulang.\n"
                "• Stars sotib olish – balansdan yechib stars olish.\n"
                "• Mening botlarim – ulangan botlaringizni boshqaring.\n"
                "• Mening balansim – hisobingizdagi pul miqdori.\n"
                "• Admin paneli – /admin orqali kirish.")
    else:
        bot_info = await get_bot_info(token)
        text = (f"🤖 @{bot_info['bot_username']} uchun yordam:\n"
                f"• /start – botni ishga tushirish\n"
                f"• /stats – statistika (faqat egasi uchun)")
    await message.answer(text, parse_mode="Markdown")

@dp.message(Command("stats"))
async def cmd_stats(message: Message, bot: Bot):
    token = bot.token
    user_id = message.from_user.id
    ok, channel = await check_subscription(user_id, token)
    if not ok:
        await message.answer(f"❌ Avval {channel} kanaliga a'zo bo'ling!")
        return

    bot_info = await get_bot_info(token)
    if not bot_info:
        await message.answer("Bot topilmadi.")
        return
    if bot_info['owner_id'] != user_id and user_id not in ADMIN_IDS:
        await message.answer("🚫 Bu statistika faqat bot egasi uchun.")
        return
    users_count = await get_bot_users_count(token)
    text = (f"📊 *Bot statistikasi*\n\n"
            f"🤖 @{bot_info['bot_username']}\n"
            f"👥 Foydalanuvchilar: {users_count}\n"
            f"📅 Ulangan: {bot_info['connected_at'].strftime('%Y-%m-%d')}")
    await message.answer(text, parse_mode="Markdown")

# ------------------- PLATFORMA BOTIGA XOS HANDLERLAR -------------------
# Ushbu handlerlar faqat MAIN_BOT_TOKEN bo'lganda ishlaydi
@dp.message(F.text == "🤖 Bot ulash")
async def platform_bot_ulash(message: Message, bot: Bot, state: FSMContext):
    if bot.token != MAIN_BOT_TOKEN:
        return
    # ... (avvalgi bot_ulash_start kodi)
    await state.set_state(BotUlashState.token_kutish)
    await message.answer(
        "🤖 *Bot token yuboring*\n\nMasalan: `1234567890:ABCdefGHIjklMNOpqrsTUVwxyz`",
        reply_markup=cancel_menu(),
        parse_mode="Markdown"
    )

# Holatlar
class BotUlashState(StatesGroup):
    token_kutish = State()

class SetChannelState(StatesGroup):
    channel_kutish = State()

@dp.message(BotUlashState.token_kutish, F.text == "❌ Bekor qilish")
async def cancel_bot_ulash(message: Message, state: FSMContext, bot: Bot):
    if bot.token != MAIN_BOT_TOKEN:
        return
    await state.clear()
    await message.answer("❌ Bekor qilindi. Asosiy menu:", reply_markup=platform_main_menu(message.from_user.id))

@dp.message(BotUlashState.token_kutish)
async def receive_token(message: Message, state: FSMContext, bot: Bot):
    if bot.token != MAIN_BOT_TOKEN:
        return
    token = message.text.strip()
    # Token validatsiyasi
    try:
        temp_bot = Bot(token=token)
        me = await temp_bot.get_me()
        await temp_bot.session.close()
        bot_username = me.username
    except Exception:
        await message.answer("❌ *Noto‘g‘ri token*. Qaytadan urinib ko‘ring.", parse_mode="Markdown")
        return
    user_id = message.from_user.id
    existing = await get_bot_info(token)
    if existing:
        await message.answer("❌ Bu bot allaqachon platformaga ulangan.")
        return
    await add_bot(token, bot_username, user_id)

    # Webhook'ni o'rnatish (bu bot uchun)
    webhook_url = f"{RENDER_EXTERNAL_URL}/webhook/{token}"
    await temp_bot.set_webhook(webhook_url)

    await state.clear()
    await message.answer(
        f"✅ *Bot muvaffaqiyatli ulandi!*\n\n🤖 @{bot_username}\n👤 Admin: @{message.from_user.username or 'no username'}",
        reply_markup=platform_main_menu(user_id),
        parse_mode="Markdown"
    )
    if CHANNEL_ID:
        try:
            report = (f"🤖 *BOT ULANDI*\n\n👤 Admin: @{message.from_user.username or 'no username'}\n"
                      f"🤖 Bot: @{bot_username}\n📅 {datetime.now().strftime('%Y-%m-%d %H:%M')}")
            await bot.send_message(CHANNEL_ID, report, parse_mode="Markdown")
        except Exception as e:
            logging.error(f"Kanalga xabar yuborishda xatolik: {e}")

@dp.message(F.text == "📊 Mening botlarim")
async def platform_my_bots(message: Message, bot: Bot):
    if bot.token != MAIN_BOT_TOKEN:
        return
    user_id = message.from_user.id
    user_bots = await get_bots_by_owner(user_id)
    if not user_bots:
        await message.answer("Sizda hali hech qanday bot yo‘q.", reply_markup=platform_main_menu(user_id))
        return
    for bot_info in user_bots:
        users_count = await get_bot_users_count(bot_info['bot_token'])
        masked = mask_token(bot_info['bot_token'])
        text = (f"🤖 @{bot_info['bot_username']}\n"
                f"🔑 Token: `{masked}`\n"
                f"📅 Ulangan: {bot_info['connected_at'].strftime('%Y-%m-%d %H:%M')}\n"
                f"📊 Users: {users_count}")
        await message.answer(text, reply_markup=inline_unlink_button(bot_info['bot_token']))

@dp.callback_query(F.data.startswith("unlink:"))
async def unlink_bot(callback: CallbackQuery, bot: Bot):
    if bot.token != MAIN_BOT_TOKEN:
        return
    token = callback.data.split(":", 1)[1]
    user_id = callback.from_user.id
    removed = await remove_bot(token, user_id)
    if not removed:
        await callback.answer("❌ Bot topilmadi yoki sizga tegishli emas.", show_alert=True)
        return
    # Webhook'ni o'chirish
    try:
        temp_bot = Bot(token=token)
        await temp_bot.delete_webhook()
        await temp_bot.session.close()
    except:
        pass
    await callback.message.edit_text(
        f"❌ *Bot uzildi* (token: `{mask_token(token)}`)\n👤 @{callback.from_user.username or 'no username'}",
        parse_mode="Markdown"
    )
    await callback.answer("✅ Bot muvaffaqiyatli uzildi!")
    if CHANNEL_ID:
        try:
            await bot.send_message(CHANNEL_ID,
                f"❌ *BOT UZILDI*\n\n👤 Admin: @{callback.from_user.username or 'no username'}\n🤖 Bot tokeni: `{mask_token(token)}`",
                parse_mode="Markdown")
        except Exception as e:
            logging.error(f"Kanalga xabar yuborishda xatolik: {e}")

@dp.callback_query(F.data.startswith("settings:"))
async def bot_settings(callback: CallbackQuery, bot: Bot):
    if bot.token != MAIN_BOT_TOKEN:
        return
    token = callback.data.split(":", 1)[1]
    bot_info = await get_bot_info(token)
    if not bot_info or bot_info['owner_id'] != callback.from_user.id:
        await callback.answer("❌ Siz bu botning egasi emassiz.", show_alert=True)
        return
    text = (f"⚙️ *@{bot_info['bot_username']} sozlamalari*\n\n"
            f"🔗 Majburiy kanal: {bot_info.get('required_channel') or '❌ O‘rnatilmagan'}")
    await callback.message.edit_text(text, reply_markup=bot_settings_menu(token), parse_mode="Markdown")
    await callback.answer()

@dp.callback_query(F.data.startswith("set_channel:"))
async def set_channel_start(callback: CallbackQuery, state: FSMContext, bot: Bot):
    if bot.token != MAIN_BOT_TOKEN:
        return
    token = callback.data.split(":", 1)[1]
    bot_info = await get_bot_info(token)
    if not bot_info or bot_info['owner_id'] != callback.from_user.id:
        await callback.answer("❌ Siz bu botning egasi emassiz.", show_alert=True)
        return
    await state.update_data(bot_token=token)
    await state.set_state(SetChannelState.channel_kutish)
    await callback.message.edit_text(
        "🔗 Yangi majburiy kanal username'ini yuboring (masalan: @kanal_nomi).\n"
        "Bekor qilish uchun /cancel yoki ❌ Bekor qilish tugmasini bosing.",
        reply_markup=cancel_menu()
    )
    await callback.answer()

@dp.message(SetChannelState.channel_kutish, F.text == "❌ Bekor qilish")
async def set_channel_cancel(message: Message, state: FSMContext, bot: Bot):
    if bot.token != MAIN_BOT_TOKEN:
        return
    await state.clear()
    await message.answer("❌ Bekor qilindi.", reply_markup=platform_main_menu(message.from_user.id))

@dp.message(SetChannelState.channel_kutish)
async def set_channel_receive(message: Message, state: FSMContext, bot: Bot):
    if bot.token != MAIN_BOT_TOKEN:
        return
    channel = message.text.strip()
    if not channel.startswith('@'):
        await message.answer("❌ Kanal @ bilan boshlanishi kerak. Qaytadan urinib ko‘ring.")
        return
    data = await state.get_data()
    token = data['bot_token']
    user_id = message.from_user.id
    success = await update_bot_channel(token, channel, user_id)
    if success:
        await message.answer(f"✅ Majburiy kanal {channel} qilib o‘rnatildi.", reply_markup=platform_main_menu(user_id))
    else:
        await message.answer("❌ Xatolik yuz berdi.", reply_markup=platform_main_menu(user_id))
    await state.clear()

# ------------------- WEBHOOK ROUTER -------------------
async def handle_webhook(request: web.Request) -> web.Response:
    token = request.match_info.get('token')
    if not token:
        return web.Response(status=404, text="Token not found")

    # Bot instance'ni olish
    bot = get_bot_instance(token)
    # Update ni o'qish
    try:
        update_data = await request.json()
        update = Update.model_validate(update_data, context={"bot": bot})
    except Exception as e:
        logging.error(f"Update parse error: {e}")
        return web.Response(status=400, text="Invalid update")

    # Dispatcher orqali qayta ishlash
    try:
        await dp.feed_update(bot, update)
    except Exception as e:
        logging.exception(f"Error processing update for bot {token}: {e}")
        return web.Response(status=500, text="Error processing update")
    return web.Response(status=200, text="OK")

# ------------------- WEBHOCH SOZLASH -------------------
async def on_startup(app: web.Application):
    await init_db()
    # Asosiy bot uchun webhook o'rnatish
    main_bot = get_bot_instance(MAIN_BOT_TOKEN)
    main_webhook_url = f"{RENDER_EXTERNAL_URL}/webhook/{MAIN_BOT_TOKEN}"
    await main_bot.set_webhook(main_webhook_url)
    print(f"✅ Asosiy bot webhook o‘rnatildi: {main_webhook_url}")

async def on_shutdown(app: web.Application):
    await close_db()
    # Barcha botlarning webhook'larini o'chirish (ixtiyoriy)
    for token, bot in bot_instances.items():
        try:
            await bot.delete_webhook()
        except:
            pass
    print("🔴 Bot to‘xtatildi")

def main():
    app = web.Application()
    app.router.add_post('/webhook/{token}', handle_webhook)
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)
    web.run_app(app, host=WEBAPP_HOST, port=WEBAPP_PORT)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()

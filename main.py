import os
import logging
import asyncio
import uuid
from datetime import datetime
from typing import Optional, List, Dict, Tuple

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
    Message
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from aiohttp import web
from dotenv import load_dotenv

load_dotenv()

# ------------------- KONFIGURATSIYA -------------------
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN topilmadi!")

# Ikkala database URL
DATABASE1_URL = os.getenv("DATABASE1_URL")
DATABASE2_URL = os.getenv("DATABASE2_URL")
if not DATABASE1_URL or not DATABASE2_URL:
    raise ValueError("DATABASE1_URL va DATABASE2_URL berilishi kerak!")

ADMIN_IDS_STR = os.getenv("ADMIN_IDS")
ADMIN_IDS = [int(x) for x in ADMIN_IDS_STR.split(",")] if ADMIN_IDS_STR else []

REQUIRED_CHANNEL = "@MonkeyLabBotBuilder"  # Majburiy kanal
CHANNEL_ID_STR = os.getenv("CHANNEL_ID")
CHANNEL_ID = int(CHANNEL_ID_STR) if CHANNEL_ID_STR else None

# Webhook
RENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL")
if not RENDER_EXTERNAL_URL:
    raise ValueError("RENDER_EXTERNAL_URL topilmadi!")
WEBHOOK_PATH = "/webhook"
WEBHOOK_URL = f"{RENDER_EXTERNAL_URL}{WEBHOOK_PATH}"
WEBAPP_HOST = "0.0.0.0"
WEBAPP_PORT = int(os.getenv("PORT", 8080))

# ------------------- DATABASE POOLLAR VA SHARDING -------------------
class Database:
    pool1: asyncpg.Pool = None
    pool2: asyncpg.Pool = None

db = Database()

def get_shard(user_id: int) -> asyncpg.Pool:
    """user_id ga qarab qaysi database pool'ini ishlatishni aniqlaydi."""
    return db.pool1 if user_id % 2 == 0 else db.pool2

async def create_tables(conn: asyncpg.Connection):
    """Jadvallarni yaratish (har bir database'da alohida chaqiriladi)."""
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
            type TEXT DEFAULT 'stars_bot',
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
    # Jadvallarni ikkala database'da yaratish
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
    print("🔌 PostgreSQL uzildi")

# ------------------- MA'LUMOTLAR FUNKSIYALARI -------------------
# Har bir foydalanuvchi faqat o'z shardida yashaydi
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

async def add_bot(bot_token: str, bot_username: str, owner_id: int):
    pool = get_shard(owner_id)
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO bots (bot_token, bot_username, owner_id, connected_at)
            VALUES ($1, $2, $3, CURRENT_TIMESTAMP)
        """, bot_token, bot_username, owner_id)
        await conn.execute("UPDATE users SET bots_count = bots_count + 1 WHERE telegram_id = $1", owner_id)

async def remove_bot(bot_token: str, owner_id: int) -> bool:
    pool = get_shard(owner_id)
    async with pool.acquire() as conn:
        result = await conn.execute("DELETE FROM bots WHERE bot_token = $1 AND owner_id = $2", bot_token, owner_id)
        if result == "DELETE 1":
            await conn.execute("UPDATE users SET bots_count = bots_count - 1 WHERE telegram_id = $1", owner_id)
            await conn.execute("DELETE FROM bot_users WHERE bot_id = $1", bot_token)
            return True
        return False

async def get_bot_by_token(bot_token: str) -> Optional[Dict]:
    # Bot token qaysi user'ga tegishli? Buni bilish uchun avval user'ni topish kerak.
    # Ammo biz token bo'yicha qidirishni ikkala database'da ham amalga oshirishimiz kerak.
    async def search(pool):
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM bots WHERE bot_token = $1", bot_token)
            return dict(row) if row else None
    results = await asyncio.gather(
        search(db.pool1),
        search(db.pool2),
        return_exceptions=True
    )
    for res in results:
        if isinstance(res, dict) and res:
            return res
    return None

async def get_bots_by_owner(owner_id: int) -> List[Dict]:
    pool = get_shard(owner_id)
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM bots WHERE owner_id = $1 ORDER BY connected_at DESC", owner_id)
        return [dict(row) for row in rows]

async def get_bot_users_count(bot_token: str) -> int:
    # Bot qaysi shardda ekanligini bilish uchun oldin bot ma'lumotini olish kerak
    bot = await get_bot_by_token(bot_token)
    if not bot:
        return 0
    pool = get_shard(bot['owner_id'])
    async with pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM bot_users WHERE bot_id = $1", bot_token)
        return count or 0

async def add_bot_user(bot_token: str, user_id: int):
    # Bot qaysi shardda bo'lsa, o'shanga yozamiz
    bot = await get_bot_by_token(bot_token)
    if not bot:
        return
    pool = get_shard(bot['owner_id'])
    async with pool.acquire() as conn:
        try:
            await conn.execute("INSERT INTO bot_users (bot_id, user_id) VALUES ($1, $2)", bot_token, user_id)
            await conn.execute("UPDATE bots SET users_count = users_count + 1 WHERE bot_token = $1", bot_token)
        except asyncpg.UniqueViolationError:
            pass

# ------------------- BALANS FUNKSIYALARI -------------------
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

# ------------------- GLOBAL KONFIGURATSIYA (ikkala database'da bir xil) -------------------
async def get_payment_link() -> Optional[str]:
    async def fetch(pool):
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT value FROM config WHERE key = 'payment_link'")
            return row['value'] if row else None
    # Ikkala database'dan biridan olish mumkin (bir xil bo'lishi kerak)
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
    # Ikkala database'ga parallel yozish
    await asyncio.gather(update(db.pool1), update(db.pool2), return_exceptions=True)

# ------------------- ADMIN PANEL UCHUN STATISTIKA -------------------
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

# ------------------- MAJBURIY OBUNA TEKSHIRISH -------------------
async def check_subscription(user_id: int) -> bool:
    try:
        chat = await bot.get_chat(REQUIRED_CHANNEL)
        member = await bot.get_chat_member(chat_id=chat.id, user_id=user_id)
        return member.status in ["member", "administrator", "creator"]
    except Exception as e:
        logging.error(f"Obuna tekshirishda xatolik: {e}")
        return False

# ------------------- HOLATLAR -------------------
class BotUlashState(StatesGroup):
    token_kutish = State()

class ReklamaYuborishState(StatesGroup):
    xabar_kutish = State()
    tasdiqlash = State()

class BalanceEditState(StatesGroup):
    user_id_kutish = State()
    miqdor_kutish = State()

class PaymentLinkState(StatesGroup):
    link_kutish = State()

# ------------------- KLAVIATURALAR -------------------
def main_menu(user_id: int = None):
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

def admin_settings_menu():
    builder = InlineKeyboardBuilder()
    builder.add(InlineKeyboardButton(text="💰 Balans qo'shish", callback_data="admin_add_balance"))
    builder.add(InlineKeyboardButton(text="🔗 To'lov havolasini sozlash", callback_data="admin_set_payment"))
    builder.add(InlineKeyboardButton(text="📊 Statistika", callback_data="admin_stats"))
    builder.adjust(1)
    return builder.as_markup()

def inline_unlink_button(bot_token: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.add(InlineKeyboardButton(text="❌ Botni uzish", callback_data=f"unlink:{bot_token}"))
    return builder.as_markup()

# ------------------- BOT OB'EKTI -------------------
bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# ------------------- YORDAMCHI FUNKSIYA -------------------
async def token_is_valid(token: str) -> Optional[str]:
    try:
        temp_bot = Bot(token=token)
        me = await temp_bot.get_me()
        await temp_bot.session.close()
        return me.username
    except Exception:
        return None

# ------------------- HANDLERLAR -------------------
@dp.message(Command("start"))
async def cmd_start(message: Message):
    user = message.from_user
    await update_user(user.id, user.username or "", user.first_name or "", user.id in ADMIN_IDS)

    if not await check_subscription(user.id):
        await message.answer(
            f"❌ Botdan foydalanish uchun avval {REQUIRED_CHANNEL} kanaliga a'zo bo'ling!\n"
            f"Obuna bo'lgach, /start ni qayta bosing.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔗 Kanalga o'tish", url=f"https://t.me/{REQUIRED_CHANNEL.lstrip('@')}")]
            ])
        )
        return

    await message.answer(
        "🤖 *BOT PLATFORM*\n\nQuyidagilardan birini tanlang:",
        reply_markup=main_menu(user.id),
        parse_mode="Markdown"
    )

@dp.message(Command("admin"))
async def cmd_admin(message: Message):
    if not await check_subscription(message.from_user.id):
        await message.answer(f"❌ Avval {REQUIRED_CHANNEL} kanaliga a'zo bo'ling!")
        return
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("🚫 Siz admin emassiz.")
        return

    total_users = await get_total_users()
    total_bots = await get_total_bots()
    total_balance = await get_total_balance()
    text = (f"👑 *Admin panel*\n\n"
            f"👥 Foydalanuvchilar: {total_users}\n"
            f"🤖 Ulangan botlar: {total_bots}\n"
            f"💰 Umumiy balans: {total_balance} so'm")
    await message.answer(text, parse_mode="Markdown", reply_markup=admin_settings_menu())

# ------------------- ADMIN SOZLAMALARI -------------------
@dp.callback_query(F.data == "admin_stats")
async def admin_stats(callback: CallbackQuery):
    total_users = await get_total_users()
    total_bots = await get_total_bots()
    total_balance = await get_total_balance()
    text = (f"📊 *Statistika*\n\n"
            f"👥 Foydalanuvchilar: {total_users}\n"
            f"🤖 Botlar: {total_bots}\n"
            f"💰 Umumiy balans: {total_balance} so'm")
    await callback.message.edit_text(text, parse_mode="Markdown", reply_markup=admin_settings_menu())
    await callback.answer()

@dp.callback_query(F.data == "admin_add_balance")
async def admin_add_balance_start(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text("Foydalanuvchi Telegram ID sini yuboring:")
    await state.set_state(BalanceEditState.user_id_kutish)
    await callback.answer()

@dp.message(BalanceEditState.user_id_kutish, F.text == "❌ Bekor qilish")
async def balance_cancel(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("❌ Bekor qilindi.", reply_markup=main_menu(message.from_user.id))

@dp.message(BalanceEditState.user_id_kutish)
async def balance_user_id(message: Message, state: FSMContext):
    try:
        target_id = int(message.text.strip())
        await state.update_data(target_id=target_id)
        await state.set_state(BalanceEditState.miqdor_kutish)
        await message.answer("Qancha so'm qo'shish kerak?", reply_markup=cancel_menu())
    except ValueError:
        await message.answer("❌ Noto'g'ri ID. Qaytadan urinib ko'ring yoki bekor qiling.")

@dp.message(BalanceEditState.miqdor_kutish)
async def balance_amount(message: Message, state: FSMContext):
    try:
        amount = int(message.text.strip())
        data = await state.get_data()
        target_id = data['target_id']
        await add_balance(target_id, amount, message.from_user.id, "Admin tomonidan qo'shildi")
        await state.clear()
        await message.answer(f"✅ {amount} so'm foydalanuvchi {target_id} ga qo'shildi.", reply_markup=main_menu(message.from_user.id))
    except ValueError:
        await message.answer("❌ Noto'g'ri miqdor. Qaytadan urinib ko'ring.")

@dp.callback_query(F.data == "admin_set_payment")
async def admin_set_payment_start(callback: CallbackQuery, state: FSMContext):
    current = await get_payment_link()
    text = f"🔗 Hozirgi to'lov havolasi: {current or 'O‘rnatilmagan'}\n\nYangi havolani yuboring (yoki /cancel):"
    await callback.message.edit_text(text)
    await state.set_state(PaymentLinkState.link_kutish)
    await callback.answer()

@dp.message(PaymentLinkState.link_kutish, F.text == "❌ Bekor qilish")
async def payment_cancel(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("❌ Bekor qilindi.", reply_markup=main_menu(message.from_user.id))

@dp.message(PaymentLinkState.link_kutish)
async def payment_link_set(message: Message, state: FSMContext):
    link = message.text.strip()
    await set_payment_link(link)
    await state.clear()
    await message.answer("✅ To'lov havolasi saqlandi.", reply_markup=main_menu(message.from_user.id))

# ------------------- REKLAMA YUBORISH -------------------
@dp.message(F.text == "📢 Reklama yuborish")
async def reklama_start(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    await state.set_state(ReklamaYuborishState.xabar_kutish)
    await message.answer("📝 Reklama matnini yuboring:", reply_markup=cancel_menu())

@dp.message(ReklamaYuborishState.xabar_kutish, F.text == "❌ Bekor qilish")
async def reklama_cancel(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("❌ Bekor qilindi.", reply_markup=main_menu(message.from_user.id))

@dp.message(ReklamaYuborishState.xabar_kutish)
async def reklama_receive(message: Message, state: FSMContext):
    await state.update_data(text=message.text, entities=message.entities)
    await state.set_state(ReklamaYuborishState.tasdiqlash)
    await message.answer(
        "📨 Reklama tayyor. Yuboramizmi?",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Ha, hammaga yubor", callback_data="send_all")],
            [InlineKeyboardButton(text="🤖 Faqat bot egalariga", callback_data="send_owners")],
            [InlineKeyboardButton(text="❌ Bekor qilish", callback_data="cancel_send")]
        ])
    )

@dp.callback_query(ReklamaYuborishState.tasdiqlash)
async def reklama_send(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    text = data['text']
    entities = data.get('entities')

    if callback.data == "cancel_send":
        await state.clear()
        await callback.message.edit_text("❌ Bekor qilindi.")
        await callback.answer()
        return

    # Foydalanuvchilarni ikkala database'dan olish
    async def get_all_users(pool):
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT telegram_id FROM users")
            return [r['telegram_id'] for r in rows]

    async def get_owners(pool):
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT DISTINCT owner_id FROM bots")
            return [r['owner_id'] for r in rows]

    if callback.data == "send_owners":
        users1, users2 = await asyncio.gather(get_owners(db.pool1), get_owners(db.pool2))
    else:
        users1, users2 = await asyncio.gather(get_all_users(db.pool1), get_all_users(db.pool2))

    user_ids = list(set(users1 + users2))  # takrorlanmasligi uchun

    await callback.message.edit_text(f"⏳ Yuborilmoqda... {len(user_ids)} ta foydalanuvchiga")

    success = 0
    for uid in user_ids:
        try:
            await bot.send_message(uid, text, entities=entities)
            success += 1
            await asyncio.sleep(0.05)  # rate limit
        except Exception as e:
            logging.error(f"Reklama yuborishda xatolik {uid}: {e}")

    await callback.message.answer(f"✅ Reklama yuborildi. Muvaffaqiyatli: {success}/{len(user_ids)}")
    await state.clear()
    await callback.answer()

# ------------------- BALANS KO'RISH -------------------
@dp.message(F.text == "👤 Mening balansim")
async def my_balance(message: Message):
    if not await check_subscription(message.from_user.id):
        await message.answer(f"❌ Avval {REQUIRED_CHANNEL} kanaliga a'zo bo'ling!")
        return
    bal = await get_balance(message.from_user.id)
    payment_link = await get_payment_link()
    text = f"💰 Sizning balansingiz: *{bal} so'm*\n\n"
    if payment_link:
        text += f"🔗 Toʻlov qilish: {payment_link}\n\n"
    text += "Balans toʻlgandan soʻng, admin uni qoʻlda qoʻshadi."
    await message.answer(text, parse_mode="Markdown")

# ------------------- BOT ULASH -------------------
@dp.message(F.text == "🤖 Bot ulash")
async def bot_ulash_start(message: Message, state: FSMContext):
    if not await check_subscription(message.from_user.id):
        await message.answer(f"❌ Avval {REQUIRED_CHANNEL} kanaliga a'zo bo'ling!")
        return
    await state.set_state(BotUlashState.token_kutish)
    await message.answer(
        "🤖 *Bot token yuboring*\n\nMasalan: `1234567890:ABCdefGHIjklMNOpqrsTUVwxyz`",
        reply_markup=cancel_menu(),
        parse_mode="Markdown"
    )

@dp.message(BotUlashState.token_kutish, F.text == "❌ Bekor qilish")
async def cancel_bot_ulash(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("❌ Bekor qilindi. Asosiy menu:", reply_markup=main_menu(message.from_user.id))

@dp.message(BotUlashState.token_kutish)
async def receive_token(message: Message, state: FSMContext):
    token = message.text.strip()
    bot_username = await token_is_valid(token)
    if not bot_username:
        await message.answer("❌ *Noto‘g‘ri token*. Qaytadan urinib ko‘ring.", parse_mode="Markdown")
        return
    user_id = message.from_user.id
    existing = await get_bot_by_token(token)
    if existing:
        await message.answer("❌ Bu bot allaqachon platformaga ulangan.")
        return
    await add_bot(token, bot_username, user_id)
    await state.clear()
    await message.answer(
        f"✅ *Bot muvaffaqiyatli ulandi!*\n\n🤖 @{bot_username}\n👤 Admin: @{message.from_user.username or 'no username'}",
        reply_markup=main_menu(user_id),
        parse_mode="Markdown"
    )
    if CHANNEL_ID:
        try:
            report = (f"🤖 *BOT ULANDI*\n\n👤 Admin: @{message.from_user.username or 'no username'}\n"
                      f"🤖 Bot: @{bot_username}\n📅 {datetime.now().strftime('%Y-%m-%d %H:%M')}")
            await bot.send_message(CHANNEL_ID, report, parse_mode="Markdown")
        except Exception as e:
            logging.error(f"Kanalga xabar yuborishda xatolik: {e}")

# ------------------- STARS SOTIB OLISH -------------------
@dp.message(F.text == "⭐ Stars sotib olish")
async def stars_sotib_olish(message: Message):
    if not await check_subscription(message.from_user.id):
        await message.answer(f"❌ Avval {REQUIRED_CHANNEL} kanaliga a'zo bo'ling!")
        return
    price_per_star = 100
    text = (f"⭐ *Stars paketlari*\n\n"
            f"• 10 Stars – {price_per_star*10} so'm\n"
            f"• 50 Stars – {price_per_star*50} so'm\n"
            f"• 100 Stars – {price_per_star*100} so'm\n\n"
            f"💳 Sotib olish uchun /buy 10 (yoki 50, 100) deb yozing.\n\n"
            f"💰 Balansingiz: {await get_balance(message.from_user.id)} so'm")
    await message.answer(text, parse_mode="Markdown")

@dp.message(Command("buy"))
async def buy_stars(message: Message, command: CommandObject):
    if not await check_subscription(message.from_user.id):
        await message.answer(f"❌ Avval {REQUIRED_CHANNEL} kanaliga a'zo bo'ling!")
        return
    if not command.args:
        await message.answer("❌ Miqdorni kiriting. Masalan: /buy 10")
        return
    try:
        stars = int(command.args)
        price_per_star = 100
        amount = stars * price_per_star
        user_id = message.from_user.id
        if await deduct_balance(user_id, amount):
            # Bu yerda stars ni foydalanuvchiga berish kerak (masalan, alohida jadval)
            await message.answer(f"✅ {stars} stars muvaffaqiyatli sotib olindi! Balans: {await get_balance(user_id)} so'm")
        else:
            await message.answer("❌ Balansingizda yetarli mablag' yo'q.")
    except ValueError:
        await message.answer("❌ Noto'g'ri miqdor.")

# ------------------- MENING BOTLARIM -------------------
@dp.message(F.text == "📊 Mening botlarim")
async def mening_botlarim(message: Message):
    if not await check_subscription(message.from_user.id):
        await message.answer(f"❌ Avval {REQUIRED_CHANNEL} kanaliga a'zo bo'ling!")
        return
    user_id = message.from_user.id
    user_bots = await get_bots_by_owner(user_id)
    if not user_bots:
        await message.answer("Sizda hali hech qanday bot yo‘q.", reply_markup=main_menu(user_id))
        return
    for bot_info in user_bots:
        users_count = await get_bot_users_count(bot_info['bot_token'])
        text = (f"🤖 @{bot_info['bot_username']}\n"
                f"📅 Ulangan: {bot_info['connected_at'].strftime('%Y-%m-%d %H:%M')}\n"
                f"📊 Users: {users_count}")
        await message.answer(text, reply_markup=inline_unlink_button(bot_info['bot_token']))

@dp.callback_query(F.data.startswith("unlink:"))
async def unlink_bot(callback: CallbackQuery):
    if not await check_subscription(callback.from_user.id):
        await callback.answer("❌ Avval kanalga a'zo bo'ling!", show_alert=True)
        return
    token = callback.data.split(":", 1)[1]
    user_id = callback.from_user.id
    removed = await remove_bot(token, user_id)
    if not removed:
        await callback.answer("❌ Bot topilmadi yoki sizga tegishli emas.", show_alert=True)
        return
    await callback.message.edit_text(
        f"❌ *Bot uzildi* (token: `{token}`)\n👤 @{callback.from_user.username or 'no username'}",
        parse_mode="Markdown"
    )
    await callback.answer("✅ Bot muvaffaqiyatli uzildi!")
    if CHANNEL_ID:
        try:
            await bot.send_message(CHANNEL_ID,
                f"❌ *BOT UZILDI*\n\n👤 Admin: @{callback.from_user.username or 'no username'}\n🤖 Bot tokeni: `{token}`",
                parse_mode="Markdown")
        except Exception as e:
            logging.error(f"Kanalga xabar yuborishda xatolik: {e}")

# ------------------- YORDAM -------------------
@dp.message(F.text == "ℹ️ Yordam")
async def yordam(message: Message):
    if not await check_subscription(message.from_user.id):
        await message.answer(f"❌ Avval {REQUIRED_CHANNEL} kanaliga a'zo bo'ling!")
        return
    text = ("📚 *Yordam*\n\n"
            "• Bot ulash – o‘z botingizni platformaga ulang.\n"
            "• Stars sotib olish – balansdan yechib stars olish.\n"
            "• Mening botlarim – ulangan botlaringizni boshqaring.\n"
            "• Mening balansim – hisobingizdagi pul miqdori.\n"
            "• Admin paneli – /admin orqali kirish.\n\n"
            f"📢 Majburiy kanal: {REQUIRED_CHANNEL}")
    await message.answer(text, parse_mode="Markdown")

# ------------------- BEKOR QILISH -------------------
@dp.message(F.text == "❌ Bekor qilish")
async def any_cancel(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("❌ Bekor qilindi.", reply_markup=main_menu(message.from_user.id))

# Qolgan xabarlar
@dp.message()
async def echo_all(message: Message):
    await message.answer("Iltimos, menyudan birini tanlang.", reply_markup=main_menu(message.from_user.id))

# ------------------- WEBHOOK -------------------
async def on_startup(app: web.Application):
    await init_db()
    await bot.set_webhook(WEBHOOK_URL)
    print(f"✅ Webhook o‘rnatildi: {WEBHOOK_URL}")

async def on_shutdown(app: web.Application):
    await bot.delete_webhook()
    await close_db()
    print("🔴 Bot to‘xtatildi")

def main():
    app = web.Application()
    webhook_requests_handler = SimpleRequestHandler(dispatcher=dp, bot=bot)
    webhook_requests_handler.register(app, path=WEBHOOK_PATH)
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)
    setup_application(app, dp, bot=bot)
    web.run_app(app, host=WEBAPP_HOST, port=WEBAPP_PORT)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
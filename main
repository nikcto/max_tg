#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
max2tg.py — мост MAX Web <-> Telegram (в одном файле)

ЧТО ЭТО
--------
Скрипт держит открытой сессию MAX Web (web.max.ru) через Playwright под
твоим собственным аккаунтом, ловит новые сообщения и пересылает их в
Telegram-бота. Из бота (или через встроенный мини-веб-интерфейс) можно
отвечать — скрипт вводит текст в нужный чат MAX и отправляет его.

Три части работают в одном asyncio-процессе:
  1) Playwright-воркер   — следит за MAX Web, ловит новые сообщения,
                            умеет отправлять исходящие.
  2) Telegram-бот         — пересылает тебе сообщения, принимает ответы
                            (просто ответь reply на пересланное сообщение).
  3) Мини-веб-интерфейс   — aiohttp-сервер: список чатов, история,
                            форма ответа. Открывается кнопкой в боте,
                            доступ закрыт секретным ключом.

ВАЖНО ПРО СЕЛЕКТОРЫ MAX WEB
----------------------------
Разметка web.max.ru не документирована и может отличаться от указанной
здесь. Все CSS-селекторы вынесены в блок CONFIG и в переменные окружения —
их нужно проверить и поправить через DevTools (F12 -> Elements) в твоём
браузере, прежде чем скрипт заработает "из коробки". Ищи атрибуты вида
data-testid, aria-label или устойчивые классы у элементов списка чатов,
сообщений и поля ввода.

Более надёжный путь (рекомендуется) — вместо парсинга DOM слушать сетевые
ответы (page.on("response")) или WebSocket-фреймы, которые MAX Web получает
с сервера, и парсить уже готовый JSON. Открой DevTools -> Network -> WS/Fetch,
напиши себе сообщение и посмотри, какой запрос/фрейм это вызывает. Заготовка
для этого пути тоже есть ниже (см. handle_network_response) — включается
переменной окружения USE_NETWORK_SNIFFING=1.

УСТАНОВКА
---------
    pip install playwright aiogram aiohttp
    playwright install chromium

ПЕРВЫЙ ЗАПУСК (логин в MAX)
----------------------------
    python max2tg.py --login
Откроется видимый (не headless) браузер — залогинься в MAX Web вручную
(по QR или как у тебя принято). Сессия сохранится в файл MAX_STORAGE_STATE,
после этого можно запускать скрипт в обычном (headless/фоновом) режиме.

ОБЫЧНЫЙ ЗАПУСК
--------------
    export TG_BOT_TOKEN=...      # токен от @BotFather
    export TG_ADMIN_ID=...       # твой числовой Telegram user_id
    export WEBAPP_SECRET=...     # любой случайный секрет для мини-интерфейса
    python max2tg.py

ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ (все опциональны, кроме TG_BOT_TOKEN / TG_ADMIN_ID)
-------------------------------------------------------------------------
    TG_BOT_TOKEN         — токен Telegram-бота (обязателен)
    TG_ADMIN_ID          — твой Telegram user_id, только ему бот отвечает (обязателен)
    WEBAPP_SECRET         — секретный ключ для мини-интерфейса (обязателен для веб-морды)
    WEBAPP_PORT           — порт aiohttp-сервера, по умолчанию 8080
    WEBAPP_PUBLIC_URL      — публичный URL, под которым сервер доступен извне
                              (для кнопки WebApp в боте), например https://example.com
    MAX_WEB_URL            — адрес MAX Web, по умолчанию https://web.max.ru
    MAX_STORAGE_STATE       — путь к файлу сессии Playwright, по умолчанию max_storage_state.json
    DB_PATH                 — путь к SQLite-базе, по умолчанию max2tg.db
    POLL_INTERVAL           — период опроса DOM в секундах (если не используется network sniffing)
    USE_NETWORK_SNIFFING     — "1", чтобы ловить сообщения через сетевые ответы вместо DOM
    HEADLESS                 — "0", чтобы видеть окно браузера при обычном запуске (по умолчанию "1")
    SEL_CHAT_LIST_ITEM, SEL_CHAT_TITLE, SEL_MESSAGE_ITEM, SEL_MESSAGE_TEXT,
    SEL_MESSAGE_AUTHOR_OUT, SEL_MESSAGE_INPUT, SEL_SEND_BUTTON
                              — CSS-селекторы элементов MAX Web, подбираются через DevTools

ОГРАНИЧЕНИЯ / ЧЕСТНО
---------------------
- Это учебный скелет, а не готовый продукт: селекторы MAX Web придётся
  подогнать под актуальную вёрстку самостоятельно.
- Скрипт работает только с ТВОИМ собственным аккаунтом MAX (та же сессия,
  что и у тебя в браузере) — это не инструмент для доступа к чужим чатам.
- Храни файл сессии (MAX_STORAGE_STATE) и WEBAPP_SECRET так же аккуратно,
  как пароль: с ними можно писать от твоего имени в MAX.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import secrets
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    Message,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    WebAppInfo,
)
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties

from aiohttp import web
from playwright.async_api import async_playwright, Page, BrowserContext, Response

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("max2tg")

# =============================== CONFIG =====================================

BOT_TOKEN = os.environ.get("TG_BOT_TOKEN", "")
ADMIN_ID = int(os.environ.get("TG_ADMIN_ID", "0") or 0)
WEBAPP_SECRET = os.environ.get("WEBAPP_SECRET", secrets.token_urlsafe(16))
WEBAPP_PORT = int(os.environ.get("WEBAPP_PORT", "8080"))
WEBAPP_PUBLIC_URL = os.environ.get("WEBAPP_PUBLIC_URL", f"http://localhost:{WEBAPP_PORT}")

MAX_WEB_URL = os.environ.get("MAX_WEB_URL", "https://web.max.ru")
STATE_PATH = Path(os.environ.get("MAX_STORAGE_STATE", "max_storage_state.json"))
DB_PATH = Path(os.environ.get("DB_PATH", "max2tg.db"))
POLL_INTERVAL = float(os.environ.get("POLL_INTERVAL", "2.0"))
USE_NETWORK_SNIFFING = os.environ.get("USE_NETWORK_SNIFFING", "0") == "1"
HEADLESS = os.environ.get("HEADLESS", "1") == "1"

# Селекторы MAX Web — ПРОВЕРЬ И ПОПРАВЬ через DevTools перед запуском.
SEL_CHAT_LIST_ITEM = os.environ.get("SEL_CHAT_LIST_ITEM", "[data-testid='chat-list-item']")
SEL_CHAT_TITLE = os.environ.get("SEL_CHAT_TITLE", "[data-testid='chat-title']")
SEL_MESSAGE_ITEM = os.environ.get("SEL_MESSAGE_ITEM", "[data-testid='message']")
SEL_MESSAGE_TEXT = os.environ.get("SEL_MESSAGE_TEXT", "[data-testid='message-text']")
SEL_MESSAGE_AUTHOR_OUT = os.environ.get("SEL_MESSAGE_AUTHOR_OUT", ".message--outgoing")
SEL_MESSAGE_INPUT = os.environ.get("SEL_MESSAGE_INPUT", "[contenteditable='true']")
SEL_SEND_BUTTON = os.environ.get("SEL_SEND_BUTTON", "[data-testid='send-button']")

# ============================== DATABASE =====================================

def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def db_init() -> None:
    conn = db_connect()
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS chats (
            max_chat_id   TEXT PRIMARY KEY,
            title         TEXT,
            last_seen_at  REAL
        );
        CREATE TABLE IF NOT EXISTS messages (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            max_chat_id   TEXT NOT NULL,
            direction     TEXT NOT NULL,     -- 'in' (из MAX) или 'out' (в MAX)
            text          TEXT,
            max_msg_key   TEXT,              -- локальный ключ дедупликации (не ID MAX)
            tg_message_id INTEGER,           -- id сообщения в Telegram, если пересылали
            created_at    REAL NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_messages_chat ON messages(max_chat_id);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_dedup
            ON messages(max_chat_id, max_msg_key) WHERE max_msg_key IS NOT NULL;
        CREATE TABLE IF NOT EXISTS tg_reply_map (
            tg_message_id INTEGER PRIMARY KEY,
            max_chat_id   TEXT NOT NULL
        );
        """
    )
    conn.commit()
    conn.close()


def db_upsert_chat(chat_id: str, title: str) -> None:
    conn = db_connect()
    conn.execute(
        "INSERT INTO chats (max_chat_id, title, last_seen_at) VALUES (?, ?, ?) "
        "ON CONFLICT(max_chat_id) DO UPDATE SET title=excluded.title, last_seen_at=excluded.last_seen_at",
        (chat_id, title, time.time()),
    )
    conn.commit()
    conn.close()


def db_add_message(chat_id: str, direction: str, text: str, dedup_key: Optional[str], tg_message_id: Optional[int] = None) -> Optional[int]:
    conn = db_connect()
    try:
        cur = conn.execute(
            "INSERT INTO messages (max_chat_id, direction, text, max_msg_key, tg_message_id, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (chat_id, direction, text, dedup_key, tg_message_id, time.time()),
        )
        conn.commit()
        return cur.lastrowid
    except sqlite3.IntegrityError:
        # уже видели это сообщение (dedup_key совпал) — пропускаем
        return None
    finally:
        conn.close()


def db_list_chats() -> list[sqlite3.Row]:
    conn = db_connect()
    rows = conn.execute("SELECT * FROM chats ORDER BY last_seen_at DESC").fetchall()
    conn.close()
    return rows


def db_list_messages(chat_id: str, limit: int = 200) -> list[sqlite3.Row]:
    conn = db_connect()
    rows = conn.execute(
        "SELECT * FROM messages WHERE max_chat_id=? ORDER BY id DESC LIMIT ?",
        (chat_id, limit),
    ).fetchall()
    conn.close()
    return list(reversed(rows))


def db_map_tg_reply(tg_message_id: int, chat_id: str) -> None:
    conn = db_connect()
    conn.execute(
        "INSERT OR REPLACE INTO tg_reply_map (tg_message_id, max_chat_id) VALUES (?, ?)",
        (tg_message_id, chat_id),
    )
    conn.commit()
    conn.close()


def db_lookup_tg_reply(tg_message_id: int) -> Optional[str]:
    conn = db_connect()
    row = conn.execute(
        "SELECT max_chat_id FROM tg_reply_map WHERE tg_message_id=?", (tg_message_id,)
    ).fetchone()
    conn.close()
    return row["max_chat_id"] if row else None


# ============================ OUTGOING QUEUE =================================

@dataclass
class OutgoingMessage:
    chat_id: str
    text: str
    source: str  # "telegram" | "webapp"


outgoing_queue: "asyncio.Queue[OutgoingMessage]" = asyncio.Queue()

# =============================== TELEGRAM BOT =================================

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML)) if BOT_TOKEN else None
dp = Dispatcher()


def admin_only(message: Message) -> bool:
    return ADMIN_ID != 0 and message.from_user is not None and message.from_user.id == ADMIN_ID


@dp.message(CommandStart())
async def cmd_start(message: Message):
    if not admin_only(message):
        await message.answer("Этот бот приватный.")
        return
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[
            InlineKeyboardButton(
                text="MAX чаты",
                web_app=WebAppInfo(url=f"{WEBAPP_PUBLIC_URL}/?key={WEBAPP_SECRET}"),
            )
        ]]
    )
    await message.answer(
        "Мост MAX ↔ Telegram запущен.\n"
        "Новые сообщения из MAX будут приходить сюда — просто ответь (reply) "
        "на пересланное сообщение, чтобы отправить ответ обратно в MAX.\n\n"
        "Или открой мини-интерфейс со списком чатов:",
        reply_markup=kb,
    )


@dp.message(Command("chats"))
async def cmd_chats(message: Message):
    if not admin_only(message):
        return
    chats = db_list_chats()
    if not chats:
        await message.answer("Пока нет ни одного чата (жду данные из MAX Web).")
        return
    lines = [f"• {c['title'] or c['max_chat_id']}  (id: <code>{c['max_chat_id']}</code>)" for c in chats]
    await message.answer("\n".join(lines))


@dp.message(F.reply_to_message)
async def handle_reply(message: Message):
    """Ответ (reply) на пересланное сообщение -> уходит обратно в MAX."""
    if not admin_only(message):
        return
    chat_id = db_lookup_tg_reply(message.reply_to_message.message_id)
    if not chat_id:
        await message.answer("Не понимаю, к какому чату MAX относится это сообщение.")
        return
    text = message.text or message.caption
    if not text:
        await message.answer("Пока умею пересылать только текст.")
        return
    await outgoing_queue.put(OutgoingMessage(chat_id=chat_id, text=text, source="telegram"))
    await message.answer("Отправляю в MAX…")


async def forward_incoming_to_telegram(chat_id: str, chat_title: str, text: str) -> None:
    """Playwright-воркер вызывает это при новом сообщении из MAX."""
    if not bot or ADMIN_ID == 0:
        return
    sent = await bot.send_message(
        ADMIN_ID,
        f"<b>{chat_title or chat_id}</b>\n{text}",
    )
    db_map_tg_reply(sent.message_id, chat_id)


# ============================== MINI WEB-APP ===================================

INDEX_HTML = """<!doctype html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>MAX чаты</title>
<style>
  body { font-family: -apple-system, sans-serif; margin: 0; background: #111; color: #eee; }
  #chats { display: flex; flex-direction: column; }
  .chat-row { padding: 12px 16px; border-bottom: 1px solid #222; cursor: pointer; }
  .chat-row:active { background: #1b1b1b; }
  #messages { padding: 12px 16px; display: none; flex-direction: column; gap: 6px; }
  .msg { max-width: 80%; padding: 8px 10px; border-radius: 10px; }
  .msg.in { background: #222; align-self: flex-start; }
  .msg.out { background: #2b5278; align-self: flex-end; }
  #composer { display: none; position: fixed; bottom: 0; left: 0; right: 0; padding: 8px; background: #111; gap: 6px; }
  #composer input { flex: 1; padding: 10px; border-radius: 8px; border: none; }
  #composer button { padding: 10px 14px; border-radius: 8px; border: none; background: #2b5278; color: #fff; }
  #back { display: none; padding: 10px 16px; color: #7ab8ff; }
</style>
</head>
<body>
  <div id="back" onclick="showChats()">&larr; Назад к чатам</div>
  <div id="chats"></div>
  <div id="messages" style="flex-direction:column;"></div>
  <div id="composer">
    <input id="text" placeholder="Сообщение…">
    <button onclick="send()">Отправить</button>
  </div>

<script>
const key = new URLSearchParams(location.search).get('key') || '';
let currentChat = null;

async function api(path, opts) {
  const res = await fetch(path + (path.includes('?') ? '&' : '?') + 'key=' + encodeURIComponent(key), opts);
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

async function loadChats() {
  const chats = await api('/api/chats');
  const el = document.getElementById('chats');
  el.innerHTML = '';
  chats.forEach(c => {
    const row = document.createElement('div');
    row.className = 'chat-row';
    row.textContent = c.title || c.max_chat_id;
    row.onclick = () => openChat(c.max_chat_id, c.title);
    el.appendChild(row);
  });
}

async function openChat(chatId, title) {
  currentChat = chatId;
  document.getElementById('chats').style.display = 'none';
  document.getElementById('back').style.display = 'block';
  document.getElementById('composer').style.display = 'flex';
  const msgsEl = document.getElementById('messages');
  msgsEl.style.display = 'flex';
  const msgs = await api('/api/messages?chat_id=' + encodeURIComponent(chatId));
  msgsEl.innerHTML = '';
  msgs.forEach(m => {
    const d = document.createElement('div');
    d.className = 'msg ' + (m.direction === 'out' ? 'out' : 'in');
    d.textContent = m.text;
    msgsEl.appendChild(d);
  });
  msgsEl.scrollTop = msgsEl.scrollHeight;
}

function showChats() {
  currentChat = null;
  document.getElementById('chats').style.display = 'flex';
  document.getElementById('messages').style.display = 'none';
  document.getElementById('composer').style.display = 'none';
  document.getElementById('back').style.display = 'none';
  loadChats();
}

async function send() {
  const input = document.getElementById('text');
  const text = input.value.trim();
  if (!text || !currentChat) return;
  input.value = '';
  await api('/api/send', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({chat_id: currentChat, text}),
  });
  openChat(currentChat);
}

loadChats();
</script>
</body>
</html>
"""


def check_key(request: web.Request) -> bool:
    return request.query.get("key") == WEBAPP_SECRET


async def http_index(request: web.Request) -> web.Response:
    if not check_key(request):
        return web.Response(status=403, text="forbidden")
    return web.Response(text=INDEX_HTML, content_type="text/html")


async def http_chats(request: web.Request) -> web.Response:
    if not check_key(request):
        return web.json_response({"error": "forbidden"}, status=403)
    chats = [dict(row) for row in db_list_chats()]
    return web.json_response(chats)


async def http_messages(request: web.Request) -> web.Response:
    if not check_key(request):
        return web.json_response({"error": "forbidden"}, status=403)
    chat_id = request.query.get("chat_id", "")
    msgs = [dict(row) for row in db_list_messages(chat_id)]
    return web.json_response(msgs)


async def http_send(request: web.Request) -> web.Response:
    if not check_key(request):
        return web.json_response({"error": "forbidden"}, status=403)
    data = await request.json()
    chat_id = data.get("chat_id")
    text = data.get("text")
    if not chat_id or not text:
        return web.json_response({"error": "chat_id and text required"}, status=400)
    await outgoing_queue.put(OutgoingMessage(chat_id=chat_id, text=text, source="webapp"))
    return web.json_response({"ok": True})


def build_webapp() -> web.Application:
    app = web.Application()
    app.router.add_get("/", http_index)
    app.router.add_get("/api/chats", http_chats)
    app.router.add_get("/api/messages", http_messages)
    app.router.add_post("/api/send", http_send)
    return app


# ============================ PLAYWRIGHT WORKER ================================

class MaxWatcher:
    """Держит сессию MAX Web, ловит новые сообщения, отправляет исходящие."""

    def __init__(self):
        self._context: Optional[BrowserContext] = None
        self._page: Optional[Page] = None
        self._seen: set[str] = set()

    async def start(self, headless: bool = True) -> None:
        self._pw = await async_playwright().start()
        browser = await self._pw.chromium.launch(headless=headless)
        storage_state = str(STATE_PATH) if STATE_PATH.exists() else None
        self._context = await browser.new_context(storage_state=storage_state)
        self._page = await self._context.new_page()

        if USE_NETWORK_SNIFFING:
            self._page.on("response", self._on_response_wrapper)

        await self._page.goto(MAX_WEB_URL, wait_until="networkidle")
        log.info("MAX Web открыт: %s", MAX_WEB_URL)

    async def save_session(self) -> None:
        if self._context:
            await self._context.storage_state(path=str(STATE_PATH))
            log.info("Сессия сохранена в %s", STATE_PATH)

    def _on_response_wrapper(self, response: Response):
        asyncio.create_task(self._handle_network_response(response))

    async def _handle_network_response(self, response: Response) -> None:
        """
        ЗАГОТОВКА для сетевого перехвата сообщений.
        Реши через DevTools -> Network, какой URL/паттерн отдаёт новые
        сообщения (обычно что-то вроде /api/.../messages или WebSocket),
        и разбери JSON здесь, вызывая self._register_incoming(...).
        """
        url = response.url
        if "messages" not in url and "message" not in url:
            return
        try:
            data = await response.json()
        except Exception:
            return
        # TODO: подставь реальную структуру ответа MAX Web, пример-заготовка:
        # for item in data.get("messages", []):
        #     await self._register_incoming(
        #         chat_id=str(item["chatId"]),
        #         chat_title=item.get("chatTitle", ""),
        #         text=item.get("text", ""),
        #         dedup_key=str(item["id"]),
        #     )
        _ = data  # избавляемся от предупреждения линтера, пока TODO не заполнен

    async def _register_incoming(self, chat_id: str, chat_title: str, text: str, dedup_key: str) -> None:
        if not text:
            return
        db_upsert_chat(chat_id, chat_title)
        row_id = db_add_message(chat_id, "in", text, dedup_key)
        if row_id is None:
            return  # уже видели
        log.info("Новое сообщение из MAX [%s]: %s", chat_title or chat_id, text[:80])
        await forward_incoming_to_telegram(chat_id, chat_title, text)

    async def poll_dom_once(self) -> None:
        """
        Фолбэк-режим: читаем список чатов и последние сообщения прямо из DOM.
        Селекторы SEL_* нужно подогнать под актуальную вёрстку web.max.ru.
        """
        if not self._page:
            return
        chat_items = await self._page.query_selector_all(SEL_CHAT_LIST_ITEM)
        for item in chat_items:
            try:
                title_el = await item.query_selector(SEL_CHAT_TITLE)
                title = (await title_el.inner_text()).strip() if title_el else "chat"
                chat_id = await item.get_attribute("data-chat-id") or title
            except Exception:
                continue
            db_upsert_chat(chat_id, title)

            # Открываем чат, чтобы прочитать последние сообщения.
            # В реальной вёрстке, скорее всего, потребуется click() + ожидание.
            try:
                await item.click()
                await self._page.wait_for_timeout(300)
            except Exception:
                continue

            msg_items = await self._page.query_selector_all(SEL_MESSAGE_ITEM)
            for msg_el in msg_items[-10:]:
                try:
                    text_el = await msg_el.query_selector(SEL_MESSAGE_TEXT)
                    text = (await text_el.inner_text()).strip() if text_el else ""
                    is_out = await msg_el.query_selector(SEL_MESSAGE_AUTHOR_OUT) is not None
                    msg_key = await msg_el.get_attribute("data-message-id") or f"{chat_id}:{hash(text)}"
                except Exception:
                    continue
                if not text or is_out:
                    continue
                await self._register_incoming(chat_id, title, text, msg_key)

    async def send_message(self, chat_id: str, text: str) -> None:
        """
        Открывает чат по chat_id и отправляет текст.
        Нужно подставить твою логику поиска чата по id/заголовку в списке.
        """
        if not self._page:
            return
        chat_selector = f"{SEL_CHAT_LIST_ITEM}[data-chat-id='{chat_id}']"
        chat_el = await self._page.query_selector(chat_selector)
        if not chat_el:
            log.warning("Не нашёл чат %s в списке MAX Web, ищу по заголовку", chat_id)
            chat_el = await self._page.query_selector(
                f"{SEL_CHAT_LIST_ITEM}:has-text('{chat_id}')"
            )
        if not chat_el:
            log.error("Не удалось найти чат %s для отправки", chat_id)
            return

        await chat_el.click()
        await self._page.wait_for_timeout(300)

        input_el = await self._page.query_selector(SEL_MESSAGE_INPUT)
        if not input_el:
            log.error("Не нашёл поле ввода сообщения (SEL_MESSAGE_INPUT)")
            return
        await input_el.click()
        await input_el.type(text, delay=15)

        send_btn = await self._page.query_selector(SEL_SEND_BUTTON)
        if send_btn:
            await send_btn.click()
        else:
            await self._page.keyboard.press("Enter")

        db_add_message(chat_id, "out", text, dedup_key=None)
        log.info("Отправлено в MAX [%s]: %s", chat_id, text[:80])

    async def close(self) -> None:
        if self._context:
            await self.save_session()
            await self._context.close()
        if hasattr(self, "_pw"):
            await self._pw.stop()


async def run_playwright_watcher() -> None:
    watcher = MaxWatcher()
    await watcher.start(headless=HEADLESS)
    try:
        while True:
            if not USE_NETWORK_SNIFFING:
                try:
                    await watcher.poll_dom_once()
                except Exception:
                    log.exception("Ошибка при опросе MAX Web")

            # обрабатываем исходящие сообщения из очереди
            while not outgoing_queue.empty():
                out = await outgoing_queue.get()
                try:
                    await watcher.send_message(out.chat_id, out.text)
                except Exception:
                    log.exception("Не удалось отправить сообщение в MAX")

            await asyncio.sleep(POLL_INTERVAL)
    finally:
        await watcher.close()


# =================================== LOGIN MODE =================================

async def run_login_flow() -> None:
    """Открывает видимый браузер для ручного логина, сохраняет сессию."""
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()
        await page.goto(MAX_WEB_URL)
        print("Залогинься в MAX Web в открывшемся окне, затем вернись в консоль и нажми Enter…")
        await asyncio.get_event_loop().run_in_executor(None, input)
        await context.storage_state(path=str(STATE_PATH))
        print(f"Сессия сохранена в {STATE_PATH}")
        await browser.close()


# ====================================== MAIN =====================================

async def run_all() -> None:
    if not BOT_TOKEN or ADMIN_ID == 0:
        raise SystemExit(
            "Задай переменные окружения TG_BOT_TOKEN и TG_ADMIN_ID перед запуском."
        )
    db_init()
    log.info("Мини-интерфейс: %s/?key=%s", WEBAPP_PUBLIC_URL, WEBAPP_SECRET)

    app = build_webapp()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", WEBAPP_PORT)
    await site.start()

    await asyncio.gather(
        dp.start_polling(bot),
        run_playwright_watcher(),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="MAX <-> Telegram bridge")
    parser.add_argument("--login", action="store_true", help="ручной логин в MAX Web и сохранение сессии")
    args = parser.parse_args()

    if args.login:
        asyncio.run(run_login_flow())
    else:
        asyncio.run(run_all())


if __name__ == "__main__":
    main()

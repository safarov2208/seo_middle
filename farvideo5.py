import asyncio
import logging
import os
import re
import time
from typing import Optional

import redis.asyncio as aioredis
from pyrogram import Client, filters
from pyrogram.errors import FloodWait, MessageNotModified
from pyrogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

API_ID = int(os.environ.get("API_ID", "0"))
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
USERBOT_SESSION = os.environ.get("USERBOT_SESSION", "userbot")
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")

SUPPORTED_PLATFORMS = [
    "instagram",
    "tiktok",
    "youtube",
    "likee",
    "pinterest",
    "snapchat",
    "telegram",
]

PLATFORM_CACHE_TTL = {
    "instagram": 86400 * 7,
    "tiktok": 86400 * 7,
    "youtube": 86400 * 3,
    "likee": 86400 * 7,
    "pinterest": 86400 * 7,
    "snapchat": 86400 * 7,
}

TELEGRAM_CONTENT_TTL = 86400 * 2


class RedisCache:
    def __init__(self, url: str = REDIS_URL):
        self.url = url
        self.redis: Optional[aioredis.Redis] = None

    async def ensure(self):
        if self.redis is None:
            self.redis = await aioredis.from_url(self.url, decode_responses=True)

    async def get(self, key: str) -> Optional[str]:
        await self.ensure()
        return await self.redis.get(key)

    async def set(self, key: str, value: str, ttl: int = 3600):
        await self.ensure()
        await self.redis.set(key, value, ex=ttl)

    async def setnx(self, key: str, value: str, ttl: int) -> bool:
        await self.ensure()
        result = await self.redis.set(key, value, nx=True, ex=ttl)
        return result is not None

    async def delete(self, key: str):
        await self.ensure()
        await self.redis.delete(key)

    async def hset(self, key: str, mapping: dict, ttl: int = 3600):
        await self.ensure()
        await self.redis.hset(key, mapping=mapping)
        await self.redis.expire(key, ttl)

    async def hgetall(self, key: str) -> dict:
        await self.ensure()
        return await self.redis.hgetall(key)

    async def close(self):
        if self.redis:
            await self.redis.close()
            self.redis = None


redis_cache = RedisCache()

bot = Client("farvideo_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
userbot = Client(USERBOT_SESSION, api_id=API_ID, api_hash=API_HASH)


def detect_platform_from_caption(caption: str) -> Optional[str]:
    if not caption:
        return None
    lower = caption.lower()
    for platform in SUPPORTED_PLATFORMS:
        if f"[[[{platform}]]]" in lower:
            return platform
    return None


def extract_original_url_from_caption(caption: str) -> Optional[str]:
    if not caption:
        return None
    match = re.search(r"url\[([^\]]+)\]", caption, re.IGNORECASE)
    if match:
        return match.group(1)
    return None


def extract_youtube_shortcode(caption: str) -> Optional[str]:
    if not caption:
        return None
    match = re.search(r"shortcode\[([^\]]+)\]", caption, re.IGNORECASE)
    if match:
        return match.group(1)
    return None


def build_clean_caption(caption: str) -> str:
    if not caption:
        return ""
    cleaned = re.sub(r"\[\[\[[^\]]+\]\]\]", "", caption)
    cleaned = re.sub(r"url\[[^\]]*\]", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"shortcode\[[^\]]*\]", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"story\[[^\]]*\]", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s{2,}", " ", cleaned).strip()
    return cleaned


def create_inline_share_button(
    file_id: str, media_type: str, caption: str
) -> InlineKeyboardMarkup:
    if media_type == "video":
        share_text = "Поделиться видео"
    elif media_type == "audio":
        share_text = "Поделиться аудио"
    else:
        share_text = "Поделиться фото"
    bot_username = os.environ.get("BOT_USERNAME", "farvideo_bot")
    share_url = f"https://t.me/{bot_username}?start=share_{file_id}"
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton(share_text, url=share_url)]]
    )


async def cache_media(url: str, platform: str, data: dict):
    ttl = PLATFORM_CACHE_TTL.get(platform, 86400)
    cache_key = f"media:{platform}:{url}"
    await redis_cache.hset(cache_key, data, ttl=ttl)
    logger.info("Cached media for %s url=%s", platform, url)


async def get_cached_media(url: str, platform: str) -> Optional[dict]:
    cache_key = f"media:{platform}:{url}"
    data = await redis_cache.hgetall(cache_key)
    if data:
        logger.info("Cache HIT for %s url=%s", platform, url)
        return data
    return None


async def send_media_to_user(
    client: Client,
    chat_id: int,
    file_id: str,
    media_type: str,
    caption: str,
    reply_markup=None,
    reply_to_message_id: Optional[int] = None,
):
    kwargs = {
        "chat_id": chat_id,
        "caption": caption,
        "reply_markup": reply_markup,
    }
    if reply_to_message_id:
        kwargs["reply_to_message_id"] = reply_to_message_id

    if media_type == "video":
        return await client.send_video(file_id=file_id, **kwargs)
    elif media_type == "photo":
        return await client.send_photo(file_id=file_id, **kwargs)
    elif media_type == "audio":
        return await client.send_audio(file_id=file_id, **kwargs)
    elif media_type == "document":
        return await client.send_document(file_id=file_id, **kwargs)
    elif media_type == "animation":
        return await client.send_animation(file_id=file_id, **kwargs)
    else:
        logger.warning("Unknown media type: %s", media_type)
        return None


class TelegramCopier:
    min_interval: float = 5.0
    max_retries: int = 3

    def __init__(self, client: Client):
        self.client = client
        self._last_send: float = 0.0

    async def _wait_for_rate(self):
        now = time.monotonic()
        elapsed = now - self._last_send
        if elapsed < self.min_interval:
            await asyncio.sleep(self.min_interval - elapsed)
        self._last_send = time.monotonic()

    async def copy_message(
        self,
        from_chat_id: int | str,
        message_id: int,
        to_chat_id: int | str,
        caption: Optional[str] = None,
        reply_markup=None,
    ) -> Optional[Message]:
        for attempt in range(1, self.max_retries + 1):
            await self._wait_for_rate()
            try:
                msg = await self.client.copy_message(
                    chat_id=to_chat_id,
                    from_chat_id=from_chat_id,
                    message_id=message_id,
                    caption=caption,
                    reply_markup=reply_markup,
                )
                return msg
            except FloodWait as e:
                wait_seconds = e.value + 5
                logger.warning(
                    "FloodWait %ds on attempt %d/%d, sleeping...",
                    wait_seconds,
                    attempt,
                    self.max_retries,
                )
                await asyncio.sleep(wait_seconds)
                self._last_send = time.monotonic()
            except Exception as exc:
                logger.error(
                    "Error copying message attempt %d/%d: %s",
                    attempt,
                    self.max_retries,
                    exc,
                )
                if attempt == self.max_retries:
                    raise
                await asyncio.sleep(2)
        return None


PLATFORM_URL_PATTERNS = {
    "instagram": re.compile(r"instagram\.com|instagr\.am", re.IGNORECASE),
    "tiktok": re.compile(r"tiktok\.com|vm\.tiktok\.com", re.IGNORECASE),
    "youtube": re.compile(
        r"youtube\.com|youtu\.be|youtube shorts", re.IGNORECASE
    ),
    "likee": re.compile(r"likee\.video|l\.likee\.video", re.IGNORECASE),
    "pinterest": re.compile(r"pinterest\.\w+|pin\.it", re.IGNORECASE),
    "snapchat": re.compile(r"snapchat\.com|snap\.com", re.IGNORECASE),
    "telegram": re.compile(r"t\.me|telegram\.me", re.IGNORECASE),
}


def detect_platform_from_url(url: str) -> Optional[str]:
    if not url:
        return None
    for platform, pattern in PLATFORM_URL_PATTERNS.items():
        if pattern.search(url):
            return platform
    return None


async def handle_userbot_media_message(
    client: Client,
    message: Message,
    target_chat_id: int,
    original_url: Optional[str] = None,
    request_message_id: Optional[int] = None,
):
    cap = message.caption or message.text or ""

    platform = detect_platform_from_caption(cap)
    if not platform and original_url:
        platform = detect_platform_from_url(original_url)

    is_telegram = platform == "telegram"
    is_story = "story[" in cap.lower() or bool(
        original_url and "/s/" in original_url
    )

    clean_cap = build_clean_caption(cap)

    if not original_url:
        original_url = extract_original_url_from_caption(cap)

    media = (
        message.video
        or message.photo
        or message.audio
        or message.document
        or message.animation
    )
    if not media:
        logger.warning("No media found in userbot message %d", message.id)
        return

    if message.video or message.animation:
        media_type = "video" if message.video else "animation"
    elif message.photo:
        media_type = "photo"
    elif message.audio:
        media_type = "audio"
    else:
        media_type = "document"

    file_id = media.file_id

    keyboard = None
    if platform and platform != "telegram" and media_type in ("photo", "video", "audio"):
        keyboard = create_inline_share_button(file_id, media_type, clean_cap)

    sent = await send_media_to_user(
        client,
        chat_id=target_chat_id,
        file_id=file_id,
        media_type=media_type,
        caption=clean_cap,
        reply_markup=keyboard,
        reply_to_message_id=request_message_id,
    )

    if sent and platform and platform != "telegram" and file_id and original_url:
        cache_data = {
            "file_id": file_id,
            "caption": clean_cap,
            "type": media_type,
        }
        if platform == "youtube":
            shortcode = extract_youtube_shortcode(cap)
            if shortcode:
                yt_cache_type = "audio" if media_type == "audio" else "video"
                yt_key = f"youtube:{yt_cache_type}:{shortcode}"
                await redis_cache.hset(
                    yt_key,
                    cache_data,
                    ttl=PLATFORM_CACHE_TTL.get("youtube", 86400 * 3),
                )
                logger.info("Cached YouTube %s shortcode=%s", yt_cache_type, shortcode)
        await cache_media(original_url, platform, cache_data)

    if is_telegram and not is_story and sent:
        msg_id = sent.id
        ttl_key = f"telegram_ttl:{target_chat_id}:{msg_id}"
        await redis_cache.set(ttl_key, "1", TELEGRAM_CONTENT_TTL)
        logger.info(
            "Scheduled TTL for Telegram content chat=%d msg=%d",
            target_chat_id,
            msg_id,
        )

    return sent


async def process_url(
    client: Client,
    url: str,
    chat_id: int,
    request_message_id: Optional[int] = None,
):
    platform = detect_platform_from_url(url)
    if not platform:
        await client.send_message(
            chat_id, "Не удалось определить платформу для данной ссылки."
        )
        return

    cached = await get_cached_media(url, platform)
    if cached:
        file_id = cached.get("file_id")
        caption = cached.get("caption", "")
        media_type = cached.get("type", "video")

        keyboard = None
        if platform != "telegram" and media_type in ("photo", "video", "audio"):
            keyboard = create_inline_share_button(file_id, media_type, caption)

        await send_media_to_user(
            client,
            chat_id=chat_id,
            file_id=file_id,
            media_type=media_type,
            caption=caption,
            reply_markup=keyboard,
            reply_to_message_id=request_message_id,
        )
        return

    await client.send_message(
        chat_id,
        "⏳ Загружаю...",
        reply_to_message_id=request_message_id,
    )


@bot.on_message(filters.private & filters.text & ~filters.command(["start", "help"]))
async def url_handler(client: Client, message: Message):
    url = message.text.strip()
    if not url.startswith("http"):
        return
    await process_url(client, url, message.chat.id, message.id)


@bot.on_message(
    filters.private & filters.command("start")
)
async def start_handler(client: Client, message: Message):
    await message.reply(
        "👋 Привет! Отправь мне ссылку на видео из Instagram, TikTok, YouTube, "
        "Likee, Pinterest или Snapchat и я скачаю его для тебя."
    )


@bot.on_message(
    filters.private & filters.command("help")
)
async def help_handler(client: Client, message: Message):
    await message.reply(
        "📖 Поддерживаемые платформы:\n"
        "• Instagram (Reels, Stories, Posts)\n"
        "• TikTok\n"
        "• YouTube (Shorts)\n"
        "• Likee\n"
        "• Pinterest\n"
        "• Snapchat\n\n"
        "Просто отправь ссылку и бот скачает медиафайл."
    )


@bot.on_message(
    filters.regex(r"^\.wow$") & (filters.group | filters.private)
)
async def business_message_handler(client: Client, message: Message):
    msg_key = f"wow:{message.chat.id}:{message.id}"
    acquired = await redis_cache.setnx(msg_key, "1", 60)
    if not acquired:
        logger.info(
            "Duplicate .wow detected for chat=%d msg=%d, skipping",
            message.chat.id,
            message.id,
        )
        return

    reply = message.reply_to_message
    if not reply:
        await message.reply("Ответьте на сообщение со ссылкой используя .wow")
        return

    url = None
    if reply.text:
        match = re.search(r"https?://\S+", reply.text)
        if match:
            url = match.group(0)
    elif reply.caption:
        match = re.search(r"https?://\S+", reply.caption)
        if match:
            url = match.group(0)

    if not url:
        await message.reply("Не найдена ссылка в сообщении.")
        return

    await process_url(client, url, message.chat.id, message.id)


@userbot.on_message(
    filters.private & (filters.video | filters.photo | filters.audio | filters.document | filters.animation)
)
async def userbot_media_handler(client: Client, message: Message):
    cap = message.caption or ""
    target_chat_str = os.environ.get("TARGET_CHAT_ID", "")
    if not target_chat_str:
        logger.error("TARGET_CHAT_ID not set")
        return
    try:
        target_chat_id = int(target_chat_str)
    except ValueError:
        logger.error("Invalid TARGET_CHAT_ID: %s", target_chat_str)
        return

    original_url = extract_original_url_from_caption(cap)

    await handle_userbot_media_message(
        bot,
        message,
        target_chat_id=target_chat_id,
        original_url=original_url,
    )


async def main():
    logger.info("Starting FarVideo bot...")
    await asyncio.gather(
        bot.start(),
        userbot.start(),
    )
    logger.info("Bot and userbot started successfully.")
    await asyncio.gather(
        bot.idle(),
        userbot.idle(),
    )


if __name__ == "__main__":
    asyncio.run(main())

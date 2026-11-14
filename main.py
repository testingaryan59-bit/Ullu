import os
import asyncio
import aiohttp
import re
import logging
import hashlib
from bs4 import BeautifulSoup
from pyrogram import Client
from pyrogram.errors import FloodWait, RPCError
from datetime import datetime
from pathlib import Path

# ================= CONFIG =================
BOT_TOKEN      = os.getenv("BOT_TOKEN")
CHANNEL_ID     = -1003635369137
MAX_CONCURRENT = 6
CHECK_INTERVAL = 1800
CATEGORY_URL   = "https://xmaza.movie/category/ullu/page/{}/"
START_PAGE     = 1
END_PAGE       = 53
SEEN_FILE      = "uploaded.txt"
FAIL_FILE      = "failed.txt"
TMP_DIR        = Path("/tmp/xmaza")
TMP_DIR.mkdir(exist_ok=True)
# ==========================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("bot.log")
    ]
)
logger = logging.getLogger("XMaza")

app = Client("xmaza_bot", bot_token=BOT_TOKEN)

def load_seen(path=SEEN_FILE) -> set:
    p = Path(path)
    if not p.exists():
        return set()
    return set(l.strip() for l in p.read_text().splitlines() if l.strip())

def save_entry(url: str, path=SEEN_FILE):
    with open(path, "a") as f:
        f.write(url + "\n")

seen_lock = asyncio.Lock()

async def mark_seen(url: str, seen: set):
    async with seen_lock:
        seen.add(url)
        save_entry(url)

async def mark_failed(url: str):
    save_entry(url, FAIL_FILE)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}

async def get_video_links(session: aiohttp.ClientSession, page: int) -> list[str]:
    url = CATEGORY_URL.format(page)
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            if resp.status != 200:
                logger.warning(f"Page {page} → HTTP {resp.status}")
                return []
            text = await resp.text()
        soup = BeautifulSoup(text, "html.parser")
        links = []
        for a in soup.select("a.video"):
            href = a.get("href", "")
            if "xmaza.movie" in href and "/category/" not in href and "/page/" not in href:
                links.append(href.strip())
        return list(set(links))
    except Exception as e:
        logger.error(f"Page {page} scrape fail: {e}")
        return []

async def extract_data(session: aiohttp.ClientSession, page_url: str) -> dict | None:
    try:
        async with session.get(page_url, timeout=aiohttp.ClientTimeout(total=25)) as resp:
            text = await resp.text()

        video_patterns = [
            r'https://video\.maalcdn\.com/[^"\']+\.mp4',
            r'https://[^"\']+\.maalcdn\.com/[^"\']+\.mp4',
            r'file:\s*["\']?(https://[^"\']+\.mp4)',
        ]
        video_url = None
        for pat in video_patterns:
            m = re.search(pat, text)
            if m:
                video_url = m.group(1) if m.lastindex else m.group(0)
                break

        if not video_url:
            return None

        thumb_match = re.search(
            r'https://xmaza\.movie/wp-content/uploads/[^"\']+\.(?:webp|jpg|png)', text
        )
        thumb_url = thumb_match.group(0) if thumb_match else None

        title_match = re.search(r'<title>(.*?)</title>', text, re.I)
        raw_title = title_match.group(1) if title_match else "ULLU Video"
        title = raw_title.split("»")[0].split("|")[0].strip()

        return {
            "video_url": video_url,
            "thumb_url": thumb_url,
            "title": title,
            "page_url": page_url
        }
    except Exception as e:
        logger.error(f"Extract fail {page_url}: {e}")
        return None

async def download_file(session: aiohttp.ClientSession, url: str, path: Path, retries=3):
    for attempt in range(retries):
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=300)) as resp:
                resp.raise_for_status()
                with open(path, "wb") as f:
                    async for chunk in resp.content.iter_chunked(2 * 1024 * 1024):
                        f.write(chunk)
            return True
        except Exception as e:
            logger.warning(f"Download attempt {attempt+1} fail: {e}")
            await asyncio.sleep(2 ** attempt)
    return False

def build_caption(data: dict) -> str:
    return (
        f"**{data['title']}**\n\n"
        f"🎬 ULLU Collection\n"
        f"📅 {datetime.now().strftime('%d %b %Y')}\n"
        f"🔗 [Source]({data['page_url']})\n\n"
        f"#ULLU #XMaza #WebSeries"
    )

async def upload_with_retry(data: dict, local_path: Path, thumb_path: Path | None, retries=3):
    caption = build_caption(data)
    file_size = local_path.stat().st_size

    for attempt in range(retries):
        try:
            if file_size > 49 * 1024 * 1024:
                await app.send_document(
                    chat_id=CHANNEL_ID,
                    document=str(local_path),
                    caption=caption,
                    thumb=str(thumb_path) if thumb_path else None,
                    force_document=True
                )
            else:
                await app.send_video(
                    chat_id=CHANNEL_ID,
                    video=str(local_path),
                    caption=caption,
                    thumb=str(thumb_path) if thumb_path else None,
                    supports_streaming=True,
                    width=1280,
                    height=720
                )
            return True

        except FloodWait as e:
            logger.warning(f"FloodWait {e.value}s — waiting...")
            await asyncio.sleep(e.value + 2)

        except RPCError as e:
            logger.error(f"Telegram RPC error: {e}")
            await asyncio.sleep(5 * (attempt + 1))

        except Exception as e:
            logger.error(f"Upload error attempt {attempt+1}: {e}")
            await asyncio.sleep(3)

    return False

async def process_video(
    session: aiohttp.ClientSession,
    data: dict,
    sem: asyncio.Semaphore,
    seen: set
):
    async with sem:
        video_url = data["video_url"]
        if video_url in seen:
            return

        uid = hashlib.md5(video_url.encode()).hexdigest()[:10]
        local_path = TMP_DIR / f"{uid}.mp4"
        thumb_path = TMP_DIR / f"{uid}_thumb.jpg" if data["thumb_url"] else None

        try:
            ok = await download_file(session, video_url, local_path)
            if not ok:
                logger.error(f"Download failed: {data['title']}")
                await mark_failed(video_url)
                return

            if thumb_path and data["thumb_url"]:
                await download_file(session, data["thumb_url"], thumb_path)
                if not thumb_path.exists():
                    thumb_path = None

            success = await upload_with_retry(data, local_path, thumb_path)
            if success:
                await mark_seen(video_url, seen)
                logger.info(f"✅ {data['title']}")
            else:
                await mark_failed(video_url)
                logger.error(f"❌ Upload failed: {data['title']}")

        except Exception as e:
            logger.error(f"Process error {data['title']}: {e}")
            await mark_failed(video_url)

        finally:
            for p in [local_path, thumb_path]:
                if p and p.exists():
                    try:
                        p.unlink()
                    except:
                        pass

async def crawl_pages(session: aiohttp.ClientSession, start: int, end: int) -> list[str]:
    sem = asyncio.Semaphore(5)

    async def _get(page):
        async with sem:
            return await get_video_links(session, page)

    results = await asyncio.gather(*[_get(p) for p in range(start, end + 1)])
    all_links = []
    for r in results:
        all_links.extend(r)
    return list(set(all_links))

async def full_dump():
    seen = load_seen()
    failed = load_seen(FAIL_FILE)
    sem = asyncio.Semaphore(MAX_CONCURRENT)

    connector = aiohttp.TCPConnector(limit=20, ttl_dns_cache=300)
    async with aiohttp.ClientSession(headers=HEADERS, connector=connector) as session:

        logger.info("Crawling pages...")
        all_links = await crawl_pages(session, START_PAGE, END_PAGE)
        logger.info(f"Total unique links: {len(all_links)}")

        retry_links = list(failed - seen)
        if retry_links:
            logger.info(f"Retrying {len(retry_links)} failed...")
            all_links = list(set(all_links + retry_links))

        ext_sem = asyncio.Semaphore(10)
        async def _extract(link):
            async with ext_sem:
                return await extract_data(session, link)

        results = await asyncio.gather(*[_extract(l) for l in all_links])
        video_list = [r for r in results if r and r["video_url"] not in seen]
        logger.info(f"Videos to upload: {len(video_list)}")

        await asyncio.gather(*[
            process_video(session, data, sem, seen)
            for data in video_list
        ])

    logger.info("Full dump complete.")

async def monitor_new():
    seen = load_seen()
    sem = asyncio.Semaphore(3)

    while True:
        try:
            connector = aiohttp.TCPConnector(limit=10)
            async with aiohttp.ClientSession(headers=HEADERS, connector=connector) as session:
                links = await get_video_links(session, 1)
                new_links = [l for l in links if l not in seen]

                if new_links:
                    logger.info(f"Monitor: {len(new_links)} new videos")
                    tasks = []
                    for link in new_links:
                        data = await extract_data(session, link)
                        if data and data["video_url"] not in seen:
                            tasks.append(process_video(session, data, sem, seen))
                    if tasks:
                        await asyncio.gather(*tasks)
                else:
                    logger.info("Monitor: no new videos")

        except Exception as e:
            logger.error(f"Monitor error: {e}")

        await asyncio.sleep(CHECK_INTERVAL)

async def main():
    await app.start()
    logger.info(f"Bot live — Channel: {CHANNEL_ID}")
    await full_dump()
    await monitor_new()

if __name__ == "__main__":
    asyncio.run(main())

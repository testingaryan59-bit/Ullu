import os
import asyncio
import aiohttp
import re
import logging
from bs4 import BeautifulSoup
from pyrogram import Client
from pyrogram.types import InputMediaVideo
from datetime import datetime

# ================= CONFIG (Railway Variables se lega) =================
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHANNEL_ID = int(os.getenv("CHANNEL_ID"))          # -100xxxxxxxxxx
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", "6"))
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "1800"))  # 30 min
CATEGORY_URL = "https://xmaza.movie/category/ullu/page/{}/"
START_PAGE = 1
END_PAGE = 53
SEEN_FILE = "uploaded.txt"
# ======================================================================

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger("XMaza-Dumper")

app = Client("xmaza_bot", bot_token=BOT_TOKEN)

def load_seen():
    if not os.path.exists(SEEN_FILE):
        return set()
    with open(SEEN_FILE) as f:
        return set(line.strip() for line in f if line.strip())

def save_seen(url):
    with open(SEEN_FILE, "a") as f:
        f.write(url + "\n")

async def get_video_links(session, page):
    url = CATEGORY_URL.format(page)
    try:
        async with session.get(url, timeout=25) as resp:
            text = await resp.text()
        soup = BeautifulSoup(text, "html.parser")
        links = []
        for a in soup.select("a.video"):
            href = a.get("href")
            if href and "xmaza.movie" in href and "/category/" not in href and "/page/" not in href:
                links.append(href)
        return list(set(links))
    except Exception as e:
        logger.error(f"Page {page} fail: {e}")
        return []

async def extract_data(session, page_url):
    try:
        async with session.get(page_url, timeout=20) as resp:
            text = await resp.text()

        video_match = re.search(r'https://video\.maalcdn\.com/[^"\']+\.mp4', text)
        if not video_match:
            return None
        video_url = video_match.group(0)

        thumb_match = re.search(r'https://xmaza\.movie/wp-content/uploads/[^"\']+\.(?:webp|jpg|png)', text)
        thumb_url = thumb_match.group(0) if thumb_match else None

        title_match = re.search(r'<title>(.*?)</title>', text, re.I)
        title = title_match.group(1).split("»")[0].strip() if title_match else "ULLU Video"

        return {
            "video_url": video_url,
            "thumb_url": thumb_url,
            "title": title,
            "page_url": page_url
        }
    except:
        return None

async def download_file(session, url, path):
    async with session.get(url) as resp:
        with open(path, "wb") as f:
            async for chunk in resp.content.iter_chunked(1024*1024):
                f.write(chunk)

async def process_video(session, data, sem, seen):
    async with sem:
        video_url = data["video_url"]
        if video_url in seen:
            return

        filename = video_url.split("/")[-1].replace("%20", "_")[:80]
        local_path = f"/tmp/{filename}"
        thumb_path = None

        try:
            await download_file(session, video_url, local_path)

            if data["thumb_url"]:
                thumb_path = f"/tmp/thumb_{filename}.jpg"
                await download_file(session, data["thumb_url"], thumb_path)

            caption = f"""**{data['title']}**

🎬 ULLU Collection
📅 {datetime.now().strftime('%d %b %Y')}
🔗 [Source]({data['page_url']})

#ULLU #XMaza #WebSeries"""

            # 50MB se upar document mode me bhejega
            file_size = os.path.getsize(local_path)
            if file_size > 49 * 1024 * 1024:
                await app.send_document(
                    chat_id=CHANNEL_ID,
                    document=local_path,
                    caption=caption,
                    thumb=thumb_path
                )
            else:
                await app.send_video(
                    chat_id=CHANNEL_ID,
                    video=local_path,
                    caption=caption,
                    thumb=thumb_path,
                    supports_streaming=True
                )

            save_seen(video_url)
            seen.add(video_url)
            logger.info(f"✅ Uploaded → {data['title']}")

        except Exception as e:
            logger.error(f"❌ {data['title']}: {e}")
        finally:
            for p in [local_path, thumb_path]:
                if p and os.path.exists(p):
                    os.remove(p)

async def full_dump():
    seen = load_seen()
    sem = asyncio.Semaphore(MAX_CONCURRENT)

    async with aiohttp.ClientSession() as session:
        all_links = []
        for page in range(START_PAGE, END_PAGE + 1):
            links = await get_video_links(session, page)
            all_links.extend(links)
            logger.info(f"Page {page} → {len(links)} videos")

        all_links = list(set(all_links))
        logger.info(f"Total unique: {len(all_links)}")

        tasks = [extract_data(session, link) for link in all_links]
        results = await asyncio.gather(*tasks)
        video_list = [r for r in results if r]

        logger.info(f"Ready to upload: {len(video_list)}")

        upload_tasks = [process_video(session, data, sem, seen) for data in video_list]
        await asyncio.gather(*upload_tasks)

async def monitor_new():
    """Naye videos ke liye continuous monitoring"""
    seen = load_seen()
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                # Sirf page 1 check karo latest ke liye
                links = await get_video_links(session, 1)
                for link in links:
                    data = await extract_data(session, link)
                    if data and data["video_url"] not in seen:
                        await process_video(session, data, asyncio.Semaphore(3), seen)
        except Exception as e:
            logger.error(f"Monitor error: {e}")
        
        await asyncio.sleep(CHECK_INTERVAL)

async def main():
    await app.start()
    logger.info("Bot started on Railway")

    # Pehle full dump (page 1-53)
    await full_dump()

    # Phir continuous new video monitor
    await monitor_new()

if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())

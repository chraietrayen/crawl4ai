import os
import sys
import psutil
import asyncio
import requests
from xml.etree import ElementTree
from typing import List
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator

# ========== Configuration ==========
HEADLESS_ARGS = ["--disable-gpu", "--disable-dev-shm-usage", "--no-sandbox"]
SITEMAP_URL = "https://ai.pydantic.dev/sitemap.xml"

# ========== Ethics ==========
def check_ethics():
    print("\n[Ethics] Respect robots.txt of each site before crawling.")
    print("Examples:")
    print("  - https://www.youtube.com/robots.txt")
    print("  - https://www.github.com/robots.txt")

# ========== Get Sitemap URLs ==========
def get_sitemap_urls() -> List[str]:
    try:
        response = requests.get(SITEMAP_URL)
        response.raise_for_status()
        root = ElementTree.fromstring(response.content)
        namespace = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
        return [loc.text for loc in root.findall('.//ns:loc', namespace)]
    except Exception as e:
        print(f"Error fetching sitemap: {e}")
        return []

# ========== Single Page Example ==========
async def crawl_single_page():
    print("\n=== Single Page Crawl ===")
    async with AsyncWebCrawler() as crawler:
        result = await crawler.arun(url="https://ai.pydantic.dev/")
        print(result.markdown)

# ========== Sequential Crawl ==========
async def crawl_sequential(urls: List[str]):
    print("\n=== Sequential Crawl ===")
    browser_config = BrowserConfig(headless=True, extra_args=HEADLESS_ARGS)
    crawl_config = CrawlerRunConfig(markdown_generator=DefaultMarkdownGenerator())
    crawler = AsyncWebCrawler(config=browser_config)
    await crawler.start()
    try:
        session_id = "seq_session"
        for url in urls:
            result = await crawler.arun(url=url, config=crawl_config, session_id=session_id)
            if result.success:
                print(f"✅ {url} [Markdown size: {len(result.markdown.raw_markdown)}]")
            else:
                print(f"❌ {url} - Error: {result.error_message}")
    finally:
        await crawler.close()

# ========== Parallel Crawl ==========
async def crawl_parallel(urls: List[str], max_concurrent: int = 5):
    print("\n=== Parallel Crawl ===")
    peak_memory = 0
    process = psutil.Process(os.getpid())

    def log_memory(tag: str):
        nonlocal peak_memory
        mem = process.memory_info().rss
        peak_memory = max(peak_memory, mem)
        print(f"{tag} Memory: {mem // (1024*1024)}MB (Peak: {peak_memory // (1024*1024)}MB)")

    browser_config = BrowserConfig(headless=True, verbose=False, extra_args=HEADLESS_ARGS)
    crawl_config = CrawlerRunConfig(cache_mode=CacheMode.BYPASS)
    crawler = AsyncWebCrawler(config=browser_config)
    await crawler.start()
    try:
        success = fail = 0
        for i in range(0, len(urls), max_concurrent):
            batch = urls[i:i + max_concurrent]
            tasks = [
                crawler.arun(url=u, config=crawl_config, session_id=f"p_session_{i+j}")
                for j, u in enumerate(batch)
            ]
            log_memory(f"[Batch {i // max_concurrent + 1}] Before")
            results = await asyncio.gather(*tasks, return_exceptions=True)
            log_memory(f"[Batch {i // max_concurrent + 1}] After")

            for url, res in zip(batch, results):
                if isinstance(res, Exception):
                    print(f"❌ {url} - Exception: {res}")
                    fail += 1
                elif res.success:
                    print(f"✅ {url} [Markdown size: {len(res.markdown.raw_markdown)}]")
                    success += 1
                else:
                    print(f"❌ {url} - Error: {res.error_message}")
                    fail += 1
        print(f"\nSummary: {success} succeeded, {fail} failed")
    finally:
        await crawler.close()
        log_memory("Final")
        print(f"Peak Memory Usage: {peak_memory // (1024*1024)}MB")

# ========== Main ==========
async def main():
    check_ethics()
    await crawl_single_page()

    urls = get_sitemap_urls()
    if urls:
        print(f"\nFound {len(urls)} URLs from sitemap.")
        await crawl_sequential(urls[:5])  # Small batch
        await crawl_parallel(urls[:10], max_concurrent=3)
    else:
        print("No URLs found to crawl.")

if __name__ == "__main__":
    asyncio.run(main())

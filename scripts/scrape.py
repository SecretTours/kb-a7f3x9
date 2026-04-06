#!/usr/bin/env python3
"""
Scrapes secretfoodtours.com, filters out blog pages and third-party tours,
and generates a clean static site for use as a CRM knowledge source.
"""

import re
import sys
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

SITE_URL = "https://www.secretfoodtours.com"
SITEMAP_URL = f"{SITE_URL}/sitemap.xml"
OUTPUT_DIR = Path("site")
EXCLUDED_PATH_PATTERNS = ["/blog", "/world-tours"]
THIRD_PARTY_MARKERS = ["iframe-body", "fareharbor", "rezdy", "classpop"]
REQUEST_TIMEOUT = 10
MAX_WORKERS = 10


def fetch_sitemap_urls(sitemap_url: str) -> list[str]:
    print(f"Fetching sitemap: {sitemap_url}", flush=True)
    resp = requests.get(sitemap_url, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()

    root = ET.fromstring(resp.content)
    ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}

    sitemap_refs = root.findall("ns:sitemap/ns:loc", ns)
    if sitemap_refs:
        urls = []
        for ref in sitemap_refs:
            urls.extend(fetch_sitemap_urls(ref.text.strip()))
        return urls

    urls = [loc.text.strip() for loc in root.findall("ns:url/ns:loc", ns)]
    # Sitemap may use web.secrettours.com which doesn't resolve externally
    urls = [u.replace("https://web.secrettours.com", SITE_URL) for u in urls]
    return urls


def is_excluded_path(url: str) -> bool:
    path = urlparse(url).path.lower()
    return any(path.startswith(pattern) for pattern in EXCLUDED_PATH_PATTERNS)


def is_third_party_tour(html: str) -> bool:
    html_lower = html.lower()
    return any(marker in html_lower for marker in THIRD_PARTY_MARKERS)


def extract_main_content(html: str, url: str) -> dict | None:
    soup = BeautifulSoup(html, "html.parser")

    for tag in soup.find_all(["nav", "footer", "script", "style", "noscript", "iframe"]):
        tag.decompose()
    for tag in soup.find_all(class_=re.compile(r"cookie|popup|modal|newsletter|whatsapp", re.I)):
        tag.decompose()

    title_tag = soup.find("title")
    title = title_tag.get_text(strip=True) if title_tag else url

    meta_desc = soup.find("meta", attrs={"name": "description"})
    description = meta_desc["content"] if meta_desc and meta_desc.get("content") else ""

    main = soup.find("main") or soup.find("article") or soup.find(id="__next")
    if not main:
        main = soup.find("body")
    if not main:
        return None

    text = main.get_text(separator="\n", strip=True)
    text = re.sub(r"\n{3,}", "\n\n", text)

    if len(text.strip()) < 50:
        return None

    return {"title": title, "description": description, "text": text, "url": url}


def url_to_filepath(url: str) -> Path:
    path = urlparse(url).path.strip("/")
    if not path:
        return Path("index.html")
    return Path(path) / "index.html"


def generate_page_html(content: dict) -> str:
    escaped_title = content["title"].replace("<", "&lt;").replace(">", "&gt;")
    escaped_desc = content["description"].replace("<", "&lt;").replace(">", "&gt;")
    paragraphs = "\n".join(
        f"<p>{line}</p>" for line in content["text"].split("\n") if line.strip()
    )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="robots" content="noindex, nofollow">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{escaped_title}</title>
    <meta name="description" content="{escaped_desc}">
    <style>
        body {{ font-family: sans-serif; max-width: 800px; margin: 0 auto; padding: 20px; }}
        h1 {{ color: #333; }}
        a {{ color: #c49959; }}
    </style>
</head>
<body>
    <h1>{escaped_title}</h1>
    {paragraphs}
    <hr>
    <p><small>Source: <a href="{content['url']}">{content['url']}</a></small></p>
</body>
</html>"""


def generate_index(pages: list[dict]) -> str:
    links = "\n".join(
        f'<li><a href="/{url_to_filepath(p["url"]).parent}/">{p["title"]}</a></li>'
        for p in sorted(pages, key=lambda x: x["url"])
    )
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="robots" content="noindex, nofollow">
    <title>Secret Food Tours - Knowledge Base</title>
    <style>
        body {{ font-family: sans-serif; max-width: 800px; margin: 0 auto; padding: 20px; }}
        a {{ color: #c49959; }}
    </style>
</head>
<body>
    <h1>Secret Food Tours - Knowledge Base</h1>
    <p>Pages: {len(pages)}</p>
    <ul>
        {links}
    </ul>
</body>
</html>"""


def scrape_url(url: str) -> dict:
    """Scrape a single URL. Returns a result dict."""
    try:
        resp = requests.get(url, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        html = resp.text

        if is_third_party_tour(html):
            return {"url": url, "status": "third_party"}

        content = extract_main_content(html, url)
        if not content:
            return {"url": url, "status": "no_content"}

        return {"url": url, "status": "ok", "content": content}

    except Exception as e:
        return {"url": url, "status": "error", "error": str(e)}


def main():
    all_urls = fetch_sitemap_urls(SITEMAP_URL)
    print(f"Found {len(all_urls)} URLs in sitemap", flush=True)

    urls = [u for u in all_urls if not is_excluded_path(u)]
    print(f"After excluding blog paths: {len(urls)} URLs", flush=True)

    import shutil
    if OUTPUT_DIR.exists():
        shutil.rmtree(OUTPUT_DIR)
    OUTPUT_DIR.mkdir(parents=True)

    (OUTPUT_DIR / "robots.txt").write_text("User-agent: *\nDisallow: /\n")

    pages = []
    skipped_third_party = 0
    skipped_no_content = 0
    errors = 0

    print(f"Scraping with {MAX_WORKERS} parallel workers...", flush=True)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(scrape_url, url): url for url in urls}
        done = 0
        total = len(urls)

        for future in as_completed(futures):
            done += 1
            result = future.result()
            url = result["url"]

            if result["status"] == "ok":
                content = result["content"]
                filepath = OUTPUT_DIR / url_to_filepath(url)
                filepath.parent.mkdir(parents=True, exist_ok=True)
                filepath.write_text(generate_page_html(content))
                pages.append(content)
                print(f"[{done}/{total}] OK: {url}", flush=True)
            elif result["status"] == "third_party":
                skipped_third_party += 1
                print(f"[{done}/{total}] SKIP (third-party): {url}", flush=True)
            elif result["status"] == "no_content":
                skipped_no_content += 1
                print(f"[{done}/{total}] SKIP (no content): {url}", flush=True)
            else:
                errors += 1
                print(f"[{done}/{total}] ERROR: {url} - {result['error']}", flush=True)

    (OUTPUT_DIR / "index.html").write_text(generate_index(pages))

    print(f"\n{'='*50}", flush=True)
    print(f"Scraped: {len(pages)} pages", flush=True)
    print(f"Skipped (third-party): {skipped_third_party}", flush=True)
    print(f"Skipped (no content): {skipped_no_content}", flush=True)
    print(f"Errors: {errors}", flush=True)
    print(f"Output: {OUTPUT_DIR}/", flush=True)

    if errors > len(urls) * 0.5:
        print("ERROR: Too many failures, something is wrong", flush=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

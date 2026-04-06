#!/usr/bin/env python3
"""
Scrapes secretfoodtours.com using Firecrawl, filters out blog pages and
third-party tours, and generates a clean static site for CRM knowledge source.
"""

import json
import os
import re
import subprocess
import sys
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urlparse

SITE_URL = "https://www.secretfoodtours.com"
SITEMAP_URL = f"{SITE_URL}/sitemap.xml"
OUTPUT_DIR = Path("site")
EXCLUDED_PATH_PATTERNS = ["/blog", "/world-tours"]
THIRD_PARTY_MARKERS = ["iframe-body", "fareharbor", "rezdy", "classpop"]
BASE_PATH = os.environ.get("BASE_PATH", "/kb-a7f3x9")
MAX_WORKERS = 10

# Lines containing these patterns are noise and should be removed
NOISE_PATTERNS = [
    "Over 100,000 5 Star Reviews",
    "Also Recommended By",
    "Your browser does not support",
    "SCROLL DOWN",
    "Book Now Learn More",
    "Our Cultural Tours",
    "Book your Tour",
    "BOOK YOUR TOUR",
    "Our Top Recommendations For You",
    "Book Now to Save on These Amazing Tours",
    "Search Locations",
    "Start typing destination",
    "PRIVATE TOURS",
    "GIFT CARDS",
    "ABOUT US",
    "CORPORATE TOURS",
    "DESTINATIONS",
    "Historical Tours",
    "PRIVATE\\",
    "Food Tour Drink Upgrade",
    "Cooking Classes:",
    "Food Tours:",
    "Upgrades:",
    "Upgrades",
    "En Español",
    "Faq Contact",
]
FOOTER_START_MARKERS = [
    "Now In Over",
    "## Join Our Newsletter",
    "Secret Food Tours is a registered",
]


def fetch_sitemap_urls(sitemap_url: str) -> list[str]:
    import urllib.request
    print(f"Fetching sitemap: {sitemap_url}", flush=True)
    with urllib.request.urlopen(sitemap_url, timeout=15) as resp:
        content = resp.read()

    root = ET.fromstring(content)
    ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}

    sitemap_refs = root.findall("ns:sitemap/ns:loc", ns)
    if sitemap_refs:
        urls = []
        for ref in sitemap_refs:
            urls.extend(fetch_sitemap_urls(ref.text.strip()))
        return urls

    urls = [loc.text.strip() for loc in root.findall("ns:url/ns:loc", ns)]
    urls = [u.replace("https://web.secrettours.com", SITE_URL) for u in urls]
    return urls


def is_excluded_path(url: str) -> bool:
    path = urlparse(url).path.lower()
    return any(path.startswith(pattern) for pattern in EXCLUDED_PATH_PATTERNS)


def clean_markdown(md: str) -> str:
    """Strip navigation, footer, images, and other noise from markdown."""
    lines = md.split("\n")

    # Find where footer starts
    content_end = len(lines)
    for i, line in enumerate(lines):
        if any(marker in line for marker in FOOTER_START_MARKERS):
            content_end = i
            break

    # Find where nav ends — look for the last continent link, then find
    # the first real heading after that
    nav_end = 0
    for i, line in enumerate(lines[:content_end]):
        if "South America" in line or "North America" in line:
            nav_end = i
    content_start = nav_end
    for i, line in enumerate(lines[nav_end:content_end]):
        if re.match(r"^#{1,3}\s+.+", line) and not any(p in line for p in NOISE_PATTERNS):
            content_start = nav_end + i
            break

    lines = lines[content_start:content_end]
    cleaned = []
    for line in lines:
        stripped = line.strip()
        # Skip noise lines
        if any(pattern in stripped for pattern in NOISE_PATTERNS):
            continue
        # Skip image-only lines
        if re.match(r"^\s*!\[.*?\]\(.*?\)\s*$", line):
            continue
        # Skip lines that are just links to images
        if re.match(r"^\s*\[!\[.*?\]\(.*?\)\]\(.*?\)\s*$", line):
            continue
        # Remove inline images but keep surrounding text
        line = re.sub(r"!\[.*?\]\(.*?\)", "", line)
        # Convert markdown links to just the text
        line = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", line)
        # Skip junk
        stripped = line.strip()
        if stripped in ("×", "\\", "", "Home", "(5)", "Book Now", "Learn More",
                        "Book Now Learn More"):
            continue
        cleaned.append(line)

    text = "\n".join(cleaned)
    # Collapse multiple blank lines
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def scrape_with_firecrawl(url: str) -> dict:
    """Scrape a URL using firecrawl CLI and return result."""
    try:
        result = subprocess.run(
            ["firecrawl", "scrape", url, "--format", "markdown,rawHtml"],
            capture_output=True, text=True, timeout=30
        )
        if result.returncode != 0:
            return {"url": url, "status": "error", "error": result.stderr[:200]}

        data = json.loads(result.stdout)
        raw_html = data.get("rawHtml", "")
        markdown = data.get("markdown", "")

        # Check for third-party markers in raw HTML
        html_lower = raw_html.lower()
        if any(marker in html_lower for marker in THIRD_PARTY_MARKERS):
            return {"url": url, "status": "third_party"}

        # Clean the markdown
        cleaned = clean_markdown(markdown)
        if len(cleaned) < 50:
            return {"url": url, "status": "no_content"}

        # Extract title from markdown (first heading)
        title_match = re.search(r"^#+ (.+)$", cleaned, re.MULTILINE)
        title = title_match.group(1) if title_match else urlparse(url).path.strip("/")

        return {
            "url": url,
            "status": "ok",
            "content": {"title": title, "text": cleaned, "url": url},
        }

    except subprocess.TimeoutExpired:
        return {"url": url, "status": "error", "error": "timeout"}
    except Exception as e:
        return {"url": url, "status": "error", "error": str(e)}


def url_to_filepath(url: str) -> Path:
    path = urlparse(url).path.strip("/")
    if not path:
        return Path("index.html")
    return Path(path) / "index.html"


def generate_page_html(content: dict) -> str:
    escaped_title = content["title"].replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")
    # Convert markdown to simple HTML
    text_html = content["text"]
    # Convert headers
    text_html = re.sub(r"^### (.+)$", r"<h3>\1</h3>", text_html, flags=re.MULTILINE)
    text_html = re.sub(r"^## (.+)$", r"<h2>\1</h2>", text_html, flags=re.MULTILINE)
    text_html = re.sub(r"^# (.+)$", r"<h1>\1</h1>", text_html, flags=re.MULTILINE)
    # Convert bold
    text_html = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", text_html)
    # Wrap remaining lines in paragraphs
    lines = text_html.split("\n")
    processed = []
    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("<h") or stripped.startswith("<ul") or stripped.startswith("<li"):
            processed.append(stripped)
        else:
            processed.append(f"<p>{stripped}</p>")
    body = "\n".join(processed)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="robots" content="noindex, nofollow">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{escaped_title}</title>
    <style>
        body {{ font-family: sans-serif; max-width: 800px; margin: 0 auto; padding: 20px; line-height: 1.6; }}
        h1, h2, h3 {{ color: #333; }}
        a {{ color: #c49959; }}
    </style>
</head>
<body>
    {body}
    <hr>
    <p><small>Source: <a href="{content['url']}">{content['url']}</a></small></p>
</body>
</html>"""


def generate_index(pages: list[dict]) -> str:
    links = "\n".join(
        f'<li><a href="{BASE_PATH}/{url_to_filepath(p["url"]).parent}/">{p["title"]}</a></li>'
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


def main():
    # Check firecrawl is available
    try:
        subprocess.run(["firecrawl", "--version"], capture_output=True, check=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("ERROR: firecrawl CLI not found. Install with: npm install -g firecrawl-cli", flush=True)
        sys.exit(1)

    all_urls = fetch_sitemap_urls(SITEMAP_URL)
    print(f"Found {len(all_urls)} URLs in sitemap", flush=True)

    urls = [u for u in all_urls if not is_excluded_path(u)]
    print(f"After excluding paths: {len(urls)} URLs", flush=True)

    import shutil
    if OUTPUT_DIR.exists():
        shutil.rmtree(OUTPUT_DIR)
    OUTPUT_DIR.mkdir(parents=True)

    (OUTPUT_DIR / "robots.txt").write_text("User-agent: *\nDisallow: /\n")

    pages = []
    skipped_third_party = 0
    skipped_no_content = 0
    errors = 0

    print(f"Scraping with {MAX_WORKERS} parallel workers via Firecrawl...", flush=True)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(scrape_with_firecrawl, url): url for url in urls}
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
                print(f"[{done}/{total}] ERROR: {url} - {result.get('error', 'unknown')}", flush=True)

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

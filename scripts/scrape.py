#!/usr/bin/env python3
"""
Scrapes secretfoodtours.com, filters out blog pages and third-party tours,
and generates a clean static site for use as a CRM knowledge source.
"""

import os
import re
import sys
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

SITE_URL = "https://www.secretfoodtours.com"
SITEMAP_URL = f"{SITE_URL}/sitemap.xml"
OUTPUT_DIR = Path("site")
EXCLUDED_PATH_PATTERNS = ["/blog"]
THIRD_PARTY_MARKERS = ["iframe-body", "fareharbor", "rezdy", "classpop"]
REQUEST_DELAY = 1  # seconds between requests to be polite
REQUEST_TIMEOUT = 30


def fetch_sitemap_urls(sitemap_url: str) -> list[str]:
    """Fetch and parse all URLs from the sitemap."""
    print(f"Fetching sitemap: {sitemap_url}")
    resp = requests.get(sitemap_url, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()

    root = ET.fromstring(resp.content)
    ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}

    # Handle sitemap index (references to other sitemaps)
    sitemap_refs = root.findall("ns:sitemap/ns:loc", ns)
    if sitemap_refs:
        urls = []
        for ref in sitemap_refs:
            urls.extend(fetch_sitemap_urls(ref.text.strip()))
        return urls

    return [loc.text.strip() for loc in root.findall("ns:url/ns:loc", ns)]


def is_excluded_path(url: str) -> bool:
    """Check if URL matches an excluded path pattern."""
    path = urlparse(url).path.lower()
    return any(path.startswith(pattern) for pattern in EXCLUDED_PATH_PATTERNS)


def is_third_party_tour(html: str) -> bool:
    """Check if page HTML contains third-party tour markers."""
    html_lower = html.lower()
    return any(marker in html_lower for marker in THIRD_PARTY_MARKERS)


def extract_main_content(html: str, url: str) -> dict | None:
    """Extract the main content from a page, stripping navigation, footer, etc."""
    soup = BeautifulSoup(html, "html.parser")

    # Remove unwanted elements
    for tag in soup.find_all(["nav", "footer", "script", "style", "noscript", "iframe"]):
        tag.decompose()
    for tag in soup.find_all(class_=re.compile(r"cookie|popup|modal|newsletter|whatsapp", re.I)):
        tag.decompose()

    # Get title
    title_tag = soup.find("title")
    title = title_tag.get_text(strip=True) if title_tag else url

    # Get meta description
    meta_desc = soup.find("meta", attrs={"name": "description"})
    description = meta_desc["content"] if meta_desc and meta_desc.get("content") else ""

    # Try to find main content area
    main = soup.find("main") or soup.find("article") or soup.find(id="__next")
    if not main:
        main = soup.find("body")
    if not main:
        return None

    # Extract text content, preserving some structure
    text = main.get_text(separator="\n", strip=True)
    # Clean up excessive blank lines
    text = re.sub(r"\n{3,}", "\n\n", text)

    if len(text.strip()) < 50:
        return None

    return {"title": title, "description": description, "text": text, "url": url}


def url_to_filepath(url: str) -> Path:
    """Convert a URL to a file path for the static site."""
    path = urlparse(url).path.strip("/")
    if not path:
        return Path("index.html")
    return Path(path) / "index.html"


def generate_page_html(content: dict) -> str:
    """Generate a clean HTML page from extracted content."""
    escaped_title = content["title"].replace("<", "&lt;").replace(">", "&gt;")
    escaped_desc = content["description"].replace("<", "&lt;").replace(">", "&gt;")
    # Convert newlines to paragraphs
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
    """Generate an index page linking to all scraped pages."""
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


def main():
    # Fetch all URLs from sitemap
    all_urls = fetch_sitemap_urls(SITEMAP_URL)
    print(f"Found {len(all_urls)} URLs in sitemap")

    # Filter out excluded paths
    urls = [u for u in all_urls if not is_excluded_path(u)]
    print(f"After excluding blog paths: {len(urls)} URLs")

    # Clean output directory
    if OUTPUT_DIR.exists():
        import shutil
        shutil.rmtree(OUTPUT_DIR)
    OUTPUT_DIR.mkdir(parents=True)

    # Write robots.txt
    (OUTPUT_DIR / "robots.txt").write_text("User-agent: *\nDisallow: /\n")

    pages = []
    skipped_third_party = 0
    skipped_no_content = 0
    errors = 0

    for i, url in enumerate(urls):
        print(f"[{i+1}/{len(urls)}] {url}")
        try:
            resp = requests.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            html = resp.text

            if is_third_party_tour(html):
                print(f"  -> SKIPPED (third-party tour)")
                skipped_third_party += 1
                time.sleep(REQUEST_DELAY)
                continue

            content = extract_main_content(html, url)
            if not content:
                print(f"  -> SKIPPED (no meaningful content)")
                skipped_no_content += 1
                time.sleep(REQUEST_DELAY)
                continue

            # Write the page
            filepath = OUTPUT_DIR / url_to_filepath(url)
            filepath.parent.mkdir(parents=True, exist_ok=True)
            filepath.write_text(generate_page_html(content))
            pages.append(content)
            print(f"  -> OK ({len(content['text'])} chars)")

        except Exception as e:
            print(f"  -> ERROR: {e}")
            errors += 1

        time.sleep(REQUEST_DELAY)

    # Write index page
    (OUTPUT_DIR / "index.html").write_text(generate_index(pages))

    # Summary
    print(f"\n{'='*50}")
    print(f"Scraped: {len(pages)} pages")
    print(f"Skipped (third-party): {skipped_third_party}")
    print(f"Skipped (no content): {skipped_no_content}")
    print(f"Errors: {errors}")
    print(f"Output: {OUTPUT_DIR}/")

    if errors > len(urls) * 0.5:
        print("ERROR: Too many failures, something is wrong")
        sys.exit(1)


if __name__ == "__main__":
    main()

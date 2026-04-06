#!/usr/bin/env python3
"""
Scrapes secretfoodtours.com using Firecrawl, filters out blog pages and
third-party tours, and generates a mirror site structure for CRM knowledge source.
Mirrors the original site: index -> city pages -> individual tour pages.
"""

import json
import os
import re
import subprocess
import sys
import xml.etree.ElementTree as ET
from collections import defaultdict
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

PAGE_STYLE = """
    body { font-family: sans-serif; max-width: 900px; margin: 0 auto; padding: 20px; line-height: 1.6; }
    h1 { color: #333; border-bottom: 2px solid #c49959; padding-bottom: 10px; }
    h2 { color: #c49959; margin-top: 30px; }
    h3 { color: #555; }
    a { color: #c49959; }
    .tour-list { list-style: none; padding: 0; }
    .tour-list li { padding: 8px 0; border-bottom: 1px solid #eee; }
    .city-list { list-style: none; padding: 0; display: flex; flex-wrap: wrap; gap: 10px; }
    .city-list li { background: #f5f5f5; padding: 8px 16px; border-radius: 4px; }
    small { color: #999; }
    hr { border: none; border-top: 1px solid #eee; margin: 20px 0; }
"""


def fetch_sitemap_urls(sitemap_url: str) -> list[str]:
    import urllib.request
    print(f"Fetching sitemap: {sitemap_url}", flush=True)
    req = urllib.request.Request(sitemap_url, headers={
        "User-Agent": "Mozilla/5.0 (compatible; KnowledgeBaseBot/1.0)"
    })
    with urllib.request.urlopen(req, timeout=15) as resp:
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


def get_url_parts(url: str) -> list[str]:
    """Get path parts from URL. e.g. /paris/le-marais/ -> ['paris', 'le-marais']"""
    return [p for p in urlparse(url).path.strip("/").split("/") if p]


def clean_markdown(md: str) -> str:
    """Strip navigation, footer, images, and other noise from markdown."""
    lines = md.split("\n")

    content_end = len(lines)
    for i, line in enumerate(lines):
        if any(marker in line for marker in FOOTER_START_MARKERS):
            content_end = i
            break

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
        if any(pattern in stripped for pattern in NOISE_PATTERNS):
            continue
        if re.match(r"^\s*!\[.*?\]\(.*?\)\s*$", line):
            continue
        if re.match(r"^\s*\[!\[.*?\]\(.*?\)\]\(.*?\)\s*$", line):
            continue
        line = re.sub(r"!\[.*?\]\(.*?\)", "", line)
        # Rewrite internal links to mirror site, strip external links
        def rewrite_link(m):
            text = m.group(1)
            href = m.group(2)
            if SITE_URL in href or href.startswith("/"):
                # Internal link — rewrite to mirror
                path = href.replace(SITE_URL, "")
                if not path.startswith("/"):
                    path = "/" + path
                return f'[{text}]({BASE_PATH}{path})'
            # External link — keep text only
            return text
        line = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", rewrite_link, line)
        stripped = line.strip()
        if stripped in ("×", "\\", "", "Home", "(5)", "Book Now", "Learn More",
                        "Book Now Learn More"):
            continue
        cleaned.append(line)

    text = "\n".join(cleaned)
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

        html_lower = raw_html.lower()
        if any(marker in html_lower for marker in THIRD_PARTY_MARKERS):
            return {"url": url, "status": "third_party"}

        cleaned = clean_markdown(markdown)
        if len(cleaned) < 50:
            return {"url": url, "status": "no_content"}

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


def markdown_to_html(text: str) -> str:
    """Convert markdown text to simple HTML."""
    html = text
    html = re.sub(r"^#### (.+)$", r"<h4>\1</h4>", html, flags=re.MULTILINE)
    html = re.sub(r"^### (.+)$", r"<h3>\1</h3>", html, flags=re.MULTILINE)
    html = re.sub(r"^## (.+)$", r"<h2>\1</h2>", html, flags=re.MULTILINE)
    html = re.sub(r"^# (.+)$", r"<h1>\1</h1>", html, flags=re.MULTILINE)
    html = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", html)
    html = re.sub(r"\*(.+?)\*", r"<em>\1</em>", html)
    # Convert markdown links to HTML links
    html = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r'<a href="\2">\1</a>', html)

    lines = html.split("\n")
    processed = []
    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("<h"):
            processed.append(stripped)
        else:
            processed.append(f"<p>{stripped}</p>")
    return "\n".join(processed)


def escape_html(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


def wrap_page(title: str, body: str, breadcrumb: str = "") -> str:
    escaped_title = escape_html(title)
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="robots" content="noindex, nofollow">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{escaped_title}</title>
    <style>{PAGE_STYLE}</style>
</head>
<body>
    {breadcrumb}
    {body}
</body>
</html>"""


def generate_tour_page(page: dict) -> str:
    """Generate an individual tour page."""
    parts = get_url_parts(page["url"])
    city = parts[0] if parts else ""
    city_display = city.replace("-", " ").title()

    breadcrumb = (
        f'<p><a href="{BASE_PATH}/">Home</a> &gt; '
        f'<a href="{BASE_PATH}/{city}/">{city_display}</a></p>'
    )

    text = page["text"]
    # Remove first heading since we put it in <h1>
    text = re.sub(r"^#+ .+\n*", "", text, count=1).strip()
    body_html = markdown_to_html(text)

    body = f"""
    <h1>{escape_html(page['title'])}</h1>
    {body_html}
    <hr>
    <p><small>Source: <a href="{page['url']}">{page['url']}</a></small></p>
    """
    return wrap_page(page["title"], body, breadcrumb)


def generate_city_page(city: str, city_page: dict | None, tour_pages: list[dict]) -> str:
    """Generate a city page with its overview and links to tours."""
    city_display = city.replace("-", " ").title()

    breadcrumb = f'<p><a href="{BASE_PATH}/">Home</a></p>'

    sections = [f"<h1>{city_display} Food Tours</h1>"]

    # City overview content if we have a city page
    if city_page:
        text = city_page["text"]
        text = re.sub(r"^#+ .+\n*", "", text, count=1).strip()
        sections.append(markdown_to_html(text))

    # List of tours in this city
    if tour_pages:
        sections.append("<h2>Available Tours</h2>")
        sections.append('<ul class="tour-list">')
        for tp in sorted(tour_pages, key=lambda x: x["title"]):
            parts = get_url_parts(tp["url"])
            tour_slug = parts[1] if len(parts) >= 2 else ""
            link = f"{BASE_PATH}/{city}/{tour_slug}/"
            sections.append(f'<li><a href="{link}">{escape_html(tp["title"])}</a></li>')
        sections.append("</ul>")

    body = "\n".join(sections)
    return wrap_page(f"{city_display} Food Tours", body, breadcrumb)


def generate_index_page(cities: dict, general_pages: list[dict]) -> str:
    """Generate the main index page with links to all pages."""
    sections = [
        "<h1>Secret Food Tours</h1>",
        "<p>Secret Food Tours offers food tours, cooking classes, and unique culinary "
        "experiences in over 110 cities worldwide.</p>",
    ]

    # Cities with tours
    cities_with_tours = {k: v for k, v in cities.items() if v["tours"]}
    if cities_with_tours:
        sections.append("<h2>Destinations</h2>")
        sections.append('<ul class="city-list">')
        for city in sorted(cities_with_tours.keys()):
            city_display = city.replace("-", " ").title()
            count = len(cities_with_tours[city]["tours"])
            sections.append(
                f'<li><a href="{BASE_PATH}/{city}/">{city_display}</a> ({count} tours)</li>'
            )
        sections.append("</ul>")

    # All other pages (countries, continents, info pages)
    if general_pages:
        sections.append("<h2>More Information</h2>")
        sections.append('<ul class="tour-list">')
        for page in sorted(general_pages, key=lambda x: x["title"]):
            parts = get_url_parts(page["url"])
            slug = parts[0] if parts else "info"
            link = f"{BASE_PATH}/{slug}/"
            sections.append(f'<li><a href="{link}">{escape_html(page["title"])}</a></li>')
        sections.append("</ul>")

    body = "\n".join(sections)
    return wrap_page("Secret Food Tours", body)


def generate_general_page(page: dict) -> str:
    """Generate a general info page (not city/tour specific)."""
    breadcrumb = f'<p><a href="{BASE_PATH}/">Home</a></p>'

    text = page["text"]
    text = re.sub(r"^#+ .+\n*", "", text, count=1).strip()
    body_html = markdown_to_html(text)

    body = f"""
    <h1>{escape_html(page['title'])}</h1>
    {body_html}
    <hr>
    <p><small>Source: <a href="{page['url']}">{page['url']}</a></small></p>
    """
    return wrap_page(page["title"], body, breadcrumb)


def main():
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
                pages.append(result["content"])
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

    # Organize pages: group tour pages under their parent slug
    cities = defaultdict(lambda: {"city_page": None, "tours": []})
    top_level_pages = []  # All single-segment pages (cities, countries, continents, info)

    for page in pages:
        parts = get_url_parts(page["url"])
        if len(parts) == 0:
            top_level_pages.append(page)
        elif len(parts) == 1:
            top_level_pages.append(page)
            # Also register as city page if tours exist under this slug
            cities[parts[0]]["city_page"] = page
        elif len(parts) >= 2:
            city = parts[0]
            cities[city]["tours"].append(page)

    # Separate cities (have tours) from other top-level pages
    city_slugs = {slug for slug, data in cities.items() if data["tours"]}
    general_pages = [p for p in top_level_pages
                     if get_url_parts(p["url"])[0] not in city_slugs
                     if get_url_parts(p["url"])]

    print(f"\nGenerating site structure...", flush=True)
    print(f"  Cities with tours: {len(city_slugs)}", flush=True)
    print(f"  Other pages: {len(general_pages)}", flush=True)

    # Write index page — links to EVERYTHING
    (OUTPUT_DIR / "index.html").write_text(
        generate_index_page(cities, general_pages)
    )

    # Write all top-level pages (countries, continents, info, etc.)
    for page in general_pages:
        parts = get_url_parts(page["url"])
        slug = parts[0] if parts else "info"
        page_dir = OUTPUT_DIR / slug
        page_dir.mkdir(parents=True, exist_ok=True)
        (page_dir / "index.html").write_text(generate_general_page(page))

    # Write city pages and tour pages
    for city, data in cities.items():
        if not data["tours"]:
            continue  # Already written as general page above
        city_dir = OUTPUT_DIR / city
        city_dir.mkdir(parents=True, exist_ok=True)

        # City overview page
        (city_dir / "index.html").write_text(
            generate_city_page(city, data["city_page"], data["tours"])
        )

        # Individual tour pages
        for tour in data["tours"]:
            parts = get_url_parts(tour["url"])
            if len(parts) >= 2:
                tour_dir = city_dir / parts[1]
                tour_dir.mkdir(parents=True, exist_ok=True)
                (tour_dir / "index.html").write_text(generate_tour_page(tour))

    print(f"\n{'='*50}", flush=True)
    print(f"Scraped: {len(pages)} pages", flush=True)
    print(f"Skipped (third-party): {skipped_third_party}", flush=True)
    print(f"Skipped (no content): {skipped_no_content}", flush=True)
    print(f"Errors: {errors}", flush=True)
    print(f"Output: {OUTPUT_DIR}/", flush=True)
    print(f"  Index -> {len(cities)} city pages -> {sum(len(d['tours']) for d in cities.values())} tour pages", flush=True)
    print(f"  + {len(general_pages)} general info pages", flush=True)

    if errors > len(urls) * 0.5:
        print("ERROR: Too many failures, something is wrong", flush=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Scrapes secretfoodtours.com using Firecrawl, filters out blog pages and
third-party tours, and generates a mirror site structure for CRM knowledge source.
Mirrors the original site: index -> city pages -> individual tour pages.

Integrates with TicketingHub Supplier API to add real pricing and availability
data to each tour page.
"""

import json
import os
import re
import subprocess
import sys
import xml.etree.ElementTree as ET
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlparse, quote

import urllib.request

SITE_URL = "https://www.secretfoodtours.com"
SITEMAP_URL = f"{SITE_URL}/sitemap.xml"
OUTPUT_DIR = Path("site")
EXCLUDED_PATH_PATTERNS = ["/blog", "/world-tours"]
THIRD_PARTY_MARKERS = ["iframe-body", "fareharbor", "rezdy", "classpop"]
BASE_PATH = os.environ.get("BASE_PATH", "/kb-a7f3x9")
MAX_WORKERS = 5  # Keep low to avoid Firecrawl rate limits

# TicketingHub Supplier API
TH_API_BASE = "https://api.ticketinghub.com/supplier/v1"
TH_TOKEN = os.environ.get("TH_TOKEN", "at~qqj0cWVTnNV-vG6hb7Bpzw")
TH_AVAIL_DAYS = 60  # How many days of availability to fetch (2 months)

############################
# TicketingHub Integration #
############################

def th_api_get(endpoint: str):
    """Make a GET request to the TicketingHub Supplier API via curl."""
    url = f"{TH_API_BASE}/{endpoint}"
    try:
        result = subprocess.run(
            ["curl", "-s", "--compressed", "-H", f"Authorization: Bearer {TH_TOKEN}", url],
            capture_output=True, text=True, timeout=30
        )
        if result.returncode != 0:
            print(f"  TH API curl error ({endpoint}): {result.stderr[:200]}", flush=True)
            return None
        return json.loads(result.stdout)
    except Exception as e:
        print(f"  TH API error ({endpoint}): {e}", flush=True)
        return None


def th_fetch_all_data() -> dict:
    """Fetch all products, tiers, and availability from TicketingHub.

    Returns a dict keyed by product ID with structure:
    {
        "name": str,
        "currency": str,
        "tiers": [{"name": str, "price": str, "tier_type": str}, ...],
        "availability": {date_str: {"status": "OPEN"|"CLOSED", "times": {time: {booked, capacity}}, ...}},
    }
    """
    print("Fetching TicketingHub data...", flush=True)

    # Paginate through all products (API returns max 400 per request)
    products = []
    offset = 0
    while True:
        if offset == 0:
            batch = th_api_get("products?limit=400")
        else:
            batch = th_api_get(f"products?limit=400&offset={offset}")
        if not batch or not isinstance(batch, list):
            if offset == 0:
                print("  WARNING: Could not fetch TH products", flush=True)
                return {}
            break
        products.extend(batch)
        if len(batch) < 400:
            break
        offset += 400

    active = [p for p in products if isinstance(p, dict) and p.get("deleted_at") is None]
    print(f"  Found {len(active)} active products (from {len(products)} total)", flush=True)

    result = {}
    for p in active:
        pid = p["id"]
        result[pid] = {
            "name": p["name"],
            "short_name": p.get("short_name", ""),
            "currency": p["currency"],
            "description": p.get("description", ""),
            "time_zone": p.get("time_zone", ""),
            "tiers": [],
            "availability": {},
        }

        # Fetch tiers (pricing)
        tiers = th_api_get(f"products/{pid}/tiers")
        if tiers:
            result[pid]["tiers"] = [
                {"name": t["name"], "price": t["price"], "tier_type": t.get("tier_type", "")}
                for t in tiers
            ]

    # Fetch availability in 30-day chunks (API max is 31 days per request)
    today = datetime.now()
    all_avail_data = {}
    days_remaining = TH_AVAIL_DAYS
    chunk_start = today
    while days_remaining > 0:
        chunk_days = min(days_remaining, 30)
        chunk_end = chunk_start + timedelta(days=chunk_days)
        from_str = chunk_start.strftime("%Y-%m-%d")
        to_str = chunk_end.strftime("%Y-%m-%d")
        chunk_data = th_api_get(f"availability?from={from_str}&to={to_str}")
        if chunk_data:
            all_avail_data.update(chunk_data)
        chunk_start = chunk_end + timedelta(days=1)
        days_remaining -= chunk_days + 1

    avail_data = all_avail_data
    if avail_data:
        for date_str, date_entries in avail_data.items():
            for pid, info in date_entries.items():
                if pid not in result:
                    continue
                times = info.get("times", {})
                parsed_times = {}
                total_cap = 0
                total_booked = 0
                for time_str, slot_val in times.items():
                    if isinstance(slot_val, str) and "/" in slot_val:
                        booked, cap = slot_val.split("/")
                        booked, cap = int(booked), int(cap)
                    else:
                        booked, cap = 0, int(slot_val) if slot_val else 0
                    parsed_times[time_str] = {"booked": booked, "capacity": cap}
                    total_cap += cap
                    total_booked += booked

                status = "OPEN" if total_cap > 0 else "CLOSED"
                remaining = total_cap - total_booked
                result[pid]["availability"][date_str] = {
                    "status": status,
                    "remaining": remaining,
                    "capacity": total_cap,
                    "times": parsed_times,
                }

    products_with_avail = sum(1 for v in result.values() if v["availability"])
    print(f"  Fetched tiers for {len(result)} products, availability for {products_with_avail}", flush=True)
    return result


def _normalize(s: str) -> str:
    """Lowercase, strip accents/punctuation, collapse whitespace for matching."""
    s = s.lower()
    # Common accent replacements
    for src, dst in [("á","a"),("é","e"),("í","i"),("ó","o"),("ú","u"),("ñ","n"),("ü","u")]:
        s = s.replace(src, dst)
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    return " ".join(s.split())


# City name mapping: URL slug -> possible TH product city names
CITY_ALIASES = {
    "paris": ["paris", "paris"],
    "new-york": ["new york", "nueva york", "nyc"],
    "mexico-city": ["mexico city", "ciudad de mexico"],
    "miami": ["miami"],
    "rome": ["rome", "roma"],
    "london": ["london"],
    "barcelona": ["barcelona"],
    "madrid": ["madrid"],
    "bilbao": ["bilbao"],
    "lisbon": ["lisbon", "lisboa"],
    "porto": ["porto"],
    "prague": ["prague", "praha"],
    "budapest": ["budapest"],
    "vienna": ["vienna", "wien"],
    "warsaw": ["warsaw", "varsovia"],
    "zurich": ["zurich"],
    "nice": ["nice", "niza"],
    "marseille": ["marseille", "marsella"],
    "lyon": ["lyon"],
    "florence": ["florence", "firenze"],
    "venice": ["venice", "venecia"],
    "naples": ["naples", "napoles"],
    "milan": ["milan"],
    "seville": ["seville", "sevilla"],
    "san-sebastian": ["san sebastian"],
    "valencia": ["valencia"],
    "palma-de-mallorca": ["palma", "mallorca"],
    "palermo": ["palermo"],
    "modena": ["modena"],
    "parma": ["parma"],
    "sorrento": ["sorrento"],
    "bologna": ["bologna"],
    "tokyo": ["tokyo"],
    "osaka": ["osaka"],
    "seoul": ["seoul"],
    "taipei": ["taipei"],
    "bangkok": ["bangkok"],
    "singapore": ["singapore"],
    "ho-chi-minh": ["saigon", "ho chi minh"],
    "phuket": ["phuket"],
    "sydney": ["sydney"],
    "melbourne": ["melbourne"],
    "stockholm": ["stockholm"],
    "oslo": ["oslo"],
    "reykjavik": ["reykjavik"],
    "munich": ["munich", "munchen"],
    "berlin": ["berlin"],
    "cartagena": ["cartagena"],
    "medellin": ["medellin"],
    "cusco": ["cusco"],
    "lima": ["lima"],
    "louisville": ["louisville"],
    "atlanta": ["atlanta"],
    "denver": ["denver"],
    "scottsdale": ["scottsdale"],
    "tucson": ["tucson"],
    "phoenix": ["phoenix"],
    "las-vegas": ["las vegas"],
    "san-francisco": ["san francisco"],
    "los-angeles": ["los angeles"],
    "san-diego": ["san diego"],
    "chicago": ["chicago"],
    "toronto": ["toronto"],
    "vancouver": ["vancouver"],
    "malaga": ["malaga"],
}

# Keywords that help distinguish tour types
TOUR_TYPE_KEYWORDS = {
    "chocolate": ["chocolate", "chocolateria", "pastry", "pastries", "pasteleria"],
    "express": ["express", "expres"],
    "drink": ["drink", "upgrade", "bebida"],
    "cooking": ["cooking", "class", "making", "noodle", "dumpling", "pasta", "tea"],
    "montmartre": ["montmartre"],
    "le-marais": ["marais"],
    "notre-dame": ["notre dame", "notre-dame"],
    "saint-germain": ["saint germain", "st. germain", "st germain"],
    "trastevere": ["trastevere"],
    "bbq": ["bbq"],
    "evening": ["evening"],
    "private": ["private"],
}


def match_tour_to_product(tour_url: str, tour_title: str, th_data: dict):
    """Try to match a website tour page to a TicketingHub product ID.

    Returns the best matching product ID, or None if no match found.
    Uses the URL slug as primary matching signal since page titles can be generic.
    """
    parts = get_url_parts(tour_url)
    if len(parts) < 2:
        return None

    city_slug = parts[0]
    tour_slug = parts[1]

    # Get city aliases for matching
    city_names = CITY_ALIASES.get(city_slug, [city_slug.replace("-", " ")])

    # Normalize tour slug (primary signal) and title (secondary)
    norm_slug = _normalize(tour_slug.replace("-", " "))
    norm_title = _normalize(tour_title)

    # Combine slug + title for keyword matching (slug is more reliable)
    combined = norm_slug + " " + norm_title

    # Detect tour type from URL slug and title
    tour_types = set()
    for ttype, keywords in TOUR_TYPE_KEYWORDS.items():
        if any(kw in combined for kw in keywords):
            tour_types.add(ttype)

    is_private = "private" in combined

    # Specific neighborhood/location keywords from the slug
    slug_words = set(norm_slug.split())

    best_match = None
    best_score = 0

    for pid, pdata in th_data.items():
        pname = _normalize(pdata["name"])

        # Must match city
        if not any(cn in pname for cn in city_names):
            continue

        score = 1  # Base score for city match

        # Private matching
        p_is_private = "private" in pname
        if is_private == p_is_private:
            score += 2
        elif is_private != p_is_private:
            score -= 3  # Strong penalty for private mismatch

        # Neighborhood/location matching (strong signal)
        location_keywords = ["montmartre", "marais", "notre dame", "saint germain",
                           "st germain", "trastevere", "chinatown", "little italy",
                           "south beach", "little havana", "latin quarter", "obuda",
                           "le marais"]
        for loc_kw in location_keywords:
            in_slug = loc_kw.replace(" ", "") in norm_slug.replace(" ", "")
            in_product = loc_kw in pname
            if in_slug and in_product:
                score += 5  # Strong match for location
            elif in_slug and not in_product:
                score -= 3  # Slug says location X but product doesn't
            elif not in_slug and in_product:
                score -= 2  # Product has location not in slug

        # Tour type matching
        for ttype in tour_types:
            keywords = TOUR_TYPE_KEYWORDS[ttype]
            if any(kw in pname for kw in keywords):
                score += 3

        # "express" mismatch penalty
        is_express = "express" in combined
        p_is_express = "expres" in pname
        if is_express != p_is_express:
            score -= 4

        # Penalize drink upgrades unless the tour is a drink upgrade
        if "drink" not in tour_types and ("drink" in pname or "upgrade" in pname):
            score -= 5
        if "drink" in tour_types and ("drink" not in pname and "upgrade" not in pname):
            score -= 5

        # Penalize cooking classes unless matching
        if "cooking" not in tour_types and ("class" in pname or "making" in pname):
            score -= 5

        # Language matching: Spanish product names for English pages (and vice versa)
        is_spanish_page = norm_slug.endswith(" es")
        spanish_indicators = ["tour gastronomico", "pasteleria", "chocolateria",
                            "ciudad de mexico", "nueva york"]
        p_is_spanish = any(ind in pname for ind in spanish_indicators)
        if is_spanish_page and p_is_spanish:
            score += 3  # Spanish page + Spanish product = good
        elif not is_spanish_page and p_is_spanish:
            score -= 4  # English page + Spanish product = bad
        elif is_spanish_page and not p_is_spanish:
            score -= 4  # Spanish page + English product = bad

        if score > best_score:
            best_score = score
            best_match = pid

    if best_score >= 2:
        return best_match
    return None


def generate_availability_html(product: dict) -> str:
    """Generate HTML for pricing and availability of a matched TicketingHub product."""
    sections = []

    # Pricing table
    if product["tiers"]:
        sections.append('<h2>Pricing</h2>')
        sections.append('<table style="border-collapse:collapse;width:100%;max-width:400px;">')
        sections.append('<tr style="border-bottom:2px solid #c49959;">'
                       '<th style="text-align:left;padding:6px;">Ticket Type</th>'
                       '<th style="text-align:right;padding:6px;">Price</th></tr>')
        for tier in product["tiers"]:
            sections.append(
                f'<tr style="border-bottom:1px solid #eee;">'
                f'<td style="padding:6px;">{escape_html(translate_tier_name(tier["name"]))}</td>'
                f'<td style="text-align:right;padding:6px;">{escape_html(tier["price"])}</td>'
                f'</tr>'
            )
        sections.append('</table>')

    # Availability calendar — one row per time slot
    avail = product.get("availability", {})
    if avail:
        sections.append('<h2>Upcoming Availability</h2>')
        sections.append('<table style="border-collapse:collapse;width:100%;max-width:600px;">')
        sections.append('<tr style="border-bottom:2px solid #c49959;">'
                       '<th style="text-align:left;padding:6px;">Date</th>'
                       '<th style="text-align:left;padding:6px;">Time</th>'
                       '<th style="text-align:center;padding:6px;">Capacity</th>'
                       '<th style="text-align:center;padding:6px;">Booked</th>'
                       '<th style="text-align:right;padding:6px;">Spots Left</th></tr>')

        for date_str in sorted(avail.keys()):
            day = avail[date_str]
            # Filter to active time slots (capacity > 0)
            active_times = {t: info for t, info in day["times"].items()
                          if info["capacity"] > 0}

            if not active_times:
                # All slots closed — show one row
                sections.append(
                    f'<tr style="border-bottom:1px solid #eee;color:#999;">'
                    f'<td style="padding:6px;">{date_str}</td>'
                    f'<td style="padding:6px;" colspan="4">'
                    f'<span style="color:#c62828;">&#9679; Closed</span></td>'
                    f'</tr>'
                )
                continue

            first_row = True
            for t, info in sorted(active_times.items()):
                remaining = info["capacity"] - info["booked"]
                if remaining <= 0:
                    spots_html = '<strong style="color:#c62828;">Sold out</strong>'
                elif remaining <= 3:
                    spots_html = f'<strong style="color:#c62828;">{remaining} (limited!)</strong>'
                else:
                    spots_html = str(remaining)

                date_cell = date_str if first_row else ""
                sections.append(
                    f'<tr style="border-bottom:1px solid #eee;">'
                    f'<td style="padding:6px;">{date_cell}</td>'
                    f'<td style="padding:6px;">{t}</td>'
                    f'<td style="text-align:center;padding:6px;">{info["capacity"]}</td>'
                    f'<td style="text-align:center;padding:6px;">{info["booked"]}</td>'
                    f'<td style="text-align:right;padding:6px;">{spots_html}</td>'
                    f'</tr>'
                )
                first_row = False

        sections.append('</table>')
    elif product["tiers"]:
        # Has pricing but no availability data
        sections.append('<p><em>Availability for this tour is on request. '
                       'Please contact us or visit the booking page for dates.</em></p>')

    return "\n".join(sections)


# Translate Spanish tier names to English for display
TIER_NAME_TRANSLATIONS = {
    "adulto": "Adult",
    "adultos": "Adult",
    "juventud": "Youth",
    "niños": "Child",
    "niño": "Child",
    "adolescente": "Teen",
}


def translate_tier_name(name: str) -> str:
    """Translate Spanish tier names to English."""
    return TIER_NAME_TRANSLATIONS.get(name.lower().strip(), name)


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
# Sections that contain duplicate/cross-linked tour content to strip
SECTION_CUT_MARKERS = [
    "Other Secret Tours",
    "Other Secret Food Tours",
    "See Our Other",
    "Nuestros otros tours",
    "Ver nuestros otros",
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


def fetch_sitemap_urls(sitemap_url: str) -> list:
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


def get_url_parts(url: str) -> list:
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

    # Also cut at "Other Secret Tours" sections (duplicate/cross-linked content)
    for i, line in enumerate(lines[:content_end]):
        if any(marker in line for marker in SECTION_CUT_MARKERS):
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
        # For image-links like [![alt](img)](url), extract just the URL as a link
        line = re.sub(
            r'\[!\[[^\]]*\]\([^)]*\)\]\(([^)]+)\)',
            lambda m: f'[Link]({m.group(1)})' if SITE_URL in m.group(1) else '',
            line
        )
        # Remove standalone images
        line = re.sub(r"!\[.*?\]\(.*?\)", "", line)
        # Keep markdown links (will be converted to HTML links later)
        stripped = line.strip()
        if stripped in ("×", "\\", "", "Home", "(5)", "Book Now", "Learn More",
                        "Book Now Learn More"):
            continue
        cleaned.append(line)

    text = "\n".join(cleaned)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def scrape_with_firecrawl(url: str, max_retries: int = 3) -> dict:
    """Scrape a URL using firecrawl CLI and return result. Retries on rate limits."""
    import time
    for attempt in range(max_retries):
        try:
            result = subprocess.run(
                ["firecrawl", "scrape", url, "--format", "markdown,rawHtml"],
                capture_output=True, text=True, timeout=60
            )
            if result.returncode != 0:
                error_text = result.stderr[:300] + result.stdout[:300]
                if "rate limit" in error_text.lower() or "429" in error_text:
                    wait = 10 * (attempt + 1)
                    print(f"  Rate limited, waiting {wait}s (attempt {attempt+1}/{max_retries})", flush=True)
                    time.sleep(wait)
                    continue
                return {"url": url, "status": "error", "error": error_text[:200]}

            data = json.loads(result.stdout)
            raw_html = data.get("rawHtml", "")
            markdown = data.get("markdown", "")

            html_lower = raw_html.lower()
            if any(marker in html_lower for marker in THIRD_PARTY_MARKERS):
                return {"url": url, "status": "third_party"}

            # Extract all internal page links from raw markdown before cleaning
            internal_links = set(re.findall(
                r'https://www\.secretfoodtours\.com/[a-zA-Z][a-zA-Z0-9\-/]*/',
                markdown
            ))
            internal_links = {
                link for link in internal_links
                if "/_next/" not in link
                and "/uploads/" not in link
                and link.rstrip("/") != url.rstrip("/")
                and not is_excluded_path(urlparse(link).path)
            }

            cleaned = clean_markdown(markdown)
            if len(cleaned) < 50:
                return {"url": url, "status": "no_content"}

            title_match = re.search(r"^#+ (.+)$", cleaned, re.MULTILINE)
            title = title_match.group(1) if title_match else urlparse(url).path.strip("/")

            return {
                "url": url,
                "status": "ok",
                "content": {
                    "title": title,
                    "text": cleaned,
                    "url": url,
                    "internal_links": sorted(internal_links),
                },
            }

        except subprocess.TimeoutExpired:
            return {"url": url, "status": "error", "error": "timeout"}
        except Exception as e:
            return {"url": url, "status": "error", "error": str(e)}

    return {"url": url, "status": "error", "error": "max retries exceeded (rate limited)"}


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


def rewrite_urls(html: str) -> str:
    """Replace all original site URLs with mirror URLs in final HTML.

    Links with data-external="true" are preserved as-is (they point to the real site).
    """
    # Preserve external links by temporarily replacing them
    externals = []
    def save_external(m):
        externals.append(m.group(0))
        return f"__EXTERNAL_{len(externals) - 1}__"
    html = re.sub(r'<a\s+data-external="true"[^>]*>.*?</a>', save_external, html)

    # Rewrite internal links to mirror
    html = html.replace(SITE_URL + "/", BASE_PATH + "/")
    html = html.replace(SITE_URL, BASE_PATH + "/")
    # Also handle image CDN URLs — just remove them
    html = re.sub(r'https://prod\.secretfoodtours\.com/[^"\'>\s]*', '', html)

    # Restore external links
    for i, ext in enumerate(externals):
        html = html.replace(f"__EXTERNAL_{i}__", ext)

    return html


def escape_html(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


def wrap_page(title: str, body: str, breadcrumb: str = "") -> str:
    escaped_title = escape_html(title)
    page_html = f"""<!DOCTYPE html>
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
    return rewrite_urls(page_html)


def generate_links_section(links: list) -> str:
    """Generate HTML for internal links found in the page."""
    if not links:
        return ""
    items = []
    for link in links:
        path = link.replace(SITE_URL, "")
        display = path.strip("/").replace("-", " ").replace("/", " - ").title()
        items.append(f'<li><a href="{BASE_PATH}{path}">{display}</a></li>')
    return f'<h2>Related Pages</h2>\n<ul class="tour-list">\n' + "\n".join(items) + "\n</ul>"


# Generic titles that should be replaced with a URL-derived name
GENERIC_TITLES = {
    "what you'll do", "what you\u2019ll do", "lo que harás", "lo que haras",
    "about the tour", "sobre el tour", "meet the best",
    "see itinerary", "book your tour", "book now", "faq",
    "frequently asked questions", "ready to taste",
}


def derive_tour_title(page_title: str, url: str) -> str:
    """Return a proper tour title. If the scraped title is generic (e.g.
    'What you'll do'), derive one from the URL slug instead."""
    title_lower = page_title.lower().strip()
    is_generic = (
        title_lower in GENERIC_TITLES
        or any(title_lower.startswith(g) for g in GENERIC_TITLES)
        or len(page_title.strip()) < 5
    )
    if not is_generic:
        return page_title

    parts = get_url_parts(url)
    if len(parts) >= 2:
        city = parts[0].replace("-", " ").title()
        tour = parts[1].replace("-", " ").title()
        # Remove city name from tour slug if it's repeated
        if tour.lower().startswith(city.lower()):
            tour = tour[len(city):].strip(" -")
        if tour:
            return f"Secret Food Tours: {city} - {tour}"
        return f"Secret Food Tours: {city}"
    return page_title


def generate_tour_page(page: dict, th_data: dict = None) -> str:
    """Generate an individual tour page with optional TicketingHub data."""
    parts = get_url_parts(page["url"])
    city = parts[0] if parts else ""
    city_display = city.replace("-", " ").title()

    # Use a proper title instead of generic "What you'll do"
    title = derive_tour_title(page["title"], page["url"])

    breadcrumb = (
        f'<p><a href="{BASE_PATH}/">Home</a> &gt; '
        f'<a href="{BASE_PATH}/{city}/">{city_display}</a></p>'
    )

    text = page["text"]
    text = re.sub(r"^#+ .+\n*", "", text, count=1).strip()
    # Strip "Other Secret Tours" sections (duplicate/cross-linked content)
    for marker in SECTION_CUT_MARKERS:
        idx = text.find(marker)
        if idx > 0:
            text = text[:idx].strip()
    body_html = markdown_to_html(text)

    # TicketingHub: pricing, availability, and booking link
    th_html = ""
    if th_data:
        matched_pid = match_tour_to_product(page["url"], title, th_data)
        if matched_pid:
            product = th_data[matched_pid]
            th_html = generate_availability_html(product)
            url_path = "/".join(get_url_parts(page["url"]))
            print(f"    TH match: /{url_path}/ -> {product['name']}", flush=True)

    # Booking link (always add, pointing to the real tour page with #BOOKING anchor)
    booking_url = page["url"].rstrip("/") + "/#BOOKING"
    booking_html = (
        f'<hr>\n'
        f'<p style="text-align:center;margin:20px 0;">'
        f'<a data-external="true" href="{booking_url}" target="_blank" '
        f'style="background:#ff5a5f;color:white;padding:12px 32px;'
        f'text-decoration:none;border-radius:4px;font-size:16px;font-weight:bold;">'
        f'Book This Tour on secretfoodtours.com</a></p>'
    )

    body = f"""
    <h1>{escape_html(title)}</h1>
    {body_html}
    {th_html}
    {booking_html}
    """
    return wrap_page(title, body, breadcrumb)


def generate_city_page(city: str, city_page: dict, tour_pages: list) -> str:
    """Generate a city page with its overview and links to tours."""
    city_display = city.replace("-", " ").title()

    breadcrumb = f'<p><a href="{BASE_PATH}/">Home</a></p>'

    sections = [f"<h1>{city_display}</h1>"]

    # City overview content if we have a city page
    if city_page:
        text = city_page["text"]
        text = re.sub(r"^#+ .+\n*", "", text, count=1).strip()
        # Strip everything after known section markers to keep it short
        for marker in SECTION_CUT_MARKERS:
            idx = text.find(marker)
            if idx > 0:
                text = text[:idx].strip()
        sections.append(markdown_to_html(text))

    # List of tours in this city
    if tour_pages:
        sections.append("<h2>Tours</h2>")
        sections.append('<ul class="tour-list">')
        for tp in sorted(tour_pages, key=lambda x: x["title"]):
            parts = get_url_parts(tp["url"])
            tour_slug = parts[1] if len(parts) >= 2 else ""
            link = f"{BASE_PATH}/{city}/{tour_slug}/"
            sections.append(f'<li><a href="{link}">{escape_html(tp["title"])}</a></li>')
        sections.append("</ul>")

    body = "\n".join(sections)
    return wrap_page(city_display, body, breadcrumb)


def generate_index_page(cities: dict, general_pages: list) -> str:
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
    """Generate a general info page (countries, continents, info)."""
    breadcrumb = f'<p><a href="{BASE_PATH}/">Home</a></p>'

    text = page["text"]
    text = re.sub(r"^#+ .+\n*", "", text, count=1).strip()
    for marker in SECTION_CUT_MARKERS:
        idx = text.find(marker)
        if idx > 0:
            text = text[:idx].strip()
    body_html = markdown_to_html(text)

    body = f"""
    <h1>{escape_html(page['title'])}</h1>
    {body_html}
    """
    return wrap_page(page["title"], body, breadcrumb)


CACHE_FILE = Path("scraped_pages.json")


def main():
    regen_mode = "--regen" in sys.argv

    if regen_mode and CACHE_FILE.exists():
        print("Regeneration mode: using cached scraped data", flush=True)
        with open(CACHE_FILE) as f:
            pages = json.load(f)
        print(f"Loaded {len(pages)} pages from cache", flush=True)
    else:
        try:
            subprocess.run(["firecrawl", "--version"], capture_output=True, check=True)
        except (subprocess.CalledProcessError, FileNotFoundError):
            print("ERROR: firecrawl CLI not found. Install with: npm install -g firecrawl-cli", flush=True)
            sys.exit(1)

        all_urls = fetch_sitemap_urls(SITEMAP_URL)
        print(f"Found {len(all_urls)} URLs in sitemap", flush=True)

        urls = [u for u in all_urls if not is_excluded_path(u)]
        print(f"After excluding paths: {len(urls)} URLs", flush=True)

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

        # Save cache for --regen mode
        with open(CACHE_FILE, "w") as f:
            json.dump(pages, f)
        print(f"Cached {len(pages)} pages to {CACHE_FILE}", flush=True)

    # Recreate output directory
    import shutil
    if OUTPUT_DIR.exists():
        shutil.rmtree(OUTPUT_DIR)
    OUTPUT_DIR.mkdir(parents=True)
    (OUTPUT_DIR / "robots.txt").write_text("User-agent: *\nDisallow: /\n")

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
                     if get_url_parts(p["url"])
                     and get_url_parts(p["url"])[0] not in city_slugs]

    print(f"\nGenerating site structure...", flush=True)
    print(f"  Cities with tours: {len(city_slugs)}", flush=True)
    print(f"  Other pages: {len(general_pages)}", flush=True)

    # Fetch TicketingHub data for availability and pricing
    th_data = th_fetch_all_data()

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

        # Individual tour pages (with TH availability data)
        for tour in data["tours"]:
            parts = get_url_parts(tour["url"])
            if len(parts) >= 2:
                tour_dir = city_dir / parts[1]
                tour_dir.mkdir(parents=True, exist_ok=True)
                (tour_dir / "index.html").write_text(
                    generate_tour_page(tour, th_data)
                )

    print(f"\n{'='*50}", flush=True)
    print(f"Pages: {len(pages)}", flush=True)
    if not regen_mode:
        print(f"Skipped (third-party): {skipped_third_party}", flush=True)
        print(f"Skipped (no content): {skipped_no_content}", flush=True)
        print(f"Errors: {errors}", flush=True)
    print(f"Output: {OUTPUT_DIR}/", flush=True)
    print(f"  Index -> {len(cities)} city pages -> {sum(len(d['tours']) for d in cities.values())} tour pages", flush=True)
    print(f"  + {len(general_pages)} general info pages", flush=True)


if __name__ == "__main__":
    main()

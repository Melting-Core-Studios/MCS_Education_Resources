#!/usr/bin/env python3
from __future__ import annotations

import datetime as _dt
import json
import os
import random
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


# ----------------------------
# Config (env overrides)
# ----------------------------

USER_AGENT = os.getenv(
    "CATALOG_BOT_UA",
    "MCS-Education-CatalogBot/1.0 (+https://github.com/your-org/your-repo)"
)

# Batch sizes keep URLs manageable; 10–25 is generally safe for Fandom wikis.
BATCH_SIZE = int(os.getenv("CATALOG_BATCH_SIZE", "20"))

# Weekly deep build: fetch planet wikitext for both franchises by default.
STARTREK_FETCH_PLANET_WIKITEXT = os.getenv("STARTREK_FETCH_PLANET_WIKITEXT", "1").strip().lower() in (
    "1", "true", "yes", "y", "on"
)
STARWARS_FETCH_PLANET_WIKITEXT = os.getenv("STARWARS_FETCH_PLANET_WIKITEXT", "1").strip().lower() in (
    "1", "true", "yes", "y", "on"
)

# Optional: also try to map Star Wars planets via their categories.
# Wookieepedia encourages “<System> system locations” categorization, which we can exploit.
STARWARS_USE_CATEGORY_SYSTEM_HINTS = os.getenv("STARWARS_USE_CATEGORY_SYSTEM_HINTS", "1").strip().lower() in (
    "1", "true", "yes", "y", "on"
)

# 0 = no limit. Useful for debugging locally.
STARTREK_MAX_PLANETS = int(os.getenv("STARTREK_MAX_PLANETS", "0"))
STARWARS_MAX_PLANETS = int(os.getenv("STARWARS_MAX_PLANETS", "0"))

HTTP_TIMEOUT_S = int(os.getenv("CATALOG_HTTP_TIMEOUT_S", "120"))
HTTP_RETRIES = int(os.getenv("CATALOG_HTTP_RETRIES", "6"))

# Small sleep between requests to be a good citizen and reduce 429s.
THROTTLE_S = float(os.getenv("CATALOG_THROTTLE_S", "0.15"))


# ----------------------------
# HTTP + MediaWiki helpers
# ----------------------------

def _sleep_backoff(attempt: int) -> None:
    # exponential backoff + jitter (cap at 60s)
    time.sleep(min(60.0, (2 ** attempt) + random.random() * 2.0))


def http_get_json(url: str, timeout: int = HTTP_TIMEOUT_S, retries: int = HTTP_RETRIES) -> dict:
    last_err: Optional[Exception] = None

    for attempt in range(retries):
        try:
            req = urllib.request.Request(
                url,
                headers={
                    "User-Agent": USER_AGENT,
                    "Accept": "application/json",
                },
            )
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                data = resp.read().decode("utf-8", errors="replace")
            return json.loads(data)

        except urllib.error.HTTPError as e:
            # Retry on rate-limit / transient server errors
            if e.code in (429, 500, 502, 503, 504):
                last_err = e
                _sleep_backoff(attempt)
                continue
            raise

        except (TimeoutError, urllib.error.URLError) as e:
            last_err = e
            _sleep_backoff(attempt)
            continue

    raise RuntimeError(f"HTTP fetch failed after {retries} attempts: {url}") from last_err


def mw_api_url(api_base: str, params: dict) -> str:
    return f"{api_base}?{urllib.parse.urlencode(params)}"


def chunked(items: List[str], n: int) -> Iterable[List[str]]:
    for i in range(0, len(items), max(1, n)):
        yield items[i:i + n]


def mw_category_members(
    api_base: str,
    category_title: str,
    namespace: int = 0,
    limit: int = 200,
) -> List[str]:
    """
    Return all page titles in Category:<category_title> in given namespace using list=categorymembers.
    """
    titles: List[str] = []
    cmcontinue: Optional[str] = None

    while True:
        params = {
            "action": "query",
            "list": "categorymembers",
            "cmtitle": f"Category:{category_title}",
            "cmnamespace": str(namespace),
            "cmlimit": str(limit),
            "format": "json",
            "formatversion": "2",
        }
        if cmcontinue:
            params["cmcontinue"] = cmcontinue

        payload = http_get_json(mw_api_url(api_base, params))
        members = (payload.get("query") or {}).get("categorymembers") or []
        for m in members:
            t = m.get("title")
            if t:
                titles.append(t)

        cmcontinue = (payload.get("continue") or {}).get("cmcontinue")
        if not cmcontinue:
            break

        time.sleep(THROTTLE_S)

    return titles


def mw_pages_wikitext_bulk(api_base: str, titles: List[str]) -> Dict[str, str]:
    """
    Fetch wikitext for multiple titles per request (batching) to reduce request count.
    """
    out: Dict[str, str] = {}

    for batch in chunked(titles, BATCH_SIZE):
        params = {
            "action": "query",
            "prop": "revisions",
            "rvprop": "content",
            "rvslots": "main",
            "titles": "|".join(batch),
            "format": "json",
            "formatversion": "2",
        }
        payload = http_get_json(mw_api_url(api_base, params))
        pages = (payload.get("query") or {}).get("pages") or []

        for p in pages:
            title = p.get("title")
            if not title:
                continue
            revs = p.get("revisions") or []
            if not revs:
                out[title] = ""
                continue
            slots = (revs[0].get("slots") or {})
            main = slots.get("main") or {}
            out[title] = main.get("content") or ""

        time.sleep(THROTTLE_S)

    return out


def mw_pages_categories_bulk(api_base: str, titles: List[str], per_page_limit: int = 500) -> Dict[str, List[str]]:
    """
    Fetch categories for multiple pages per request.
    Returns dict: {title: ["Category:Foo", "Category:Bar", ...]}
    """
    out: Dict[str, List[str]] = {}

    for batch in chunked(titles, BATCH_SIZE):
        params = {
            "action": "query",
            "prop": "categories",
            "cllimit": str(per_page_limit),
            "titles": "|".join(batch),
            "format": "json",
            "formatversion": "2",
        }
        payload = http_get_json(mw_api_url(api_base, params))
        pages = (payload.get("query") or {}).get("pages") or []

        for p in pages:
            title = p.get("title")
            if not title:
                continue
            cats = p.get("categories") or []
            out[title] = [c.get("title") for c in cats if c.get("title")]

        time.sleep(THROTTLE_S)

    return out


# ----------------------------
# Parsing helpers
# ----------------------------

_WIKILINK_RE = re.compile(r"\[\[([^\]]+)\]\]")
_TEMPLATE_RE = re.compile(r"\{\{[^{}]*\}\}")
_REF_RE = re.compile(r"<ref[^>]*>.*?</ref>", flags=re.DOTALL | re.IGNORECASE)
_HTML_TAG_RE = re.compile(r"<[^>]+>")


def strip_wiki_markup(value: str) -> str:
    value = value.strip()

    # [[Target|Label]] -> Label ; [[Target]] -> Target
    def _repl(m: re.Match) -> str:
        inner = m.group(1)
        if "|" in inner:
            return inner.split("|", 1)[1].strip()
        return inner.strip()

    value = _WIKILINK_RE.sub(_repl, value)
    value = _TEMPLATE_RE.sub("", value)
    value = _REF_RE.sub("", value)
    value = _HTML_TAG_RE.sub("", value)
    return value.strip()


def _canon_key(name: str) -> str:
    # A conservative canonical key for matching.
    return re.sub(r"\s+", " ", name.strip().lower())


def _try_match_system_name(system_index: Dict[str, dict], raw: str) -> Optional[dict]:
    """
    Try multiple match strategies:
      - exact
      - add/remove " system"
    """
    if not raw:
        return None

    raw_clean = re.sub(r"\s+", " ", raw.strip())
    k = _canon_key(raw_clean)

    # exact
    if k in system_index:
        return system_index[k]

    # if "system" missing, try append
    if not k.endswith(" system"):
        k2 = _canon_key(raw_clean + " system")
        if k2 in system_index:
            return system_index[k2]

    # if it ends with " system", try removing
    if k.endswith(" system"):
        k3 = _canon_key(re.sub(r"\s+system$", "", raw_clean, flags=re.IGNORECASE))
        if k3 in system_index:
            return system_index[k3]

    return None


def extract_system_from_value(value: str) -> Optional[str]:
    """
    From a cleaned text value, try to extract something that looks like a system name.
    """
    if not value:
        return None

    v = strip_wiki_markup(value)
    if not v:
        return None

    # Prefer phrases containing "... system"
    m = re.search(r"([A-Za-z0-9][^,;()\n]*?\bsystem\b)", v, flags=re.IGNORECASE)
    if m:
        s = m.group(1).strip()
        # Avoid returning generic "star system" alone
        if s.lower() in ("system", "star system", "a star system"):
            return None
        return s

    return None


def extract_system_from_wikitext(wikitext: str, param_names: List[str]) -> Optional[str]:
    """
    Look for common infobox parameters like:
      | system = ...
      | star system = ...
      | location = ...
    and try to extract a system-like phrase from the value.
    """
    if not wikitext:
        return None

    # Build regex patterns for each param name
    # Example line: | system = [[Tatoo system]]
    for pn in param_names:
        rx = re.compile(rf"^\|\s*{re.escape(pn)}\s*=\s*(.+)$", flags=re.IGNORECASE)
        for line in wikitext.splitlines():
            m = rx.match(line)
            if not m:
                continue
            val = m.group(1).strip()
            sys_name = extract_system_from_value(val)
            if sys_name:
                return sys_name

    return None


def extract_system_from_categories(category_titles: List[str]) -> Optional[str]:
    """
    Star Wars: derive system from categories like "Category:Coruscant system locations".
    """
    if not category_titles:
        return None

    # Titles look like "Category:Something"
    cats = [c.split("Category:", 1)[1] if c.startswith("Category:") else c for c in category_titles]

    # Most useful pattern per Wookieepedia policy: "<System> system locations"
    for c in cats:
        m = re.match(r"^(.+?)\s+system\s+locations$", c, flags=re.IGNORECASE)
        if m:
            base = m.group(1).strip()
            if base:
                return f"{base} system"

    return None


# ----------------------------
# Catalog builders
# ----------------------------

def make_system_obj(name: str, notes: str) -> dict:
    # Minimal schema-aligned object with placeholder star and empty planets list.
    return {
        "name": name,
        "category": "Single",
        "notes": notes,
        "stars": [{"name": f"{name} primary", "type": "star"}],
        "planets": [],
    }


def build_catalog(
    *,
    franchise: str,
    api_base: str,
    systems_category: str,
    planets_category: str,
    planet_param_names: List[str],
    fetch_planet_wikitext: bool,
    max_planets: int,
    use_category_system_hints: bool,
    source_label: str,
) -> dict:
    generated_at = _dt.datetime.now(_dt.timezone.utc).replace(microsecond=0).isoformat()

    # 1) Systems
    system_titles = mw_category_members(api_base, category_title=systems_category)
    system_index: Dict[str, dict] = {}

    for t in system_titles:
        name = t.strip()
        sys_obj = make_system_obj(
            name=name,
            notes=f"Auto-generated from {source_label} category listings.",
        )
        system_index[_canon_key(name)] = sys_obj

    # Ensure a sink for planets we cannot map
    unassigned = make_system_obj(
        name="Unassigned system",
        notes="Catch-all for planets whose star system could not be reliably derived from source pages.",
    )
    system_index[_canon_key(unassigned["name"])] = unassigned

    # 2) Planets list
    planet_titles = mw_category_members(api_base, category_title=planets_category)
    if max_planets > 0:
        planet_titles = planet_titles[:max_planets]

    planet_summary = {
        "enabled": True,
        "planetPages": len(planet_titles),
        "wikitextFetched": bool(fetch_planet_wikitext),
        "categoryHintsUsed": bool(use_category_system_hints),
        "attached": 0,
        "unassigned": 0,
        "createdSystemsFromPlanets": 0,
    }

    # Optionally prefetch categories for better system inference (Star Wars).
    categories_map: Dict[str, List[str]] = {}
    if use_category_system_hints and planet_titles:
        categories_map = mw_pages_categories_bulk(api_base, planet_titles)

    # 3) Planet → System mapping (wikitext bulk)
    if fetch_planet_wikitext and planet_titles:
        attached = 0
        unassigned_count = 0
        created_from_planets = 0

        for batch in chunked(planet_titles, BATCH_SIZE):
            texts = mw_pages_wikitext_bulk(api_base, batch)

            for pt in batch:
                planet_name = pt.strip()

                # First: category hint (if enabled)
                sys_name: Optional[str] = None
                if use_category_system_hints:
                    sys_name = extract_system_from_categories(categories_map.get(pt, []))

                # Second: infobox/wikitext inference
                if not sys_name:
                    sys_name = extract_system_from_wikitext(texts.get(pt, "") or "", planet_param_names)

                # Attach planet
                if sys_name:
                    sys_obj = _try_match_system_name(system_index, sys_name)

                    # If the system was not in the systems category, create it to avoid losing structure
                    if not sys_obj:
                        new_name = sys_name.strip()
                        sys_obj = make_system_obj(
                            name=new_name,
                            notes=(
                                f"Created from planet pages (derived system reference). "
                                f"Source: {source_label}."
                            ),
                        )
                        system_index[_canon_key(new_name)] = sys_obj
                        created_from_planets += 1

                    sys_obj["planets"].append({"name": planet_name})
                    attached += 1
                else:
                    unassigned["planets"].append({"name": planet_name})
                    unassigned_count += 1

        planet_summary["attached"] = attached
        planet_summary["unassigned"] = unassigned_count
        planet_summary["createdSystemsFromPlanets"] = created_from_planets

    else:
        # Still include all planets, but everything goes to Unassigned
        for pt in planet_titles:
            unassigned["planets"].append({"name": pt.strip()})
        planet_summary["attached"] = 0
        planet_summary["unassigned"] = len(planet_titles)

    # Deterministic output ordering
    systems_out = sorted(system_index.values(), key=lambda s: s["name"].lower())

    return {
        "meta": {
            "franchise": franchise,
            "source": source_label,
            "generatedAt": generated_at,
            "planetEnrichment": planet_summary,
        },
        "systems": systems_out,
    }


def build_startrek() -> dict:
    return build_catalog(
        franchise="Star Trek",
        api_base="https://memory-alpha.fandom.com/api.php",
        systems_category="Star systems",
        planets_category="Planets",
        # Memory Alpha: common parameters (best-effort)
        planet_param_names=["system", "star system", "starsystem"],
        fetch_planet_wikitext=STARTREK_FETCH_PLANET_WIKITEXT,
        max_planets=STARTREK_MAX_PLANETS,
        use_category_system_hints=False,
        source_label="Memory Alpha (MediaWiki Action API)",
    )


def build_starwars() -> dict:
    return build_catalog(
        franchise="Star Wars",
        api_base="https://starwars.fandom.com/api.php",
        systems_category="Star systems",
        planets_category="Planets",
        # Wookieepedia: common parameters differ; include 'location' as a fallback.
        planet_param_names=["system", "star system", "starsystem", "location"],
        fetch_planet_wikitext=STARWARS_FETCH_PLANET_WIKITEXT,
        max_planets=STARWARS_MAX_PLANETS,
        use_category_system_hints=STARWARS_USE_CATEGORY_SYSTEM_HINTS,
        source_label="Wookieepedia (MediaWiki Action API)",
    )


def main() -> None:
    base_dir = Path(__file__).resolve().parents[1]  # Fictional Stars/
    catalogs_dir = base_dir / "catalogs"
    catalogs_dir.mkdir(parents=True, exist_ok=True)

    startrek = build_startrek()
    (catalogs_dir / "startrek_star_systems_catalog.json").write_text(
        json.dumps(startrek, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    starwars = build_starwars()
    (catalogs_dir / "starwars_star_systems_catalog.json").write_text(
        json.dumps(starwars, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(f"Wrote {catalogs_dir / 'startrek_star_systems_catalog.json'}")
    print(f"Wrote {catalogs_dir / 'starwars_star_systems_catalog.json'}")


if __name__ == "__main__":
    main()

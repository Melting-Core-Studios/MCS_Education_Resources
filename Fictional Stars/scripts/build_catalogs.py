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
    "MCS-Education-CatalogBot/1.0 (+https://github.com/your-org/your-repo)",
)

BATCH_SIZE = int(os.getenv("CATALOG_BATCH_SIZE", "20"))
THROTTLE_S = float(os.getenv("CATALOG_THROTTLE_S", "0.15"))
HTTP_TIMEOUT_S = int(os.getenv("CATALOG_HTTP_TIMEOUT_S", "120"))
HTTP_RETRIES = int(os.getenv("CATALOG_HTTP_RETRIES", "6"))

STARTREK_FETCH_PLANET_WIKITEXT = os.getenv("STARTREK_FETCH_PLANET_WIKITEXT", "1").strip().lower() in (
    "1", "true", "yes", "y", "on"
)
STARWARS_FETCH_PLANET_WIKITEXT = os.getenv("STARWARS_FETCH_PLANET_WIKITEXT", "1").strip().lower() in (
    "1", "true", "yes", "y", "on"
)

STARWARS_INCLUDE_MOONS = os.getenv("STARWARS_INCLUDE_MOONS", "1").strip().lower() in (
    "1", "true", "yes", "y", "on"
)

# Safety caps for recursive category walking
STARTREK_MAX_CATEGORIES = int(os.getenv("STARTREK_MAX_CATEGORIES", "8000"))
STARWARS_MAX_CATEGORIES = int(os.getenv("STARWARS_MAX_CATEGORIES", "8000"))

# Optional hard caps (0 = no cap)
STARTREK_MAX_BODIES = int(os.getenv("STARTREK_MAX_BODIES", "0"))
STARWARS_MAX_BODIES = int(os.getenv("STARWARS_MAX_BODIES", "0"))


# ----------------------------
# HTTP + MediaWiki helpers
# ----------------------------

def _sleep_backoff(attempt: int) -> None:
    time.sleep(min(60.0, (2 ** attempt) + random.random() * 2.0))


def http_get_json(url: str, timeout: int = HTTP_TIMEOUT_S, retries: int = HTTP_RETRIES) -> dict:
    last_err: Optional[Exception] = None

    for attempt in range(retries):
        try:
            req = urllib.request.Request(
                url,
                headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
            )
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                data = resp.read().decode("utf-8", errors="replace")
            payload = json.loads(data)

            # IMPORTANT: MediaWiki can return 200 with an "error" object
            if isinstance(payload, dict) and "error" in payload:
                raise RuntimeError(f"MediaWiki API error: {payload['error']}")

            return payload

        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503, 504):
                last_err = e
                _sleep_backoff(attempt)
                continue
            raise

        except (TimeoutError, urllib.error.URLError, json.JSONDecodeError) as e:
            last_err = e
            _sleep_backoff(attempt)
            continue

    raise RuntimeError(f"HTTP fetch failed after {retries} attempts: {url}") from last_err


def mw_api_url(api_base: str, params: dict) -> str:
    return f"{api_base}?{urllib.parse.urlencode(params)}"


def chunked(items: List[str], n: int) -> Iterable[List[str]]:
    for i in range(0, len(items), max(1, n)):
        yield items[i:i + n]


def mw_category_members_typed(
    api_base: str,
    category_title: str,
    limit: int = 200,
) -> Tuple[List[str], List[str]]:
    """
    Returns (pages, subcategories) for Category:<category_title>.

    FIX: We must request cmprop=...|type, otherwise member["type"] may not exist. (Default is ids|title)
    We also avoid cmnamespace to reduce the chance of "miser mode" edge-cases.
    """
    pages: List[str] = []
    subcats: List[str] = []
    cmcontinue: Optional[str] = None

    while True:
        params = {
            "action": "query",
            "list": "categorymembers",
            "cmtitle": f"Category:{category_title}",
            "cmtype": "page|subcat",
            "cmlimit": str(limit),
            "cmprop": "ids|title|type",
            "format": "json",
            "formatversion": "2",
        }
        if cmcontinue:
            params["cmcontinue"] = cmcontinue

        payload = http_get_json(mw_api_url(api_base, params))
        members = (payload.get("query") or {}).get("categorymembers") or []

        for m in members:
            title = m.get("title")
            if not title:
                continue

            mtype = m.get("type")
            # Resilience: if type is missing, infer by namespace (Category = 14)
            if not mtype:
                ns = m.get("ns")
                if ns == 14:
                    mtype = "subcat"
                else:
                    mtype = "page"

            if mtype == "page":
                pages.append(title)
            elif mtype == "subcat":
                # "Category:Ice planets" -> "Ice planets"
                if title.startswith("Category:"):
                    subcats.append(title.split("Category:", 1)[1])
                else:
                    subcats.append(title)

        cmcontinue = (payload.get("continue") or {}).get("cmcontinue")
        if not cmcontinue:
            break

        time.sleep(THROTTLE_S)

    return pages, subcats


def mw_category_pages_recursive(
    api_base: str,
    root_category: str,
    *,
    max_categories: int,
) -> List[str]:
    """
    BFS walk: return all mainspace pages in root category + all subcategories (recursive).
    """
    seen_cats: set[str] = set()
    seen_pages: set[str] = set()
    queue: List[str] = [root_category]

    while queue:
        cat = queue.pop(0)
        if cat in seen_cats:
            continue
        seen_cats.add(cat)

        pages, subcats = mw_category_members_typed(api_base, cat)
        for p in pages:
            seen_pages.add(p)
        for sc in subcats:
            if sc not in seen_cats and len(seen_cats) < max_categories:
                queue.append(sc)

        if len(seen_cats) >= max_categories:
            break

    return sorted(seen_pages, key=lambda s: s.lower())


def mw_pages_wikitext_bulk(api_base: str, titles: List[str]) -> Dict[str, str]:
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
        payload = http_get_json(mw_api_url(api_base, params), timeout=max(HTTP_TIMEOUT_S, 120))
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

    def _repl(m: re.Match) -> str:
        inner = m.group(1)
        return inner.split("|", 1)[1].strip() if "|" in inner else inner.strip()

    value = _WIKILINK_RE.sub(_repl, value)
    value = _TEMPLATE_RE.sub("", value)
    value = _REF_RE.sub("", value)
    value = _HTML_TAG_RE.sub("", value)
    return value.strip()


def canon_key(name: str) -> str:
    return re.sub(r"\s+", " ", name.strip().lower())


def try_match_system(system_index: Dict[str, dict], raw: str) -> Optional[dict]:
    if not raw:
        return None
    raw_clean = re.sub(r"\s+", " ", raw.strip())
    k = canon_key(raw_clean)

    if k in system_index:
        return system_index[k]

    if not k.endswith(" system"):
        k2 = canon_key(raw_clean + " system")
        if k2 in system_index:
            return system_index[k2]
    else:
        k3 = canon_key(re.sub(r"\s+system$", "", raw_clean, flags=re.IGNORECASE))
        if k3 in system_index:
            return system_index[k3]

    return None


def extract_system_from_value(value: str) -> Optional[str]:
    if not value:
        return None
    v = strip_wiki_markup(value)
    if not v:
        return None

    m = re.search(r"([A-Za-z0-9][^,;()\n]*?\bsystem\b)", v, flags=re.IGNORECASE)
    if m:
        s = m.group(1).strip()
        if s.lower() in ("system", "star system", "a star system"):
            return None
        return s
    return None


def extract_system_from_wikitext(wikitext: str, param_names: List[str]) -> Optional[str]:
    if not wikitext:
        return None

    for pn in param_names:
        rx = re.compile(rf"^\|\s*{re.escape(pn)}\s*=\s*(.+)$", flags=re.IGNORECASE)
        for line in wikitext.splitlines():
            m = rx.match(line)
            if not m:
                continue
            sys_name = extract_system_from_value(m.group(1).strip())
            if sys_name:
                return sys_name

    return None


def extract_system_from_categories(category_titles: List[str]) -> Optional[str]:
    """
    Star Wars: derive system from categories like "Category:Hoth system locations".
    """
    if not category_titles:
        return None

    cats = [c.split("Category:", 1)[1] if c.startswith("Category:") else c for c in category_titles]

    for c in cats:
        m = re.match(r"^(.+?)\s+system\s+locations$", c, flags=re.IGNORECASE)
        if m:
            base = m.group(1).strip()
            if base:
                return f"{base} system"

    return None


# ----------------------------
# Catalog helpers
# ----------------------------

def make_system_obj(name: str, notes: str) -> dict:
    return {
        "name": name,
        "category": "Single",
        "notes": notes,
        "stars": [{"name": f"{name} primary", "type": "star"}],
        "planets": [],
    }


def load_existing_catalog(path: Path) -> Optional[dict]:
    try:
        if not path.exists():
            return None
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return None
        if "systems" not in data or not isinstance(data["systems"], list):
            return None
        return data
    except Exception:
        return None


def build_system_index_from_catalog(data: dict, notes_suffix: str) -> Dict[str, dict]:
    systems: Dict[str, dict] = {}
    for s in data.get("systems", []):
        if not isinstance(s, dict):
            continue
        name = (s.get("name") or "").strip()
        if not name:
            continue
        out = dict(s)
        out["name"] = name
        out["planets"] = []
        out["notes"] = (out.get("notes") or "") + notes_suffix
        if not isinstance(out.get("stars"), list) or len(out["stars"]) == 0:
            out["stars"] = [{"name": f"{name} primary", "type": "star"}]
        systems[canon_key(name)] = out
    return systems


def build_from_wiki_systems(api_base: str, systems_category: str, source_label: str) -> Dict[str, dict]:
    titles = mw_category_pages_recursive(api_base, systems_category, max_categories=4000)
    idx: Dict[str, dict] = {}
    for t in titles:
        name = t.strip()
        idx[canon_key(name)] = make_system_obj(
            name=name,
            notes=f"Auto-generated from {source_label} category listings.",
        )
    return idx


def attach_bodies_to_systems(
    *,
    api_base: str,
    bodies: List[str],
    system_index: Dict[str, dict],
    unassigned: dict,
    body_param_names: List[str],
    use_category_system_hints: bool,
    fetch_wikitext: bool,
    source_label: str,
) -> dict:
    created_from_bodies = 0
    attached = 0
    unmapped = 0

    categories_map: Dict[str, List[str]] = {}
    if use_category_system_hints and bodies:
        categories_map = mw_pages_categories_bulk(api_base, bodies)

    if fetch_wikitext and bodies:
        for batch in chunked(bodies, BATCH_SIZE):
            texts = mw_pages_wikitext_bulk(api_base, batch)
            for title in batch:
                body_name = title.strip()

                sys_name: Optional[str] = None
                if use_category_system_hints:
                    sys_name = extract_system_from_categories(categories_map.get(title, []))

                if not sys_name:
                    sys_name = extract_system_from_wikitext(texts.get(title, "") or "", body_param_names)

                if sys_name:
                    sys_obj = try_match_system(system_index, sys_name)
                    if not sys_obj:
                        new_name = sys_name.strip()
                        sys_obj = make_system_obj(
                            name=new_name,
                            notes=f"Created from body pages (derived system reference). Source: {source_label}.",
                        )
                        system_index[canon_key(new_name)] = sys_obj
                        created_from_bodies += 1

                    sys_obj["planets"].append({"name": body_name})
                    attached += 1
                else:
                    unassigned["planets"].append({"name": body_name})
                    unmapped += 1
    else:
        for title in bodies:
            unassigned["planets"].append({"name": title.strip()})
        unmapped = len(bodies)

    return {
        "enabled": True,
        "bodyPages": len(bodies),
        "wikitextFetched": bool(fetch_wikitext),
        "categoryHintsUsed": bool(use_category_system_hints),
        "attached": attached,
        "unassigned": unmapped,
        "createdSystemsFromBodies": created_from_bodies,
    }


def _utc_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).replace(microsecond=0).isoformat()


def build_startrek_catalog(catalogs_dir: Path) -> dict:
    franchise = "Star Trek"
    api_base = "https://memory-alpha.fandom.com/api.php"
    source_label = "Memory Alpha (MediaWiki Action API)"
    generated_at = _utc_iso()

    system_index = build_from_wiki_systems(api_base, "Star systems", source_label)

    unassigned = make_system_obj(
        "Unassigned system",
        "Catch-all for bodies whose star system could not be reliably derived from source pages.",
    )
    system_index[canon_key(unassigned["name"])] = unassigned

    bodies = mw_category_pages_recursive(api_base, "Planets", max_categories=STARTREK_MAX_CATEGORIES)
    if STARTREK_MAX_BODIES > 0:
        bodies = bodies[:STARTREK_MAX_BODIES]

    if len(bodies) == 0 or len(system_index) <= 1:
        raise RuntimeError("Sanity check failed: Star Trek crawl returned 0 bodies and/or 0 systems. Aborting.")

    planet_summary = attach_bodies_to_systems(
        api_base=api_base,
        bodies=bodies,
        system_index=system_index,
        unassigned=unassigned,
        body_param_names=["system", "star system", "starsystem"],
        use_category_system_hints=False,
        fetch_wikitext=STARTREK_FETCH_PLANET_WIKITEXT,
        source_label=source_label,
    )

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


def build_starwars_catalog(catalogs_dir: Path) -> dict:
    franchise = "Star Wars"
    api_base = "https://starwars.fandom.com/api.php"
    source_label = "Wookieepedia (MediaWiki Action API)"
    generated_at = _utc_iso()

    existing_path = catalogs_dir / "starwars_star_systems_catalog.json"
    existing = load_existing_catalog(existing_path)

    # Build systems from wiki (fallback), but prefer a large existing baseline if present.
    wiki_system_index = build_from_wiki_systems(api_base, "Star systems", source_label)

    system_index: Dict[str, dict]
    baseline_note: str = ""

    if existing and isinstance(existing.get("systems"), list):
        existing_count = len(existing["systems"])
        wiki_count = len(wiki_system_index)

        # Prefer existing if it is materially larger than what we can derive from wiki category structure.
        if existing_count >= 200 and existing_count >= max(200, wiki_count * 2):
            baseline_note = " Baseline preserved from existing catalog; planets refreshed from Wookieepedia."
            system_index = build_system_index_from_catalog(existing, baseline_note)
        else:
            system_index = wiki_system_index
    else:
        system_index = wiki_system_index

    unassigned = make_system_obj(
        "Unassigned system",
        "Catch-all for bodies whose star system could not be reliably derived from source pages.",
    )
    system_index[canon_key(unassigned["name"])] = unassigned

    planets = mw_category_pages_recursive(api_base, "Planets", max_categories=STARWARS_MAX_CATEGORIES)
    bodies = planets

    if STARWARS_INCLUDE_MOONS:
        moons = mw_category_pages_recursive(api_base, "Moons", max_categories=STARWARS_MAX_CATEGORIES)
        bodies = sorted(set(planets) | set(moons), key=lambda s: s.lower())

    if STARWARS_MAX_BODIES > 0:
        bodies = bodies[:STARWARS_MAX_BODIES]

    if len(bodies) == 0 or len(system_index) <= 1:
        raise RuntimeError("Sanity check failed: Star Wars crawl returned 0 bodies and/or 0 systems. Aborting.")

    planet_summary = attach_bodies_to_systems(
        api_base=api_base,
        bodies=bodies,
        system_index=system_index,
        unassigned=unassigned,
        body_param_names=["system", "star system", "starsystem", "location"],
        use_category_system_hints=True,
        fetch_wikitext=STARWARS_FETCH_PLANET_WIKITEXT,
        source_label=source_label,
    )

    systems_out = sorted(system_index.values(), key=lambda s: s["name"].lower())
    return {
        "meta": {
            "franchise": franchise,
            "source": source_label + baseline_note,
            "generatedAt": generated_at,
            "planetEnrichment": planet_summary,
            "includesMoons": STARWARS_INCLUDE_MOONS,
        },
        "systems": systems_out,
    }


def main() -> None:
    base_dir = Path(__file__).resolve().parents[1]  # Fictional Stars/
    catalogs_dir = base_dir / "catalogs"
    catalogs_dir.mkdir(parents=True, exist_ok=True)

    startrek = build_startrek_catalog(catalogs_dir)
    (catalogs_dir / "startrek_star_systems_catalog.json").write_text(
        json.dumps(startrek, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    starwars = build_starwars_catalog(catalogs_dir)
    (catalogs_dir / "starwars_star_systems_catalog.json").write_text(
        json.dumps(starwars, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(f"Wrote {catalogs_dir / 'startrek_star_systems_catalog.json'}")
    print(f"Wrote {catalogs_dir / 'starwars_star_systems_catalog.json'}")


if __name__ == "__main__":
    main()

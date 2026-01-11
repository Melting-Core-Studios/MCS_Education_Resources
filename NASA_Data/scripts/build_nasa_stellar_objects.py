import argparse
import json
import math
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

SBDB_QUERY_URL = "https://ssd-api.jpl.nasa.gov/sbdb_query.api"
SBDB_LOOKUP_URL = "https://ssd-api.jpl.nasa.gov/sbdb.api"

# NASA Open Data Portal "Meteorite Landings API" (legacy docs JSON)
METEORITE_PRIMARY_URL = "https://data.nasa.gov/docs/legacy/meteorite_landings/gh4g-9sfh.json"
# Common Socrata endpoint fallback
METEORITE_FALLBACK_SOCRATA = "https://data.nasa.gov/resource/y77d-th95.json"

UA = "MCS-Education-Stellar-Objects-Updater/1.1"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def to_float(x: Any) -> Optional[float]:
    try:
        if x is None or x == "":
            return None
        v = float(x)
        return v if math.isfinite(v) else None
    except Exception:
        return None


def sleep_backoff(i: int) -> None:
    time.sleep(min(30.0, 2.0 * (i + 1)))


def get_json(
    session: requests.Session,
    url: str,
    params: Optional[Dict[str, Any]] = None,
    tries: int = 6,
) -> Any:
    last = None
    for i in range(tries):
        try:
            r = session.get(url, params=params, timeout=(30, 300))
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last = e
            sleep_backoff(i)
    raise last


def sbdb_query_count(
    session: requests.Session,
    sb_kind: str = "c",
    sb_xfrag: int = 1,
    sb_ns: Optional[str] = None,
) -> int:
    params: Dict[str, Any] = {"sb-kind": sb_kind, "sb-xfrag": sb_xfrag}
    if sb_ns:
        params["sb-ns"] = sb_ns
    j = get_json(session, SBDB_QUERY_URL, params=params)
    c = j.get("count")
    return int(c) if c is not None else 0


def sbdb_query_page(
    session: requests.Session,
    fields: str,
    limit: int,
    limit_from: int,
    sb_kind: str = "c",
    sb_xfrag: int = 1,
    sb_ns: Optional[str] = None,
    sort: str = "spkid",
) -> Tuple[List[str], List[List[Any]]]:
    params: Dict[str, Any] = {
        "fields": fields,
        "sb-kind": sb_kind,
        "sb-xfrag": sb_xfrag,
        "limit": int(limit),
        "limit-from": int(limit_from),
        "sort": sort,
        "full-prec": 0,
    }
    if sb_ns:
        params["sb-ns"] = sb_ns

    j = get_json(session, SBDB_QUERY_URL, params=params)
    f = j.get("fields") or []
    data = j.get("data") or []

    if not isinstance(f, list) or not isinstance(data, list):
        return [], []

    return [str(x) for x in f], data  # type: ignore[return-value]


def rows_to_dicts(field_names: List[str], rows: List[List[Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in rows:
        if not isinstance(r, list):
            continue
        d: Dict[str, Any] = {}
        for i, k in enumerate(field_names):
            d[k] = r[i] if i < len(r) else None
        out.append(d)
    return out


def parse_meteorite_year(value: Any) -> Tuple[Optional[int], Optional[str]]:
    if value is None:
        return None, None
    s = str(value).strip()
    if not s:
        return None, None
    try:
        y = int(s[:4])
        if 0 < y < 9999:
            return y, s
    except Exception:
        pass
    return None, s


def fetch_meteorites(session: requests.Session) -> Dict[str, Any]:
    # Primary: legacy docs JSON
    try:
        j = get_json(session, METEORITE_PRIMARY_URL, params=None)
        if isinstance(j, list):
            return {"sourceUrlUsed": METEORITE_PRIMARY_URL, "rows": j}
    except Exception:
        pass

    # Fallback: Socrata paging
    all_rows: List[Dict[str, Any]] = []
    limit = 50000
    offset = 0
    while True:
        params = {"$limit": limit, "$offset": offset}
        page = get_json(session, METEORITE_FALLBACK_SOCRATA, params=params)
        if not isinstance(page, list) or not page:
            break
        for r in page:
            if isinstance(r, dict):
                all_rows.append(r)
        if len(page) < limit:
            break
        offset += limit

    return {"sourceUrlUsed": METEORITE_FALLBACK_SOCRATA, "rows": all_rows}


def normalize_meteorites(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        name = (r.get("name") or "").strip()
        if not name:
            continue

        year_int, year_iso = parse_meteorite_year(r.get("year"))
        obj = {
            "id": f"meteorite:{r.get('id') or name}",
            "kind": "meteorite",
            "name": name,
            "nameType": r.get("nametype"),
            "class": r.get("recclass"),
            "massG": to_float(r.get("mass")),
            "fall": r.get("fall"),
            "year": year_int,
            "yearRaw": year_iso,
            "reclat": to_float(r.get("reclat")),
            "reclong": to_float(r.get("reclong")),
            "geo": r.get("geolocation"),
        }
        out.append({k: v for k, v in obj.items() if v is not None})

    out.sort(key=lambda x: (str(x.get("name", ""))).lower())
    return out


def fetch_numbered_comet_discovery(session: requests.Session, des: str) -> Optional[Dict[str, Any]]:
    params = {"des": des, "discovery": 1}
    try:
        j = get_json(session, SBDB_LOOKUP_URL, params=params, tries=6)
        disc = j.get("discovery")
        if isinstance(disc, dict):
            keep: Dict[str, Any] = {}
            for k in ("date", "location", "site", "who", "ref"):
                if disc.get(k) not in (None, "", []):
                    keep[k] = disc.get(k)
            return keep if keep else None
    except Exception:
        return None
    return None


def normalize_comets(
    session: requests.Session,
    page_size: int,
    enrich_numbered_discovery: bool,
    include_fragments: bool,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    sb_xfrag = 0 if include_fragments else 1

    # Use only documented SBDB Query fields (case-sensitive).
    fields = ",".join(
        [
            "spkid",
            "full_name",
            "pdes",
            "name",
            "prefix",
            "class",
            "first_obs",
            "last_obs",
            "producer",
            "diameter",
            "albedo",
            "H",
            "epoch_cal",
            "e",
            "a",
            "q",
            "i",
            "om",
            "w",
            "tp_cal",
            "per_y",
        ]
    )

    total = sbdb_query_count(session, sb_kind="c", sb_xfrag=sb_xfrag)
    comets_by_spkid: Dict[str, Dict[str, Any]] = {}

    offset = 0
    while True:
        f_names, rows = sbdb_query_page(
            session=session,
            fields=fields,
            limit=page_size,
            limit_from=offset,
            sb_kind="c",
            sb_xfrag=sb_xfrag,
            sb_ns=None,
            sort="spkid",
        )
        if not f_names or not rows:
            break

        dict_rows = rows_to_dicts(f_names, rows)
        for r in dict_rows:
            spkid = str(r.get("spkid") or "").strip()
            full_name = r.get("full_name")
            if not spkid or not full_name:
                continue

            obj: Dict[str, Any] = {
                "id": f"sbdb:{spkid}",
                "kind": "comet",
                "spkid": spkid,
                "fullName": full_name,
                "pdes": r.get("pdes"),
                "name": r.get("name"),
                "prefix": r.get("prefix"),
                "orbitClass": r.get("class"),
                "firstObs": r.get("first_obs"),
                "lastObs": r.get("last_obs"),
                "orbitProducer": r.get("producer"),
                "diameterKm": to_float(r.get("diameter")),
                "albedo": to_float(r.get("albedo")),
                "H": to_float(r.get("H")),
                "elements": {
                    "epochCal": r.get("epoch_cal"),
                    "e": to_float(r.get("e")),
                    "aAU": to_float(r.get("a")),
                    "qAU": to_float(r.get("q")),
                    "iDeg": to_float(r.get("i")),
                    "omDeg": to_float(r.get("om")),
                    "wDeg": to_float(r.get("w")),
                    "tpCal": r.get("tp_cal"),
                    "periodYears": to_float(r.get("per_y")),
                },
            }
            obj["elements"] = {k: v for k, v in obj["elements"].items() if v is not None}
            comets_by_spkid[spkid] = {k: v for k, v in obj.items() if v is not None}

        offset += len(rows)
        if len(rows) < page_size:
            break

    enriched = 0
    attempted = 0
    if enrich_numbered_discovery:
        n_offset = 0
        n_fields = "spkid,pdes,full_name"
        while True:
            f_names, rows = sbdb_query_page(
                session=session,
                fields=n_fields,
                limit=page_size,
                limit_from=n_offset,
                sb_kind="c",
                sb_xfrag=sb_xfrag,
                sb_ns="n",
                sort="spkid",
            )
            if not f_names or not rows:
                break
            dict_rows = rows_to_dicts(f_names, rows)
            for r in dict_rows:
                spkid = str(r.get("spkid") or "").strip()
                pdes = str(r.get("pdes") or "").strip()
                if not spkid or not pdes:
                    continue
                attempted += 1
                disc = fetch_numbered_comet_discovery(session, pdes)
                if disc and spkid in comets_by_spkid:
                    comets_by_spkid[spkid]["discovery"] = disc
                    if disc.get("date"):
                        comets_by_spkid[spkid]["discoveryDate"] = disc.get("date")
                    if disc.get("who"):
                        comets_by_spkid[spkid]["discoveredBy"] = disc.get("who")
                    if disc.get("location"):
                        comets_by_spkid[spkid]["discoveryLocation"] = disc.get("location")
                    enriched += 1
                time.sleep(0.05)

            n_offset += len(rows)
            if len(rows) < page_size:
                break

    comets = list(comets_by_spkid.values())
    comets.sort(key=lambda x: (str(x.get("fullName", ""))).lower())

    meta = {
        "count": len(comets),
        "sbdbCountReported": total,
        "includeFragments": bool(include_fragments),
        "discoveryEnrichment": {
            "enabled": bool(enrich_numbered_discovery),
            "attempted": attempted,
            "enriched": enriched,
            "note": "Discovery circumstances are only available via SBDB lookup for numbered comets (per SBDB API).",
        },
    }
    return comets, meta


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--output", required=True, type=str)
    ap.add_argument("--comet-page-size", type=int, default=5000)
    ap.add_argument("--enrich-numbered-comet-discovery", type=int, default=1)
    ap.add_argument("--include-comet-fragments", type=int, default=0)
    args = ap.parse_args()

    sess = requests.Session()
    sess.headers.update({"User-Agent": UA})

    generated_at = utc_now_iso()

    comets, comet_meta = normalize_comets(
        session=sess,
        page_size=max(100, int(args.comet_page_size)),
        enrich_numbered_discovery=(int(args.enrich_numbered_comet_discovery) == 1),
        include_fragments=(int(args.include_comet_fragments) == 1),
    )

    met_blob = fetch_meteorites(sess)
    meteorite_rows = met_blob.get("rows") or []
    meteorites = normalize_meteorites(meteorite_rows if isinstance(meteorite_rows, list) else [])

    out = {
        "meta": {
            "generatedAt": generated_at,
            "schema": "mcs-education-stellar-objects-v1",
            "sources": {
                "comets": {
                    "name": "NASA/JPL SBDB Query API",
                    "url": SBDB_QUERY_URL,
                    "lookupUrl": SBDB_LOOKUP_URL,
                    "meta": comet_meta,
                },
                "meteorites": {
                    "name": "NASA Open Data Portal - Meteorite Landings API",
                    "urlUsed": met_blob.get("sourceUrlUsed"),
                    "count": len(meteorites),
                },
            },
        },
        "comets": comets,
        "meteorites": meteorites,
    }

    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, separators=(",", ":"))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

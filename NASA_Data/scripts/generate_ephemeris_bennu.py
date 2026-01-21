import json
import os
import re
import time
from datetime import datetime, timedelta, timezone

import requests

"""
Generate a JPL Horizons-based ephemeris feed for asteroid (101955) Bennu.

Outputs:
  NASA_Data/events_in_our_solar_system/output/asteroid_bennu/
    manifest.json
    ephemeris/planets_5d.json
    ephemeris/bennu_1d.json
    ephemeris/bennu_osiris_arrival_10m.json
    ephemeris/bennu_tag_10m.json

Notes:
- Bennu NAIF/SPK-ID is 2101955.
- The Horizons API supports small-bodies and can output vectors in AU and AU/day.

This script is designed to match the output format used by other MCS ephemeris generators.
"""

# ---- Horizons defaults (aligned with existing generators) ----
HORIZONS_URL = "https://ssd.jpl.nasa.gov/api/horizons.api"
UA = "MCS-EphemerisBot/1.0"

CENTER = "@0"  # Solar system barycenter
REF_SYSTEM = "J2000"
REF_PLANE = "ECLIPTIC"
OUT_UNITS = "AU-D"  # Position in AU, velocity in AU/day
VEC_TABLE = 3
TIME_TYPE = "UTC"

# ---- Bennu specifics ----
BENNU_ID = 2101955  # NAIF/SPK-ID for (101955) Bennu

# Mission-relevant anchors for higher-resolution windows (UTC anchors).
# - OSIRIS-REx rendezvous/arrival at Bennu: 2018-12-03
# - TAG sampling event: 2020-10-20
OSIRIS_ARRIVAL = "2018-12-03T00:00:00Z"
TAG_EVENT = "2020-10-20T00:00:00Z"

# Generate from J2000 forward (keeps datasets reasonably sized and avoids pre-1990 small-body coverage edge cases).
START_TIME_DEFAULT = "2000-01-01 00:00"

# Major bodies for context dataset (5-day grid)
MAJOR_BODIES = [
    ("Sun", 10),
    ("Mercury", 199),
    ("Venus", 299),
    ("Earth", 399),
    ("Mars", 499),
    ("Jupiter", 599),
    ("Saturn", 699),
    ("Uranus", 799),
    ("Neptune", 899),
]

HTTP_TIMEOUT_S = 45
RETRIES = 5
BACKOFF_S = 2.0
DELAY_BETWEEN_CALLS_S = 0.35
MAX_SAMPLES_PER_CALL = 8900  # conservative guard for Horizons


def q(s: str) -> str:
    """Quote for Horizons API."""
    return "'" + str(s) + "'"


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def write_json(path: str, data):
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def stop_time_today_00z() -> str:
    """UTC midnight today (00:00Z) formatted for Horizons."""
    dt = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    return dt.strftime("%Y-%m-%d %H:%M")


EARLIEST_RE = re.compile(r"earliest\s+available\s+date\s+is\s+([\w\s:\-]+)\s+\(UT\)", re.I)


def parse_earliest_from_error(msg: str):
    m = EARLIEST_RE.search(msg or "")
    if not m:
        return None
    s = m.group(1).strip()
    # Horizons uses forms like "1950-Jan-01 00:00"
    try:
        return datetime.strptime(s, "%Y-%b-%d %H:%M").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def request_json(params: dict) -> dict:
    headers = {"User-Agent": UA}
    last_err = None
    for i in range(RETRIES):
        try:
            r = requests.get(HORIZONS_URL, params=params, headers=headers, timeout=HTTP_TIMEOUT_S)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            time.sleep(BACKOFF_S * (i + 1))
    raise RuntimeError(f"Horizons request failed after {RETRIES} retries: {last_err}")


def extract_block(text: str, start_marker="$$SOE", end_marker="$$EOE") -> str:
    if start_marker not in text or end_marker not in text:
        raise RuntimeError("Missing SOE/EOE markers in Horizons response.")
    return text.split(start_marker, 1)[1].split(end_marker, 1)[0].strip()


def parse_vectors(block: str):
    """Parse Horizons VECTORS block ($$SOE..$$EOE) into (t_jd, pv_flat).

    Supports CSV format. Accepts E or D exponent markers.
    """
    t = []
    pv = []

    for raw in block.splitlines():
        line = raw.strip()
        if not line:
            continue

        # Expect CSV_FORMAT=YES: JD, CAL, X, Y, Z, VX, VY, VZ, ...
        if "," not in line:
            continue

        parts = [p.strip() for p in line.split(",")]
        if len(parts) < 8:
            continue

        # First field is JD
        try:
            jd = float(parts[0])
        except ValueError:
            continue

        nums = parts[2:8]
        try:
            vals = [float(s.replace("D", "E").replace("d", "E")) for s in nums]
        except ValueError:
            continue

        x, y, z, vx, vy, vz = vals
        t.append(jd)
        pv.extend([x, y, z, vx, vy, vz])

    if len(t) < 2 or len(pv) != len(t) * 6:
        preview = "\n".join(block.splitlines()[:25])
        raise RuntimeError("Parsed too few samples. Preview:\n" + preview)

    return t, pv


def _format_horizons_time(dt: datetime) -> str:
    """Format datetime for Horizons START/STOP_TIME."""
    return dt.strftime("%Y-%m-%d %H:%M")


def _parse_horizons_ad_time(s: str) -> datetime:
    """Parse ISO-ish time used in this script (YYYY-MM-DDTHH:MM:SSZ)."""
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def _maybe_clip_to_horizons_limits(command: int, start_dt: datetime, stop_dt: datetime):
    """If Horizons complains about earliest date, clip start."""
    params = {
        "format": "json",
        "COMMAND": q(str(command)),
        "MAKE_EPHEM": q("YES"),
        "EPHEM_TYPE": q("VECTORS"),
        "OBJ_DATA": q("NO"),
        "CENTER": q(CENTER),
        "START_TIME": q(_format_horizons_time(start_dt)),
        "STOP_TIME": q(_format_horizons_time(stop_dt)),
        "STEP_SIZE": q("1 d"),
        "REF_SYSTEM": q(REF_SYSTEM),
        "REF_PLANE": q(REF_PLANE),
        "OUT_UNITS": q(OUT_UNITS),
        "VEC_TABLE": VEC_TABLE,
        "CSV_FORMAT": q("YES"),
        "VEC_LABELS": q("NO"),
        "VEC_DELTA_T": q("NO"),
        "VEC_CORR": q("NONE"),
        "TIME_TYPE": q(TIME_TYPE),
    }
    try:
        _ = request_json(params)
        return start_dt, stop_dt
    except Exception as e:
        msg = str(e)
        earliest = parse_earliest_from_error(msg)
        if earliest and earliest > start_dt:
            return earliest, stop_dt
        return start_dt, stop_dt


def horizons_vectors(command: int, start_time: str, stop_time: str, step_size: str, center_override: str | None = None):
    """Fetch vectors from Horizons and return (t_jd, pv, signature)."""
    params = {
        "format": "json",
        "COMMAND": q(str(command)),
        "MAKE_EPHEM": q("YES"),
        "EPHEM_TYPE": q("VECTORS"),
        "OBJ_DATA": q("YES"),
        "CENTER": q(center_override if center_override else CENTER),
        "START_TIME": q(start_time),
        "STOP_TIME": q(stop_time),
        "STEP_SIZE": q(step_size),
        "REF_SYSTEM": q(REF_SYSTEM),
        "REF_PLANE": q(REF_PLANE),
        "OUT_UNITS": q(OUT_UNITS),
        "VEC_TABLE": VEC_TABLE,
        "CSV_FORMAT": q("YES"),
        "VEC_LABELS": q("NO"),
        "VEC_DELTA_T": q("NO"),
        "VEC_CORR": q("NONE"),
        "TIME_TYPE": q(TIME_TYPE),
    }

    time.sleep(DELAY_BETWEEN_CALLS_S)
    data = request_json(params)
    if "result" not in data:
        raise RuntimeError("Unexpected Horizons response (no 'result').")

    block = extract_block(data["result"])
    t_jd, pv = parse_vectors(block)

    signature = {
        "command": str(command),
        "center": center_override if center_override else CENTER,
        "start_time": start_time,
        "stop_time": stop_time,
        "step_size": step_size,
        "ref_system": REF_SYSTEM,
        "ref_plane": REF_PLANE,
        "out_units": OUT_UNITS,
        "time_type": TIME_TYPE,
        "vec_table": VEC_TABLE,
    }
    return t_jd, pv, signature


def horizons_vectors_chunked(command: int, start_time: str, stop_time: str, step_size: str, center_override: str | None = None):
    """Fetch vectors, chunking if the requested time span is too large for Horizons sample limits."""
    # Parse times (YYYY-MM-DD HH:MM)
    start_dt = datetime.strptime(start_time, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
    stop_dt = datetime.strptime(stop_time, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)

    # Determine samples per day for step_size (supports "Nd" and "Nm" and "Nh")
    m = re.match(r"^\s*(\d+)\s*([dhm])\s*$", step_size.strip().lower())
    if not m:
        # Horizons also supports "1 d" or "10 m" -> normalize
        m = re.match(r"^\s*(\d+)\s*([dhm])\s*$", step_size.replace(" ", "").lower())
    if not m:
        # Fallback: request directly (may fail if too big)
        return horizons_vectors(command, start_time, stop_time, step_size, center_override=center_override)

    n = int(m.group(1))
    unit = m.group(2)
    step_sec = {"d": 86400, "h": 3600, "m": 60}[unit] * n
    total_sec = (stop_dt - start_dt).total_seconds()
    total_samples = int(total_sec // step_sec) + 1

    if total_samples <= MAX_SAMPLES_PER_CALL:
        return horizons_vectors(command, start_time, stop_time, step_size, center_override=center_override)

    # Chunk by time span so each call remains below MAX_SAMPLES_PER_CALL
    chunk_span_sec = (MAX_SAMPLES_PER_CALL - 1) * step_sec
    t_all = []
    pv_all = []
    sig = None

    cur = start_dt
    while cur < stop_dt:
        chunk_stop = min(stop_dt, cur + timedelta(seconds=chunk_span_sec))
        t, pv, sig = horizons_vectors(
            command,
            _format_horizons_time(cur),
            _format_horizons_time(chunk_stop),
            step_size,
            center_override=center_override,
        )

        # Merge with de-dup at boundary
        if t_all and t and abs(t[0] - t_all[-1]) < 1e-10:
            t = t[1:]
            pv = pv[6:]

        t_all.extend(t)
        pv_all.extend(pv)
        cur = chunk_stop

    return t_all, pv_all, sig


def main():
    # Output root
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    out_root = os.path.join(repo_root, "events_in_our_solar_system", "output", "asteroid_bennu")
    ephem_dir = os.path.join(out_root, "ephemeris")
    ensure_dir(ephem_dir)

    start_time = os.environ.get("BENNU_START_TIME", START_TIME_DEFAULT)
    stop_time = os.environ.get("BENNU_STOP_TIME", stop_time_today_00z())

    # Context: planets (5-day)
    # Use same start/stop as Bennu for a coherent reference set
    t_ref = None
    objects = {}
    sig_any = None
    for name, spkid in MAJOR_BODIES:
        t, pv, sig = horizons_vectors_chunked(spkid, start_time, stop_time, "5 d")
        if t_ref is None:
            t_ref = t
        else:
            if len(t) != len(t_ref):
                raise RuntimeError(f"Time grid mismatch for {name}")
            for a, b in zip(t, t_ref):
                if abs(a - b) > 1e-10:
                    raise RuntimeError(f"Time grid mismatch for {name}")
        objects[str(spkid)] = {"name": name, "pv": pv}
        sig_any = sig_any or sig

    planets_json = {
        "schema": "mcs-ephem-multi-v1",
        "t_jd": t_ref,
        "objects": objects,
        "meta": {
            "generated_at": now_iso(),
            "source": {"name": "JPL Horizons", "service": HORIZONS_URL},
            "frame": {
                "center": CENTER,
                "ref_system": REF_SYSTEM,
                "ref_plane": REF_PLANE,
                "out_units": OUT_UNITS,
                "time_type": TIME_TYPE,
                "vec_table": VEC_TABLE,
            },
            "signature": sig_any,
        },
    }
    write_json(os.path.join(ephem_dir, "planets_5d.json"), planets_json)

    # Bennu (daily)
    t_bennu, pv_bennu, sig_bennu = horizons_vectors_chunked(BENNU_ID, start_time, stop_time, "1 d")
    bennu_json = {
        "schema": "mcs-ephem-v1",
        "t_jd": t_bennu,
        "pv": pv_bennu,
        "meta": {
            "generated_at": now_iso(),
            "source": {"name": "JPL Horizons", "service": HORIZONS_URL},
            "frame": {
                "center": CENTER,
                "ref_system": REF_SYSTEM,
                "ref_plane": REF_PLANE,
                "out_units": OUT_UNITS,
                "time_type": TIME_TYPE,
                "vec_table": VEC_TABLE,
            },
            "object": {"name": "(101955) Bennu", "spkid": BENNU_ID},
            "signature": sig_bennu,
        },
    }
    write_json(os.path.join(ephem_dir, "bennu_1d.json"), bennu_json)

    # Higher-resolution windows (10 minutes) around OSIRIS-relevant dates
    def write_window(file_name: str, center_time_iso: str, days_each_side: int = 2):
        cdt = _parse_horizons_ad_time(center_time_iso)
        a = cdt - timedelta(days=days_each_side)
        b = cdt + timedelta(days=days_each_side)
        t_hi, pv_hi, sig_hi = horizons_vectors_chunked(
            BENNU_ID,
            _format_horizons_time(a),
            _format_horizons_time(b),
            "10 m",
        )
        hi_json = {
            "schema": "mcs-ephem-v1",
            "t_jd": t_hi,
            "pv": pv_hi,
            "meta": {
                "generated_at": now_iso(),
                "source": {"name": "JPL Horizons", "service": HORIZONS_URL},
                "frame": {
                    "center": CENTER,
                    "ref_system": REF_SYSTEM,
                    "ref_plane": REF_PLANE,
                    "out_units": OUT_UNITS,
                    "time_type": TIME_TYPE,
                    "vec_table": VEC_TABLE,
                },
                "object": {"name": "(101955) Bennu", "spkid": BENNU_ID},
                "signature": {**(sig_hi or {}), "window_center": center_time_iso, "window_days_each_side": days_each_side},
            },
        }
        write_json(os.path.join(ephem_dir, file_name), hi_json)

    write_window("bennu_osiris_arrival_10m.json", OSIRIS_ARRIVAL, days_each_side=2)
    write_window("bennu_tag_10m.json", TAG_EVENT, days_each_side=1)

    manifest = {
        "schema": "mcs-ephem-manifest-v1",
        "generated_at": now_iso(),
        "source": {"name": "JPL Horizons", "service": HORIZONS_URL},
        "frame": {
            "center": CENTER,
            "ref_system": REF_SYSTEM,
            "ref_plane": REF_PLANE,
            "out_units": OUT_UNITS,
            "time_type": TIME_TYPE,
            "vec_table": VEC_TABLE,
        },
        "datasets": [
            {"id": "planets_5d", "file": "ephemeris/planets_5d.json"},
            {"id": "bennu_1d", "file": "ephemeris/bennu_1d.json"},
            {"id": "bennu_osiris_arrival_10m", "file": "ephemeris/bennu_osiris_arrival_10m.json"},
            {"id": "bennu_tag_10m", "file": "ephemeris/bennu_tag_10m.json"},
        ],
        "object_hint": {"name": "(101955) Bennu", "spkid": BENNU_ID},
    }
    write_json(os.path.join(out_root, "manifest.json"), manifest)


if __name__ == "__main__":
    main()

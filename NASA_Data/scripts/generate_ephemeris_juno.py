import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

# Ephemeris generator for the MCS Education "Events in our Solar System" framework.
# Data source: JPL Horizons API (vectors, CSV).

HORIZONS_URL = "https://ssd.jpl.nasa.gov/api/horizons.api"
UA = "MCS-Education-EphemerisBot/1.3"

CENTER = "@0"  # Solar system barycenter
REF_SYSTEM = "ICRF"
REF_PLANE = "FRAME"
OUT_UNITS = "AU-D"  # Position in AU, velocity in AU/day
VEC_TABLE = "2"
TIME_TYPE = "UT"

# Juno spacecraft NAIF ID code: -61 (see NAIF IDs Required Reading)
# Launch: 2011-08-05 16:25:00 UTC
LAUNCH_TIME = "2011-08-05 16:25:00"
JUNO_ID = -61

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

HTTP_TIMEOUT_S = 120
RETRIES = 5
BACKOFF_S = 1.8
DELAY_BETWEEN_CALLS_S = 0.25
MAX_SAMPLES_PER_CALL = 2000

MONTH = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}

EARLIEST_RE = re.compile(
    r"prior to A\\.D\\. (\\d{4})-([A-Z]{3})-(\\d{2}) (\\d{2}):(\\d{2}):(\\d{2})(?:\\.(\\d+))? UT"
)


def q(s: str) -> str:
    return f"'{s}'"


class StartTooEarly(Exception):
    def __init__(self, earliest_dt: datetime, msg: str):
        super().__init__(msg)
        self.earliest_dt = earliest_dt


def parse_utcish(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)


def fmt_utcish(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc).replace(microsecond=0)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def step_seconds(step: str) -> int:
    s = (step or "").strip().lower()
    # Allow formats like "5d", "5 d", "5 day", "5 days", "10m", "10 min", "10 minutes"
    m = re.match(r"^\s*(\d+)\s*([a-z]+)\s*$", s)
    if not m:
        raise ValueError(f"Unsupported STEP_SIZE: {step}")
    n = int(m.group(1))
    u = m.group(2)
    if u in ("d", "day", "days"):
        return n * 86400
    if u in ("m", "min", "mins", "minute", "minutes"):
        return n * 60
    raise ValueError(f"Unsupported STEP_SIZE unit: {step}")



def stop_time_today_00z() -> str:
    d = datetime.now(timezone.utc).date()
    return f"{d.isoformat()} 00:00:00"


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def write_json(path: Path, obj: dict) -> None:
    ensure_dir(path.parent)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, separators=(",", ":"))
        f.write("\n")
    os.replace(tmp, path)


def timedelta_seconds(seconds: int):
    # returns a datetime.timedelta without importing timedelta (keeps dependency footprint tiny)
    return datetime.fromtimestamp(seconds, tz=timezone.utc) - datetime.fromtimestamp(0, tz=timezone.utc)


def parse_earliest_from_error(err: str):
    m = EARLIEST_RE.search(err or "")
    if not m:
        return None
    yyyy, mon, dd, hh, mm, ss, _frac = m.groups()
    dt = datetime(
        int(yyyy),
        MONTH.get(mon, 1),
        int(dd),
        int(hh),
        int(mm),
        int(ss),
        tzinfo=timezone.utc,
    )
    dt = dt.replace(microsecond=0) + timedelta_seconds(1)
    return dt


def request_json(params: dict) -> dict:
    last = None
    for i in range(RETRIES):
        try:
            r = requests.get(
                HORIZONS_URL,
                params=params,
                timeout=HTTP_TIMEOUT_S,
                headers={"User-Agent": UA},
            )
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last = e
            if i == RETRIES - 1:
                break
            time.sleep(BACKOFF_S * (2**i))
    raise RuntimeError(f"Horizons request failed: {last}")


def extract_block(result: str) -> str:
    i0 = result.find("$$SOE")
    i1 = result.find("$$EOE")
    if i0 < 0 or i1 < 0 or i1 <= i0:
        snippet = result.strip().replace("\r", "")[:1200]
        raise RuntimeError("Missing $$SOE/$$EOE. " + snippet)
    return result[i0 + 5 : i1].strip()



_HORIZONS_PRIOR_RE = re.compile(r'prior to\s+A\.D\.\s+(\d{4}-[A-Z]{3}-\d{2}\s+\d{2}:\d{2}:\d{2}\.\d+)', re.IGNORECASE)
_HORIZONS_AFTER_RE = re.compile(r'after\s+A\.D\.\s+(\d{4}-[A-Z]{3}-\d{2}\s+\d{2}:\d{2}:\d{2}\.\d+)', re.IGNORECASE)

def _parse_horizons_ad_time(s: str):
    try:
        # Example: 2011-AUG-05 17:18:06.0000
        return datetime.datetime.strptime(s.strip().title(), "%Y-%b-%d %H:%M:%S.%f")
    except Exception:
        return None

def _format_horizons_time(dt):
    return dt.strftime("%Y-%b-%d %H:%M:%S")

def _maybe_clip_to_horizons_limits(err_text: str, start_time: str, stop_time: str):
    # If Horizons indicates start is too early / stop too late, clip and return (new_start, new_stop).
    et = err_text or ""
    m1 = _HORIZONS_PRIOR_RE.search(et)
    if m1:
        dt = _parse_horizons_ad_time(m1.group(1))
        if dt:
            dt = dt + datetime.timedelta(seconds=1)
            return _format_horizons_time(dt), stop_time
    m2 = _HORIZONS_AFTER_RE.search(et)
    if m2:
        dt = _parse_horizons_ad_time(m2.group(1))
        if dt:
            dt = dt - datetime.timedelta(seconds=1)
            return start_time, _format_horizons_time(dt)
    return start_time, stop_time


def parse_vectors(block: str):
    """Parse Horizons VECTORS ($$SOE..$$EOE) into (t, pv).

    Supports both CSV and whitespace table formats.
    Accepts E or D exponent markers.
    """
    t = []
    pv = []

    for raw in block.splitlines():
        line = raw.strip()
        if not line:
            continue

        # CSV_FORMAT=YES typically yields: JD, CAL, X, Y, Z, VX, VY, VZ, ...
        if "," in line:
            parts = [p.strip() for p in line.split(",")]
            # Drop empty trailing fields (some responses end lines with a comma)
            while parts and parts[-1] == "":
                parts.pop()
            if not parts:
                continue
            if not re.match(r"^\d", parts[0]):
                continue
            try:
                jd = float(parts[0])
            except ValueError:
                continue

            # Prefer fields after JD + calendar date
            start_idx = 2 if len(parts) >= 8 else 1
            nums = parts[start_idx : start_idx + 6]
            if len(nums) < 6:
                continue
            try:
                vals = [float(s.replace("D", "E").replace("d", "E")) for s in nums]
            except ValueError:
                continue

            x, y, z, vx, vy, vz = vals

        else:
            # Whitespace-delimited table: take JD as first token, vector components as last 6 tokens
            if not re.match(r"^\d", line):
                continue
            fields = line.split()
            if len(fields) < 7:
                continue
            try:
                jd = float(fields[0])
                nums = fields[-6:]
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


def horizons_vectors_once(command: int, start_time: str, stop_time: str, step_size: str, allow_retry: bool = True):
    params = {
        "format": "json",
        "EPHEM_TYPE": q("VECTORS"),
        "MAKE_EPHEM": q("YES"),
        "OBJ_DATA": q("NO"),
        "COMMAND": q(str(command)),
        "CENTER": q(CENTER),
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
    j = request_json(params)
    if isinstance(j, dict) and j.get("error"):
        err = str(j["error"])
        earliest = parse_earliest_from_error(err)
        if earliest is not None:
            raise StartTooEarly(earliest, err)
        raise RuntimeError(err)

    result = j.get("result")
    if not isinstance(result, str):
        raise RuntimeError("Missing result field.")

    try:
        block = extract_block(result)
    except RuntimeError:
        snippet = (result or '').strip().replace("\\r", "")
        new_start, new_stop = _maybe_clip_to_horizons_limits(snippet, start_time, stop_time)
        if allow_retry and (new_start != start_time or new_stop != stop_time):
            return horizons_vectors_once(command, new_start, new_stop, step_size, allow_retry=False)
        raise

    t, pv = parse_vectors(block)

    sig = j.get("signature")
    if not isinstance(sig, dict):
        sig = {}

    return t, pv, sig


def horizons_vectors_chunked(command: int, start_time: str, stop_time: str, step_size: str):
    step_s = step_seconds(step_size)
    max_span_s = step_s * (MAX_SAMPLES_PER_CALL - 1)

    start_dt = parse_utcish(start_time)
    stop_dt = parse_utcish(stop_time)

    all_t = []
    all_pv = []
    sig_any = {}

    while start_dt < stop_dt:
        chunk_stop = start_dt + timedelta_seconds(max_span_s)
        if chunk_stop > stop_dt:
            chunk_stop = stop_dt

        s = fmt_utcish(start_dt)
        e = fmt_utcish(chunk_stop)

        try:
            t, pv, sig = horizons_vectors_once(command, s, e, step_size)
        except StartTooEarly as ex:
            if ex.earliest_dt >= stop_dt:
                raise RuntimeError(str(ex))
            start_dt = ex.earliest_dt
            continue

        if not all_t:
            all_t = t
            all_pv = pv
            sig_any = sig_any or sig
        else:
            last = all_t[-1]
            idx0 = 0
            while idx0 < len(t) and t[idx0] <= last + 1e-10:
                idx0 += 1
            if idx0 < len(t):
                all_t.extend(t[idx0:])
                all_pv.extend(pv[idx0 * 6 :])

        time.sleep(DELAY_BETWEEN_CALLS_S)

        if chunk_stop == start_dt:
            break
        start_dt = chunk_stop

    if len(all_t) < 2 or len(all_pv) != len(all_t) * 6:
        raise RuntimeError("Chunked parse produced invalid output.")

    return all_t, all_pv, sig_any


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def main():
    repo_root = Path(__file__).resolve().parents[2]
    out_root = repo_root / "NASA_Data" / "events_in_our_solar_system" / "output" / "juno"
    ephem_dir = out_root / "ephemeris"
    ensure_dir(ephem_dir)

    stop_time = stop_time_today_00z()

    # Planets (coarse grid, shared by the visualization)
    t_ref = None
    objects = {}
    sig_any = {}

    for name, spkid in MAJOR_BODIES:
        t, pv, sig = horizons_vectors_chunked(spkid, LAUNCH_TIME, stop_time, "5 d")
        if t_ref is None:
            t_ref = t
        else:
            if len(t) != len(t_ref):
                raise RuntimeError(f"Time grid mismatch: {name}")
            for a, b in zip(t, t_ref):
                if abs(a - b) > 1e-10:
                    raise RuntimeError(f"Time grid mismatch: {name}")

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
    write_json(ephem_dir / "planets_5d.json", planets_json)

    # Juno (daily)
    t_juno, pv_juno, sig_juno = horizons_vectors_chunked(JUNO_ID, LAUNCH_TIME, stop_time, "1 d")
    juno_json = {
        "schema": "mcs-ephem-v1",
        "t_jd": t_juno,
        "pv": pv_juno,
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
            "object": {"name": "Juno", "spkid": JUNO_ID},
            "signature": sig_juno,
        },
    }
    write_json(ephem_dir / "juno_1d.json", juno_json)

    # Higher-resolution windows for key milestones
    windows = [
        (
            "juno_earth_ga_10m",
            "2013-10-09 12:00:00",
            "2013-10-10 06:00:00",
            "10 m",
            "juno_earth_ga_10m.json",
        ),
        (
            "juno_jupiter_orbit_insertion_10m",
            "2016-07-04 18:00:00",
            "2016-07-05 12:00:00",
            "10 m",
            "juno_jupiter_orbit_insertion_10m.json",
        ),
    ]

    for ds_id, s, e, step, fn in windows:
        t_hi, pv_hi, sig_hi = horizons_vectors_chunked(JUNO_ID, s, e, step)
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
                "object": {"name": "Juno", "spkid": JUNO_ID},
                "window": {"start": s, "stop": e, "step": step},
                "signature": sig_hi,
            },
        }
        write_json(ephem_dir / fn, hi_json)

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
            {"id": "juno_1d", "file": "ephemeris/juno_1d.json"},
            {"id": "juno_earth_ga_10m", "file": "ephemeris/juno_earth_ga_10m.json"},
            {"id": "juno_jupiter_orbit_insertion_10m", "file": "ephemeris/juno_jupiter_orbit_insertion_10m.json"},
        ],
    }
    write_json(out_root / "manifest.json", manifest)


if __name__ == "__main__":
    main()

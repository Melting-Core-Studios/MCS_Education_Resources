import math
import os
import re
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests

# Ephemeris generator for the MCS Education "Events in our Solar System" framework.
#
# Explorer 1 (NORAD 4 / 1958-001A) does not have a widely-available public JPL/NAIF spacecraft ephemeris
# comparable to deep-space missions. This generator therefore:
#   1) fetches barycentric Earth state vectors from JPL Horizons (spkid=399), and
#   2) adds a two-body Keplerian Earth-orbit approximation based on published perigee/apogee/inclination.
#
# The resulting ephemeris is suitable for educational visualization and is explicitly marked as an approximation.

HORIZONS_URL = "https://ssd.jpl.nasa.gov/api/horizons.api"
UA = "MCS-Education-EphemerisBot/1.3"

CENTER = "@0"          # Solar system barycenter
REF_SYSTEM = "ICRF"
REF_PLANE = "FRAME"
OUT_UNITS = "AU-D"
TIME_TYPE = "UT"
VEC_TABLE = 2          # State vectors
CSV_FORMAT = "YES"
MAX_SAMPLES_PER_CALL = 2000  # Safety bound for Horizons API


# Explorer 1 key dates
LAUNCH_TIME = "1958-02-01 03:48:00"   # UTC (seconds are historically reported with small variations)
STOP_TIME = "1970-03-31 00:00:00"     # UTC (reentry date)

# Major bodies (for background visualization)
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

# Orbit approximation inputs (from NASA "Explorer 1 Overview")
# Perigee: 354 km, Apogee: 2515 km, Inclination: 33.24°, Period: 114.8 min
MU_EARTH_KM3_S2 = 398600.4418
R_EARTH_KM = 6378.137
PERIGEE_ALT_KM = 354.0
APOGEE_ALT_KM = 2515.0
INCLINATION_DEG = 33.24

AU_KM = 149597870.7


def q(s: str) -> str:
    # Horizons API expects many params quoted (including COMMAND/CENTER/etc)
    return f"'{s}'"


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def write_json(path: Path, obj) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(_json_dumps(obj), encoding="utf-8")
    tmp.replace(path)


def _json_dumps(obj) -> str:
    import json
    return json.dumps(obj, indent=2, ensure_ascii=False)


def parse_utcish(s: str) -> datetime:
    # "YYYY-MM-DD HH:MM:SS"
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)


def fmt_utcish(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def step_seconds(step: str) -> int:
    m = re.match(r"^\s*(\d+)\s*([A-Za-z]+)\s*$", step)
    if not m:
        raise ValueError(f"Bad STEP_SIZE: {step}")
    n = int(m.group(1))
    u = m.group(2).lower()
    if u in ("d", "day", "days"):
        return n * 86400
    if u in ("h", "hr", "hrs", "hour", "hours"):
        return n * 3600
    if u in ("m", "min", "mins", "minute", "minutes"):
        return n * 60
    raise ValueError(f"Unsupported STEP_SIZE unit: {step}")


def parse_vectors(block: str):
    """Parse Horizons VECTORS ($$SOE..$$EOE) into (t_jd, pv_flat).

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
            # Whitespace table (rare when CSV_FORMAT not honored)
            cols = re.split(r"\s+", line)
            if len(cols) < 7:
                continue
            if not re.match(r"^\d", cols[0]):
                continue
            try:
                jd = float(cols[0])
                vals = [float(c.replace("D", "E").replace("d", "E")) for c in cols[1:7]]
            except ValueError:
                continue
            x, y, z, vx, vy, vz = vals

        t.append(jd)
        pv.extend([x, y, z, vx, vy, vz])

    if len(t) < 2 or len(pv) != len(t) * 6:
        preview = "\n".join(block.splitlines()[:25])
        raise RuntimeError("Parsed too few samples. Preview:\n" + preview)
    return t, pv


def _extract_soe_block(result: str) -> str:
    m = re.search(r"\$\$SOE\s*(.*?)\s*\$\$EOE", result, re.S)
    if not m:
        raise RuntimeError("Could not locate $$SOE/$$EOE block in Horizons output.")
    return m.group(1).strip()


def horizons_vectors_once(command: int, start_time: str, stop_time: str, step_size: str):
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
        "CSV_FORMAT": q(CSV_FORMAT),
        "TIME_TYPE": q(TIME_TYPE),
    }

    r = requests.get(HORIZONS_URL, params=params, headers={"User-Agent": UA}, timeout=90)
    r.raise_for_status()
    j = r.json()

    if "result" not in j or not isinstance(j["result"], str):
        raise RuntimeError("Horizons response missing 'result' field.")
    block = _extract_soe_block(j["result"])
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

    cur = start_dt
    while cur < stop_dt:
        chunk_stop = cur + timedelta(seconds=max_span_s)
        if chunk_stop > stop_dt:
            chunk_stop = stop_dt

        s = fmt_utcish(cur)
        e = fmt_utcish(chunk_stop)

        # Basic retry (Horizons occasionally returns transient 5xx)
        for attempt in range(4):
            try:
                t, pv, sig = horizons_vectors_once(command, s, e, step_size)
                break
            except Exception as ex:
                if attempt == 3:
                    raise
                time.sleep(2 * (attempt + 1))

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

        cur = chunk_stop

    if len(all_t) < 2 or len(all_pv) != len(all_t) * 6:
        raise RuntimeError("Chunked Horizons parse produced invalid output.")
    return all_t, all_pv, sig_any


def datetime_to_jd(dt: datetime) -> float:
    """UTC datetime -> Julian Date (UTC proxy)."""
    dt = dt.astimezone(timezone.utc)
    y = dt.year
    m = dt.month
    d = dt.day
    hh = dt.hour
    mm = dt.minute
    ss = dt.second + dt.microsecond / 1e6
    frac = (hh + (mm + ss / 60.0) / 60.0) / 24.0

    if m <= 2:
        y -= 1
        m += 12
    A = y // 100
    B = 2 - A + (A // 4)
    jd0 = math.floor(365.25 * (y + 4716)) + math.floor(30.6001 * (m + 1)) + d + B - 1524.5
    return jd0 + frac


def kepler_E(M: float, e: float) -> float:
    """Solve Kepler's equation E - e*sin(E) = M for E (radians)."""
    E = M if e < 0.8 else math.pi
    for _ in range(15):
        f = E - e * math.sin(E) - M
        fp = 1.0 - e * math.cos(E)
        dE = -f / fp
        E += dE
        if abs(dE) < 1e-12:
            break
    return E


def earth_orbit_offset_pv_au_day(jd: float, jd_epoch: float):
    """Return (x,y,z,vx,vy,vz) offset of Explorer 1 relative to Earth in AU and AU/day.

    This is a simplified two-body orbit using published perigee/apogee/inclination.
    RAAN, argument of periapsis, and mean anomaly at epoch are assumed 0.
    """
    rp = R_EARTH_KM + PERIGEE_ALT_KM
    ra = R_EARTH_KM + APOGEE_ALT_KM
    a = 0.5 * (rp + ra)
    e = (ra - rp) / (ra + rp)
    inc = math.radians(INCLINATION_DEG)

    n = math.sqrt(MU_EARTH_KM3_S2 / (a ** 3))  # rad/s
    dt_s = (jd - jd_epoch) * 86400.0
    M = (n * dt_s) % (2.0 * math.pi)

    E = kepler_E(M, e)
    cE = math.cos(E)
    sE = math.sin(E)

    r = a * (1.0 - e * cE)
    # Position in perifocal frame
    x_p = a * (cE - e)
    y_p = a * math.sqrt(1.0 - e * e) * sE

    # Velocity in perifocal frame (km/s)
    fac = math.sqrt(MU_EARTH_KM3_S2 * a) / r
    vx_p = -fac * sE
    vy_p = fac * math.sqrt(1.0 - e * e) * cE

    # Rotate to ECI with Ω=0, ω=0: only inclination about x-axis
    x = x_p
    y = y_p * math.cos(inc)
    z = y_p * math.sin(inc)

    vx = vx_p
    vy = vy_p * math.cos(inc)
    vz = vy_p * math.sin(inc)

    # Convert to AU and AU/day
    x_au = x / AU_KM
    y_au = y / AU_KM
    z_au = z / AU_KM
    vx_au_d = (vx * 86400.0) / AU_KM
    vy_au_d = (vy * 86400.0) / AU_KM
    vz_au_d = (vz * 86400.0) / AU_KM

    return x_au, y_au, z_au, vx_au_d, vy_au_d, vz_au_d


def add_offsets(t_jd, pv_earth, jd_epoch):
    if len(pv_earth) != len(t_jd) * 6:
        raise RuntimeError("Earth pv length mismatch.")
    pv_out = []
    for i, jd in enumerate(t_jd):
        ex, ey, ez, evx, evy, evz = earth_orbit_offset_pv_au_day(jd, jd_epoch)
        bx = pv_earth[i * 6 + 0] + ex
        by = pv_earth[i * 6 + 1] + ey
        bz = pv_earth[i * 6 + 2] + ez
        bvx = pv_earth[i * 6 + 3] + evx
        bvy = pv_earth[i * 6 + 4] + evy
        bvz = pv_earth[i * 6 + 5] + evz
        pv_out.extend([bx, by, bz, bvx, bvy, bvz])
    return pv_out


def main():
    repo_root = Path(__file__).resolve().parents[2]
    out_root = repo_root / "NASA_Data" / "events_in_our_solar_system" / "output" / "explorer_one"
    ephem_dir = out_root / "ephemeris"
    ensure_dir(ephem_dir)

    launch_dt = parse_utcish(LAUNCH_TIME)
    jd_epoch = datetime_to_jd(launch_dt)

    stop_time = STOP_TIME

    # Planets (coarse grid)
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

    # Earth vectors (daily) -> Explorer 1 barycentric (daily)
    t_e1d, pv_e1d, sig_e1d = horizons_vectors_chunked(399, LAUNCH_TIME, stop_time, "1 d")
    pv_x1d = add_offsets(t_e1d, pv_e1d, jd_epoch)

    explorer_1d = {
        "schema": "mcs-ephem-v1",
        "t_jd": t_e1d,
        "pv": pv_x1d,
        "meta": {
            "generated_at": now_iso(),
            "source": {"name": "JPL Horizons (Earth 399) + Derived Keplerian orbit", "service": HORIZONS_URL},
            "frame": {
                "center": CENTER,
                "ref_system": REF_SYSTEM,
                "ref_plane": REF_PLANE,
                "out_units": OUT_UNITS,
                "time_type": TIME_TYPE,
                "vec_table": VEC_TABLE,
            },
            "object": {
                "name": "Explorer 1 (approximate)",
                "basis": {
                    "earth_spkid": 399,
                    "perigee_km": PERIGEE_ALT_KM,
                    "apogee_km": APOGEE_ALT_KM,
                    "inclination_deg": INCLINATION_DEG,
                    "assumptions": "Two-body Earth orbit; RAAN=0, argp=0, M0=0; no drag/perturbations.",
                },
            },
            "signature": {"earth": sig_e1d, "derived": {"method": "two-body-kepler", "units": "AU-D"}},
        },
    }
    write_json(ephem_dir / "explorer_one_1d.json", explorer_1d)

    # Higher resolution: first 10 days at 10 minutes (shows orbiting behavior in trails)
    end_10d = launch_dt + timedelta(days=10)
    stop_10d = fmt_utcish(end_10d)

    t_e10m, pv_e10m, sig_e10m = horizons_vectors_chunked(399, LAUNCH_TIME, stop_10d, "10 m")
    pv_x10m = add_offsets(t_e10m, pv_e10m, jd_epoch)

    explorer_10m = {
        "schema": "mcs-ephem-v1",
        "t_jd": t_e10m,
        "pv": pv_x10m,
        "meta": {
            "generated_at": now_iso(),
            "source": {"name": "JPL Horizons (Earth 399) + Derived Keplerian orbit", "service": HORIZONS_URL},
            "frame": {
                "center": CENTER,
                "ref_system": REF_SYSTEM,
                "ref_plane": REF_PLANE,
                "out_units": OUT_UNITS,
                "time_type": TIME_TYPE,
                "vec_table": VEC_TABLE,
            },
            "object": {
                "name": "Explorer 1 (approximate, first 10 days)",
                "basis": {
                    "earth_spkid": 399,
                    "perigee_km": PERIGEE_ALT_KM,
                    "apogee_km": APOGEE_ALT_KM,
                    "inclination_deg": INCLINATION_DEG,
                    "assumptions": "Two-body Earth orbit; RAAN=0, argp=0, M0=0; no drag/perturbations.",
                },
            },
            "signature": {"earth": sig_e10m, "derived": {"method": "two-body-kepler", "units": "AU-D"}},
        },
    }
    write_json(ephem_dir / "explorer_one_first10d_10m.json", explorer_10m)

    # Manifest
    manifest = {
        "schema": "mcs-ephem-manifest-v1",
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
            {"id": "explorer_one_1d", "file": "ephemeris/explorer_one_1d.json"},
            {"id": "explorer_one_first10d_10m", "file": "ephemeris/explorer_one_first10d_10m.json"},
        ],
    }
    write_json(out_root / "manifest.json", manifest)


if __name__ == "__main__":
    main()

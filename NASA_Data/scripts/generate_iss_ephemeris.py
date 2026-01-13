#!/usr/bin/env python3
"""
Generate an ISS (ZARYA) "live" ephemeris dataset using the latest public TLE and SGP4 propagation.

Outputs:
  NASA_Data/events_in_our_solar_system/output/iss/manifest.json
  NASA_Data/events_in_our_solar_system/output/iss/ephemeris/iss_1m.json

Manifest also references the existing planets_5d dataset from voyager1 output using a relative path,
so the app can reuse the already-generated major-body ephemerides without duplication.

Data sources:
  - CelesTrak ISS (25544) TLE: https://celestrak.org/NORAD/elements/gp.php?CATNR=25544&FORMAT=TLE
  - SGP4 propagation via the 'sgp4' Python library (Vallado et al.)

Notes:
  - The propagated state vectors are in the TEME frame (as returned by SGP4). In-app we treat these as Earth-relative
    and add Earth's barycentric ephemeris to render within the solar-system scene. This is an approximation sufficient
    for educational visualization at typical zoom levels.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
import math
import requests

from sgp4.api import Satrec, jday


TLE_URL = "https://celestrak.org/NORAD/elements/gp.php?CATNR=25544&FORMAT=TLE"

SCHEMA_EPHEM = "mcs-ephem-v1"
SCHEMA_MANIFEST = "mcs-ephem-manifest-v1"

# 1-minute samples over this horizon
HORIZON_HOURS = 24
STEP_SECONDS = 60


def repo_root() -> Path:
    # This script is intended to live at: NASA_Data/scripts/generate_iss_ephemeris.py
    # parents[2] -> <repo_root>
    return Path(__file__).resolve().parents[2]


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def fetch_tle() -> tuple[str, str, str]:
    r = requests.get(TLE_URL, timeout=20)
    r.raise_for_status()
    lines = [ln.strip() for ln in r.text.splitlines() if ln.strip()]
    if len(lines) < 3:
        raise RuntimeError("TLE fetch returned too few lines.")
    # Many CelesTrak feeds return: name + line1 + line2
    name, l1, l2 = lines[0], lines[1], lines[2]
    if not (l1.startswith("1 ") and l2.startswith("2 ")):
        # Try to recover if the feed omits name
        if len(lines) >= 2 and lines[0].startswith("1 ") and lines[1].startswith("2 "):
            name, l1, l2 = "ISS (ZARYA)", lines[0], lines[1]
        else:
            raise RuntimeError("Unexpected TLE format.")
    return name, l1, l2


def datetime_to_jd(dt: datetime) -> float:
    dt = dt.astimezone(timezone.utc)
    jd, fr = jday(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second + dt.microsecond * 1e-6)
    return float(jd + fr)


def build_samples(now_utc: datetime) -> list[datetime]:
    # Start at "now" rounded down to the minute for stable outputs
    start = now_utc.replace(second=0, microsecond=0)
    end = start + timedelta(hours=HORIZON_HOURS)
    samples: list[datetime] = []
    t = start
    while t <= end:
        samples.append(t)
        t += timedelta(seconds=STEP_SECONDS)
    return samples


def main() -> None:
    root = repo_root()
    out_root = root / "NASA_Data" / "events_in_our_solar_system" / "output" / "iss"
    ephem_dir = out_root / "ephemeris"
    ensure_dir(ephem_dir)

    name, l1, l2 = fetch_tle()
    sat = Satrec.twoline2rv(l1, l2)

    now = datetime.now(timezone.utc)
    samples_dt = build_samples(now)

    t_jd: list[float] = []
    pv: list[float] = []  # flat: x,y,z,vx,vy,vz

    for dt in samples_dt:
        jd = datetime_to_jd(dt)
        t_jd.append(jd)

        # SGP4 propagation: returns TEME position (km) and velocity (km/s)
        e, r, v = sat.sgp4(*jday(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second + dt.microsecond*1e-6))
        if e != 0:
            # If a particular sample fails, duplicate last valid state (keeps array length consistent)
            if pv:
                pv.extend(pv[-6:])
            else:
                pv.extend([0.0, 0.0, 0.0, 0.0, 0.0, 0.0])
            continue

        pv.extend([float(r[0]), float(r[1]), float(r[2]), float(v[0]), float(v[1]), float(v[2])])

    ephem_file = ephem_dir / "iss_1m.json"
    ephem_payload = {
        "schema": SCHEMA_EPHEM,
        "meta": {
            "object": name,
            "source": "CelesTrak TLE + SGP4",
            "generated_utc": now.isoformat().replace("+00:00", "Z"),
            "frame": "TEME (Earth-centered, Earth-relative)",
            "units": {"pos": "km", "vel": "km/s"},
            "step_seconds": STEP_SECONDS,
            "horizon_hours": HORIZON_HOURS,
            "tle": {"line1": l1, "line2": l2},
        },
        "t_jd": t_jd,
        "pv": pv,
    }
    ephem_file.write_text(json.dumps(ephem_payload, separators=(",", ":"), ensure_ascii=False), encoding="utf-8")

    # Manifest: reuse planets from voyager1 output via relative path
    manifest_payload = {
        "schema": SCHEMA_MANIFEST,
        "generated_utc": now.isoformat().replace("+00:00", "Z"),
        "datasets": [
            {"id": "planets_5d", "file": "../voyager1/ephemeris/planets_5d.json"},
            {"id": "iss_1m", "file": "ephemeris/iss_1m.json"},
        ],
    }
    (out_root / "manifest.json").write_text(json.dumps(manifest_payload, separators=(",", ":"), ensure_ascii=False), encoding="utf-8")


if __name__ == "__main__":
    main()

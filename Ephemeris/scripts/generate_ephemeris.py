import json
import os
import re
import time
from datetime import datetime, timezone

import requests

API_URL = "https://ssd.jpl.nasa.gov/api/horizons.api"

START_TIME = "1977-09-05 12:56:00"

FRAME = {
    "center": "@0",
    "ref_system": "ICRF",
    "ref_plane": "FRAME",
    "out_units": "AU-D",
    "time_type": "UT",
    "vec_table": "2",
}

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DOCS_DIR = os.path.join(REPO_ROOT, "docs")
EPHEM_DIR = os.path.join(DOCS_DIR, "ephemeris")
MANIFEST_PATH = os.path.join(DOCS_DIR, "manifest.json")

DATASETS = {
    "planets_5d": {
        "type": "multi",
        "step": "5 d",
        "objects": [
            ("Sun", "10"),
            ("Mercury", "199"),
            ("Venus", "299"),
            ("Earth", "399"),
            ("Mars", "499"),
            ("Jupiter", "599"),
            ("Saturn", "699"),
            ("Uranus", "799"),
            ("Neptune", "899"),
        ],
        "file": os.path.join(EPHEM_DIR, "planets_5d.json"),
    },
    "voyager1_1d": {
        "type": "single",
        "name": "Voyager 1",
        "command": "-31",
        "step": "1 d",
        "file": os.path.join(EPHEM_DIR, "voyager1_1d.json"),
    },
    "voyager1_jupiter_30m": {
        "type": "single",
        "name": "Voyager 1 (Jupiter encounter hi-res)",
        "command": "-31",
        "start": "1979-02-20 00:00:00",
        "stop":  "1979-03-15 00:00:00",
        "step": "30 m",
        "file": os.path.join(EPHEM_DIR, "voyager1_jupiter_30m.json"),
    },
    "voyager1_saturn_30m": {
        "type": "single",
        "name": "Voyager 1 (Saturn encounter hi-res)",
        "command": "-31",
        "start": "1980-11-01 00:00:00",
        "stop":  "1980-11-20 00:00:00",
        "step": "30 m",
        "file": os.path.join(EPHEM_DIR, "voyager1_saturn_30m.json"),
    },
}

NUMBER = r"[+-]?(?:\d+\.\d*|\d*\.\d+|\d+)(?:E[+-]?\d+)?"
ROW_RE = re.compile(
    rf"({NUMBER})\s*,\s*[^,]*,\s*({NUMBER})\s*,\s*({NUMBER})\s*,\s*({NUMBER})\s*,\s*({NUMBER})\s*,\s*({NUMBER})\s*,\s*({NUMBER})",
    re.IGNORECASE,
)

def ensure_dir(path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)

def horizons_vectors(command: str, start: str, stop: str, step: str):
    params = {
        "format": "json",
        "EPHEM_TYPE": "VECTORS",
        "MAKE_EPHEM": "YES",
        "OBJ_DATA": "NO",
        "COMMAND": command,
        "CENTER": FRAME["center"],
        "START_TIME": start,
        "STOP_TIME": stop,
        "STEP_SIZE": step,
        "REF_SYSTEM": FRAME["ref_system"],
        "REF_PLANE": FRAME["ref_plane"],
        "OUT_UNITS": FRAME["out_units"],
        "VEC_TABLE": FRAME["vec_table"],
        "CSV_FORMAT": "YES",
        "VEC_LABELS": "NO",
        "VEC_DELTA_T": "NO",
        "VEC_CORR": "NONE",
        "TIME_TYPE": FRAME["time_type"],
    }

    r = requests.get(API_URL, params=params, timeout=120)
    r.raise_for_status()
    j = r.json()

    signature = j.get("signature", {})
    result = j.get("result", "")

    if "$$SOE" not in result or "$$EOE" not in result:
        raise RuntimeError("Horizons response missing $$SOE/$$EOE block.")

    block = result.split("$$SOE", 1)[1].split("$$EOE", 1)[0]
    flat = " ".join(block.replace("\r", "\n").split("\n"))

    t_jd = []
    pv = []

    for m in ROW_RE.finditer(flat):
        jd = float(m.group(1))
        x  = float(m.group(2))
        y  = float(m.group(3))
        z  = float(m.group(4))
        vx = float(m.group(5))
        vy = float(m.group(6))
        vz = float(m.group(7))
        t_jd.append(jd)
        pv.extend([x, y, z, vx, vy, vz])

    if len(t_jd) < 2:
        raise RuntimeError("Parsed too few samples from Horizons output.")

    return t_jd, pv, signature

def write_json(path: str, obj: dict):
    ensure_dir(path)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, separators=(",", ":"), ensure_ascii=False)
    os.replace(tmp, path)

def main():
    now_utc = datetime.now(timezone.utc)
    stop_time = now_utc.strftime("%Y-%m-%d 00:00:00")
    generated_at = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

    os.makedirs(EPHEM_DIR, exist_ok=True)

    manifest = {
        "schema": "mcs-ephem-manifest-v1",
        "generated_at": generated_at,
        "source": {
            "provider": "JPL Horizons (via ssd.jpl.nasa.gov API)",
            "endpoint": API_URL,
        },
        "frame": FRAME,
        "datasets": []
    }

    last_signature = None

    planets = DATASETS["planets_5d"]
    t_ref = None
    objects_out = {}

    for (name, cmd) in planets["objects"]:
        t_jd, pv, sig = horizons_vectors(cmd, START_TIME, stop_time, planets["step"])
        last_signature = sig
        if t_ref is None:
            t_ref = t_jd
        elif len(t_jd) != len(t_ref):
            raise RuntimeError("Time grid mismatch in multi ephemeris.")
        objects_out[cmd] = {"name": name, "command": cmd, "pv": pv}
        time.sleep(0.25)

    write_json(planets["file"], {
        "schema": "mcs-ephem-multi-v1",
        "meta": {
            "generated_at": generated_at,
            "frame": FRAME,
            "step": planets["step"],
            "signature": last_signature or {},
        },
        "t_jd": t_ref,
        "objects": objects_out,
    })

    manifest["datasets"].append({
        "id": "planets_5d",
        "file": "ephemeris/planets_5d.json",
        "start": START_TIME,
        "stop": stop_time,
        "step": planets["step"],
        "objects": [n for (n, _) in planets["objects"]],
    })

    for ds_id in ["voyager1_1d", "voyager1_jupiter_30m", "voyager1_saturn_30m"]:
        ds = DATASETS[ds_id]
        s = ds.get("start", START_TIME)
        e = ds.get("stop", stop_time)

        t_jd, pv, sig = horizons_vectors(ds["command"], s, e, ds["step"])

        write_json(ds["file"], {
            "schema": "mcs-ephem-v1",
            "meta": {
                "generated_at": generated_at,
                "frame": FRAME,
                "step": ds["step"],
                "name": ds["name"],
                "command": ds["command"],
                "signature": sig or {},
            },
            "t_jd": t_jd,
            "pv": pv,
        })

        manifest["datasets"].append({
            "id": ds_id,
            "file": f"ephemeris/{os.path.basename(ds['file'])}",
            "start": s,
            "stop": e,
            "step": ds["step"],
            "object": ds["name"],
        })

        time.sleep(0.25)

    write_json(MANIFEST_PATH, manifest)

if __name__ == "__main__":
    main()

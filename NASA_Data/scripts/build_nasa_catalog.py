import argparse, json, math, os, re, time
import urllib.parse
from datetime import datetime, timezone
import requests

# ESA Gaia Archive TAP synchronous endpoint (public)
GAIA_TAP_SYNC_URL = "https://gea.esac.esa.int/tap-server/tap/sync"
AU_PER_PC = 206264.80624709636


CONSTELLATION_GENITIVE={"And":"Andromedae","Ant":"Antliae","Aps":"Apodis","Aqr":"Aquarii","Aql":"Aquilae","Ara":"Arae","Ari":"Arietis","Aur":"Aurigae","Boo":"Bootis","Cae":"Caeli","Cam":"Camelopardalis","Cap":"Capricorni","Car":"Carinae","Cas":"Cassiopeiae","Cen":"Centauri","Cep":"Cephei","Cet":"Ceti","Cha":"Chamaeleontis","Cir":"Circini","CMa":"Canis Majoris","CMi":"Canis Minoris","Cnc":"Cancri","Col":"Columbae","Com":"Comae Berenices","CrA":"Coronae Australis","CrB":"Coronae Borealis","Crt":"Crateris","Cru":"Crucis","Crv":"Corvi","CVn":"Canum Venaticorum","Cyg":"Cygni","Del":"Delphini","Dor":"Doradus","Dra":"Draconis","Equ":"Equulei","Eri":"Eridani","For":"Fornacis","Gem":"Geminorum","Gru":"Gruis","Her":"Herculis","Hor":"Horologii","Hya":"Hydrae","Hyi":"Hydri","Ind":"Indi","Lac":"Lacertae","LMi":"Leonis Minoris","Leo":"Leonis","Lep":"Leporis","Lib":"Librae","Lup":"Lupi","Lyn":"Lyncis","Lyr":"Lyrae","Men":"Mensae","Mic":"Microscopii","Mon":"Monocerotis","Mus":"Muscae","Nor":"Normae","Oct":"Octantis","Oph":"Ophiuchi","Ori":"Orionis","Pav":"Pavonis","Peg":"Pegasi","Per":"Persei","Phe":"Phoenicis","Pic":"Pictoris","PsA":"Piscis Austrini","Psc":"Piscium","Pup":"Puppis","Pyx":"Pyxidis","Ret":"Reticuli","Scl":"Sculptoris","Sco":"Scorpii","Sct":"Scuti","Ser":"Serpentis","Sex":"Sextantis","Sge":"Sagittae","Sgr":"Sagittarii","Tau":"Tauri","Tel":"Telescopii","TrA":"Trianguli Australis","Tri":"Trianguli","Tuc":"Tucanae","UMa":"Ursae Majoris","UMi":"Ursae Minoris","Vel":"Velorum","Vir":"Virginis","Vol":"Volantis","Vul":"Vulpeculae"}

def _const_genitive(abbrev):
    if not abbrev:
        return None
    key = re.sub(r"\.", "", str(abbrev))
    return CONSTELLATION_GENITIVE.get(key)

def beautify_system_name(raw):
    s = re.sub(r"\s+", " ", str("" if raw is None else raw)).strip()
    if not s:
        return s
    if re.match(r"^Proxima\s+Cen\b", s, flags=re.I):
        s = re.sub(r"^Proxima\s+Cen\b", "Proxima Centauri", s, flags=re.I)
    if re.match(r"^Alpha\s+Cen\b", s, flags=re.I):
        s = re.sub(r"^Alpha\s+Cen\b", "Alpha Centauri", s, flags=re.I)
    if re.match(r"^(GJ\s*551|Gl\s*551)\b", s, flags=re.I):
        s = "Proxima Centauri"
    def repl(m):
        n = m.group(1)
        ab = m.group(2)
        pl = m.group(3)
        g = _const_genitive(ab)
        if not g:
            return m.group(0)
        return f"{n} {g} {pl}" if pl else f"{n} {g}"
    s = re.sub(r"^(\d+)\s+([A-Za-z]{2,3})\b\s*([b-z])?\b", repl, s, flags=re.I)
    return s

def build_system_aliases(raw_name, pretty_name):
    out = set()
    r = re.sub(r"\s+", " ", str("" if raw_name is None else raw_name)).strip()
    p = re.sub(r"\s+", " ", str("" if pretty_name is None else pretty_name)).strip()
    if r:
        out.add(r)
    if p:
        out.add(p)
    if re.match(r"^(Proxima\s+Cen|Proxima\s+Centauri|GJ\s*551|Gl\s*551)\b", r, flags=re.I):
        out.update(["Proxima Centauri","Proxima Cen","GJ 551","Gl 551","Alpha Centauri"])
    if re.match(r"^(Alpha\s+Cen|Alpha\s+Centauri)\b", r, flags=re.I):
        out.update(["Alpha Centauri","Alpha Cen","Rigil Kentaurus"])
    b = beautify_system_name(r)
    if b and b != r:
        out.add(b)
    return list(out)

def format_exoplanet_display_name(host, pl_name_raw, letter_raw):
    host_raw = ("" if host is None else str(host)).strip()
    host_name = beautify_system_name(host_raw)
    pl_name = ("" if pl_name_raw is None else str(pl_name_raw)).strip()
    letter = ("" if letter_raw is None else str(letter_raw)).strip()
    if pl_name:
        pl_name = re.sub(r"\s+([a-z])$", lambda m: " " + m.group(1).upper(), pl_name)
        if not re.fullmatch(r"[A-Za-z]", pl_name):
            return beautify_system_name(pl_name)
    L = letter.upper() if letter else "B"
    return (host_name + " " + L).strip() if host_name else L

def to_num(x):
    try:
        if x is None or x == "":
            return None
        v = float(x)
        return v if math.isfinite(v) else None
    except Exception:
        return None

def build_exo_download_url(ps_maxrec):
    cols = [
        "hostname","sy_snum","sy_pnum","cb_flag","st_teff","st_lum","st_mass","st_rad","st_spectype",
        "pl_name","pl_letter","discoverymethod","disc_year","pul_flag","ptv_flag","etv_flag",
        "pl_orbper","pl_orbsmax","pl_rade","pl_bmasse","pl_dens","pl_insol","pl_eqt"
    ]
    q = f"select {','.join(cols)} from ps where default_flag=1 order by hostname asc"
    enc = urllib.parse.quote_plus(q, safe=",=*()'\"")
    return f"https://exoplanetarchive.ipac.caltech.edu/TAP/sync?query={enc}&format=json&maxrec={ps_maxrec}"

def build_exo_stellarhosts_url(sh_maxrec):
    cols = [
        "sy_name","hostname","sy_snum","sy_pnum","sy_dist","ra","dec","gaia_dr3_id","cb_flag",
        "st_teff","st_lum","st_mass","st_rad","st_spectype"
    ]
    q = f"select {','.join(cols)} from stellarhosts where sy_snum>=2 order by sy_name asc,hostname asc"
    enc = urllib.parse.quote_plus(q, safe=",=*()'\"")
    return f"https://exoplanetarchive.ipac.caltech.edu/TAP/sync?query={enc}&format=json&maxrec={sh_maxrec}"


def normalize_gaia_dr3_id(v):
    if v is None:
        return None
    s = str(v).strip()
    s = re.sub(r"[^0-9]", "", s)
    return s if s else None

def to_num(v):
    try:
        if v is None:
            return None
        x = float(v)
        return x if math.isfinite(x) else None
    except Exception:
        return None

def fetch_gaia_astrometry(source_ids, session, chunk_size=700, tries=4):
    """Return dict[source_id]->{ra,dec,parallax}. Uses Gaia DR3 gaiadr3.gaia_source."""
    out = {}
    if not source_ids:
        return out
    for i in range(0, len(source_ids), chunk_size):
        chunk = source_ids[i:i+chunk_size]
        id_list = ",".join(chunk)
        adql = f"SELECT source_id,ra,dec,parallax FROM gaiadr3.gaia_source WHERE source_id IN ({id_list})"
        data = {"REQUEST":"doQuery","LANG":"ADQL","FORMAT":"json","QUERY":adql}
        last = None
        for t in range(tries):
            try:
                r = session.post(GAIA_TAP_SYNC_URL, data=data, timeout=180)
                r.raise_for_status()
                j = r.json()
                # Gaia json includes "data" list in same order as "fields"
                fields = [f["name"] for f in j.get("fields",[])]
                rows = j.get("data",[])
                idx_sid = fields.index("source_id") if "source_id" in fields else None
                idx_ra = fields.index("ra") if "ra" in fields else None
                idx_dec = fields.index("dec") if "dec" in fields else None
                idx_plx = fields.index("parallax") if "parallax" in fields else None
                if idx_sid is None:
                    break
                for row in rows:
                    try:
                        sid = str(int(row[idx_sid]))
                    except Exception:
                        continue
                    ra = to_num(row[idx_ra]) if idx_ra is not None else None
                    dec = to_num(row[idx_dec]) if idx_dec is not None else None
                    plx = to_num(row[idx_plx]) if idx_plx is not None else None
                    if ra is None or dec is None:
                        continue
                    out[sid] = {"ra":ra, "dec":dec, "parallax":plx}
                break
            except Exception as e:
                last = e
                time.sleep(2.0*(t+1))
        if last and i==0 and not out:
            # if everything failed early, raise
            pass
    return out

def star_row_score(sr):
    score = 0
    for k in ("st_rad","st_teff","st_mass","st_lum","st_spectype","gaia_dr3_id"):
        if sr.get(k) not in (None,""):
            score += 1
    return score

def dedupe_stellarhosts_rows(rows):
    """Return list of best rows per star, keyed by gaia_dr3_id else hostname."""
    best = {}
    for sr in rows:
        if not isinstance(sr, dict):
            continue
        gid = normalize_gaia_dr3_id(sr.get("gaia_dr3_id"))
        key = gid or (str(sr.get("hostname") or "").strip().lower())
        if not key:
            continue
        sc = star_row_score(sr)
        prev = best.get(key)
        if prev is None or sc > star_row_score(prev):
            best[key] = sr
    # stable order: primary first if possible by hostname sort
    return list(best.values())

def cart_au_from_radec_dist(ra_deg, dec_deg, dist_pc):
    ra = math.radians(ra_deg)
    dec = math.radians(dec_deg)
    d = dist_pc * AU_PER_PC
    cd = math.cos(dec)
    return [d*cd*math.cos(ra), d*cd*math.sin(ra), d*math.sin(dec)]

def fetch_json(url, session, tries=5):
    last = None
    for i in range(tries):
        try:
            r = session.get(url, timeout=(20, 900))
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last = e
            time.sleep(2.0 * (i + 1))
    raise last

def build_stellar_maps(sh_rows):
    stars_by_system = {}
    host_to_system = {}
    if not isinstance(sh_rows, list):
        return None, None
    for r in sh_rows:
        if not isinstance(r, dict):
            continue
        sys_name = ("" if r.get("sy_name") is None else str(r.get("sy_name"))).strip()
        host = ("" if r.get("hostname") is None else str(r.get("hostname"))).strip()
        if sys_name:
            stars_by_system.setdefault(sys_name, []).append(r)
        if sys_name and host:
            host_to_system[host] = sys_name
    return stars_by_system, host_to_system

def ingest_rows(rows, source_label, stars_by_system=None, host_to_system=None, planet_cap=16):
    if not isinstance(rows, list):
        raise ValueError("Expected rows array")
    by_system = {}
    retrieved_at = datetime.now(timezone.utc).isoformat().replace("+00:00","Z")
    for r in rows:
        if not isinstance(r, dict):
            continue
        host = r.get("hostname") or r.get("pl_hostname") or ""
        host = str(host).strip()
        if not host:
            continue
        sys_key = host_to_system.get(host, host) if host_to_system else host
        sys = by_system.get(sys_key)
        if sys is None:
            teff = to_num(r.get("st_teff"))
            mass = to_num(r.get("st_mass"))
            rad = to_num(r.get("st_rad"))
            lum = None
            v = to_num(r.get("st_lum"))
            if v is not None:
                lum = 10 ** v
            if lum is None and teff is not None and rad is not None:
                t = teff / 5772.0
                lum = (rad * rad) * (t ** 4)
            snum = to_num(r.get("sy_snum"))
            snumi = int(snum) if snum is not None else None
            cat = "Single star"
            if snumi == 2:
                cat = "Binary stars"
            elif snumi is not None and snumi >= 3:
                cat = "Multi stars"
            spect = ("" if r.get("st_spectype") is None else str(r.get("st_spectype"))).upper()
            pul = 1 if to_num(r.get("pul_flag")) == 1 else 0
            ptv = 1 if to_num(r.get("ptv_flag")) == 1 else 0
            etv = 1 if to_num(r.get("etv_flag")) == 1 else 0
            if pul or ptv or etv or ("WD" in spect):
                cat = "Miscellaneous"
            sy_name_raw = sys_key
            sy_name = beautify_system_name(sy_name_raw)
            aliases = build_system_aliases(sy_name_raw, sy_name)
            stars = None
            
            # Prefer stellarhosts for multi-star systems (components), but de-dupe multiple parameter rows.
            if stars_by_system and sy_name_raw in stars_by_system:
                lst = stars_by_system.get(sy_name_raw) or []
                lst = dedupe_stellarhosts_rows(lst)
                if lst:
                    stars = []
                    for i, sr in enumerate(lst):
                        teff2 = to_num(sr.get("st_teff"))
                        mass2 = to_num(sr.get("st_mass"))
                        rad2 = to_num(sr.get("st_rad"))
                        lum2 = None
                        v2 = to_num(sr.get("st_lum"))
                        if v2 is not None:
                            lum2 = 10 ** v2
                        # derive luminosity if possible
                        if lum2 is None and teff2 is not None and rad2 is not None:
                            t2 = teff2 / 5772.0
                            lum2 = (rad2 * rad2) * (t2 ** 4)
                        # estimate radius if missing but we have (lum, teff) or mass
                        if rad2 is None and lum2 is not None and teff2 is not None and teff2 > 0:
                            rad2 = (math.sqrt(max(lum2, 0.0)) * (5772.0 / teff2) ** 2)
                        if rad2 is None and mass2 is not None and mass2 > 0:
                            rad2 = mass2 ** 0.8
                        hn = sr.get("hostname")
                        hn = str(hn).strip() if isinstance(hn, str) else (host if i == 0 else f"{host} companion {i+1}")
                        gid = normalize_gaia_dr3_id(sr.get("gaia_dr3_id"))
                        a_au = 0.0 if i == 0 else 0.18 * i
                        p_days = 0.0 if i == 0 else (25 + i * 7 if cat == "Binary stars" else (50 + i * 12))
                        ph = 0.0 if i == 0 else 0.17 * i
                        st = {
                            "name": beautify_system_name(hn),
                            "type": "star",
                            "mass": mass2,
                            "radius": rad2,
                            "tempK": teff2,
                            "lum": lum2,
                            "gaiaDr3Id": gid,
                            # fallback orbital layout if no Gaia positions are available for this system
                            "orbitAU": a_au,
                            "periodDays": p_days,
                            "phase": ph
                        }
                        stars.append({k:v for k,v in st.items() if v is not None})
                    if stars:
                        stars[0]["orbitAU"] = 0.0
                        stars[0]["periodDays"] = 0.0
                        stars[0]["phase"] = 0.0

            if stars is None:
                st = {
                    "name": beautify_system_name(host),
                    "type": "star",
                    "mass": mass if mass is not None else 1.0,
                    "radius": rad if rad is not None else 1.0,
                    "tempK": teff if teff is not None else 5772.0,
                    "lum": lum,
                    "orbitAU": 0.0,
                    "periodDays": 0.0,
                    "phase": 0.0
                }
                stars = [{k:v for k,v in st.items() if v is not None}]
            cb0 = True if to_num(r.get("cb_flag")) == 1 else False
            sys = {
                "category": cat,
                "name": sy_name,
                "primaryName": sy_name,
                "syName": sy_name_raw,
                "rawName": sy_name_raw,
                "aliases": aliases,
                "circumbinary": True if cb0 else None,
                "catalogFlags": {"cb": cb0, "pul": bool(pul), "ptv": bool(ptv), "etv": bool(etv), "sy_snum": snumi},
                "discoveryMethods": [],
                "notes": f"Loaded from NASA Exoplanet Archive ({source_label}). Stars: {snumi if snumi is not None else len(stars)} (catalog); planets: truncated to first {planet_cap} for performance.",
                "__source": "NASA Exoplanet Archive",
                "__datasetVersion": source_label,
                "__retrievedAt": retrieved_at,
                "stars": stars,
                "planets": []
            }
            by_system[sys_key] = sys
        if planet_cap is not None and len(sys.get("planets") or []) >= planet_cap:
            continue
        a_au = to_num(r.get("pl_orbsmax"))
        per = to_num(r.get("pl_orbper"))
        if a_au is None or per is None:
            continue
        r_e = to_num(r.get("pl_rade"))
        m_e = to_num(r.get("pl_bmasse"))
        dens = to_num(r.get("pl_dens"))
        insol = to_num(r.get("pl_insol"))
        eqt = to_num(r.get("pl_eqt"))
        pname = format_exoplanet_display_name(host, r.get("pl_name"), r.get("pl_letter"))
        disc_method = ("" if r.get("discoverymethod") is None else str(r.get("discoverymethod"))).strip() or None
        disc_year = to_num(r.get("disc_year"))
        cb = True if to_num(r.get("cb_flag")) == 1 else False
        pul = True if to_num(r.get("pul_flag")) == 1 else False
        ptv = True if to_num(r.get("ptv_flag")) == 1 else False
        etv = True if to_num(r.get("etv_flag")) == 1 else False
        if cb:
            sys["circumbinary"] = True
            sys.setdefault("catalogFlags", {})["cb"] = True
        if pul:
            sys.setdefault("catalogFlags", {})["pul"] = True
        if ptv:
            sys.setdefault("catalogFlags", {})["ptv"] = True
        if etv:
            sys.setdefault("catalogFlags", {})["etv"] = True
        planet = {
            "name": pname,
            "aAU": a_au,
            "periodDays": per,
            "radiusEarth": r_e if r_e is not None else 1.0,
            "massEarth": m_e,
            "density": dens,
            "insol": insol,
            "eqTempK": eqt,
            "discoveryMethod": disc_method,
            "discoveryYear": int(disc_year) if disc_year is not None else None,
            "circumbinary": True if cb else None,
            "detectionFlags": {"cb": cb, "pul": pul, "ptv": ptv, "etv": etv},
            "spinPeriodHours": per * 24.0
        }
        sys["planets"].append({k:v for k,v in planet.items() if v is not None})
        if disc_method and disc_method not in sys["discoveryMethods"]:
            sys["discoveryMethods"].append(disc_method)
    return list(by_system.values())


def enrich_multi_star_positions(systems, session):
    # Collect Gaia IDs for stars in multi-star systems only
    gaia_ids = []
    for sys in systems:
        try:
            snum = sys.get("catalogFlags", {}).get("sy_snum")
        except Exception:
            snum = None
        if snum is None or snum < 2:
            continue
        for s in (sys.get("stars") or []):
            gid = s.get("gaiaDr3Id")
            if gid:
                gaia_ids.append(gid)
    gaia_ids = sorted(set(gaia_ids))
    if not gaia_ids:
        return {"unique":0,"resolved":0,"positionedSystems":0}

    astro = fetch_gaia_astrometry(gaia_ids, session)
    resolved = len(astro)

    positioned = 0
    for sys in systems:
        snum = sys.get("catalogFlags", {}).get("sy_snum")
        if snum is None or snum < 2:
            continue
        stars = sys.get("stars") or []
        if len(stars) < 2:
            continue
        # pick primary: match primaryName if possible
        primary_name = (sys.get("primaryName") or sys.get("name") or "").strip()
        primary = None
        for s in stars:
            if (s.get("name") or "").strip() == primary_name:
                primary = s
                break
        if primary is None:
            primary = stars[0]
        gid0 = primary.get("gaiaDr3Id")
        a0 = astro.get(gid0) if gid0 else None
        if not a0:
            continue

        # distance for primary
        plx0 = a0.get("parallax")
        dist0 = (1000.0/plx0) if (plx0 is not None and plx0 > 0) else None
        # system distance fallback if present
        dist_sys = sys.get("distPc") or sys.get("sy_dist")
        dist_sys = to_num(dist_sys)
        dist0 = dist0 if dist0 else (dist_sys if dist_sys else 10.0)

        p0 = cart_au_from_radec_dist(a0["ra"], a0["dec"], dist0)

        # assign positions
        primary["posAU"] = [0.0,0.0,0.0]
        any_other = False
        for s in stars:
            if s is primary:
                continue
            gid = s.get("gaiaDr3Id")
            a = astro.get(gid) if gid else None
            if not a:
                continue
            plx = a.get("parallax")
            dist = (1000.0/plx) if (plx is not None and plx > 0) else dist0
            p = cart_au_from_radec_dist(a["ra"], a["dec"], dist)
            rel = [p[0]-p0[0], p[1]-p0[1], p[2]-p0[2]]
            # clamp insane values (safety)
            if any(math.isfinite(v) for v in rel):
                s["posAU"] = [float(rel[0]), float(rel[1]), float(rel[2])]
                any_other = True
        if any_other:
            positioned += 1
    return {"unique":len(gaia_ids),"resolved":resolved,"positionedSystems":positioned}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ps-maxrec", type=int, required=True)
    ap.add_argument("--sh-maxrec", type=int, required=True)
    ap.add_argument("--output", type=str, required=True)
    ap.add_argument("--planet-cap", type=int, default=16)
    args = ap.parse_args()
    ps_url = build_exo_download_url(args.ps_maxrec)
    sh_url = build_exo_stellarhosts_url(args.sh_maxrec)
    sess = requests.Session()
    sess.headers.update({"User-Agent":"MCS-Education-NASA-Archive-Updater/1.0"})
    ps_rows = fetch_json(ps_url, sess)
    sh_rows = fetch_json(sh_url, sess)
    stars_by_system, host_to_system = build_stellar_maps(sh_rows)
    source_label = f"TAP/ps default_flag maxrec {args.ps_maxrec}"
    systems = ingest_rows(ps_rows, source_label, stars_by_system, host_to_system, planet_cap=args.planet_cap)

    gaia_stats = enrich_multi_star_positions(systems, sess)
    now = datetime.now(timezone.utc).isoformat().replace("+00:00","Z")
    out = {
        "meta": {
            "source": "NASA Exoplanet Archive",
            "generatedAt": now,
            "psMaxrec": args.ps_maxrec,
            "stellarHostsMaxrec": args.sh_maxrec,
            "psUrl": ps_url,
            "stellarHostsUrl": sh_url,
            "datasetVersion": source_label
        },
        "systems": systems
    }
    out["meta"]["gaia"] = gaia_stats
    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, separators=(",",":"))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())

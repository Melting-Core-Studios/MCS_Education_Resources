# Solar System Explorer — Custom Catalog JSON Format

This document describes the JSON structure accepted by the [**MCS Education: Solar System Explorer**](htps://www.meltingcorestudios.com/education/astronomy/solar_system_explorer) when loading a dataset via:

- **Tools → Data → Load from URL**
- **Tools → Data → Load from File**

This method allows you to create a custom JSON file for your own dataset and load it into our app for further study. Our app automatically attempts to calculate certain points, such as the habitable zone of a selected solar system.

## 1) File shape

You can provide either:

### A) An array of systems

```json
[
  { "name": "My System", "stars": [ ... ], "planets": [ ... ] },
  { "name": "Another System", "stars": [ ... ] }
]
```

### B) An object with `meta` and `systems`

```json
{
  "meta": {
    "datasetVersion": "my-dataset-2026-01-11",
    "generatedAt": "2026-01-11T12:00:00Z",
    "source": "My Observatory"
  },
  "systems": [
    { "name": "My System", "stars": [ ... ], "planets": [ ... ] }
  ]
}
```

Notes:

- Only a system name is required; unknown fields are preserved.
- For performance, the renderer targets **up to 16 bodies** total (planets + moons).

---

## 2) System object

### Required

- `name` (string)

### Recommended

- `stars` (array): if omitted, the app creates a single star.
- `planets` (array): can be empty.
- `primaryName` (string)
- `aliases` (string[]): improves search.
- `category` (string): `"Single star" | "Binary stars" | "Multi stars" | "Miscellaneous"`

### Optional (common)

- `displayName` (string)
- `notes` (string)
- `circumbinary` (boolean)
- `catalogFlags` (object): typically `cb`, `pul`, `ptv`, `etv`, `sy_snum`
- `discoveryMethods` (string[])
- `habitable` (object):
  - `mode`: `"circumprimary" | "circumbinary" | "none"`
  - `overrideAU`: `{ "inner": number, "outer": number }`

---

## 3) Star object

### Required

- `name` (string)

### Supported types

- `"star"` (default)
- `"white_dwarf"`
- `"neutron_star"`
- `"black_hole"`

### Optional physical parameters

- `mass` (solar masses)
- `radius` (solar radii)
- `tempK` (Kelvin)
- `lum` (solar luminosities)

### Orbit / placement

You may use either:

**Orbit form**

```json
{
  "name": "Alpha Centauri B",
  "orbit": { "aAU": 23.4, "periodDays": 29193, "phase": 0.35 }
}
```

or flattened:

- `orbitAU` (or `aAU`)
- `periodDays`
- `phase`

**Explicit 3D coordinates (recommended for accurate multi-star layouts)**

- `posAU`: `[x, y, z]` in AU (relative to barycenter or primary—be consistent)

```json
{ "name": "16 Cygni B", "posAU": [820.5, -120.3, 410.2] }
```

### Identifiers (optional)

- `gaiaDr3Id` (string)

---

## 4) Planet object

### Required (for stable rendering)

- `name` (string)
- `aAU` (number)
- `periodDays` (number)
- `radiusEarth` (number)

### Optional

- `spinPeriodHours` (number)
- `color` (array of 3 numbers, 0..1)
- `rings` (object): `innerRp`, `outerRp`, `tiltDeg`, `alpha`, `color`
- `circumbinary` (boolean)
- `discoveryMethod` (string), `discoveryYear` (number)
- `detectionFlags` (object): typically `cb`, `pul`, `ptv`, `etv`
- `moons` (array)

---

## 5) Moon object

Moons live under `planet.moons`.

Recommended fields:

- `name` (string)
- `periodDays` (number)
- `aKm` (km) **or** `aRp` (parent radii)
- `radiusKm` **or** `radiusEarth`
- `color` (RGB array, 0..1)
- `spinPeriodHours` (optional; defaults to synchronous rotation)

---

## 6) Minimal examples

### Single star

```json
{
  "name": "Demo System",
  "category": "Single star",
  "stars": [
    { "name": "Demo Star", "type": "star", "radius": 1.0, "tempK": 5772, "lum": 1.0 }
  ],
  "planets": [
    { "name": "Demo b", "aAU": 1.0, "periodDays": 365.25, "radiusEarth": 1.0 }
  ]
}
```

### Binary with explicit coordinates

```json
{
  "name": "Demo Binary",
  "category": "Binary stars",
  "stars": [
    { "name": "Demo A", "type": "star", "posAU": [0,0,0], "lum": 1.0 },
    { "name": "Demo B", "type": "star", "posAU": [0.4,0.0,0.1], "lum": 0.2 }
  ],
  "habitable": { "mode": "circumbinary" },
  "planets": [
    { "name": "Demo Binary b", "aAU": 1.8, "periodDays": 900, "radiusEarth": 3.0, "circumbinary": true }
  ]
}
```

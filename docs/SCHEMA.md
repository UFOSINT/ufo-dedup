# `ufo_public.db` — Schema Reference

Column-by-column reference for the public export. All numbers below are from a real v0.11 build (614,505 sightings, 502,985 with narrative text). Coverage percentages tell you how often a column is non-null — useful for planning queries that won't return mostly empty rows.

> **What's NOT in this file**: the long narrative text columns (`description`, `summary`, `notes`, `raw_json`) are stripped during the public export. Everything below is what survives. See `data/raw/README.md` for how to obtain the source data and rebuild from scratch if you need the full text.

## Tables at a glance

| Table | Rows | Purpose |
|---|---:|---|
| `sighting` | 614,505 | One row per reported sighting. The main table. 69 columns. |
| `location` | 214,782 | Deduplicated locations. Joined via `sighting.location_id`. 11 columns. |
| `sighting_analysis` | 614,505 | JSON side-fields per sighting. Joined via `sighting_analysis.sighting_id`. 8 columns. |
| `source_database` | 5 | Source lookup. Joined via `sighting.source_db_id`. |
| `source_collection` | 3 | Higher-level grouping above source_database. |
| `source_origin` | 31 | Upstream sources within aggregator databases. Joined via `sighting.origin_id`. |

---

## Provenance & primary keys

| Column | Type | Coverage | Notes |
|---|---|---:|---|
| `sighting.id` | INTEGER PK | 100% | Stable within a single rebuild. **Not stable across rebuilds** — the importer reassigns IDs in source-load order each time. |
| `sighting.source_db_id` | INTEGER FK→source_database | 100% | `1=MUFON`, `2=NUFORC`, `3=UFOCAT`, `4=UPDB`, `5=UFO-search`. Use this for source-comparison queries. |
| `sighting.source_record_id` | TEXT | 100% | Original ID from the source database. The pair `(source_db_id, source_record_id)` is the closest thing to a stable cross-rebuild identifier. |
| `sighting.origin_id` | INTEGER FK→source_origin | 19.3% | Set when an aggregator (UFOCAT, UPDB, UFO-search) attributes a record to an upstream source like NICAP, Blue Book, Hatch's catalog, etc. |
| `sighting.origin_record_id` | TEXT | 10.6% | Upstream-source record ID, when the aggregator preserved it. |

## Dates & times

| Column | Type | Coverage | Notes |
|---|---|---:|---|
| `sighting.date_event` | TEXT (ISO 8601) | 98.4% | Normalized event date. Format varies: `YYYY-MM-DD`, `YYYY-MM`, or `YYYY` for year-only sources. NULL means the parser couldn't extract a date at all. **Quality scoring caps NULL-date rows at 15** (relaxed to 35 for rich rows). |
| `sighting.date_event_raw` | TEXT | 100% | Source-as-imported date string. Useful for debugging the parser. |
| `sighting.date_end` | TEXT | 0.1% | Multi-day events. Almost always NULL. |
| `sighting.time_raw` | TEXT | 48.7% | Source-as-imported time string (`"21:30"`, `"9:30 PM"`, `"midnight"`, etc.). Format is whatever the source provided. |
| `sighting.timezone` | TEXT | 31.8% | Source-supplied tz string. Inconsistent format. |
| `sighting.date_reported` | TEXT | 48.4% | When the witness submitted the report. NULL for sources that don't track this (UFOCAT, UFO-search). |
| `sighting.date_posted` | TEXT | 25.9% | When the source published the report (NUFORC only — they timestamp publication separately). |
| `sighting.sighting_datetime` | TEXT | 98.4% | **Derived.** Combined ISO 8601 datetime: `date_event` + (parsed `time_raw` as `HH:MM:SS` if available). Falls back to `date_event` alone (date-only or year-only). NULL only when `date_event` is NULL. |

## Location

| Column | Type | Coverage | Notes |
|---|---|---:|---|
| `sighting.location_id` | INTEGER FK→location | 100% | Many sightings share the same location row (deduped on `raw_text`). |
| `sighting.lat` | REAL | 64.5% | **Derived.** Denormalized from `location.latitude`. Use this for map queries — avoids the JOIN. |
| `sighting.lng` | REAL | 64.5% | **Derived.** Denormalized from `location.longitude`. |
| `location.raw_text` | TEXT | 99.3% | Original location string from the source. Often `"City, ST, Country"` but format varies wildly. |
| `location.city` | TEXT | 84.6% | Parsed/normalized city name. |
| `location.county` | TEXT | 29.2% | Mostly NULL — only some sources track county. |
| `location.state` | TEXT | 75.4% | US state code, Canadian province, or other admin1 region. |
| `location.country` | TEXT | 50.1% | ISO 2-letter code (US, CA, GB, AU, etc.). NULL for many older records. |
| `location.region` | TEXT | 34.9% | Free-text region info. |
| `location.latitude` | REAL | 49.3% | Either source-supplied or geocoded via GeoNames. Use `sighting.lat`/`lng` instead — they're denormalized for speed. |
| `location.longitude` | REAL | 49.3% | Same as above. |
| `location.geocode_src` | TEXT | 20.5% | NULL = original source coords; otherwise: `geonames_exact`, `geonames_city_country`, `geonames_city_only`. |

## Observation details

| Column | Type | Coverage | Notes |
|---|---|---:|---|
| `sighting.shape` | TEXT | 38.5% | Source-supplied shape string. Free-text — many spellings ("Disc", "disc", "DISCS", "disc-shaped"). Use `standardized_shape` for queries. |
| `sighting.color` | TEXT | 10.2% | Source-supplied color, free-text. Use `primary_color` for queries. |
| `sighting.size_estimated` | TEXT | 5.6% | Free-text size description ("car-sized", "very large"). |
| `sighting.angular_size` | TEXT | 1.3% | Angular size in degrees, where reported. |
| `sighting.distance` | TEXT | 1.2% | Estimated distance from observer. |
| `sighting.duration` | TEXT | 36.6% | Free-text duration ("5 minutes", "several hours", "a moment"). |
| `sighting.duration_seconds` | INTEGER | **0%** | Reserved column. Importers don't currently parse the free-text `duration` field into seconds. See "Known gaps" below. |
| `sighting.num_objects` | INTEGER | 23.1% | Number of objects observed. |
| `sighting.num_witnesses` | INTEGER | 42.2% | Heavily weighted in `quality_score`. Tier mapping: 1 = +5pts, 2 = +10pts, 3+ = +15pts. |
| `sighting.sound` | TEXT | 4.2% | Free-text sound description. |
| `sighting.direction` | TEXT | 1.7% | Direction of travel/origin. |
| `sighting.elevation_angle` | TEXT | 1.9% | Where in the sky. |
| `sighting.viewed_from` | TEXT | 1.9% | Observer's viewing context. |

## Witness info

| Column | Type | Coverage | Notes |
|---|---|---:|---|
| `sighting.witness_age` | TEXT | 9.2% | Free-text age. Inconsistent ("adult", "teenager", "45"). |
| `sighting.witness_sex` | TEXT | 11.7% | M / F / mixed. |
| `sighting.witness_names` | TEXT | 15.9% | **Privacy note**: short labels, mostly initials/surnames or generic ("AMATEUR ASTRONOMERS"). Some full names appear. The public app exposes these in the detail modal; verify your use case before redistributing this column. |

## Classification

| Column | Type | Coverage | Notes |
|---|---|---:|---|
| `sighting.hynek` | TEXT | 43.0% | Hynek classification (NL=Nocturnal Light, DD=Daylight Disc, CE1/CE2/CE3/CE4=Close Encounter 1-4, RV=Radar/Visual, etc.). NUFORC has none natively — 102,554 NUFORC records get this from UFOCAT enrichment. Top values: NL (123k), DD (32k), CE1 (23k). |
| `sighting.vallee` | TEXT | 37.6% | Vallée classification (FB1=Fly-By, MA1=Maneuver, AN1/2/3=Anomaly, CE1/2/3/4=Close Encounter, etc.). Top values: FB1 (109k), MA1 (25k), CE1 (21k). |
| `sighting.event_type` | TEXT | 28.2% | Source-specific event type tag. |
| `sighting.svp_rating` | TEXT | 7.1% | Strangeness/Probability rating where supplied. |

## Resolution & context

| Column | Type | Coverage | Notes |
|---|---|---:|---|
| `sighting.explanation` | TEXT | 14.6% | Investigator's explanation, where the case was resolved. Short labels like `"Planet/Star - Possible"`, `"Camera Anomaly - Probable"`. |
| `sighting.characteristics` | TEXT | 21.6% | Comma-separated tags like `"Emitted other objects, Landed"`, `"Possible abduction, Missing Time"`. |
| `sighting.weather` | TEXT | 1.7% | Weather conditions. |
| `sighting.terrain` | TEXT | 19.1% | Terrain context: `"Urban"`, `"Rural-wooded"`, etc. |
| `sighting.source_ref` | TEXT | 41.0% | Original publication reference. |
| `sighting.page_volume` | TEXT | 28.7% | Page/volume citation for printed sources. |
| `sighting.created_at` | TEXT | 100% | Pipeline insert timestamp — when this row was created during the rebuild. Not the event time. |

---

## Derived analysis fields (populated by `analyze.py`)

These are computed from the raw text + structured fields during the pipeline. They're stable, machine-readable, and safe to expose publicly.

| Column | Type | Coverage | Notes |
|---|---|---:|---|
| `sighting.standardized_shape` | TEXT | 38.5% | Canonical shape from a 25-value vocabulary, fuzzy-matched via rapidfuzz. Top values: `Light` (42k), `Disc` (37k), `Other` (27k), `Sphere` (23k), `Circle` (19k), `Triangle` (18k), `Fireball` (12k). NULL when raw `shape` is NULL. |
| `sighting.primary_color` | TEXT | 38.1% | First/longest color match from a 21-color whitelist scanned against the raw text. Examples: `"red"`, `"bright white"`, `"metallic silver"`. |
| `sighting.movement_type` | TEXT | 47.3% | Coarse single-label movement: `linear` (178k), `hover` (88k), `fast` (12k), `stationary` (7k), `erratic` (5k). For richer movement detail, use `movement_categories`. |
| `sighting.has_movement_mentioned` | INTEGER (0/1) | 83.0% | 1 if narrative mentions any structured movement signal. NULL only when there's no narrative text at all. **249,217 rows = 1**. |
| `sighting.movement_categories` | TEXT (JSON array) | 83.0% | JSON array of 0+ category labels: `hovering`, `linear`, `erratic`, `accelerating`, `rotating`, `ascending`, `descending`, `vanished`, `followed`, `landed`. Example: `["hovering","vanished"]`. Empty array `"[]"` means scanned but found nothing. |
| `sighting.duration_bucket` | TEXT | **0%** | Reserved. Maps `duration_seconds` to `instant`/`seconds`/`minutes`/`hours`/`days` — but `duration_seconds` is currently 0% populated. |
| `sighting.topic_id` | INTEGER | **0%** | Reserved for v0.9 BERTopic clustering. |
| `sighting.quality_score` | INTEGER (0-100) | 100% | Weighted heuristic. **High quality (≥60): 118,320 rows (19.3%).** Distribution: 00-19=23.1%, 20-39=24.6%, 40-59=33.0%, 60-79=17.6%, 80-100=1.7%. See "Quality score formula" in main README for the weighting. NULL-date rows are capped at 15 (or 35 if rich content). |
| `sighting.richness_score` | INTEGER | 100% | Count of meaningful filled fields (description / media / witnesses / movement / structured fields / coords). Range 0-14. Used by hoax detector to decide if a "dramatic" report has any specifics. |
| `sighting.hoax_likelihood` | REAL (0.0-1.0) | 100% | Sum of weights from triggered rules, capped at 1.0. **94% of rows = 0.0**. Distributions: 0.0=577k, 0.2=23k (very short text), 0.4=7k (duplicate phrasing), 0.5=326, 0.7=76. |
| `sighting.has_description` | INTEGER (0/1) | 100% | 1 if any narrative text existed in the raw (description OR summary). Source of truth for "does this row have story behind it". |
| `sighting.has_media` | INTEGER (0/1) | 100% | 1 if narrative mentions photo/video/recording, OR an attachment row exists for this sighting. |

## Sentiment / emotion (populated by `emotions.py`)

GPU-accelerated transformer classification. Coverage is uniformly 81.9% — every sighting with sufficient narrative text gets all 12 emotion fields.

| Column | Type | Coverage | Notes |
|---|---|---:|---|
| `sighting.emotion_28_dominant` | TEXT | 81.9% | GoEmotions 28-class dominant label. Values: `neutral` (87% of classified), `realization`, `confusion`, `surprise`, `admiration`, `fear`, `joy`, `curiosity`, `excitement`, `gratitude`, `amusement`, `sadness`, `remorse`, `optimism`, `disappointment`, `love`, `approval`, `desire`, `nervousness`, `disapproval`, `annoyance`, `caring`, `disgust`, `embarrassment`, `anger`, `pride`, `relief`. (28 labels total but `grief` doesn't appear in this corpus.) |
| `sighting.emotion_28_group` | TEXT | 81.9% | Aggregated sentiment group from the 28-class label: `neutral` (87%), `ambiguous` (8%), `positive` (3.4%), `negative` (1.9%). Mapping is fixed (see `emotions.py:GOEMOTION_GROUPS`). |
| `sighting.emotion_7_dominant` | TEXT | 81.9% | 7-class RoBERTa emotion. More differentiated than the 28-class because it has fewer labels: `neutral` (31.5%), `surprise` (31.3%), `fear` (26.7%), `disgust` (6%), `sadness` (2%), `anger` (1.6%), `joy` (0.8%). |
| `sighting.vader_compound` | REAL (-1.0 to +1.0) | 81.9% | VADER lexicon-based sentiment compound score. Mean: +0.13 (slightly positive — known artifact of VADER on long observational text). |
| `sighting.roberta_sentiment` | REAL (-1.0 to +1.0) | 81.9% | RoBERTa-large sentiment, computed as `positive_prob - negative_prob`. Mean: +0.009 (near-neutral). Use this when you want a transformer-quality score; use `vader_compound` for the established baseline. |
| `sighting.emotion_7_surprise` | REAL (0.0-1.0) | 81.9% | 7-class softmax probability for `surprise`. |
| `sighting.emotion_7_fear` | REAL | 81.9% | 7-class softmax probability for `fear`. |
| `sighting.emotion_7_neutral` | REAL | 81.9% | 7-class softmax probability for `neutral`. |
| `sighting.emotion_7_anger` | REAL | 81.9% | 7-class softmax probability for `anger`. |
| `sighting.emotion_7_disgust` | REAL | 81.9% | 7-class softmax probability for `disgust`. |
| `sighting.emotion_7_sadness` | REAL | 81.9% | 7-class softmax probability for `sadness`. |
| `sighting.emotion_7_joy` | REAL | 81.9% | 7-class softmax probability for `joy`. |
| `sighting.sentiment_score` | REAL | 81.9% | Legacy: copy of `vader_compound` from the v0.8.2 era. Same values; kept for backward compatibility with older queries. |
| `sighting.dominant_emotion` | TEXT | 41.2% | Legacy: argmax of NRC keyword-classifier emotion counts (joy/fear/anger/sadness/surprise/disgust/trust/anticipation). Replaced by `emotion_7_dominant` (transformer) and `emotion_28_dominant` (more granular). Lower coverage because NRC counts are sparse. |

---

## `sighting_analysis` (JSON side-fields, joined by `sighting_id`)

Richer derived fields kept in a side table because they're JSON blobs not suited for indexing.

| Column | Type | Coverage | Notes |
|---|---|---:|---|
| `sighting_id` | INTEGER FK→sighting | 100% | Join key. One-to-one with `sighting`. |
| `behavior_tags` | TEXT (JSON array) | 83.0% | Broader than `movement_categories` — includes appearance/sound: `hovering`, `silent`, `bright`, `pulsing`, `rotating`, `zigzag`, `vanished`, `accelerated`, `split`, `merged`, `formation`, `chased`, `followed`, `landed`. 14 tags total. |
| `color_list` | TEXT (JSON array) | 38.1% | All colors found in the narrative (deduplicated, in order of first appearance). `primary_color` = first element. |
| `emotion_scores` | TEXT (JSON object) | 81.9% | Normalized NRC emotion proportions: `{"joy":0.1, "fear":0.4, ...}`. Sum=1.0. Useful for back-compatibility with legacy code that expects NRC scores. |
| `hoax_flags` | TEXT (JSON array) | 100% | Which rules fired: `["very_short_text"]`, `["duplicate_phrasing"]`, `["dramatic_no_specifics","all_caps_text"]`, etc. Empty array `"[]"` for clean rows (94% of dataset). 5 possible flags. |
| `raw_shape_matched_via` | TEXT | 38.5% | How the standardized_shape was assigned: `exact` (188k = 79% of matches), `substring` (33k = 14%), `fuzzy` (0%), `unmatched` (17k = 7% → "Other"). Useful for auditing the shape clustering. |

---

## `source_database` lookup

| id | name | description |
|---:|---|---|
| 1 | MUFON | Mutual UFO Network case reports |
| 2 | NUFORC | National UFO Reporting Center |
| 3 | UFOCAT | CUFOS UFOCAT 2023 catalog |
| 4 | UPDB | PhenomAInon Unified Phenomena Database |
| 5 | UFO-search | Geldreich's Majestic Timeline compilation |

`source_collection` groups these into PUBLIUS (MUFON+NUFORC+UPDB), GELDREICH (UFO-search), UFOCAT (just UFOCAT). Mostly internal — most queries care about `source_database.name`.

`source_origin` (31 rows) tracks upstream sources within aggregator databases — NICAP, BLUEBOOK, UFODNA, BAASS, NIDS, SKINWALKER, PILOTS, BRAZILGOV, CANADAGOV, UKTNA, Hatch, ValleeMagonia, WondersInTheSky, EberhartUFOI, etc. Joined via `sighting.origin_id`.

---

## Indexes worth knowing about

The public DB ships with indexes on the columns most queries filter on:

```
sighting(date_event)          sighting(quality_score)
sighting(source_db_id)        sighting(hoax_likelihood)
sighting(shape)               sighting(standardized_shape)
sighting(hynek)               sighting(emotion_28_dominant)
sighting(vallee)              sighting(emotion_28_group)
sighting(event_type)          sighting(emotion_7_dominant)
sighting(location_id)         sighting(dominant_emotion)
sighting(sighting_datetime)   sighting(has_description)
sighting(lat, lng)            sighting(has_media)
                              sighting(has_movement_mentioned)
location(country)
location(city)                sighting_analysis(sighting_id)
location(latitude, longitude)
```

Filters on indexed columns are O(log n). Free-text scans of `characteristics` / `explanation` / `terrain` are O(n) — write those as `LIKE '%term%'` only when you need them, or pre-filter on an indexed column first.

---

## Known gaps & caveats

- **`duration_seconds` is 0% populated.** The importers parse the free-text `duration` column but never convert "5 minutes" / "several hours" into a seconds value. This in turn leaves `duration_bucket` empty. Fixing this is upstream work in the importers (regex + heuristic parser); ticketed for v0.9.
- **`topic_id` is 0% populated.** Reserved for BERTopic-style topic modeling, deferred to v0.9.
- **`location.geoname_id` is 0% populated.** Geocoder writes coordinates but doesn't preserve the matched GeoNames row ID. Could be added in a follow-up.
- **`source_origin.description` is 0% populated.** Names only; descriptions never written.
- **GoEmotions 28-class is 87% "neutral".** Expected for factual observational text — the model was trained on Reddit comments. Use `emotion_7_dominant` for a more differentiated distribution if "neutral" is hiding signal.
- **Sighting IDs are not stable across rebuilds.** Use `(source_db_id, source_record_id)` if you need a join key that survives a re-import.
- **`witness_names` exists with 97,991 populated rows.** Mostly initials and surnames, but some full names. Same content the live app exposes via the detail modal — but a downloadable file is a different distribution surface. Filter or strip if you redistribute.

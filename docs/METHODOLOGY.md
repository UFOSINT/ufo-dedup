# Methodology — How the Algorithms Work

A "show your work" companion for researchers who want to understand, audit, or extend the algorithmic choices behind the unified database. Where the [README](../README.md) summarizes, this doc explains *why* and *how*, with examples from the actual codebase.

## 1. Cross-source deduplication

### The problem

Five aggregator databases (NUFORC, MUFON, UFOCAT, UPDB, UFO-search) overlap heavily. UFOCAT alone is itself an aggregator that includes ~123K records flagged `SOURCE=UFOReportCtr` (NUFORC re-imports). UPDB includes ~1.82M records flagged `name=MUFON` or `name=NUFORC`. Naive union would multi-count these.

### Two-phase strategy

**Phase 1 — Import-time skip.** Records known to be exact re-imports of richer originals are filtered at the importer level rather than detected post-hoc:

| Source | Skipped | Reason |
|---|---:|---|
| UFOCAT | 123,304 records (`SOURCE=UFOReportCtr`) | Copies of NUFORC; NUFORC imported separately with richer descriptions |
| UPDB | 131,506 records (`name=MUFON`) | MUFON imported separately |
| UPDB | 1,689,235 records (`name=NUFORC`) | NUFORC imported separately |

Total eliminated before dedup: **1,944,045 records**. Working set drops from ~2.56M raw to **614,505** (plus 3,811 from the r/UFOs Reddit ingest, for **618,316 total**).

The skipped UFOCAT UFOReportCtr records aren't discarded — they go to a sidecar JSONL file (`ufocat_enrichment.jsonl`) and `enrich.py` later transfers their Hynek/Vallée classifications to matching NUFORC records. This is where NUFORC's 102,554 Hynek codes and 83,710 Vallée codes come from — NUFORC has **none natively**.

**Phase 2 — Three-tier post-import matching** (`dedup.py`):

```
Tier 1: MUFON ↔ NUFORC          (date + city + state)
Tier 2a: MUFON ↔ UFOCAT          (date + city + state)
Tier 2b: NUFORC ↔ UFOCAT         (date + city + state)
Tier 2c: UPDB ↔ MUFON/NUFORC/UFOCAT (date + city only — UPDB state data is unreliable)
Tier 2d: UFO-search ↔ MUFON/NUFORC/UFOCAT (date + city + state, regex-parsed location)
Tier 3: All cross-source pairs   (date only, fuzzy description matching)
```

Each tier skips pairs already flagged by an earlier tier. Tiers 1+2 use exact match-key joins; Tier 3 is the catch-all for records that share a date but had location data different enough to miss the join.

### Similarity scoring

Every candidate pair gets a similarity score in [0.0, 1.0] from `compute_similarity()`:

1. **Source-specific preprocessing**: NUFORC strips its `NUFORC UFO Sighting NNNNN` prefix; MUFON strips razor boilerplate.
2. **"Starts with" shortcut**: If both descriptions share their first ≥20 chars, score = 0.95. Catches UFOCAT records that copy NUFORC text.
3. **Token Jaccard pre-filter**: Lowercase tokens, intersection / union. If < 0.03, skip alignment, return that score directly.
4. **Full alignment**: `difflib.SequenceMatcher` ratio on first 1,000 chars of each description.

### Tier 3 cost control

Tier 3 is the dangerous one — pairwise comparison within a single date can blow up. The constraint:

```
date has records from 2+ sources AND date has ≤20 total records
```

A date with 100 records from 3 sources would generate `100*100/2 = 5000` pairs; Tier 3 caps at 20 to keep the comparison space manageable. In practice this filter excludes <0.1% of dates while making Tier 3 finish in ~20 seconds against 17,268 qualifying dates.

### What dedup does NOT do

- **No deletion.** All 618,316 sightings remain in the database. `duplicate_candidate` is purely advisory.
- **No within-source dedup.** Two NUFORC records for the same event are not flagged.
- **No transitive closure.** `A↔B` and `B↔C` does not infer `A↔C`.
- **No automatic merging.** Downstream tooling decides what to do with high-confidence pairs.

### Result

**126,730 candidate pairs across 127,440 unique sightings (20.7% of all records).**

| Confidence | Score | Pairs | Interpretation |
|---|---|---:|---|
| Certain | 0.9–1.0 | 14,260 | Near-identical descriptions; safe to auto-merge |
| Likely | 0.7–0.9 | 9,567 | Strong match; minor wording differences |
| Possible | 0.5–0.7 | 13,303 | Same event reported differently across sources |
| Weak | 0.3–0.5 | 11,144 | Same date+location, descriptions partially overlap |
| Unlikely | 0.0–0.3 | 78,456 | Same date+location but likely different events |

The Unlikely bucket is large because Tier 2c (UPDB↔others, city-only matching) generates many pairs that share a city on a date but turn out to be unrelated. They're flagged for completeness; the score is the safety filter.

---

## 2. Quality score (v0.8.3b formula)

The quality score is a weighted heuristic mapping each sighting to [0, 100]. The current formula was tuned through three iterations to land 18-20% of rows in the high-quality bucket (`>=60`) — high enough to be useful as a default filter, low enough to be selective.

### Formula

```
score = 0

# Description length
if desc_len >= 200:    score += 25
elif desc_len >= 50:   score += 15
elif desc_len > 0:     score += 5

# Has photo/video reference (boolean)
if has_media:          score += 15

# Number of witnesses (tiered)
if num_witnesses >= 3: score += 15
elif num_witnesses == 2: score += 10
elif num_witnesses == 1: score += 5

# Movement signal in narrative
if has_movement_mentioned:
    score += 10
    if len(movement_categories) >= 2:
        score += 5

# Structured fields (3 pts each, max 27)
for f in [time_raw, shape, color, duration, sound, direction,
          elevation_angle, hynek, vallee]:
    if f is not None: score += 3

# Coordinates available
if lat and lng:        score += 5

# Specificity bonus (regex hit on time-of-day / direction / altitude)
if specificity_match:  score += 5

# Unknown-date penalty
if date_event is None:
    score = min(score, 15)              # default cap
    if features >= 8 and has_description:
        score = min(score, 35)          # relaxed cap for rich content

# Final clamp
score = min(100, score)
```

### Why these weights

Five iterations of work went into picking these:

1. **v0.8.2 (initial)**: structured fields × 5 pts, 11 fields = up to 55 pts. Result: 22.6% landed at >=60. Good number, but the weighting was wrong — UFOCAT records with lots of structured fields but no narrative scored as high as NUFORC reports with rich text. Felt unprincipled.

2. **v0.8.3a**: rebalance toward narrative content. Structured fields × 2 pts; introduced media (+15), movement (+10), tiered witnesses. Result: 14.3% at >=60. **Too aggressive** — the structured-only sources got demoted too hard.

3. **v0.8.3b (current)**: structured fields restored to 3 pts (compromise between 2 and 5), plus the unknown-date relaxation to preserve text-rich NICAP/historical reports that just had bad date parsing. Result: **19.3% at >=60** — exactly in the target window.

The rebalance was monotonically positive vs v0.8.3a — every row either moved up or stayed put, none moved down. See [CHANGELOG.md](../CHANGELOG.md) for the bucket-transition matrix.

### Why the unknown-date cap

About 10,000 sightings have `date_event = NULL` because the source's date string couldn't be parsed (NICAP records like "Summer 1947" or year-only Geldreich entries that fail strict parsing). Without a cap, ~7,400 of those would score >=60 on the strength of their other fields. But "no date at all" makes a sighting unverifiable for time-series analysis, so they're capped at 15 — well below the typical filter threshold.

The relaxed cap (35 instead of 15) for rows with `richness_score >= 8 AND has_description = 1` exists because some NICAP/historical reports are genuinely high-signal — they have rich narratives, multiple witnesses, photos, structured metadata — but the source happened to give a date string the parser couldn't handle. 1,282 rows hit the relaxed cap in the v0.11 build. Without this carve-out, those well-documented historical cases would be unjustly demoted to "junk".

### Audit recipe

To see the score distribution on your build:

```sql
SELECT
    CASE WHEN quality_score < 20 THEN '00-19'
         WHEN quality_score < 40 THEN '20-39'
         WHEN quality_score < 60 THEN '40-59'
         WHEN quality_score < 80 THEN '60-79'
         ELSE '80-100' END AS bucket,
    COUNT(*) AS n
FROM sighting GROUP BY 1 ORDER BY 1;
```

Expected (v0.11 build): `00-19=142,072 / 20-39=151,375 / 40-59=202,738 / 60-79=108,117 / 80-100=10,203`.

---

## 3. Hoax detection

Five rule-based flags. Each flag adds weight to `hoax_likelihood` (capped at 1.0):

| Flag | Trigger | Weight |
|---|---|---:|
| `very_short_text` | `len(text) < 20` | 0.2 |
| `generic_phrasing` | Matches one of 5 canned-phrase regexes (`"saw a ufo"`, `"strange lights"`, `"i saw something"`, etc.) | 0.3 |
| `duplicate_phrasing` | First 120 chars of description shared with ≥10 other rows (pre-computed) | 0.4 |
| `dramatic_no_specifics` | Text matches `\b(alien|abducted|probed|reptilian|grey|illuminati)\b` AND `richness_score < 3` | 0.3 |
| `all_caps_text` | >80% of letters in text are uppercase, length >= 20 | 0.15 |

### Real distribution (v0.11)

```
hoax_likelihood   count       what they are
0.0               576,988    clean rows (94%)
0.1                 3,941    only all_caps_text fired
0.2                23,141    only very_short_text
0.3                 2,615    only dramatic_no_specifics
0.4                 7,418    only duplicate_phrasing (the boilerplate batch)
0.5                   326    multi-flag combinations
0.7                    76    rare 3-flag combinations
```

The `duplicate_phrasing` flag catches form-letter spam — pre-computing a SQL `GROUP BY SUBSTR(description, 1, 120) HAVING COUNT(*) >= 10` finds 237 distinct boilerplate prefixes that produce 7,481 flagged rows. Most are legitimate-but-templated reports (NUFORC has some MADAR Node automated reports that share prefixes); a few are actual spam.

`dramatic_no_specifics` is the most useful for filtering: it requires both an emotionally charged keyword AND low feature richness. A genuine abduction report with witnesses, location, duration, etc. won't fire — only the bare `"Aliens abducted me last Tuesday."` style.

### Threshold guidance

For a clean dataset, filter `WHERE hoax_likelihood < 0.3` — keeps 583,544 rows (94.9%) and drops the spammy/templated/all-caps tail.

---

## 4. Movement category extraction

10 movement categories, each backed by a list of regex patterns. Compiled at module load for speed.

```python
MOVEMENT_CATEGORY_PATTERNS = {
    "hovering":     [r"\bhover(?:ed|ing|s)?\b", r"\bstationary\b",
                     r"\bsuspended\b", r"\bmotionless\b"],
    "linear":       [r"\bstraight line\b", r"\bstraight path\b",
                     r"\bin a line\b", r"\bheaded (?:north|south|east|west)\b"],
    "erratic":      [r"\bzig.?zag\b", r"\berratic\b", r"\bdarted\b", r"\bjerky\b"],
    "accelerating": [r"\baccelerat\w*\b", r"\bshot (?:off|up|away|out)\b",
                     r"\bhigh speed\b", r"\bsped (?:off|away)\b", r"\bzipped\b"],
    "rotating":     [r"\brotat\w*\b", r"\bspin(?:ning|ned)?\b",
                     r"\brevolv\w*\b", r"\bwobbl\w*\b"],
    "ascending":    [r"\bascend\w*\b", r"\bclimb(?:ed|ing)?\b",
                     r"\bshot up\b", r"\bstraight up\b", r"\bupward\b"],
    "descending":   [r"\bdescend\w*\b", r"\bdropp\w*\b", r"\bfell\b",
                     r"\bdownward\b", r"\bplummet\w*\b"],
    "vanished":     [r"\bvanish\w*\b", r"\bdisappear\w*\b",
                     r"\bgone in\b", r"\bfaded\b"],
    "followed":     [r"\bfollow(?:ed|ing)?\b", r"\btrail(?:ed|ing)?\b",
                     r"\bchased\b", r"\bpursued\b"],
    "landed":       [r"\bland(?:ed|ing)?\b", r"\btouched down\b",
                     r"\bon the ground\b"],
}
```

A row's `movement_categories` is the JSON array of every category that fired; `has_movement_mentioned = 1` if any did. 249,217 rows (40.6%) have at least one category.

### Why regex, not ML

Each category corresponds to a small, well-defined English vocabulary. Regex gives:
- Deterministic results — same input always produces same output, no model drift
- Inspectable rules — anyone can read the patterns and predict matches
- Trivial to extend — append to the dict, no retraining
- Zero compute cost vs. an LLM classifier (which would dwarf the rest of the pipeline)

The trade-off: missed patterns. "It crept across the sky" doesn't match anything. We accept this — false negatives are tolerable for an aggregate analysis pipeline; false positives would be worse.

### Distribution

```
vanished       102,178    accelerating    22,627
hovering        89,964    linear          21,106
followed        51,499    rotating        19,641
descending      27,148    erratic          8,877
ascending       27,067    landed          26,592
```

`vanished` is the most common — "it disappeared" / "it faded out" / "it was gone in a flash" are extremely frequent narrative endings. `hovering` / `followed` / `landed` cluster in the structured close-encounter accounts. `erratic` is rare in observational text — when present, it's often a strong signal.

---

## 5. Sentiment + emotion classification

Two parallel pipelines:

### Legacy: VADER + NRC (sentiment.py)

`sentiment.py` runs VADER (rule-based, lexicon-driven) for compound polarity and NRCLex (word-list-driven) for 8-emotion counts (joy, fear, anger, sadness, surprise, disgust, trust, anticipation). Results land in the `sentiment_analysis` table.

`analyze.py:derive_sentiment_summary` then reads the table and writes:
- `sentiment_score` (= VADER compound)
- `dominant_emotion` (= argmax of NRC emotion counts, NULL on ties)
- `emotion_scores` (= JSON normalized NRC vector, on `sighting_analysis`)

NRC was the original primary classifier in v0.8.x. It has two weaknesses: (1) it's a word-list lookup, so it doesn't understand context — "no fear" still scores fear; (2) UFO sighting text is heavy on observational verbs ("approach", "appear", "witness") that NRC tags strongly as anticipation/trust. The dominant emotion distribution skews heavily toward anticipation and trust as a result, which is a model artifact, not signal about the reports.

### Current: transformer models (emotions.py)

v0.11 added three HuggingFace transformer models running on GPU:

| Model | Why this one |
|---|---|
| **GoEmotions 28-class** (`SamLowe/roberta-base-go_emotions`) | The widely-used reference 28-class emotion model. Trained on 58K Reddit comments. Granular labels useful for filtering ("show me sightings classified as confusion"). |
| **7-class RoBERTa emotion** (`j-hartmann/emotion-english-distilroberta-base`) | Smaller label space (7 classes) gives a more balanced distribution on observational text. Better for grouped charts. |
| **RoBERTa sentiment** (`cardiffnlp/twitter-roberta-base-sentiment-latest`) | Transformer-quality sentiment alongside VADER's lexicon-based score. Compound = `positive_prob - negative_prob` in [-1, +1]. |

Why three models instead of one: each gives a different lens. The 28-class is for granular filtering, the 7-class is for distribution charts, the sentiment scores are for cross-validation against VADER. Researchers can pick whichever fits their analysis.

### Coverage on UFO text

GoEmotions classifies **86.7% of sightings as `neutral`** because it was trained on emotionally expressive Reddit comments. Observational sighting text ("I saw a bright light moving across the sky at 9pm...") doesn't register as emotional in that model's calibration. The 13.3% non-neutral tail is the interesting signal: confusion (10.7K), surprise (8.1K), fear (5.9K), admiration (6.4K), joy (3.0K).

The 7-class model gives a more differentiated distribution because fewer classes force more probability mass into each:

```
neutral    158,470  (31.5%)    sadness     10,301  (2.0%)
surprise   157,644  (31.3%)    anger        8,089  (1.6%)
fear       134,322  (26.7%)    joy          4,204  (0.8%)
disgust     29,955  (6.0%)
```

For visualization, prefer the 7-class. For specific-emotion filtering, the 28-class gives more precise queries (e.g. "show me reports labeled `confusion`").

### VADER vs RoBERTa agreement

By 7-class emotion bucket (mean compound scores):

| 7-class label | VADER mean | RoBERTa mean | Agreement |
|---|---:|---:|---|
| joy | +0.411 | +0.320 | strong + |
| surprise | +0.239 | +0.057 | weak + |
| neutral | +0.110 | +0.022 | mild + |
| fear | +0.097 | -0.007 | disagree (VADER over-positive) |
| disgust | -0.015 | -0.110 | mild - |
| anger | -0.090 | -0.212 | strong - |
| sadness | -0.152 | -0.320 | strong - |

Sign agrees on the clearly-emotional categories (sadness, anger, joy, disgust). VADER systematically over-scores positive sentiment on observational text — its lexicon flags words like "saw", "bright", "amazing", "fantastic" that don't carry emotional weight in this domain. **Use `roberta_sentiment` when you want a more accurate signed sentiment**; use `vader_compound` for backward compatibility with prior research.

---

## 6. Geocoding

Offline geocoding via the GeoNames `cities15000` gazetteer (~10 MB, 191K cities ≥15K population). Three matching strategies in priority order:

| Method | Match key | Hit count |
|---|---|---:|
| `geonames_exact` | City + admin1 (state) + country code | 13,777 |
| `geonames_city_country` | City + country code (any state) | 19,617 |
| `geonames_city_only` | City (any country) — first hit | 10,600 |

Total geocoded **locations**: 43,994 / 152,785 (28.8%). But because many sightings share the same location row, this propagates to **396,240 sightings (64.5%)** with coords.

Why offline GeoNames instead of live Nominatim/Google: deterministic, free, fast (in-memory dict), no rate limits, no privacy implications. The gazetteer is downloaded once at build time (`python geocode.py --download`) and committed at the version recorded in `geodata/cities15000.txt` (~30 MB extracted, gitignored).

Per-source coverage:

```
UFOCAT      176,308 / 197,108 (89.4%)    -- structured lat/lng in source
NUFORC       97,761 / 159,320 (61.4%)
MUFON        85,332 / 138,310 (61.7%)
UPDB         23,466 /  65,016 (36.1%)
UFO-search   13,373 /  54,751 (24.4%)    -- historical reports, sparse
```

The drop-off for UPDB and UFO-search is expected — both include older or aggregated records where the source's location data is too vague (country only, region descriptions like "Eastern seaboard") to gazetteer-match.

---

## 7. The ANALYSIS_STEPS plug-in pattern

`analyze.py` defines its 9-step pipeline as a top-level list of (name, function, label) tuples:

```python
ANALYSIS_STEPS = [
    ("shapes",        normalize_and_cluster_shapes, "Normalizing shapes"),
    ("movement",      classify_movement,            "Classifying movement/behavior"),
    ("colors",        extract_colors,               "Extracting colors"),
    ("sentiment",     derive_sentiment_summary,     "Deriving sentiment summary"),
    ("duration",      clean_duration,               "Bucketing durations"),
    ("public_fields", derive_public_fields,         "Deriving public fields"),
    ("quality",       calculate_quality_score,      "Calculating quality score"),
    ("hoax",          flag_potential_hoaxes,        "Flagging potential hoaxes"),
    ("topic",         run_topic_modeling,           "Topic modeling"),
]
```

`run_analysis(db_path, steps=None)` iterates the list. To plug in a new analysis (e.g. an offline LLM enrichment), the contributor:

1. Writes a function that takes a `sqlite3.Connection` and updates the DB
2. Appends `("my_step", my_fn, "My step description")` to `ANALYSIS_STEPS`
3. Adds the columns it writes to `DERIVED_SIGHTING_COLUMNS` (so `--reset` clears them)

No edit to `run_analysis` itself. See [CONTRIBUTING.md](CONTRIBUTING.md) for the full walkthrough.

This pattern was added in v0.11 specifically to make future recursive-AI / LLM enrichment steps a one-line addition, not a refactor.

---

## 8. Public export — what's stripped and why

`export_public.py` produces a clean SQLite suitable for redistribution:

| Action | Why |
|---|---|
| Drop `description`, `summary`, `notes`, `raw_json` from sighting | The actual narrative blobs. These are the source-owned text content with redistribution restrictions. All derived fields are computed before stripping, so analyses survive intact. |
| Drop `sentiment_analysis` table | Raw VADER/NRC scores. Redundant — `sighting.sentiment_score` and `sighting.dominant_emotion` are denormalized copies. |
| Drop `duplicate_candidate` table | Internal dedup metadata. Not useful to consumers and reveals the matching strategy. |
| Drop `reference`, `sighting_reference`, `attachment` tables | Citation text and attachment metadata. Empty in current build, defensively excluded. |
| Allowlist via `PUBLIC_TABLES` | Anything not in the allowlist is dropped on every export. New tables default to private unless explicitly added. |
| `VACUUM` after drops | Reclaims the disk space (PG and SQLite both leave dead tuples in heap pages until vacuumed). |

Result: 1.8 GB private DB → 553 MB public DB (70% reclaimed). 618,316 sightings × 94 columns, all derived fields intact, zero raw narrative text (except LLM-generated summaries for Reddit sightings).

See [PITFALLS.md](PITFALLS.md) for the `witness_names` privacy note — short structured-adjacent text that's intentionally kept but worth a second look if you redistribute the export.

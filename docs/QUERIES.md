# `ufo_public.db` — Query Recipes

A practical, copy-pasteable cookbook of SQL queries against the public SQLite. Every query in this file has been run against the live v0.11 build (614,505 sightings) and the actual result counts are inline below the query.

> **Companion**: see [SCHEMA.md](SCHEMA.md) for a column-by-column reference of every field referenced here.

## Setup

```bash
# Open with the sqlite3 CLI
sqlite3 data/output/ufo_public.db
.headers on
.mode column

# Or in Python
python -c "
import sqlite3
conn = sqlite3.connect('data/output/ufo_public.db')
cur = conn.cursor()
cur.execute('SELECT COUNT(*) FROM sighting')
print(cur.fetchone()[0])  # 614505
"
```

`source_db_id` lookup (worth memorizing):

```
1 = MUFON     2 = NUFORC     3 = UFOCAT     4 = UPDB     5 = UFO-search
```

---

## Quality filtering

### Find high-quality multi-witness sightings

```sql
SELECT id, sighting_datetime, lat, lng, standardized_shape, quality_score
FROM sighting
WHERE quality_score >= 60
  AND num_witnesses >= 2
  AND has_movement_mentioned = 1
ORDER BY quality_score DESC, num_witnesses DESC
LIMIT 25;
```

→ **58,120 rows** match the WHERE clause. Map-ready high-quality (with coords) is **88,108 rows**.

### Quality score distribution

```sql
SELECT
    CASE WHEN quality_score < 20 THEN '00-19'
         WHEN quality_score < 40 THEN '20-39'
         WHEN quality_score < 60 THEN '40-59'
         WHEN quality_score < 80 THEN '60-79'
         ELSE '80-100' END AS bucket,
    COUNT(*) AS n
FROM sighting
GROUP BY 1 ORDER BY 1;
```

→
```
00-19   142,072
20-39   151,375
40-59   202,738
60-79   108,117
80-100   10,203
```

### Filter out probable hoaxes

```sql
SELECT COUNT(*) FROM sighting WHERE hoax_likelihood < 0.3;
-- 583,544 rows (94.9%) — most data is clean
```

### High-quality + clean for analysis

```sql
SELECT *
FROM sighting
WHERE quality_score >= 60
  AND hoax_likelihood < 0.2
  AND has_description = 1;
-- the 'serious research' subset
```

---

## Source comparison

### Per-source row counts

```sql
SELECT sd.name, COUNT(*) AS n
FROM sighting s JOIN source_database sd ON s.source_db_id = sd.id
GROUP BY sd.name ORDER BY n DESC;
```

→
```
UFOCAT       197,108
NUFORC       159,320
MUFON        138,310
UPDB          65,016
UFO-search    54,751
```

### Emotion classification coverage by source

```sql
SELECT sd.name,
       COUNT(*) AS total,
       COUNT(s.emotion_7_dominant) AS classified,
       ROUND(100.0 * COUNT(s.emotion_7_dominant) / COUNT(*), 1) AS pct
FROM sighting s JOIN source_database sd ON s.source_db_id = sd.id
GROUP BY sd.name ORDER BY total DESC;
```

→
```
UFOCAT       197,108    93,302    47.3%
NUFORC       159,320   159,319   100.0%
MUFON        138,310   137,385    99.3%
UPDB          65,016    58,228    89.6%
UFO-search    54,751    54,751   100.0%
```

UFOCAT's lower coverage is real — many UFOCAT records have minimal narrative text, so the emotion classifier finds nothing to score.

### Emotion x source cross-tab

```sql
SELECT sd.name AS source, s.emotion_7_dominant AS emotion, COUNT(*) AS n
FROM sighting s JOIN source_database sd ON s.source_db_id = sd.id
WHERE s.emotion_7_dominant IS NOT NULL
GROUP BY 1, 2 ORDER BY 1, 3 DESC;
```

Top cells:
```
NUFORC  surprise   77,632
MUFON   surprise   58,354
NUFORC  fear       54,122
UFOCAT  neutral    43,984
MUFON   fear       36,580
```

---

## Movement & behavior

### Movement category counts (flattened across rows)

The `movement_categories` column is a JSON array — one row can have multiple categories. Use `json_each` to flatten:

```sql
SELECT cat.value AS category, COUNT(*) AS n
FROM sighting s, json_each(s.movement_categories) AS cat
WHERE s.movement_categories IS NOT NULL AND s.movement_categories != '[]'
GROUP BY cat.value
ORDER BY n DESC;
```

→
```
vanished       102,178
hovering        89,964
followed        51,499
descending      27,148
ascending       27,067
landed          26,592
accelerating    22,627
linear          21,106
rotating        19,641
erratic          8,877
```

### Multi-category movement (sightings with 2+ movement signals)

```sql
SELECT id, sighting_datetime, movement_categories
FROM sighting
WHERE movement_categories IS NOT NULL
  AND json_array_length(movement_categories) >= 2
ORDER BY quality_score DESC
LIMIT 20;
```

### Find "hovering then accelerating away" patterns

```sql
SELECT COUNT(*) FROM sighting
WHERE movement_categories LIKE '%hovering%'
  AND movement_categories LIKE '%accelerating%';
```

This is a substring match against the JSON text — fast (no parsing) and works because category names don't appear as substrings of each other.

---

## Sentiment & emotion

### VADER vs RoBERTa: do the two sentiment scores agree?

```sql
SELECT emotion_7_dominant,
       ROUND(AVG(vader_compound), 3)    AS vader_mean,
       ROUND(AVG(roberta_sentiment), 3) AS roberta_mean,
       COUNT(*) AS n
FROM sighting
WHERE emotion_7_dominant IS NOT NULL
GROUP BY 1
ORDER BY 4 DESC;
```

→
```
neutral    +0.110  +0.022   158,470
surprise   +0.239  +0.057   157,644
fear       +0.097  -0.007   134,322
disgust    -0.015  -0.110    29,955
sadness    -0.152  -0.320    10,301
anger      -0.090  -0.212     8,089
joy        +0.411  +0.320     4,204
```

Both models agree on the sign for clearly-emotional categories (sadness, anger, joy, disgust). VADER over-scores positive sentiment on observational text — its lexicon flags words like "saw", "bright", "amazing" that don't carry emotional weight in this domain.

### Rows where VADER and RoBERTa disagree strongly

```sql
SELECT id, vader_compound, roberta_sentiment,
       (vader_compound - roberta_sentiment) AS delta,
       emotion_7_dominant, emotion_28_dominant
FROM sighting
WHERE vader_compound IS NOT NULL
  AND roberta_sentiment IS NOT NULL
  AND ABS(vader_compound - roberta_sentiment) > 0.7
ORDER BY ABS(vader_compound - roberta_sentiment) DESC
LIMIT 50;
```

→ **50,407 rows** disagree by more than 0.7 — about 10% of classified rows. These are usually cases where the lexicon-based VADER picks up positive-leaning words in objectively neutral observational reports.

### High-quality fear-classified sightings with media

```sql
SELECT id, sighting_datetime, lat, lng,
       quality_score, num_witnesses, standardized_shape,
       emotion_7_fear, vader_compound, roberta_sentiment
FROM sighting
WHERE quality_score >= 60
  AND emotion_7_dominant = 'fear'
  AND has_media = 1
ORDER BY emotion_7_fear DESC
LIMIT 25;
```

→ **16,692 rows** match. These are well-documented fear-toned reports with photo or video evidence — useful seed for case studies.

### GoEmotions 28-class distribution

```sql
SELECT emotion_28_dominant, emotion_28_group, COUNT(*) AS n
FROM sighting
WHERE emotion_28_dominant IS NOT NULL
GROUP BY 1, 2
ORDER BY n DESC;
```

The 28-class is heavily biased toward `neutral` (87% of classified) because GoEmotions was trained on Reddit comments — observational sighting text registers as low-emotion. The 13% non-neutral tail (`realization`, `confusion`, `surprise`, `admiration`, `fear`...) is the interesting signal. Use `emotion_7_dominant` for a more balanced view.

---

## Shapes

### Top standardized shapes

```sql
SELECT standardized_shape, COUNT(*) AS n
FROM sighting
WHERE standardized_shape IS NOT NULL
GROUP BY 1 ORDER BY n DESC;
```

→
```
Light       41,715      Cigar       8,508
Disc        36,920      Formation   5,135
Other       27,264      Changing    4,556
Sphere      23,492      Cylinder    4,226
Circle      18,892      Rectangle   2,890
Triangle    17,529      Flash       2,775
Fireball    11,948      ...
Unknown     10,708
Oval         9,606
```

### Shape × Hynek classification

```sql
SELECT standardized_shape, hynek, COUNT(*) AS n
FROM sighting
WHERE standardized_shape IS NOT NULL AND hynek IS NOT NULL
GROUP BY 1, 2
ORDER BY 1, 3 DESC;
```

Triangle sightings:
```
Triangle  NO    7,129    -- "Nighttime Object"
Triangle  NL    3,156    -- "Nocturnal Light"
Triangle  DO    1,405    -- "Daytime Object"
Triangle  CE1     661
Triangle  CE2     202
```

### Top emotion per shape (window function)

```sql
WITH ranked AS (
    SELECT standardized_shape, emotion_7_dominant, COUNT(*) AS n,
           ROW_NUMBER() OVER (PARTITION BY standardized_shape ORDER BY COUNT(*) DESC) AS rk
    FROM sighting
    WHERE standardized_shape IS NOT NULL AND emotion_7_dominant IS NOT NULL
    GROUP BY 1, 2
)
SELECT standardized_shape, emotion_7_dominant, n
FROM ranked
WHERE rk = 1
ORDER BY n DESC;
```

→
```
Light       surprise   18,684     Triangle    surprise   6,851
Disc        neutral    11,596     Fireball    surprise   4,798
Sphere      surprise    8,938     Oval        surprise   3,701
Circle      surprise    8,695     Cigar       neutral    2,867
Other       neutral     8,625     ...
```

Disc and Other lean neutral (more "saw a flat object"-style descriptions); everything else leans surprise.

---

## Time series

### Yearly counts

```sql
SELECT SUBSTR(date_event, 1, 4) AS year, COUNT(*) AS n
FROM sighting
WHERE date_event IS NOT NULL
GROUP BY year
ORDER BY year;
```

Top years:
```
1967    19,212    -- Hatch catalog dump
2014    17,040
2012    16,962
2013    15,826
2015    15,805
```

The 1967 spike is a Hatch catalog historical aggregation, not a real wave. Modern years (2010-2020) are dominated by NUFORC and MUFON online reporting.

### Monthly counts for a specific year

```sql
SELECT SUBSTR(date_event, 1, 7) AS yyyy_mm, COUNT(*) AS n
FROM sighting
WHERE date_event LIKE '2014-%'
GROUP BY yyyy_mm
ORDER BY yyyy_mm;
```

### Decade-by-decade growth

```sql
SELECT (CAST(SUBSTR(date_event, 1, 4) AS INTEGER) / 10) * 10 AS decade,
       COUNT(*) AS n
FROM sighting
WHERE date_event IS NOT NULL
  AND CAST(SUBSTR(date_event, 1, 4) AS INTEGER) >= 1900
GROUP BY decade
ORDER BY decade;
```

---

## Geographic queries

### Sightings within a bounding box (great for map tiles)

```sql
SELECT id, lat, lng, standardized_shape, quality_score, sighting_datetime
FROM sighting
WHERE lat BETWEEN 32.5 AND 42.0       -- California latitude range
  AND lng BETWEEN -124.0 AND -114.0
  AND quality_score >= 40
ORDER BY quality_score DESC
LIMIT 1000;
```

The `idx_sighting_latlng` index makes this O(log n).

### Per-country counts (with geocoded coverage)

```sql
SELECT l.country, COUNT(*) AS sightings,
       COUNT(s.lat) AS with_coords
FROM sighting s
JOIN location l ON s.location_id = l.id
WHERE l.country IS NOT NULL
GROUP BY l.country
ORDER BY sightings DESC
LIMIT 15;
```

### Spatial clustering: city counts

```sql
SELECT l.city, l.state, COUNT(*) AS n
FROM sighting s
JOIN location l ON s.location_id = l.id
WHERE l.city IS NOT NULL AND l.country = 'US'
GROUP BY l.city, l.state
ORDER BY n DESC
LIMIT 25;
```

---

## Hoax / data quality

### Hoax flag combinations

```sql
SELECT a.hoax_flags, COUNT(*) AS n
FROM sighting_analysis a
WHERE a.hoax_flags != '[]'
GROUP BY 1
ORDER BY n DESC
LIMIT 10;
```

→
```
["very_short_text"]                              23,141
["duplicate_phrasing"]                            7,405
["all_caps_text"]                                 3,941
["dramatic_no_specifics"]                         2,615
["very_short_text", "dramatic_no_specifics"]        320
```

### Find duplicate-phrasing rows (suspected boilerplate / spam reports)

```sql
SELECT s.id, s.sighting_datetime, s.standardized_shape, s.quality_score, a.hoax_flags
FROM sighting s
JOIN sighting_analysis a ON s.id = a.sighting_id
WHERE a.hoax_flags LIKE '%duplicate_phrasing%'
ORDER BY s.id
LIMIT 30;
```

### Filter to "trustworthy" subset

```sql
-- High quality, no hoax flags, has description, has coords
SELECT *
FROM sighting s
JOIN sighting_analysis a ON s.id = a.sighting_id
WHERE s.quality_score >= 60
  AND a.hoax_flags = '[]'
  AND s.has_description = 1
  AND s.lat IS NOT NULL;
```

---

## Cross-tabs for visualization

### Movement type × emotion

```sql
SELECT movement_type, emotion_7_dominant, COUNT(*) AS n
FROM sighting
WHERE movement_type IS NOT NULL
  AND emotion_7_dominant IS NOT NULL
GROUP BY 1, 2
ORDER BY 1, 3 DESC;
```

Useful for a heatmap: "hovering" sightings tend to be `surprise`, "fast" sightings tend to be `fear`, etc.

### Sentiment group composition by source

```sql
SELECT sd.name AS source,
       SUM(CASE WHEN s.emotion_28_group = 'positive'  THEN 1 ELSE 0 END) AS pos,
       SUM(CASE WHEN s.emotion_28_group = 'negative'  THEN 1 ELSE 0 END) AS neg,
       SUM(CASE WHEN s.emotion_28_group = 'ambiguous' THEN 1 ELSE 0 END) AS amb,
       SUM(CASE WHEN s.emotion_28_group = 'neutral'   THEN 1 ELSE 0 END) AS neu
FROM sighting s JOIN source_database sd ON s.source_db_id = sd.id
WHERE s.emotion_28_group IS NOT NULL
GROUP BY sd.name
ORDER BY sd.name;
```

### Quality × emotion

```sql
SELECT
    CASE WHEN quality_score >= 60 THEN 'high'
         WHEN quality_score >= 40 THEN 'mid'
         ELSE 'low' END AS quality_tier,
    emotion_7_dominant,
    COUNT(*) AS n
FROM sighting
WHERE emotion_7_dominant IS NOT NULL
GROUP BY 1, 2
ORDER BY 1, 3 DESC;
```

Lets you see whether high-quality reports skew toward different emotional content than low-quality ones (they do — high-quality lean more `fear` and `surprise`, low-quality lean more `neutral`).

---

## Joining everything

The "give me everything" query for a research export. Returns one row per sighting with the most useful fields denormalized:

```sql
SELECT
    s.id,
    s.sighting_datetime,
    s.lat, s.lng,
    sd.name                  AS source,
    s.source_record_id,
    l.city, l.state, l.country,
    s.standardized_shape,
    s.primary_color,
    s.movement_type,
    s.movement_categories,
    s.has_movement_mentioned,
    s.has_media,
    s.has_description,
    s.num_witnesses,
    s.hynek, s.vallee,
    s.quality_score,
    s.richness_score,
    s.hoax_likelihood,
    s.emotion_28_dominant,
    s.emotion_28_group,
    s.emotion_7_dominant,
    s.vader_compound,
    s.roberta_sentiment
FROM sighting s
JOIN source_database sd ON s.source_db_id = sd.id
LEFT JOIN location l ON s.location_id = l.id
WHERE s.quality_score >= 40   -- adjust threshold as needed
ORDER BY s.id;
```

---

## Performance notes

- **Indexed columns are fast to filter on**: `date_event`, `source_db_id`, `shape`, `standardized_shape`, `hynek`, `vallee`, `lat`/`lng` (composite), `quality_score`, `hoax_likelihood`, `has_description`, `has_media`, `has_movement_mentioned`, `emotion_28_dominant`, `emotion_28_group`, `emotion_7_dominant`, `dominant_emotion`, `location.city`, `location.country`. Filters on these are O(log n).

- **Free-text scans are O(n)**: filtering on `characteristics`, `explanation`, `terrain`, `weather`, `witness_names`, `source_ref`, `page_volume`, `movement_categories` (the JSON content) requires a full table scan. Add an indexed-column filter first to narrow the rows the scan has to traverse.

- **JSON columns**: `movement_categories`, `behavior_tags`, `color_list`, `emotion_scores`, `hoax_flags` are TEXT columns containing JSON. Use `json_each(col)` for iteration, `json_array_length(col)` for size, or `LIKE '%substring%'` for fast presence checks (works because category/tag names are distinctive enough not to substring-collide).

- **614k rows fits comfortably in memory.** A single full-table scan is ~1 second on a modern SSD. Don't be precious about ad-hoc queries — just run them.

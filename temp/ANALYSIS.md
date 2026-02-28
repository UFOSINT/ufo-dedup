# Historic Pre-1901 Date Analysis

**Dataset:** `temp/historic_pre1901.db` (8,046 records extracted from `ufo_unified.db`)

## Category Breakdown

| Category | Source | Count | Year Range | Description |
|----------|--------|------:|------------|-------------|
| `ufocat_ancient` | UFOCAT | 4,436 | 1001-1900 | 4-digit raw years, legitimately pre-1901 |
| `ufocat_century_only` | UFOCAT | 692 | 19 (=19xx) | 2-digit raw year `19//` = "sometime in the 1900s, year unknown" |
| `ufocat_3digit_review` | UFOCAT | 88 | 34-999 | 3-digit raw years, mostly legit ancient, a few suspicious |
| `other_source_review` | UFO-search | 1,984 | 61-1900 | Geldreich timeline, mostly legitimate historic records |
| `other_source_review` | UPDB | 780 | 100-1900 | Mix of legit ancient + ~20 records with mangled modern years |
| `other_source_review` | MUFON | 40 | 1890-1900 | All look legitimate (1890s sightings) |
| `other_source_review` | NUFORC | 26 | 205-1899 | Mix of legit historic + ~2 data entry errors |

## Problem Categories

### 1. UFOCAT `19//` Century-Only (692 records)
- **Raw date:** `19//` = YEAR=19, MO=empty, DAY=empty
- **Parsed as:** `0019-01-01` (zero-padded 2-digit year)
- **Actual meaning:** "Sometime in the 1900s" -- year is unknown, only century known
- **Evidence:** Descriptions include "recalled abduction from orphanage as little girl", "Motion pictures", modern US cities (Sacramento, Miami, Chicago, Houston)
- **Fix options:**
  - A) Set date_event to NULL (unknown year = no usable date)
  - B) Set date_event to `19xx` marker (preserves century info but non-standard)
  - C) Leave as-is with a flag/note column

### 2. UFOCAT 3-Digit Year Ambiguity (88 records)
- Most are legitimately ancient (34 AD Jerusalem, 70 AD, 497 AD, 776 AD Sigiburg, etc.)
- **2 confirmed modern mislabels:**
  - `195//` "H-BOMB TEST" -- clearly 1950s, not 195 AD
  - `188//` with states CN, NZL, FRA, TUR and no descriptions -- could be 1880s
- **Fix:** Manual classification in `date_analysis.corrected_year` + `notes`

### 3. UPDB Mangled Modern Years (20 records)
- Raw dates already broken in upstream UPDB data (`0196-01-01 00:00:00` in raw_json)
- Pattern: looks like 3-4 digit truncation of modern years
  - `0100` -> probably 2001 or 1001? (Topanga Canyon = modern)
  - `0191` -> 1991 (Boxford, "bright red object...upside down saucer")
  - `0196` -> 1962 (Columbus, description says "June 22-23, 1962")
  - `0200` -> 2000 (Ellsworth AFB radar, Auburn, etc.)
  - `0300` -> 2003? (Yakima Indian Reservation)
  - `0400`-`0900` -> 2004-2009? or other mangling
- **Fix:** Manual correction where description provides the real year; NULL the rest

### 4. NUFORC Data Entry Errors (2-3 records)
- `0205-01-05` -> description says "Dad seen outside window" = modern, likely 2005
- `1071-06-16` -> "my friend spotted object in sky" = modern, likely 2007 or 1971
- `1721-02-01` -> "straight line of lights in sky" = modern, likely 2021
- Most other NUFORC pre-1901 are legitimate historic sighting reports
- **Fix:** Manual correction from description context

### 5. Legitimately Ancient Records (~7,200 records)
- UFOCAT 4-digit years: 4,436 records (1001-1900) -- correct
- UFOCAT 3-digit years: ~86 of 88 are correct ancient dates
- UFO-search: 1,984 records -- Geldreich Majestic Timeline, appear legitimate
- UPDB: ~760 of 780 are legitimate (1000-1900)
- MUFON: 40 records (1890s) -- legitimate
- NUFORC: ~23 of 26 -- legitimate historic reports

## Recommended Action Plan

1. **UFOCAT `19//` (692 records):** Set `date_event = NULL` since year is genuinely unknown
2. **UFOCAT 3-digit review (88 records):** Mark corrected_year in temp DB, apply fixes
3. **UPDB mangled years (20 records):** Manually assign corrected_year from descriptions
4. **NUFORC errors (~3 records):** Manually assign corrected_year from descriptions
5. **Everything else:** Leave as-is (legitimately ancient)

## Database Schema

```sql
-- date_analysis table has classification columns ready for annotation:
--   category       TEXT  -- auto-classified category
--   corrected_year INT   -- manual override (NULL = no correction needed)
--   notes          TEXT  -- reviewer notes
```

Use `UPDATE date_analysis SET corrected_year=X, notes='reason' WHERE sighting_id=Y`
to annotate records, then generate SQL fixes for the main database.

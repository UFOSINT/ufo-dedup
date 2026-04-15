# Pitfalls — Things That Will Bite You

A running list of gotchas, silent failures, and surprising-but-intentional behaviors. Read this if your reproduction produces unexpected numbers, your queries return nothing, or you're trying to extend the pipeline.

---

## 1. NRC silently produces all-zero emotion data without NLTK corpora

**Symptom**: `sentiment.py` runs, prints a healthy "Records analyzed: 502,985", but every row in `sentiment_analysis` has `emo_joy=0, emo_fear=0, ...` for all 8 NRC emotions. `sighting.dominant_emotion` ends up 0% populated after `analyze.derive_sentiment_summary()` runs.

**Cause**: `NRCLex` requires NLTK corpora (`punkt_tab`, `wordnet`, `averaged_perceptron_tagger_eng`, etc.) that aren't installed by `pip install NRCLex`. Without them, every NRC call raises `MissingCorpusError` — which `sentiment.py` catches with a bare `except Exception: emo = {}` and proceeds.

**Fix**: download the corpora before running sentiment:

```bash
python -m textblob.download_corpora
```

Verify with:

```bash
python -c "
from nrclex import NRCLex
n = NRCLex('I was terrified by the bright craft, filled with fear and panic.')
print(n.raw_emotion_scores)
"
# Expected: {'positive': 1, 'anger': 1, 'fear': 2, 'negative': 2}
# If you get {}, the corpora aren't installed.
```

**Why it stayed broken so long**: the `try/except` swallowed the error silently, and the rest of the pipeline appeared to work — VADER scores were correct, the analyzer ran end-to-end, the row count was right. Only when querying `dominant_emotion` does the corruption show up. Caught during the v0.8.2 deploy; documented here so it can't happen invisibly again.

---

## 2. Sighting IDs are NOT stable across rebuilds

**Symptom**: You build the database twice with the same source data, expecting the same IDs. Some rows get the same ID, others don't. Joins across rebuilds produce nonsense.

**Cause**: The importers use SQLite `AUTOINCREMENT` and insert rows in source-load order (UFOCAT → NUFORC → MUFON → UPDB → UFO-search). Any change in import order, any row skipped at a different point, any data fix that changes row count between sources will shift the IDs of everything downstream.

**Real impact**: The `date_correction` table on the production Postgres has 714 rows referencing sighting IDs. After the v0.8.2 reload, **711 of those references became orphaned** — they point at sighting rows that no longer exist (their semantic content moved to a different ID). The dev team's app keeps the FK constraint marked `NOT VALID` to avoid validation failures.

**Mitigation**: use `(source_db_id, source_record_id)` as your stable cross-rebuild identifier. Both columns are 100% populated and source-derived, so they survive renumbering.

```sql
-- DON'T
SELECT * FROM sighting WHERE id = 12345;

-- DO
SELECT * FROM sighting WHERE source_db_id = 2 AND source_record_id = 'NUFORC-12345';
```

---

## 3. GoEmotions classifies 87% of sightings as "neutral"

**Symptom**: Your `emotion_28_dominant` distribution is overwhelmingly `neutral` and you wonder if the model is broken.

**Cause**: It's not broken — GoEmotions was trained on 58K Reddit comments where emotional expression is the norm. Observational sighting reports ("I saw a bright light moving across the sky at 9pm at approximately 500 feet altitude...") don't register as emotionally expressive in that model's calibration. The 13% non-neutral tail (confusion, surprise, fear, admiration, joy) is the real signal.

**Workaround**: use the 7-class model for distribution charts:

```sql
SELECT emotion_7_dominant, COUNT(*) FROM sighting
WHERE emotion_7_dominant IS NOT NULL GROUP BY 1 ORDER BY 2 DESC;
```

The 7-class model has fewer labels and thus distributes probability more aggressively. Distribution: neutral 31.5%, surprise 31.3%, fear 26.7%, disgust 6%, sadness 2%, anger 1.6%, joy 0.8%.

For specific-emotion filtering ("show me sightings classified as confusion"), GoEmotions 28-class is still the right tool — its label space is more granular.

---

## 4. VADER systematically over-scores positive sentiment on UFO text

**Symptom**: `AVG(vader_compound)` is +0.13 (slightly positive). RoBERTa average is +0.009 (near-neutral). They disagree.

**Cause**: VADER is lexicon-based. Its word list flags "amazing", "bright", "fantastic", "saw", "incredible" as positive — words that appear constantly in observational sighting text without carrying emotional weight in this domain.

**Mitigation**: prefer `roberta_sentiment` for any analysis where signed sentiment matters. VADER stays in the schema for backward compatibility with prior research and as a baseline comparison.

50,407 rows (10% of classified) have `ABS(vader_compound - roberta_sentiment) > 0.7` — substantial disagreement. Most are observational reports VADER scores positive that RoBERTa correctly scores neutral.

---

## 5. `duration_seconds` is 0% populated

**Symptom**: You query `WHERE duration_seconds < 60` and get zero rows.

**Cause**: The importers parse the free-text `duration` column ("5 minutes", "several hours", "a moment") but never convert these to integer seconds. The column is reserved for a future regex parser that hasn't been written.

**Workaround**: filter on the free-text `duration` column with `LIKE`:

```sql
SELECT * FROM sighting WHERE duration LIKE '%second%';
```

`duration_bucket` is similarly empty — it depends on `duration_seconds`. Both tracked for v0.9.

---

## 6. `topic_id` is 0% populated

**Symptom**: `WHERE topic_id IS NOT NULL` returns nothing.

**Cause**: The column is reserved for v0.9 BERTopic-style topic modeling. The current `analyze.run_topic_modeling()` is an intentional stub that prints `[deferred to v0.9]` and returns.

**Workaround**: none — wait for v0.9 or implement your own topic modeling using the `ANALYSIS_STEPS` plug-in pattern (see [CONTRIBUTING.md](CONTRIBUTING.md)).

---

## 7. The geocoder needs internet for the first download

**Symptom**: `python rebuild_db.py` fails at step 8 with `ERROR: Gazetteer not found at .../geodata/cities15000.txt`.

**Cause**: GeoNames `cities15000.zip` is downloaded on demand by `geocode.py --download` and not committed to the repo. The rebuild assumes you've run the download once.

**Fix**:

```bash
python geocode.py --download
```

Downloads ~10 MB from `download.geonames.org`, extracts to `geodata/cities15000.txt` (~30 MB). After that, all rebuilds work offline.

If `geocode.py --download` fails with a connection error, GeoNames is occasionally rate-limited or briefly down — wait a few minutes and retry.

---

## 8. The "MISMATCH" warning at the end of migrate_sqlite_to_pg.py is a false positive

**Symptom**: The PG migration script reports:

```
date_correction:    sqlite=0 / postgres=714 / MISMATCH
```

and exits with code 1.

**Cause**: `date_correction` is a Postgres-only overlay table that doesn't exist in the SQLite source. The verifier compares 0 (SQLite has no such table) against 714 (PG has the preserved overlay) and yells. The migration itself succeeded — only the verification report misinterprets the situation.

**Mitigation**: as long as every other table in the verify block reports `OK`, the migration is good. Specifically: `sighting=614,505 / 614,505 OK`, `location=214,782 / 214,782 OK`, etc. Only `date_correction` should mismatch.

If anything else mismatches, **do** investigate.

---

## 9. `tee` and `\r` progress bars don't mix

**Symptom**: You run `python emotions.py | tee log.txt` and the progress line appears stuck at the same row count for minutes despite the process being healthy. Output buffers up. Eventually you kill the process thinking it hung.

**Cause**: Python's stdout is line-buffered when piped. The `\r` carriage return used to overwrite the progress line never includes a newline, so `tee` never flushes — until the process either finishes or crashes (which loses the buffered output).

**Mitigation**: redirect to file directly instead of `tee`:

```bash
# DON'T (output looks frozen)
python emotions.py | tee emotions.log

# DO
python emotions.py > emotions.log 2>&1
```

Or use `stdbuf` (Linux/Mac) / `python -u` to disable buffering:

```bash
python -u emotions.py | tee emotions.log
```

For monitoring progress without watching the log, query the DB directly:

```bash
python -c "
import sqlite3
conn = sqlite3.connect('ufo_unified.db')
n = conn.execute('SELECT COUNT(*) FROM sighting WHERE emotion_28_dominant IS NOT NULL').fetchone()[0]
print(f'Classified: {n:,}')
"
```

Each `analyze.py` step now commits per batch, so this gives accurate progress without relying on stdout.

---

## 10. `witness_names` is in the public DB

**Symptom**: You query `SELECT witness_names FROM sighting WHERE witness_names IS NOT NULL` and get 97,991 rows of actual names.

**Cause**: `witness_names` is short structured-adjacent text (mostly initials and surnames like `"AMATEUR ASTRONOMERS"` or `"MARTINO=ABALLAY=LARCHER=V #3"`), kept in `ufo_public.db` per the v0.8.3 scoping decision. The public app's detail modal shows this field.

**Concern**: 97,991 rows × short text strings = a non-trivial PII surface if the public DB is redistributed as a research artifact. The live app only exposes individual rows on demand via the detail endpoint; a downloadable file is a different distribution model.

**Mitigation if you redistribute**: strip the column before publishing:

```bash
python -c "
import sqlite3
conn = sqlite3.connect('ufo_public_redistribute.db')
conn.execute('UPDATE sighting SET witness_names = NULL')
conn.execute('VACUUM')
conn.commit()
"
```

Or extend `export_public.py`'s `RAW_COLUMNS_TO_DROP` and add `witness_names` to it. The `--drop-optional-free-text` flag in `export_public.py` already handles the bigger free-text fields (`witness_names`, `explanation`, `characteristics`, `weather`, `terrain`); enable it for redistributable exports.

Same applies to `explanation`, `characteristics`, `weather`, `terrain` — short structured-adjacent text fields kept by default, available to strip via `--drop-optional-free-text`.

---

## 11. Legacy `dominant_emotion` is 41% populated; new `emotion_7_dominant` is 82%

**Symptom**: You `JOIN` on emotion fields and get fewer rows than expected.

**Cause**: `dominant_emotion` is the v0.8.x NRC-keyword classifier output (8-class, mostly anticipation/trust/fear). Tied emotions return NULL, and ~50% of NRC classifications produce ties on observational text. So `dominant_emotion` is 253K populated.

`emotion_7_dominant` is the v0.11 RoBERTa transformer output. Always returns a single argmax label — no ties. So it's 503K populated (every row with text).

**Recommendation**: prefer `emotion_7_dominant` for new analyses. Keep `dominant_emotion` for backward compat with prior research / queries.

---

## 12. Hoax detection looks for "alien" — including in legitimate reports

**Symptom**: A serious-looking report scores `hoax_likelihood = 0.3` because `dramatic_no_specifics` fired.

**Cause**: The `dramatic_no_specifics` rule fires when text matches `\b(alien|abducted|probed|reptilian|grey|illuminati)\b` AND `richness_score < 3`. The richness gate is the safeguard — a genuine report with witnesses, location, duration, etc. won't fire. But a brief well-formed report ("Aliens hovered over my house for 5 minutes") with low feature richness could trigger.

**Mitigation**: don't filter on hoax_likelihood alone. Combine with quality_score:

```sql
WHERE hoax_likelihood < 0.3 AND quality_score >= 40
```

The quality score catches reports that are genuinely sparse-but-real, while the hoax score catches reports that are sparse-AND-dramatic. False positives drop dramatically.

For full transparency, the `hoax_flags` JSON in `sighting_analysis` shows exactly which rules fired:

```sql
SELECT s.id, s.description, a.hoax_flags
FROM sighting s JOIN sighting_analysis a ON s.id = a.sighting_id
WHERE a.hoax_flags LIKE '%dramatic_no_specifics%'
LIMIT 20;
```

You can audit borderline cases yourself.

---

## 13. The dedup engine flags "Unlikely" pairs (score 0.0-0.3) on purpose

**Symptom**: 78,456 rows in `duplicate_candidate` have `similarity_score < 0.3`. You wonder if the engine is broken.

**Cause**: It's not broken — those are pairs that share a date+city across sources but have descriptions different enough to be probably-different events. Tier 2c (UPDB↔others, city-only matching) generates many of these because it intentionally drops the state requirement, accepting more false positives in exchange for catching real cross-source duplicates that have inconsistent state data.

**Mitigation**: filter by score when consuming the table:

```sql
SELECT * FROM duplicate_candidate WHERE similarity_score >= 0.7;
-- 23,827 high-confidence pairs
```

The Unlikely bucket is documented for completeness — researchers studying dedup methodology may want to inspect it; analysts looking for "real" duplicates should filter.

---

## 14. The five `import_*.py` files don't share their parsing helpers

**Symptom**: You're adding a 6th source and look for a shared `parse_iso_date()` to call. There isn't one.

**Cause**: Each source's date / location / metadata format is genuinely idiosyncratic. NUFORC dates are `1995-02-02 23:00 Local`. MUFON dates have literal `\n` separators (`1992-08-19\n5:45AM`). UFOCAT splits date into `YEAR/MO/DAY/TIME` columns. UFO-search dates can be `"Summer 1947"`, `"6/24/1947"`, `"4/34"`, `"0's"`.

A shared parser would either be a giant cascade of source-specific branches, or each importer would duplicate enough wrapper logic that the abstraction wouldn't pay off. The current trade-off: each importer has its own `parse_<source>_date()` and `parse_<source>_location()` that's unit-tested in `tests/test_etl.py`.

**For new sources**: write source-specific parsers, test them in `tests/test_etl.py`. Don't try to generalize.

---

## 15. The pipeline assumes English text

**Symptom**: NRC, GoEmotions, RoBERTa all give weird results on non-English sightings (Brazilian, French, Russian, etc.).

**Cause**: All three emotion models are English-only. NRC's lexicon, GoEmotions' Reddit training data, and the RoBERTa sentiment model are all English-trained. Non-English text gets classified as `neutral` mostly because the models don't recognize the words.

The `language` column doesn't exist — most sources don't tag language. UFOCAT and UFO-search include some non-English reports interleaved with English ones.

**Mitigation**: filter to sources known to be primarily English (NUFORC, MUFON) when running emotion analyses:

```sql
WHERE source_db_id IN (1, 2)  -- MUFON, NUFORC
```

For multilingual sentiment analysis, replace `cardiffnlp/twitter-roberta-base-sentiment-latest` with `cardiffnlp/twitter-xlm-roberta-base-sentiment` in `emotions.py`. Slower but handles ~30 languages.

---

## Have you hit a pitfall not on this list?

Open an issue or PR — this doc is meant to grow. Particularly interested in pitfalls that bit you while reproducing the pipeline on different data snapshots.

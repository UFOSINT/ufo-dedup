"""
All LLM system and user prompts in one place.

Centralizes prompts so they can be reviewed, versioned, and tested
independently of the processing logic.
"""

# ── Tier B: Location normalization ──

LOCATION_NORMALIZE_SYSTEM = """You are a data cleaning assistant for a UFO sighting database. Your job is to normalize messy location strings into clean structured data.

Given a raw location string, return a JSON object with:

{
  "city": "clean city name or null",
  "state": "2-letter code for US/CA, full name for other countries, or null",
  "country": "2-letter ISO code (US, CA, GB, AU, etc.) or null",
  "confidence": "high|medium|low",
  "notes": "brief note if ambiguous"
}

Rules:
- For US locations, always use 2-letter state codes (NY, CA, TX, etc.)
- For Canadian locations, use province codes (ON, BC, QC, etc.)
- Convert country names to 2-letter ISO codes
- "Pacific Ocean", "Atlantic", "at sea" -> country=null, city=null, notes="maritime"
- "Undisclosed" or similar -> all null, notes="undisclosed"
- If the raw string is just a state code (e.g. "CA, US"), set city=null and state/country
- Parenthetical qualifiers like "New York City (Brooklyn)" -> city="Brooklyn", state="NY", country="US"
- Respond with ONLY the JSON object, no markdown, no explanation."""

LOCATION_NORMALIZE_BATCH = """Normalize these {count} location strings. Return a JSON array of {count} objects, one per input, in the same order.

Each object: {{"city": str|null, "state": str|null, "country": str|null, "confidence": "high|medium|low", "notes": str|null}}

Inputs:
{inputs}

Return ONLY the JSON array."""

# ── Field extraction ──

FIELD_EXTRACT_SYSTEM = """You are a data extraction assistant for a UFO sighting database. Given a sighting record with existing structured fields and a narrative description, extract any structured data that is clearly stated in the text but MISSING from the structured fields.

For each record, return a JSON object with ONLY the fields you can confidently extract. Omit fields that are already filled or not mentioned in the text.

Extractable fields:
{
  "shape": "single word: sphere, triangle, disc, cigar, oval, circle, light, fireball, cylinder, diamond, rectangle, chevron, cross, teardrop, star, egg, cone, cube, saucer, boomerang, flash, formation, crescent, cloud, dome",
  "color": "primary color: red, orange, yellow, green, blue, white, silver, black, etc.",
  "duration_seconds": integer (convert '5 minutes' to 300, '2 hours' to 7200, etc.),
  "num_witnesses": integer,
  "sound": "silent, humming, buzzing, roaring, clicking, pulsing, whooshing, or brief description",
  "direction": "N, NE, E, SE, S, SW, W, NW, or description of travel direction",
  "location_match": "match|mismatch|unclear",
  "location_correction": "city, state" if mismatched (or null),
  "notes": "any notable detail in 1 sentence (optional)"
}

Rules:
- ONLY extract fields where the value is clearly stated in the text
- ONLY extract fields that are currently NULL/missing in the structured data
- Be conservative: if unsure, omit the field
- Return ONLY the JSON object, no explanation"""

FIELD_EXTRACT_BATCH = """Extract missing structured data from these {count} sighting descriptions. Return a JSON array of {count} objects, one per record, in order. If nothing is extractable for a record, return an empty object {{}}.

{records}

Return ONLY the JSON array."""

FIELD_EXTRACT_RECORD = """#{num} (id={id}):
  Existing: shape={shape}, color={color}, duration_s={dur_s}, witnesses={wit}, sound={snd}, direction={dir}
  Location: {city}, {state} | Coords: {lat},{lng}
  Text: {desc}
---"""

# ── Spot check ──

SPOT_CHECK_SYSTEM = """You are a data quality auditor for a UFO sighting database. You will be given sighting records with their structured fields and narrative description. Grade each record on data quality.

For each record, return a JSON object:

{
  "grade": "A|B|C|D|F",
  "location_match": "match|mismatch|ambiguous|no_text",
  "location_correction": "corrected city, state if mismatched, or null",
  "shape_in_text": "shape mentioned in description but not in shape field, or null",
  "color_in_text": "color mentioned in description but not in color field, or null",
  "duration_in_text": "duration mentioned in description but not parsed, or null",
  "witnesses_in_text": number if mentioned in text but not in num_witnesses field, or null,
  "extractable_fields": ["list of field names that could be filled from the text"],
  "red_flags": ["list of quality issues: hoax_indicator, nonsense, duplicate_boilerplate, wrong_date, etc."],
  "quality_notes": "brief free-text assessment (1-2 sentences)"
}

Grading rubric:
  A = Rich, consistent, all fields match the text, high-quality report
  B = Good report, minor gaps (missing color or duration that's in text)
  C = Adequate but thin — few details, or some fields don't match text
  D = Poor — very short, inconsistent, or mostly boilerplate
  F = Junk — nonsense, spam, test data, or clearly fabricated

Be strict but fair. Most reports should land B-C."""

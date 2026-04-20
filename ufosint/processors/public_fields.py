"""
Public field denormalization processor.

Copies lat/lng from location table, builds sighting_datetime,
sets has_description and has_media flags.
"""

import re

from ufosint.processors.base import Processor, executemany_batched

# Media mentions in descriptions — feeds has_media
MEDIA_RE = re.compile(
    r"\b(photo(?:graph(?:ed|s)?)?|picture(?:s|d)?|\bpic(?:s)?\b|video(?:s|ed)?|"
    r"footage|film(?:ed|ing)?|recording|recorded|camera|camcorder|snapshot|"
    r"cellphone picture|cell phone picture|phone (?:photo|video|pic))\b",
    re.IGNORECASE,
)

# Time-of-day in a raw time_raw string
TIME_HHMM_RE = re.compile(r"\b(\d{1,2}):(\d{2})(?::\d{2})?\s*(am|pm)?\b", re.IGNORECASE)


def _build_sighting_datetime(date_event, time_raw):
    """Combine ISO date + raw time string into a best-effort ISO datetime.

    Rules:
      - date_event is the authoritative base (already normalized during import).
      - If date_event is NULL -> return None.
      - If date_event already contains 'T' or a space + digits, assume it has
        a time component and return as-is.
      - Otherwise try to parse time_raw as HH:MM[:SS] [am|pm]; if successful,
        concat as 'YYYY-MM-DDTHH:MM:SS'.
      - If no time is parseable, return date_event unchanged (date-only or year-only).
    """
    if not date_event:
        return None

    if "T" in date_event or re.search(r"\d{4}-\d{2}-\d{2}\s+\d{1,2}:", date_event):
        return date_event

    if not time_raw:
        return date_event

    m = TIME_HHMM_RE.search(time_raw)
    if not m:
        return date_event

    hour = int(m.group(1))
    minute = int(m.group(2))
    ampm = (m.group(3) or "").lower()
    if ampm == "pm" and hour < 12:
        hour += 12
    elif ampm == "am" and hour == 12:
        hour = 0

    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        return date_event

    # date_event may be 'YYYY', 'YYYY-MM', or 'YYYY-MM-DD' — only append time
    # when we have a full date.
    if len(date_event) >= 10 and date_event[4] == "-" and date_event[7] == "-":
        return f"{date_event[:10]}T{hour:02d}:{minute:02d}:00"
    return date_event


class PublicFieldDeriver(Processor):
    name = "public_fields"
    label = "Deriving public fields"

    def process(self, conn):
        cur = conn.cursor()

        # 1. lat / lng — one SQL update via location JOIN
        cur.execute("""
            UPDATE sighting SET
                lat = (SELECT latitude  FROM location WHERE location.id = sighting.location_id),
                lng = (SELECT longitude FROM location WHERE location.id = sighting.location_id)
        """)
        conn.commit()

        # 2. has_description (no need for Python — trivial SQL)
        cur.execute("""
            UPDATE sighting SET has_description = CASE
                WHEN (description IS NOT NULL AND TRIM(description) != '')
                  OR (summary IS NOT NULL AND TRIM(summary) != '')
                THEN 1 ELSE 0
            END
        """)
        conn.commit()

        # 3. sighting_datetime — needs Python to parse time_raw
        cur.execute("SELECT id, date_event, time_raw FROM sighting")
        dt_updates = [
            (_build_sighting_datetime(d, t), sid) for sid, d, t in cur.fetchall()
        ]
        executemany_batched(
            conn,
            "UPDATE sighting SET sighting_datetime = ? WHERE id = ?",
            dt_updates,
        )

        # 4. has_media — regex on description/summary OR attachment row exists
        cur.execute("SELECT DISTINCT sighting_id FROM attachment WHERE sighting_id IS NOT NULL")
        has_attachment = {row[0] for row in cur.fetchall()}

        cur.execute("SELECT id, description, summary FROM sighting")
        media_updates = []
        for sid, desc, summ in cur.fetchall():
            text = desc or summ or ""
            flag = 1 if (sid in has_attachment or MEDIA_RE.search(text)) else 0
            media_updates.append((flag, sid))

        executemany_batched(
            conn,
            "UPDATE sighting SET has_media = ? WHERE id = ?",
            media_updates,
        )

        # Stats
        cur.execute("SELECT COUNT(*) FROM sighting WHERE lat IS NOT NULL AND lng IS NOT NULL")
        coord_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM sighting WHERE sighting_datetime IS NOT NULL")
        dt_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM sighting WHERE has_description = 1")
        desc_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM sighting WHERE has_media = 1")
        media_count = cur.fetchone()[0]
        print(f"  Public fields derived: "
              f"coords={coord_count:,}, datetime={dt_count:,}, "
              f"has_description={desc_count:,}, has_media={media_count:,}")

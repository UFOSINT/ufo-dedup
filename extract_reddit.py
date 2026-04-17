"""
Pass 2: LLM-based structured extraction from raw Reddit post JSON.

Reads the raw scraped posts from data/raw/reddit/raw/{post_id}.json,
sends each to a cheap LLM via OpenRouter to extract structured sighting
fields, and saves the result to data/raw/reddit/extracted/{post_id}.json.

The LLM reconciles the xlsx metadata (date, location) with the actual
post content (title, body, OP comments) to produce a clean sighting record
matching the ufo-dedup schema.

Models (via OpenRouter, ~$0.05-0.10/M tokens):
  - Default: meta-llama/llama-3.1-8b-instruct (~$0.06/M)
  - Alternative: mistralai/mistral-7b-instruct (~$0.06/M)
  - Better quality: meta-llama/llama-3.1-70b-instruct (~$0.35/M)

Total cost estimate: 4,695 posts x ~1,300 tokens = ~6.1M tokens = ~$0.40

Usage:
    export OPENROUTER_API_KEY='sk-or-...'
    python extract_reddit.py                       # all unprocessed posts
    python extract_reddit.py --limit 10            # test with 10
    python extract_reddit.py --model meta-llama/llama-3.1-70b-instruct  # higher quality

Requires: OPENROUTER_API_KEY env var.
"""
import json
import os
import sys
import time

import requests

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DIR = os.path.join(BASE_DIR, "data", "raw", "reddit", "raw")
EXTRACTED_DIR = os.path.join(BASE_DIR, "data", "raw", "reddit", "extracted")
EXTRACT_LOG = os.path.join(BASE_DIR, "data", "raw", "reddit", "extract_log.json")

DEFAULT_MODEL = "meta-llama/llama-3.1-8b-instruct"
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"

SYSTEM_PROMPT = """You are a data extraction assistant for a UFO sighting research database. Given a Reddit post from r/UFOs (title, body, OP's comments, and metadata from a sighting spreadsheet), extract structured sighting information.

Return ONLY a valid JSON object with these fields. Use null for anything you cannot determine from the text. Do NOT invent or hallucinate information — only extract what the witness actually stated.

{
  "date_event": "ISO 8601 date or datetime of the SIGHTING (not the Reddit post date). Use YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS format.",
  "time_of_day": "HH:MM in 24-hour format if the witness mentioned a time, else null",
  "timezone": "timezone abbreviation if mentioned (EST, PST, UTC, etc.), else null",
  "city": "city or town name, else null",
  "state": "US state or Canadian province 2-letter code, else null",
  "country": "ISO 2-letter country code (US, CA, GB, AU, etc.), else null",
  "latitude": "float if coordinates were given by the witness, else null",
  "longitude": "float if coordinates were given by the witness, else null",
  "shape": "object shape as described (triangle, orb, disc, light, cigar, etc.), else null",
  "color": "color(s) described, else null",
  "duration": "free-text duration as stated by witness (e.g. '5 minutes', 'few seconds')",
  "duration_seconds": "integer estimate in seconds, else null",
  "num_witnesses": "integer number of witnesses if stated, else null",
  "num_objects": "integer number of objects if stated, else null",
  "sound": "sound description, 'silent' if explicitly stated silent, else null",
  "direction": "direction of travel or cardinal direction if mentioned, else null",
  "elevation": "altitude or elevation description if mentioned, else null",
  "movement": "movement description (hovering, fast, zigzag, stationary, etc.), else null",
  "has_photo": "true if the post includes or references a photo/image, false otherwise",
  "has_video": "true if the post includes or references video footage, false otherwise",
  "description": "Clean 1-3 paragraph factual narrative of the sighting, synthesized from the post body AND the OP's clarifying comments. Write as a sighting report, not as a Reddit post. Remove Reddit-specific language, calls to action, and social commentary. Keep the witness's factual observations intact.",
  "confidence": "high, medium, or low — your confidence that the extracted fields accurately represent what the witness reported"
}

Important rules:
- The spreadsheet date/location may be WRONG or imprecise. The post body and OP comments are the primary source of truth. Use the spreadsheet fields only when the post text is ambiguous.
- If the post is deleted/removed and has no text, return all fields as null with confidence: 'none'.
- If the post is primarily a video/image with no descriptive text, extract what you can from the title and OP comments.
- For date_event: this is when the SIGHTING happened, not when the Reddit post was submitted. Many posts describe sightings from days, months, or years earlier.
- For description: combine the main post body with any OP comments that add factual detail. Exclude off-topic discussion, debates, or meta-commentary."""

USER_PROMPT_TEMPLATE = """Reddit post from r/UFOs sighting report spreadsheet:

**Spreadsheet fields (may be imprecise):**
- Location: {xlsx_location}
- Date: {xlsx_date}
- Post submitted: {xlsx_submitted}

**Reddit post:**
- Title: {title}
- Author: {author}
- Posted: {created_utc}
- Score: {score} (upvote ratio: {upvote_ratio})
- Flair: {flair}
- Has media: {has_media}
- Media URLs: {media_urls}

**Post body:**
{selftext}

**OP's comments in the thread ({num_op_comments} comments):**
{op_comments}

**OP's Q&A pairs ({num_qa} exchanges):**
{qa_pairs}

Extract the structured sighting record as JSON."""


# ============================================================
# LLM call
# ============================================================

def call_openrouter(prompt, system=SYSTEM_PROMPT, model=DEFAULT_MODEL, api_key=None):
    """Call OpenRouter chat completion. Returns parsed JSON or error dict."""
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://ufosint.com",
        "X-Title": "UFOSINT Reddit Extractor",
    }

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.1,  # low temp for extraction tasks
        "max_tokens": 1000,
        "response_format": {"type": "json_object"},
    }

    try:
        resp = requests.post(OPENROUTER_URL, json=payload, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        content = data["choices"][0]["message"]["content"]
        # Parse the JSON response
        extracted = json.loads(content)

        # Attach usage stats
        usage = data.get("usage", {})
        extracted["_tokens_in"] = usage.get("prompt_tokens", 0)
        extracted["_tokens_out"] = usage.get("completion_tokens", 0)
        extracted["_model"] = model

        return extracted

    except requests.RequestException as e:
        return {"_error": f"request: {e}"}
    except (json.JSONDecodeError, KeyError, IndexError) as e:
        # Try to salvage partial JSON from the response
        try:
            content = data["choices"][0]["message"]["content"]
            # Sometimes the LLM wraps in markdown code blocks
            content = content.strip()
            if content.startswith("```"):
                content = content.split("```")[1]
                if content.startswith("json"):
                    content = content[4:]
            extracted = json.loads(content)
            extracted["_tokens_in"] = usage.get("prompt_tokens", 0)
            extracted["_tokens_out"] = usage.get("completion_tokens", 0)
            extracted["_model"] = model
            return extracted
        except Exception:
            return {"_error": f"parse: {e}", "_raw": content if "content" in dir() else ""}


def build_prompt(raw_post):
    """Build the user prompt from a raw scraped post."""
    selftext = raw_post.get("selftext", "") or "(no post body)"
    if raw_post.get("deleted") and not selftext.strip():
        selftext = "(post was deleted/removed)"

    op_comments = "\n---\n".join(raw_post.get("op_comments", [])[:10]) or "(none)"

    qa_pairs = ""
    for qa in raw_post.get("question_answer_pairs", [])[:5]:
        qa_pairs += f'Q: {qa["question"][:200]}\nA: {qa["answer"][:300]}\n---\n'
    qa_pairs = qa_pairs or "(none)"

    media_list = ", ".join(raw_post.get("media_urls", [])[:5]) or "(none)"

    created = raw_post.get("created_utc", "")
    if isinstance(created, (int, float)):
        from datetime import datetime, timezone
        created = datetime.fromtimestamp(created, tz=timezone.utc).isoformat()

    return USER_PROMPT_TEMPLATE.format(
        xlsx_location=raw_post.get("xlsx_location") or "(not provided)",
        xlsx_date=raw_post.get("xlsx_date") or "(not provided)",
        xlsx_submitted=raw_post.get("xlsx_submitted") or "(not provided)",
        title=raw_post.get("title", "(no title)"),
        author=raw_post.get("author", "[deleted]"),
        created_utc=created,
        score=raw_post.get("score", 0),
        upvote_ratio=raw_post.get("upvote_ratio", ""),
        flair=raw_post.get("flair") or "(none)",
        has_media="yes" if raw_post.get("media_urls") else "no",
        media_urls=media_list,
        selftext=selftext[:3000],  # cap to control token count
        num_op_comments=len(raw_post.get("op_comments", [])),
        op_comments=op_comments[:2000],
        num_qa=len(raw_post.get("question_answer_pairs", [])),
        qa_pairs=qa_pairs[:1500],
    )


# ============================================================
# Orchestration
# ============================================================

def load_extract_progress():
    if os.path.exists(EXTRACT_LOG):
        with open(EXTRACT_LOG, "r") as f:
            return set(json.load(f).get("completed", []))
    return set()


def save_extract_progress(completed):
    with open(EXTRACT_LOG, "w") as f:
        json.dump({"completed": sorted(completed), "count": len(completed)}, f)


def run_extraction(model=DEFAULT_MODEL, limit=None, resume=True):
    """Run LLM extraction on all raw-scraped posts."""
    api_key = os.environ.get("OPENROUTER_API_KEY")
    if not api_key:
        sys.exit("ERROR: set OPENROUTER_API_KEY env var. Get one at https://openrouter.ai/keys")

    # Find all raw-scraped posts
    if not os.path.isdir(RAW_DIR):
        sys.exit(f"ERROR: raw scrape directory not found: {RAW_DIR}\n  Run scrape_reddit.py first.")

    raw_files = sorted(f for f in os.listdir(RAW_DIR) if f.endswith(".json"))
    print(f"Raw posts available: {len(raw_files):,}")

    if limit:
        raw_files = raw_files[:limit]
        print(f"  (limited to first {limit})")

    # Resume support
    completed = load_extract_progress() if resume else set()
    remaining = [f for f in raw_files if f.replace(".json", "") not in completed]
    print(f"Already extracted: {len(completed):,}")
    print(f"Remaining: {len(remaining):,}")

    if not remaining:
        print("Nothing to extract.")
        return

    os.makedirs(EXTRACTED_DIR, exist_ok=True)

    print(f"Model: {model}")
    print(f"Estimated cost: ~${len(remaining) * 1300 * 0.07 / 1_000_000:.2f}")
    print()

    t0 = time.time()
    total_tokens_in = 0
    total_tokens_out = 0
    errors = 0

    for i, filename in enumerate(remaining):
        post_id = filename.replace(".json", "")

        # Load raw post
        with open(os.path.join(RAW_DIR, filename), "r", encoding="utf-8") as f:
            raw_post = json.load(f)

        # Skip deleted posts with no content
        if raw_post.get("deleted") and not raw_post.get("selftext", "").strip() and not raw_post.get("op_comments"):
            result = {
                "confidence": "none",
                "description": None,
                "_skipped": "deleted_no_content",
                "_model": model,
            }
        else:
            prompt = build_prompt(raw_post)
            result = call_openrouter(prompt, model=model, api_key=api_key)

        if "_error" in result:
            errors += 1
            if "429" in str(result.get("_error", "")):
                print(f"\n  Rate limited. Waiting 10s...")
                time.sleep(10)
                # Retry once
                result = call_openrouter(prompt, model=model, api_key=api_key)
                if "_error" in result:
                    errors += 1

        # Attach metadata
        result["post_id"] = post_id
        result["reddit_url"] = f"https://www.reddit.com/r/UFOs/comments/{post_id}/"
        result["media_urls"] = raw_post.get("media_urls", [])
        result["xlsx_location"] = raw_post.get("xlsx_location")
        result["xlsx_date"] = raw_post.get("xlsx_date")

        # Save
        outpath = os.path.join(EXTRACTED_DIR, f"{post_id}.json")
        with open(outpath, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)

        completed.add(post_id)
        total_tokens_in += result.get("_tokens_in", 0)
        total_tokens_out += result.get("_tokens_out", 0)

        # Progress
        elapsed = time.time() - t0
        rate = (i + 1) / elapsed if elapsed > 0 else 0
        eta = (len(remaining) - i - 1) / rate if rate > 0 else 0
        status = "ERR" if "_error" in result else "SKP" if result.get("_skipped") else "OK "
        sys.stdout.write(
            f"\r  [{status}] {i+1:,}/{len(remaining):,} "
            f"({100*(i+1)/len(remaining):.1f}%) "
            f"{rate:.1f}/s, ~{eta/60:.0f}m remaining  "
        )
        sys.stdout.flush()

        # Save progress every 25 posts
        if (i + 1) % 25 == 0:
            save_extract_progress(completed)

        # Brief delay to avoid OpenRouter rate limits
        time.sleep(0.5)

    save_extract_progress(completed)

    elapsed = time.time() - t0
    cost = (total_tokens_in + total_tokens_out) * 0.07 / 1_000_000
    print(f"\n\nExtraction complete:")
    print(f"  Posts: {len(remaining):,} in {elapsed:.0f}s ({elapsed/60:.1f} min)")
    print(f"  Errors: {errors:,}")
    print(f"  Tokens: {total_tokens_in:,} in + {total_tokens_out:,} out = {total_tokens_in+total_tokens_out:,}")
    print(f"  Estimated cost: ${cost:.3f}")
    print(f"  Saved to: {EXTRACTED_DIR}")


# ============================================================
# Merge extracted results into a single CSV
# ============================================================

def merge_to_csv(output_path=None):
    """Merge all extracted JSONs into a single CSV for import_reddit.py."""
    import csv

    if not output_path:
        output_path = os.path.join(BASE_DIR, "data", "raw", "reddit", "reddit_sightings_extracted.csv")

    if not os.path.isdir(EXTRACTED_DIR):
        sys.exit(f"ERROR: extracted directory not found: {EXTRACTED_DIR}")

    files = sorted(f for f in os.listdir(EXTRACTED_DIR) if f.endswith(".json"))
    print(f"Merging {len(files):,} extracted records...")

    fieldnames = [
        "post_id", "reddit_url", "date_event", "time_of_day", "timezone",
        "city", "state", "country", "latitude", "longitude",
        "shape", "color", "duration", "duration_seconds",
        "num_witnesses", "num_objects", "sound", "direction", "elevation",
        "movement", "has_photo", "has_video", "description", "confidence",
        "media_urls", "xlsx_location", "xlsx_date",
    ]

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()

        good = 0
        skipped = 0
        for filename in files:
            with open(os.path.join(EXTRACTED_DIR, filename), "r", encoding="utf-8") as jf:
                rec = json.load(jf)

            if rec.get("_skipped") or rec.get("_error") or rec.get("confidence") == "none":
                skipped += 1
                continue

            # Flatten media_urls to a JSON string
            rec["media_urls"] = json.dumps(rec.get("media_urls", []))

            writer.writerow(rec)
            good += 1

    print(f"  Written: {good:,} records to {output_path}")
    print(f"  Skipped: {skipped:,} (deleted/errored/no-content)")


# ============================================================
# CLI
# ============================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Extract structured sighting data via LLM (Pass 2)")
    parser.add_argument("--model", default=DEFAULT_MODEL, help=f"OpenRouter model (default: {DEFAULT_MODEL})")
    parser.add_argument("--limit", type=int, help="Only process first N posts")
    parser.add_argument("--no-resume", action="store_true", help="Start fresh")
    parser.add_argument("--merge", action="store_true", help="Only merge existing extractions to CSV (no LLM calls)")
    args = parser.parse_args()

    if args.merge:
        merge_to_csv()
    else:
        run_extraction(model=args.model, limit=args.limit, resume=not args.no_resume)

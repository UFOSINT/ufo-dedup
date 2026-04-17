"""
Pass 1: Scrape raw post data from Reddit for r/UFOs sighting reports.

Reads the work queue (data/raw/reddit/work_queue.json) produced by parsing
the xlsx index, fetches each post's full content + OP comments via the
Reddit JSON API, and saves raw JSON to data/raw/reddit/raw/{post_id}.json.

Two fetch modes:
  - Public JSON endpoint (default): appends .json to the post URL.
    No auth needed, ~60 req/min rate limit. Sufficient for a one-time
    backfill of 4,695 posts (~80 min).
  - PRAW (--use-praw): uses Reddit OAuth2 via the praw library.
    Faster, more reliable, handles rate limits automatically.
    Requires REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USER_AGENT
    env vars.

Usage:
    python scrape_reddit.py                        # public JSON, all posts
    python scrape_reddit.py --use-praw             # authenticated via PRAW
    python scrape_reddit.py --limit 10             # test with first 10 posts
    python scrape_reddit.py --resume               # skip already-scraped posts

Output:
    data/raw/reddit/raw/{post_id}.json  — one file per post
    data/raw/reddit/scrape_log.json     — progress tracker
"""
import json
import os
import re
import sys
import time

import requests

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
WORK_QUEUE = os.path.join(BASE_DIR, "data", "raw", "reddit", "work_queue.json")
RAW_DIR = os.path.join(BASE_DIR, "data", "raw", "reddit", "raw")
SCRAPE_LOG = os.path.join(BASE_DIR, "data", "raw", "reddit", "scrape_log.json")

DEFAULT_DELAY = 1.1  # seconds between requests (public endpoint ~60/min)
USER_AGENT = "UFOSINTScraper/1.0 (research; ufosint.com; contact: torylogos@gmail.com)"


# ============================================================
# Public JSON endpoint fetcher
# ============================================================

def fetch_post_json(reddit_url, session=None):
    """Fetch a Reddit post + comments via the public .json endpoint.

    Returns a dict with post data + OP comments extracted, or
    {'error': reason} on failure.
    """
    json_url = reddit_url.rstrip("/") + ".json"
    s = session or requests.Session()
    s.headers["User-Agent"] = USER_AGENT

    try:
        resp = s.get(json_url, timeout=15)
        if resp.status_code == 429:
            return {"error": "rate_limited", "retry_after": resp.headers.get("Retry-After", "60")}
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as e:
        return {"error": str(e)}
    except (json.JSONDecodeError, ValueError) as e:
        return {"error": f"json_decode: {e}"}

    if not isinstance(data, list) or len(data) < 2:
        return {"error": "unexpected_structure"}

    post = data[0]["data"]["children"][0]["data"]
    comments = data[1]["data"]["children"]

    op_author = post.get("author", "[deleted]")

    # Walk the comment tree to find OP's own replies
    op_comments = []
    question_answer_pairs = []

    def walk_comments(nodes, parent_body=None):
        for node in nodes:
            if node.get("kind") != "t1":
                continue
            c = node["data"]
            body = c.get("body", "")
            author = c.get("author", "")

            if author == op_author and author != "[deleted]":
                op_comments.append(body)
                if parent_body:
                    question_answer_pairs.append({
                        "question": parent_body[:500],
                        "answer": body,
                    })

            # Recurse into replies
            replies = c.get("replies")
            if isinstance(replies, dict):
                walk_comments(
                    replies.get("data", {}).get("children", []),
                    parent_body=body,
                )

    walk_comments(comments)

    # Extract media URLs
    media_urls = []
    post_url = post.get("url", "")

    # Direct media link
    if post_url and any(
        sig in post_url
        for sig in [
            "i.redd.it", "v.redd.it", "imgur.com", "youtube.com",
            "youtu.be", ".jpg", ".png", ".gif", ".mp4",
        ]
    ):
        media_urls.append(post_url)

    # Reddit gallery
    if post.get("media_metadata"):
        for mid, meta in post["media_metadata"].items():
            url = (meta.get("s") or {}).get("u")
            if url:
                media_urls.append(url.replace("&amp;", "&"))

    # Reddit video
    if post.get("is_video") and post.get("media"):
        rv = post["media"].get("reddit_video", {})
        if rv.get("fallback_url"):
            media_urls.append(rv["fallback_url"])

    # Crosspost media
    if post.get("crosspost_parent_list"):
        for xp in post["crosspost_parent_list"]:
            xp_url = xp.get("url", "")
            if xp_url and xp_url not in media_urls:
                media_urls.append(xp_url)

    return {
        "post_id": post.get("id", post.get("name", "")),
        "title": post.get("title", ""),
        "selftext": post.get("selftext", ""),
        "selftext_html": post.get("selftext_html", ""),
        "author": op_author,
        "created_utc": post.get("created_utc"),
        "score": post.get("score", 0),
        "upvote_ratio": post.get("upvote_ratio"),
        "flair": post.get("link_flair_text"),
        "url": post_url,
        "permalink": post.get("permalink", ""),
        "is_self": post.get("is_self", False),
        "is_video": post.get("is_video", False),
        "num_comments": post.get("num_comments", 0),
        "media_urls": media_urls,
        "op_comments": op_comments,
        "question_answer_pairs": question_answer_pairs,
        "deleted": op_author == "[deleted]" or post.get("selftext") == "[removed]",
    }


# ============================================================
# PRAW-based fetcher (optional, faster)
# ============================================================

def fetch_post_praw(post_id, reddit_instance):
    """Fetch via PRAW. Requires an authenticated reddit instance."""
    try:
        submission = reddit_instance.submission(id=post_id)
        submission.comments.replace_more(limit=0)

        op_author = str(submission.author) if submission.author else "[deleted]"

        op_comments = []
        question_answer_pairs = []

        for comment in submission.comments.list():
            if str(comment.author) == op_author and op_author != "[deleted]":
                op_comments.append(comment.body)
                if comment.parent_id.startswith("t1_"):
                    try:
                        parent = comment.parent()
                        question_answer_pairs.append({
                            "question": parent.body[:500],
                            "answer": comment.body,
                        })
                    except Exception:
                        pass

        media_urls = []
        if submission.url and submission.url != submission.permalink:
            media_urls.append(submission.url)
        if hasattr(submission, "media_metadata") and submission.media_metadata:
            for mid, meta in submission.media_metadata.items():
                url = (meta.get("s") or {}).get("u")
                if url:
                    media_urls.append(url.replace("&amp;", "&"))

        return {
            "post_id": submission.id,
            "title": submission.title,
            "selftext": submission.selftext,
            "selftext_html": submission.selftext_html or "",
            "author": op_author,
            "created_utc": submission.created_utc,
            "score": submission.score,
            "upvote_ratio": submission.upvote_ratio,
            "flair": submission.link_flair_text,
            "url": submission.url,
            "permalink": submission.permalink,
            "is_self": submission.is_self,
            "is_video": submission.is_video,
            "num_comments": submission.num_comments,
            "media_urls": media_urls,
            "op_comments": op_comments,
            "question_answer_pairs": question_answer_pairs,
            "deleted": op_author == "[deleted]" or submission.selftext == "[removed]",
        }
    except Exception as e:
        return {"error": str(e), "post_id": post_id}


# ============================================================
# Orchestration
# ============================================================

def load_progress():
    """Load scrape progress (set of completed post_ids)."""
    if os.path.exists(SCRAPE_LOG):
        with open(SCRAPE_LOG, "r") as f:
            return set(json.load(f).get("completed", []))
    return set()


def save_progress(completed):
    """Save scrape progress."""
    with open(SCRAPE_LOG, "w") as f:
        json.dump({"completed": sorted(completed), "count": len(completed)}, f)


def run_scrape(use_praw=False, limit=None, resume=True, delay=DEFAULT_DELAY):
    """Main scrape loop."""
    # Load work queue
    with open(WORK_QUEUE, "r", encoding="utf-8") as f:
        queue = json.load(f)
    print(f"Work queue: {len(queue):,} posts")

    if limit:
        queue = queue[:limit]
        print(f"  (limited to first {limit})")

    # Resume support
    completed = load_progress() if resume else set()
    remaining = [p for p in queue if p["post_id"] not in completed]
    print(f"Already scraped: {len(completed):,}")
    print(f"Remaining: {len(remaining):,}")

    if not remaining:
        print("Nothing to scrape.")
        return

    os.makedirs(RAW_DIR, exist_ok=True)

    # Init PRAW if requested
    reddit = None
    if use_praw:
        try:
            import praw
            reddit = praw.Reddit(
                client_id=os.environ["REDDIT_CLIENT_ID"],
                client_secret=os.environ["REDDIT_CLIENT_SECRET"],
                user_agent=os.environ.get("REDDIT_USER_AGENT", USER_AGENT),
            )
            print(f"PRAW authenticated as: {reddit.user.me()}")
        except Exception as e:
            print(f"PRAW init failed: {e}")
            print("Falling back to public JSON endpoint.")
            use_praw = False

    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT

    t0 = time.time()
    errors = 0

    for i, post in enumerate(remaining):
        pid = post["post_id"]
        url = post["url"]

        # Fetch
        if use_praw and reddit:
            result = fetch_post_praw(pid, reddit)
        else:
            result = fetch_post_json(url, session)

        # Handle rate limiting
        if result.get("error") == "rate_limited":
            wait = int(result.get("retry_after", 60))
            print(f"\n  Rate limited. Waiting {wait}s...")
            time.sleep(wait)
            result = fetch_post_json(url, session)

        # Save
        if "error" in result:
            errors += 1
            result["post_id"] = pid
            result["url"] = url

        # Attach xlsx metadata
        result["xlsx_location"] = post.get("xlsx_location")
        result["xlsx_date"] = post.get("xlsx_date")
        result["xlsx_submitted"] = post.get("xlsx_submitted")
        result["xlsx_row"] = post.get("xlsx_row")

        outpath = os.path.join(RAW_DIR, f"{pid}.json")
        with open(outpath, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)

        completed.add(pid)

        # Progress
        elapsed = time.time() - t0
        rate = (i + 1) / elapsed if elapsed > 0 else 0
        eta = (len(remaining) - i - 1) / rate if rate > 0 else 0
        status = "ERR" if "error" in result else "DEL" if result.get("deleted") else "OK "
        sys.stdout.write(
            f"\r  [{status}] {i+1:,}/{len(remaining):,} "
            f"({100*(i+1)/len(remaining):.1f}%) "
            f"{rate:.1f}/s, ~{eta/60:.0f}m remaining  "
        )
        sys.stdout.flush()

        # Save progress every 50 posts
        if (i + 1) % 50 == 0:
            save_progress(completed)

        # Rate limit delay (public endpoint only)
        if not use_praw:
            time.sleep(delay)

    save_progress(completed)

    elapsed = time.time() - t0
    print(f"\n\nScrape complete:")
    print(f"  Total: {len(remaining):,} posts in {elapsed:.0f}s ({elapsed/60:.1f} min)")
    print(f"  Errors: {errors:,}")
    print(f"  Saved to: {RAW_DIR}")

    # Quick stats on the scraped data
    deleted = 0
    has_text = 0
    has_media = 0
    has_op_comments = 0
    for p in remaining:
        path = os.path.join(RAW_DIR, f"{p['post_id']}.json")
        if not os.path.exists(path):
            continue
        with open(path, "r", encoding="utf-8") as f:
            d = json.load(f)
        if d.get("deleted"):
            deleted += 1
        if d.get("selftext") and len(d["selftext"]) > 10:
            has_text += 1
        if d.get("media_urls"):
            has_media += 1
        if d.get("op_comments"):
            has_op_comments += 1

    print(f"\n  Stats:")
    print(f"    Deleted/removed: {deleted:,}")
    print(f"    Has selftext: {has_text:,}")
    print(f"    Has media: {has_media:,}")
    print(f"    Has OP comments: {has_op_comments:,}")


# ============================================================
# CLI
# ============================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Scrape r/UFOs sighting posts (Pass 1)")
    parser.add_argument("--use-praw", action="store_true", help="Use PRAW (authenticated) instead of public JSON")
    parser.add_argument("--limit", type=int, help="Only scrape first N posts (for testing)")
    parser.add_argument("--no-resume", action="store_true", help="Start fresh, ignore previous progress")
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY, help=f"Seconds between requests (default {DEFAULT_DELAY})")
    args = parser.parse_args()

    run_scrape(
        use_praw=args.use_praw,
        limit=args.limit,
        resume=not args.no_resume,
        delay=args.delay,
    )

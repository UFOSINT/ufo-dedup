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

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
except ImportError:
    pass

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
WORK_QUEUE = os.path.join(BASE_DIR, "data", "raw", "reddit", "work_queue.json")
RAW_DIR = os.path.join(BASE_DIR, "data", "raw", "reddit", "raw")
SCRAPE_LOG = os.path.join(BASE_DIR, "data", "raw", "reddit", "scrape_log.json")

DEFAULT_DELAY = 1.1  # seconds between requests (public endpoint ~60/min)
USER_AGENT = "UFOSINTScraper/1.0 (research; ufosint.com; contact: torylogos@gmail.com)"


# ============================================================
# UFO-mode CLI chrome
# ============================================================
if sys.platform == "win32":
    os.system("")  # enable ANSI VT processing on Windows conhost

NO_COLOR = os.environ.get("NO_COLOR") or not sys.stdout.isatty()

class C:
    RESET  = "" if NO_COLOR else "\033[0m"
    DIM    = "" if NO_COLOR else "\033[2m"
    BOLD   = "" if NO_COLOR else "\033[1m"
    GREEN  = "" if NO_COLOR else "\033[92m"
    YELLOW = "" if NO_COLOR else "\033[93m"
    RED    = "" if NO_COLOR else "\033[91m"
    CYAN   = "" if NO_COLOR else "\033[96m"
    MAGENTA= "" if NO_COLOR else "\033[95m"
    BLUE   = "" if NO_COLOR else "\033[94m"
    GREY   = "" if NO_COLOR else "\033[90m"

BANNER = r"""
{c}       ╔═══════════════════════════════════════════════════════════════╗
       ║                       .  ·  ·   ·  ·  .                      ║
       ║                 ·    ▄▄▄███████▄▄▄    ·                      ║
       ║              ·   ▄██▀▀         ▀▀██▄   ·                     ║
       ║               ▄██                   ██▄                      ║
       ║              █████████████████████████     ·                 ║
       ║               ▀▀██▄▄  ·  ·  ·  ▄▄██▀▀                        ║
       ║              ·    ▀▀▀▀███████▀▀▀▀    ·                       ║
       ║                 ·     ·  ·  ·  ·  ·                          ║
       ║                                                               ║
       ║     {m}r/UFOs SIGHTING SCRAPER{c} · {g}PASS 1{c} · {d}transmission v1.0{c}      ║
       ╚═══════════════════════════════════════════════════════════════╝{r}
""".format(c=C.CYAN, m=C.MAGENTA + C.BOLD, g=C.GREEN + C.BOLD, d=C.DIM + C.CYAN, r=C.RESET)

REPORT_WIDTH = 54  # inner width of the intelligence report box

def _row(plain, display=None):
    """One row of the intelligence report. `plain` is used for width; `display`
    may include ANSI color codes (zero visual width)."""
    if display is None:
        display = plain
    pad = max(0, REPORT_WIDTH - len(plain))
    return f"{C.CYAN}   ║{C.RESET}{display}{' ' * pad}{C.CYAN}║{C.RESET}"

def _tag(color, label):
    return f"{color}[{label:^5}]{C.RESET}"

TAG_SYS  = lambda: _tag(C.CYAN,    "SYS")
TAG_NET  = lambda: _tag(C.BLUE,    "NET")
TAG_OK   = lambda: _tag(C.GREEN,   " OK ")
TAG_DEL  = lambda: _tag(C.YELLOW,  "DEAD")
TAG_ERR  = lambda: _tag(C.RED,     "ERR!")
TAG_RATE = lambda: _tag(C.MAGENTA, "WAIT")
TAG_INTEL= lambda: _tag(C.MAGENTA, "INTL")

def sysline(msg):  print(f"{TAG_SYS()} {msg}")
def netline(msg):  print(f"{TAG_NET()} {msg}")
def warnline(msg): print(f"{TAG_DEL()} {C.YELLOW}{msg}{C.RESET}")
def errline(msg):  print(f"{TAG_ERR()} {C.RED}{msg}{C.RESET}")

def bar(frac, width=32):
    filled = int(width * frac)
    return (
        f"{C.GREEN}{'█' * filled}{C.GREY}{'░' * (width - filled)}{C.RESET}"
    )


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
                        "question": parent_body,
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

    # Reddit gallery — prefer gallery_data ordering when present
    media_metadata = post.get("media_metadata") or {}
    gallery_data = post.get("gallery_data") or {}
    gallery_items = gallery_data.get("items") or []
    if gallery_items:
        for item in gallery_items:
            meta = media_metadata.get(item.get("media_id"), {})
            url = (meta.get("s") or {}).get("u")
            if url:
                media_urls.append(url.replace("&amp;", "&"))
    elif media_metadata:
        for _mid, meta in media_metadata.items():
            url = (meta.get("s") or {}).get("u")
            if url:
                media_urls.append(url.replace("&amp;", "&"))

    # Reddit video
    if post.get("is_video") and post.get("media"):
        rv = post["media"].get("reddit_video", {})
        if rv.get("fallback_url"):
            media_urls.append(rv["fallback_url"])

    # Preview images (lower priority — thumbnails, but often the only media
    # source for link posts where the external URL is no longer reachable)
    preview = post.get("preview") or {}
    for img in preview.get("images") or []:
        src = (img.get("source") or {}).get("url")
        if src:
            media_urls.append(src.replace("&amp;", "&"))

    # Crosspost media — recurse into parent's url + media_metadata
    for xp in post.get("crosspost_parent_list") or []:
        xp_url = xp.get("url", "")
        if xp_url:
            media_urls.append(xp_url)
        for _mid, meta in (xp.get("media_metadata") or {}).items():
            url = (meta.get("s") or {}).get("u")
            if url:
                media_urls.append(url.replace("&amp;", "&"))

    # Dedupe while preserving order
    seen = set()
    media_urls = [u for u in media_urls if not (u in seen or seen.add(u))]

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
                            "question": parent.body,
                            "answer": comment.body,
                        })
                    except Exception:
                        pass

        media_urls = []
        if submission.url and submission.url != submission.permalink:
            media_urls.append(submission.url)

        mm = getattr(submission, "media_metadata", None) or {}
        gd = getattr(submission, "gallery_data", None) or {}
        gitems = gd.get("items") if isinstance(gd, dict) else []
        if gitems:
            for item in gitems:
                meta = mm.get(item.get("media_id"), {})
                url = (meta.get("s") or {}).get("u")
                if url:
                    media_urls.append(url.replace("&amp;", "&"))
        elif mm:
            for _mid, meta in mm.items():
                url = (meta.get("s") or {}).get("u")
                if url:
                    media_urls.append(url.replace("&amp;", "&"))

        if submission.is_video:
            media = getattr(submission, "media", None) or {}
            rv = media.get("reddit_video", {}) if isinstance(media, dict) else {}
            if rv.get("fallback_url"):
                media_urls.append(rv["fallback_url"])

        preview = getattr(submission, "preview", None) or {}
        for img in preview.get("images") or []:
            src = (img.get("source") or {}).get("url")
            if src:
                media_urls.append(src.replace("&amp;", "&"))

        for xp in getattr(submission, "crosspost_parent_list", None) or []:
            xp_url = xp.get("url", "")
            if xp_url:
                media_urls.append(xp_url)
            for _mid, meta in (xp.get("media_metadata") or {}).items():
                url = (meta.get("s") or {}).get("u")
                if url:
                    media_urls.append(url.replace("&amp;", "&"))

        seen = set()
        media_urls = [u for u in media_urls if not (u in seen or seen.add(u))]

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
    print(BANNER)

    with open(WORK_QUEUE, "r", encoding="utf-8") as f:
        queue = json.load(f)
    sysline(f"target queue loaded         :: {C.BOLD}{len(queue):,}{C.RESET} sightings catalogued")

    if limit:
        queue = queue[:limit]
        sysline(f"{C.YELLOW}reconnaissance mode{C.RESET}         :: first {C.BOLD}{limit}{C.RESET} targets only")

    completed = load_progress() if resume else set()
    if resume and os.path.isdir(RAW_DIR):
        for fname in os.listdir(RAW_DIR):
            if fname.endswith(".json"):
                completed.add(fname[:-5])
    remaining = [p for p in queue if p["post_id"] not in completed]
    sysline(f"previously acquired         :: {C.DIM}{len(completed):,}{C.RESET}")
    sysline(f"remaining signals           :: {C.BOLD}{C.CYAN}{len(remaining):,}{C.RESET}")

    if not remaining:
        sysline(f"{C.GREEN}all targets acquired — nothing to do.{C.RESET}")
        return

    os.makedirs(RAW_DIR, exist_ok=True)

    reddit = None
    if use_praw:
        try:
            import praw
            netline(f"{C.DIM}handshaking with reddit data gateway...{C.RESET}")
            reddit = praw.Reddit(
                client_id=os.environ["REDDIT_CLIENT_ID"],
                client_secret=os.environ["REDDIT_CLIENT_SECRET"],
                username=os.environ.get("REDDIT_USERNAME"),
                password=os.environ.get("REDDIT_PASSWORD"),
                user_agent=os.environ.get("REDDIT_USER_AGENT", USER_AGENT),
            )
            me = reddit.user.me()
            netline(f"authenticated as            :: {C.GREEN}{C.BOLD}{me}{C.RESET} {C.GREEN}●{C.RESET} {C.DIM}OAUTH SECURE{C.RESET}")
        except KeyError as e:
            warnline(f"missing env var: {e}")
            warnline("set REDDIT_CLIENT_ID / _SECRET / _USERNAME / _PASSWORD in .env")
            warnline("falling back to public JSON endpoint.")
            use_praw = False
        except Exception as e:
            warnline(f"PRAW init failed: {e}")
            warnline("falling back to public JSON endpoint.")
            use_praw = False
    else:
        netline(f"mode                        :: {C.DIM}public json endpoint (unauth){C.RESET}")

    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT

    print(f"\n{C.CYAN}    ▼ ▼ ▼  engaging deep scan  ▼ ▼ ▼{C.RESET}\n")

    t0 = time.time()
    errors = 0
    dead = 0

    for i, post in enumerate(remaining):
        pid = post["post_id"]
        url = post["url"]

        if use_praw and reddit:
            result = fetch_post_praw(pid, reddit)
        else:
            result = fetch_post_json(url, session)

        if result.get("error") == "rate_limited":
            wait = int(result.get("retry_after", 60))
            print(f"\n{TAG_RATE()} {C.MAGENTA}throttled by gateway — cooling down {wait}s...{C.RESET}")
            time.sleep(wait)
            result = fetch_post_json(url, session)

        if "error" in result:
            errors += 1
            result["post_id"] = pid
            result["url"] = url

        result["xlsx_location"] = post.get("xlsx_location")
        result["xlsx_date"] = post.get("xlsx_date")
        result["xlsx_submitted"] = post.get("xlsx_submitted")
        result["xlsx_row"] = post.get("xlsx_row")

        outpath = os.path.join(RAW_DIR, f"{pid}.json")
        with open(outpath, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)

        completed.add(pid)

        elapsed = time.time() - t0
        rate = (i + 1) / elapsed if elapsed > 0 else 0
        eta = (len(remaining) - i - 1) / rate if rate > 0 else 0
        frac = (i + 1) / len(remaining)

        if "error" in result:
            tag = TAG_ERR()
        elif result.get("deleted"):
            tag = TAG_DEL()
            dead += 1
        else:
            tag = TAG_OK()

        eta_str = (
            f"{eta/60:4.0f}m" if eta >= 60 else f"{eta:4.0f}s"
        )
        sys.stdout.write(
            f"\r  {tag} {bar(frac)} "
            f"{C.BOLD}{i+1:>5,}{C.RESET}{C.DIM}/{len(remaining):,}{C.RESET} "
            f"{C.CYAN}{frac*100:5.1f}%{C.RESET}  "
            f"{C.DIM}{rate:4.1f}/s  eta {eta_str}{C.RESET}  "
            f"{C.MAGENTA}◉{C.RESET} {C.DIM}{pid}{C.RESET}    "
        )
        sys.stdout.flush()

        if (i + 1) % 50 == 0:
            save_progress(completed)

        if not use_praw:
            time.sleep(delay)

    save_progress(completed)

    elapsed = time.time() - t0

    # Recount from disk for intelligence report
    has_text = has_media = has_op = deleted_total = 0
    for p in remaining:
        path = os.path.join(RAW_DIR, f"{p['post_id']}.json")
        if not os.path.exists(path):
            continue
        with open(path, "r", encoding="utf-8") as f:
            d = json.load(f)
        if d.get("deleted"): deleted_total += 1
        if d.get("selftext") and len(d["selftext"]) > 10: has_text += 1
        if d.get("media_urls"): has_media += 1
        if d.get("op_comments"): has_op += 1

    n = len(remaining)
    def pct(x): return f"{100*x/n:5.1f}%" if n else "    -"

    err_color = C.RED if errors else C.DIM
    print("\n")
    print(f"{C.CYAN}   ╔══════════════════════════════════════════════════════╗{C.RESET}")

    title_plain = "         ◉  INTELLIGENCE REPORT  ◉"
    title_disp  = f"         {C.MAGENTA}{C.BOLD}◉  INTELLIGENCE REPORT  ◉{C.RESET}"
    print(_row(title_plain, title_disp))

    print(f"{C.CYAN}   ╠══════════════════════════════════════════════════════╣{C.RESET}")

    rows = [
        (f"   signals acquired       ::  {n:>6,}",
         f"   signals acquired       ::  {C.BOLD}{n:>6,}{C.RESET}"),
        (f"   transmission duration  ::  {elapsed/60:>5.1f}m ({elapsed:.0f}s)",
         f"   transmission duration  ::  {C.BOLD}{elapsed/60:>5.1f}m{C.RESET} ({elapsed:.0f}s)"),
        (f"   errors / anomalies     ::  {errors:>6,}",
         f"   errors / anomalies     ::  {err_color}{errors:>6,}{C.RESET}"),
    ]
    for p, d in rows:
        print(_row(p, d))

    print(f"{C.CYAN}   ╟──────────────────────────────────────────────────────╢{C.RESET}")

    sig_rows = [
        ("classified / redacted", deleted_total, C.YELLOW),
        ("testimony captured   ", has_text,      C.GREEN),
        ("media attached       ", has_media,     C.GREEN),
        ("OP cross-examined    ", has_op,        C.GREEN),
    ]
    for label, val, color in sig_rows:
        plain = f"   {label} ::  {val:>6,}  ({pct(val)})"
        disp  = f"   {color}{label}{C.RESET} ::  {C.BOLD}{val:>6,}{C.RESET}  {C.DIM}({pct(val)}){C.RESET}"
        print(_row(plain, disp))

    print(f"{C.CYAN}   ╚══════════════════════════════════════════════════════╝{C.RESET}")
    print(f"\n   {C.DIM}vault →{C.RESET} {C.CYAN}{RAW_DIR}{C.RESET}")
    print(f"   {C.MAGENTA}▲ standing by for analysis team handoff ▲{C.RESET}\n")


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

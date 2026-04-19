"""
Live-updating terminal dashboard for long-running batch operations.

Renders a full-screen display with progress bar, stats, and recent activity.
Thread-safe — call update() from worker threads, render() from main thread.

Usage:
    from ufosint.display.dashboard import Dashboard

    dash = Dashboard(
        title="LLM Field Extraction",
        total=50000,
        workers=15,
        counters=["enriched", "errors", "loc_mismatches"],
        log_file="enrich_extract.log",
    )

    # From worker threads:
    dash.increment("enriched")
    dash.add_recent("id=12345: shape=triangle, color=red")
    dash.add_field("shape")
    dash.tick()  # +1 to processed count

    # From main thread (after each batch completes):
    dash.render()

    # When done:
    dash.finish()
"""

import sys
import threading
import time
from collections import Counter
from datetime import datetime

from ufosint.display.colors import C
from ufosint.display.progress import progress_bar, format_eta, format_duration
from ufosint.display.log import Logger


class Dashboard:
    """Thread-safe live dashboard for batch processing."""

    def __init__(self, title, total, workers=1, counters=None, log_file=None,
                 max_recent=6):
        """
        Args:
            title: display title (e.g., "LLM Field Extraction")
            total: total items to process
            workers: number of parallel workers
            counters: list of named counters (e.g., ["enriched", "errors"])
            log_file: filename for Logger (None = no file logging)
            max_recent: max recent activity lines to show
        """
        self.title = title
        self.total = total
        self.workers = workers
        self.max_recent = max_recent

        self.lock = threading.Lock()
        self.processed = 0
        self.counters = {name: 0 for name in (counters or [])}
        self.fields = Counter()  # field name -> count
        self.recent = []         # recent activity strings
        self.batches_done = 0
        self.batches_total = 0
        self.t0 = time.time()
        self._last_render = 0

        # Logger
        self.logger = None
        if log_file:
            self.logger = Logger(log_file)
            self.logger.info(
                f"START: {title} total={total} workers={workers}"
            )

        # Ensure UTF-8 on Windows
        if sys.platform == "win32":
            try:
                sys.stdout.reconfigure(encoding="utf-8", errors="replace")
            except Exception:
                pass

        # Clear screen
        sys.stdout.write("\033[2J\033[H")
        sys.stdout.flush()

    def tick(self, n=1):
        """Increment processed count (thread-safe)."""
        with self.lock:
            self.processed += n

    def increment(self, counter_name, n=1):
        """Increment a named counter (thread-safe)."""
        with self.lock:
            if counter_name in self.counters:
                self.counters[counter_name] += n

    def add_field(self, field_name, n=1):
        """Increment a field extraction counter (thread-safe)."""
        with self.lock:
            self.fields[field_name] += n

    def add_recent(self, text):
        """Add a recent activity line (thread-safe)."""
        with self.lock:
            self.recent.append(text)
            if len(self.recent) > self.max_recent:
                self.recent.pop(0)

    def log(self, msg):
        """Write to log file if available."""
        if self.logger:
            self.logger.info(msg)

    def batch_done(self):
        """Increment batch counter."""
        with self.lock:
            self.batches_done += 1

    def render(self):
        """Render the dashboard to stdout. Throttled to max 3/s."""
        now = time.time()
        if now - self._last_render < 0.3:
            return
        self._last_render = now

        with self.lock:
            elapsed = now - self.t0
            pct = self.processed / self.total if self.total > 0 else 0
            rate = self.processed / elapsed if elapsed > 0 else 0
            eta = (self.total - self.processed) / rate if rate > 0 else 0

            lines = []
            lines.append("")
            lines.append(f"  {C.BOLD}{C.CYAN}{'=' * 62}{C.RESET}")
            lines.append(f"  {C.BOLD}{C.WHITE}  UFOSINT {self.title}{C.RESET}")
            lines.append(f"  {C.BOLD}{C.CYAN}{'=' * 62}{C.RESET}")
            lines.append("")

            # Progress bar
            bar = progress_bar(pct)
            lines.append(f"  {C.BOLD}Progress:{C.RESET}  {bar}  {C.BOLD}{pct*100:5.1f}%{C.RESET}")
            lines.append("")

            # Stats grid — left column: counters, right column: timing
            lines.append(
                f"  {C.DIM}Records:{C.RESET}   {C.BOLD}{self.processed:>8,}{C.RESET}"
                f" / {self.total:,}"
                f"      {C.DIM}Rate:{C.RESET}    {C.BOLD}{rate:>6.1f}{C.RESET}/s"
            )

            # Dynamic counter lines
            counter_items = list(self.counters.items())
            timing_labels = [
                ("Elapsed", format_duration(elapsed)),
                ("ETA", format_eta(eta)),
                ("Workers", str(self.workers)),
            ]
            if self.batches_total:
                timing_labels.append(("Batches", f"{self.batches_done}/{self.batches_total}"))

            for i, (cname, cval) in enumerate(counter_items):
                color = C.GREEN if cname in ("enriched", "improved", "fixed") else (
                    C.RED if cname == "errors" else C.YELLOW
                )
                right = ""
                if i < len(timing_labels):
                    rlabel, rval = timing_labels[i]
                    right = f"{C.DIM}{rlabel}:{C.RESET} {C.BOLD}{rval:>6}{C.RESET}"

                lines.append(
                    f"  {color}{cname.replace('_', ' ').title()}:{C.RESET}"
                    f"  {C.BOLD}{cval:>8,}{C.RESET}"
                    f"                   {right}"
                )

            # Any remaining timing labels
            for j in range(len(counter_items), len(timing_labels)):
                rlabel, rval = timing_labels[j]
                lines.append(f"  {'':>30}           {C.DIM}{rlabel}:{C.RESET} {C.BOLD}{rval:>6}{C.RESET}")

            lines.append("")

            # Field extraction counts
            if self.fields:
                lines.append(f"  {C.BOLD}{C.MAGENTA}Fields:{C.RESET}")
                for field, n in self.fields.most_common(8):
                    fpct = 100 * n / max(self.processed, 1)
                    lines.append(
                        f"    {field:<18} {C.GREEN}{n:>6,}{C.RESET}  ({fpct:.0f}%)"
                    )
                lines.append("")

            # Recent activity
            if self.recent:
                lines.append(f"  {C.BOLD}{C.YELLOW}Recent:{C.RESET}")
                for text in self.recent[-self.max_recent:]:
                    # Truncate and ASCII-safe
                    safe = text[:70].encode("ascii", errors="replace").decode()
                    lines.append(f"    {C.DIM}{safe}{C.RESET}")
            else:
                lines.append(f"  {C.DIM}Waiting for first results...{C.RESET}")

            lines.append("")
            if self.logger:
                lines.append(f"  {C.DIM}Log: {self.logger.path}{C.RESET}")
            lines.append(f"  {C.BOLD}{C.CYAN}{'=' * 62}{C.RESET}")

            # Clear and redraw
            output = "\033[H\033[J" + "\n".join(lines)
            sys.stdout.write(output)
            sys.stdout.flush()

    def finish(self):
        """Render final state and print summary below."""
        self.render()
        elapsed = time.time() - self.t0
        rate = self.processed / elapsed if elapsed > 0 else 0

        summary = {
            "processed": self.processed,
            "elapsed_s": round(elapsed, 1),
            "rate": round(rate, 1),
        }
        summary.update(self.counters)
        if self.fields:
            summary["fields"] = dict(self.fields)

        if self.logger:
            self.logger.complete(summary)
            self.logger.close()

        # Print summary below the dashboard
        print("\n\n")
        print(f"  {C.BOLD}{C.GREEN}COMPLETE{C.RESET}")
        print(f"  {C.DIM}{'_' * 50}{C.RESET}")
        print(f"  Processed:  {self.processed:>8,}")
        for cname, cval in self.counters.items():
            color = C.GREEN if cname in ("enriched", "improved", "fixed") else ""
            print(f"  {cname.replace('_', ' ').title():<12} {color}{cval:>8,}{C.RESET}")
        print(f"  Elapsed:    {format_duration(elapsed):>8}")
        print(f"  Rate:       {rate:>8.1f}/s")

        if self.fields:
            print(f"  {C.DIM}{'_' * 50}{C.RESET}")
            print(f"  {C.BOLD}Fields:{C.RESET}")
            for field, n in self.fields.most_common():
                print(f"    {field:<18} {n:>6,}")

        print(f"  {C.DIM}{'_' * 50}{C.RESET}")
        if self.logger:
            print(f"  {C.DIM}Log: {self.logger.path}{C.RESET}")
        print()

        return summary

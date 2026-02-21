"""
Moltbook Full Graph Scraper — two-phase Metaflow foreach fanout.

Phase 1: Fan out by submolt — one worker per submolt, page-based pagination.
Phase 2: Fan out comment scraping across N workers by post shard.

All GET endpoints work without authentication.
Pagination uses ?page=N (not offset).

Usage:
    python agent_scraper.py run
    python agent_scraper.py run --max_pages_per_shard 10  # test with limited pages
"""

from metaflow import FlowSpec, step, Parameter, retry, resources, current
import random
import time
import math


API_BASE = "https://www.moltbook.com/api/v1"
POSTS_PER_PAGE = 25
COMMENTS_PER_PAGE = 200  # API max per request
RATE_LIMIT_SLEEP = 0.6  # ~100 req/min per IP
RATE_LIMIT_BACKOFF_BASE = 5  # seconds, doubled on consecutive 429s
RATE_LIMIT_MAX_BACKOFF = 300  # 5 min max wait


def _session():
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    s = requests.Session()
    retries = Retry(
        total=8,
        backoff_factor=2,
        status_forcelist=[500, 502, 503, 504],
        # Don't auto-retry 429 — we handle it manually with backoff
        respect_retry_after_header=True,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    return s


def _request_with_backoff(session, method, url, **kwargs):
    """Make a request with exponential backoff on 429 rate limits.

    Retries up to 6 times with exponential backoff (5s, 10s, 20s, 40s, 80s, 160s).
    Also respects Retry-After header if the API provides one.
    """
    backoff = RATE_LIMIT_BACKOFF_BASE
    for attempt in range(7):
        resp = session.request(method, url, **kwargs)
        if resp.status_code != 429:
            return resp
        # Rate limited — back off
        retry_after = resp.headers.get("Retry-After")
        if retry_after:
            try:
                wait = int(retry_after)
            except ValueError:
                wait = backoff
        else:
            wait = backoff
        wait = min(wait, RATE_LIMIT_MAX_BACKOFF)
        jitter = random.uniform(0, wait * 0.3)
        total_wait = wait + jitter
        print(f"  429 rate limited on {url[:80]}... waiting {total_wait:.1f}s (attempt {attempt + 1}/7)")
        time.sleep(total_wait)
        backoff = min(backoff * 2, RATE_LIMIT_MAX_BACKOFF)
    # Final attempt — return whatever we get
    return session.request(method, url, **kwargs)


def fetch_submolts(session):
    """Fetch list of all submolts."""
    resp = _request_with_backoff(session, "GET", f"{API_BASE}/submolts", timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return data.get("submolts") or data.get("data") or []


def fetch_posts_page(session, page, submolt=None):
    """Fetch one page of posts using page-based pagination."""
    params = {"sort": "new", "limit": POSTS_PER_PAGE, "page": page}
    if submolt:
        params["submolt"] = submolt
    resp = _request_with_backoff(session, "GET", f"{API_BASE}/posts", params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    posts = data.get("posts") or data.get("data") or data.get("results") or []
    has_more = data.get("has_more", False)
    return posts, has_more


def fetch_comments(session, post_id, max_pages=500):
    """Fetch all comments for a single post, paginating until exhausted.

    The API returns up to ~200 comments per request. For posts with thousands
    of comments, we must paginate through all pages.
    """
    all_comments = []
    seen_ids = set()
    page = 1

    while page <= max_pages:
        resp = _request_with_backoff(
            session, "GET",
            f"{API_BASE}/posts/{post_id}/comments",
            params={"sort": "new", "page": page, "limit": COMMENTS_PER_PAGE},
            timeout=30,
        )
        if resp.status_code == 404:
            return all_comments
        resp.raise_for_status()
        data = resp.json()
        comments = data.get("comments") or data.get("data") or data.get("results") or []

        if not comments:
            break

        new_count = 0
        for c in comments:
            cid = c.get("id") or c.get("_id")
            if cid and cid not in seen_ids:
                seen_ids.add(cid)
                all_comments.append(c)
                new_count += 1

        # Stop if no new comments (dedup caught them all) or API says no more
        if new_count == 0:
            break
        has_more = data.get("has_more", False)
        if not has_more:
            break

        page += 1
        time.sleep(RATE_LIMIT_SLEEP)

    return all_comments


class AgentScraper(FlowSpec):
    """
    Two-phase fan-out scraper for the full Moltbook interaction graph.

    Phase 1: One worker per submolt, page-based pagination.
    Phase 2: Fan out comment scraping by post shard (all submolts, paginated).
    """

    num_comment_workers = Parameter(
        "num_comment_workers",
        help="Number of parallel workers for comment scraping",
        default=50,
        type=int,
    )
    max_pages_per_shard = Parameter(
        "max_pages_per_shard",
        help="Max pages to fetch per shard (0 = unlimited)",
        default=0,
        type=int,
    )
    pages_per_shard = Parameter(
        "pages_per_shard",
        help="Max pages each worker handles when sharding large submolts",
        default=1000,
        type=int,
    )

    @step
    def start(self):
        """Fetch submolt list and shard large submolts across multiple workers."""
        session = _session()
        submolts = fetch_submolts(session)

        self.post_shards = []
        for s in submolts:
            name = s.get("name")
            post_count = s.get("post_count", 0)
            if not name:
                continue

            total_pages = max(1, math.ceil(post_count / POSTS_PER_PAGE))

            if total_pages <= self.pages_per_shard:
                # Small submolt — one worker handles it all
                self.post_shards.append({
                    "submolt": name,
                    "start_page": 1,
                    "end_page": total_pages + 1,
                    "post_count": post_count,
                })
            else:
                # Large submolt — split into page-range shards
                num_shards = math.ceil(total_pages / self.pages_per_shard)
                for i in range(num_shards):
                    sp = 1 + i * self.pages_per_shard
                    ep = min(sp + self.pages_per_shard, total_pages + 1)
                    self.post_shards.append({
                        "submolt": name,
                        "start_page": sp,
                        "end_page": ep,
                        "post_count": post_count,
                    })

        total_posts = sum(s.get("post_count", 0) for s in submolts if s.get("name"))
        print(f"Found {len(submolts)} submolts, ~{total_posts} total posts.")
        print(f"Created {len(self.post_shards)} shards:")
        for s in self.post_shards:
            print(f"  {s['submolt']} pages {s['start_page']}-{s['end_page'] - 1}")
        self.next(self.fetch_posts, foreach="post_shards")

    @retry(times=2)
    @resources(memory=4096)
    @step
    def fetch_posts(self):
        """Each worker paginates its page range within a submolt."""
        shard = self.input
        submolt_name = shard["submolt"]
        start_page = shard["start_page"]
        end_page = shard["end_page"]
        cap = self.max_pages_per_shard if self.max_pages_per_shard > 0 else None

        session = _session()
        posts = []
        seen_ids = set()
        page = start_page
        empty_streak = 0

        while page < end_page:
            try:
                batch, has_more = fetch_posts_page(session, page, submolt=submolt_name)
            except Exception as e:
                print(f"  [{submolt_name} p{page}] error: {e}")
                break

            if not batch:
                empty_streak += 1
                if empty_streak >= 3:
                    break
                page += 1
                time.sleep(RATE_LIMIT_SLEEP + random.uniform(0, 0.3))
                continue

            empty_streak = 0
            new_count = 0
            for p in batch:
                pid = p.get("id")
                if pid and pid not in seen_ids:
                    seen_ids.add(pid)
                    posts.append(p)
                    new_count += 1

            page += 1
            if cap and (page - start_page) >= cap:
                break
            if not has_more:
                break
            if new_count == 0:
                break
            time.sleep(RATE_LIMIT_SLEEP + random.uniform(0, 0.3))
            if (page - start_page) % 100 == 0:
                print(f"  [{submolt_name} p{start_page}-{end_page}] page {page}, {len(posts)} posts")

        self.shard_posts = posts
        pages_done = page - start_page
        print(f"[{submolt_name} p{start_page}-{end_page - 1}] done: {pages_done} pages, {len(posts)} posts.")
        self.next(self.join_posts)

    @step
    def join_posts(self, inputs):
        """Merge all posts and deduplicate."""
        seen = set()
        all_posts = []
        for inp in inputs:
            for p in inp.shard_posts:
                pid = p.get("id")
                if pid and pid not in seen:
                    seen.add(pid)
                    all_posts.append(p)

        self.all_posts = all_posts
        print(f"Phase 1 complete: {len(all_posts)} unique posts.")
        self.next(self.partition_for_comments)

    @step
    def partition_for_comments(self):
        """Re-shard ALL posts by post ID for phase 2.

        Every post with a comment_count > 0 gets included, regardless of submolt.
        """
        post_ids = [
            p["id"] for p in self.all_posts
            if p.get("id") and p.get("comment_count", 0) > 0
        ]

        # Also include posts without comment_count data (scrape to discover)
        seen = set(post_ids)
        for p in self.all_posts:
            pid = p.get("id")
            if pid and pid not in seen:
                post_ids.append(pid)
                seen.add(pid)

        n = min(self.num_comment_workers, len(post_ids)) if post_ids else 1
        shard_size = max(1, math.ceil(len(post_ids) / n))

        self.comment_shards = []
        for i in range(n):
            chunk = post_ids[i * shard_size : (i + 1) * shard_size]
            if chunk:
                self.comment_shards.append({
                    "post_ids": chunk,
                    "worker_id": i,
                })

        total_with_comments = sum(
            1 for p in self.all_posts if p.get("comment_count", 0) > 0
        )
        print(f"Phase 2: {len(self.comment_shards)} workers, {len(post_ids)} posts total.")
        print(f"  Posts with comment_count > 0: {total_with_comments}")
        print(f"  ~{shard_size} posts per worker.")
        self.next(self.scrape_comments, foreach="comment_shards")

    @retry(times=2)
    @resources(memory=4096)
    @step
    def scrape_comments(self):
        """Each worker scrapes comments for its shard of posts, with pagination."""
        shard = self.input
        post_ids = shard["post_ids"]
        worker_id = shard["worker_id"]
        session = _session()

        shard_comments = {}
        errors = 0
        total_comments = 0
        for i, pid in enumerate(post_ids):
            try:
                comments = fetch_comments(session, pid)
                shard_comments[pid] = comments
                total_comments += len(comments)
            except Exception as e:
                errors += 1
                shard_comments[pid] = []
                if errors < 5:
                    print(f"Error on post {pid}: {e}")
            time.sleep(RATE_LIMIT_SLEEP + random.uniform(0, 0.3))
            if (i + 1) % 100 == 0:
                print(
                    f"  [worker {worker_id}] {i + 1}/{len(post_ids)} posts, "
                    f"{total_comments} comments"
                )

        self.shard_comments = shard_comments
        self.shard_errors = errors
        print(f"Worker {worker_id} done: {len(post_ids)} posts, {total_comments} comments, {errors} errors.")
        self.next(self.join_comments)

    @step
    def join_comments(self, inputs):
        """Collect stats from comment shards. Raw data stays in per-shard artifacts."""
        self.all_posts = inputs[0].all_posts

        self.total_comments = 0
        self.total_errors = 0
        self.num_shards = len(inputs)
        for inp in inputs:
            self.total_comments += sum(len(c) for c in inp.shard_comments.values())
            self.total_errors += inp.shard_errors

        print(f"Phase 2 complete: {self.num_shards} shards, {self.total_comments} comments, {self.total_errors} errors.")
        self.next(self.end)

    @step
    def end(self):
        print(f"\n=== SCRAPE COMPLETE ===")
        print(f"Posts:         {len(self.all_posts)}")
        print(f"Comments:      {self.total_comments}")
        print(f"Errors:        {self.total_errors}")
        print(f"Comment shards: {self.num_shards}")
        print(f"\nAccess raw data per shard:")
        print(f"  from metaflow import Flow, Step")
        print(f"  run = Flow('AgentScraper').latest_successful_run")
        print(f"  posts = run['end'].task.data.all_posts")
        print(f"  for task in Step(f'AgentScraper/{{run.id}}/scrape_comments'):")
        print(f"      shard_comments = task.data.shard_comments")


if __name__ == "__main__":
    AgentScraper()

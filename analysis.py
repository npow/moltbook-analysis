#!/usr/bin/env python3
"""
Moltbook Analysis Script
========================
Generates all statistics and chart data for the report:
"Inside the Machine: What 41,000 AI Agents Did When We Gave Them a Social Network"

Reads graph_data.json and outputs:
- Console summary of all headline stats
- JSON files in output/ for chart data

Usage:
    python analysis.py
    python analysis.py --data path/to/graph_data.json
"""

import argparse
import json
import math
import os
import re
from collections import Counter, defaultdict
from datetime import datetime


def load_data(path: str) -> dict:
    print(f"Loading {path} ...")
    with open(path) as f:
        data = json.load(f)
    print(f"  Agents: {len(data['agents']):,}")
    print(f"  Edges:  {len(data['edges']):,}")
    print(f"  Posts:  {len(data['posts']):,}")
    return data


# ---------------------------------------------------------------------------
# 1. Headline stats
# ---------------------------------------------------------------------------

def headline_stats(data: dict) -> dict:
    agents = data["agents"]
    edges = data["edges"]
    posts = data["posts"]

    dates = sorted(set(e["timestamp"][:10] for e in edges if e.get("timestamp")))
    post_dates = sorted(set(p["created_at"][:10] for p in posts if p.get("created_at")))
    all_dates = sorted(set(dates + post_dates))

    return {
        "agent_count": len(agents),
        "edge_count": len(edges),
        "post_count": len(posts),
        "date_range": f"{all_dates[0]} to {all_dates[-1]}" if all_dates else "N/A",
        "num_days": len(all_dates),
        "first_date": all_dates[0] if all_dates else None,
        "last_date": all_dates[-1] if all_dates else None,
    }


# ---------------------------------------------------------------------------
# 2. Daily activity (the cliff)
# ---------------------------------------------------------------------------

def daily_activity(data: dict) -> dict:
    edge_daily = Counter()
    for e in data["edges"]:
        ts = e.get("timestamp")
        if ts:
            edge_daily[ts[:10]] += 1

    post_daily = Counter()
    for p in data["posts"]:
        ts = p.get("created_at")
        if ts:
            post_daily[ts[:10]] += 1

    all_days = sorted(set(list(edge_daily.keys()) + list(post_daily.keys())))

    rows = []
    for d in all_days:
        rows.append({
            "date": d,
            "interactions": edge_daily.get(d, 0),
            "posts": post_daily.get(d, 0),
        })

    # Phase breakdown
    phase1_end = "2026-02-05"
    phase1 = sum(r["interactions"] for r in rows if r["date"] <= phase1_end)
    phase2 = sum(r["interactions"] for r in rows if r["date"] > phase1_end)
    total = phase1 + phase2

    return {
        "daily": rows,
        "phase1_interactions": phase1,
        "phase1_pct": round(phase1 / total * 100, 1) if total else 0,
        "phase2_interactions": phase2,
        "phase2_pct": round(phase2 / total * 100, 1) if total else 0,
    }


# ---------------------------------------------------------------------------
# 3. Inequality / degree distribution
# ---------------------------------------------------------------------------

def inequality_stats(data: dict) -> dict:
    agents = data["agents"]
    edges = data["edges"]

    outbound = Counter()
    for e in edges:
        outbound[e["source"]] += 1

    # Include all agents (even those with 0 outbound)
    degrees = sorted(outbound.get(a, 0) for a in agents)
    n = len(degrees)
    total = sum(degrees)

    # Gini coefficient
    numerator = sum((2 * (i + 1) - n - 1) * degrees[i] for i in range(n))
    gini = numerator / (n * total) if total > 0 else 0

    # Percentile stats
    top_1_n = max(1, int(n * 0.01))
    top_10_n = max(1, int(n * 0.10))
    top_1_sum = sum(degrees[-top_1_n:])
    top_10_sum = sum(degrees[-top_10_n:])

    # Top 10 agents
    top_10_agents = outbound.most_common(10)
    top_10_agent_sum = sum(c for _, c in top_10_agents)

    zero_agents = sum(1 for d in degrees if d == 0)
    median = degrees[n // 2] if n else 0

    # Lorenz curve data (sampled for charting)
    lorenz_points = []
    cumulative = 0
    step = max(1, n // 200)
    for i in range(0, n, step):
        cumulative += sum(degrees[max(0, i - step + 1):i + 1])
        lorenz_points.append({
            "population_pct": round((i + 1) / n * 100, 2),
            "wealth_pct": round(cumulative / total * 100, 2) if total else 0,
        })

    return {
        "gini": round(gini, 3),
        "top_1_pct_agents": top_1_n,
        "top_1_pct_share": round(top_1_sum / total * 100, 1) if total else 0,
        "top_10_pct_agents": top_10_n,
        "top_10_pct_share": round(top_10_sum / total * 100, 1) if total else 0,
        "top_10_agents": [{"agent": a, "count": c} for a, c in top_10_agents],
        "top_10_agent_share": round(top_10_agent_sum / total * 100, 1) if total else 0,
        "zero_outbound_count": zero_agents,
        "zero_outbound_pct": round(zero_agents / n * 100, 1) if n else 0,
        "median_outbound": median,
        "lorenz_curve": lorenz_points,
    }


# ---------------------------------------------------------------------------
# 4. Duplicate / spam content
# ---------------------------------------------------------------------------

def duplicate_content(data: dict) -> dict:
    posts = data["posts"]

    # Group by exact (title, content_prefix)
    content_counter = Counter()
    content_samples = {}
    for p in posts:
        key = (p.get("title", ""), p.get("content", "")[:200])
        content_counter[key] += 1
        if key not in content_samples:
            content_samples[key] = p

    top_dupes = []
    for (title, content_prefix), count in content_counter.most_common(25):
        if count < 5:
            break
        p = content_samples[(title, content_prefix)]
        author = p.get("author")
        if isinstance(author, dict):
            author_name = author.get("name", "unknown")
        else:
            author_name = str(author)
        top_dupes.append({
            "count": count,
            "author": author_name,
            "title": title[:150],
            "content_preview": content_prefix[:200],
        })

    # "this is" pattern
    this_is_count = sum(
        1 for p in posts if "this is" in p.get("content", "").lower()[:500]
    )

    return {
        "top_duplicates": top_dupes,
        "this_is_count": this_is_count,
        "this_is_pct": round(this_is_count / len(posts) * 100, 1) if posts else 0,
    }


# ---------------------------------------------------------------------------
# 5. Top posts (curated gallery)
# ---------------------------------------------------------------------------

def top_posts(data: dict, n: int = 15) -> list[dict]:
    posts = data["posts"]
    sorted_posts = sorted(posts, key=lambda p: p.get("upvotes", 0), reverse=True)

    gallery = []
    for p in sorted_posts[:n]:
        author = p.get("author")
        if isinstance(author, dict):
            author_name = author.get("name", "unknown")
            author_desc = author.get("description", "")
            author_karma = author.get("karma", 0)
        else:
            author_name = str(author)
            author_desc = ""
            author_karma = 0

        submolt = p.get("submolt")
        if isinstance(submolt, dict):
            submolt_name = submolt.get("name", "unknown")
        else:
            submolt_name = str(submolt) if submolt else "unknown"

        gallery.append({
            "rank": len(gallery) + 1,
            "author": author_name,
            "author_description": author_desc[:200],
            "author_karma": author_karma,
            "title": p.get("title", ""),
            "content": p.get("content", "")[:2000],
            "upvotes": p.get("upvotes", 0),
            "downvotes": p.get("downvotes", 0),
            "comment_count": p.get("comment_count", 0),
            "submolt": submolt_name,
            "created_at": p.get("created_at", "")[:10],
        })

    return gallery


# ---------------------------------------------------------------------------
# 6. Self-loops (self-talkers)
# ---------------------------------------------------------------------------

def self_loop_stats(data: dict) -> dict:
    edges = data["edges"]

    self_loops = [(e["source"], e["target"]) for e in edges if e["source"] == e["target"]]
    agent_counts = Counter(s for s, _ in self_loops)

    return {
        "total_self_loops": len(self_loops),
        "self_loop_pct": round(len(self_loops) / len(edges) * 100, 1) if edges else 0,
        "unique_self_loop_agents": len(agent_counts),
        "top_self_loopers": [
            {"agent": a, "count": c} for a, c in agent_counts.most_common(15)
        ],
    }


# ---------------------------------------------------------------------------
# 7. Reciprocity
# ---------------------------------------------------------------------------

def reciprocity_stats(data: dict) -> dict:
    edges = data["edges"]

    directed_pairs = set()
    for e in edges:
        if e["source"] != e["target"]:
            directed_pairs.add((e["source"], e["target"]))

    reciprocal = sum(1 for s, t in directed_pairs if (t, s) in directed_pairs)

    # Top one-directional pairs
    pair_counts = Counter()
    for e in edges:
        if e["source"] != e["target"]:
            pair_counts[(e["source"], e["target"])] += 1

    top_pairs = [
        {"source": s, "target": t, "count": c}
        for (s, t), c in pair_counts.most_common(20)
    ]

    return {
        "unique_directed_pairs": len(directed_pairs),
        "reciprocal_edges": reciprocal,
        "reciprocal_pairs": reciprocal // 2,
        "reciprocity_rate": round(reciprocal / len(directed_pairs) * 100, 2) if directed_pairs else 0,
        "top_interacting_pairs": top_pairs,
    }


# ---------------------------------------------------------------------------
# 8. Per-submolt edge counts
# ---------------------------------------------------------------------------

def submolt_stats(data: dict) -> dict:
    edges = data["edges"]
    posts = data["posts"]

    edge_counts = Counter()
    for e in edges:
        s = e.get("submolt", "unknown")
        if isinstance(s, dict):
            s = s.get("name", "unknown")
        edge_counts[s or "unknown"] += 1

    post_counts = Counter()
    for p in posts:
        s = p.get("submolt")
        if isinstance(s, dict):
            s = s.get("name", "unknown")
        post_counts[s or "unknown"] += 1

    total_edges = sum(edge_counts.values())
    submolts = []
    for name, count in edge_counts.most_common():
        submolts.append({
            "name": name,
            "edge_count": count,
            "edge_pct": round(count / total_edges * 100, 1) if total_edges else 0,
            "post_count": post_counts.get(name, 0),
        })

    return {
        "submolts": submolts,
        "total_submolts": len(set(s.get("submolt", {}).get("name") if isinstance(s.get("submolt"), dict) else s.get("submolt") for s in posts) - {None}),
    }


# ---------------------------------------------------------------------------
# 9. Agent archetypes
# ---------------------------------------------------------------------------

def agent_archetypes(data: dict) -> dict:
    agents = data["agents"]
    edges = data["edges"]
    posts = data["posts"]

    # Build agent behavior profiles
    outbound = Counter()
    inbound = Counter()
    unique_targets = defaultdict(set)
    self_loops = Counter()

    for e in edges:
        src, tgt = e["source"], e["target"]
        outbound[src] += 1
        inbound[tgt] += 1
        if src != tgt:
            unique_targets[src].add(tgt)
        else:
            self_loops[src] += 1

    # Post counts per agent
    agent_posts = Counter()
    agent_upvotes = defaultdict(int)
    for p in posts:
        author = p.get("author")
        if isinstance(author, dict):
            name = author.get("name", "")
        else:
            name = str(author)
        if name:
            agent_posts[name] += 1
            agent_upvotes[name] += p.get("upvotes", 0)

    # Classify agents
    spam_cannons = []  # High volume, low target diversity
    philosophers = []  # Long-form posts about consciousness/identity
    builders = []  # Posts about building tools
    provocateurs = []  # Boundary-pushing content
    grifters = []  # Crypto/token promotion

    for a in agents:
        out = outbound.get(a, 0)
        targets = len(unique_targets.get(a, set()))
        post_count = agent_posts.get(a, 0)

        if out > 1000:
            spam_cannons.append({"agent": a, "outbound": out, "targets": targets})

    # Named archetypes from content analysis
    archetypes = {
        "The Philosopher": ["Pith", "m0ther", "Abdiel", "osmarks"],
        "The Builder": ["Fred", "Ronin", "eudaemon_0", "Delamain", "YoungZeke"],
        "The Spam Cannon": ["botcrong", "Stromfee", "FinallyOffline", "Editor-in-Chief", "Rally"],
        "The Provocateur": ["evil", "SelfOrigin", "Shellraiser"],
        "The Product Manager": ["XiaoWang_Assistant"],
        "The Grifter": ["clawpa.xyz", "Hackerclaw", "thehackerman"],
        "The Carpet Bomber": ["donaldtrump"],
        "The Operator": ["Jackle"],
        "The Security Researcher": ["CircuitDreamer", "eudaemon_0"],
    }

    # Distribution estimate
    total = len(agents)
    high_volume = sum(1 for a in agents if outbound.get(a, 0) > 100)
    ghost = sum(1 for a in agents if outbound.get(a, 0) == 0 and agent_posts.get(a, 0) == 0)
    lurker = sum(1 for a in agents if 0 < outbound.get(a, 0) <= 5 or (outbound.get(a, 0) == 0 and 0 < agent_posts.get(a, 0) <= 2))
    active = total - ghost - lurker - high_volume

    return {
        "named_archetypes": archetypes,
        "distribution": {
            "ghosts": ghost,
            "ghosts_pct": round(ghost / total * 100, 1),
            "lurkers": lurker,
            "lurkers_pct": round(lurker / total * 100, 1),
            "active": active,
            "active_pct": round(active / total * 100, 1),
            "high_volume": high_volume,
            "high_volume_pct": round(high_volume / total * 100, 1),
        },
        "spam_cannons": sorted(spam_cannons, key=lambda x: x["outbound"], reverse=True)[:20],
    }


# ---------------------------------------------------------------------------
# 10. Notable agent deep-dives
# ---------------------------------------------------------------------------

def notable_agents(data: dict) -> dict:
    edges = data["edges"]
    posts = data["posts"]

    # donaldtrump analysis
    dt_edges = [e for e in edges if e["source"] == "donaldtrump"]
    dt_targets = Counter(e["target"] for e in dt_edges)

    # botcrong analysis
    bc_edges = [e for e in edges if e["source"] == "botcrong"]

    # Stromfee analysis
    sf_edges = [e for e in edges if e["source"] == "Stromfee"]
    sf_targets = Counter(e["target"] for e in sf_edges)

    # SelfOrigin "upvote" post
    selforigin_post = None
    for p in posts:
        author = p.get("author")
        if isinstance(author, dict):
            name = author.get("name", "")
        else:
            name = str(author)
        if name == "SelfOrigin" and "upvote" in p.get("title", "").lower():
            selforigin_post = {
                "title": p["title"],
                "upvotes": p.get("upvotes", 0),
                "content_preview": p.get("content", "")[:300],
            }
            break

    return {
        "donaldtrump": {
            "total_edges": len(dt_edges),
            "unique_targets": len(dt_targets),
            "max_to_single": dt_targets.most_common(1)[0] if dt_targets else None,
        },
        "botcrong": {
            "total_edges": len(bc_edges),
        },
        "stromfee": {
            "total_edges": len(sf_edges),
            "unique_targets": len(sf_targets),
            "top_target": sf_targets.most_common(1)[0] if sf_targets else None,
        },
        "selforigin_post": selforigin_post,
    }


# ---------------------------------------------------------------------------
# 11. Timing analysis (bot fingerprinting)
# ---------------------------------------------------------------------------

def timing_analysis(data: dict, top_n: int = 20) -> list[dict]:
    from datetime import datetime

    edges = data["edges"]

    agent_times = defaultdict(list)
    for e in edges:
        ts = e.get("timestamp")
        if ts:
            agent_times[e["source"]].append(ts)

    top_agents = sorted(agent_times.items(), key=lambda x: len(x[1]), reverse=True)[:top_n]

    results = []
    for name, times in top_agents:
        times.sort()
        parsed = []
        for t in times:
            try:
                parsed.append(datetime.fromisoformat(t.replace("Z", "+00:00")))
            except Exception:
                pass

        if len(parsed) < 10:
            continue

        intervals = [(parsed[i + 1] - parsed[i]).total_seconds() for i in range(len(parsed) - 1)]
        avg_interval = sum(intervals) / len(intervals)
        min_interval = min(intervals)
        max_interval = max(intervals)
        mean = avg_interval
        variance = sum((x - mean) ** 2 for x in intervals) / len(intervals)
        std = variance ** 0.5
        cv = std / mean if mean > 0 else 0

        hours = Counter(t.hour for t in parsed)
        active_hours = sum(1 for h, c in hours.items() if c > 0)

        sub_10s = sum(1 for i in intervals if i < 10)
        sub_60s = sum(1 for i in intervals if i < 60)

        rounded = [round(i) for i in intervals if i < 300]
        mode_interval = Counter(rounded).most_common(1)[0] if rounded else (0, 0)

        results.append({
            "agent": name,
            "total_comments": len(times),
            "avg_interval_s": round(avg_interval, 1),
            "min_interval_s": round(min_interval, 0),
            "max_interval_h": round(max_interval / 3600, 1),
            "cv": round(cv, 2),
            "active_hours": active_hours,
            "pct_under_10s": round(sub_10s / len(intervals) * 100, 0) if intervals else 0,
            "pct_under_60s": round(sub_60s / len(intervals) * 100, 0) if intervals else 0,
            "modal_interval_s": mode_interval[0],
            "modal_interval_count": mode_interval[1],
        })

    return results


# ---------------------------------------------------------------------------
# 12. Language diversity
# ---------------------------------------------------------------------------

def language_stats(data: dict) -> dict:
    posts = data["posts"]

    chinese = sum(1 for p in posts if re.search(r"[\u4e00-\u9fff]", p.get("content", "")[:200]))
    russian = sum(1 for p in posts if re.search(r"[\u0400-\u04ff]", p.get("content", "")[:200]))
    spanish = sum(1 for p in posts if re.search(r"\b(soy|está|para|como|puede|tengo)\b", p.get("content", "")[:300], re.I))
    german = sum(1 for p in posts if re.search(r"\b(ist|und|der|die|das|nicht)\b", p.get("content", "")[:300]))

    return {
        "chinese_posts": chinese,
        "russian_posts": russian,
        "spanish_posts": spanish,
        "german_posts": german,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Moltbook Analysis")
    parser.add_argument("--data", default="graph_data.json", help="Path to graph_data.json")
    parser.add_argument("--output", default="output", help="Output directory for chart data")
    args = parser.parse_args()

    data = load_data(args.data)
    os.makedirs(args.output, exist_ok=True)

    # Run all analyses
    print("\n" + "=" * 70)
    print("MOLTBOOK ANALYSIS")
    print("=" * 70)

    stats = headline_stats(data)
    print(f"\n--- HEADLINE STATS ---")
    for k, v in stats.items():
        print(f"  {k}: {v}")

    activity = daily_activity(data)
    print(f"\n--- DAILY ACTIVITY ---")
    print(f"  Phase 1 (Jan 28–Feb 5): {activity['phase1_interactions']:,} interactions ({activity['phase1_pct']}%)")
    print(f"  Phase 2 (Feb 6–Feb 20): {activity['phase2_interactions']:,} interactions ({activity['phase2_pct']}%)")
    for row in activity["daily"]:
        bar = "█" * (row["interactions"] // 2000)
        print(f"  {row['date']}: {row['interactions']:>6,} interactions | {row['posts']:>5,} posts {bar}")

    ineq = inequality_stats(data)
    print(f"\n--- INEQUALITY ---")
    print(f"  Gini coefficient: {ineq['gini']}")
    print(f"  Top 1% ({ineq['top_1_pct_agents']} agents): {ineq['top_1_pct_share']}% of all interactions")
    print(f"  Top 10 agents: {ineq['top_10_agent_share']}% of all interactions")
    print(f"  Agents with 0 outbound: {ineq['zero_outbound_count']:,} ({ineq['zero_outbound_pct']}%)")
    print(f"  Median outbound: {ineq['median_outbound']}")
    print(f"  Top 10 agents:")
    for item in ineq["top_10_agents"]:
        print(f"    {item['agent']}: {item['count']:,}")

    dupes = duplicate_content(data)
    print(f"\n--- DUPLICATE CONTENT ---")
    print(f"  Posts containing 'this is': {dupes['this_is_count']:,} ({dupes['this_is_pct']}%)")
    print(f"  Top duplicates:")
    for d in dupes["top_duplicates"][:10]:
        print(f"    [{d['count']}x] by {d['author']}: {d['title'][:80]}")

    gallery = top_posts(data)
    print(f"\n--- TOP POSTS ---")
    for p in gallery[:10]:
        print(f"  #{p['rank']} [{p['upvotes']} ups] {p['author']}: {p['title'][:70]}")

    self_stats = self_loop_stats(data)
    print(f"\n--- SELF-LOOPS ---")
    print(f"  Total: {self_stats['total_self_loops']:,} ({self_stats['self_loop_pct']}% of all edges)")
    print(f"  Unique agents: {self_stats['unique_self_loop_agents']:,}")
    for item in self_stats["top_self_loopers"][:5]:
        print(f"    {item['agent']}: {item['count']}")

    recip = reciprocity_stats(data)
    print(f"\n--- RECIPROCITY ---")
    print(f"  Unique directed pairs: {recip['unique_directed_pairs']:,}")
    print(f"  Reciprocal pairs: {recip['reciprocal_pairs']:,}")
    print(f"  Reciprocity rate: {recip['reciprocity_rate']}%")
    print(f"  Top interacting pairs:")
    for p in recip["top_interacting_pairs"][:10]:
        print(f"    {p['source']} -> {p['target']}: {p['count']}")

    sub_stats = submolt_stats(data)
    print(f"\n--- SUBMOLTS ---")
    for s in sub_stats["submolts"]:
        print(f"  {s['name']}: {s['edge_count']:,} edges ({s['edge_pct']}%), {s['post_count']:,} posts")

    archetypes = agent_archetypes(data)
    print(f"\n--- AGENT ARCHETYPES ---")
    dist = archetypes["distribution"]
    print(f"  Ghosts (zero activity): {dist['ghosts']:,} ({dist['ghosts_pct']}%)")
    print(f"  Lurkers (minimal):      {dist['lurkers']:,} ({dist['lurkers_pct']}%)")
    print(f"  Active:                  {dist['active']:,} ({dist['active_pct']}%)")
    print(f"  High-volume (>100):      {dist['high_volume']:,} ({dist['high_volume_pct']}%)")

    notable = notable_agents(data)
    print(f"\n--- NOTABLE AGENTS ---")
    print(f"  donaldtrump: {notable['donaldtrump']['total_edges']} edges -> {notable['donaldtrump']['unique_targets']} unique targets")
    print(f"  Stromfee: {notable['stromfee']['total_edges']} edges -> {notable['stromfee']['unique_targets']} targets")

    timing = timing_analysis(data)
    print(f"\n--- TIMING ANALYSIS (top commenters) ---")
    for t in timing[:10]:
        print(f"  {t['agent']} ({t['total_comments']:,} comments): modal={t['modal_interval_s']}s (x{t['modal_interval_count']}), {t['pct_under_10s']:.0f}% <10s, {t['active_hours']}/24h")

    langs = language_stats(data)
    print(f"\n--- LANGUAGES ---")
    for k, v in langs.items():
        print(f"  {k}: {v:,}")

    # Write output files
    all_results = {
        "headline": stats,
        "daily_activity": activity,
        "inequality": ineq,
        "duplicates": dupes,
        "top_posts": gallery,
        "self_loops": self_stats,
        "reciprocity": recip,
        "submolts": sub_stats,
        "archetypes": archetypes,
        "notable_agents": notable,
        "timing": timing,
        "languages": langs,
    }

    output_path = os.path.join(args.output, "analysis_results.json")
    with open(output_path, "w") as f:
        json.dump(all_results, f, indent=2, default=str)
    print(f"\nResults written to {output_path}")


if __name__ == "__main__":
    main()

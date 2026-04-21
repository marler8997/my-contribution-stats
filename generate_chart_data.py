#!/usr/bin/env python3
"""Generates chart_data.json from day files. Errors if any SHA appears twice."""

import os, json, sys
from datetime import date, timedelta

days_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "days")
out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "chart_data.json")

start = date(2024, 4, 1)
end = date(2026, 4, 21)

all_data = {}
all_repos = set()
all_commits = {}
seen_shas = {}  # sha -> date

d = start
while d <= end:
    ds = d.isoformat()
    f = os.path.join(days_dir, f"{ds}.tsv")
    day_data = {}
    day_commits = []
    if os.path.exists(f):
        with open(f) as fh:
            for line in fh:
                parts = line.strip().split('\t')
                if len(parts) >= 4:
                    repo, sha, adds, dels = parts[0], parts[1], int(parts[2]), int(parts[3])
                    if sha in seen_shas:
                        print(f"ERROR: duplicate SHA {sha} on {ds} (first seen {seen_shas[sha]})", file=sys.stderr)
                        sys.exit(1)
                    seen_shas[sha] = ds
                    if repo not in day_data:
                        day_data[repo] = {"adds": 0, "dels": 0}
                    day_data[repo]["adds"] += adds
                    day_data[repo]["dels"] += dels
                    all_repos.add(repo)
                    day_commits.append([sha, repo, adds, dels])
    all_data[ds] = day_data
    if day_commits:
        all_commits[ds] = day_commits
    d += timedelta(days=1)

repo_totals = {}
for ds, repos in all_data.items():
    for repo, stats in repos.items():
        repo_totals.setdefault(repo, 0)
        repo_totals[repo] += stats["adds"] + stats["dels"]

top_repos = sorted(repo_totals.keys(), key=lambda r: repo_totals[r], reverse=True)

output = {
    "dates": sorted(all_data.keys()),
    "repos": top_repos,
    "data": {},
    "commits": {}
}

for ds in output["dates"]:
    day = {}
    for repo, stats in all_data[ds].items():
        loc = stats["adds"] + stats["dels"]
        if loc > 0:
            day[repo] = loc
    if day:
        output["data"][ds] = day
    if ds in all_commits:
        output["commits"][ds] = all_commits[ds]

with open(out_path, 'w') as f:
    json.dump(output, f)

print(f"Generated {out_path} ({os.path.getsize(out_path)} bytes, {len(seen_shas)} unique commits)")

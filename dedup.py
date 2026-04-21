#!/usr/bin/env python3
"""Deduplicates commit data across day files.
Processes days earliest-to-latest; if a SHA appeared in an earlier day,
it gets removed from all later days."""

import os

days_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "days")

seen = set()
removed = 0

for fname in sorted(os.listdir(days_dir)):
    if not fname.endswith(".tsv"):
        continue
    path = os.path.join(days_dir, fname)
    with open(path) as f:
        lines = f.readlines()

    kept = []
    day_removed = 0
    for line in lines:
        parts = line.strip().split("\t")
        if len(parts) >= 2:
            sha = parts[1]
            if sha in seen:
                day_removed += 1
            else:
                seen.add(sha)
                kept.append(line)
        else:
            kept.append(line)

    if day_removed > 0:
        with open(path, "w") as f:
            f.writelines(kept)
        removed += day_removed
        print(f"{fname[:-4]}: removed {day_removed} duplicate(s)")

print(f"\nDone. Removed {removed} duplicate entries across {len(seen) + removed} total.")

#!/bin/bash
# Incrementally fetches commit data for marler8997 from GitHub, day by day.
# Each day gets its own file in data/days/YYYY-MM-DD.tsv
# Format per line: repo\tsha\tadditions\tdeletions
#
# Progress is implicit: if the file for a day exists, that day is done.
# Handles rate limits by stopping gracefully — just run again later.
#
# Usage:
#   ./fetch_commits.sh              # fetch missing days
#   ./fetch_commits.sh --stats      # print summary of collected data

set -euo pipefail

DATA_DIR="$(dirname "$0")/data"
DAYS_DIR="$DATA_DIR/days"
USER="marler8997"
START_DATE="2024-04-01"
END_DATE="2026-04-21"

mkdir -p "$DAYS_DIR"

if [[ "${1:-}" == "--stats" ]]; then
    total_files=$(ls "$DAYS_DIR"/*.tsv 2>/dev/null | wc -l)
    total_commits=$(cat "$DAYS_DIR"/*.tsv 2>/dev/null | wc -l)
    total_adds=$(awk -F'\t' '{s+=$3} END {print s+0}' "$DAYS_DIR"/*.tsv 2>/dev/null)
    total_dels=$(awk -F'\t' '{s+=$4} END {print s+0}' "$DAYS_DIR"/*.tsv 2>/dev/null)
    echo "Days fetched:   $total_files"
    echo "Total commits:  $total_commits"
    echo "Total lines added:   $total_adds"
    echo "Total lines deleted: $total_dels"
    echo ""
    echo "=== Per-repo totals ==="
    cat "$DAYS_DIR"/*.tsv 2>/dev/null | awk -F'\t' '{
        commits[$1]++; adds[$1]+=$3; dels[$1]+=$4
    } END {
        for (r in commits) printf "%6d commits  +%8d  -%8d  %s\n", commits[r], adds[r], dels[r], r
    }' | sort -rn
    exit 0
fi

check_rate_limit() {
    local remaining
    remaining=$(gh api rate_limit --jq '.resources.core.remaining' 2>/dev/null || echo "0")
    if [[ "$remaining" -lt 100 ]]; then
        local reset
        reset=$(gh api rate_limit --jq '.resources.core.reset' 2>/dev/null || echo "0")
        local now
        now=$(date +%s)
        local wait=$((reset - now))
        echo "Rate limit low (${remaining} remaining). Resets in ${wait}s."
        echo "Progress saved. Run again later."
        exit 0
    fi
    echo "  (API calls remaining: $remaining)"
}

# Generate list of all dates in range, newest first
current="$END_DATE"
dates=()
while [[ "$current" > "$START_DATE" ]] || [[ "$current" == "$START_DATE" ]]; do
    dates+=("$current")
    current=$(date -d "$current - 1 day" +%Y-%m-%d)
done

echo "Date range: $START_DATE to $END_DATE (${#dates[@]} days)"

# Find days we still need to fetch
missing=()
for d in "${dates[@]}"; do
    if [[ ! -f "$DAYS_DIR/$d.tsv" ]]; then
        missing+=("$d")
    fi
done

echo "Already fetched: $((${#dates[@]} - ${#missing[@]})) days"
echo "Remaining: ${#missing[@]} days"
echo ""

if [[ ${#missing[@]} -eq 0 ]]; then
    echo "All days fetched!"
    exit 0
fi

for day in "${missing[@]}"; do
    next_day=$(date -d "$day + 1 day" +%Y-%m-%d)
    echo "Fetching $day..."
    check_rate_limit

    tmpfile=$(mktemp)

    # Search for all commits by this user on this day
    # The search API returns commits across all repos
    page=1
    while true; do
        response=$(gh api "search/commits?q=author:${USER}+committer-date:${day}..${next_day}&per_page=100&page=${page}" \
            --jq '.items[] | "\(.sha) \(.repository.full_name)"' 2>/dev/null) || {
            echo "  Search API failed (rate limit?)"
            rm -f "$tmpfile"
            check_rate_limit
            break
        }

        if [[ -z "$response" ]]; then
            break
        fi

        while IFS=' ' read -r sha repo; do
            # Fetch individual commit stats
            stats=$(gh api "repos/$repo/commits/$sha" \
                --jq '"\(.stats.additions // 0)\t\(.stats.deletions // 0)"' 2>/dev/null) || {
                echo "  Failed to get stats for $sha in $repo"
                stats="0\t0"
            }
            echo -e "${repo}\t${sha}\t${stats}" >> "$tmpfile"
        done <<< "$response"

        count=$(echo "$response" | wc -l)
        if [[ "$count" -lt 100 ]]; then
            break
        fi
        page=$((page + 1))
    done

    # Only save the file if we successfully completed the day
    # (even if 0 commits — that's valid, means no public commits that day)
    if [[ -f "$tmpfile" ]]; then
        mv "$tmpfile" "$DAYS_DIR/$day.tsv"
        commits=$(wc -l < "$DAYS_DIR/$day.tsv")
        echo "  -> $day: $commits commits"
    fi
done

echo ""
echo "Done. Run with --stats for summary."

#!/usr/bin/env bash
# download-adsb.sh
# Usage: ./download-adsb.sh [date] [start_hour] [num_hours]
# Example: ./download-adsb.sh 2026/03/01 0 6      ← 6 hours from midnight
#          ./download-adsb.sh 2026/03/01 14 2     ← 2 hours from 14:00 UTC

set -euo pipefail

DATE="${1:-2026/03/01}"
START_HOUR="${2:-0}"
NUM_HOURS="${3:-6}"
PARALLEL="${4:-16}"

BASE_URL="https://samples.adsbexchange.com/readsb-hist/${DATE}"
OUT_DIR="./data/raw/$(echo "$DATE" | tr '/' '-')"

mkdir -p "$OUT_DIR"

# ── Generate all timestamps (one every 5 seconds) ──────────────────────────
TIMESTAMPS=()
END_HOUR=$(( START_HOUR + NUM_HOURS ))

for (( h=START_HOUR; h<END_HOUR; h++ )); do
  for (( m=0; m<60; m++ )); do
    for (( s=0; s<60; s+=5 )); do
      TIMESTAMPS+=( "$(printf '%02d%02d%02dZ' "$h" "$m" "$s")" )
    done
  done
done

TOTAL=${#TIMESTAMPS[@]}
echo "Downloading ${TOTAL} files (${NUM_HOURS}h from $(printf '%02d:00' $START_HOUR) UTC)"
echo "Date: ${DATE}  →  ${OUT_DIR}"
echo "Parallel connections: ${PARALLEL}"
echo ""

# ── Download ────────────────────────────────────────────────────────────────
FAILED=0
COUNT=0

download_file() {
  local ts="$1"
  local url="${BASE_URL}/${ts}.json.gz"
  local dest="${OUT_DIR}/${ts}.json.gz"

  if [[ -f "$dest" ]]; then
    echo "SKIP  ${ts}.json.gz  (already exists)"
    return 0
  fi

  if curl -sf --retry 3 --retry-delay 2 --max-time 30 \
       -o "$dest" "$url" 2>/dev/null; then
    echo "OK    ${ts}.json.gz"
  else
    echo "FAIL  ${ts}.json.gz  (${url})"
    rm -f "$dest"
    return 1
  fi
}

export -f download_file
export BASE_URL OUT_DIR

# Use GNU parallel if available, otherwise xargs
if command -v parallel &>/dev/null; then
  printf '%s\n' "${TIMESTAMPS[@]}" \
    | parallel -j "$PARALLEL" --bar download_file {}
else
  printf '%s\n' "${TIMESTAMPS[@]}" \
    | xargs -P "$PARALLEL" -I {} bash -c 'download_file "$@"' _ {}
fi

# ── Summary ─────────────────────────────────────────────────────────────────
DOWNLOADED=$(ls "$OUT_DIR"/*.json.gz 2>/dev/null | wc -l)
echo ""
echo "────────────────────────────────────"
echo "Done.  ${DOWNLOADED}/${TOTAL} files in ${OUT_DIR}"
echo "Disk usage: $(du -sh "$OUT_DIR" | cut -f1)"
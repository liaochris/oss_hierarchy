#!/usr/bin/env bash
# Usage: ./run_link_using_issue_loop.sh <proxy_num>
# proxy_num must be an integer 0..5

set -euo pipefail

proxy_num="${1:?usage: $0 <proxy_num (0..5)>}"

# validate integer 0..5
case "$proxy_num" in
  0|1|2|3|4|5) ;;
  *) echo "error: proxy_num must be 0..5" >&2; exit 2 ;;
esac

SCRIPT="source/scrape/link_issue_pull_request/code/link_using_issue.py"

while true; do
  echo "$(date '+%Y-%m-%d %H:%M:%S%z') starting: python $SCRIPT --proxy-num $proxy_num"
  python "$SCRIPT" --proxy-num "$proxy_num" &
  pid=$!

  # every 24 hours restart
  sleep 86400 || true

  echo "$(date '+%Y-%m-%d %H:%M:%S%z') restarting; killing PID $pid"
  kill -TERM "$pid" 2>/dev/null || true

  # wait up to ~30s for clean exit
  for _ in 1 2 3 4 5 6 7 8 9 10; do
    if ! kill -0 "$pid" 2>/dev/null; then
      break
    fi
    sleep 3
  done

  # force kill if still alive
  if kill -0 "$pid" 2>/dev/null; then
    echo "$(date '+%Y-%m-%d %H:%M:%S%z') PID $pid still alive; kill -KILL"
    kill -KILL "$pid" 2>/dev/null || true
  fi

  # reap
  wait "$pid" 2>/dev/null || true
done

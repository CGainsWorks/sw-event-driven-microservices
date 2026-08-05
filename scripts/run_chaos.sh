#!/usr/bin/env bash
# Run the failure injection suite and print the results table.
#
#   make chaos
#
# This is DESTRUCTIVE: it stops containers, severs networks and republishes messages. It
# restores everything afterwards, including when a scenario fails, but anything else using
# the stack will be disrupted while it runs.
#
# Results are written to tests/chaos/results/chaos-results.json. Every number the README
# quotes about failure behaviour comes from that file, which is why it is generated rather
# than typed.

set -euo pipefail

cd "$(dirname "$0")/.."

bold() { printf '\033[1m%s\033[0m\n' "$*"; }
fail() { printf '\033[31m%s\033[0m\n' "$*" >&2; }

command -v go >/dev/null 2>&1 || { fail "missing required tool: go"; exit 1; }
command -v docker >/dev/null 2>&1 || { fail "missing required tool: docker"; exit 1; }

bold "Ensuring the stack is up"
docker compose up -d --build

bold "Running failure injection"
echo
echo "  Scenarios: broker down, dependency down, duplicate delivery, network partition."
echo "  This takes several minutes: each one waits out a real outage."
echo

# A generous timeout. The partition and dependency-down scenarios deliberately hold an
# outage open for tens of seconds and then wait for the saga's own backoff to expire, so a
# short timeout would fail the test rather than the system.
set +e
go test -tags=chaos ./tests/chaos/... -count=1 -v -timeout 30m
STATUS=$?
set -e

RESULTS=tests/chaos/results/chaos-results.json
echo
if [ -f "$RESULTS" ]; then
    bold "Results  ($RESULTS)"
    if command -v jq >/dev/null 2>&1; then
        jq -r '.scenarios[] |
            "  \(if .passed then "PASS" else "FAIL" end)  \(.scenario)\n" +
            "        \(.property_asserted)\n" +
            "        measured: \(.measured // {} | to_entries | map("\(.key)=\(.value)") | join("  "))" +
            (if .timings then "\n        timings:  \(.timings | to_entries | map("\(.key)=\(.value)") | join("  "))" else "" end) +
            (if .failure then "\n        FAILURE:  \(.failure)" else "" end)'  "$RESULTS"
    else
        cat "$RESULTS"
    fi
else
    fail "no results file was written; the suite did not reach the end"
fi

echo
if [ "$STATUS" -eq 0 ]; then
    bold "All scenarios passed."
else
    fail "Some scenarios failed. The stack has been restored; see the output above."
fi

# Leave the stack healthy whatever happened, so the next thing to run against it is not
# fighting a container this suite stopped.
bold "Restoring the stack"
docker compose up -d >/dev/null
docker compose ps --format 'table {{.Service}}\t{{.Status}}'

exit "$STATUS"

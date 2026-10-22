#!/usr/bin/env bash
# Discovery demo: one node announces, another finds it and dials what it found.
#
#   alice  --listen tcp://... --listen udp://... --announce
#   bob    --discover --discover-connect        (no address typed by hand)
#
# The discovery channel is UDP broadcast; alice answers with *both* of her
# endpoints, so bob ends up with a TCP link and a UDP link from one query.
set -uo pipefail

cd "$(dirname "$0")/.."
BIN=./target/debug/retexis
PORT="${1:-47901}"
TCP_A=7300
UDP_A=7301

if [ ! -x "$BIN" ]; then
  echo "build first: cargo build" >&2
  exit 1
fi

cleanup() { kill "${ALICE_PID:-0}" 2>/dev/null; wait 2>/dev/null; }
trap cleanup EXIT

echo "════════════════════════════════════════════════════════════════"
echo " alice: listens on tcp + udp, answers discovery queries"
echo "════════════════════════════════════════════════════════════════"
# A pipe that stays open keeps the REPL alive without a terminal.
(sleep 25) | "$BIN" --name alice \
  --listen "tcp://127.0.0.1:$TCP_A" \
  --listen "udp://127.0.0.1:$UDP_A" \
  --announce --discovery-port "$PORT" >/tmp/retexis-alice.log 2>&1 &
ALICE_PID=$!
sleep 1.2
grep -E "listening|answering|media " /tmp/retexis-alice.log

echo
echo "════════════════════════════════════════════════════════════════"
echo " bob: --discover --discover-connect (no --connect, no address)"
echo "════════════════════════════════════════════════════════════════"
(sleep 3; echo "/peers"; sleep 1; echo "/quit") | "$BIN" --name bob \
  --discover --discover-ms 900 --discover-connect \
  --discovery-port "$PORT" --no-reconnect 2>&1 | grep -vE "^\s*· (route|> route)" | head -30

echo
echo "════════════════════════════════════════════════════════════════"
echo " alice's view"
echo "════════════════════════════════════════════════════════════════"
grep -E "link up|route \+" /tmp/retexis-alice.log | head -6

echo
echo "one broadcast query → bob learned both of alice's endpoints and dialled them."

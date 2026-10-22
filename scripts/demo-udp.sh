#!/usr/bin/env bash
# Same protocol, different medium: everything below is UDP.
#
# The point of the demo is what is *not* here: no UDP-specific code in routing,
# no special flags. `--listen udp://` and `--connect udp://` are the only
# differences from the TCP demo.
set -uo pipefail

cd "$(dirname "$0")/.."
BIN=./target/debug/retexis
PORT_A="${1:-7201}"

if [ ! -x "$BIN" ]; then
  echo "build first: cargo build" >&2
  exit 1
fi

cleanup() { kill "${ALICE_PID:-0}" 2>/dev/null; wait 2>/dev/null; }
trap cleanup EXIT

echo "════════════════════════════════════════════════════════════════"
echo " alice: udp://127.0.0.1:$PORT_A   (one datagram per envelope)"
echo "════════════════════════════════════════════════════════════════"
(sleep 12) | "$BIN" --name alice --listen "udp://127.0.0.1:$PORT_A" \
  >/tmp/retexis-udp-alice.log 2>&1 &
ALICE_PID=$!
sleep 1.0

echo
echo "════════════════════════════════════════════════════════════════"
echo " bob: dials over udp, broadcasts, then asks for its routes"
echo "════════════════════════════════════════════════════════════════"
(sleep 1.5; echo "hello over udp"; sleep 1.2; echo "/routes"; sleep 0.8; echo "/quit") \
  | "$BIN" --name bob --connect "udp://127.0.0.1:$PORT_A" --no-reconnect 2>&1 \
  | grep -E "dialling|link up|route|retexis 0|PEER|udp" | head -12

echo
echo "════════════════════════════════════════════════════════════════"
echo " alice's view"
echo "════════════════════════════════════════════════════════════════"
grep -E "listening|link up|hello over udp|route \+" /tmp/retexis-udp-alice.log | head -8

echo
echo "same routing code, datagram medium: no length prefix, no reassembly."

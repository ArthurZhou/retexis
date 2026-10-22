#!/usr/bin/env bash
# Three nodes in a line:  alice <--tcp--> bob <--tcp--> carol
#
# alice and carol are NOT connected to each other. Everything they exchange
# has to be routed through bob, which is the whole point of the demo.
#
#   ./scripts/demo.sh            (uses target/debug/retexis)
#   ./scripts/demo.sh target/release/retexis
set -uo pipefail

BIN="${1:-./target/debug/retexis}"
OUT="$(mktemp -d)"
PORT_A="${PORT_A:-7100}"
PORT_B="${PORT_B:-7101}"

if [ ! -x "$BIN" ]; then
    echo "building $BIN ..."
    cargo build --quiet || exit 1
fi

echo "logs: $OUT"

# alice: the far end of the line, only knows bob.
( sleep 4; echo "hello mesh, this is alice"; sleep 5; echo "/quit" ) \
    | "$BIN" --name alice --listen "tcp://127.0.0.1:$PORT_A" >"$OUT/alice.log" 2>&1 &

sleep 1

# bob: the relay. Connected to both ends.
( sleep 1; echo "bob here, relaying"; sleep 6; echo "/peers"; sleep 1; echo "/quit" ) \
    | "$BIN" --name bob --listen "tcp://127.0.0.1:$PORT_B" \
             --connect "tcp://127.0.0.1:$PORT_A" >"$OUT/bob.log" 2>&1 &

sleep 1

# carol: two hops away from alice. Unicasts to alice by name.
( sleep 2; echo "carol here, two hops away"; sleep 1; echo "/net"; sleep 0.4; echo "/routes"; sleep 2; \
  echo "/msg alice hello alice, this is a unicast that bob forwarded"; sleep 2; echo "/quit" ) \
    | "$BIN" --name carol --connect "tcp://127.0.0.1:$PORT_B" >"$OUT/carol.log" 2>&1 &

wait

for n in alice bob carol; do
    echo
    echo "════════════════════════ $n ════════════════════════"
    cat "$OUT/$n.log"
done

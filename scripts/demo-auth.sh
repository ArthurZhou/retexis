#!/usr/bin/env bash
# Authenticated mesh + reconnect demo.
#
#   alice <--tcp--> bob <--tcp--> carol      all three share a key
#                       ^
#                       +-- mallory          wrong key: refused at the filter
#
# bob also kicks alice mid-run to show the supervised dialer coming back.
#
#   ./scripts/demo-auth.sh
set -uo pipefail

BIN="${1:-./target/debug/retexis}"
OUT="$(mktemp -d)"
PORT_A="${PORT_A:-7200}"
PORT_B="${PORT_B:-7201}"
PSK="correct horse battery staple"

if [ ! -x "$BIN" ]; then
    echo "building $BIN ..."
    cargo build --quiet || exit 1
fi

echo "logs: $OUT"
echo "mesh key: \"$PSK\""

( sleep 5;  echo "hello from alice"; sleep 3; echo "/status"; sleep 2; echo "/quit" ) \
    | "$BIN" --name alice --psk "$PSK" --listen "tcp://127.0.0.1:$PORT_A" \
        >"$OUT/alice.log" 2>&1 &

sleep 1

( sleep 1.5; echo "bob here"; sleep 2.5; echo "/kick alice"; sleep 2.5; echo "/peers"; sleep 2; echo "/quit" ) \
    | "$BIN" --name bob --psk "$PSK" --listen "tcp://127.0.0.1:$PORT_B" \
             --connect "tcp://127.0.0.1:$PORT_A" >"$OUT/bob.log" 2>&1 &

sleep 1

( sleep 2; echo "carol here, two hops away"; sleep 1; \
  echo "/msg alice this DM is acked and retransmitted"; sleep 2; echo "/status"; sleep 1; echo "/quit" ) \
    | "$BIN" --name carol --psk "$PSK" --connect "tcp://127.0.0.1:$PORT_B" \
        >"$OUT/carol.log" 2>&1 &

# The intruder: same mesh address, different key.
( sleep 3; echo "let me in"; sleep 1; echo "/msg bob hello?"; sleep 2; echo "/routes"; sleep 1; echo "/quit" ) \
    | "$BIN" --name mallory --psk "i guessed the password" --connect "tcp://127.0.0.1:$PORT_B" \
        >"$OUT/mallory.log" 2>&1 &

wait

for n in alice bob carol mallory; do
    echo
    echo "════════════════════════ $n ════════════════════════"
    cat "$OUT/$n.log"
done

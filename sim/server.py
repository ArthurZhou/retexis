#!/usr/bin/env python3
"""Reference-model simulator: JSON API + the shared single-page UI.

    python sim/server.py                 # http://127.0.0.1:8770
    python sim/server.py --port 9000 --nodes 6 --preset ring

    NOTE: `cargo run -p retexis-sim` is the simulator you probably want. It
    drives the *production* routers over a virtual switch, so it cannot drift
    from the shipped protocol. This server drives the Python re-implementation
    in `sim/retexis/` instead — a second implementation kept on purpose, to
    check the wire format and to run statistical scenarios fast.

    It serves the same API and the same HTML, which is what makes the two
    engines comparable: same screen, different code underneath.

The browser is only a view and a controller. Every protocol decision happens
in `sim/retexis/` (the model), not in this file.

Concurrency: the simulator is single-threaded by design (it is a discrete event
queue). One lock guards it. The tick thread advances simulated time at
`rate` x wall-clock; HTTP handlers take the same lock for the few microseconds
they need to apply a command.
"""

from __future__ import annotations

import argparse
import collections
import json
import math
import os
import sys
import threading
import time
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from retexis.net import Sim, Supervisor  # noqa: E402

WEB_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "web")

CANVAS_W = 1000
CANVAS_H = 620

NAMES = [
    "alice", "bob", "carol", "dave", "erin", "frank", "grace", "heidi",
    "ivan", "judy", "kenji", "lena", "oscar", "peggy", "quinn", "rita",
]

STAT_KEYS = (
    "sent", "delivered", "forwarded", "retransmits", "acked",
    "delivery_failed", "dropped_duplicate", "flood_suppressed",
    "auth_rejected", "duplicate_links",
)


def link_key(a: str, b: str) -> str:
    return f"{a}|{b}" if a <= b else f"{b}|{a}"


class Simulation:
    """Owns one `Sim` plus the presentation state the protocol does not have."""

    def __init__(self, seed: int = 1234):
        self.lock = threading.RLock()
        self.seed = seed
        self.sim: Sim = None  # type: ignore[assignment]
        self.pos: dict[str, list] = {}
        self.links: dict[str, dict] = {}
        self.supers: dict[str, Supervisor] = {}
        self.log: collections.deque = collections.deque(maxlen=600)
        self.log_seq = 0
        self.running = False
        self.rate = 1.0
        self._stop = False
        self.reset("line", 4)
        threading.Thread(target=self._loop, name="sim-tick", daemon=True).start()

    # ------------------------------------------------------------ lifecycle

    def reset(self, kind: str = "line", count: int = 4) -> None:
        with self.lock:
            for sup in self.supers.values():
                sup.stop()
            self.supers.clear()
            self.links.clear()
            self.pos.clear()
            self.log.clear()
            self.log_seq = 0
            self.sim = Sim(seed=self.seed, on_log=self._record)
            self.running = False

            count = max(2, min(len(NAMES), int(count)))
            coords = self._layout(kind, count)
            nodes = []
            for i, (x, y) in enumerate(coords):
                node = self.sim.node(
                    NAMES[i], ttl=8, announce=0.5, route_timeout=3.0
                )
                self.pos[node.id.hex()] = [x, y]
                nodes.append(node)
            for a, b in self._edges(kind, count):
                self._add_link(nodes[a], nodes[b], 0.02, 0.0, False)

    def _layout(self, kind: str, n: int) -> list[tuple[float, float]]:
        if kind == "ring":
            cx, cy, r = CANVAS_W / 2, CANVAS_H / 2, 210
            return [
                (cx + r * math.cos(2 * math.pi * i / n - math.pi / 2),
                 cy + r * math.sin(2 * math.pi * i / n - math.pi / 2))
                for i in range(n)
            ]
        if kind == "star":
            cx, cy, r = CANVAS_W / 2, CANVAS_H / 2, 220
            pts = [(cx, cy)]
            for i in range(n - 1):
                a = 2 * math.pi * i / max(1, n - 1) - math.pi / 2
                pts.append((cx + r * math.cos(a), cy + r * math.sin(a)))
            return pts
        if kind == "grid":
            cols = math.ceil(math.sqrt(n))
            rows = math.ceil(n / cols)
            dx = (CANVAS_W - 240) / max(1, cols - 1) if cols > 1 else 0
            dy = (CANVAS_H - 220) / max(1, rows - 1) if rows > 1 else 0
            pts = []
            for i in range(n):
                r, c = divmod(i, cols)
                pts.append((120 + c * dx, 130 + r * dy))
            return pts
        # line
        left, right = 130, CANVAS_W - 130
        step = (right - left) / max(1, n - 1)
        return [(left + i * step, CANVAS_H / 2) for i in range(n)]

    def _edges(self, kind: str, n: int) -> list[tuple[int, int]]:
        if kind == "ring":
            return [(i, (i + 1) % n) for i in range(n)]
        if kind == "star":
            return [(0, i) for i in range(1, n)]
        if kind == "grid":
            cols = math.ceil(math.sqrt(n))
            edges = []
            for i in range(n):
                if i % cols != cols - 1 and i + 1 < n:
                    edges.append((i, i + 1))
                if i + cols < n:
                    edges.append((i, i + cols))
            return edges
        return [(i, i + 1) for i in range(n - 1)]

    def _loop(self) -> None:
        """Advances simulated time at `rate` x wall clock."""
        last = time.monotonic()
        while not self._stop:
            time.sleep(0.02)
            now = time.monotonic()
            dt = now - last
            last = now
            if dt <= 0:
                continue
            with self.lock:
                if self.running and self.sim is not None:
                    # Cap the batch so a slow tick never blocks the UI for long.
                    self.sim.run_until(self.sim.now + min(dt, 0.1) * self.rate)

    def _record(self, entry) -> None:
        when, who, tag, detail = entry
        self.log_seq += 1
        self.log.append(
            {
                "seq": self.log_seq,
                "t": round(when, 3),
                "who": who,
                "tag": tag,
                "detail": detail,
            }
        )

    # --------------------------------------------------------------- helpers

    def _node(self, node_id: str):
        for node in self.sim.nodes.values():
            if node.id.hex() == node_id:
                return node
        return None

    def _unique_name(self, base: str) -> str:
        existing = {n.name for n in self.sim.nodes.values()}
        if base not in existing:
            return base
        i = 2
        while f"{base}{i}" in existing:
            i += 1
        return f"{base}{i}"

    def _add_link(self, a, b, latency: float, loss: float, supervise: bool) -> str:
        key = link_key(a.id.hex(), b.id.hex())
        if key in self.links:
            return key
        self.sim.link(a, b, latency, loss)
        self.links[key] = {
            "a": a.id.hex(),
            "b": b.id.hex(),
            "latency": latency,
            "loss": loss,
            "supervise": supervise,
        }
        if supervise:
            self.supers[key] = Supervisor(
                self.sim, a, b, latency=latency, loss=loss
            ).start()
        return key

    def _drop_link(self, key: str) -> None:
        rec = self.links.pop(key, None)
        if rec is None:
            return
        sup = self.supers.pop(key, None)
        if sup is not None:
            sup.stop()
        a = self._node(rec["a"])
        b = self._node(rec["b"])
        if a is not None and b is not None:
            self.sim.cut(a, b, "removed")

    # ---------------------------------------------------------------- view

    def _node_view(self, node, now: float) -> dict:
        x, y = self.pos.get(node.id.hex(), [CANVAS_W / 2, CANVAS_H / 2])
        routes = [
            {
                "dst": dst.hex()[:8],
                "name": node.names.get(dst, ""),
                "hops": entry["metric"],
                "age": round(now - entry["last_seen"], 2),
            }
            for dst, entry in node.table.items()
        ]
        routes.sort(key=lambda r: (r["hops"], r["name"]))
        inbox = [
            {
                "t": round(d.t, 2),
                "from": d.from_name or d.from_id.hex()[:8],
                "broadcast": d.broadcast,
                "hops": d.hops,
                "text": d.text[:200],
            }
            for d in node.inbox[-24:]
        ]
        return {
            "id": node.id.hex(),
            "short": node.short(),
            "name": node.name,
            "alive": node.alive,
            "ttl": node.ttl,
            "x": round(x, 1),
            "y": round(y, 1),
            "pending": len(node.pending),
            "links": len(node.links),
            "routes": routes,
            "stats": {k: node.stats.get(k, 0) for k in STAT_KEYS},
            "inbox": inbox,
        }

    def snapshot(self, since_log: int = 0) -> dict:
        with self.lock:
            now = self.sim.now
            nodes = [self._node_view(n, now) for n in self.sim.nodes.values()]
            nodes.sort(key=lambda v: v["name"])
            links = []
            for key, rec in self.links.items():
                a = self._node(rec["a"])
                b = self._node(rec["b"])
                if a is None or b is None:
                    continue
                links.append(
                    {
                        "key": key,
                        "a": rec["a"],
                        "b": rec["b"],
                        "latency": rec["latency"],
                        "loss": rec["loss"],
                        "up": self.sim.are_linked(a, b),
                        "supervise": rec["supervise"],
                    }
                )
            return {
                "clock": round(now, 3),
                "running": self.running,
                "rate": self.rate,
                "nodes": nodes,
                "links": links,
                "log": [e for e in self.log if e["seq"] > since_log],
                "log_seq": self.log_seq,
            }

    # ------------------------------------------------------------- commands

    def control(self, running=None, rate=None, step=None) -> None:
        with self.lock:
            if running is not None:
                self.running = bool(running)
            if rate is not None:
                self.rate = max(0.1, min(20.0, float(rate)))
            if step:
                self.sim.run_until(self.sim.now + float(step))

    def add_node(self, name=None, ttl=8, x=None, y=None) -> str:
        with self.lock:
            base = (name or "").strip() or NAMES[len(self.sim.nodes) % len(NAMES)]
            node = self.sim.node(
                self._unique_name(base),
                ttl=int(ttl or 8),
                announce=0.5,
                route_timeout=3.0,
            )
            if x is None or y is None:
                angle = len(self.pos) * 2.399  # golden-angle spiral
                x = CANVAS_W / 2 + 190 * math.cos(angle)
                y = CANVAS_H / 2 + 150 * math.sin(angle)
            self.pos[node.id.hex()] = [float(x), float(y)]
            return node.id.hex()

    def update_node(self, node_id: str, patch: dict) -> None:
        with self.lock:
            node = self._node(node_id)
            if node is None:
                raise ValueError("unknown node")
            if "name" in patch and patch["name"]:
                node.name = str(patch["name"])[:24]
                node.names[node.id] = node.name
            if "ttl" in patch:
                node.ttl = max(1, min(255, int(patch["ttl"])))
            if "x" in patch and "y" in patch:
                self.pos[node_id] = [float(patch["x"]), float(patch["y"])]
            if "alive" in patch:
                want = bool(patch["alive"])
                if want and not node.alive:
                    self.sim.revive(node)
                elif not want and node.alive:
                    # Supervised dialers keep retrying while the node is down,
                    # so reviving it reconnects the links automatically.
                    self.sim.kill(node)

    def delete_node(self, node_id: str) -> None:
        with self.lock:
            node = self._node(node_id)
            if node is None:
                raise ValueError("unknown node")
            for key, rec in list(self.links.items()):
                if node_id in (rec["a"], rec["b"]):
                    self._drop_link(key)
            self.pos.pop(node_id, None)
            self.sim.nodes.pop(node.name, None)

    def add_link(self, a_id: str, b_id: str, latency=0.02, loss=0.0,
                 supervise=False) -> str:
        with self.lock:
            a = self._node(a_id)
            b = self._node(b_id)
            if a is None or b is None:
                raise ValueError("unknown node")
            if a is b:
                raise ValueError("cannot link a node to itself")
            return self._add_link(a, b, float(latency), float(loss), bool(supervise))

    def update_link(self, key: str, patch: dict) -> None:
        with self.lock:
            rec = self.links.get(key)
            if rec is None:
                raise ValueError("unknown link")
            a = self._node(rec["a"])
            b = self._node(rec["b"])
            if a is None or b is None:
                raise ValueError("link endpoints are gone")

            if "latency" in patch:
                rec["latency"] = max(0.0, min(2.0, float(patch["latency"])))
            if "loss" in patch:
                rec["loss"] = max(0.0, min(0.95, float(patch["loss"])))

            for node, peer in ((a, b), (b, a)):
                for link in node.links.values():
                    if link.peer is peer:
                        link.latency = rec["latency"]
                        link.loss = rec["loss"]

            if "up" in patch:
                want = bool(patch["up"])
                up = self.sim.are_linked(a, b)
                if want and not up:
                    self.sim.link(a, b, rec["latency"], rec["loss"])
                elif not want and up:
                    self.sim.cut(a, b, "cut by operator")

            if "supervise" in patch:
                want = bool(patch["supervise"])
                rec["supervise"] = want
                sup = self.supers.pop(key, None)
                if sup is not None:
                    sup.stop()
                if want:
                    self.supers[key] = Supervisor(
                        self.sim, a, b, latency=rec["latency"], loss=rec["loss"]
                    ).start()

    def send(self, from_id: str, to_id: str | None, text: str, reliable: bool) -> None:
        with self.lock:
            src = self._node(from_id)
            if src is None:
                raise ValueError("unknown sender")
            if not src.alive:
                raise ValueError("that node is down")
            if not text:
                raise ValueError("empty message")
            if to_id:
                dst = self._node(to_id)
                if dst is None:
                    raise ValueError("unknown recipient")
                if reliable:
                    src.send_reliable(dst, text)
                else:
                    src.send_to(dst, text)
            else:
                src.broadcast(text)
            # A message that cannot move is confusing; start the clock.
            self.running = True


class Handler(BaseHTTPRequestHandler):
    server_version = "retexis-sim/1.0"
    simulation: Simulation = None  # type: ignore[assignment]

    def log_message(self, fmt, *args):  # noqa: A003 - silence the access log
        pass

    # ------------------------------------------------------------ plumbing

    def _json(self, payload, status=200):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("content-type", "application/json; charset=utf-8")
        self.send_header("content-length", str(len(body)))
        self.send_header("cache-control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _error(self, status, message):
        self._json({"error": message}, status)

    def _body(self) -> dict:
        length = int(self.headers.get("content-length") or 0)
        if not length:
            return {}
        raw = self.rfile.read(length)
        return json.loads(raw.decode("utf-8")) if raw else {}

    def _parts(self):
        return [p for p in urlparse(self.path).path.split("/") if p]

    def _file(self, path, mime):
        try:
            with open(path, "rb") as fh:
                body = fh.read()
        except OSError:
            return self._error(404, f"{os.path.basename(path)} not found")
        self.send_response(200)
        self.send_header("content-type", mime)
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    # --------------------------------------------------------------- routes

    def do_GET(self):
        try:
            parts = self._parts()
            if not parts or parts[0] in ("index.html", "web"):
                return self._file(
                    os.path.join(WEB_DIR, "index.html"), "text/html; charset=utf-8"
                )
            if parts[:2] == ["api", "state"]:
                since = int(
                    (parse_qs(urlparse(self.path).query).get("log_since") or ["0"])[0]
                    or 0
                )
                return self._json(self.simulation.snapshot(since))
            return self._error(404, "not found")
        except Exception as exc:  # noqa: BLE001 - report, never crash the server
            return self._error(500, f"{type(exc).__name__}: {exc}")

    def do_POST(self):
        try:
            parts = self._parts()
            body = self._body()
            sim = self.simulation
            if parts[:2] == ["api", "control"]:
                sim.control(
                    running=body.get("running"),
                    rate=body.get("rate"),
                    step=body.get("step"),
                )
            elif parts[:2] == ["api", "preset"]:
                sim.reset(body.get("kind", "line"), body.get("count", 4))
            elif parts[:2] == ["api", "node"]:
                node_id = sim.add_node(
                    body.get("name"),
                    body.get("ttl", 8),
                    body.get("x"),
                    body.get("y"),
                )
                return self._json({"ok": True, "id": node_id})
            elif parts[:2] == ["api", "link"]:
                key = sim.add_link(
                    body.get("a"),
                    body.get("b"),
                    body.get("latency", 0.02),
                    body.get("loss", 0.0),
                    body.get("supervise", False),
                )
                return self._json({"ok": True, "key": key})
            elif parts[:2] == ["api", "send"]:
                sim.send(
                    body.get("from"),
                    body.get("to"),
                    body.get("text", ""),
                    bool(body.get("reliable")),
                )
            else:
                return self._error(404, "not found")
            return self._json({"ok": True})
        except Exception as exc:  # noqa: BLE001
            return self._error(400, f"{type(exc).__name__}: {exc}")

    def do_PATCH(self):
        try:
            parts = self._parts()
            body = self._body()
            sim = self.simulation
            if len(parts) == 3 and parts[:2] == ["api", "node"]:
                sim.update_node(parts[2], body)
            elif len(parts) == 3 and parts[:2] == ["api", "link"]:
                sim.update_link(parts[2], body)
            else:
                return self._error(404, "not found")
            return self._json({"ok": True})
        except Exception as exc:  # noqa: BLE001
            return self._error(400, f"{type(exc).__name__}: {exc}")

    def do_DELETE(self):
        try:
            parts = self._parts()
            sim = self.simulation
            if len(parts) == 3 and parts[:2] == ["api", "node"]:
                sim.delete_node(parts[2])
            elif len(parts) == 3 and parts[:2] == ["api", "link"]:
                sim._drop_link(parts[2])
            else:
                return self._error(404, "not found")
            return self._json({"ok": True})
        except Exception as exc:  # noqa: BLE001
            return self._error(400, f"{type(exc).__name__}: {exc}")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="retexis interactive simulator")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8770)
    ap.add_argument("--preset", default="line", choices=["line", "ring", "grid", "star"])
    ap.add_argument("--nodes", type=int, default=4)
    ap.add_argument("--seed", type=int, default=1234)
    ap.add_argument("--no-open", action="store_true", help="do not open a browser")
    args = ap.parse_args(argv)

    Handler.simulation = Simulation(seed=args.seed)
    Handler.simulation.reset(args.preset, args.nodes)

    httpd = ThreadingHTTPServer((args.host, args.port), Handler)
    url = f"http://{args.host}:{args.port}/"
    print(f"retexis simulator  →  {url}")
    print(f"  topology: {args.preset} x{args.nodes}   Ctrl-C to stop")
    if not args.no_open:
        threading.Timer(0.5, lambda: webbrowser.open(url)).start()
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nbye")
    finally:
        Handler.simulation._stop = True
        httpd.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

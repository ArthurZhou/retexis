#!/usr/bin/env python3
"""Tests for the interactive simulator's HTTP API.

The UI is thin on purpose: everything it can do is one API call. So this file
tests the API surface rather than the DOM — start the server in-process, drive
it over real HTTP, assert on the state it reports back.

    python -m unittest sim/test_server.py -v
"""

from __future__ import annotations

import json
import os
import sys
import threading
import time
import unittest
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import server as simserver  # noqa: E402


class ApiTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sim = simserver.Simulation(seed=7)
        simserver.Handler.simulation = cls.sim
        cls.httpd = ThreadingHTTPServer(("127.0.0.1", 0), simserver.Handler)
        cls.base = f"http://127.0.0.1:{cls.httpd.server_address[1]}"
        threading.Thread(target=cls.httpd.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls):
        cls.httpd.shutdown()
        cls.httpd.server_close()
        cls.sim._stop = True

    def setUp(self):
        self.sim.reset("line", 4)
        self.call("/api/control", "POST", {"running": False, "rate": 1.0})

    # ------------------------------------------------------------- helpers

    def call(self, path, method="GET", body=None):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(
            self.base + path,
            data=data,
            method=method,
            headers={"content-type": "application/json"},
        )
        with urllib.request.urlopen(req) as res:
            return json.loads(res.read() or b"{}")

    def state(self):
        return self.call("/api/state")

    def ids(self):
        return {n["name"]: n["id"] for n in self.state()["nodes"]}

    def run_for(self, seconds):
        """Advances the simulation by stepping, so tests never sleep long."""
        self.call("/api/control", "POST", {"step": seconds})

    def converge(self, seconds=4.0):
        self.run_for(seconds)

    def wait_for(self, pred, timeout=6.0, step=0.25):
        """Steps simulated time until `pred` holds (or the budget runs out)."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if pred():
                return True
            self.run_for(step)
        return pred()

    # --------------------------------------------------------------- tests

    def test_initial_topology(self):
        st = self.state()
        self.assertEqual(len(st["nodes"]), 4)
        self.assertEqual(len(st["links"]), 3)
        self.assertEqual([n["name"] for n in st["nodes"]], ["alice", "bob", "carol", "dave"])
        for n in st["nodes"]:
            self.assertGreater(n["x"], 0)
            self.assertLess(n["x"], 1000)
            self.assertEqual(n["routes"], [], "nothing is routed before time moves")

    def test_converges_when_run(self):
        # Exercises the wall-clock tick thread rather than `step`. Distance
        # vector spreads one hop per advertisement, so a 3-hop line needs a
        # couple of announce intervals (0.5s each); poll instead of sleeping a
        # fixed amount, or a loaded machine makes this flaky.
        self.call("/api/control", "POST", {"running": True})
        deadline = time.monotonic() + 10.0
        converged = False
        while time.monotonic() < deadline:
            time.sleep(0.1)
            if all(len(n["routes"]) == 3 for n in self.state()["nodes"]):
                converged = True
                break
        self.call("/api/control", "POST", {"running": False})
        self.assertTrue(converged, "the tick thread should converge a 4-node line")
        for n in self.state()["nodes"]:
            self.assertEqual(len(n["routes"]), 3, f"{n['name']} should know the other three")

    def test_broadcast_reaches_every_node(self):
        ids = self.ids()
        self.converge(2.0)
        self.call("/api/send", "POST", {"from": ids["alice"], "text": "hi", "to": None})
        self.run_for(1.0)
        st = self.state()
        hops = {}
        for n in st["nodes"]:
            hits = [m for m in n["inbox"] if m["text"] == "hi"]
            self.assertEqual(len(hits), 1, f"{n['name']} should receive it exactly once")
            hops[n["name"]] = hits[0]["hops"]
        self.assertEqual(hops, {"alice": 0, "bob": 1, "carol": 2, "dave": 3})

    def test_reliable_unicast_is_acked(self):
        ids = self.ids()
        self.converge(2.0)
        self.call(
            "/api/send",
            "POST",
            {"from": ids["dave"], "to": ids["alice"], "text": "dm", "reliable": True},
        )
        self.run_for(1.0)
        st = self.state()
        dave = next(n for n in st["nodes"] if n["name"] == "dave")
        alice = next(n for n in st["nodes"] if n["name"] == "alice")
        self.assertEqual(dave["stats"]["acked"], 1)
        self.assertEqual(dave["pending"], 0)
        self.assertTrue(any(m["text"] == "dm" for m in alice["inbox"]))

    def test_cut_and_heal(self):
        ids = self.ids()
        self.converge(2.0)
        key = self.state()["links"][0]["key"]
        self.call(f"/api/link/{key}", "PATCH", {"up": False})
        self.assertFalse(
            next(l for l in self.state()["links"] if l["key"] == key)["up"]
        )
        self.run_for(4.0)  # let routes expire
        st = self.state()
        self.assertEqual(len(st["nodes"][0]["routes"]), 0, "the cut end is isolated")

        self.call(f"/api/link/{key}", "PATCH", {"up": True})
        self.run_for(2.0)
        self.assertEqual(len(self.state()["nodes"][0]["routes"]), 3)

    def test_kill_and_revive(self):
        ids = self.ids()
        self.converge(2.0)
        self.call(f"/api/node/{ids['carol']}", "PATCH", {"alive": False})
        st = self.state()
        carol = next(n for n in st["nodes"] if n["name"] == "carol")
        self.assertFalse(carol["alive"])
        self.assertEqual(carol["routes"], [])

        # A message must not reach the dead node, and must still reach the rest.
        self.call("/api/send", "POST", {"from": ids["alice"], "text": "anyone?", "to": None})
        self.run_for(1.5)
        st = self.state()
        carol = next(n for n in st["nodes"] if n["name"] == "carol")
        bob = next(n for n in st["nodes"] if n["name"] == "bob")
        self.assertFalse(any(m["text"] == "anyone?" for m in carol["inbox"]))
        self.assertTrue(any(m["text"] == "anyone?" for m in bob["inbox"]))

        # With supervised dialers the links come back on their own. Without
        # supervision they stay down: a restarted process has no connections,
        # which is exactly what happens in reality.
        for link in self.state()["links"]:
            if ids["carol"] in (link["a"], link["b"]):
                self.call(f"/api/link/{link['key']}", "PATCH", {"supervise": True})

        self.call(f"/api/node/{ids['carol']}", "PATCH", {"alive": True})
        self.assertTrue(
            self.wait_for(
                lambda: next(
                    n for n in self.state()["nodes"] if n["name"] == "carol"
                )["routes"]
                and len(next(n for n in self.state()["nodes"] if n["name"] == "carol")["routes"]) == 3
            ),
            "a revived node with a supervised link must reconnect and re-converge",
        )

    def test_link_loss_is_applied(self):
        key = self.state()["links"][0]["key"]
        self.call(f"/api/link/{key}", "PATCH", {"loss": 0.5, "latency": 0.2})
        link = next(l for l in self.state()["links"] if l["key"] == key)
        self.assertAlmostEqual(link["loss"], 0.5)
        self.assertAlmostEqual(link["latency"], 0.2)

        ids = self.ids()
        self.converge(2.0)
        for _ in range(6):
            self.call("/api/send", "POST", {"from": ids["alice"], "text": "lossy", "to": None})
        self.run_for(2.0)
        self.assertGreater(self.sim.sim.packets_lost, 0, "50% loss must drop something")

    def test_add_and_delete_node(self):
        new_id = self.call("/api/node", "POST", {"name": "zoe"})["id"]
        st = self.state()
        self.assertEqual(len(st["nodes"]), 5)
        self.assertIn("zoe", [n["name"] for n in st["nodes"]])

        self.call(f"/api/link", "POST", {"a": new_id, "b": self.ids()["dave"]})
        self.assertEqual(len(self.state()["links"]), 4)
        self.converge(2.0)
        zoe = next(n for n in self.state()["nodes"] if n["name"] == "zoe")
        self.assertEqual(len(zoe["routes"]), 4, "the new node learns the whole line")

        self.call(f"/api/node/{new_id}", "DELETE")
        st = self.state()
        self.assertEqual(len(st["nodes"]), 4)
        self.assertEqual(len(st["links"]), 3, "deleting a node removes its links")

    def test_presets(self):
        for kind, count, edges in [
            ("line", 5, 4),
            ("ring", 5, 5),
            ("star", 5, 4),
            ("grid", 9, 12),
        ]:
            self.call("/api/preset", "POST", {"kind": kind, "count": count})
            st = self.state()
            self.assertEqual(len(st["nodes"]), count, kind)
            self.assertEqual(len(st["links"]), edges, kind)

    def test_move_node_is_persisted(self):
        nid = self.ids()["alice"]
        self.call(f"/api/node/{nid}", "PATCH", {"x": 321, "y": 123})
        alice = next(n for n in self.state()["nodes"] if n["id"] == nid)
        self.assertEqual((alice["x"], alice["y"]), (321, 123))

    def test_rename_changes_what_peers_learn(self):
        ids = self.ids()
        self.converge(2.0)
        self.call(f"/api/node/{ids['bob']}", "PATCH", {"name": "robert"})
        self.run_for(2.0)
        alice = next(n for n in self.state()["nodes"] if n["name"] == "alice")
        names = [r["name"] for r in alice["routes"]]
        self.assertIn("robert", names, "names travel with route advertisements")

    def test_log_stream_is_incremental(self):
        st = self.state()
        seq = st["log_seq"]
        self.call("/api/control", "POST", {"step": 2.0})
        st = self.state()
        first = self.call(f"/api/state?log_since={seq}")
        self.assertGreater(len(first["log"]), 0, "stepping produces events")
        self.assertGreater(st["log_seq"], seq)
        second = self.call(f"/api/state?log_since={first['log_seq']}")
        self.assertEqual(second["log"], [], "no event is delivered twice")

    def test_bad_requests_are_reported_not_crashed(self):
        with self.assertRaises(urllib.error.HTTPError) as ctx:
            self.call("/api/node/nope", "PATCH", {"ttl": 3})
        self.assertEqual(ctx.exception.code, 400)
        with self.assertRaises(urllib.error.HTTPError) as ctx:
            self.call("/api/link", "POST", {"a": "x", "b": "y"})
        self.assertEqual(ctx.exception.code, 400)
        # Still alive and serving.
        self.assertEqual(len(self.state()["nodes"]), 4)

    def test_self_link_is_rejected(self):
        nid = self.ids()["alice"]
        with self.assertRaises(urllib.error.HTTPError):
            self.call("/api/link", "POST", {"a": nid, "b": nid})


if __name__ == "__main__":
    unittest.main(verbosity=2)

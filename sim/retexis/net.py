"""Discrete-event network model.

Time is a float of simulated seconds. Every transmission is scheduled as a
future event with a latency, and optionally dropped by a per-link loss rate.
That is enough to model the things that actually matter for a mesh protocol:
convergence order, duplicate paths, retransmission timers and partition
recovery.

Nothing here knows about the protocol; it only moves bytes.
"""

from __future__ import annotations

import heapq
import random
from typing import Callable, Dict, List, Optional

from .node import Link, Node


class Sim:
    def __init__(
        self,
        seed: int = 1234,
        tick: float = 0.05,
        verbose: bool = False,
        on_log: Optional[Callable[[tuple], None]] = None,
    ):
        self.now = 0.0
        self._queue: List[tuple] = []
        self._seq = 0
        self._tick = tick
        self.rng = random.Random(seed)
        self.nodes: Dict[str, Node] = {}
        self._link_seq = 0
        self.log: List[tuple] = []
        self.verbose = verbose
        self._tickers: List[Node] = []
        self._supervisors: List["Supervisor"] = []
        #: node names that refuse inbound connections (process down)
        self.refusing = set()
        self.packets_sent = 0
        self.packets_lost = 0
        self._ticks_started = False
        #: called with (when, who, tag, detail) for every event; the web UI
        #: uses it to stream the event log without polling `log` directly.
        self.on_log = on_log

    # ------------------------------------------------------------ scheduling

    def at(self, when: float, fn: Callable[[], None]) -> None:
        heapq.heappush(self._queue, (when, self._seq, fn))
        self._seq += 1

    def after(self, delay: float, fn: Callable[[], None]) -> None:
        self.at(self.now + delay, fn)

    def schedule_tick(self, node: Node) -> None:
        self._tickers.append(node)

    def _tick_loop(self) -> None:
        for node in list(self._tickers):
            node.tick()
        self.after(self._tick, self._tick_loop)

    def run_until(self, when: float) -> None:
        if not self._ticks_started:
            self._ticks_started = True
            self.after(0.0, self._tick_loop)
        while self._queue and self._queue[0][0] <= when:
            when_event, _, fn = heapq.heappop(self._queue)
            self.now = max(self.now, when_event)
            fn()
        self.now = max(self.now, when)

    def run_for(self, seconds: float) -> None:
        self.run_until(self.now + seconds)

    # ---------------------------------------------------------------- nodes

    def node(self, name: str, **kwargs) -> Node:
        n = Node(self, name, **kwargs)
        self.nodes[name] = n
        return n

    def next_link_id(self) -> int:
        self._link_seq += 1
        return self._link_seq

    def link(
        self,
        a: Node,
        b: Node,
        latency: float = 0.02,
        loss: float = 0.0,
    ) -> None:
        """Creates a bidirectional link: two one-directional `Link`s."""
        id_ab = self.next_link_id()
        id_ba = self.next_link_id()
        a.on_link_up(Link(id_ab, b, id_ba, True, latency, loss))
        b.on_link_up(Link(id_ba, a, id_ab, False, latency, loss))

    def cut(self, a: Node, b: Node, reason: str = "cable cut") -> None:
        """Severs both directions, as a physical failure would."""
        for lid, link in list(a.links.items()):
            if link.peer is b:
                a.on_link_down(lid, reason)
        for lid, link in list(b.links.items()):
            if link.peer is a:
                b.on_link_down(lid, reason)

    def are_linked(self, a: Node, b: Node) -> bool:
        return any(l.peer is b and l.up for l in a.links.values())

    def kill(self, node: Node, reason: str = "process died") -> None:
        """Crashes a node: its links stop working and it stops dialling."""
        node.go_down(reason)
        self.refusing.add(node.name)
        for other in self.nodes.values():
            if other is node:
                continue
            for lid, link in list(other.links.items()):
                if link.peer is node:
                    other.on_link_down(lid, f"{node.name} died")

    def revive(self, node: Node) -> None:
        node.come_back()
        self.refusing.discard(node.name)

    # ----------------------------------------------------------- transport

    def transmit(self, link: Link, sealed: bytes) -> None:
        if not link.up or not link.peer.alive:
            return
        if link.loss > 0.0 and self.rng.random() < link.loss:
            self.packets_lost += 1
            return
        self.packets_sent += 1
        peer = link.peer
        peer_link_id = link.peer_link_id
        self.after(link.latency, lambda: peer.receive(peer_link_id, sealed))

    # -------------------------------------------------------------- helpers

    def log_event(self, when: float, who: str, tag: str, detail: str) -> None:
        entry = (when, who, tag, detail)
        self.log.append(entry)
        # A long-running simulation would otherwise grow without bound.
        if len(self.log) > 20_000:
            del self.log[:10_000]
        if self.on_log is not None:
            self.on_log(entry)
        if self.verbose:
            print(f"{when:7.3f}  {who:>10}  {tag:<12} {detail}")

    def totals(self) -> dict:
        out = {
            "frames_out": 0,
            "delivered": 0,
            "forwarded": 0,
            "flood_suppressed": 0,
            "retransmits": 0,
            "dropped_duplicate": 0,
            "auth_rejected": 0,
        }
        for node in self.nodes.values():
            for key in out:
                out[key] += node.stats.get(key, 0)
        out["packets_sent"] = self.packets_sent
        out["packets_lost"] = self.packets_lost
        return out

    def converge(self, timeout: float = 5.0, expect: Optional[int] = None) -> bool:
        """Runs until every node knows every other node (or timeout)."""
        alive = [n for n in self.nodes.values() if n.alive]
        target = expect if expect is not None else len(alive) - 1
        deadline = self.now + timeout
        while self.now < deadline:
            if all(len(n.table) >= target for n in alive):
                return True
            self.run_for(0.05)
        return all(len(n.table) >= target for n in alive)


class Supervisor:
    """Models a `--connect` dialer: keeps one link to a peer alive.

    Mirrors `crates/retexis-routing/src/dial.rs`: probe the link, and on loss
    retry with exponential backoff. Refused attempts (target down) count as
    failures, so the backoff is observable.
    """

    def __init__(
        self,
        sim: Sim,
        a: Node,
        b: Node,
        latency: float = 0.02,
        loss: float = 0.0,
        base: float = 0.25,
        max_delay: float = 5.0,
        probe: float = 0.2,
    ):
        self.sim = sim
        self.a = a
        self.b = b
        self.latency = latency
        self.loss = loss
        self.base = base
        self.max_delay = max_delay
        self.probe = probe
        self.failures = 0
        self.dial_failures = 0
        self.reconnects = 0
        self.enabled = True
        sim._supervisors.append(self)

    def start(self) -> "Supervisor":
        self.sim.after(0.0, self._check)
        return self

    def _can_dial(self) -> bool:
        return self.b.alive and self.b.name not in self.sim.refusing

    def stop(self) -> None:
        """Stops supervising; the current link (if any) is left alone."""
        self.enabled = False

    def _check(self) -> None:
        if not self.enabled or not self.a.alive:
            return
        if not self.sim.are_linked(self.a, self.b):
            if self._can_dial():
                self.sim.link(self.a, self.b, self.latency, self.loss)
                self.failures = 0
                self.reconnects += 1
                self.sim.log_event(
                    self.sim.now, self.a.name, "~ connected", self.b.name
                )
            else:
                self.failures += 1
                self.dial_failures += 1
                delay = min(
                    self.max_delay, self.base * (2 ** min(10, self.failures - 1))
                )
                self.sim.log_event(
                    self.sim.now,
                    self.a.name,
                    "~ retry",
                    f"{self.b.name} in {delay * 1000:.0f}ms (attempt {self.failures})",
                )
                self.sim.after(delay, self._check)
                return
        self.sim.after(self.probe, self._check)

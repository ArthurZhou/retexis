"""Scenarios: each one builds a mesh, runs it, and asserts on the outcome.

A scenario returns a `Result` with a list of checks. `run.py` prints them and
exits non-zero if anything failed, so this doubles as a test suite for the
protocol that runs in about a second.
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from retexis import wire  # noqa: E402
from retexis.net import Sim, Supervisor  # noqa: E402
from retexis.node import FloodPolicy, Node, RetryPolicy  # noqa: E402


@dataclass
class Result:
    name: str
    title: str
    checks: List[Tuple[str, bool, str]] = field(default_factory=list)
    metrics: Dict[str, object] = field(default_factory=dict)
    notes: List[str] = field(default_factory=list)

    def check(self, label: str, ok: bool, detail: str = "") -> bool:
        self.checks.append((label, bool(ok), detail))
        return bool(ok)

    @property
    def ok(self) -> bool:
        return all(passed for _, passed, _ in self.checks)

    def failures(self) -> List[Tuple[str, str]]:
        return [(label, detail) for label, passed, detail in self.checks if not passed]


def _pct(part: float, whole: float) -> str:
    if whole <= 0:
        return "n/a"
    return f"{100.0 * part / whole:.1f}%"


# ---------------------------------------------------------------- scenarios


def scenario_line(verbose: bool = False) -> Result:
    """Four nodes in a chain: a-b-c-d. Proves multi-hop routing both ways."""
    r = Result("line", "4-node chain, 3 hops end to end")
    sim = Sim(verbose=verbose)
    names = ["a", "b", "c", "d"]
    nodes = [sim.node(n) for n in names]
    for x, y in zip(nodes, nodes[1:]):
        sim.link(x, y)

    r.check("mesh converges", sim.converge(timeout=5.0), "every node knows all others")

    env = nodes[0].broadcast("hello mesh")
    sim.run_for(0.5)
    got = [n.received(lambda d: d.msg_id == env.msg_id) for n in nodes]
    r.check(
        "broadcast reaches all 4 nodes",
        all(got),
        f"{sum(1 for g in got if g)}/4",
    )
    hops = [g[0].hops for g in got if g]
    r.check("hop count is 0,1,2,3", hops == [0, 1, 2, 3], f"hops={hops}")

    env2 = nodes[3].send_to(nodes[0], "unicast back")
    sim.run_for(0.5)
    back = nodes[0].received(lambda d: d.msg_id == env2.msg_id)
    r.check("unicast d->a arrives", bool(back), f"{len(back)} delivery")
    if back:
        r.check("unicast crossed 3 hops", back[0].hops == 3, f"hops={back[0].hops}")
        r.check("sender name is known", back[0].from_name == "d", str(back[0].from_name))

    r.metrics = {"frames": sim.totals()["frames_out"], "hops": hops}
    return r


def scenario_triangle(verbose: bool = False) -> Result:
    """Three nodes in a ring. A flood must be seen exactly once per node."""
    r = Result("triangle", "ring topology, duplicate suppression")
    sim = Sim(verbose=verbose)
    a, b, c = (sim.node(n) for n in "abc")
    sim.link(a, b)
    sim.link(b, c)
    sim.link(c, a)

    r.check("mesh converges", sim.converge(timeout=5.0), "all routes present")
    env = a.broadcast("loop test")
    sim.run_for(1.0)

    counts = {n.name: len(n.received(lambda d: d.msg_id == env.msg_id)) for n in (a, b, c)}
    r.check(
        "each node delivers exactly once",
        all(v == 1 for v in counts.values()),
        f"{counts}",
    )
    dups = sum(n.stats["dropped_duplicate"] for n in (a, b, c))
    r.check("duplicates were suppressed", dups > 0, f"{dups} duplicate packets dropped")
    r.metrics = {"deliveries": counts, "duplicates_dropped": dups}
    return r


def scenario_ttl(verbose: bool = False) -> Result:
    """ttl=1 means neighbours only: the third node must never see it."""
    r = Result("ttl", "ttl=1 stays within one hop")
    sim = Sim(verbose=verbose)
    a = sim.node("a", ttl=1)
    b = sim.node("b")
    c = sim.node("c")
    sim.link(a, b)
    sim.link(b, c)
    sim.converge(timeout=5.0)

    env = a.broadcast("one hop only")
    sim.run_for(0.6)
    at_b = b.received(lambda d: d.msg_id == env.msg_id)
    at_c = c.received(lambda d: d.msg_id == env.msg_id)
    r.check("neighbour receives it", bool(at_b), f"hops={at_b[0].hops if at_b else '-'}")
    r.check("two-hop node does not", not at_c, f"{len(at_c)} deliveries")
    r.metrics = {"at_b": len(at_b), "at_c": len(at_c)}
    return r


def scenario_loss_retransmit(verbose: bool = False) -> Result:
    """40% packet loss: reliable unicast still lands, best effort does not."""
    r = Result("loss", "40% loss, ack + retransmit")
    sim = Sim(seed=99, verbose=verbose)
    a = sim.node("a", retry=RetryPolicy(max_attempts=10, base=0.1, max_interval=0.4))
    b = sim.node("b")
    sim.link(a, b, loss=0.4)
    sim.converge(timeout=5.0)

    reliable = [a.send_reliable(b, f"reliable {i}") for i in range(8)]
    best_effort = [a.send_to(b, f"best effort {i}") for i in range(8)]
    sim.run_for(8.0)

    landed = [e for e in reliable if b.received(lambda d, m=e.msg_id: d.msg_id == m)]
    lost = [e for e in best_effort if not b.received(lambda d, m=e.msg_id: d.msg_id == m)]
    r.check(
        "all 8 reliable messages arrive",
        len(landed) == 8,
        f"{len(landed)}/8",
    )
    r.check(
        "retransmissions happened",
        a.stats["retransmits"] > 0,
        f"{a.stats['retransmits']} retransmits",
    )
    r.check(
        "no duplicate delivery to the app",
        all(
            len(b.received(lambda d, m=e.msg_id: d.msg_id == m)) == 1 for e in reliable
        ),
        "retries re-ack without re-delivering",
    )
    r.check(
        "best effort loses some",
        len(lost) > 0,
        f"{len(lost)}/8 lost without acks",
    )
    r.metrics = {
        "reliable_arrived": len(landed),
        "best_effort_lost": len(lost),
        "retransmits": a.stats["retransmits"],
        "duplicate_acks": b.stats["duplicate_acks"],
        "packets_lost": sim.packets_lost,
    }
    r.notes.append(
        f"{sim.packets_lost} packets dropped by the medium, "
        f"{b.stats['duplicate_acks']} lost acks recovered by re-ack"
    )
    return r


def scenario_auth(verbose: bool = False) -> Result:
    """A node with the wrong key must not join an authenticated mesh."""
    r = Result("auth", "pre-shared key membership")
    sim = Sim(verbose=verbose)
    good = wire.key_from_passphrase("the mesh secret")
    bad = wire.key_from_passphrase("i guessed the password")

    a = sim.node("a", psk=good)
    b = sim.node("b", psk=good)
    intruder = sim.node("mallory", psk=bad)
    sim.link(a, b)
    sim.link(b, intruder)
    sim.converge(timeout=5.0)
    sim.run_for(0.5)

    r.check(
        "intruder has no routes",
        len(intruder.table) == 0,
        f"{len(intruder.table)} routes",
    )
    r.check(
        "intruder is not in the mesh's name table",
        intruder.id not in a.names and intruder.id not in b.names,
        f"known names: {sorted(a.names.values())}",
    )
    r.check(
        "rejections were counted",
        b.stats["auth_rejected"] > 0,
        f"b rejected {b.stats['auth_rejected']} packets",
    )

    env = a.send_to(b, "secret traffic")
    sim.run_for(0.5)
    r.check(
        "legit traffic still flows",
        bool(b.received(lambda d: d.msg_id == env.msg_id)),
        "a->b delivered",
    )
    r.check(
        "intruder sees nothing",
        len(intruder.received()) == 0,
        f"{len(intruder.received())} deliveries",
    )

    # A plaintext node on an authenticated mesh must be refused too.
    plain = sim.node("plain")
    sim.link(b, plain)
    sim.run_for(1.0)
    r.check(
        "unsigned peer is refused",
        len(plain.table) == 0 and plain.id not in b.names,
        "b drops unsigned packets before the codec, and plain cannot verify b",
    )
    r.metrics = {
        "b_rejected": b.stats["auth_rejected"],
        "intruder_routes": len(intruder.table),
        "intruder_deliveries": len(intruder.received()),
    }
    return r


def scenario_reconnect(verbose: bool = False) -> Result:
    """Kill a node, watch the dialer back off, revive it, watch it recover."""
    r = Result("reconnect", "dialer backoff and recovery")
    sim = Sim(verbose=verbose)
    a = sim.node("a")
    b = sim.node("b", announce=0.3, route_timeout=1.5)
    sup = Supervisor(sim, a, b, base=0.25, max_delay=2.0, probe=0.2).start()
    sim.run_for(1.0)
    r.check("initially connected", sim.are_linked(a, b), "a dialled b")
    r.check("routes converged", len(a.table) == 1, f"{len(a.table)} route(s)")

    sim.kill(b)
    sim.run_for(4.0)
    r.check(
        "route to the dead node expired",
        len(a.table) == 0,
        f"{len(a.table)} route(s) after the kill",
    )
    r.check(
        "dialer retried with backoff",
        sup.dial_failures >= 2,
        f"{sup.dial_failures} refused attempts",
    )
    backoff = [
        entry[3]
        for entry in sim.log
        if entry[1] == "a" and entry[2] == "~ retry"
    ]
    r.check(
        "backoff grows",
        len(backoff) >= 2 and backoff[0] != backoff[-1],
        f"{backoff[:4]}",
    )

    sim.revive(b)
    sim.run_for(4.0)
    r.check("reconnected", sim.are_linked(a, b), "a re-dialled b")
    r.check("routes came back", len(a.table) == 1, f"{len(a.table)} route(s)")

    env = a.send_to(b, "after the outage")
    sim.run_for(0.5)
    r.check(
        "traffic flows again",
        bool(b.received(lambda d: d.msg_id == env.msg_id)),
        "a->b delivered",
    )
    r.metrics = {
        "refused_attempts": sup.dial_failures,
        "reconnects": sup.reconnects,
        "backoff_delays": backoff[:4],
    }
    return r


def scenario_partition(verbose: bool = False) -> Result:
    """Cut the middle of a chain, let routes expire, heal, reconverge."""
    r = Result("partition", "split brain and heal")
    sim = Sim(verbose=verbose)
    a = sim.node("a", announce=0.3, route_timeout=1.2)
    b = sim.node("b", announce=0.3, route_timeout=1.2)
    c = sim.node("c", announce=0.3, route_timeout=1.2)
    sim.link(a, b)
    sim.link(b, c)
    r.check("converged before the cut", sim.converge(timeout=5.0), "a knows c")

    sim.cut(b, c)
    sim.run_for(2.5)
    r.check(
        "a forgot c",
        c.id not in a.table,
        f"a routes: {len(a.table)}",
    )
    r.check(
        "c forgot a",
        a.id not in c.table,
        f"c routes: {len(c.table)}",
    )
    env = a.send_to(c, "into the void")
    sim.run_for(1.0)
    r.check(
        "messages are not delivered across a partition",
        not c.received(lambda d: d.msg_id == env.msg_id),
        "no path, no delivery",
    )

    sim.link(b, c)
    sim.run_for(2.0)
    r.check("reconverged after the heal", a.table.get(c.id) is not None, "a knows c again")
    env2 = a.send_to(c, "back in business")
    sim.run_for(1.0)
    r.check(
        "traffic crosses again",
        bool(c.received(lambda d: d.msg_id == env2.msg_id)),
        "a->c delivered",
    )
    r.metrics = {"routes_after_heal": len(a.table)}
    return r


def scenario_relay_retry(verbose: bool = False) -> Result:
    """A retry must survive a relay whose next hop was briefly down.

    The relay caches the message id on first sight. If it treats the retry as
    a plain duplicate it swallows it forever, and the sender can retry until it
    gives up without the packet ever crossing the last hop.
    """
    r = Result("relay", "retry through a relay that lost its next hop")
    sim = Sim(verbose=verbose)
    a = sim.node("a", announce=0.2, route_timeout=1.0)
    b = sim.node("b", announce=0.2, route_timeout=1.0)
    c = sim.node(
        "c",
        announce=0.2,
        route_timeout=1.0,
        retry=RetryPolicy(max_attempts=12, base=0.06, max_interval=0.4),
    )
    sim.link(a, b)
    sim.link(b, c)
    sim.converge(timeout=5.0)

    sim.cut(a, b)
    env = c.send_reliable(a, "through a broken relay")
    sim.run_for(0.25)
    sim.link(a, b)
    sim.run_for(3.0)

    got = a.received(lambda d: d.msg_id == env.msg_id)
    r.check(
        "the retry crossed the repaired hop",
        bool(got),
        f"{len(got)} delivery at the destination",
    )
    r.check("delivered exactly once", len(got) == 1, f"{len(got)} deliveries")
    r.check(
        "the relay saw it as a duplicate and forwarded anyway",
        b.stats["dropped_duplicate"] > 0,
        f"{b.stats['dropped_duplicate']} duplicates at the relay",
    )
    r.metrics = {
        "retransmits": c.stats["retransmits"],
        "relay_duplicates": b.stats["dropped_duplicate"],
        "delivered": len(got),
    }
    return r


def _grid(sim: Sim, cols: int, rows: int, **node_kw) -> List[Node]:
    nodes = {}
    for i in range(cols):
        for j in range(rows):
            nodes[(i, j)] = sim.node(f"n{i}{j}", **node_kw)
    for (i, j), node in nodes.items():
        if (i + 1, j) in nodes:
            sim.link(node, nodes[(i + 1, j)])
        if (i, j + 1) in nodes:
            sim.link(node, nodes[(i, j + 1)])
    return list(nodes.values())


def _storm_run(flood: FloodPolicy, messages_per_node: int = 8, seed: int = 7) -> dict:
    sim = Sim(seed=seed)
    nodes = _grid(sim, 4, 3, flood=flood, announce=0.5, route_timeout=3.0)
    sim.converge(timeout=5.0)

    for _ in range(messages_per_node):
        for node in nodes:
            node.broadcast("storm")
        sim.run_for(0.1)
    sim.run_for(1.0)

    totals = sim.totals()
    delivered = sum(len(n.inbox) for n in nodes)
    expected = messages_per_node * len(nodes) * len(nodes)
    return {
        "nodes": len(nodes),
        "messages": messages_per_node * len(nodes),
        "frames": totals["frames_out"],
        "delivered": delivered,
        "expected": expected,
        "suppressed": totals["flood_suppressed"],
        "duplicates": totals["dropped_duplicate"],
        "delivery_ratio": delivered / expected if expected else 0.0,
    }


def scenario_storm(verbose: bool = False) -> Result:
    """12-node grid: the limiter must stay out of the way until it is needed.

    Three runs:

    * baseline     — no limiter, heavy load: what an unprotected mesh costs.
    * default, mild — the shipped defaults on a normal chat load: the limiter
      must not fire at all, or it would be dropping messages for no reason.
    * strict, heavy — a deliberately tight budget under 5x overload: this is
      the tradeoff, quantified.
    """
    r = Result("storm", "flood suppression: silent until needed, bounded after")
    baseline = _storm_run(FloodPolicy.unlimited(), 40)
    mild_unlimited = _storm_run(FloodPolicy.unlimited(), 8)
    mild_default = _storm_run(FloodPolicy(), 8)
    strict = _storm_run(
        FloodPolicy(
            enabled=True,
            rate=30.0,
            burst=15.0,
            per_source_rate=10.0,
            per_source_burst=6.0,
        ),
        40,
    )

    r.check(
        "default policy is silent on a normal load",
        mild_default["suppressed"] == 0
        and mild_default["frames"] == mild_unlimited["frames"]
        and mild_default["delivery_ratio"] == 1.0,
        f"{mild_default['messages']} broadcasts, "
        f"{mild_default['frames']} frames, "
        f"{_pct(mild_default['delivered'], mild_default['expected'])} delivered, "
        f"{mild_default['suppressed']} suppressed",
    )

    saved = 1.0 - (strict["frames"] / max(1, baseline["frames"]))
    r.check(
        "under overload, relay work drops sharply",
        saved >= 0.5,
        f"{baseline['frames']} -> {strict['frames']} frames "
        f"for {baseline['messages']} broadcasts ({saved * 100:.0f}% less)",
    )
    r.check(
        "suppression is counted, not silent",
        strict["suppressed"] > 0,
        f"{strict['suppressed']} relayed broadcasts dropped",
    )
    r.check(
        "broadcast still reaches most nodes",
        strict["delivery_ratio"] >= 0.5,
        f"{_pct(strict['delivered'], strict['expected'])} of nodes reached",
    )
    r.check(
        "duplicate suppression still does the heavy lifting",
        baseline["duplicates"] > baseline["messages"],
        f"{baseline['duplicates']} duplicates dropped by the dedup cache",
    )

    r.metrics = {
        "baseline_frames": baseline["frames"],
        "mild_frames": mild_default["frames"],
        "strict_frames": strict["frames"],
        "frames_saved_pct": round(saved * 100, 1),
        "baseline_delivery_pct": round(baseline["delivery_ratio"] * 100, 1),
        "strict_delivery_pct": round(strict["delivery_ratio"] * 100, 1),
        "strict_suppressed": strict["suppressed"],
    }
    r.notes.append(
        "broadcast is best effort: a tight budget trades reach for airtime, "
        "and the default budget never engages on chat-scale traffic"
    )
    return r


SCENARIOS: Dict[str, Callable[..., Result]] = {
    "line": scenario_line,
    "triangle": scenario_triangle,
    "ttl": scenario_ttl,
    "loss": scenario_loss_retransmit,
    "auth": scenario_auth,
    "reconnect": scenario_reconnect,
    "partition": scenario_partition,
    "storm": scenario_storm,
    "relay": scenario_relay_retry,
}


def run_scenario(name: str, verbose: bool = False) -> Result:
    fn = SCENARIOS[name]
    try:
        return fn(verbose=verbose)
    except Exception as exc:  # noqa: BLE001 - a crashed scenario is a failure
        r = Result(name, getattr(fn, "__doc__", "").strip().splitlines()[0])
        r.check("scenario completed", False, f"{type(exc).__name__}: {exc}")
        return r

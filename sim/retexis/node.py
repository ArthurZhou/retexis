"""One simulated node: the same state machine the Rust router runs.

Everything here is a direct transcription of `crates/retexis-routing/src/router.rs`.
When you change the protocol, change it in both places: the cross-language wire
test only covers the encoding, the scenarios cover the behaviour.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from . import wire


@dataclass
class RetryPolicy:
    max_attempts: int = 4
    base: float = 0.30
    max_interval: float = 5.0


@dataclass
class FloodPolicy:
    """Token buckets for relayed broadcasts. `enabled=False` shows the storm."""

    enabled: bool = True
    rate: float = 200.0
    burst: float = 100.0
    per_source_rate: float = 40.0
    per_source_burst: float = 30.0

    @classmethod
    def unlimited(cls) -> "FloodPolicy":
        return cls(enabled=False)


class Bucket:
    __slots__ = ("tokens", "last")

    def __init__(self, burst: float, now: float):
        self.tokens = burst
        self.last = now

    def take(self, rate: float, burst: float, now: float) -> bool:
        elapsed = max(0.0, now - self.last)
        self.tokens = min(burst, self.tokens + elapsed * rate)
        self.last = now
        if self.tokens >= 1.0:
            self.tokens -= 1.0
            return True
        return False


@dataclass
class Link:
    """One direction of a bidirectional link, mirroring one `TcpLink`."""

    link_id: int
    peer: "Node"
    peer_link_id: int
    initiator: bool
    latency: float
    loss: float
    up: bool = True
    peer_id: Optional[bytes] = None
    last_hello_reply: Optional[float] = None

    def __repr__(self) -> str:
        return f"<link {self.link_id}->{self.peer.name}>"


@dataclass
class Delivery:
    t: float
    from_id: bytes
    from_name: Optional[str]
    broadcast: bool
    channel: str
    payload: bytes
    hops: int
    msg_id: bytes
    ctype: int = 0

    @property
    def text(self) -> str:
        try:
            return self.payload.decode("utf-8")
        except UnicodeDecodeError:
            return f"<{len(self.payload)} bytes>"


@dataclass
class Pending:
    env: wire.Envelope
    attempts: int
    next_at: float
    started: float


def _link_key(a: int, b: int):
    """Order-independent key for one physical link, from its two local ids."""
    return (a, b) if a <= b else (b, a)


STAT_KEYS = (
    "sent",
    "delivered",
    "forwarded",
    "flooded",
    "dropped_duplicate",
    "dropped_ttl",
    "dropped_no_route",
    "dropped_malformed",
    "frames_in",
    "frames_out",
    "bytes_in",
    "bytes_out",
    "retransmits",
    "acked",
    "delivery_failed",
    "acks_sent",
    "duplicate_acks",
    "auth_rejected",
    "flood_suppressed",
    "duplicate_links",
)


class Node:
    def __init__(
        self,
        sim,
        name: str,
        node_id: Optional[bytes] = None,
        psk: Optional[bytes] = None,
        ttl: int = 8,
        announce: float = 0.5,
        route_timeout: float = 3.0,
        dedup_ttl: float = 60.0,
        retry: Optional[RetryPolicy] = None,
        flood: Optional[FloodPolicy] = None,
    ):
        self.sim = sim
        self.name = name
        self.id = node_id or wire.new_id()
        self.psk = psk
        self.ttl = ttl
        self.announce_interval = announce
        self.route_timeout = route_timeout
        self.dedup_ttl = dedup_ttl
        self.retry = retry or RetryPolicy()
        self.flood_policy = flood or FloodPolicy()

        self.links: Dict[int, Link] = {}
        self.table: Dict[bytes, dict] = {}
        # A node knows its own name: without this, its own broadcasts show up
        # in the inbox as a hex id instead of a name.
        self.names: Dict[bytes, str] = {self.id: name}
        self.dedup: Dict[bytes, float] = {}
        self.pending: Dict[bytes, Pending] = {}
        self.inbox: List[Delivery] = []
        self.stats = {k: 0 for k in STAT_KEYS}
        self.alive = True

        # flood limiter
        self._global_bucket = Bucket(self.flood_policy.burst, sim.now)
        self._source_buckets: Dict[bytes, Bucket] = {}

        self.next_announce = sim.now + announce
        self.sim.schedule_tick(self)

    # ------------------------------------------------------------- helpers

    def short(self) -> str:
        return self.id[:4].hex()

    def who(self, node_id: bytes) -> str:
        name = self.names.get(node_id, "")
        return f"{name}/{node_id[:4].hex()}" if name else node_id[:4].hex()

    def note(self, tag: str, detail: str) -> None:
        self.sim.log_event(self.sim.now, self.name, tag, detail)

    def neighbours(self) -> List["Node"]:
        return [l.peer for l in self.links.values()]

    def routes(self) -> Dict[bytes, dict]:
        return dict(self.table)

    # ---------------------------------------------------------- link events

    def on_link_up(self, link: Link) -> None:
        if not self.alive:
            return
        self.links[link.link_id] = link
        self._send_hello(link.link_id)
        self.note("+ link up", link.peer.name)

    def on_link_down(self, link_id: int, reason: str = "link down") -> None:
        link = self.links.pop(link_id, None)
        if link is None:
            return
        link.up = False
        dead = [dst for dst, e in self.table.items() if e["next_hop"] == link_id]
        for dst in dead:
            del self.table[dst]
            self.note("> route -", f"{self.who(dst)} (link down)")
        self.note("- link down", f"{link.peer.name} ({reason})")

    def _shutdown_link(self, link_id: int, reason: str) -> None:
        """Local teardown that also tears down the peer's side, like TCP."""
        link = self.links.get(link_id)
        if link is None:
            return
        peer = link.peer
        peer_link_id = link.peer_link_id
        self.on_link_down(link_id, reason)
        if peer_link_id in peer.links:
            peer.on_link_down(peer_link_id, f"{self.name} closed the link")

    def go_down(self, reason: str = "process died") -> None:
        """Simulate a crashed node: no cleanup, links just stop working."""
        self.alive = False
        for link_id in list(self.links):
            link = self.links.pop(link_id)
            link.up = False
            peer = link.peer
            if link.peer_link_id in peer.links:
                peer.on_link_down(link.peer_link_id, f"{self.name} {reason}")
        # A dead process has no state left. Keeping the table would show routes
        # on a node that cannot possibly use them.
        self.table.clear()
        self.pending.clear()
        self.note("x down", reason)

    def come_back(self) -> None:
        self.alive = True
        self.table.clear()
        self.dedup.clear()
        self.pending.clear()
        self.note("+ up", "restarted")

    # ------------------------------------------------------------- inbound

    def receive(self, link_id: int, sealed: bytes) -> None:
        if not self.alive:
            return
        link = self.links.get(link_id)
        if link is None or not link.up:
            return

        self.stats["frames_in"] += 1
        self.stats["bytes_in"] += len(sealed)

        claims_auth = len(sealed) > 4 and bool(sealed[3] & wire.FLAG_AUTH)
        if self.psk is not None:
            if not claims_auth:
                self._auth_reject(link, "unsigned packet on an authenticated mesh")
                return
            try:
                body = wire.unseal(self.psk, sealed)
            except wire.AuthError:
                self._auth_reject(link, "bad tag")
                return
        else:
            if claims_auth:
                self._auth_reject(link, "signed packet but no key configured")
                return
            body = sealed

        try:
            env = wire.decode(body)
        except wire.WireError:
            self.stats["dropped_malformed"] += 1
            return

        if env.kind == wire.KIND_HELLO:
            self._on_hello(link_id, env)
        elif env.kind == wire.KIND_ROUTE:
            self._on_route_update(link_id, env)
        elif env.kind == wire.KIND_BYE:
            self._shutdown_link(link_id, "peer said bye")
        else:
            self._on_routable(link_id, env)

    def _auth_reject(self, link: Link, reason: str) -> None:
        self.stats["auth_rejected"] += 1
        self.note("! auth", f"rejected {link.peer.name} ({reason})")

    def _on_hello(self, link_id: int, env: wire.Envelope) -> None:
        peer_id = env.src
        if peer_id == self.id:
            self._shutdown_link(link_id, "connected to ourselves")
            return
        self.names[peer_id] = env.name

        link = self.links.get(link_id)
        if link is None:
            return
        is_new = link.peer_id != peer_id
        peer_link_id = env.link_id

        if is_new:
            # Keep exactly one link per peer, using a key both ends compute
            # identically: (min, max) of the two local link ids.
            new_key = _link_key(link_id, peer_link_id)
            best = None
            for lid, other in self.links.items():
                if lid == link_id or other.peer_id != peer_id:
                    continue
                if other.peer_link_id is None:
                    continue
                key = _link_key(lid, other.peer_link_id)
                best = key if best is None else min(best, key)
            if best is not None and best <= new_key:
                self.stats["duplicate_links"] += 1
                self.note("- dup link", f"closed {link.peer.name}")
                self._shutdown_link(link_id, "duplicate link")
                return
            losers = [
                lid
                for lid, other in self.links.items()
                if lid != link_id and other.peer_id == peer_id
            ]
            for lid in losers:
                self.stats["duplicate_links"] += 1
                self.note("- dup link", f"closed {self.links[lid].peer.name}")
                self._shutdown_link(lid, "duplicate link")

        if is_new:
            link.peer_id = peer_id
            link.peer_link_id = peer_link_id
            self._set_adjacent(peer_id, link_id)

        # Always answer a Hello with our own, even when we already know the
        # peer: it may have lost ours and cannot tell us.
        now = self.sim.now
        need_reply = link.last_hello_reply is None or (
            now - link.last_hello_reply
        ) >= 0.2
        if need_reply:
            link.last_hello_reply = now
            self._send_hello(link_id)

        if is_new:
            self._send_update_to(link_id)

    def _on_route_update(self, link_id: int, env: wire.Envelope) -> None:
        link = self.links.get(link_id)
        if link is None or link.peer_id is None:
            return
        sender = link.peer_id
        now = self.sim.now
        self._set_adjacent(sender, link_id)

        for node, metric, name in env.entries:
            if node == self.id:
                continue
            if name:
                # Advertisements are authoritative: a node may have been
                # renamed since the handshake, and the sender's own entry is
                # how everyone learns the new name.
                self.names[node] = name
            if node == sender:
                continue
            candidate = min(255, metric + 1)
            outcome = self._learn(node, candidate, link_id)
            if outcome == "new":
                self.note("> route +", f"{self.who(node)} via {link.peer.name} ({candidate} hop)")
            elif outcome == "improved":
                self.note("> route ~", f"{self.who(node)} via {link.peer.name} ({candidate} hops)")

    def _on_routable(self, link_id: int, env: wire.Envelope) -> None:
        now = self.sim.now
        if env.msg_id in self.dedup:
            self.stats["dropped_duplicate"] += 1
            if env.dst == self.id:
                # Retry means our ack was lost: re-ack, never re-deliver.
                if env.wants_ack():
                    self.stats["duplicate_acks"] += 1
                    self._send_ack(env)
                return
            # As a relay, a duplicate unicast is a retry whose earlier forward
            # went nowhere. Forward it again; broadcasts stay suppressed.
            if env.dst is None:
                return
            self._forward_out(env, link_id)
            return
        self.dedup[env.msg_id] = now

        for_me = env.dst is None or env.dst == self.id
        if for_me:
            if env.kind == wire.KIND_ACK:
                self._on_ack(env.src, env.acked)
            else:
                self._deliver(env)
                if env.wants_ack() and env.dst is not None:
                    self._send_ack(env)

        if env.dst is not None and for_me:
            return
        self._forward_out(env, link_id)

    # ----------------------------------------------------------- outbound

    def _forward_out(self, env: wire.Envelope, from_link: Optional[int]) -> None:
        if env.ttl == 0:
            self.stats["dropped_ttl"] += 1
            return
        wire_env = env.clone()
        wire_env.ttl -= 1
        wire_env.hops = min(255, wire_env.hops + 1)

        if wire_env.dst is None:
            self._flood(wire_env, from_link, relayed=from_link is not None)
        else:
            self._forward_unicast(wire_env, wire_env.dst, from_link)

    def _forward_unicast(
        self, env: wire.Envelope, dst: bytes, from_link: Optional[int]
    ) -> None:
        entry = self.table.get(dst)
        if entry is None:
            self._flood(env, from_link, relayed=from_link is not None)
            return
        next_hop = entry["next_hop"]
        if from_link is not None and next_hop == from_link:
            self.stats["dropped_no_route"] += 1
            return
        if self._send_via(next_hop, env):
            self.stats["forwarded"] += 1

    def _flood(
        self, env: wire.Envelope, except_link: Optional[int], relayed: bool
    ) -> None:
        if relayed and not self._flood_allowed(env.src):
            self.stats["flood_suppressed"] += 1
            return
        targets = [
            lid
            for lid, l in self.links.items()
            if lid != except_link and l.peer_id is not None
        ]
        sent = 0
        for lid in targets:
            if self._send_via(lid, env):
                sent += 1
        if sent:
            self.stats["flooded"] += 1

    def _flood_allowed(self, src: bytes) -> bool:
        pol = self.flood_policy
        if not pol.enabled:
            return True
        now = self.sim.now
        if not self._global_bucket.take(pol.rate, pol.burst, now):
            return False
        bucket = self._source_buckets.get(src)
        if bucket is None:
            bucket = Bucket(pol.per_source_burst, now)
            self._source_buckets[src] = bucket
        return bucket.take(pol.per_source_rate, pol.per_source_burst, now)

    def _send_via(self, link_id: int, env: wire.Envelope) -> bool:
        link = self.links.get(link_id)
        if link is None or not link.up:
            return False
        stamped = env.clone()
        if self.psk is not None:
            stamped.flags |= wire.FLAG_AUTH
        try:
            raw = wire.encode(stamped)
        except wire.WireError:
            return False
        sealed = wire.seal(self.psk, raw) if self.psk is not None else raw
        self.stats["frames_out"] += 1
        self.stats["bytes_out"] += len(sealed)
        self.sim.transmit(link, sealed)
        return True

    def _deliver(self, env: wire.Envelope) -> None:
        self.stats["delivered"] += 1
        self.inbox.append(
            Delivery(
                t=self.sim.now,
                from_id=env.src,
                from_name=self.names.get(env.src) or None,
                broadcast=env.dst is None,
                channel=env.channel,
                payload=env.payload,
                hops=env.hops,
                msg_id=env.msg_id,
                ctype=env.ctype,
            )
        )

    # -------------------------------------------------------- reliability

    def _send_ack(self, env: wire.Envelope) -> None:
        ack = wire.ack_env(self.id, env.src, env.msg_id, ttl=self.ttl)
        self.stats["acks_sent"] += 1
        self._forward_out(ack, None)

    def _on_ack(self, from_id: bytes, acked: bytes) -> None:
        pending = self.pending.pop(acked, None)
        if pending is None:
            return
        self.stats["acked"] += 1
        rtt_ms = int((self.sim.now - pending.started) * 1000)
        self.note(
            "v delivered",
            f"{acked[:4].hex()} in {rtt_ms}ms ({pending.attempts} attempt"
            f"{'' if pending.attempts == 1 else 's'})",
        )

    def _retransmit_due(self, now: float) -> None:
        for msg_id in [k for k, p in self.pending.items() if p.next_at <= now]:
            pending = self.pending.get(msg_id)
            if pending is None:
                continue
            if pending.attempts >= self.retry.max_attempts:
                del self.pending[msg_id]
                self.stats["delivery_failed"] += 1
                self.note("x failed", f"{msg_id[:4].hex()} after {pending.attempts} attempts")
                continue
            shift = min(6, max(0, pending.attempts - 1))
            delay = min(self.retry.max_interval, self.retry.base * (2 ** shift))
            pending.attempts += 1
            pending.next_at = now + delay
            self.stats["retransmits"] += 1
            self._forward_out(pending.env, None)

    # ------------------------------------------------------ control plane

    def _send_hello(self, link_id: int) -> None:
        if link_id not in self.links:
            return
        self._send_via(link_id, wire.hello_env(self.id, self.name, link_id))

    def _send_update_to(self, link_id: int) -> None:
        link = self.links.get(link_id)
        if link is None or link.peer_id is None:
            return
        entries = []
        entries.append((self.id, 0, self.name))
        for dst, e in self.table.items():
            if dst == self.id or e["next_hop"] == link_id or e["metric"] >= 255:
                continue
            entries.append((dst, e["metric"], self.names.get(dst, "")))
        entries.sort(key=lambda x: x[1])
        self._send_via(link_id, wire.route_env(self.id, entries))

    def announce(self) -> None:
        for link_id in list(self.links):
            link = self.links[link_id]
            if link.peer_id is None:
                # Hello is idempotent: retry until the peer identifies itself.
                # One lost hello must not leave the link half-dead forever.
                self._send_hello(link_id)
            else:
                self._send_update_to(link_id)

    def _set_adjacent(self, node_id: bytes, link_id: int) -> bool:
        now = self.sim.now
        e = self.table.get(node_id)
        changed = e is None or e["next_hop"] != link_id or e["metric"] != 1
        self.table[node_id] = {"next_hop": link_id, "metric": 1, "last_seen": now}
        if changed:
            self.note("> route +", f"{self.who(node_id)} via link {link_id} (1 hop)")
        return changed

    def _learn(self, dst: bytes, metric: int, via: int) -> str:
        if metric >= 255:
            return "ignored"
        now = self.sim.now
        e = self.table.get(dst)
        if e is None:
            self.table[dst] = {"next_hop": via, "metric": metric, "last_seen": now}
            return "new"
        if e["next_hop"] == via:
            improved = metric < e["metric"]
            e["metric"] = metric
            e["last_seen"] = now
            return "improved" if improved else "refreshed"
        if metric < e["metric"]:
            e["next_hop"] = via
            e["metric"] = metric
            e["last_seen"] = now
            return "improved"
        return "ignored"

    def tick(self) -> None:
        if not self.alive:
            return
        now = self.sim.now
        self._retransmit_due(now)
        if now >= self.next_announce:
            self.announce()
            self._sweep(now)
            self.next_announce = now + self.announce_interval

    def _sweep(self, now: float) -> None:
        expired = [
            dst
            for dst, e in self.table.items()
            if now - e["last_seen"] > self.route_timeout
        ]
        for dst in expired:
            del self.table[dst]
            self.note("> route -", f"{self.who(dst)} (timeout)")
        stale = [k for k, t in self.dedup.items() if now - t > self.dedup_ttl]
        for k in stale:
            del self.dedup[k]

    # ---------------------------------------------------------- app facing

    def broadcast(self, text: str, channel: str = "chat") -> wire.Envelope:
        env = wire.data_env(
            self.id, None, text.encode("utf-8"), channel=channel, ttl=self.ttl
        )
        self._local_send(env)
        return env

    def send_to(self, dst: Node, text: str, channel: str = "chat") -> wire.Envelope:
        env = wire.data_env(
            self.id, dst.id, text.encode("utf-8"), channel=channel, ttl=self.ttl
        )
        self._local_send(env)
        return env

    def send_reliable(self, dst: Node, text: str, channel: str = "chat") -> wire.Envelope:
        env = wire.data_env(
            self.id, dst.id, text.encode("utf-8"), channel=channel, ttl=self.ttl
        )
        env.flags |= wire.FLAG_ACK_REQ
        if dst.id != self.id:
            now = self.sim.now
            self.pending[env.msg_id] = Pending(
                env=env.clone(),
                attempts=1,
                next_at=now + self.retry.base,
                started=now,
            )
        self._local_send(env)
        return env

    def _local_send(self, env: wire.Envelope) -> None:
        self.stats["sent"] += 1
        self.dedup[env.msg_id] = self.sim.now
        for_me = env.dst is None or env.dst == self.id
        if for_me:
            self._deliver(env)
        if env.dst is not None and for_me:
            return
        self._forward_out(env, None)

    # -------------------------------------------------------------- metrics

    def received(self, predicate=None) -> List[Delivery]:
        if predicate is None:
            return list(self.inbox)
        return [d for d in self.inbox if predicate(d)]

    def summary(self) -> dict:
        return {
            "name": self.name,
            "id": self.short(),
            "links": len(self.links),
            "routes": len(self.table),
            "delivered": self.stats["delivered"],
            "forwarded": self.stats["forwarded"],
            "frames_out": self.stats["frames_out"],
            "flood_suppressed": self.stats["flood_suppressed"],
            "retransmits": self.stats["retransmits"],
            "auth_rejected": self.stats["auth_rejected"],
        }

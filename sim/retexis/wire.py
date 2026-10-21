"""Byte-exact Python implementation of the retexis v2 wire format.

This is not a re-invention: it is the same format the Rust implementation
writes, down to the byte. `sim/golden.py` emits fixtures from this module and
`crates/retexis-core/tests/python_compat.rs` asserts that the Rust encoder
produces identical bytes and can decode them back. If the two ever drift, that
test fails.

Layout (see crates/retexis-core/src/envelope.rs for the authoritative table)::

    0   2  magic "RX"
    2   1  version (2)
    3   1  flags     0x01 authenticated, 0x02 wants ack
    4   1  ttl
    5   1  hops
    6   1  dst_kind  0 broadcast, 1 unicast
    7   1  body_kind 0 data, 1 hello, 2 route, 3 bye, 4 ack
    8  16  message id
   24  16  source node id
   40  16  destination node id (zeros when broadcast)
   56   8  created_at, unix millis, big endian
   64   -  body
    -  16  auth tag (only when flags & 0x01), HMAC-SHA256 truncated
"""

from __future__ import annotations

import hashlib
import hmac
import os
import struct
from dataclasses import dataclass, field
from typing import List, Optional, Tuple

MAGIC = b"RX"
VERSION = 2
HEADER_LEN = 64
MAX_PAYLOAD = 60 * 1024
MAX_NAME = 48
TAG_LEN = 16

FLAG_AUTH = 0x01
FLAG_ACK_REQ = 0x02

KIND_DATA = 0
KIND_HELLO = 1
KIND_ROUTE = 2
KIND_BYE = 3
KIND_ACK = 4

KIND_NAMES = {
    KIND_DATA: "data",
    KIND_HELLO: "hello",
    KIND_ROUTE: "route",
    KIND_BYE: "bye",
    KIND_ACK: "ack",
}

ZERO_ID = b"\x00" * 16


class WireError(Exception):
    """The bytes are not a valid envelope."""


class AuthError(Exception):
    """The authentication tag is missing or wrong."""


def key_from_passphrase(passphrase: str) -> bytes:
    """Same derivation as `Sealer::from_passphrase` on the Rust side."""
    return hashlib.sha256(passphrase.encode("utf-8")).digest()


def seal(key: bytes, data: bytes) -> bytes:
    return data + hmac.new(key, data, hashlib.sha256).digest()[:TAG_LEN]


def unseal(key: bytes, sealed: bytes) -> bytes:
    if len(sealed) < TAG_LEN:
        raise AuthError("sealed blob shorter than the tag")
    body, tag = sealed[:-TAG_LEN], sealed[-TAG_LEN:]
    expect = hmac.new(key, body, hashlib.sha256).digest()[:TAG_LEN]
    if not hmac.compare_digest(tag, expect):
        raise AuthError("bad tag")
    return body


def truncate_name(s: str, limit: int = MAX_NAME) -> bytes:
    """Largest UTF-8 char boundary <= limit, matching Rust's truncate_utf8."""
    raw = s.encode("utf-8")
    if len(raw) <= limit:
        return raw
    for end in range(limit, -1, -1):
        try:
            return raw[:end].decode("utf-8").encode("utf-8")
        except UnicodeDecodeError:
            continue
    return b""


def _id(value: bytes) -> bytes:
    if len(value) != 16:
        raise WireError("node/message ids must be 16 bytes")
    return value


def new_id() -> bytes:
    return os.urandom(16)


@dataclass
class Envelope:
    """A flat, fat envelope. Simplicity beats cleverness in a simulator."""

    src: bytes
    dst: Optional[bytes] = None  # None means broadcast
    ttl: int = 8
    hops: int = 0
    flags: int = 0
    kind: int = KIND_DATA
    msg_id: bytes = field(default_factory=new_id)
    created_ms: int = 0
    # body: data
    channel: str = ""
    ctype: int = 0
    payload: bytes = b""
    # body: hello
    name: str = ""
    link_id: int = 0
    # body: route
    entries: List[Tuple[bytes, int, str]] = field(default_factory=list)
    # body: ack
    acked: bytes = ZERO_ID

    # -- helpers ---------------------------------------------------------
    @property
    def broadcast(self) -> bool:
        return self.dst is None

    def wants_ack(self) -> bool:
        return bool(self.flags & FLAG_ACK_REQ)

    def needs_auth(self) -> bool:
        return bool(self.flags & FLAG_AUTH)

    def clone(self) -> "Envelope":
        return Envelope(
            src=self.src,
            dst=self.dst,
            ttl=self.ttl,
            hops=self.hops,
            flags=self.flags,
            kind=self.kind,
            msg_id=self.msg_id,
            created_ms=self.created_ms,
            channel=self.channel,
            ctype=self.ctype,
            payload=self.payload,
            name=self.name,
            link_id=self.link_id,
            entries=list(self.entries),
            acked=self.acked,
        )

    def __repr__(self) -> str:
        dest = "*" if self.dst is None else self.dst[:4].hex()
        kind = KIND_NAMES.get(self.kind, "?")
        return (
            f"<{kind} {self.msg_id[:4].hex()} {self.src[:4].hex()}->{dest} "
            f"ttl={self.ttl} hops={self.hops}>"
        )


# -- constructors --------------------------------------------------------


def data_env(src, dst, payload, channel="chat", ctype=0, ttl=8, created_ms=0) -> Envelope:
    return Envelope(
        src=src,
        dst=dst,
        ttl=ttl,
        kind=KIND_DATA,
        channel=channel,
        ctype=ctype,
        payload=payload,
        created_ms=created_ms,
    )


def hello_env(src, name, link_id=0, ttl=1, created_ms=0) -> Envelope:
    return Envelope(
        src=src,
        ttl=ttl,
        kind=KIND_HELLO,
        name=name,
        link_id=link_id,
        created_ms=created_ms,
    )


def route_env(src, entries, ttl=1, created_ms=0) -> Envelope:
    return Envelope(src=src, ttl=ttl, kind=KIND_ROUTE, entries=entries, created_ms=created_ms)


def ack_env(src, dst, acked, ttl=8, created_ms=0) -> Envelope:
    return Envelope(src=src, dst=dst, ttl=ttl, kind=KIND_ACK, acked=acked, created_ms=created_ms)


def bye_env(src, ttl=1, created_ms=0) -> Envelope:
    return Envelope(src=src, ttl=ttl, kind=KIND_BYE, created_ms=created_ms)


# -- codec ---------------------------------------------------------------


def encode(env: Envelope) -> bytes:
    out = bytearray()
    out += MAGIC
    out.append(VERSION)
    out.append(env.flags & 0xFF)
    out.append(env.ttl & 0xFF)
    out.append(env.hops & 0xFF)
    out.append(0 if env.dst is None else 1)
    out.append(env.kind)
    out += _id(env.msg_id)
    out += _id(env.src)
    out += ZERO_ID if env.dst is None else _id(env.dst)
    out += struct.pack(">Q", env.created_ms)
    if len(out) != HEADER_LEN:
        raise WireError("header is not 64 bytes")

    if env.kind == KIND_DATA:
        ch = env.channel.encode("utf-8")
        if len(ch) > 255:
            raise WireError("channel name too long")
        if len(env.payload) > MAX_PAYLOAD:
            raise WireError("payload too large")
        out.append(len(ch))
        out += ch
        out.append(env.ctype & 0xFF)
        out += struct.pack(">I", len(env.payload))
        out += env.payload
    elif env.kind == KIND_HELLO:
        name = truncate_name(env.name)
        out.append(len(name))
        out += name
        out += struct.pack(">Q", env.link_id)
    elif env.kind == KIND_ROUTE:
        if len(env.entries) > 0xFFFF:
            raise WireError("too many route entries")
        out += struct.pack(">H", len(env.entries))
        for node, metric, name in env.entries:
            out += _id(node)
            out.append(metric & 0xFF)
            nb = truncate_name(name)
            out.append(len(nb))
            out += nb
    elif env.kind == KIND_ACK:
        out += _id(env.acked)
    elif env.kind == KIND_BYE:
        pass
    else:
        raise WireError(f"unknown body kind {env.kind}")

    return bytes(out)


class _Cursor:
    def __init__(self, buf: bytes, pos: int):
        self.buf = buf
        self.pos = pos

    def take(self, n: int) -> bytes:
        if self.pos + n > len(self.buf):
            raise WireError("truncated")
        out = self.buf[self.pos:self.pos + n]
        self.pos += n
        return out

    def u8(self) -> int:
        return self.take(1)[0]

    def u16(self) -> int:
        return struct.unpack(">H", self.take(2))[0]

    def u32(self) -> int:
        return struct.unpack(">I", self.take(4))[0]

    def text(self, n: int) -> str:
        try:
            return self.take(n).decode("utf-8")
        except UnicodeDecodeError as exc:
            raise WireError("invalid utf-8 in a string field") from exc


def decode(buf: bytes) -> Envelope:
    if len(buf) < HEADER_LEN:
        raise WireError(f"packet too short: {len(buf)} bytes")
    if buf[0:2] != MAGIC:
        raise WireError("bad magic")
    if buf[2] != VERSION:
        raise WireError(f"unsupported wire version {buf[2]}")

    flags = buf[3]
    ttl = buf[4]
    hops = buf[5]
    dst_kind = buf[6]
    kind = buf[7]
    msg_id = buf[8:24]
    src = buf[24:40]
    dst = None if dst_kind == 0 else buf[40:56]
    if dst_kind not in (0, 1):
        raise WireError(f"unknown destination kind {dst_kind}")
    created_ms = struct.unpack(">Q", buf[56:64])[0]

    cur = _Cursor(buf, HEADER_LEN)
    env = Envelope(
        src=src,
        dst=dst,
        ttl=ttl,
        hops=hops,
        flags=flags,
        kind=kind,
        msg_id=msg_id,
        created_ms=created_ms,
    )

    if kind == KIND_DATA:
        env.channel = cur.text(cur.u8())
        env.ctype = cur.u8()
        plen = cur.u32()
        if plen > MAX_PAYLOAD:
            raise WireError("payload too large")
        env.payload = cur.take(plen)
    elif kind == KIND_HELLO:
        env.name = cur.text(cur.u8())
        env.link_id = struct.unpack(">Q", cur.take(8))[0]
    elif kind == KIND_ROUTE:
        count = cur.u16()
        entries = []
        for _ in range(count):
            node = cur.take(16)
            metric = cur.u8()
            name = cur.text(cur.u8())
            entries.append((node, metric, name))
        env.entries = entries
    elif kind == KIND_ACK:
        env.acked = cur.take(16)
    elif kind == KIND_BYE:
        pass
    else:
        raise WireError(f"unknown body kind {kind}")

    return env

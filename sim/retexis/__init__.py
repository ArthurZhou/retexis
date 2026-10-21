"""retexis simulator: a protocol-level model of the mesh, in pure Python.

Why a simulator when there is a Rust implementation? Because the interesting
questions are statistical: does the mesh converge in a 5x5 grid, how many
packets does a flood storm cost, what happens when 30% of packets are lost,
does the duplicate-link rule oscillate when both sides dial each other. Those
questions take seconds to answer here and minutes-plus to answer with real
sockets.

The simulator implements the *same* protocol, byte for byte (`wire.py` is
checked against the Rust encoder by a test), so a scenario that fails here is
a real protocol bug, not a simulation artefact.
"""

from . import wire
from .net import Sim, Supervisor
from .node import Delivery, Node, RetryPolicy, FloodPolicy

__all__ = [
    "wire",
    "Sim",
    "Supervisor",
    "Node",
    "Delivery",
    "RetryPolicy",
    "FloodPolicy",
]

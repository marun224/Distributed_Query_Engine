"""Distributed writes (PLAN §4.6, Phase 5): workers write the data files, the coordinator
commits them in one transaction."""

from icefleet.write.target import WriteTarget, write_target

__all__ = ["WriteTarget", "write_target"]

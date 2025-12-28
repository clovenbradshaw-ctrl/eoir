"""
Frame Registry

Frames are epistemic actors - selecting a frame is making a claim.

A frame is:
- a named, versioned interpretation policy
- defining definitions, thresholds, exclusions, synthesis preferences
- itself subject to provenance and disagreement

The registry provides:
- Frame resolution (FrameRef -> FrameDefinition)
- Version management
- Frame comparison
- Frame validation
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Protocol
from abc import ABC, abstractmethod

from eoql.ir.model import FrameRef


@dataclass(frozen=True)
class FrameDefinition:
    """
    A fully resolved frame definition.

    Contains all the interpretation rules that determine
    how claims are evaluated under this frame.
    """

    frame_id: str
    version: str
    name: str
    description: Optional[str] = None

    # Interpretation configuration
    config: Dict[str, Any] = field(default_factory=dict)

    # Provenance
    created_at: Optional[datetime] = None
    created_by: Optional[str] = None
    supersedes: Optional[str] = None  # Previous version

    # Grounding reference
    grounding_ref: Optional[str] = None

    def get_threshold(self, key: str, default: float = 0.0) -> float:
        """Get a threshold value from frame config."""
        thresholds = self.config.get("thresholds", {})
        return thresholds.get(key, default)

    def get_definition(self, term: str) -> Optional[Dict[str, Any]]:
        """Get a term definition from frame config."""
        definitions = self.config.get("definitions", {})
        return definitions.get(term)

    def is_excluded(self, entity_type: str) -> bool:
        """Check if an entity type is excluded under this frame."""
        exclusions = self.config.get("exclusions", [])
        return entity_type in exclusions

    def get_synthesis_preference(self, key: str) -> Optional[str]:
        """Get synthesis preference setting."""
        preferences = self.config.get("synthesis_preferences", {})
        return preferences.get(key)


class FrameNotFoundError(Exception):
    """Raised when a referenced frame cannot be resolved."""

    def __init__(self, frame_ref: FrameRef):
        self.frame_ref = frame_ref
        msg = f"Frame not found: {frame_ref.frame_id}"
        if frame_ref.version:
            msg += f" (version: {frame_ref.version})"
        super().__init__(msg)


class FrameVersionConflictError(Exception):
    """Raised when frame versions conflict."""

    def __init__(self, frame_id: str, versions: List[str]):
        self.frame_id = frame_id
        self.versions = versions
        super().__init__(
            f"Multiple versions of frame '{frame_id}' found: {versions}"
        )


class FrameStore(Protocol):
    """Protocol for frame storage backends."""

    def get(self, frame_id: str, version: Optional[str] = None) -> Optional[FrameDefinition]:
        """Retrieve a frame by ID and optional version."""
        ...

    def list_versions(self, frame_id: str) -> List[str]:
        """List all versions of a frame."""
        ...

    def put(self, frame: FrameDefinition) -> None:
        """Store a frame definition."""
        ...

    def exists(self, frame_id: str, version: Optional[str] = None) -> bool:
        """Check if a frame exists."""
        ...


class InMemoryFrameStore:
    """In-memory frame store for testing and simple use cases."""

    def __init__(self) -> None:
        self._frames: Dict[str, Dict[str, FrameDefinition]] = {}
        # Initialize with default frame
        self._init_default_frame()

    def _init_default_frame(self) -> None:
        """Initialize the default frame that must always exist."""
        default = FrameDefinition(
            frame_id="F_default",
            version="latest",
            name="Default Frame",
            description="System default interpretation frame",
            config={
                "thresholds": {"certainty_minimum": 0.0},
                "definitions": {},
                "exclusions": [],
                "synthesis_preferences": {},
            },
            created_at=datetime.now(),
        )
        self.put(default)

    def get(self, frame_id: str, version: Optional[str] = None) -> Optional[FrameDefinition]:
        """Retrieve a frame by ID and optional version."""
        if frame_id not in self._frames:
            return None

        versions = self._frames[frame_id]

        if version is None or version == "latest":
            # Return the most recent version
            if "latest" in versions:
                return versions["latest"]
            # Otherwise return the highest version
            sorted_versions = sorted(versions.keys(), reverse=True)
            return versions[sorted_versions[0]] if sorted_versions else None

        return versions.get(version)

    def list_versions(self, frame_id: str) -> List[str]:
        """List all versions of a frame."""
        if frame_id not in self._frames:
            return []
        return list(self._frames[frame_id].keys())

    def put(self, frame: FrameDefinition) -> None:
        """Store a frame definition."""
        if frame.frame_id not in self._frames:
            self._frames[frame.frame_id] = {}
        self._frames[frame.frame_id][frame.version] = frame

    def exists(self, frame_id: str, version: Optional[str] = None) -> bool:
        """Check if a frame exists."""
        return self.get(frame_id, version) is not None


class FrameRegistry:
    """
    Central registry for frame resolution and management.

    EOQL must:
    - require a frame (even if default)
    - return the frame identity with every answer
    - allow frames themselves to be queried and compared
    """

    def __init__(self, store: Optional[FrameStore] = None) -> None:
        self._store: FrameStore = store or InMemoryFrameStore()
        self._cache: Dict[str, FrameDefinition] = {}

    def resolve(self, ref: FrameRef) -> FrameDefinition:
        """
        Resolve a FrameRef to its full definition.

        Raises:
            FrameNotFoundError: If the frame cannot be found.
        """
        cache_key = f"{ref.frame_id}:{ref.version or 'latest'}"

        if cache_key in self._cache:
            return self._cache[cache_key]

        frame = self._store.get(ref.frame_id, ref.version)
        if frame is None:
            raise FrameNotFoundError(ref)

        self._cache[cache_key] = frame
        return frame

    def register(self, frame: FrameDefinition) -> None:
        """Register a new frame definition."""
        self._store.put(frame)
        # Invalidate cache for this frame
        keys_to_remove = [k for k in self._cache if k.startswith(f"{frame.frame_id}:")]
        for k in keys_to_remove:
            del self._cache[k]

    def exists(self, ref: FrameRef) -> bool:
        """Check if a frame exists."""
        return self._store.exists(ref.frame_id, ref.version)

    def list_versions(self, frame_id: str) -> List[str]:
        """List all versions of a frame."""
        return self._store.list_versions(frame_id)

    def compare(
        self, ref1: FrameRef, ref2: FrameRef
    ) -> Dict[str, Any]:
        """
        Compare two frames and return their differences.

        Returns a dict with:
        - 'same': bool indicating if frames are identical
        - 'config_diff': differences in configuration
        - 'threshold_diff': differences in thresholds
        - 'definition_diff': differences in definitions
        """
        frame1 = self.resolve(ref1)
        frame2 = self.resolve(ref2)

        if frame1 == frame2:
            return {"same": True}

        result: Dict[str, Any] = {"same": False}

        # Compare thresholds
        t1 = frame1.config.get("thresholds", {})
        t2 = frame2.config.get("thresholds", {})
        if t1 != t2:
            result["threshold_diff"] = {
                "only_in_first": {k: v for k, v in t1.items() if k not in t2},
                "only_in_second": {k: v for k, v in t2.items() if k not in t1},
                "different": {
                    k: {"first": t1[k], "second": t2[k]}
                    for k in t1
                    if k in t2 and t1[k] != t2[k]
                },
            }

        # Compare definitions
        d1 = frame1.config.get("definitions", {})
        d2 = frame2.config.get("definitions", {})
        if d1 != d2:
            result["definition_diff"] = {
                "only_in_first": list(set(d1.keys()) - set(d2.keys())),
                "only_in_second": list(set(d2.keys()) - set(d1.keys())),
                "different": [k for k in d1 if k in d2 and d1[k] != d2[k]],
            }

        # Compare exclusions
        e1 = set(frame1.config.get("exclusions", []))
        e2 = set(frame2.config.get("exclusions", []))
        if e1 != e2:
            result["exclusion_diff"] = {
                "only_in_first": list(e1 - e2),
                "only_in_second": list(e2 - e1),
            }

        return result

    def get_default(self) -> FrameDefinition:
        """Get the default frame."""
        return self.resolve(FrameRef(frame_id="F_default", version="latest"))

    def clear_cache(self) -> None:
        """Clear the resolution cache."""
        self._cache.clear()

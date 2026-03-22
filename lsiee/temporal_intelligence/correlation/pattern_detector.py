"""Recurring event-pattern detection helpers."""

from __future__ import annotations

from collections import Counter, defaultdict
from statistics import mean, pstdev
from typing import Any, Dict, List


class PatternDetector:
    """Detect common temporal patterns across event streams."""

    def detect_sequences(
        self,
        events: List[Dict[str, Any]],
        max_gap: float = 60.0,
        min_count: int = 2,
        sequence_length: int = 3,
    ) -> List[Dict[str, Any]]:
        """Detect frequent fixed-length event sequences within a maximum gap."""
        ordered_events = sorted(events, key=lambda item: item["timestamp"])
        sequence_counts: Counter[tuple[str, ...]] = Counter()

        for start in range(len(ordered_events) - sequence_length + 1):
            sequence = [ordered_events[start]["event_type"]]
            previous_timestamp = float(ordered_events[start]["timestamp"])

            for offset in range(start + 1, len(ordered_events)):
                current = ordered_events[offset]
                if float(current["timestamp"]) - previous_timestamp > max_gap:
                    break

                sequence.append(current["event_type"])
                previous_timestamp = float(current["timestamp"])
                if len(sequence) == sequence_length:
                    sequence_counts[tuple(sequence)] += 1
                    break

        return [
            {"sequence": list(sequence), "count": count}
            for sequence, count in sequence_counts.items()
            if count >= min_count
        ]

    def detect_periodic_events(
        self,
        events: List[Dict[str, Any]],
        min_count: int = 3,
        max_relative_deviation: float = 0.2,
    ) -> List[Dict[str, Any]]:
        """Detect event types that recur at a near-constant interval."""
        timestamps_by_type: Dict[str, List[float]] = defaultdict(list)
        for event in sorted(events, key=lambda item: item["timestamp"]):
            timestamps_by_type[event["event_type"]].append(float(event["timestamp"]))

        periodic_patterns = []
        for event_type, timestamps in timestamps_by_type.items():
            if len(timestamps) < min_count:
                continue

            intervals = [
                timestamps[index + 1] - timestamps[index] for index in range(len(timestamps) - 1)
            ]
            avg_interval = mean(intervals)
            if avg_interval <= 0:
                continue

            deviation = pstdev(intervals) if len(intervals) > 1 else 0.0
            if deviation / avg_interval <= max_relative_deviation:
                periodic_patterns.append(
                    {
                        "event_type": event_type,
                        "interval_seconds": avg_interval,
                        "count": len(timestamps),
                    }
                )

        return periodic_patterns

    def detect_bursts(
        self,
        events: List[Dict[str, Any]],
        window_seconds: float = 30.0,
        min_events: int = 5,
    ) -> List[Dict[str, Any]]:
        """Detect dense bursts of events inside a sliding time window."""
        ordered_events = sorted(events, key=lambda item: item["timestamp"])
        bursts = []
        start = 0

        for end, event in enumerate(ordered_events):
            while (
                float(event["timestamp"]) - float(ordered_events[start]["timestamp"])
                > window_seconds
            ):
                start += 1

            window = ordered_events[start : end + 1]
            if len(window) >= min_events:
                bursts.append(
                    {
                        "start_time": float(window[0]["timestamp"]),
                        "end_time": float(window[-1]["timestamp"]),
                        "event_count": len(window),
                        "event_types": sorted({item["event_type"] for item in window}),
                    }
                )

        return bursts

    def detect_cascades(
        self,
        events: List[Dict[str, Any]],
        time_window: float = 60.0,
        min_children: int = 2,
    ) -> List[Dict[str, Any]]:
        """Detect one event type repeatedly followed by others in a short window."""
        ordered_events = sorted(events, key=lambda item: item["timestamp"])
        cascade_map: Dict[str, Counter[str]] = defaultdict(Counter)

        for index, event in enumerate(ordered_events):
            origin_type = event["event_type"]
            seen_children = set()
            for follower in ordered_events[index + 1 :]:
                delay = float(follower["timestamp"]) - float(event["timestamp"])
                if delay > time_window:
                    break
                if follower["event_type"] == origin_type:
                    continue
                seen_children.add(follower["event_type"])
            for child_type in seen_children:
                cascade_map[origin_type][child_type] += 1

        cascades = []
        for source_event, children in cascade_map.items():
            if len(children) < min_children:
                continue
            cascades.append(
                {
                    "source_event": source_event,
                    "triggered_events": dict(children),
                    "child_event_count": len(children),
                }
            )

        return cascades

    def detect_patterns(self, events: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """Run the full pattern-detection suite over an event stream."""
        return {
            "sequences": self.detect_sequences(events),
            "periodic_events": self.detect_periodic_events(events),
            "bursts": self.detect_bursts(events),
            "cascades": self.detect_cascades(events),
        }

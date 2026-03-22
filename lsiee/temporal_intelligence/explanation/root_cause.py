"""Root cause analysis for temporal intelligence."""

from __future__ import annotations

import sqlite3
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from lsiee.config import config, get_db_path
from lsiee.storage.schemas import configure_connection, initialize_database
from lsiee.temporal_intelligence.events import EventLogger


class RecommendationEngine:
    """Generate actionable recommendations for diagnosed issues."""

    def __init__(self):
        """Initialize recommendation rules."""
        self.rules = self._load_rules()

    def _load_rules(self) -> Dict[str, List[str]]:
        """Load built-in recommendation rules."""
        return {
            "system_slowdown": [
                "Identify and reschedule resource-intensive tasks away from peak usage hours",
                "Check whether multiple heavy jobs overlap in the same time window",
            ],
            "cpu_high": [
                "Check which process is consuming CPU",
                "Consider scheduling resource-intensive tasks during off-peak hours",
                "Investigate whether the process is stuck in a hot loop",
            ],
            "memory_pressure": [
                "Investigate potential memory leaks",
                "Restart or isolate the highest-memory process",
                "Consider increasing available RAM if the workload is expected",
            ],
            "anomaly_detected": [
                "Review the anomalous process behavior and recent system events",
                "Check for malware or unauthorized processes",
                "Monitor for continued anomalies before the next workload peak",
            ],
        }

    def recommend(self, issue_type: str, context: Dict[str, Any]) -> List[str]:
        """Return deduplicated recommendations for an issue and context."""
        recommendations = list(self.rules.get(issue_type, []))

        process_name = context.get("process_name")
        if process_name:
            recommendations.append(f"Specifically investigate {process_name}")

        if context.get("max_memory_percent", 0.0) >= 80.0:
            recommendations.extend(self.rules.get("memory_pressure", []))

        if context.get("max_cpu", 0.0) >= 80.0:
            recommendations.extend(self.rules.get("cpu_high", []))

        if context.get("correlated_event_types"):
            event_list = ", ".join(context["correlated_event_types"][:3])
            recommendations.append(f"Review correlated events around the incident: {event_list}")

        deduplicated = []
        seen = set()
        for recommendation in recommendations:
            if recommendation not in seen:
                deduplicated.append(recommendation)
                seen.add(recommendation)

        return deduplicated


class EvidenceGatherer:
    """Collect process, event, correlation, and historical evidence."""

    def __init__(self, db_path: Optional[Path] = None):
        """Initialize evidence access helpers."""
        self.db_path = Path(db_path) if db_path else get_db_path()
        schema = initialize_database(self.db_path)
        schema.disconnect()
        self.event_logger = EventLogger(self.db_path)

    def gather_evidence(
        self,
        issue_type: str,
        timestamp: float,
        window_seconds: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Gather all relevant evidence around an issue timestamp."""
        window = float(
            window_seconds or config.get("temporal_intelligence.explanation_window_seconds", 300.0)
        )
        return {
            "process_metrics": self._get_process_metrics(timestamp, window_seconds=window),
            "events": self._get_events(timestamp, window_seconds=window),
            "correlations": self._get_correlations(timestamp, window_seconds=window),
            "historical": self._get_historical_occurrences(
                issue_type,
                timestamp,
                window_seconds=window,
            ),
        }

    def _get_process_metrics(
        self,
        timestamp: float,
        window_seconds: float = 300.0,
        limit: int = 5,
    ) -> List[Dict[str, Any]]:
        """Get process metrics in the incident window."""
        start_time = timestamp - window_seconds
        end_time = timestamp + window_seconds

        with sqlite3.connect(self.db_path) as conn:
            configure_connection(conn)
            cursor = conn.execute(
                """
                SELECT name, pid,
                       AVG(cpu_percent) AS avg_cpu,
                       MAX(cpu_percent) AS max_cpu,
                       AVG(memory_percent) AS avg_memory_percent,
                       MAX(memory_percent) AS max_memory_percent,
                       AVG(memory_mb) AS avg_memory_mb,
                       MAX(memory_mb) AS max_memory_mb,
                       COUNT(*) AS sample_count
                FROM process_snapshots
                WHERE timestamp BETWEEN ? AND ?
                GROUP BY name, pid
                ORDER BY max_cpu DESC, max_memory_percent DESC, sample_count DESC
                LIMIT ?
                """,
                (start_time, end_time, limit),
            )
            return [dict(row) for row in cursor.fetchall()]

    def _get_events(self, timestamp: float, window_seconds: float = 300.0) -> List[Dict[str, Any]]:
        """Get logged events around the issue window."""
        return self.event_logger.get_events(
            start_time=timestamp - window_seconds,
            end_time=timestamp + window_seconds,
            limit=100,
        )

    def _get_correlations(
        self,
        timestamp: float,
        window_seconds: float = 300.0,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """Get stored correlations relevant to nearby event types."""
        nearby_events = self._get_events(timestamp, window_seconds=window_seconds)
        event_types = sorted({event["event_type"] for event in nearby_events})
        if len(event_types) < 2:
            return []

        placeholders = ",".join("?" for _ in event_types)
        params = [*event_types, *event_types, limit]
        with sqlite3.connect(self.db_path) as conn:
            configure_connection(conn)
            cursor = conn.execute(
                f"""
                SELECT event_type_a, event_type_b, support, confidence, lift,
                       occurrences, first_seen, last_seen, avg_delay_seconds
                FROM correlations
                WHERE event_type_a IN ({placeholders}) AND event_type_b IN ({placeholders})
                ORDER BY lift DESC, occurrences DESC
                LIMIT ?
                """,
                params,
            )
            return [dict(row) for row in cursor.fetchall()]

    def _get_historical_occurrences(
        self,
        issue_type: str,
        timestamp: float,
        window_seconds: float = 300.0,
        limit: int = 5,
    ) -> List[Dict[str, Any]]:
        """Find previous events or snapshots resembling the current issue."""
        start_time = timestamp - window_seconds

        with sqlite3.connect(self.db_path) as conn:
            configure_connection(conn)

            if issue_type in {"system_slowdown", "cpu_high"}:
                cursor = conn.execute(
                    """
                    SELECT timestamp, name, pid, cpu_percent, memory_percent
                    FROM process_snapshots
                    WHERE timestamp < ? AND cpu_percent >= 70
                    ORDER BY timestamp DESC
                    LIMIT ?
                    """,
                    (start_time, limit),
                )
                return [dict(row) for row in cursor.fetchall()]

            if issue_type == "memory_pressure":
                cursor = conn.execute(
                    """
                    SELECT timestamp, name, pid, cpu_percent, memory_percent
                    FROM process_snapshots
                    WHERE timestamp < ? AND memory_percent >= 80
                    ORDER BY timestamp DESC
                    LIMIT ?
                    """,
                    (start_time, limit),
                )
                return [dict(row) for row in cursor.fetchall()]

            cursor = conn.execute(
                """
                SELECT timestamp, event_type, source, severity, data
                FROM events
                WHERE timestamp < ? AND event_type = 'anomaly_detected'
                ORDER BY timestamp DESC
                LIMIT ?
                """,
                (start_time, limit),
            )
            rows = []
            for row in cursor.fetchall():
                parsed = dict(row)
                parsed["data"] = self.event_logger._deserialize_row(
                    {"data": row["data"], "tags": "[]"}
                )["data"]
                rows.append(parsed)
            return rows


class RootCauseAnalyzer:
    """Analyze root causes for user-facing issue explanations."""

    def __init__(
        self,
        db_path: Optional[Path] = None,
        evidence_gatherer: Optional[EvidenceGatherer] = None,
        recommendation_engine: Optional[RecommendationEngine] = None,
    ):
        """Initialize the root-cause analyzer."""
        self.db_path = Path(db_path) if db_path else get_db_path()
        self.evidence_gatherer = evidence_gatherer or EvidenceGatherer(self.db_path)
        self.recommendation_engine = recommendation_engine or RecommendationEngine()

    def explain_issue(self, issue: str, timestamp: float) -> Dict[str, Any]:
        """Dispatch to the appropriate issue-specific explanation flow."""
        issue_type = self._normalize_issue(issue)

        if issue_type == "system_slowdown":
            return self.explain_slowdown(timestamp)
        if issue_type == "cpu_high":
            return self._build_explanation(issue_type, timestamp)
        if issue_type == "memory_pressure":
            return self._build_explanation(issue_type, timestamp)
        if issue_type == "anomaly_detected":
            return self._build_explanation(issue_type, timestamp)

        raise ValueError(f"Unknown issue type: {issue}")

    def explain_slowdown(self, timestamp: float) -> Dict[str, Any]:
        """Explain a system slowdown at the provided timestamp."""
        return self._build_explanation("system_slowdown", timestamp)

    def _build_explanation(self, issue_type: str, timestamp: float) -> Dict[str, Any]:
        """Assemble evidence, root causes, and recommendations for an incident."""
        evidence_bundle = self.evidence_gatherer.gather_evidence(issue_type, timestamp)
        explanation = {
            "issue": issue_type,
            "timestamp": timestamp,
            "evidence": [],
            "root_causes": [],
            "recommendations": [],
        }

        process_metrics = evidence_bundle["process_metrics"]
        if process_metrics:
            explanation["evidence"].append(
                {"type": "process_metrics", "processes": process_metrics}
            )
            hottest = process_metrics[0]
            if float(hottest.get("max_cpu", 0.0) or 0.0) >= 50.0:
                explanation["root_causes"].append(
                    f"High CPU usage from {hottest['name']} (peak {hottest['max_cpu']:.1f}%)"
                )
            if float(hottest.get("max_memory_percent", 0.0) or 0.0) >= 80.0:
                explanation["root_causes"].append(
                    "High memory pressure from "
                    f"{hottest['name']} (peak {hottest['max_memory_percent']:.1f}%)"
                )

        events = evidence_bundle["events"]
        if events:
            explanation["evidence"].append({"type": "events", "events": events})
            important_events = [
                event for event in events if event["severity"] in {"WARNING", "ERROR", "CRITICAL"}
            ]
            if important_events:
                counts = Counter(event["event_type"] for event in important_events)
                dominant_event, occurrences = counts.most_common(1)[0]
                explanation["root_causes"].append(
                    "Warning-level activity included "
                    f"{dominant_event} ({occurrences} nearby event(s))"
                )

        correlations = evidence_bundle["correlations"]
        if correlations:
            explanation["evidence"].append({"type": "correlations", "correlations": correlations})
            strongest = correlations[0]
            explanation["root_causes"].append(
                "Known correlation observed between "
                f"{strongest['event_type_a']} and {strongest['event_type_b']} "
                f"(lift {strongest['lift']:.2f})"
            )

        historical = evidence_bundle["historical"]
        if historical:
            explanation["evidence"].append({"type": "historical", "occurrences": historical})
            explanation["root_causes"].append(
                "Similar "
                f"{issue_type.replace('_', ' ')} symptoms occurred {len(historical)} time(s) before"
            )

        if not explanation["root_causes"]:
            explanation["root_causes"].append(
                "Insufficient evidence was found for a confident diagnosis"
            )

        top_process = process_metrics[0] if process_metrics else {}
        correlated_event_types = []
        for correlation in correlations:
            correlated_event_types.extend(
                [correlation["event_type_a"], correlation["event_type_b"]]
            )

        explanation["recommendations"] = self.recommendation_engine.recommend(
            issue_type,
            {
                "process_name": top_process.get("name"),
                "max_cpu": float(top_process.get("max_cpu", 0.0) or 0.0),
                "max_memory_percent": float(top_process.get("max_memory_percent", 0.0) or 0.0),
                "correlated_event_types": sorted(set(correlated_event_types)),
            },
        )

        if not explanation["recommendations"]:
            explanation["recommendations"] = [
                "Collect more monitoring history and retry the explanation command"
            ]

        return explanation

    @staticmethod
    def _normalize_issue(issue: str) -> str:
        """Map free-form issue text into supported explanation categories."""
        normalized = issue.strip().lower()
        if "slow" in normalized or "slowdown" in normalized:
            return "system_slowdown"
        if "cpu" in normalized:
            return "cpu_high"
        if "memory" in normalized or "ram" in normalized:
            return "memory_pressure"
        if "anomaly" in normalized:
            return "anomaly_detected"
        return normalized.replace(" ", "_")


def parse_issue_timestamp(raw_timestamp: Optional[str]) -> float:
    """Parse ISO-8601 or raw epoch timestamps for the CLI."""
    if raw_timestamp is None:
        return datetime.now().timestamp()

    try:
        return float(raw_timestamp)
    except ValueError:
        return datetime.fromisoformat(raw_timestamp).timestamp()

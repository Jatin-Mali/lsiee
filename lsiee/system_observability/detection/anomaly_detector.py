"""Anomaly detection for process monitoring."""

from __future__ import annotations

import logging
from collections import defaultdict, deque
from typing import Any, Deque, Dict, Iterable, List

import numpy as np
from sklearn.ensemble import IsolationForest

logger = logging.getLogger(__name__)


class FeatureEngineer:
    """Engineer temporal process features for anomaly detection."""

    def compute_temporal_features(self, history: List[Dict[str, Any]]) -> Dict[str, float]:
        """Compute features from a process time series."""
        if len(history) < 2:
            return {
                "cpu_mean": 0.0,
                "cpu_std": 0.0,
                "cpu_max": 0.0,
                "cpu_trend": 0.0,
                "mem_mean": 0.0,
                "mem_std": 0.0,
                "mem_growth_rate": 0.0,
                "duration": 0.0,
            }

        ordered_history = sorted(history, key=lambda item: item["timestamp"])
        cpu_series = [float(item.get("cpu_percent", 0.0) or 0.0) for item in ordered_history]
        mem_series = [float(item.get("memory_mb", 0.0) or 0.0) for item in ordered_history]

        return {
            "cpu_mean": float(np.mean(cpu_series)),
            "cpu_std": float(np.std(cpu_series)),
            "cpu_max": float(np.max(cpu_series)),
            "cpu_trend": self._compute_trend(cpu_series),
            "mem_mean": float(np.mean(mem_series)),
            "mem_std": float(np.std(mem_series)),
            "mem_growth_rate": self._compute_growth_rate(mem_series),
            "duration": float(ordered_history[-1]["timestamp"] - ordered_history[0]["timestamp"]),
        }

    def _compute_trend(self, series: List[float]) -> float:
        """Compute a simple linear slope for the provided series."""
        if len(series) < 2:
            return 0.0

        x_axis = np.arange(len(series))
        slope, _intercept = np.polyfit(x_axis, np.asarray(series, dtype=float), deg=1)
        return float(slope)

    def _compute_growth_rate(self, series: List[float]) -> float:
        """Compute relative growth across the series."""
        if len(series) < 2 or series[0] == 0:
            return 0.0

        return float((series[-1] - series[0]) / series[0])


class AnomalyDetector:
    """Detect anomalous process behavior with Isolation Forest."""

    def __init__(
        self,
        contamination: float = 0.1,
        random_state: int = 42,
        min_samples: int = 25,
    ):
        """Initialize the anomaly detector."""
        self.model = IsolationForest(
            contamination=contamination,
            random_state=random_state,
            n_estimators=100,
        )
        self.is_fitted = False
        self.min_samples = min_samples

    def fit(self, process_data: List[Dict[str, Any]]):
        """Train the model on baseline process data."""
        if len(process_data) < self.min_samples:
            raise ValueError(
                f"Need at least {self.min_samples} snapshots to train detector, "
                f"received {len(process_data)}"
            )

        features = self._extract_features(process_data)
        self.model.fit(features)
        self.is_fitted = True
        logger.info("Trained anomaly detector on %s samples", len(features))

    def predict(self, process_snapshot: Dict[str, Any]) -> Dict[str, Any]:
        """Predict whether a single process snapshot is anomalous."""
        if not self.is_fitted:
            raise ValueError("Model not fitted yet")

        features = self._extract_features([process_snapshot])
        prediction = self.model.predict(features)[0]
        score = self.model.score_samples(features)[0]

        return {
            "is_anomaly": bool(prediction == -1),
            "anomaly_score": float(score),
            "process_name": process_snapshot.get("name", "<unknown>"),
            "pid": process_snapshot.get("pid"),
        }

    def predict_many(self, process_snapshots: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Predict anomalies for multiple process snapshots."""
        return [self.predict(snapshot) for snapshot in process_snapshots]

    def _extract_features(self, process_data: List[Dict[str, Any]]) -> np.ndarray:
        """Convert process snapshots into an ML feature matrix."""
        rows: List[List[float]] = []

        for proc in process_data:
            rows.append(
                [
                    float(proc.get("cpu_percent", 0.0) or 0.0),
                    float(proc.get("memory_mb", 0.0) or 0.0),
                    float(proc.get("memory_percent", 0.0) or 0.0),
                    float(proc.get("num_threads", 0.0) or 0.0),
                    float(np.log1p(proc.get("io_read_bytes", 0) or 0)),
                    float(np.log1p(proc.get("io_write_bytes", 0) or 0)),
                    float(proc.get("cpu_mean", 0.0) or 0.0),
                    float(proc.get("cpu_std", 0.0) or 0.0),
                    float(proc.get("cpu_max", 0.0) or 0.0),
                    float(proc.get("cpu_trend", 0.0) or 0.0),
                    float(proc.get("mem_mean", 0.0) or 0.0),
                    float(proc.get("mem_std", 0.0) or 0.0),
                    float(proc.get("mem_growth_rate", 0.0) or 0.0),
                    float(proc.get("duration", 0.0) or 0.0),
                ]
            )

        return np.asarray(rows, dtype=float)


class RealtimeAnomalyDetector:
    """Keep rolling process history and detect anomalies over time."""

    def __init__(
        self,
        contamination: float = 0.1,
        history_window: int = 100,
        retrain_interval: int = 25,
        min_training_samples: int = 25,
        detector: AnomalyDetector | None = None,
        feature_engineer: FeatureEngineer | None = None,
    ):
        """Initialize the real-time detector."""
        self.history_window = history_window
        self.retrain_interval = retrain_interval
        self.detector = detector or AnomalyDetector(
            contamination=contamination,
            min_samples=min_training_samples,
        )
        self.feature_engineer = feature_engineer or FeatureEngineer()
        self.history_by_pid: Dict[int, Deque[Dict[str, Any]]] = defaultdict(
            lambda: deque(maxlen=self.history_window)
        )
        self.training_rows: List[Dict[str, Any]] = []
        self._last_fit_size = 0

    def update(self, snapshot: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Update rolling history and return anomalies from the current snapshot."""
        anomalies = self.check_anomalies(snapshot)

        for proc in snapshot:
            if proc.get("pid") is None:
                continue

            pid = int(proc["pid"])
            history = self.history_by_pid[pid]
            history.append(proc)
            enriched_row = {
                **proc,
                **self.feature_engineer.compute_temporal_features(list(history)),
            }
            self.training_rows.append(enriched_row)

        if len(self.training_rows) > self.history_window * 10:
            self.training_rows = self.training_rows[-self.history_window * 10 :]

        should_fit = len(self.training_rows) >= self.detector.min_samples and (
            not self.detector.is_fitted
            or (len(self.training_rows) - self._last_fit_size) >= self.retrain_interval
        )
        if should_fit:
            self.detector.fit(self.training_rows)
            self._last_fit_size = len(self.training_rows)

        return anomalies

    def check_anomalies(self, snapshot: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Run anomaly detection against the current snapshot."""
        if not self.detector.is_fitted:
            return []

        anomalies: List[Dict[str, Any]] = []

        for proc in snapshot:
            if proc.get("pid") is None:
                continue

            pid = int(proc["pid"])
            history = list(self.history_by_pid.get(pid, []))
            temporal_features = self.feature_engineer.compute_temporal_features(history)
            enriched_row = {**proc, **temporal_features}
            prediction = self.detector.predict(enriched_row)
            temporal_outlier = self._is_temporal_outlier(proc, history, temporal_features)
            if prediction["is_anomaly"] or temporal_outlier:
                anomalies.append(
                    {
                        **prediction,
                        "is_anomaly": True,
                        "cpu_percent": float(proc.get("cpu_percent", 0.0) or 0.0),
                        "memory_mb": float(proc.get("memory_mb", 0.0) or 0.0),
                        "memory_percent": float(proc.get("memory_percent", 0.0) or 0.0),
                        "num_threads": int(proc.get("num_threads", 0) or 0),
                        "detection_method": "model" if prediction["is_anomaly"] else "temporal",
                    }
                )

        return anomalies

    def _is_temporal_outlier(
        self,
        proc: Dict[str, Any],
        history: List[Dict[str, Any]],
        temporal_features: Dict[str, float],
    ) -> bool:
        """Detect obvious spikes relative to recent per-process behavior."""
        if len(history) < 3:
            return False

        cpu_percent = float(proc.get("cpu_percent", 0.0) or 0.0)
        memory_mb = float(proc.get("memory_mb", 0.0) or 0.0)
        num_threads = float(proc.get("num_threads", 0.0) or 0.0)
        baseline_threads = float(
            np.mean([float(item.get("num_threads", 0.0) or 0.0) for item in history])
        )

        cpu_outlier = cpu_percent > max(
            temporal_features["cpu_max"] * 2.5,
            temporal_features["cpu_mean"] + max(15.0, temporal_features["cpu_std"] * 3.0),
        )
        memory_outlier = memory_mb > max(
            temporal_features["mem_mean"] * 3.0,
            temporal_features["mem_mean"] + max(100.0, temporal_features["mem_std"] * 3.0),
        )
        thread_outlier = baseline_threads > 0 and num_threads > max(
            baseline_threads * 4.0,
            baseline_threads + 20.0,
        )

        return cpu_outlier or memory_outlier or thread_outlier

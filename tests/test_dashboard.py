"""Owner dashboard — auth gate + data shape (2026-09-25)."""
from app.api.v1.dashboard import _totals


def test_totals_shape():
    rows = [{"input_tokens": 100, "output_tokens": 50, "total_tokens": 150,
             "cost": 0.0001, "latency_ms": 1000},
            {"input_tokens": None, "output_tokens": None, "total_tokens": 200,
             "cost": None, "latency_ms": None}]
    t = _totals(rows)
    assert t["calls"] == 2
    assert t["input_tokens"] == 100
    assert t["total_tokens"] == 350
    assert abs(t["cost"] - 0.0001) < 1e-9
    assert t["avg_latency_ms"] == 1000
    assert _totals([]) ["calls"] == 0

import threading

import pytest

from sentry_sdk._span_batcher import SpanBatcher


class _Segment:
    def _dynamic_sampling_context(self):
        return {"trace_id": "abc"}


def _span(name):
    return {
        "trace_id": "abc",
        "attributes": {},
        "_segment_span": _Segment(),
        "is_segment": False,
        "name": name,
    }


def _batcher():
    captured = []
    batcher = SpanBatcher(
        capture_func=captured.append, record_lost_func=lambda **kwargs: None
    )
    batcher._span_buffer["abc"].append(_span("first"))
    batcher._running_size["abc"] = 300
    batcher._pending_flush.add("abc")
    return batcher, captured


def test_span_flush_serializes_outside_lock(monkeypatch):
    batcher, captured = _batcher()
    serialize = batcher._to_transport_format

    def check_lock(item):
        assert batcher._lock.acquire(blocking=False)
        batcher._lock.release()
        return serialize(item)

    monkeypatch.setattr(batcher, "_to_transport_format", check_lock)
    batcher._flush()
    assert len(captured) == 1


def test_span_flush_does_not_block_new_spans(monkeypatch):
    batcher, captured = _batcher()
    started = threading.Event()
    resume = threading.Event()
    serialize = batcher._to_transport_format

    def blocked_serializer(item):
        started.set()
        assert resume.wait(2)
        return serialize(item)

    monkeypatch.setattr(batcher, "_to_transport_format", blocked_serializer)
    worker = threading.Thread(target=batcher._flush)
    worker.start()
    try:
        assert started.wait(2)
        available = batcher._lock.acquire(blocking=False)
        if available:
            try:
                batcher._span_buffer["abc"].append(_span("second"))
                batcher._running_size["abc"] += 300
            finally:
                batcher._lock.release()
    finally:
        resume.set()
        worker.join(3)

    assert not worker.is_alive()
    assert available
    assert [s["name"] for s in captured[0].items[0].payload.json["items"]] == ["first"]
    assert [s["name"] for s in batcher._span_buffer["abc"]] == ["second"]
    batcher._flush()
    assert [s["name"] for s in captured[1].items[0].payload.json["items"]] == ["second"]


def test_span_flush_restores_batch_on_serialization_error(monkeypatch):
    batcher, captured = _batcher()

    def fail(_):
        assert batcher._lock.acquire(blocking=False)
        try:
            batcher._span_buffer["abc"].append(_span("second"))
            batcher._running_size["abc"] += 300
        finally:
            batcher._lock.release()
        raise ValueError("bad attribute")

    monkeypatch.setattr(batcher, "_to_transport_format", fail)
    with pytest.raises(ValueError, match="bad attribute"):
        batcher._flush()

    assert [s["name"] for s in batcher._span_buffer["abc"]] == ["first", "second"]
    assert batcher._running_size["abc"] == 600
    assert "abc" in batcher._pending_flush
    assert not captured

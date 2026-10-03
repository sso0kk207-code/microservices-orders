import pytest

from shared.bus import DLQ_STREAM, STREAM, Consumer, Event, publish
from tests.conftest import new_redis


@pytest.fixture()
def r(redis_server):
    return new_redis(redis_server)


def consumer(r, handlers, group="g1", **kw):
    return Consumer(r, group=group, name=f"{group}-1", handlers=handlers, claim_idle_ms=0, **kw)


def test_event_roundtrip(r):
    got = []
    c = consumer(r, {"a.happened": got.append})
    e = Event("a.happened", {"x": 1, "text": "привет"})
    publish(r, e)
    assert c.run_once() == 1
    assert got[0].id == e.id and got[0].data == {"x": 1, "text": "привет"} and got[0].type == "a.happened"


def test_processed_event_is_acked(r):
    c = consumer(r, {"t": lambda e: None})
    publish(r, Event("t", {}))
    c.run_once()
    assert c.pending_count() == 0
    assert c.run_once() == 0  # повторно не выдаётся


def test_each_group_receives_every_event(r):
    a, b = [], []
    ca, cb = consumer(r, {"t": a.append}, "service-a"), consumer(r, {"t": b.append}, "service-b")
    publish(r, Event("t", {"n": 1}))
    ca.run_once(), cb.run_once()
    assert len(a) == len(b) == 1  # так каждый сервис получает своё событие независимо от остальных


def test_events_not_handled_by_a_service_are_ignored_and_acked(r):
    c = consumer(r, {"known": lambda e: None})
    publish(r, Event("unknown.type", {}))
    assert c.run_once() == 1 and c.pending_count() == 0


def test_events_are_processed_in_order(r):
    seen = []
    c = consumer(r, {"t": lambda e: seen.append(e.data["n"])})
    for n in range(5):
        publish(r, Event("t", {"n": n}))
    c.run_once()
    assert seen == [0, 1, 2, 3, 4]


def test_failed_event_stays_pending_then_succeeds_on_retry(r):
    calls = []

    def flaky(e):
        calls.append(1)
        if len(calls) < 3:
            raise ConnectionError("temporary")

    c = consumer(r, {"t": flaky}, max_attempts=5)
    publish(r, Event("t", {}))
    c.run_once()
    assert c.pending_count() == 1  # не подтверждено: событие не потеряно
    c.run_once(), c.run_once()
    assert len(calls) == 3 and c.pending_count() == 0


def test_poison_message_goes_to_dlq_after_max_attempts(r):
    def always_fails(e):
        raise ValueError("bad payload")

    c = consumer(r, {"t": always_fails}, max_attempts=3)
    publish(r, Event("t", {"order": 7}))
    for _ in range(5):
        c.run_once()
    dead = r.xrange(DLQ_STREAM)
    assert len(dead) == 1
    fields = dead[0][1]
    assert fields["type"] == "t" and fields["group"] == "g1" and "ValueError: bad payload" in fields["error"] and fields["attempts"] == "3"
    assert c.pending_count() == 0  # основная очередь не заблокирована


def test_poison_message_does_not_block_following_events(r):
    seen = []

    def handler(e):
        if e.data["n"] == 1:
            raise ValueError("poison")
        seen.append(e.data["n"])

    c = consumer(r, {"t": handler}, max_attempts=2)
    for n in range(4):
        publish(r, Event("t", {"n": n}))
    for _ in range(4):
        c.run_once()
    assert seen == [0, 2, 3] and len(r.xrange(DLQ_STREAM)) == 1


def test_failure_in_one_group_does_not_affect_another(r):
    ok = []
    bad = consumer(r, {"t": lambda e: (_ for _ in ()).throw(RuntimeError("boom"))}, "bad", max_attempts=10)
    good = consumer(r, {"t": ok.append}, "good")
    publish(r, Event("t", {}))
    bad.run_once(), good.run_once()
    assert len(ok) == 1 and bad.pending_count() == 1 and good.pending_count() == 0


def test_group_creation_is_idempotent_and_reads_history(r):
    publish(r, Event("t", {"n": 1}))  # событие опубликовано ДО появления потребителя
    got = []
    consumer(r, {"t": got.append}).run_once()
    consumer(r, {"t": got.append})  # повторное создание той же группы не падает
    assert len(got) == 1  # сервис, запущенный позже, не теряет накопленные события


def test_stream_is_the_expected_name(r):
    publish(r, Event("t", {}))
    assert r.xlen(STREAM) == 1

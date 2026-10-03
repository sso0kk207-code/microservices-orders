import json

from sqlalchemy import select

from services.auth.main import OutboxEvent
from shared.bus import STREAM, Event, publish
from shared.outbox import relay_once


def outbox_rows(world):
    with world.apps["auth"].state.sessions() as db:
        return db.scalars(select(OutboxEvent).order_by(OutboxEvent.id)).all()


def test_event_is_written_in_the_same_transaction_as_the_business_data(world):
    world.register()
    rows = outbox_rows(world)
    assert len(rows) == 1 and rows[0].type == "user.registered" and rows[0].published_at is None
    assert json.loads(rows[0].data)["email"] == "anna@example.com"


def test_failed_registration_leaves_no_event_behind(world):
    world.register()
    dup = world.auth.post("/register", json={"email": "anna@example.com", "name": "Again", "password": "supersecret1"})
    assert dup.status_code == 409
    assert len(outbox_rows(world)) == 1  # событие от неудачной регистрации откатилось вместе с пользователем


def test_nothing_is_published_until_the_relay_runs(world):
    world.register()
    assert world.apps["auth"].state.redis.xlen(STREAM) == 0


def test_relay_publishes_once_and_marks_rows(world):
    world.register()
    auth = world.apps["auth"]
    assert relay_once(auth.state.sessions, OutboxEvent, auth.state.redis) == 1
    assert relay_once(auth.state.sessions, OutboxEvent, auth.state.redis) == 0  # уже опубликовано
    assert auth.state.redis.xlen(STREAM) == 1 and outbox_rows(world)[0].published_at is not None


def test_published_event_keeps_the_outbox_event_id(world):
    world.register()
    auth = world.apps["auth"]
    relay_once(auth.state.sessions, OutboxEvent, auth.state.redis)
    (_, fields), = auth.state.redis.xrange(STREAM)
    assert fields["id"] == outbox_rows(world)[0].event_id and fields["type"] == "user.registered"


def test_relay_publishes_in_creation_order(world):
    for i in range(3):
        world.register(f"user{i}@example.com", f"User {i}")
    auth = world.apps["auth"]
    relay_once(auth.state.sessions, OutboxEvent, auth.state.redis)
    names = [json.loads(f["data"])["name"] for _, f in auth.state.redis.xrange(STREAM)]
    assert names == ["User 0", "User 1", "User 2"]


def test_duplicate_delivery_after_relay_crash_is_harmless(world):
    """Релей опубликовал событие, но упал до отметки published_at: событие уйдёт второй раз. Потребители дубли отсекают."""
    headers = world.register()
    auth = world.apps["auth"]
    row = outbox_rows(world)[0]
    dup = Event(type=row.type, data=json.loads(row.data), id=row.event_id)
    publish(auth.state.redis, dup)                               # первая публикация (отметка не успела записаться)
    relay_once(auth.state.sessions, OutboxEvent, auth.state.redis)  # релей после перезапуска публикует его снова
    assert auth.state.redis.xlen(STREAM) == 2
    world.pump()
    notes = world.notifications.get("/notifications", headers=headers).json()
    assert [n["kind"] for n in notes] == ["welcome"]  # письмо одно

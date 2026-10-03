"""Шина событий на Redis Streams: публикация, группы потребителей, повторная обработка и dead-letter stream."""
import json
import logging
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime

import redis

log = logging.getLogger("bus")

STREAM = "events"
DLQ_STREAM = "events.dlq"


@dataclass
class Event:
    type: str
    data: dict
    id: str = field(default_factory=lambda: uuid.uuid4().hex)  # идентификатор события: по нему потребитель отсекает дубли
    ts: str = field(default_factory=lambda: datetime.now(UTC).isoformat())

    def to_fields(self) -> dict[str, str]:
        return {"id": self.id, "type": self.type, "data": json.dumps(self.data, ensure_ascii=False), "ts": self.ts}

    @classmethod
    def from_fields(cls, fields: dict[str, str]) -> "Event":
        return cls(type=fields["type"], data=json.loads(fields["data"]), id=fields["id"], ts=fields["ts"])


def make_redis(url: str) -> redis.Redis:
    return redis.Redis.from_url(url, decode_responses=True)


def publish(r: redis.Redis, event: Event, stream: str = STREAM) -> str:
    return r.xadd(stream, event.to_fields(), maxlen=100_000, approximate=True)


class Consumer:
    """Группа потребителей: каждое событие обрабатывается одним экземпляром группы, а разные группы (сервисы) получают каждое событие.

    Гарантия - «минимум один раз»: событие подтверждается (XACK) только после успешной обработки. Если обработчик упал,
    событие остаётся в pending и будет выдано снова через claim_idle_ms. После max_attempts неудач оно уходит в DLQ.
    Дубли возможны, поэтому обработчики идемпотентны (см. shared.outbox.claim_event).
    """

    def __init__(self, r: redis.Redis, group: str, name: str, handlers: dict[str, Callable[[Event], None]], max_attempts: int = 3, claim_idle_ms: int = 30_000, stream: str = STREAM, dlq: str = DLQ_STREAM):
        self.r, self.group, self.name, self.handlers = r, group, name, handlers
        self.max_attempts, self.claim_idle_ms, self.stream, self.dlq = max_attempts, claim_idle_ms, stream, dlq
        try:
            r.xgroup_create(stream, group, id="0", mkstream=True)
        except redis.ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    def run_once(self, block_ms: int | None = None, count: int = 50) -> int:
        """Один проход: сначала зависшие события (после сбоя), потом новые. Возвращает число обработанных сообщений."""
        batch: list[tuple[str, dict]] = []
        claimed = self.r.xautoclaim(self.stream, self.group, self.name, min_idle_time=self.claim_idle_ms, start_id="0-0", count=count)
        batch += [(mid, f) for mid, f in claimed[1] if f]
        fresh = self.r.xreadgroup(self.group, self.name, {self.stream: ">"}, count=count, block=block_ms)
        for _stream, messages in fresh or []:
            batch += [(mid, f) for mid, f in messages if f]
        for msg_id, fields in batch:
            self._handle(msg_id, fields)
        return len(batch)

    def _handle(self, msg_id: str, fields: dict[str, str]) -> None:
        handler = self.handlers.get(fields.get("type", ""))
        if handler is None:  # событие нам не интересно
            self.r.xack(self.stream, self.group, msg_id)
            return
        attempts = self.r.hincrby(f"attempts:{self.group}", msg_id, 1)
        try:
            handler(Event.from_fields(fields))
        except Exception as exc:  # noqa: BLE001 - любая ошибка обработчика = неудачная попытка
            log.warning("handler failed (%s, attempt %s/%s): %s", fields.get("type"), attempts, self.max_attempts, exc)
            if attempts >= self.max_attempts:
                self.r.xadd(self.dlq, {**fields, "group": self.group, "error": f"{type(exc).__name__}: {exc}"[:300], "attempts": str(attempts)})
                self.r.xack(self.stream, self.group, msg_id)
                self.r.hdel(f"attempts:{self.group}", msg_id)
            return  # иначе событие остаётся в pending и будет выдано повторно
        self.r.xack(self.stream, self.group, msg_id)
        self.r.hdel(f"attempts:{self.group}", msg_id)

    def pending_count(self) -> int:
        return self.r.xpending(self.stream, self.group)["pending"]

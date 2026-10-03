"""Transactional outbox (надёжная публикация) и inbox (идемпотентное потребление).

Проблема: «записать в БД и отправить событие» - две операции в разных системах, атомарно их не выполнить.
Если сервис упадёт между ними, заказ есть, а события нет (или наоборот).
Решение: событие пишется в таблицу outbox в ТОЙ ЖЕ транзакции, что и бизнес-данные, а отдельный релей публикует его в шину.
"""
import json
import threading
from datetime import UTC, datetime

import redis
from sqlalchemy import DateTime, String, Text, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker

from shared.bus import Event, publish


def make_outbox(Base: type[DeclarativeBase]):
    class OutboxEvent(Base):
        __tablename__ = "outbox"

        id: Mapped[int] = mapped_column(primary_key=True)
        event_id: Mapped[str] = mapped_column(String(32), unique=True)
        type: Mapped[str] = mapped_column(String(64))
        data: Mapped[str] = mapped_column(Text)
        created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))
        published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    return OutboxEvent


def make_inbox(Base: type[DeclarativeBase]):
    class ProcessedEvent(Base):
        __tablename__ = "processed_events"

        event_id: Mapped[str] = mapped_column(String(32), primary_key=True)
        processed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))

    return ProcessedEvent


def add_to_outbox(db: Session, Outbox, etype: str, data: dict) -> Event:
    """Вызывать внутри той же транзакции, что и бизнес-запись (до db.commit())."""
    event = Event(type=etype, data=data)
    db.add(Outbox(event_id=event.id, type=etype, data=json.dumps(data, ensure_ascii=False)))
    return event


def relay_once(sessions: sessionmaker, Outbox, r: redis.Redis, batch: int = 100) -> int:
    """Публикует неотправленные события по порядку. Коммит после каждого: при сбое дубль возможен лишь для одного события."""
    sent = 0
    with sessions() as db:
        rows = db.scalars(select(Outbox).where(Outbox.published_at.is_(None)).order_by(Outbox.id).limit(batch)).all()
        for row in rows:
            publish(r, Event(type=row.type, data=json.loads(row.data), id=row.event_id, ts=row.created_at.isoformat()))
            row.published_at = datetime.now(UTC)
            db.commit()
            sent += 1
    return sent


def claim_event(db: Session, ProcessedEvent, event_id: str) -> bool:
    """True, если событие обрабатывается впервые. Вызывать ПЕРВОЙ операцией в сессии обработчика.

    Отметка и побочные эффекты фиксируются одним коммитом: если обработчик упал, отметка откатывается вместе с ними,
    и повторная доставка обработает событие заново. Если отметка уже есть (дубль), сессия откатывается и обработчик выходит.
    """
    try:
        db.add(ProcessedEvent(event_id=event_id))
        db.flush()
        return True
    except IntegrityError:
        db.rollback()
        return False


class BackgroundLoop:
    """Фоновый поток для релея outbox и потребителя событий."""

    def __init__(self, fn, interval: float = 0.3):
        self.fn, self.interval, self._stop = fn, interval, threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self.fn()
            except Exception:  # noqa: BLE001 - временная недоступность Redis не должна убивать поток
                pass
            self._stop.wait(self.interval)

    def start(self) -> "BackgroundLoop":
        self._thread.start()
        return self

    def stop(self) -> None:
        self._stop.set()

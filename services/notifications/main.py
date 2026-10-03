"""Notifications-сервис: превращает события в уведомления (e-mail). Сам ничего не заказывает, только реагирует на события."""
import os
from contextlib import asynccontextmanager
from datetime import UTC, datetime

from fastapi import Depends, FastAPI, Header, HTTPException
from sqlalchemy import DateTime, Integer, String, Text, select
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from shared.bus import DLQ_STREAM, Consumer, Event, make_redis
from shared.db import make_engine, make_session_factory
from shared.outbox import BackgroundLoop, claim_event, make_inbox
from shared.security import JWT_SECRET, make_current_user


class Base(DeclarativeBase):
    pass


class Notification(Base):
    __tablename__ = "notifications"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(Integer, index=True)
    kind: Mapped[str] = mapped_column(String(32))
    to_email: Mapped[str] = mapped_column(String(255))
    subject: Mapped[str] = mapped_column(String(200))
    body: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))


ProcessedEvent = make_inbox(Base)
SENT_EMAILS: list[dict] = []  # заглушка почтового провайдера (в проде здесь SMTP или API провайдера)


def send_email(to: str, subject: str, body: str) -> None:
    if to.endswith("@fail.example.com"):  # имитация отказа почтового провайдера - для демонстрации ретраев и DLQ
        raise ConnectionError(f"SMTP provider rejected recipient {to}")
    SENT_EMAILS.append({"to": to, "subject": subject})


def money(cents: int) -> str:
    return f"{cents / 100:,.2f}"


def create_app(database_url: str | None = None, redis_client=None, jwt_secret: str = JWT_SECRET, background: bool | None = None, admin_key: str | None = None) -> FastAPI:
    engine = make_engine(database_url or os.getenv("DATABASE_URL", "sqlite:///./data/notifications.db"))
    sessions = make_session_factory(engine)
    r = redis_client or make_redis(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    run_background = os.getenv("BACKGROUND", "1") == "1" if background is None else background
    admin_key = admin_key or os.getenv("ADMIN_KEY", "dev-admin-key")

    def notify(event: Event, kind: str, subject: str, body: str) -> None:
        d = event.data
        with sessions() as db:
            if not claim_event(db, ProcessedEvent, event.id):
                return  # событие пришло повторно: письмо второй раз не отправляем
            db.add(Notification(user_id=d["user_id"], kind=kind, to_email=d["email"], subject=subject, body=body))
            send_email(d["email"], subject, body)  # если отправка упала, транзакция откатывается вместе с отметкой события: повтор безопасен
            db.commit()

    handlers = {
        "user.registered": lambda e: notify(e, "welcome", "Добро пожаловать!", f"Здравствуйте, {e.data['name']}! Аккаунт создан."),
        "order.created": lambda e: notify(e, "order_created", f"Заказ №{e.data['order_id']} принят", f"{e.data['item']} × {e.data['quantity']}, сумма {money(e.data['total_cents'])}"),
        "order.cancelled": lambda e: notify(e, "order_cancelled", f"Заказ №{e.data['order_id']} отменён", f"Заказ «{e.data['item']}» отменён."),
    }
    consumer = Consumer(r, group="notifications", name="notifications-1", handlers=handlers, claim_idle_ms=int(os.getenv("CLAIM_IDLE_MS", "5000")))

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        Base.metadata.create_all(engine)
        loop = BackgroundLoop(consumer.run_once).start() if run_background else None
        yield
        if loop:
            loop.stop()

    app = FastAPI(title="Notifications Service", version="1.0.0", description="Слушает события и рассылает уведомления. Идемпотентен, неудачные события уходят в DLQ.", lifespan=lifespan)
    app.state.sessions, app.state.redis, app.state.consumer = sessions, r, consumer
    current_user = make_current_user(jwt_secret)

    def get_db():
        with sessions() as db:
            yield db

    @app.get("/health", tags=["service"])
    def health():
        return {"service": "notifications", "status": "ok"}

    @app.get("/notifications")
    def my_notifications(claims: dict = Depends(current_user), db: Session = Depends(get_db)):
        rows = db.scalars(select(Notification).where(Notification.user_id == claims["user_id"]).order_by(Notification.id))
        return [{"id": n.id, "kind": n.kind, "subject": n.subject, "body": n.body, "created_at": n.created_at} for n in rows]

    @app.get("/admin/dlq", tags=["admin"], summary="События, которые не удалось обработать")
    def dead_letters(x_admin_key: str | None = Header(None)):
        if x_admin_key != admin_key:
            raise HTTPException(403, "Admin key required")
        return [{"stream_id": mid, **fields} for mid, fields in r.xrange(DLQ_STREAM)]

    return app

"""Orders-сервис: заказы. Публикует order.created / order.cancelled, слушает user.registered (локальная копия клиентов)."""
import os
from contextlib import asynccontextmanager
from datetime import UTC, datetime

from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import DateTime, Integer, String, select
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from shared.bus import Consumer, Event, make_redis
from shared.db import make_engine, make_session_factory
from shared.outbox import BackgroundLoop, add_to_outbox, claim_event, make_inbox, make_outbox, relay_once
from shared.security import JWT_SECRET, make_current_user


class Base(DeclarativeBase):
    pass


class Order(Base):
    __tablename__ = "orders"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(Integer, index=True)
    item: Mapped[str] = mapped_column(String(120))
    quantity: Mapped[int] = mapped_column(Integer)
    total_cents: Mapped[int] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String(12), default="created")  # created | cancelled
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))


class Customer(Base):
    """Локальная копия данных клиента из auth-сервиса (read model). Обновляется событиями, синхронных вызовов в auth нет."""

    __tablename__ = "customers"

    user_id: Mapped[int] = mapped_column(primary_key=True)
    email: Mapped[str] = mapped_column(String(255))
    name: Mapped[str] = mapped_column(String(100))


OutboxEvent = make_outbox(Base)
ProcessedEvent = make_inbox(Base)


class OrderIn(BaseModel):
    item: str = Field(min_length=1, max_length=120)
    quantity: int = Field(ge=1, le=100)
    unit_price_cents: int = Field(gt=0)


def order_out(o: Order, customer: Customer | None) -> dict:
    return {"id": o.id, "item": o.item, "quantity": o.quantity, "total_cents": o.total_cents, "status": o.status, "customer": customer.name if customer else None, "created_at": o.created_at}


def create_app(database_url: str | None = None, redis_client=None, jwt_secret: str = JWT_SECRET, background: bool | None = None) -> FastAPI:
    engine = make_engine(database_url or os.getenv("DATABASE_URL", "sqlite:///./data/orders.db"))
    sessions = make_session_factory(engine)
    r = redis_client or make_redis(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    run_background = os.getenv("BACKGROUND", "1") == "1" if background is None else background

    def on_user_registered(event: Event) -> None:
        with sessions() as db:
            if not claim_event(db, ProcessedEvent, event.id):
                return  # дубль доставки: уже обработано
            d = event.data
            customer = db.get(Customer, d["user_id"])
            if customer:
                customer.email, customer.name = d["email"], d["name"]
            else:
                db.add(Customer(user_id=d["user_id"], email=d["email"], name=d["name"]))
            db.commit()

    consumer = Consumer(r, group="orders", name="orders-1", handlers={"user.registered": on_user_registered}, claim_idle_ms=int(os.getenv("CLAIM_IDLE_MS", "5000")))

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        Base.metadata.create_all(engine)
        loops = [BackgroundLoop(lambda: relay_once(sessions, OutboxEvent, r)).start(), BackgroundLoop(consumer.run_once).start()] if run_background else []
        yield
        for loop in loops:
            loop.stop()

    app = FastAPI(title="Orders Service", version="1.0.0", description="Заказы. Публикует `order.created`, `order.cancelled`. Токены проверяет локально.", lifespan=lifespan)
    app.state.sessions, app.state.redis, app.state.consumer, app.state.Outbox = sessions, r, consumer, OutboxEvent
    current_user = make_current_user(jwt_secret)

    def get_db():
        with sessions() as db:
            yield db

    @app.get("/health", tags=["service"])
    def health():
        return {"service": "orders", "status": "ok"}

    @app.post("/orders", status_code=201)
    def create_order(data: OrderIn, claims: dict = Depends(current_user), db: Session = Depends(get_db)):
        order = Order(user_id=claims["user_id"], item=data.item, quantity=data.quantity, total_cents=data.quantity * data.unit_price_cents, status="created")
        db.add(order)
        db.flush()
        add_to_outbox(db, OutboxEvent, "order.created", {"order_id": order.id, "user_id": claims["user_id"], "email": claims["email"], "name": claims["name"], "item": order.item, "quantity": order.quantity, "total_cents": order.total_cents})
        db.commit()
        return order_out(order, db.get(Customer, claims["user_id"]))

    @app.get("/orders")
    def my_orders(claims: dict = Depends(current_user), db: Session = Depends(get_db)):
        customer = db.get(Customer, claims["user_id"])
        return [order_out(o, customer) for o in db.scalars(select(Order).where(Order.user_id == claims["user_id"]).order_by(Order.id.desc()))]

    @app.get("/orders/{order_id}")
    def get_order(order_id: int, claims: dict = Depends(current_user), db: Session = Depends(get_db)):
        order = db.scalar(select(Order).where(Order.id == order_id, Order.user_id == claims["user_id"]))
        if not order:
            raise HTTPException(404, "Order not found")
        return order_out(order, db.get(Customer, order.user_id))

    @app.post("/orders/{order_id}/cancel")
    def cancel_order(order_id: int, claims: dict = Depends(current_user), db: Session = Depends(get_db)):
        order = db.scalar(select(Order).where(Order.id == order_id, Order.user_id == claims["user_id"]))
        if not order:
            raise HTTPException(404, "Order not found")
        if order.status == "cancelled":
            raise HTTPException(409, "Order is already cancelled")
        order.status = "cancelled"
        add_to_outbox(db, OutboxEvent, "order.cancelled", {"order_id": order.id, "user_id": order.user_id, "email": claims["email"], "name": claims["name"], "item": order.item})
        db.commit()
        return order_out(order, db.get(Customer, order.user_id))

    return app

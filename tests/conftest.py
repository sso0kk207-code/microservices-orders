from contextlib import ExitStack
from types import SimpleNamespace

import fakeredis
import pytest
from fastapi.testclient import TestClient

from services.auth.main import OutboxEvent as AuthOutbox
from services.auth.main import create_app as create_auth
from services.notifications import main as notif_module
from services.notifications.main import create_app as create_notifications
from services.orders.main import create_app as create_orders
from shared.outbox import relay_once


@pytest.fixture()
def redis_server():
    return fakeredis.FakeServer()


def new_redis(server):
    return fakeredis.FakeRedis(server=server, decode_responses=True)


@pytest.fixture()
def world(redis_server):
    """Три сервиса, у каждого своя in-memory БД; общая у них только шина событий (Redis)."""
    notif_module.SENT_EMAILS.clear()
    with ExitStack() as stack:
        apps = {
            "auth": create_auth("sqlite://", new_redis(redis_server), background=False),
            "orders": create_orders("sqlite://", new_redis(redis_server), background=False),
            "notifications": create_notifications("sqlite://", new_redis(redis_server), background=False, admin_key="secret-admin"),
        }
        clients = {name: stack.enter_context(TestClient(app)) for name, app in apps.items()}
        w = SimpleNamespace(apps=apps, server=redis_server, **clients)
        for c in (w.auth, w.orders, w.notifications):
            c.raise_server_exceptions = True

        def pump(rounds: int = 2) -> None:
            """Один «такт» системы: релеи outbox публикуют события, потребители их обрабатывают."""
            for _ in range(rounds):
                relay_once(apps["auth"].state.sessions, AuthOutbox, apps["auth"].state.redis)
                relay_once(apps["orders"].state.sessions, apps["orders"].state.Outbox, apps["orders"].state.redis)
                apps["orders"].state.consumer.run_once()
                apps["notifications"].state.consumer.run_once()

        def register(email="anna@example.com", name="Anna", password="supersecret1") -> dict:
            r = w.auth.post("/register", json={"email": email, "name": name, "password": password})
            assert r.status_code == 201, r.text
            return {"Authorization": f"Bearer {r.json()['access_token']}"}

        w.pump, w.register = pump, register
        for app in apps.values():  # claim_idle_ms=0: упавшие события выдаются повторно сразу, без ожидания
            if hasattr(app.state, "consumer"):
                app.state.consumer.claim_idle_ms = 0
        yield w

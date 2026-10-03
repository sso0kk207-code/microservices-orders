"""Сквозные сценарии: события проходят путь сервис → outbox → шина → потребители."""
from services.notifications import main as notif
from shared.bus import STREAM, Event, publish

ORDER = {"item": "Mechanical keyboard", "quantity": 2, "unit_price_cents": 8900}
ADMIN = {"X-Admin-Key": "secret-admin"}


def kinds(world, headers) -> list[str]:
    return [n["kind"] for n in world.notifications.get("/notifications", headers=headers).json()]


def test_registration_triggers_welcome_notification_and_customer_copy(world):
    h = world.register()
    assert kinds(world, h) == []  # пока событие не дошло, уведомления нет
    world.pump()
    assert kinds(world, h) == ["welcome"]
    assert notif.SENT_EMAILS == [{"to": "anna@example.com", "subject": "Добро пожаловать!"}]
    # orders получил событие и создал локальную копию клиента: в заказе видно имя без запроса в auth
    order = world.orders.post("/orders", json=ORDER, headers=h).json()
    assert world.orders.get(f"/orders/{order['id']}", headers=h).json()["customer"] == "Anna"


def test_order_lifecycle_produces_notifications(world):
    h = world.register()
    oid = world.orders.post("/orders", json=ORDER, headers=h).json()["id"]
    world.pump()
    world.orders.post(f"/orders/{oid}/cancel", headers=h)
    world.pump()
    notes = world.notifications.get("/notifications", headers=h).json()
    assert [n["kind"] for n in notes] == ["welcome", "order_created", "order_cancelled"]
    assert notes[1]["subject"] == f"Заказ №{oid} принят" and "178.00" in notes[1]["body"]


def test_notifications_are_private(world):
    anna, bob = world.register(), world.register("bob@example.com", "Bob")
    world.orders.post("/orders", json=ORDER, headers=anna)
    world.pump()
    assert kinds(world, anna) == ["welcome", "order_created"] and kinds(world, bob) == ["welcome"]


def test_duplicate_events_do_not_duplicate_effects(world):
    h = world.register()
    world.pump()
    r = world.apps["notifications"].state.redis
    (_, fields), = r.xrange(STREAM)
    r.xadd(STREAM, fields)  # то же событие (тот же event id) пришло второй раз
    r.xadd(STREAM, fields)
    world.pump()
    assert kinds(world, h) == ["welcome"] and len(notif.SENT_EMAILS) == 1


def test_orders_service_keeps_working_while_notifications_is_down(world):
    """Временная развязка: orders не знает о живости notifications. События копятся в шине и доходят позже."""
    h = world.register()
    for _ in range(3):
        assert world.orders.post("/orders", json=ORDER, headers=h).status_code == 201
    from shared.outbox import relay_once
    orders = world.apps["orders"]
    relay_once(orders.state.sessions, orders.state.Outbox, orders.state.redis)  # события лежат в шине, потребителя нет
    assert kinds(world, h) == []
    world.pump()  # notifications «поднялся»
    # порядок между событиями РАЗНЫХ сервисов не гарантируется (у каждого свой outbox), поэтому сравниваем состав
    assert sorted(kinds(world, h)) == sorted(["welcome"] + ["order_created"] * 3)


def test_failed_email_is_retried_then_lands_in_dlq(world):
    h = world.register("bad@fail.example.com", "Broken")  # почтовый провайдер отклоняет этот адрес
    world.pump(rounds=5)
    assert kinds(world, h) == []  # уведомление не создано, транзакция откатилась
    dead = world.notifications.get("/admin/dlq", headers=ADMIN).json()
    assert len(dead) == 1 and dead[0]["type"] == "user.registered" and dead[0]["group"] == "notifications"
    assert "SMTP provider rejected" in dead[0]["error"] and dead[0]["attempts"] == "3"


def test_poison_event_does_not_block_other_users(world):
    world.register("bad@fail.example.com", "Broken")
    good = world.register("good@example.com", "Good")
    world.pump(rounds=5)
    assert kinds(world, good) == ["welcome"]
    assert len(world.notifications.get("/admin/dlq", headers=ADMIN).json()) == 1


def test_dlq_requires_admin_key(world):
    assert world.notifications.get("/admin/dlq").status_code == 403
    assert world.notifications.get("/admin/dlq", headers={"X-Admin-Key": "nope"}).status_code == 403


def test_services_have_independent_databases(world):
    """В БД сервиса auth нет таблиц заказов, а в БД orders нет пользователей: общие данные ходят только событиями."""
    from sqlalchemy import inspect

    tables = {name: set(inspect(app.state.engine).get_table_names()) for name, app in world.apps.items() if hasattr(app.state, "engine")}
    assert "users" in tables["auth"] and "orders" not in tables["auth"]
    from services.orders.main import Base as OrdersBase
    assert "users" not in OrdersBase.metadata.tables


def test_unrelated_events_are_ignored_by_services(world):
    publish(world.apps["auth"].state.redis, Event("something.else", {"x": 1}))
    world.pump()
    assert world.apps["notifications"].state.consumer.pending_count() == 0

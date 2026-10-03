import jwt
import pytest

from shared.security import create_token

ORDER = {"item": "Mechanical keyboard", "quantity": 2, "unit_price_cents": 8900}


# ---------- auth ----------
def test_register_login_me(world):
    h = world.register()
    login = world.auth.post("/login", data={"username": "ANNA@example.com", "password": "supersecret1"})
    assert login.status_code == 200
    me = world.auth.get("/me", headers={"Authorization": f"Bearer {login.json()['access_token']}"}).json()
    assert me == {"id": 1, "email": "anna@example.com", "name": "Anna"}
    assert world.auth.get("/me", headers=h).status_code == 200


def test_auth_errors(world):
    world.register()
    assert world.auth.post("/register", json={"email": "anna@example.com", "name": "A", "password": "supersecret1"}).status_code == 409
    assert world.auth.post("/register", json={"email": "x@example.com", "name": "X", "password": "short"}).status_code == 422
    assert world.auth.post("/login", data={"username": "anna@example.com", "password": "wrong-password"}).status_code == 401
    assert world.auth.get("/me").status_code == 401


# ---------- токены проверяются каждым сервисом самостоятельно ----------
def test_other_services_verify_token_locally(world):
    h = world.register()
    assert world.orders.get("/orders", headers=h).status_code == 200
    assert world.notifications.get("/notifications", headers=h).status_code == 200


@pytest.mark.parametrize("make_headers", [
    lambda: {},
    lambda: {"Authorization": "Bearer garbage"},
    lambda: {"Authorization": f"Bearer {create_token(1, 'a@b.c', 'A', secret='some-other-secret')}"},       # подпись чужим ключом
    lambda: {"Authorization": f"Bearer {create_token(1, 'a@b.c', 'A', minutes=-5)}"},                         # просрочен
    lambda: {"Authorization": "Bearer " + jwt.encode({"sub": "1", "email": "a@b.c", "name": "A"}, "x", algorithm="HS256")},
])
def test_orders_rejects_bad_tokens(world, make_headers):
    assert world.orders.get("/orders", headers=make_headers()).status_code == 401
    assert world.orders.post("/orders", json=ORDER, headers=make_headers()).status_code == 401


# ---------- orders ----------
def test_create_list_get_order(world):
    h = world.register()
    o = world.orders.post("/orders", json=ORDER, headers=h)
    assert o.status_code == 201 and o.json()["total_cents"] == 17800 and o.json()["status"] == "created"
    assert len(world.orders.get("/orders", headers=h).json()) == 1
    assert world.orders.get(f"/orders/{o.json()['id']}", headers=h).json()["item"] == "Mechanical keyboard"


def test_orders_are_private_between_users(world):
    anna, bob = world.register(), world.register("bob@example.com", "Bob")
    oid = world.orders.post("/orders", json=ORDER, headers=anna).json()["id"]
    assert world.orders.get(f"/orders/{oid}", headers=bob).status_code == 404
    assert world.orders.post(f"/orders/{oid}/cancel", headers=bob).status_code == 404
    assert world.orders.get("/orders", headers=bob).json() == []


@pytest.mark.parametrize("patch", [{"quantity": 0}, {"quantity": 101}, {"unit_price_cents": 0}, {"item": ""}])
def test_order_validation(world, patch):
    h = world.register()
    assert world.orders.post("/orders", json={**ORDER, **patch}, headers=h).status_code == 422


def test_cancel_flow(world):
    h = world.register()
    oid = world.orders.post("/orders", json=ORDER, headers=h).json()["id"]
    assert world.orders.post(f"/orders/{oid}/cancel", headers=h).json()["status"] == "cancelled"
    assert world.orders.post(f"/orders/{oid}/cancel", headers=h).status_code == 409


def test_health_endpoints(world):
    assert world.auth.get("/health").json()["service"] == "auth"
    assert world.orders.get("/health").json()["service"] == "orders"
    assert world.notifications.get("/health").json()["service"] == "notifications"

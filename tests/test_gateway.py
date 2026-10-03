import json

import httpx
import pytest
from fastapi.testclient import TestClient

from services.gateway.main import create_app
from shared.circuit import CircuitBreaker

ROUTES = {"auth": "http://auth.local", "orders": "http://orders.local", "notifications": "http://notif.local"}


class Clock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t


def gateway(handler, clock=None):
    app = create_app(ROUTES, transport=httpx.MockTransport(handler), timeout=1.0, clock=clock)
    return TestClient(app)


def echo(request: httpx.Request) -> httpx.Response:
    return httpx.Response(200, json={"host": request.url.host, "path": request.url.path, "query": request.url.query.decode(), "auth": request.headers.get("authorization"), "rid": request.headers.get("x-request-id"), "body": request.content.decode()})


def test_routes_to_the_right_service_with_path_and_query():
    with gateway(echo) as g:
        body = g.get("/orders/orders/5?expand=items").json()
    assert (body["host"], body["path"], body["query"]) == ("orders.local", "/orders/5", "expand=items")


def test_forwards_method_body_and_authorization():
    with gateway(echo) as g:
        body = g.post("/orders/orders", json={"item": "x"}, headers={"Authorization": "Bearer abc"}).json()
    assert body["auth"] == "Bearer abc" and json.loads(body["body"]) == {"item": "x"}


def test_request_id_is_generated_and_propagated():
    with gateway(echo) as g:
        r = g.get("/auth/me")
        assert r.headers["x-request-id"] == r.json()["rid"] and len(r.headers["x-request-id"]) >= 8
        assert g.get("/auth/me", headers={"X-Request-ID": "trace-9"}).headers["x-request-id"] == "trace-9"


def test_unknown_service_is_404_and_upstream_errors_pass_through():
    def handler(request):
        return httpx.Response(404, json={"detail": "Order not found"})

    with gateway(handler) as g:
        assert g.get("/payments/x").status_code == 404
        r = g.get("/orders/orders/99")
        assert r.status_code == 404 and r.json() == {"detail": "Order not found"}


def test_unreachable_service_gives_502_and_timeout_gives_504():
    def down(request):
        raise httpx.ConnectError("refused")

    def slow(request):
        raise httpx.ReadTimeout("slow")

    with gateway(down) as g:
        assert g.get("/orders/orders").status_code == 502
    with gateway(slow) as g:
        assert g.get("/orders/orders").status_code == 504


def test_circuit_opens_after_repeated_failures_and_recovers():
    clock, calls, mode = Clock(), [], {"ok": False}

    def handler(request):
        calls.append(1)
        return httpx.Response(200, json={}) if mode["ok"] else httpx.Response(503)

    with gateway(handler, clock) as g:
        for _ in range(3):
            assert g.get("/orders/orders").status_code == 503
        n = len(calls)
        fast = g.get("/orders/orders")
        assert fast.status_code == 503 and fast.headers["retry-after"] == "10" and len(calls) == n  # до сервиса запрос не дошёл
        assert g.get("/auth/me").status_code == 503 and len(calls) == n + 1  # другие сервисы работают как раньше (503 это ответ заглушки)
        clock.t = 11  # cooldown прошёл: пробуем один запрос
        mode["ok"] = True
        assert g.get("/orders/orders").status_code == 200
        assert g.app.state.breakers["orders"].state == "closed"


def test_health_aggregates_services():
    def handler(request):
        if request.url.host == "notif.local":
            raise httpx.ConnectError("down")
        return httpx.Response(200, json={"status": "ok"})

    with gateway(handler) as g:
        body = g.get("/health").json()
    assert body["services"] == {"auth": "ok", "orders": "ok", "notifications": "down"}


@pytest.mark.parametrize("failures, expected", [(2, "closed"), (3, "open")])
def test_circuit_breaker_threshold(failures, expected):
    cb = CircuitBreaker(threshold=3, cooldown=10, clock=lambda: 0)
    for _ in range(failures):
        cb.failure()
    assert cb.state == expected


def test_circuit_breaker_half_open_failure_reopens():
    clock = Clock()
    cb = CircuitBreaker(threshold=1, cooldown=10, clock=clock)
    cb.failure()
    clock.t = 10
    assert cb.state == "half-open" and cb.allow()
    cb.failure()
    assert cb.state == "open"
    clock.t = 25
    cb.success()
    assert cb.state == "closed"

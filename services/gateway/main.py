"""API Gateway: единая точка входа. Маршрутизирует запросы к сервисам, пробрасывает токен и X-Request-ID, защищает от каскадных отказов."""
import asyncio
import os
import uuid

import httpx
from fastapi import FastAPI, Request, Response

from shared.circuit import CircuitBreaker

DEFAULT_ROUTES = {
    "auth": os.getenv("AUTH_URL", "http://127.0.0.1:8001"),
    "orders": os.getenv("ORDERS_URL", "http://127.0.0.1:8002"),
    "notifications": os.getenv("NOTIFICATIONS_URL", "http://127.0.0.1:8003"),
}
HOP_BY_HOP = {"host", "content-length", "connection", "transfer-encoding", "keep-alive"}


def create_app(routes: dict[str, str] | None = None, transport: httpx.AsyncBaseTransport | None = None, timeout: float = 5.0, clock=None) -> FastAPI:
    routes = routes or DEFAULT_ROUTES
    client = httpx.AsyncClient(transport=transport, timeout=timeout)
    breakers = {name: CircuitBreaker(threshold=3, cooldown=10.0, **({"clock": clock} if clock else {})) for name in routes}

    app = FastAPI(title="API Gateway", version="1.0.0", description="Единый вход: `/auth/*`, `/orders/*`, `/notifications/*`. При сбое сервиса отвечает быстро и понятно (502/503/504).")
    app.state.breakers = breakers

    @app.get("/health", tags=["service"], summary="Состояние всех сервисов")
    async def health():
        async def probe(name: str, base: str):
            try:
                r = await client.get(f"{base}/health", timeout=2)
                return name, "ok" if r.status_code == 200 else f"http {r.status_code}"
            except httpx.HTTPError:
                return name, "down"

        statuses = dict(await asyncio.gather(*[probe(n, b) for n, b in routes.items()]))
        return {"gateway": "ok", "services": statuses, "circuits": {n: b.state for n, b in breakers.items()}}

    @app.api_route("/{service}/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"], tags=["proxy"], include_in_schema=False)
    async def proxy(service: str, path: str, request: Request):
        if service not in routes:
            return Response('{"detail":"Unknown service"}', status_code=404, media_type="application/json")
        breaker = breakers[service]
        if not breaker.allow():
            return Response(f'{{"detail":"Service {service} is temporarily unavailable"}}', status_code=503, media_type="application/json", headers={"Retry-After": "10"})
        headers = {k: v for k, v in request.headers.items() if k.lower() not in HOP_BY_HOP}
        headers["X-Request-ID"] = request.headers.get("x-request-id") or uuid.uuid4().hex[:16]
        try:
            upstream = await client.request(request.method, f"{routes[service]}/{path}", params=request.query_params, content=await request.body(), headers=headers)
        except httpx.TimeoutException:
            breaker.failure()
            return Response(f'{{"detail":"Service {service} timed out"}}', status_code=504, media_type="application/json", headers={"X-Request-ID": headers["X-Request-ID"]})
        except httpx.HTTPError:
            breaker.failure()
            return Response(f'{{"detail":"Service {service} is unreachable"}}', status_code=502, media_type="application/json", headers={"X-Request-ID": headers["X-Request-ID"]})
        if upstream.status_code >= 500:
            breaker.failure()
        else:
            breaker.success()
        out_headers = {k: v for k, v in upstream.headers.items() if k.lower() not in HOP_BY_HOP | {"content-encoding"}}
        out_headers["X-Request-ID"] = headers["X-Request-ID"]
        return Response(upstream.content, status_code=upstream.status_code, headers=out_headers)

    return app

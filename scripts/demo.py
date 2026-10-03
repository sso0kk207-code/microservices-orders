"""Демо: python scripts/demo.py http://127.0.0.1:8000  (запрос идёт через gateway; сервисы работают отдельными процессами)"""
import time

from _demo import client, section, show

section("1. Gateway видит все сервисы (auth, orders, notifications - отдельные процессы со своими БД)")
show("GET", "/health")

section("2. Регистрация через gateway. Auth сохраняет пользователя и событие user.registered ОДНОЙ транзакцией (outbox)")
r = show("POST", "/auth/register", json_body={"email": "anna@example.com", "name": "Anna", "password": "supersecret1"}, max_lines=0)
h = {"Authorization": f"Bearer {r.json()['access_token']}"}
print("   токен выдан; orders и notifications проверят его сами, без запроса в auth")
print()

section("3. Заказ в orders. Событие order.created уходит в шину; orders не знает, кто его прочитает")
show("POST", "/orders/orders", json_body={"item": "Mechanical keyboard", "quantity": 2, "unit_price_cents": 8900}, headers=h, max_lines=12)

section("4. Notifications получил события асинхронно и создал уведомления (проверяем несколько раз)")
for _ in range(25):
    notes = client.get("/notifications/notifications", headers=h).json()
    if len(notes) >= 2:
        break
    time.sleep(0.3)
print("$ curl /notifications/notifications")
for n in notes:
    print(f"   [{n['kind']:<14}] {n['subject']}  |  {n['body']}")
print()

section("5. Заказ показывает имя клиента из ЛОКАЛЬНОЙ копии (orders получил её событием, в auth не ходил)")
show("GET", "/orders/orders", headers=h, max_lines=14)

section("6. Сбой почтового провайдера: событие повторяется и уходит в DLQ, остальные не блокируются")
client.post("/auth/register", json={"email": "bad@fail.example.com", "name": "Broken", "password": "supersecret1"})
client.post("/auth/register", json={"email": "bob@example.com", "name": "Bob", "password": "supersecret1"})
bob = client.post("/auth/login", data={"username": "bob@example.com", "password": "supersecret1"}).json()["access_token"]
for _ in range(30):
    dlq = client.get("/notifications/admin/dlq", headers={"X-Admin-Key": "dev-admin-key"}).json()
    bobs = client.get("/notifications/notifications", headers={"Authorization": f"Bearer {bob}"}).json()
    if dlq and bobs:
        break
    time.sleep(0.3)
print("$ curl /notifications/admin/dlq")
for d in dlq:
    print(f"   DLQ: type={d['type']} attempts={d['attempts']}  error={d['error']}")
print(f"   Боб, зарегистрированный ПОСЛЕ сбойного пользователя, получил письмо: {[n['kind'] for n in bobs]}")

"""Вся система одной командой, без Docker и Redis: python scripts/dev.py [--port 8000] [--fresh]

Поднимает: Redis-совместимый сервер (fakeredis, настоящий протокол и Streams), сервисы auth / orders / notifications
как ОТДЕЛЬНЫЕ процессы и gateway на --port. В продакшене Redis настоящий (docker compose).
"""
import argparse
import os
import shutil
import socket
import subprocess
import sys
import threading
from pathlib import Path

import uvicorn
from fakeredis import TcpFakeServer

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


parser = argparse.ArgumentParser()
parser.add_argument("--port", type=int, default=8000)
parser.add_argument("--fresh", action="store_true", help="удалить данные предыдущего запуска")
args = parser.parse_args()

data = ROOT / "data"
if args.fresh:
    shutil.rmtree(data, ignore_errors=True)
data.mkdir(exist_ok=True)

redis_port = free_port()
redis_server = TcpFakeServer(("127.0.0.1", redis_port), server_type="redis")
threading.Thread(target=redis_server.serve_forever, daemon=True).start()

ports = {name: free_port() for name in ("auth", "orders", "notifications")}
base_env = {**os.environ, "REDIS_URL": f"redis://127.0.0.1:{redis_port}/0", "CLAIM_IDLE_MS": os.getenv("CLAIM_IDLE_MS", "300"), "PYTHONPATH": str(ROOT)}
procs = []
for name, port in ports.items():
    env = {**base_env, "DATABASE_URL": f"sqlite:///{(data / (name + '.db')).as_posix()}"}
    procs.append(subprocess.Popen([sys.executable, "-m", "uvicorn", f"services.{name}.main:create_app", "--factory", "--port", str(port), "--log-level", "warning"], cwd=ROOT, env=env))

os.environ.update({f"{n.upper()}_URL": f"http://127.0.0.1:{p}" for n, p in ports.items()})
try:
    uvicorn.run("services.gateway.main:create_app", factory=True, host="127.0.0.1", port=args.port, log_level="warning")
finally:
    for p in procs:
        p.terminate()
    for p in procs:
        try:
            p.wait(timeout=10)
        except subprocess.TimeoutExpired:
            p.kill()
    redis_server.shutdown()

"""JWT, который выдаёт auth-сервис и проверяют остальные сервисы локально, без запроса к auth на каждый вызов.

В разработке общий секрет HS256. В продакшене лучше RS256: подпись приватным ключом только у auth,
а у остальных сервисов лишь публичный ключ (JWKS), поэтому компрометация orders не позволит выпускать токены.
"""
import os
from datetime import UTC, datetime, timedelta

import bcrypt
import jwt
from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

JWT_SECRET = os.getenv("JWT_SECRET", "dev-shared-secret-change-in-production-0123456789")
ALGORITHM = "HS256"
bearer = HTTPBearer(auto_error=False)


def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()


def verify_password(password: str, hashed: str) -> bool:
    return bcrypt.checkpw(password.encode(), hashed.encode())


def create_token(user_id: int, email: str, name: str, secret: str = JWT_SECRET, minutes: int = 60) -> str:
    claims = {"sub": str(user_id), "email": email, "name": name, "exp": datetime.now(UTC) + timedelta(minutes=minutes)}
    return jwt.encode(claims, secret, algorithm=ALGORITHM)


def make_current_user(secret: str = JWT_SECRET):
    """Зависимость FastAPI: возвращает claims токена. Каждый сервис вызывает её со своим (общим) секретом."""

    def current_user(creds: HTTPAuthorizationCredentials | None = Depends(bearer)) -> dict:
        if not creds:
            raise HTTPException(401, "Missing token", headers={"WWW-Authenticate": "Bearer"})
        try:
            claims = jwt.decode(creds.credentials, secret, algorithms=[ALGORITHM])
        except jwt.PyJWTError:
            raise HTTPException(401, "Invalid or expired token", headers={"WWW-Authenticate": "Bearer"})
        return {"user_id": int(claims["sub"]), "email": claims["email"], "name": claims["name"]}

    return current_user

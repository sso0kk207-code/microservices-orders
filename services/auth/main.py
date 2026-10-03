"""Auth-сервис: регистрация, вход, выдача JWT. Публикует событие user.registered. Своя БД."""
import os
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException
from fastapi.security import OAuth2PasswordRequestForm
from pydantic import BaseModel, EmailStr, Field
from sqlalchemy import String, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from shared.bus import make_redis
from shared.db import make_engine, make_session_factory
from shared.outbox import BackgroundLoop, add_to_outbox, make_outbox, relay_once
from shared.security import JWT_SECRET, create_token, hash_password, make_current_user, verify_password


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True)
    email: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    name: Mapped[str] = mapped_column(String(100))
    password_hash: Mapped[str] = mapped_column(String(128))


OutboxEvent = make_outbox(Base)


class RegisterIn(BaseModel):
    email: EmailStr
    name: str = Field(min_length=1, max_length=100)
    password: str = Field(min_length=8, max_length=128)


class TokenOut(BaseModel):
    access_token: str
    token_type: str = "bearer"


def create_app(database_url: str | None = None, redis_client=None, jwt_secret: str = JWT_SECRET, background: bool | None = None) -> FastAPI:
    engine = make_engine(database_url or os.getenv("DATABASE_URL", "sqlite:///./data/auth.db"))
    sessions = make_session_factory(engine)
    r = redis_client or make_redis(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    run_background = os.getenv("BACKGROUND", "1") == "1" if background is None else background

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        Base.metadata.create_all(engine)
        loop = BackgroundLoop(lambda: relay_once(sessions, OutboxEvent, r)).start() if run_background else None
        yield
        if loop:
            loop.stop()

    app = FastAPI(title="Auth Service", version="1.0.0", description="Регистрация и вход. Выдаёт JWT, публикует `user.registered`.", lifespan=lifespan)
    app.state.sessions, app.state.redis, app.state.engine = sessions, r, engine
    current_user = make_current_user(jwt_secret)

    def get_db():
        with sessions() as db:
            yield db

    @app.get("/health", tags=["service"])
    def health():
        return {"service": "auth", "status": "ok"}

    @app.post("/register", response_model=TokenOut, status_code=201)
    def register(data: RegisterIn, db: Session = Depends(get_db)):
        user = User(email=data.email.lower(), name=data.name, password_hash=hash_password(data.password))
        db.add(user)
        try:
            db.flush()  # получаем user.id
            # событие и пользователь сохраняются ОДНОЙ транзакцией: нет пользователя без события и события без пользователя
            add_to_outbox(db, OutboxEvent, "user.registered", {"user_id": user.id, "email": user.email, "name": user.name})
            db.commit()
        except IntegrityError:
            db.rollback()
            raise HTTPException(409, "Email already registered")
        return TokenOut(access_token=create_token(user.id, user.email, user.name, jwt_secret))

    @app.post("/login", response_model=TokenOut)
    def login(form: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)):
        user = db.scalar(select(User).where(User.email == form.username.lower()))
        if not user or not verify_password(form.password, user.password_hash):
            raise HTTPException(401, "Wrong email or password")
        return TokenOut(access_token=create_token(user.id, user.email, user.name, jwt_secret))

    @app.get("/me")
    def me(claims: dict = Depends(current_user), db: Session = Depends(get_db)):
        user = db.get(User, claims["user_id"])
        if not user:
            raise HTTPException(404, "User not found")
        return {"id": user.id, "email": user.email, "name": user.name}

    return app

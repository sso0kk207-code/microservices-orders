FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /srv

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared ./shared
COPY services ./services

RUN useradd -m appuser && mkdir -p /srv/data && chown -R appuser /srv
USER appuser

# Один образ на все сервисы: что запускать, задаётся командой в docker-compose (services.<имя>.main:create_app)
EXPOSE 8000
CMD ["uvicorn", "services.gateway.main:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]

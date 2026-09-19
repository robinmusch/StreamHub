FROM python:3.12-slim

LABEL \
    io.hass.version="2.0.4" \
    io.hass.type="app" \
    io.hass.arch="aarch64|amd64"

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY run.sh /app/run.sh
COPY app.py /app/app.py

RUN chmod +x /app/run.sh

EXPOSE 8088

CMD ["/app/run.sh"]

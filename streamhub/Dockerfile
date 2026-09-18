FROM ghcr.io/home-assistant/base:latest

WORKDIR /app

COPY run.sh /app/run.sh
COPY app.py /app/app.py

RUN chmod +x /app/run.sh

EXPOSE 8088

CMD ["/app/run.sh"]

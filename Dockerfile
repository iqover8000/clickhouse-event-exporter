FROM --platform=$TARGETPLATFORM python:3.14.4-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY application/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY application /app

EXPOSE 8080

CMD ["python", "app.py"]

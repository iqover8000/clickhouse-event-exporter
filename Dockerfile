FROM python:3.14.4-slim@sha256:557811b1000883f3c2bed1ad0e7b6c7a2fe8b4c4966c6ad26107e0ea4e62070f

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY application/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY application /app

EXPOSE 8080

CMD ["python", "app.py"]

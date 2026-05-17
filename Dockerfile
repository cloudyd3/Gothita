FROM python:3.14-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY config.yaml .
COPY main.py .
COPY src/ src/

ENV PYTHONUNBUFFERED=1
ENV GOTHITA_CONFIG=/app/config.yaml

CMD ["python", "main.py"]

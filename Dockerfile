FROM python:3.9-slim

LABEL maintainer="Neeraj Bhadani bhadaneeraj@gmail.com"

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --upgrade pip && \
    pip install -r requirements.txt

CMD [ "python", "src/main.py" ]


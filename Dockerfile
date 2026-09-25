FROM python:3.12-slim

# Устанавливаем системные зависимости для opus и edge-tts
RUN apt-get update && apt-get install -y \
    ffmpeg \
    libopus0 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Копируем зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем исходники
COPY . .

# Открываем порт (Render сам назначит PORT)
EXPOSE 5003

# Скрипт запуска: и прокси, и чат-сервер
CMD ["sh", "-c", "python proxy.py & sleep 3 && python chat_server.py"]

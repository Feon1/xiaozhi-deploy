import asyncio
import websockets
import os
import json
import uuid
import requests
import sys
from urllib.parse import urlparse
from dotenv import load_dotenv
import numpy as np

from system_info import setup_opus

import logging

logging.getLogger("websockets.server").setLevel(logging.CRITICAL)
logging.getLogger("websockets.asyncio.server").setLevel(logging.CRITICAL)

setup_opus()

try:
    import opuslib
except Exception as e:
    print(f"导入 opuslib 失败: {e}")
    sys.exit(1)

load_dotenv()


# ============================================================
# КОНФИГ
# ============================================================
WS_URL = os.getenv("WS_URL", "wss://api.tenclass.net/xiaozhi/v1/")
TOKEN = os.getenv("DEVICE_TOKEN", "test-token")
LOCAL_PROXY_URL = os.getenv("LOCAL_PROXY_URL", "ws://localhost:5002")
OTA_URL = os.getenv("OTA_URL", "https://api.tenclass.net/xiaozhi/ota/")

DEVICE_MAC = 'e0:2e:0b:ae:79:ea'
CLIENT_ID = "9cc3e5e4-adcf-4eff-8d23-95d4eaa21020"

# Интервал keep-alive ping (сек). Должен быть меньше таймаута Xiaozhi (обычно 60с).
KEEP_ALIVE_INTERVAL = int(os.getenv("KEEP_ALIVE_INTERVAL", "25"))

try:
    parsed_url = urlparse(LOCAL_PROXY_URL)
    PROXY_HOST = '127.0.0.1'
    PROXY_PORT = parsed_url.port or 5002
except Exception:
    PROXY_HOST = '127.0.0.1'
    PROXY_PORT = 5002


# ============================================================
# УТИЛИТЫ
# ============================================================
def get_mac_address():
    return DEVICE_MAC


def get_client_id():
    global CLIENT_ID
    if not CLIENT_ID:
        new_client_id = str(uuid.uuid4())
        with open(".env", "a", encoding="utf-8") as env_file:
            env_file.write(f"CLIENT_ID={new_client_id}\n")
        os.environ["CLIENT_ID"] = new_client_id
        CLIENT_ID = new_client_id
        return new_client_id
    return CLIENT_ID


# ============================================================
# OTA-ЗАПРОС
# ============================================================
def register_device(device_mac, client_id, token):
    headers = {
        "Device-Id": device_mac,
        "Client-Id": client_id,
        "Content-Type": "application/json"
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"

    payload = {
        "version": "1.0.0",
        "board": "web-client",
        "mac": device_mac
    }

    try:
        print(f"📡 OTA-запрос на {OTA_URL}...", flush=True)
        response = requests.post(OTA_URL, headers=headers, json=payload, timeout=10)
        print(f"📡 HTTP {response.status_code}", flush=True)
        response.raise_for_status()
        data = response.json()
        print("📡 OTA ответ получен", flush=True)
        return data
    except Exception as e:
        print(f"❌ OTA ошибка: {e}", flush=True)
        return None


# ============================================================
# АУДИО
# ============================================================
class AudioProcessor:
    def __init__(self, buffer_size=960):
        self.buffer_size = buffer_size
        self.buffer = np.array([], dtype=np.float32)
        self.encoder = opuslib.Encoder(16000, 1, 'voip')

    def reset_buffer(self):
        self.buffer = np.array([], dtype=np.float32)
        try:
            self.encoder = opuslib.Encoder(16000, 1, 'voip')
        except Exception:
            pass

    def encode_chunk(self, float32_chunk):
        pcm_int16 = (float32_chunk * 32767).astype(np.int16)
        return self.encoder.encode(pcm_int16.tobytes(), 960)

    def process_audio(self, input_data):
        input_array = np.frombuffer(input_data, dtype=np.float32)
        self.buffer = np.append(self.buffer, input_array)
        chunks = []
        while len(self.buffer) >= self.buffer_size:
            chunk = self.buffer[:self.buffer_size]
            self.buffer = self.buffer[self.buffer_size:]
            chunks.append(chunk.copy())
        return chunks

    def process_remaining(self):
        if len(self.buffer) > 0:
            chunk = self.buffer
            self.buffer = np.array([], dtype=np.float32)
            if len(chunk) < self.buffer_size:
                padded = np.zeros(self.buffer_size, dtype=np.float32)
                padded[:len(chunk)] = chunk
                chunk = padded
            return [chunk]
        return []


# ============================================================
# ОБРАБОТЧИК HTTP (для health-check Render)
# ============================================================
async def process_request(connection, request):
    if request.headers.get("Upgrade", "").lower() == "websocket":
        return None

    from websockets.http11 import Response
    from websockets.datastructures import Headers

    body = b"Xiaozhi WebSocket Proxy is running"
    headers = Headers([
        ("Content-Type", "text/plain"),
        ("Content-Length", str(len(body))),
        ("Connection", "close"),
    ])
    return Response(200, "OK", headers, body)


# ============================================================
# WEBSOCKET-ПРОКСИ
# ============================================================
class WebSocketProxy:
    def __init__(self):
        self.device_id = get_mac_address()
        self.client_id = get_client_id()
        self.enable_token = os.getenv("ENABLE_TOKEN", "true").lower() == "true"
        self.token = os.getenv("DEVICE_TOKEN", "test-token")

        self.headers = {
            "Device-Id": self.device_id,
            "Client-Id": self.client_id,
            "Protocol-Version": "1",
        }
        if self.enable_token:
            self.headers["Authorization"] = f"Bearer {self.token}"

        self.audio_processor = AudioProcessor(buffer_size=960)

    # ---------- APPLICATION-LEVEL KEEP-ALIVE ----------
    async def keep_alive(self, server_ws, session_id):
        """
        Отправляет JSON {"type":"ping"} каждые KEEP_ALIVE_INTERVAL секунд.
        Это считается активностью сессии и не даёт Xiaozhi закрыть соединение.
        """
        try:
            while True:
                await asyncio.sleep(KEEP_ALIVE_INTERVAL)
                try:
                    await server_ws.send(json.dumps({"type": "ping"}))
                    print(f"💓 [{session_id}] keep-alive ping", flush=True)
                except Exception as e:
                    print(f"💓 [{session_id}] keep-alive failed: {e}", flush=True)
                    return
        except asyncio.CancelledError:
            return

    # ---------- ОСНОВНОЙ ОБРАБОТЧИК ----------
    async def proxy_handler(self, websocket):
        session_id = "?"
        try:
            print(f"\n📡 Клиент подключился: {websocket.remote_address}", flush=True)
            print(f"🌐 Подключение к {WS_URL}", flush=True)

            await asyncio.to_thread(
                register_device, self.device_id, self.client_id, self.token
            )

            async with websockets.connect(
                WS_URL,
                additional_headers=self.headers,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=5,
                max_size=10 * 1024 * 1024,   # 10 MB — на случай длинных ответов
            ) as server_ws:
                print("✅ Подключено к серверу Xiaozhi", flush=True)

                # Отдельная задача keep-alive для сервера
                keep_alive_task = asyncio.create_task(
                    self.keep_alive(server_ws, session_id)
                )

                client_to_server = asyncio.create_task(
                    self.handle_client_messages(websocket, server_ws, session_id)
                )
                server_to_client = asyncio.create_task(
                    self.handle_server_messages(server_ws, websocket, session_id)
                )

                done, pending = await asyncio.wait(
                    [client_to_server, server_to_client, keep_alive_task],
                    return_when=asyncio.FIRST_COMPLETED
                )

                # Показать, какая задача завершилась первой
                for task in done:
                    if task.exception():
                        print(f"⚠️ Задача завершилась с ошибкой: {task.exception()}", flush=True)

                # Отменяем и ждём отмены остальных задач
                for task in pending:
                    task.cancel()
                for task in pending:
                    try:
                        await task
                    except (asyncio.CancelledError, Exception):
                        pass

                # Логируем, кто и как закрыл соединение
                try:
                    print(
                        f"🔚 Закрытие сервера: code={server_ws.close_code}, "
                        f"reason={server_ws.close_reason}",
                        flush=True
                    )
                except Exception:
                    pass

        except Exception as e:
            print(f"❌ Ошибка прокси: {type(e).__name__}: {e}", flush=True)
        finally:
            print(f"🔌 Клиент отключён (session={session_id})", flush=True)

    # ---------- СООБЩЕНИЯ ОТ XIAOZHI ----------
    async def handle_server_messages(self, server_ws, client_ws, session_id):
        try:
            async for message in server_ws:
                if isinstance(message, str):
                    # Логируем session_id, если сервер его прислал
                    try:
                        data = json.loads(message)
                        if data.get("type") == "hello" and data.get("session_id"):
                            session_id = data["session_id"]
                            print(f"✅ Session ID: {session_id}", flush=True)
                    except Exception:
                        pass
                    print(f"⬅️ От Xiaozhi: {message[:300]}", flush=True)
                    await client_ws.send(message)
                else:
                    print(f"⬅️ От Xiaozhi (bin): {len(message)} bytes", flush=True)
                    await client_ws.send(message)
        except websockets.exceptions.ConnectionClosed as e:
            print(f"❌ Сервер закрыл соединение: code={e.code}, reason={e.reason}", flush=True)
        except Exception as e:
            print(f"❌ Server messages: {type(e).__name__}: {e}", flush=True)

    # ---------- СООБЩЕНИЯ ОТ БРАУЗЕРА ----------
    async def handle_client_messages(self, client_ws, server_ws, session_id):
        try:
            async for message in client_ws:
                if isinstance(message, str):
                    try:
                        msg_data = json.loads(message)
                        msg_type = msg_data.get('type')

                        if msg_type == 'reset':
                            self.audio_processor.reset_buffer()
                        elif msg_type == 'getLastData':
                            for chunk in self.audio_processor.process_remaining():
                                try:
                                    opus_data = self.audio_processor.encode_chunk(chunk)
                                    await server_ws.send(opus_data)
                                except Exception as e:
                                    print(f"Opus err (last): {e}", flush=True)
                            await client_ws.send(json.dumps({'type': 'lastData'}))
                        else:
                            await server_ws.send(message)
                    except json.JSONDecodeError:
                        await server_ws.send(message)
                else:
                    try:
                        audio_data = np.frombuffer(message, dtype=np.float32)
                        if len(audio_data) > 0:
                            chunks = self.audio_processor.process_audio(audio_data.tobytes())
                            for chunk in chunks:
                                try:
                                    opus_data = self.audio_processor.encode_chunk(chunk)
                                    await server_ws.send(opus_data)
                                except Exception as e:
                                    print(f"Opus err: {e}", flush=True)
                    except Exception as e:
                        print(f"Клиентское аудио: {e}", flush=True)
        except websockets.exceptions.ConnectionClosed as e:
            print(f"❌ Клиент закрыл соединение: code={e.code}, reason={e.reason}", flush=True)
        except Exception as e:
            print(f"Ошибка клиентских сообщений: {type(e).__name__}: {e}", flush=True)

    # ---------- ЗАПУСК ----------
    async def main(self):
        print("=" * 60)
        print(f"🚀 Прокси запускается на {PROXY_HOST}:{PROXY_PORT}")
        print(f"📱 Device ID: {self.device_id}")
        print(f"🌐 WS URL: {WS_URL}")
        print(f"💓 Keep-alive каждые {KEEP_ALIVE_INTERVAL} сек")
        print("=" * 60)

        async with websockets.serve(
            self.proxy_handler,
            PROXY_HOST,
            PROXY_PORT,
            process_request=process_request,
            ping_interval=None,   # отключаем авто-пинг на стороне прокси-сервера
            ping_timeout=None,
        ):
            await asyncio.Future()


if __name__ == "__main__":
    proxy = WebSocketProxy()
    asyncio.run(proxy.main())

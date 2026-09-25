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

DEVICE_MAC = '00:1d:92:46:11:12'

try:
    parsed_url = urlparse(LOCAL_PROXY_URL)
    PROXY_HOST = '0.0.0.0'
    PROXY_PORT = parsed_url.port or 5002
except Exception:
    PROXY_HOST = '0.0.0.0'
    PROXY_PORT = 5002


# ============================================================
# УТИЛИТЫ
# ============================================================
def get_mac_address():
    return DEVICE_MAC


CLIENT_ID = os.getenv("CLIENT_ID", "")


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
        print(f"📡 OTA-запрос на {OTA_URL}...")
        response = requests.post(OTA_URL, headers=headers, json=payload, timeout=10)
        print(f"📡 HTTP {response.status_code}")
        response.raise_for_status()
        data = response.json()
        print("📡 OTA ответ:", json.dumps(data, indent=2, ensure_ascii=False)[:500])
        return data
    except Exception as e:
        print(f"❌ OTA ошибка: {e}")
        return None


# ============================================================
# АУДИО (только для ИСХОДЯЩЕГО — от клиента к Xiaozhi)
# ============================================================
def pcm_to_opus(pcm_data):
    try:
        encoder = opuslib.Encoder(16000, 1, 'voip')
        pcm_array = np.frombuffer(pcm_data, dtype=np.int16)
        return encoder.encode(pcm_array.tobytes(), 960)
    except Exception as e:
        print(f"Opus encode error: {e}")
        return None


class AudioProcessor:
    def __init__(self, buffer_size=960):
        self.buffer_size = buffer_size
        self.buffer = np.array([], dtype=np.float32)

    def reset_buffer(self):
        self.buffer = np.array([], dtype=np.float32)

    def process_audio(self, input_data):
        input_array = np.frombuffer(input_data, dtype=np.float32)
        self.buffer = np.append(self.buffer, input_array)
        chunks = []
        while len(self.buffer) >= self.buffer_size:
            chunk = self.buffer[:self.buffer_size]
            self.buffer = self.buffer[self.buffer_size:]
            pcm_data = (chunk * 32767).astype(np.int16)
            chunks.append(pcm_data.tobytes())
        return chunks

    def process_remaining(self):
        if len(self.buffer) > 0:
            pcm_data = (self.buffer * 32767).astype(np.int16)
            self.buffer = np.array([], dtype=np.float32)
            return [pcm_data.tobytes()]
        return []


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

        # Только для исходящего аудио (от браузера в Xiaozhi)
        self.audio_processor = AudioProcessor(buffer_size=960)

    async def proxy_handler(self, websocket):
        try:
            print(f"\n📡 Клиент подключился: {websocket.remote_address}")
            print(f"🌐 Подключение к {WS_URL}")

            register_device(self.device_id, self.client_id, self.token)

            print(f"📋 Заголовки: {self.headers}")

            async with websockets.connect(
                WS_URL,
                additional_headers=self.headers,
                ping_interval=None,
            ) as server_ws:
                print("✅ Подключено к серверу Xiaozhi")

                client_to_server = asyncio.create_task(
                    self.handle_client_messages(websocket, server_ws)
                )
                server_to_client = asyncio.create_task(
                    self.handle_server_messages(server_ws, websocket)
                )

                done, pending = await asyncio.wait(
                    [client_to_server, server_to_client],
                    return_when=asyncio.FIRST_COMPLETED
                )

                for task in pending:
                    task.cancel()

        except Exception as e:
            print(f"❌ Ошибка прокси: {e}")
        finally:
            print("🔌 Клиент отключён")

    async def handle_server_messages(self, server_ws, client_ws):
        """
        Сообщения от Xiaozhi к клиенту.
        Бинарные данные (аудио) ПОЛНОСТЬЮ игнорируются — не декодируются, не буферизуются, не отправляются.
        """
        try:
            async for message in server_ws:
                if isinstance(message, str):
                    # Все текстовые JSON-сообщения просто пересылаем клиенту
                    await client_ws.send(message)
                else:
                    # Бинарные данные (Opus-аудио от TTS) — игнорируем молча
                    continue
        except Exception as e:
            print(f"Ошибка серверных сообщений: {e}")

    async def handle_client_messages(self, client_ws, server_ws):
        """
        Сообщения от клиента к Xiaozhi.
        Поддерживает текстовые сообщения и аудио (PCM Float32 → Opus).
        """
        try:
            async for message in client_ws:
                if isinstance(message, str):
                    try:
                        msg_data = json.loads(message)
                        if msg_data.get('type') == 'reset':
                            self.audio_processor.reset_buffer()
                        elif msg_data.get('type') == 'getLastData':
                            remaining_chunks = self.audio_processor.process_remaining()
                            for chunk in remaining_chunks:
                                opus_data = pcm_to_opus(chunk)
                                if opus_data:
                                    await server_ws.send(opus_data)
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
                                opus_data = pcm_to_opus(chunk)
                                if opus_data:
                                    await server_ws.send(opus_data)
                    except Exception as e:
                        print(f"Клиентское аудио: {e}")
        except Exception as e:
            print(f"Ошибка клиентских сообщений: {e}")

    async def main(self):
        print("=" * 60)
        print(f"🚀 Прокси запускается на {PROXY_HOST}:{PROXY_PORT}")
        print(f"📱 Device ID: {self.device_id}")
        print(f"🔑 Token: {self.token}")
        print(f"🌐 WS URL: {WS_URL}")
        print("=" * 60)

        async with websockets.serve(self.proxy_handler, PROXY_HOST, PROXY_PORT):
            await asyncio.Future()


if __name__ == "__main__":
    proxy = WebSocketProxy()
    asyncio.run(proxy.main())
"""Flask-сервер чата с паролем, историей в SQLite и fallback на внешний RAG."""
import asyncio
import json
import queue
import threading
import time
import os
import sqlite3
import numpy as np
import httpx

import websockets
from flask import Flask, render_template, request, jsonify, Response
from flask_httpauth import HTTPBasicAuth

from tts_helper import text_to_pcm_float32

# ============================================================
# НАСТРОЙКИ
# ============================================================
PROXY_URL = "ws://localhost:5002/"
SHORT_LIMIT_BYTES = 30
DB_FILE = os.path.join(os.path.dirname(__file__), "chat_history.db")

# Внешний RAG-адаптер (Feon1/chat) — эндпоинт /query, формат {"message": "...", "user_id": "..."}
#RAG_URL = os.getenv("RAG_URL", "https://docker-new-chat.onrender.com")
#RAG_TIMEOUT = 60.0
# Внешний RAG-адаптер (Feon1/chat)
#RAG_URL = os.getenv("RAG_URL", "https://feon-chat.website.yandexcloud.net")
#RAG_ENDPOINT = os.getenv("RAG_ENDPOINT", "/query")   # ← уточните после проверки
#RAG_TIMEOUT = 60.0
# Внешний RAG-адаптер (Yandex Cloud)
RAG_URL = os.getenv("RAG_URL", "https://d5dq57ou1bsu78horuke.avjje9e3.apigw.yandexcloud.net/query")
RAG_TIMEOUT = 60.0
YANDEX_API_KEY = os.getenv("YANDEX_API_KEY", "")
# Пароль
USERNAME = os.getenv("CHAT_USER", "admin")
PASSWORD = os.getenv("CHAT_PASS", "xiaozhi123")

app = Flask(__name__)
auth = HTTPBasicAuth()

# Очереди
sse_queue = queue.Queue()
send_queue = queue.Queue()

# Состояние
ws_ready = False
session_id = None
last_short_text = None       # для отката detect → TTS
last_user_question = None    # для fallback на RAG
rag_in_progress = False      # защита от дублирования запросов


# ============================================================
# АВТОРИЗАЦИЯ
# ============================================================
@auth.verify_password
def verify_password(username, password):
    if username == USERNAME and password == PASSWORD:
        return username
    return None


# ============================================================
# БАЗА ДАННЫХ
# ============================================================
def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS messages
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  role TEXT NOT NULL,
                  text TEXT NOT NULL,
                  timestamp REAL NOT NULL)''')
    conn.commit()
    conn.close()


def save_message(role, text):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("INSERT INTO messages (role, text, timestamp) VALUES (?, ?, ?)",
              (role, text, time.time()))
    conn.commit()
    conn.close()


def get_history(limit=100):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT role, text FROM messages ORDER BY id DESC LIMIT ?", (limit,))
    rows = c.fetchall()
    conn.close()
    return [{"role": r[0], "text": r[1]} for r in reversed(rows)]


def clear_history():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("DELETE FROM messages")
    conn.commit()
    conn.close()


# ============================================================
# SSE-СОБЫТИЯ
# ============================================================
def push_event(kind, **payload):
    sse_queue.put({"kind": kind, **payload})


# ============================================================
# ВНЕШНИЙ RAG (Feon1/chat)
# ============================================================
async def ask_external_rag(question: str) -> str:
    """POST /query → {"message": "...", "user_id": "..."} → {"response": "..."}"""
    try:
        print(f"🌐 [RAG] Отправляю: {question[:60]}...")
        async with httpx.AsyncClient(timeout=RAG_TIMEOUT) as client:
            resp = await client.post(
                f"{RAG_URL}/query",
                json={"message": question, "user_id": "xiaozhi_web"},
            )
            resp.raise_for_status()
            data = resp.json()
            answer = data.get("response")
            if not answer:
                print(f"⚠️ [RAG] Пустой ответ: {data}")
                return None
            print(f"✅ [RAG] Ответ ({len(answer)} симв.)")
            return answer
    except Exception as e:
        print(f"❌ [RAG] Ошибка: {e}")
        return None


async def handle_rag_fallback(question: str):
    """Запрашивает ответ у внешнего RAG и отправляет его в чат."""
    global rag_in_progress
    if rag_in_progress:
        return
    rag_in_progress = True
    try:
        push_event("status", text="🌐 Ищу ответ во внешней базе...")
        answer = await ask_external_rag(question)
        if answer:
            push_event("tts_start")
            push_event("tts", state="sentence_start",
                       text=f"🌐 Внешний источник: {answer}")
            push_event("tts", state="sentence_end", text="")
            push_event("tts_stop")
        else:
            push_event("error", text="Внешний источник не ответил")
    finally:
        rag_in_progress = False


# ============================================================
# WEBSOCKET-КЛИЕНТ К ПРОКСИ
# ============================================================
async def ws_recv_loop(ws):
    global session_id, last_short_text, last_user_question
    async for msg in ws:
        if not isinstance(msg, str):
            continue  # бинарные (аудио) — игнорируем

        try:
            data = json.loads(msg)
        except json.JSONDecodeError:
            continue

        t = data.get("type")

        if t == "hello":
            session_id = data.get("session_id")
            print(f"✅ Session ID: {session_id}")
            push_event("status", text=f"Подключено (session {session_id})")
            continue

        if t == "alert":
            alert_msg = data.get("message", "alert")
            if "wake words" in alert_msg.lower() or "detect" in alert_msg.lower():
                if last_short_text:
                    print(f"⚠️ detect отклонил '{last_short_text}' → повторяю через TTS")
                    push_event("status", text="Обхожу ограничение через TTS...")
                    text_to_resend = last_short_text
                    last_short_text = None
                    send_long_text(text_to_resend)
                else:
                    push_event("error", text=alert_msg)
            else:
                push_event("error", text=alert_msg)
            continue

        if t == "stt":
            push_event("stt", text=data.get("text", ""))
            continue

        if t == "llm":
            push_event("llm", text=data.get("text", ""), emotion=data.get("emotion"))
            continue

        if t == "tts":
            state = data.get("state")
            text = data.get("text", "")

            if state in ("sentence_start", "sentence_end") and text:
                push_event("tts", state=state, text=text)

                # Проверка на "нет информации" от Феофана
                markers = [
                    "нет информации",
                    "не найдено",
                    "не могу найти",
                    "в базе знаний нет",
                    "отсутствует информация",
                    "не содержится",
                    "не упоминается",
                    "нет данных",
                    "не удалось найти",
                ]
                if (state == "sentence_end"
                        and last_user_question
                        and any(m in text.lower() for m in markers)):
                    print(f"🔄 [RAG] Феофан не нашёл → fallback")
                    q = last_user_question
                    asyncio.create_task(handle_rag_fallback(q))

            elif state == "start":
                push_event("tts_start")
            elif state == "stop":
                push_event("tts_stop")
            continue


async def ws_send_loop(ws):
    while True:
        try:
            item = send_queue.get_nowait()
        except queue.Empty:
            await asyncio.sleep(0.05)
            continue
        try:
            if item["kind"] == "text":
                await ws.send(json.dumps(item["data"]))
            elif item["kind"] == "bytes":
                await ws.send(item["data"])
        except Exception as e:
            print(f"Ошибка отправки: {e}")
            return


async def ws_worker():
    global ws_ready
    retry = 0
    while True:
        try:
            print(f"🔌 Подключение к прокси {PROXY_URL}...")
            push_event("status", text="Подключение к прокси...")
            async with websockets.connect(PROXY_URL) as ws:
                ws_ready = True
                retry = 0
                print("✅ Подключено к прокси")

                hello_msg = {
                    "type": "hello",
                    "version": 3,
                    "device_id": "00:1d:92:46:11:12",
                    "device_mac": "00:1d:92:46:11:12",
                    "token": "test-token",
                    "audio_params": {
                        "format": "opus",
                        "sample_rate": 16000,
                        "channels": 1,
                        "frame_duration": 60
                    }
                }
                await ws.send(json.dumps(hello_msg))
                print("📤 Отправлено hello")

                recv = asyncio.create_task(ws_recv_loop(ws))
                send = asyncio.create_task(ws_send_loop(ws))
                done, pending = await asyncio.wait(
                    [recv, send], return_when=asyncio.FIRST_COMPLETED
                )
                for t in pending:
                    t.cancel()
        except Exception as e:
            print(f"❌ Ошибка WebSocket: {e}")
            push_event("error", text=f"Ошибка подключения: {e}")
        finally:
            ws_ready = False

        retry += 1
        wait = min(2 ** retry, 15)
        push_event("status", text=f"Переподключение через {wait} сек...")
        await asyncio.sleep(wait)


def start_ws_thread():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(ws_worker())


# ============================================================
# ОТПРАВКА СООБЩЕНИЙ
# ============================================================
def send_short_text(text: str):
    """Короткий текст — через detect."""
    global last_short_text
    last_short_text = text
    send_queue.put({
        "kind": "text",
        "data": {
            "type": "listen",
            "state": "detect",
            "text": text,
            "source": "text"
        }
    })
    print(f"📤 [detect] {text}")


def send_long_text(text: str):
    """Длинный текст — TTS → PCM → аудио."""
    global last_short_text
    last_short_text = None

    def worker():
        try:
            print(f"🎤 [TTS] Генерирую аудио для: {text[:60]}...")
            push_event("status", text="Генерирую аудио...")

            audio = text_to_pcm_float32(text, target_rate=16000)
            print(f"🎤 [TTS] Получено {len(audio)} сэмплов ({len(audio)/16000:.2f} сек)")

            send_queue.put({
                "kind": "text",
                "data": {"type": "listen", "state": "start"}
            })
            time.sleep(0.2)

            CHUNK_SAMPLES = 960
            CHUNK_SECONDS = 0.06
            total_chunks = (len(audio) + CHUNK_SAMPLES - 1) // CHUNK_SAMPLES
            print(f"📤 [audio] Отправляю {total_chunks} чанков")

            for i in range(0, len(audio), CHUNK_SAMPLES):
                chunk = audio[i:i + CHUNK_SAMPLES]
                if len(chunk) < CHUNK_SAMPLES:
                    padded = np.zeros(CHUNK_SAMPLES, dtype=np.float32)
                    padded[:len(chunk)] = chunk
                    chunk = padded
                send_queue.put({"kind": "bytes", "data": chunk.astype(np.float32).tobytes()})
                time.sleep(CHUNK_SECONDS)

            time.sleep(0.3)
            send_queue.put({"kind": "text", "data": {"type": "listen", "state": "stop"}})
            print(f"✅ [audio] Все {total_chunks} чанков отправлены")
            push_event("status", text="Готово")
        except Exception as e:
            print(f"❌ Ошибка TTS: {e}")
            push_event("error", text=f"Ошибка TTS: {e}")

    threading.Thread(target=worker, daemon=True).start()


# ============================================================
# FLASK ROUTES
# ============================================================
@app.route("/")
@auth.login_required
def index():
    return render_template("chat.html")


@app.route("/send", methods=["POST"])
@auth.login_required
def send():
    global last_user_question
    data = request.get_json()
    text = (data.get("text") or "").strip()
    if not text:
        return jsonify({"ok": False, "error": "Пустой текст"})
    if not ws_ready:
        return jsonify({"ok": False, "error": "Прокси не подключён"})

    save_message("user", text)
    last_user_question = text

    length_bytes = len(text.encode("utf-8"))

    if length_bytes <= SHORT_LIMIT_BYTES:
        send_short_text(text)
        mode = "detect"
    else:
        send_long_text(text)
        mode = "tts"

    return jsonify({"ok": True, "mode": mode, "bytes": length_bytes})


@app.route("/events")
@auth.login_required
def events():
    def stream():
        yield f"data: {json.dumps({'kind': 'status', 'text': 'Подключено' if ws_ready else 'Прокси не подключён'})}\n\n"
        while True:
            try:
                item = sse_queue.get(timeout=20)
                if item.get("kind") == "tts" and item.get("state") == "sentence_start":
                    save_message("ai", item.get("text", ""))
                yield f"data: {json.dumps(item, ensure_ascii=False)}\n\n"
            except queue.Empty:
                yield ": keepalive\n\n"
    return Response(stream(), mimetype="text/event-stream")


@app.route("/history")
@auth.login_required
def history_route():
    return jsonify(get_history())


@app.route("/clear", methods=["POST"])
@auth.login_required
def clear():
    clear_history()
    return jsonify({"ok": True})


# ============================================================
# ЗАПУСК
# ============================================================
if __name__ == "__main__":
    init_db()
    threading.Thread(target=start_ws_thread, daemon=True).start()

    port = int(os.getenv("PORT", 5003))
    print("=" * 60)
    print(f"  Xiaozhi Chat Server (port {port})")
    print(f"  Пароль: {USERNAME} / {PASSWORD}")
    print(f"  RAG:    {RAG_URL}/query")
    print("=" * 60)

    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)

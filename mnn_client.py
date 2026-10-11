"""Клиент для локального MNN Chat API (OpenAI-совместимый)."""
import os
import httpx
import asyncio

# Настройки из переменных окружения (или значения по умолчанию)
MNN_API_URL = os.getenv("MNN_API_URL", "https://tailscale-termux.tailda5e94.ts.net/v1")
MNN_API_KEY = os.getenv("MNN_API_KEY", "Vd9amgCZJcM8Z84S")  # смените на свой!

async def fetch_models() -> list[str]:
    """Возвращает список доступных моделей из MNN Chat."""
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(
                f"{MNN_API_URL}/models",
                headers={"Authorization": f"Bearer {MNN_API_KEY}"}
            )
            resp.raise_for_status()
            data = resp.json()
            return [m["id"] for m in data.get("data", [])]
    except Exception as e:
        print(f"❌ [MNN] Ошибка получения моделей: {e}")
        return []

async def ask_mnn(prompt: str, model: str) -> str:
    """Отправляет запрос к выбранной модели и возвращает ответ."""
    try:
        async with httpx.AsyncClient(timeout=120.0) as client:
            resp = await client.post(
                f"{MNN_API_URL}/chat/completions",
                headers={
                    "Authorization": f"Bearer {MNN_API_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": model,
                    "messages": [{"role": "user", "content": prompt}],
                    "stream": False,
                },
            )
            resp.raise_for_status()
            data = resp.json()
            return data["choices"][0]["message"]["content"]
    except Exception as e:
        print(f"❌ [MNN] Ошибка запроса: {e}")
        return f"[Ошибка MNN: {e}]"

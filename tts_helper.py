"""TTS через edge-tts (Microsoft) + miniaudio (декодирование MP3)."""
import asyncio
import os
import tempfile
import numpy as np
import edge_tts
import miniaudio


# Русские голоса edge-tts:
#   ru-RU-SvetlanaNeural  — женский
#   ru-RU-DmitryNeural    — мужской
VOICE = "ru-RU-SvetlanaNeural"


def text_to_pcm_float32(text: str, target_rate: int = 16000):
    """
    Преобразует текст в PCM Float32 через edge-tts.
    Возвращает numpy-массив float32 (нормализованный от -1 до 1), моно, 16 кГц.
    """
    if not text or not text.strip():
        raise ValueError("Пустой текст")

    tmp_fd, tmp_path = tempfile.mkstemp(suffix='.mp3', prefix='tts_')
    os.close(tmp_fd)

    try:
        # edge-tts асинхронный — запускаем в отдельном event loop
        async def generate():
            communicate = edge_tts.Communicate(text, VOICE)
            await communicate.save(tmp_path)

        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(generate())
        finally:
            loop.close()

        # Декодируем MP3 → PCM 16 kHz моно
        decoded = miniaudio.decode_file(
            tmp_path,
            output_format=miniaudio.SampleFormat.SIGNED16,
            nchannels=1,
            sample_rate=target_rate,
        )
        samples = decoded.samples  # array.array('h')
        audio_int16 = np.frombuffer(samples.tobytes(), dtype=np.int16)
        audio = audio_int16.astype(np.float32) / 32768.0
    finally:
        try:
            os.remove(tmp_path)
        except Exception:
            pass

    if len(audio) == 0:
        raise RuntimeError("edge-tts вернул пустое аудио")

    return audio
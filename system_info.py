"""Кроссплатформенная загрузка opus (Windows/Linux)."""
import os
import sys
import platform


def setup_opus():
    """Загружает opus на Windows. На Linux ничего не делает."""
    if platform.system() == "Windows":
        # Windows: нужно вручную загрузить opus.dll
        try:
            import ctypes
            script_dir = os.path.dirname(os.path.abspath(__file__))
            candidates = [
                os.path.join(script_dir, "libs", "windows", "opus.dll"),
                os.path.join(script_dir, "libs", "libopus", "opus.dll"),
                os.path.join(script_dir, "opus.dll"),
            ]
            for path in candidates:
                if os.path.exists(path):
                    ctypes.CDLL(path)
                    print(f"找到 opus 库文件: {path}")
                    print(f"成功加载 opus 库: {path}")
                    return
            print("⚠️ opus.dll не найден на Windows")
        except Exception as e:
            print(f"⚠️ Ошибка загрузки opus.dll: {e}")
    else:
        # Linux (включая Docker на Render): libopus уже установлен через apt-get
        print("Linux: opus загружается автоматически (libopus.so)")

import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
IS_VERCEL = os.getenv("VERCEL") == "1"


def get_data_dir() -> str:
    if IS_VERCEL:
        return os.getenv("DATA_DIR", "/tmp/nginx-monitor")
    return BASE_DIR


def ensure_data_dir() -> str:
    data_dir = get_data_dir()
    os.makedirs(data_dir, exist_ok=True)
    return data_dir

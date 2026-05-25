from dotenv import load_dotenv as _load_dotenv


def load_dotenv(path: str = ".env") -> None:
    _load_dotenv(path, override=False)

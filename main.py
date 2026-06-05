from routes import app
from log import log_activity, log_exception

if __name__ == "__main__":
    import uvicorn
    import os
    port = int(os.getenv("PORT", "4000"))
    log_activity(f"Starting Nginx Monitor server: host=0.0.0.0 port={port}")
    try:
        uvicorn.run(app, host="0.0.0.0", port=port)
    except Exception:
        log_exception("Uvicorn server terminated unexpectedly")
        raise


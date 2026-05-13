from routes import app
from log import log_activity, log_exception

if __name__ == "__main__":
    import uvicorn
    log_activity("Starting Nginx Monitor server: host=0.0.0.0 port=8000")
    try:
        uvicorn.run(app, host="0.0.0.0", port=8000)
    except Exception:
        log_exception("Uvicorn server terminated unexpectedly")
        raise

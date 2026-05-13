from routes import app
from log import logger

if __name__ == "__main__":
    import uvicorn
    logger.info("Starting Nginx Monitor server on port 8000")
    uvicorn.run(app, host="0.0.0.0", port=8000)
from fastapi import WebSocket
from typing import List
from log import log_activity, log_error

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        log_activity(f"New WebSocket client connected. Active connections: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
            log_activity(f"WebSocket client disconnected. Active connections: {len(self.active_connections)}")

    async def broadcast(self, message: dict):
        # We broadcast to all connections, removing any stale/disconnected ones
        disconnected = []
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception as e:
                # Log issues but keep broadcasting to remaining clients
                log_error(f"Failed to send message over WebSocket: {e}")
                disconnected.append(connection)

        # Cleanup disconnected clients
        for connection in disconnected:
            self.disconnect(connection)

manager = ConnectionManager()

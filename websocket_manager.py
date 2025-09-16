from typing import List
from fastapi import WebSocket

class ConnectionManager:
    """Gestionează conexiunile WebSocket active."""
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        """Acceptă o nouă conexiune."""
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        """Închide o conexiune."""
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        """Trimite un mesaj JSON către toți clienții conectați."""
        # Creăm o copie a listei pentru a evita probleme dacă un client se deconectează în timpul broadcast-ului
        for connection in list(self.active_connections):
            try:
                await connection.send_json(message)
            except Exception:
                # Dacă trimiterea eșuează, deconectăm clientul
                self.disconnect(connection)

# Creează o instanță globală a managerului
manager = ConnectionManager()
from pydantic import BaseModel
from typing import Optional, Dict, Any

class TrackingStatus(BaseModel):
    raw_status: str
    details: Optional[str] = None
    delivered: bool = False
    refused: bool = False
    canceled: bool = False
    raw_response: Optional[Dict[str, Any]] = None
    # --- MODIFICARE AICI ---
    # Adăugăm câmpul pentru statusul standardizat (ex: 'delivered', 'in_transit')
    derived_status: Optional[str] = None
    # --- FINAL MODIFICARE ---
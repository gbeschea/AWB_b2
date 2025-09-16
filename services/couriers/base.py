# /services/couriers/base.py
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
from datetime import datetime
import httpx
from models import Order
from sqlalchemy.ext.asyncio import AsyncSession

class TrackingResponse:
    """O clasă pentru a standardiza răspunsurile de la API-urile de tracking."""
    def __init__(self, status: str, date: Optional[datetime], raw_data: Optional[Dict] = None):
        self.status = status
        self.date = date
        self.raw_data = raw_data

class BaseCourier(ABC):
    """Clasa de bază abstractă pentru toți curierii."""
    def __init__(self, client: httpx.AsyncClient):
        self.client = client

    @abstractmethod
    async def create_awb(self, db: AsyncSession, order: Order, account_key: str) -> Dict[str, Any]:
        """Generează un AWB nou pentru o comandă."""
        raise NotImplementedError

    @abstractmethod
    async def track_awb(self, awb: str, account_key: Optional[str]) -> TrackingResponse:
        """Returnează statusul curent pentru un AWB specific."""
        raise NotImplementedError

    @abstractmethod
    async def get_label(self, awb: str, creds: dict, paper_size: str) -> bytes:
        """Metodă standard pentru a descărca o etichetă PDF."""
        raise NotImplementedError
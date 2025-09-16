# /services/couriers/sameday.py

import asyncio
import logging
from typing import Optional, Dict, Any
from datetime import datetime, timezone, timedelta
import httpx
from sqlalchemy.ext.asyncio import AsyncSession
from models import Order
from .base import BaseCourier, TrackingResponse
from settings import settings

class SamedayCourier(BaseCourier):
    def __init__(self, client: httpx.AsyncClient):
        super().__init__(client)
        # Dicționar pentru a stoca token-uri în cache per cont (user)
        self._tokens: Dict[str, Dict[str, Any]] = {}
        self._token_lock = asyncio.Lock()
        self._rate_limit_interval: float = 0.5 # Pauza pentru tracking

    async def _get_token(self, creds: dict) -> Optional[str]:
        """Obține și gestionează token-ul de autentificare pentru un cont specific."""
        username = creds.get('username')
        if not username:
            raise Exception("Numele de utilizator Sameday lipsește din credențiale.")

        async with self._token_lock:
            if username in self._tokens:
                token_info = self._tokens[username]
                if datetime.now(timezone.utc) < token_info['expires_at']:
                    return token_info['token']
            
            try:
                headers = {'X-Auth-Username': creds['username'], 'X-Auth-Password': creds.get('password')}
                response = await self.client.post('https://api.sameday.ro/api/authenticate', headers=headers)
                response.raise_for_status()
                token = response.json().get("token")
                self._tokens[username] = {"token": token, "expires_at": datetime.now(timezone.utc) + timedelta(minutes=55)}
                return token
            except Exception as e:
                logging.error(f"Eroare la autentificare Sameday pentru user {username}: {e}")
                return None

    async def create_awb(self, db: AsyncSession, order: Order, account_key: str) -> Dict[str, Any]:
        raise NotImplementedError("Funcția create_awb nu este implementată.")

    async def track_awb(self, awb: str, account_key: Optional[str]) -> TrackingResponse:
        """Logica ta originală de tracking, acum integrată corect."""
        # Pentru tracking, folosim credențialele globale din `settings`
        token = await self._get_token(settings.SAMEDAY_CREDS)
        if not token:
            return TrackingResponse(status='Eroare Autentificare', date=None)

        try:
            await asyncio.sleep(self._rate_limit_interval)
            url = f'https://api.sameday.ro/api/client/awb/{awb}/status'
            r = await self.client.get(url, headers={'X-AUTH-TOKEN': token}, timeout=15.0)
            if r.status_code != 200:
                return TrackingResponse(status=f'HTTP {r.status_code}', date=None)
            
            history = r.json().get('expeditionHistory', [])
            if not history:
                return TrackingResponse(status='AWB Generat', date=None)

            latest_event = max(history, key=lambda e: datetime.fromisoformat(e['statusDate'].replace('Z', '+00:00')))
            return TrackingResponse(status=latest_event['statusLabel'], date=datetime.fromisoformat(latest_event['statusDate'].replace('Z', '+00:00')))
        except Exception as e:
            logging.error(f"Excepție la tracking Sameday AWB {awb}: {e}")
            return TrackingResponse(status='Eroare Tracking', date=None)


    async def get_label(self, awb: str, creds: dict, paper_size: str) -> bytes:
        """Funcția de printare, care folosește credențialele primite din DB."""
        token = await self._get_token(creds)
        if not token:
            raise Exception("Autentificare Sameday eșuată.")
        
        size = "A6" if (paper_size or 'a6').lower() == 'a6' else "A4"
        url = f"https://api.sameday.ro/api/awb/download/{awb}/{size}"
        
        try:
            headers = {'X-AUTH-TOKEN': token}
            res = await self.client.get(url, headers=headers, timeout=30)
            res.raise_for_status()
            if 'application/pdf' in res.headers.get('content-type', ''):
                return res.content
            raise Exception("Răspunsul de la Sameday nu este un PDF.")
        except httpx.HTTPStatusError as e:
            raise Exception(f"Eroare API Sameday: {e.response.status_code} - {e.response.text}")
        except Exception as e:
            raise Exception(f"Eroare la descărcarea etichetei Sameday: {e}")
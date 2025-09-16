# services/couriers/econt.py
import io
import logging
from typing import Optional
from datetime import datetime
from .base import BaseCourier, LabelResponse, TrackingResponse
from settings import settings

class EcontCourier(BaseCourier):
    async def get_label(self, awb: str, account_key: Optional[str], paper_size: str) -> LabelResponse:
        # API-ul Econt nu pare să aibă un endpoint standard pentru PDF A6.
        # Această funcție returnează o eroare controlată.
        # Poate fi implementată ulterior dacă se găsește o soluție (ex: generare PDF custom).
        return LabelResponse(
            success=False, 
            error_message="Generarea de etichete PDF pentru Econt nu este implementată."
        )

    async def track_awb(self, awb: str, account_key: Optional[str]) -> TrackingResponse:
        creds = settings.ECONT_CREDS
        if not creds:
            return TrackingResponse(status='Cont Necunoscut', date=None)

        body = { 'username': creds['username'], 'password': creds['password'], 'shipmentNumbers': [awb] }
        try:
            r = await self.client.post(
                'https://ee.econt.com/services/Shipments/ShipmentService.getShipmentStatuses.json', 
                json=body, 
                timeout=20.0
            )
            if r.status_code != 200:
                return TrackingResponse(status=f'HTTP {r.status_code}', date=None)
            
            data = r.json() or {}
            status_data = (data.get('shipmentStatuses', [{}])[0]).get('status', {})
            description = (status_data.get('shortDeliveryStatusEn') or 'N/A').strip()
            # API-ul Econt nu returnează data în format standard, o lăsăm None.
            event_time = None 
            
            return TrackingResponse(status=description, date=event_time, raw_data=data)
        except Exception as e:
            logging.error(f"Eroare la procesarea AWB Econt {awb}: {e}")
            return TrackingResponse(status=f"Eroare la tracking Econt: {e}", date=None)
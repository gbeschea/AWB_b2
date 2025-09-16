# /services/label_service.py

import io
import asyncio
from typing import List, Tuple, Dict, Union
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from PyPDF2 import PdfMerger
import models

# Importăm "fabrica" de servicii de curierat, piesa centrală
from services.couriers import get_courier_service

async def fetch_label_with_correct_architecture(db: AsyncSession, shipment: models.Shipment) -> Union[bytes, str]:
    """Orchestrează descărcarea unei etichete folosind arhitectura corectă."""
    try:
        courier_service = get_courier_service(shipment.courier)
        if not courier_service:
            return f"Serviciu neimplementat pentru '{shipment.courier}'"

        stmt = select(models.CourierAccount).where(models.CourierAccount.account_key == shipment.account_key)
        result = await db.execute(stmt)
        account = result.scalars().first()
        if not (account and account.credentials):
            return f"Credențiale lipsă pentru contul '{shipment.account_key}'"

        # Apelăm metoda standardizată `get_label` de pe serviciul de curierat
        return await courier_service.get_label(
            awb=shipment.awb, 
            creds=account.credentials,
            paper_size=shipment.paper_size
        )

    except Exception as e:
        return f"Eroare la procesarea AWB {shipment.awb}: {e}"

async def generate_labels_pdf(db: AsyncSession, shipments: List[models.Shipment]) -> Tuple[Dict[str, bytes], Dict[str, str]]:
    """Funcția principală care orchestrează descărcarea tuturor etichetelor."""
    awb_to_pdf_map, failed_awbs_map = {}, {}
    
    tasks = [fetch_label_with_correct_architecture(db, shipment) for shipment in shipments]
    results = await asyncio.gather(*tasks)
    
    for i, result in enumerate(results):
        awb = shipments[i].awb or f"CMD_{shipments[i].order_id}"
        if isinstance(result, bytes):
            awb_to_pdf_map[awb] = result
        else:
            failed_awbs_map[awb] = str(result)
            
    return awb_to_pdf_map, failed_awbs_map

def merge_labels(pdf_map: Dict[str, bytes]) -> bytes:
    """Combină PDF-urile într-unul singur."""
    if not pdf_map: return b''
    merger = PdfMerger()
    for awb, pdf_bytes in pdf_map.items():
        if pdf_bytes:
            try: merger.append(io.BytesIO(pdf_bytes))
            except Exception as e: print(f"PDF invalid pentru AWB {awb}: {e}")
    output_buffer = io.BytesIO()
    merger.write(output_buffer)
    merger.close()
    return output_buffer.getvalue()
# /services/courier_service.py

import asyncio
import logging
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload
import models
from services.couriers import get_courier_service  # Importăm funcția corectă din __init__.py

# ==============================================================================
# Funcția de care are nevoie `label_service`
# O re-exportăm de aici pentru a avea un singur punct de acces
# ==============================================================================

def get_courier_service_by_name(courier_name: str):
    """
    Wrapper peste funcția din `services/couriers` pentru a oferi un
    punct de acces consistent.
    """
    service = get_courier_service(courier_name)
    if not service:
        raise ValueError(f"Niciun serviciu de curierat găsit pentru numele: {courier_name}")
    return service

# ==============================================================================
# Funcția ta originală de tracking (neschimbată, doar adăugată aici)
# ==============================================================================

async def track_and_update_shipments(db: AsyncSession, full_sync: bool = False):
    logging.warning("--- COURIER SYNC A PORNIT ---")
    
    final_statuses = ['Livrat', 'Refuzat', 'Returnat']
    stmt = select(models.Shipment).options(
        joinedload(models.Shipment.order)
    ).where(models.Shipment.last_status.is_(None) | ~models.Shipment.last_status.in_(final_statuses))
    
    result = await db.execute(stmt)
    shipments_to_track = result.scalars().unique().all()

    if not shipments_to_track:
        logging.warning("COURIER SYNC: Nu s-au găsit livrări care necesită actualizare.")
        return

    total_count = len(shipments_to_track)
    logging.warning(f"COURIER SYNC: S-au găsit {total_count} livrări pentru verificare.")

    processed_count = 0
    updated_count = 0

    async def process_shipment(shipment: models.Shipment):
        nonlocal processed_count, updated_count
        
        try:
            courier_service = get_courier_service(shipment.courier)
            if not courier_service:
                logging.warning(f"   -> Nu s-a găsit serviciu pentru curierul '{shipment.courier}' (AWB: {shipment.awb})")
                return

            response = await courier_service.track_awb(shipment.awb, shipment.account_key)
            
            processed_count += 1
            if processed_count % 20 == 0 or processed_count == total_count:
                logging.warning(f"   -> Procesat {processed_count}/{total_count} AWB-uri...")

            if response and response.status and response.status != shipment.last_status:
                shipment.last_status = response.status
                shipment.last_status_at = response.date
                updated_count += 1
        except Exception as e:
            logging.error(f"Eroare la procesarea AWB {shipment.awb}: {e}")

    tasks = [process_shipment(s) for s in shipments_to_track]
    await asyncio.gather(*tasks)

    if updated_count > 0:
        logging.warning(f"COURIER SYNC: Se salvează {updated_count} statusuri noi în baza de date...")
        await db.commit()
    else:
        logging.warning("COURIER SYNC: Nu a fost găsit niciun status nou de actualizat.")

    logging.warning(f"--- COURIER SYNC FINALIZAT ---")
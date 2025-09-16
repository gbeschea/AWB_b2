# routes/background.py

import asyncio
import logging
from typing import List
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload
from sqlalchemy import select
from fastapi import APIRouter

import models
from services import shopify_service
from settings import settings # <-- IMPORTUL CORECT: Folosim 'settings.py'

# Definim un router pentru consistență
router = APIRouter()

async def update_shopify_in_background(db: AsyncSession, awb_list: List[str]):
    """Notifică Shopify despre AWB-urile procesate, folosind date din DB."""
    if not awb_list:
        logging.info("Niciun AWB de procesat în background.")
        return

    # Preluăm expedierile și încărcăm relațiile cu comanda și magazinul
    stmt = (
        select(models.Shipment)
        .options(
            joinedload(models.Shipment.order)
            .joinedload(models.Order.store)
        )
        .where(models.Shipment.awb.in_(awb_list))
    )
    result = await db.execute(stmt)
    shipments = result.unique().scalars().all()

    # Preluăm harta curierilor din setări, nu dintr-un fișier 'config'
    courier_display_names = {v: k for k, v in settings.COURIER_MAP.items()}
    
    update_tasks = []
    for ship in shipments:
        # Verificăm dacă avem toate datele necesare direct din obiectele încărcate
        if not (ship.order and ship.order.store and ship.order.shopify_order_id and ship.shopify_fulfillment_id):
            logging.warning(f"Date incomplete pentru AWB {ship.awb}, comanda ID {ship.order_id}. Sar peste notificare.")
            continue
        
        # Folosim direct 'ship.order.store' ca și configurație, citit din baza de date.
        # NU mai avem nevoie de SHOPIFY_STORES.
        store_config = ship.order.store
            
        tracking_url = f"https://sameday.ro/track-awb/{ship.awb}" if 'sameday' in (ship.courier or '').lower() else f"https://tracking.dpd.ro?shipmentNumber={ship.awb}"
        tracking_info = {
            "company": courier_display_names.get(ship.courier, ship.courier),
            "number": ship.awb,
            "url": tracking_url
        }
        
        task = shopify_service.notify_shopify_of_shipment(
            store_cfg=store_config, # Trimitem direct obiectul SQLAlchemy
            order_gid=f"gid://shopify/Order/{ship.order.shopify_order_id}",
            fulfillment_id=ship.shopify_fulfillment_id,
            tracking_info=tracking_info
        )
        update_tasks.append(task)
        
    if update_tasks:
        await asyncio.gather(*update_tasks)
        
    logging.info(f"✅ Notificarea Shopify pentru {len(update_tasks)} expedieri a fost finalizată.")
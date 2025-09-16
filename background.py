# background.py
import asyncio
import logging
from typing import List
from sqlalchemy.orm import joinedload
from sqlalchemy import select
import models
from services import shopify_service
from settings import settings
from database import AsyncSessionLocal

async def _do_update_work(session, awb_list: List[str]):
    """Funcția care conține logica efectivă, primind o sesiune validă."""
    shipments_result = await session.execute(
        select(models.Shipment)
        .options(
            joinedload(models.Shipment.order).joinedload(models.Order.store)
        )
        .where(models.Shipment.awb.in_(awb_list))
    )
    shipments = shipments_result.unique().scalars().all()
    
    store_configs = {s.domain: s for s in settings.SHOPIFY_STORES}
    courier_display_names = {v: k for k, v in settings.COURIER_MAP.items()}
    update_tasks = []

    for ship in shipments:
        if not (ship.order and ship.order.store and ship.order.shopify_order_id and ship.shopify_fulfillment_id):
            continue
        store_cfg = store_configs.get(ship.order.store.domain)
        if not store_cfg:
            continue
        
        tracking_url = f"https://sameday.ro/track-awb/{ship.awb}" if 'sameday' in (ship.courier or '').lower() else f"https://tracking.dpd.ro?shipmentNumber={ship.awb}"
        tracking_info = {
            "company": courier_display_names.get(ship.courier, ship.courier),
            "number": ship.awb,
            "url": tracking_url
        }
        order_gid = f"gid://shopify/Order/{ship.order.shopify_order_id}"
        
        task = shopify_service.notify_shopify_of_shipment(
            store_cfg=store_cfg,
            order_gid=order_gid,
            fulfillment_id=ship.shopify_fulfillment_id,
            tracking_info=tracking_info
        )
        update_tasks.append(task)
        
    if update_tasks:
        await asyncio.gather(*update_tasks)

async def update_shopify_in_background(awb_list: List[str]):
    """Wrapper-ul care creează o sesiune nouă pentru task-ul de fundal."""
    logging.info(f"Background task pornit pentru a notifica Shopify pentru {len(awb_list)} AWB-uri.")
    async with AsyncSessionLocal() as session:
        try:
            await _do_update_work(session, awb_list)
            await session.commit()
        except Exception as e:
            logging.error(f"Eroare în task-ul de fundal Shopify: {e}", exc_info=True)
            await session.rollback()
    logging.info(f"✅ Notificarea Shopify pentru {len(awb_list)} expedieri a fost finalizată.")
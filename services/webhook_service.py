import hashlib
import hmac
import base64
import logging
from fastapi import Request
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

# --- MODIFICARE AICI ---
# Am corectat importurile pentru a reflecta noua structură a codului
from services.utils import (
    parse_timestamp,
    get_payment_mapping,
    get_courier_mapping,
    extract_gid
)
# --- FINAL MODIFICARE ---

import models
from settings import settings

async def verify_webhook(request: Request, store_domain: str) -> bool:
    """Verifică dacă un webhook primit de la Shopify este autentic."""
    hmac_header = request.headers.get('X-Shopify-Hmac-Sha256')
    if not hmac_header:
        return False

    store = next((s for s in settings.SHOPIFY_STORES if s.domain == store_domain), None)
    if not store or not store.shared_secret:
        return False

    data = await request.body()
    calculated_hmac = base64.b64encode(
        hmac.new(store.shared_secret.encode(), data, hashlib.sha256).digest()
    ).decode()

    return hmac.compare_digest(calculated_hmac, hmac_header)


async def handle_order_update(payload: dict, db: AsyncSession):
    """Procesează datele dintr-un webhook de tip 'orders/updated'."""
    shopify_order_id = str(payload.get('id'))
    
    stmt = select(models.Order).where(models.Order.shopify_order_id == shopify_order_id)
    result = await db.execute(stmt)
    order = result.scalar_one_or_none()

    if order:
        logging.info(f"Actualizare comandă {order.name} via webhook.")
        order.financial_status = payload.get('financial_status')
        order.shopify_status = payload.get('fulfillment_status')
        order.cancelled_at = parse_timestamp(payload.get('cancelled_at'))
        order.tags = ", ".join(payload.get('tags', []))
        order.note = payload.get('note')
        
        # Actualizează mapările pe baza noilor date
        order.mapped_payment = get_payment_mapping(payload.get('payment_gateway_names', []))
        order.assigned_courier = get_courier_mapping(payload.get('tags', []))
        
        await db.commit()
    else:
        logging.warning(f"Webhook primit pentru o comandă inexistentă în DB: {shopify_order_id}")
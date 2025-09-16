import base64
import hashlib
import hmac
import logging
from typing import Dict, Any # <-- MODIFICAREA ESTE AICI

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request, Header, Response
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

import models
from database import get_db
from services import webhook_service


router = APIRouter(prefix="/webhooks", tags=["Webhooks"])


async def verify_shopify_webhook(
    request: Request,
    db: AsyncSession = Depends(get_db),
    x_shopify_topic: str = Header(...),
    x_shopify_hmac_sha256: str = Header(...),
    x_shopify_shop_domain: str = Header(...),
) -> Dict[str, Any]:
    """
    Dependință partajată pentru a verifica și a pre-procesa toate webhook-urile.
    """
    raw_body = await request.body()
    
    store_res = await db.execute(select(models.Store).where(models.Store.domain == x_shopify_shop_domain))
    store = store_res.scalar_one_or_none()

    if not store or not store.shared_secret:
        logging.error(f"Webhook primit pentru un magazin neconfigurat sau fără secret: {x_shopify_shop_domain}")
        raise HTTPException(status_code=404, detail="Store not configured or missing secret.")

    digest = hmac.new(store.shared_secret.encode("utf-8"), raw_body, hashlib.sha256).digest()
    computed_hmac = base64.b64encode(digest).decode()
    if not hmac.compare_digest(computed_hmac, x_shopify_hmac_sha256):
        logging.error("Verificarea HMAC a eșuat!")
        raise HTTPException(status_code=401, detail="HMAC verification failed.")
    
    payload = await request.json()
    return {"store": store, "payload": payload, "topic": x_shopify_topic, "db": db}

# --- Endpoint-uri Separate pentru Fiecare Topic ---

@router.post("/{topic:path}", include_in_schema=False)
async def receive_generic_webhook(
    background_tasks: BackgroundTasks,
    common: Dict[str, Any] = Depends(verify_shopify_webhook)
):
    """
    Un singur endpoint dinamic care prinde toate căile și le trimite la procesor.
    """
    topic = common["topic"]
    
    # Verificăm dacă există un handler specific pentru acest topic în serviciul nostru
    if topic in webhook_service.WEBHOOK_HANDLERS:
        background_tasks.add_task(
            webhook_service.process_webhook_event,
            db=common["db"],
            topic=topic,
            store_id=common["store"].id,
            payload=common["payload"]
        )
        return Response(status_code=200, content="Webhook received and queued for processing.")
    
    logging.warning(f"Webhook primit pentru un topic neimplementat: {topic}")
    return Response(status_code=404, content="Topic handler not implemented.")
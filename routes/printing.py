# routes/printing.py
import logging
import math
from pathlib import Path
from datetime import datetime, timezone
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import APIRouter, Depends, Request, Form, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import select, func, or_
from starlette.background import BackgroundTasks
import models
from database import get_db
from services import print_service
from background import update_shopify_in_background
from dependencies import get_templates
from settings import settings

router = APIRouter(tags=['Print View'])

@router.get("/print-view", response_class=HTMLResponse)
async def get_print_view_page(request: Request, db: AsyncSession = Depends(get_db), templates: Jinja2Templates = Depends(get_templates)):
    latest_shipment_subq = (select(models.Shipment.order_id, func.max(models.Shipment.id).label("max_id")).group_by(models.Shipment.order_id).alias("latest_shipment_subq"))
    supported_couriers_filter = or_(models.Shipment.courier.ilike('%dpd%'), models.Shipment.courier.ilike('%sameday%'))
    unprinted_counts_query = (
        select(models.StoreCategory.id, func.count(models.Order.id.distinct()))
        .join(models.store_category_map).join(models.Store).join(models.Order)
        .join(models.Shipment, models.Order.id == models.Shipment.order_id)
        .join(latest_shipment_subq, models.Shipment.id == latest_shipment_subq.c.max_id)
        .where(models.Shipment.printed_at.is_(None), models.Shipment.awb.isnot(None), supported_couriers_filter)
        .group_by(models.StoreCategory.id)
    )
    counts_res = await db.execute(unprinted_counts_query)
    counts_dict = dict(counts_res.all())
    categories_res = await db.execute(select(models.StoreCategory).order_by(models.StoreCategory.name))
    categories = categories_res.scalars().all()
    
    total_unprinted = 0
    batch_size = settings.PRINT_BATCH_SIZE
    for cat in categories:
        count = counts_dict.get(cat.id, 0)
        cat.unprinted_count = count
        cat.total_batches = math.ceil(count / batch_size) if count > 0 else 0
        total_unprinted += count
        
    return templates.TemplateResponse("print_view.html", {"request": request, "categories": categories, "total_unprinted": total_unprinted})

@router.post("/print/selected-batches")
async def process_and_print_selected_batches(request: Request, background_tasks: BackgroundTasks, db: AsyncSession = Depends(get_db), category_id: int = Form(...), batch_numbers: str = Form(...)):
    try:
        batch_nums_list = [int(b) for b in batch_numbers.split(',') if b.isdigit()]
        if not batch_nums_list: raise HTTPException(status_code=400, detail="Niciun lot valid selectat.")
    except (ValueError, TypeError):
        raise HTTPException(status_code=400, detail="Format invalid pentru loturi.")

    category = await db.get(models.StoreCategory, category_id)
    if not category: raise HTTPException(status_code=404, detail="Categoria nu a fost găsită.")

    pdf_buffer, successful_awbs, failed_awbs = await print_service.generate_pdf_for_selected_batches(db, category_id, batch_nums_list)
    logging.info(f"Total AWB-uri trimise la procesare: {len(successful_awbs) + len(failed_awbs)}")
    logging.info(f"AWB-uri procesate cu SUCCES ({len(successful_awbs)}): {successful_awbs}")
    if failed_awbs: logging.warning(f"AWB-uri EȘUATE ({len(failed_awbs)}): {failed_awbs}")

    if not successful_awbs:
        error_detail = f"Nu s-a putut genera nicio etichetă. {len(failed_awbs)} AWB-uri au eșuat."
        raise HTTPException(status_code=404, detail=error_detail)

    now = datetime.now(timezone.utc)
    shipments_to_update_res = await db.execute(select(models.Shipment).where(models.Shipment.awb.in_(successful_awbs)))
    shipments_to_update = shipments_to_update_res.scalars().all()
    for shipment in shipments_to_update:
        shipment.printed_at = now
    
    awb_to_order_name_res = await db.execute(select(models.Shipment.awb, models.Order.name).join(models.Order).where(models.Shipment.awb.in_(successful_awbs)))
    awb_to_order_name = dict(awb_to_order_name_res.all())
    
    new_log = models.PrintLog(category_name=category.name, category_id=category.id, awb_count=len(successful_awbs), user_ip=request.client.host)
    db.add(new_log)
    await db.flush() 

    log_entries = [models.PrintLogEntry(print_log_id=new_log.id, awb=awb, order_name=awb_to_order_name.get(awb, 'N/A')) for awb in successful_awbs]
    db.add_all(log_entries)
    
    if pdf_buffer.getbuffer().nbytes > 0:
        today_str = datetime.now().strftime('%Y-%m-%d')
        archive_dir = Path('awb_archive') / today_str
        archive_dir.mkdir(parents=True, exist_ok=True)
        pdf_filename = f"awb_log_{new_log.id}_{int(datetime.now().timestamp())}.pdf"
        pdf_path = archive_dir / pdf_filename
        with open(pdf_path, 'wb') as f: f.write(pdf_buffer.getbuffer())
        new_log.pdf_path = str(pdf_path)

    await db.commit()
    background_tasks.add_task(update_shopify_in_background, successful_awbs)
    return StreamingResponse(pdf_buffer, media_type='application/pdf')
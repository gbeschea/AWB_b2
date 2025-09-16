# routes/logs.py
import math
from pathlib import Path
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import APIRouter, Depends, Request, Query, HTTPException
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import selectinload
from sqlalchemy import select, func
import models
from database import get_db
from dependencies import get_templates, get_pagination_numbers

router = APIRouter(prefix='/logs', tags=['Logs'])

@router.get("/print", response_class=HTMLResponse, name="get_print_logs_page")
async def get_print_logs_page(
    request: Request, db: AsyncSession = Depends(get_db),
    templates: Jinja2Templates = Depends(get_templates), page: int = Query(1, ge=1)
):
    page_size = 25
    logs_query = select(models.PrintLog).options(selectinload(models.PrintLog.entries)).order_by(models.PrintLog.created_at.desc())
    total_logs_res = await db.execute(select(func.count()).select_from(models.PrintLog))
    total_logs = total_logs_res.scalar_one() or 0
    paginated_logs_res = await db.execute(logs_query.offset((page - 1) * page_size).limit(page_size))
    paginated_logs = paginated_logs_res.scalars().all()
    
    for log in paginated_logs:
        awbs = [entry.awb for entry in log.entries]
        if awbs:
            summary_query = (
                select(models.LineItem.sku, func.min(models.LineItem.title).label('title'), func.sum(models.LineItem.quantity).label('total_quantity'))
                .join(models.Order).join(models.Shipment)
                .where(models.Shipment.awb.in_(awbs)).group_by(models.LineItem.sku)
                .order_by(func.sum(models.LineItem.quantity).desc())
            )
            summary_res = await db.execute(summary_query)
            log.summary_items = summary_res.all()
        else:
            log.summary_items = []

    total_pages = (total_logs + page_size - 1) // page_size if total_logs > 0 else 1
    page_numbers = get_pagination_numbers(page, total_pages)
            
    return templates.TemplateResponse("print_logs.html", {
        "request": request, "logs": paginated_logs, "page": page, 
        "total_pages": total_pages, "page_numbers": page_numbers
    })

@router.get("/print/download/{log_id}", response_class=FileResponse, name="download_printed_pdf")
async def download_printed_pdf(log_id: int, db: AsyncSession = Depends(get_db)):
    log_entry = await db.get(models.PrintLog, log_id)
    if not log_entry or not log_entry.pdf_path:
        raise HTTPException(status_code=404, detail="Fișierul PDF nu a fost găsit în log.")
    file_path = Path(log_entry.pdf_path)
    if not file_path.is_file():
        raise HTTPException(status_code=404, detail="Fișierul PDF nu mai există pe disc.")
    return FileResponse(path=file_path, media_type='application/pdf', filename=file_path.name)
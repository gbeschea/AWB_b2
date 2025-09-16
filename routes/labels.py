# /routes/labels.py

import os
from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import Response
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.orm import selectinload
from typing import List
from datetime import datetime
import pytz

from database import get_db
import models
from services import label_service # Managerul principal pentru etichete

router = APIRouter()

# Creăm directorul pentru arhivă dacă nu există
ARCHIVE_DIR = "arhiva_printuri"
os.makedirs(ARCHIVE_DIR, exist_ok=True)

@router.get("/labels/download/{awb}")
async def download_single_label(awb: str, db: AsyncSession = Depends(get_db)):
    """
    Descarcă o singură etichetă direct de la curier, la cerere.
    """
    # Pas 1: Găsim detaliile livrării în baza de date
    stmt = select(models.Shipment).where(models.Shipment.awb == awb)
    result = await db.execute(stmt)
    shipment = result.scalars().first()

    if not shipment:
        raise HTTPException(status_code=404, detail=f"AWB-ul {awb} nu a fost găsit în baza de date.")

    # Pas 2: Folosim `label_service` pentru a descărca PDF-ul de la curier
    # `generate_labels_pdf` funcționează și pentru o singură livrare
    awb_to_pdf_map, failed_awbs_map = await label_service.generate_labels_pdf(db, [shipment])

    # Pas 3: Verificăm dacă descărcarea a reușit
    if awb not in awb_to_pdf_map:
        error_message = failed_awbs_map.get(awb, "Eroare necunoscută la descărcare.")
        raise HTTPException(status_code=500, detail=f"Nu s-a putut descărca eticheta: {error_message}")

    pdf_content = awb_to_pdf_map[awb]

    # Pas 4: Returnăm PDF-ul către utilizator
    return Response(
        content=pdf_content,
        media_type='application/pdf',
        headers={'Content-Disposition': f'inline; filename="AWB_{awb}.pdf"'}
    )


@router.post("/labels/merge_for_print")
async def merge_labels_for_printing(request: Request, awbs: str = Form(...), db: AsyncSession = Depends(get_db)):
    """
    Primește o listă de AWB-uri, descarcă etichetele și le combină într-un PDF.
    (Această funcție este acum corectă).
    """
    if not awbs:
        raise HTTPException(status_code=400, detail="Niciun AWB selectat.")

    awb_list = [awb.strip() for awb in awbs.split(',') if awb.strip()]
    if not awb_list:
        raise HTTPException(status_code=400, detail="Lista de AWB-uri este goală.")

    stmt = select(models.Shipment).options(
        selectinload(models.Shipment.order)
    ).where(models.Shipment.awb.in_(awb_list))
    
    result = await db.execute(stmt)
    shipments_data = result.scalars().unique().all()

    if not shipments_data:
        raise HTTPException(status_code=404, detail="Niciunul dintre AWB-urile selectate nu a fost găsit.")

    awb_to_pdf_map, failed_awbs_map = await label_service.generate_labels_pdf(db, shipments_data)

    if not awb_to_pdf_map:
        error_details = "; ".join([f"{awb}: {reason}" for awb, reason in failed_awbs_map.items()])
        raise HTTPException(status_code=500, detail=f"Nu s-a putut genera nicio etichetă. Detalii: {error_details}")

    combined_pdf_bytes = label_service.merge_labels(awb_to_pdf_map)
    
    # Salvarea locală și logarea (rămân la fel)
    bucharest_tz = pytz.timezone("Europe/Bucharest")
    now = datetime.now(bucharest_tz)
    filename = f"Print_{now.strftime('%Y-%m-%d_%H-%M-%S')}.pdf"
    file_path = os.path.join(ARCHIVE_DIR, filename)
    with open(file_path, "wb") as f:
        f.write(combined_pdf_bytes)

    new_log = models.PrintLog(
        created_at=now,
        category_name="Printare Manuala", 
        awb_count=len(awb_to_pdf_map),
        user_ip=request.client.host,
        pdf_path=file_path
    )
    db.add(new_log)

    for shipment in shipments_data:
        if shipment.awb in awb_to_pdf_map:
            shipment.printed_at = now
            new_log.entries.append(
                models.PrintLogEntry(order_name=shipment.order.name, awb=shipment.awb)
            )

    await db.commit()

    return Response(
        content=combined_pdf_bytes,
        media_type='application/pdf',
        headers={'Content-Disposition': 'inline; filename="AWB-uri_combinate.pdf"'}
    )
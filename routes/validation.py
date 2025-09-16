# /routes/validation.py

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload
from sqlalchemy import or_ # Am adăugat OR

import models
import schemas
from services import address_service
from database import get_db

router = APIRouter(
    prefix="/validation",
    tags=["Validation"],
)

templates = Jinja2Templates(directory="templates")

@router.get("/", name="get_validation_page")
async def get_validation_page(request: Request, db: AsyncSession = Depends(get_db)):
    """
    Afișează pagina Validation Hub cu comenzile care necesită validare.
    (Versiune corectată care încarcă statusurile corecte).
    """
    # === AICI ESTE CORECȚIA CRITICĂ ===
    # Căutăm comenzile care sunt fie 'invalid', fie 'partial_match' sau 'not_found'.
    stmt = (
        select(models.Order)
        .where(
            or_(
                models.Order.address_status == 'invalid',
                models.Order.address_status == 'partial_match',
                models.Order.address_status == 'not_found'
            )
        )
        .options(
            selectinload(models.Order.store),
            selectinload(models.Order.line_items),
            selectinload(models.Order.shipments)
        )
        .order_by(models.Order.created_at.desc())
    )
    
    result = await db.execute(stmt)
    orders_to_validate = result.scalars().unique().all()
    
    context = {
        "request": request,
        "orders": orders_to_validate
    }
    return templates.TemplateResponse("validation.html", context)


@router.post("/validate_address/{order_id}", response_model=schemas.ValidationResult)
async def validate_address_route(
    order_id: int,
    db: AsyncSession = Depends(get_db)
):
    """Validează adresa pentru o comandă specifică."""
    result = await db.execute(select(models.Order).where(models.Order.id == order_id))
    db_order = result.scalar_one_or_none()

    if not db_order:
        raise HTTPException(status_code=404, detail="Comanda nu a fost găsită")

    await address_service.validate_address_for_order(db, db_order)
    
    is_valid = db_order.address_status == 'valid'
    errors = db_order.address_validation_errors if not is_valid else []
    
    await db.commit()

    return schemas.ValidationResult(
        is_valid=is_valid,
        errors=errors,
        suggestions=[]
    )
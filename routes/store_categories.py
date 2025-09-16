# routes/store_categories.py
from fastapi import APIRouter, Depends, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import joinedload
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
import models
from database import get_db
from dependencies import get_templates

router = APIRouter(prefix='/categories', tags=['Store Categories'])

@router.get('', response_class=HTMLResponse)
async def get_categories_page(request: Request, db: AsyncSession = Depends(get_db), templates: Jinja2Templates = Depends(get_templates)):
    categories_res = await db.execute(select(models.StoreCategory).options(joinedload(models.StoreCategory.stores)).order_by(models.StoreCategory.name))
    categories = categories_res.unique().scalars().all()
    stores_res = await db.execute(select(models.Store).order_by(models.Store.name))
    stores = stores_res.scalars().all()
    return templates.TemplateResponse("categories.html", {"request": request, "categories": categories, "stores": stores})

@router.post('', response_class=RedirectResponse)
async def create_category(name: str = Form(...), db: AsyncSession = Depends(get_db)):
    existing_cat_res = await db.execute(select(models.StoreCategory).where(models.StoreCategory.name == name))
    if name.strip() and not existing_cat_res.scalar_one_or_none():
        db.add(models.StoreCategory(name=name))
        await db.commit()
    return RedirectResponse(url="/categories", status_code=303)

@router.post('/assign', response_class=RedirectResponse)
async def assign_stores_to_categories(request: Request, db: AsyncSession = Depends(get_db)):
    form_data = await request.form()
    all_categories_res = await db.execute(select(models.StoreCategory).options(joinedload(models.StoreCategory.stores)))
    all_categories = all_categories_res.unique().scalars().all()
    for category in all_categories:
        assigned_store_ids = {int(id_str) for id_str in form_data.getlist(f"stores_{category.id}")}
        if assigned_store_ids:
            stores_res = await db.execute(select(models.Store).where(models.Store.id.in_(assigned_store_ids)))
            category.stores = stores_res.scalars().all()
        else:
            category.stores = []
    await db.commit()
    return RedirectResponse(url="/categories", status_code=303)

@router.post('/{category_id}/delete', response_class=RedirectResponse)
async def delete_category(category_id: int, db: AsyncSession = Depends(get_db)):
    category = await db.get(models.StoreCategory, category_id)
    if category:
        await db.delete(category)
        await db.commit()
    return RedirectResponse(url="/categories", status_code=303)
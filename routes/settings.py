from typing import List, Optional
from fastapi import APIRouter, Depends, Request, Form, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
import json

from database import get_db
from dependencies import get_templates
from crud import stores as crud_stores
from crud import couriers as crud_couriers

router = APIRouter(
    prefix="/settings",
    tags=['Settings']
)

# --- Rute Generale ---
@router.get("/", response_class=HTMLResponse, name="get_settings_page")
async def settings_page(request: Request, templates: Jinja2Templates = Depends(get_templates)):
    return templates.TemplateResponse("settings.html", {"request": request})

# --- Rute Magazine (Stores) ---
@router.get("/stores", response_class=HTMLResponse, name="get_stores_page")
async def get_stores_page(request: Request, db: AsyncSession = Depends(get_db), templates: Jinja2Templates = Depends(get_templates)):
    all_stores = await crud_stores.get_stores(db)
    all_categories = await crud_stores.get_all_store_categories(db)
    pii_source_options = ["shopify", "metafield"]
    context = {
        "request": request, "stores": all_stores, "all_categories": all_categories,
        "pii_source_options": pii_source_options
    }
    return templates.TemplateResponse("settings_stores.html", context)

@router.post("/stores/{store_id}", name="update_store")
async def update_store_entry(
    store_id: int, db: AsyncSession = Depends(get_db), name: str = Form(...),
    domain: str = Form(...), shared_secret: str = Form(""), access_token: str = Form(""),
    is_active: str = Form("false"), paper_size: str = Form(...),
    dpd_client_id: Optional[str] = Form(None), pii_source: str = Form(...),
    category_ids: List[str] = Form([])
):
    is_active_bool = is_active.lower() == 'true'
    int_category_ids = [int(cat_id) for cat_id in category_ids]
    await crud_stores.update_store(
        db, store_id=store_id, name=name, domain=domain, shared_secret=shared_secret,
        access_token=access_token, is_active=is_active_bool, category_ids=int_category_ids,
        paper_size=paper_size, dpd_client_id=dpd_client_id, pii_source=pii_source
    )
    return RedirectResponse(url="/settings/stores", status_code=303)

@router.post("/stores", name="create_store")
async def create_store(db: AsyncSession = Depends(get_db), name: str = Form(...), domain: str = Form(...), shared_secret: str = Form(...), access_token: str = Form(...)):
    # Această funcție trebuie să existe în crud/stores.py
    await crud_stores.create_store(db, name=name, domain=domain, shared_secret=shared_secret, access_token=access_token)
    return RedirectResponse(url="/settings/stores", status_code=303)


# --- Rute Curieri (Couriers) ---
@router.get("/couriers", response_class=HTMLResponse, name="get_couriers_page")
async def get_couriers_page(request: Request, db: AsyncSession = Depends(get_db), templates: Jinja2Templates = Depends(get_templates)):
    accounts = await crud_couriers.get_courier_accounts(db)
    mappings = await crud_couriers.get_courier_mappings(db)
    categories = await crud_couriers.get_courier_categories(db)
    default_tracking_urls = {cat.name: cat.tracking_url_template for cat in categories if cat.tracking_url_template}
    context = {"request": request, "accounts": accounts, "mappings": mappings, "default_tracking_urls": default_tracking_urls}
    return templates.TemplateResponse("settings_couriers.html", context)

@router.post("/couriers/{account_id}", name="update_courier_account")
async def update_courier_account_entry(account_id: int, request: Request, db: AsyncSession = Depends(get_db)):
    form_data = await request.form()
    
    # Colectează toate câmpurile de credențiale (care încep cu 'cred_')
    credentials_dict = {key[5:]: value for key, value in form_data.items() if key.startswith('cred_')}
    
    # Convertește 'cred_test_mode' în boolean
    if 'test_mode' in credentials_dict:
        credentials_dict['test_mode'] = True # Checkbox-ul trimite 'on' dacă e bifat
    
    is_active_bool = form_data.get('is_active', 'false').lower() == 'true'

    await crud_couriers.update_courier_account(
        db,
        account_id=account_id,
        name=form_data.get('name'),
        account_key=form_data.get('account_key'),
        courier_type=form_data.get('courier_type'),
        tracking_url=form_data.get('tracking_url'),
        credentials_dict=credentials_dict,
        is_active=is_active_bool
    )
    return RedirectResponse(url="/settings/couriers", status_code=303)

@router.post("/couriers", name="create_courier_account")
async def create_courier_account(request: Request, db: AsyncSession = Depends(get_db)):
    form_data = await request.form()
    credentials_dict = {key[5:]: value for key, value in form_data.items() if key.startswith('cred_')}
    if 'test_mode' in credentials_dict:
        credentials_dict['test_mode'] = True

    await crud_couriers.create_courier_account(
        db,
        name=form_data.get('name'),
        account_key=form_data.get('account_key'),
        courier_type=form_data.get('courier_type'),
        tracking_url=form_data.get('tracking_url'),
        credentials_dict=credentials_dict
    )
    return RedirectResponse(url="/settings/couriers", status_code=303)

@router.post("/mappings", name="create_courier_mapping")
async def create_courier_mapping(db: AsyncSession = Depends(get_db), shopify_name: str = Form(...), account_key: str = Form(...)):
    await crud_couriers.create_courier_mapping(db, shopify_name=shopify_name, account_key=account_key)
    return RedirectResponse(url="/settings/couriers", status_code=303)
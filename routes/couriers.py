# gbeschea/awb-hub/AWB-Hub-2de5efa965cc539c6da369d4ca8f3d17a4613f7f/routes/couriers.py

from fastapi import APIRouter, Depends, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy.ext.asyncio import AsyncSession
import json

from database import get_db
from dependencies import get_templates
from crud import couriers as crud_couriers

router = APIRouter(prefix='/settings/couriers', tags=['Settings - Couriers'])

DEFAULT_TRACKING_URLS = {
    "dpd": "https://tracking.dpd.ro/?shipmentNumber={awb}",
    "sameday": "https://sameday.ro/#awb={awb}",
    "econt": "https://www.econt.com/en/services/track-shipment/{awb}"
}

async def _parse_credentials_from_form(form_data: dict) -> dict:
    credentials = {}
    for key, value in form_data.items():
        if key.startswith('cred_') and value:
            cred_key = key[5:]
            if value == 'on':
                credentials[cred_key] = True
            else:
                credentials[cred_key] = value
    
    if form_data.get('courier_type') == 'econt' and 'test_mode' not in credentials:
        credentials['test_mode'] = False

    return credentials

@router.get('', response_class=HTMLResponse, name="get_couriers_page")
async def get_couriers_page(request: Request, db: AsyncSession = Depends(get_db), templates = Depends(get_templates)):
    accounts = await crud_couriers.get_courier_accounts(db)
    mappings = await crud_couriers.get_courier_mappings(db)
    for acc in accounts:
        acc.credentials_str = json.dumps(acc.credentials, indent=2)
    
    # --- START MODIFICARE ---
    # Am adăugat "default_tracking_urls" în dicționarul de context
    return templates.TemplateResponse("settings_couriers.html", {
        "request": request, 
        "accounts": accounts, 
        "mappings": mappings,
        "default_tracking_urls": DEFAULT_TRACKING_URLS 
    })
    # --- FINAL MODIFICARE ---

@router.post('/accounts', response_class=RedirectResponse, name="create_courier_account")
async def create_account(request: Request, db: AsyncSession = Depends(get_db)):
    form_data = await request.form()
    form_dict = dict(form_data)
    credentials = await _parse_credentials_from_form(form_dict)
    
    courier_type = form_dict.get('courier_type')
    tracking_url = form_dict.get('tracking_url')
    if not tracking_url:
        tracking_url = DEFAULT_TRACKING_URLS.get(courier_type, "")
    
    await crud_couriers.create_courier_account(
        db, name=form_dict.get('name'), account_key=form_dict.get('account_key'),
        courier_type=courier_type, tracking_url=tracking_url,
        credentials=credentials
    )
    return RedirectResponse(url=router.url_path_for("get_couriers_page"), status_code=303)

@router.post('/accounts/{account_id}', response_class=RedirectResponse, name="update_courier_account")
async def update_account(account_id: int, request: Request, db: AsyncSession = Depends(get_db)):
    form_data = await request.form()
    form_dict = dict(form_data)
    
    # --- START MODIFICARE ---
    # Preluăm contul existent pentru a nu pierde datele
    existing_account = await crud_couriers.get_courier_account(db, account_id)
    if not existing_account:
        # Ar trebui să gestionăm eroarea, dar deocamdată ne întoarcem
        return RedirectResponse(url=router.url_path_for("get_couriers_page"), status_code=404)

    # Începem cu credențialele vechi
    updated_credentials = existing_account.credentials.copy()
    
    # Parsăm credențialele noi din formular
    new_credentials = await _parse_credentials_from_form(form_dict)
    
    # Actualizăm credențialele vechi doar cu valorile noi, completate
    updated_credentials.update(new_credentials)
    # --- FINAL MODIFICARE ---

    is_active_bool = form_dict.get('is_active', 'false').lower() == 'true'
    
    await crud_couriers.update_courier_account(
        db, account_id=account_id, name=form_dict.get('name'), account_key=form_dict.get('account_key'),
        courier_type=form_dict.get('courier_type'), tracking_url=form_dict.get('tracking_url'),
        credentials=updated_credentials, # Folosim dicționarul actualizat
        is_active=is_active_bool
    )
    return RedirectResponse(url=router.url_path_for("get_couriers_page"), status_code=303)

@router.post('/mappings', response_class=RedirectResponse, name="create_courier_mapping")
async def create_mapping(db: AsyncSession = Depends(get_db), shopify_name: str = Form(), account_key: str = Form()):
    await crud_couriers.create_courier_mapping(db, shopify_name, account_key)
    return RedirectResponse(url=router.url_path_for("get_couriers_page"), status_code=303)
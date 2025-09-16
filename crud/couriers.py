import json
import logging
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.orm import selectinload
import models

async def get_courier_accounts(db: AsyncSession):
    """Preia toate conturile de curieri din baza de date."""
    result = await db.execute(select(models.CourierAccount).options(selectinload(models.CourierAccount.mappings)))
    return result.scalars().all()

async def get_courier_mappings(db: AsyncSession):
    """Preia toate mapările de curieri din baza de date."""
    result = await db.execute(select(models.CourierMapping))
    return result.scalars().all()

async def create_courier_account(db: AsyncSession, name: str, account_key: str, courier_type: str, tracking_url: str, credentials_dict: dict):
    """Creează un nou cont de curier."""
    new_account = models.CourierAccount(
        name=name,
        account_key=account_key,
        courier_type=courier_type,
        tracking_url=tracking_url,
        credentials=credentials_dict,
        is_active=True
    )
    db.add(new_account)
    await db.commit()

async def update_courier_account(
    db: AsyncSession, account_id: int, name: str, account_key: str,
    courier_type: str, tracking_url: str, credentials_dict: dict, is_active: bool
):
    """Actualizează un cont de curier existent."""
    result = await db.execute(select(models.CourierAccount).where(models.CourierAccount.id == account_id))
    account = result.scalar_one_or_none()
    
    if account:
        account.name = name
        account.account_key = account_key
        account.courier_type = courier_type
        account.tracking_url = tracking_url
        account.is_active = is_active
        
        # Logica de actualizare a credențialelor
        existing_creds = account.credentials or {}
        
        # Tratăm parola separat: dacă e goală, o păstrăm pe cea veche
        new_password = credentials_dict.get('password')
        if not new_password: # Dacă parola e goală sau None
            if 'password' in existing_creds:
                credentials_dict['password'] = existing_creds['password']
        
        # Actualizăm dicționarul de credențiale
        existing_creds.update(credentials_dict)
        account.credentials = existing_creds
        
        await db.commit()
        await db.refresh(account)

async def create_courier_mapping(db: AsyncSession, shopify_name: str, account_key: str):
    """Creează o nouă mapare de curier."""
    new_mapping = models.CourierMapping(shopify_name=shopify_name, account_key=account_key)
    db.add(new_mapping)
    await db.commit()

async def get_courier_categories(db: AsyncSession):
    """Preia toate categoriile de curieri din baza de date."""
    result = await db.execute(select(models.CourierCategory))
    return result.scalars().all()
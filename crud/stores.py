from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
# MODIFICARE: Importăm `selectinload`
from sqlalchemy.orm import selectinload
import models
from typing import List

async def get_stores(db: AsyncSession):
    result = await db.execute(select(models.Store).options(selectinload(models.Store.categories)).order_by(models.Store.name))
    return result.scalars().all()

async def get_store_by_id(db: AsyncSession, store_id: int):
    # --- MODIFICARE AICI ---
    # Adăugăm .options(selectinload(models.Store.categories)) pentru a încărca
    # relația în mod "eager" și a evita eroarea MissingGreenlet.
    query = (
        select(models.Store)
        .where(models.Store.id == store_id)
        .options(selectinload(models.Store.categories))
    )
    # --- FINAL MODIFICARE ---
    result = await db.execute(query)
    return result.scalar_one_or_none()

async def get_stores_by_ids(db: AsyncSession, store_ids: List[int]) -> List[models.Store]:
    """
    Preia magazine pe baza unei liste de ID-uri (asincron).
    """
    if not store_ids:
        return []
    result = await db.execute(select(models.Store).filter(models.Store.id.in_(store_ids)))
    return result.scalars().all()



async def get_store_by_domain(db: AsyncSession, domain: str) -> models.Store | None:
    """
    Preia un magazin pe baza domeniului său (asincron).
    """
    result = await db.execute(select(models.Store).filter(models.Store.domain == domain))
    return result.scalar_one_or_none()


async def update_store(
    db: AsyncSession,
    store_id: int,
    name: str,
    domain: str,
    shared_secret: str,
    access_token: str,
    is_active: bool,
    category_ids: list[int],
    paper_size: str,
    dpd_client_id: str,
    pii_source: str
):
    store = await get_store_by_id(db, store_id)
    if not store:
        return None

    # Actualizează atributele simple
    store.name = name
    store.domain = domain
    store.is_active = is_active
    store.paper_size = paper_size
    store.dpd_client_id = dpd_client_id
    store.pii_source = pii_source

    # --- MODIFICARE AICI ---
    # Actualizează câmpurile secrete DOAR dacă a fost introdusă o valoare nouă.
    if shared_secret:
        store.shared_secret = shared_secret
    if access_token:
        store.access_token = access_token
    # --- FINAL MODIFICARE ---

    # Actualizează relația many-to-many cu categoriile
    if category_ids is not None:
        query = select(models.StoreCategory).where(models.StoreCategory.id.in_(category_ids))
        result = await db.execute(query)
        store.categories = result.scalars().all()

    await db.commit()
    await db.refresh(store)
    return store

# Funcție pentru a prelua toate categoriile, necesară în pagina de setări
async def get_all_store_categories(db: AsyncSession):
    result = await db.execute(select(models.StoreCategory).order_by(models.StoreCategory.name))
    return result.scalars().all()
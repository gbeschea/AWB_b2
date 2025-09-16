# check_db.py

import asyncio
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, selectinload
from sqlalchemy import select, func
import os
from dotenv import load_dotenv

# Importă modelele tale
import models

# Încarcă variabilele de mediu din fișierul .env
load_dotenv()

async def check_database():
    """Se conectează la baza de date și verifică tabelul line_items."""
    
    DATABASE_URL = os.getenv("DATABASE_URL")
    if not DATABASE_URL:
        print("Eroare: Nu am găsit DATABASE_URL în fișierul .env")
        return

    print(f"Ne conectăm la baza de date...")
    engine = create_async_engine(DATABASE_URL)
    AsyncSessionLocal = sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )

    async with AsyncSessionLocal() as session:
        print("Conexiune reușită.")
        
        # 1. Numărăm câte înregistrări există în total în tabelul line_items
        total_items_query = select(func.count(models.LineItem.id))
        total_items_result = await session.execute(total_items_query)
        total_items_count = total_items_result.scalar_one_or_none()
        
        print("-" * 30)
        print(f"REZULTAT: Am găsit în total {total_items_count or 0} produse în tabelul 'line_items'.")
        print("-" * 30)

        if not total_items_count:
            print("\nConcluzie: Tabelul de produse este gol. Problema este în `sync_service.py`.")
            print("Produsele nu sunt salvate în baza de date în timpul sincronizării.")
            return

        # 2. Dacă există produse, verificăm dacă sunt asociate corect cu comenzile
        print("\nVerificăm asocierea produselor cu 5 comenzi...")
        
        orders_with_items_query = (
            select(models.Order)
            .options(selectinload(models.Order.line_items))
            .where(models.Order.line_items.any()) # Selectăm doar comenzi care au produse
            .limit(5)
        )
        
        result = await session.execute(orders_with_items_query)
        orders = result.scalars().all()

        if not orders:
            print("Am găsit produse, dar nu par a fi asociate cu nicio comandă. Asta e ciudat.")
        else:
            for order in orders:
                print(f"\nComanda: {order.name} (ID: {order.id})")
                if order.line_items:
                    for item in order.line_items:
                        print(f"  -> {item.quantity} x {item.title} (SKU: {item.sku})")
                else:
                    # Acest mesaj nu ar trebui să apară datorită clauzei .where()
                    print("  -> Eroare: Comanda a fost găsită, dar produsele nu s-au încărcat.")
            
            print("\n\nConcluzie: Produsele EXISTĂ în baza de date și sunt asociate corect.")
            print("Dacă vezi acest mesaj, problema este una foarte subtilă în `filter_service.py`.")


if __name__ == "__main__":
    # Asigură-te că ai instalat python-dotenv: pip install python-dotenv
    asyncio.run(check_database())
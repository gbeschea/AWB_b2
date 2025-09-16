# /scripts/reset_validation_status.py

import asyncio
import sys
from pathlib import Path

# Adaugă directorul rădăcină în path pentru a putea importa modulele
sys.path.append(str(Path(__file__).resolve().parent.parent))

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import select, update, func # Am adăugat func

from settings import settings
import models

async def main():
    """
    Script pentru a reseta statusul de validare al comenzilor care nu sunt 'valid'.
    """
    print("Se conectează la baza de date...")
    engine = create_async_engine(settings.DATABASE_URL)
    AsyncSessionLocal = sessionmaker(bind=engine, class_=AsyncSession)

    async with AsyncSessionLocal() as session:
        async with session.begin():
            # Condiția pentru a găsi toate comenzile care NU sunt valide
            condition = models.Order.address_status != 'valid'

            # Contorizăm câte comenzi vor fi afectate
            count_stmt = select(func.count(models.Order.id)).where(condition)
            result = await session.execute(count_stmt)
            count = result.scalar_one()

            if count == 0:
                print("Nu s-a găsit nicio comandă cu status invalid de resetat.")
                return

            print(f"S-au găsit {count} comenzi care vor fi resetate la statusul 'nevalidat'.")
            
            # Construim și executăm comanda de actualizare
            update_stmt = (
                update(models.Order)
                .where(condition)
                .values(
                    address_status='nevalidat', # Status inițial
                    address_score=None,
                    address_validation_errors=None
                )
            )
            
            await session.execute(update_stmt)
            
            print("Statusurile au fost resetate cu succes!")

    print("Operațiune finalizată.")

if __name__ == "__main__":
    asyncio.run(main())
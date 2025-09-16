# /scripts/clear_addresses_table.py

import asyncio
import sys
from pathlib import Path

# Adaugă directorul rădăcină în path pentru a putea importa modulele
sys.path.append(str(Path(__file__).resolve().parent.parent))

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

from settings import settings
import models

async def main():
    """
    Script pentru a goli complet tabela `romania_addresses`.
    """
    print("Se conectează la baza de date...")
    engine = create_async_engine(settings.DATABASE_URL)
    AsyncSessionLocal = sessionmaker(bind=engine, class_=AsyncSession)

    async with AsyncSessionLocal() as session:
        async with session.begin():
            print("Se șterg TOATE datele din tabela 'romania_addresses'...")
            await session.execute(models.RomaniaAddress.__table__.delete())
            print("Tabela a fost golită cu succes!")

    print("Operațiune finalizată.")

if __name__ == "__main__":
    print("ATENȚIE: Acest script va șterge ireversibil toate adresele din baza de date.")
    confirm = input("Ești sigur că vrei să continui? (da/nu): ")
    if confirm.lower() == 'da':
        asyncio.run(main())
    else:
        print("Operațiune anulată.")
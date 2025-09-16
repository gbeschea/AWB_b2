# /scripts/import_addresses.py

import asyncio
import csv
import sys
from pathlib import Path

# Adaugă directorul rădăcină în path pentru a putea importa modulele
sys.path.append(str(Path(__file__).resolve().parent.parent))

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import insert  # Am importat funcția corectă 'insert'

# Importăm variabilele de configurare și modelele corect
from settings import settings
import models

async def main():
    """
    Script pentru a șterge adresele vechi și a importa o listă nouă
    dintr-un fișier CSV.
    """
    # Asigură-te că fișierul 'addresses.csv' se află în directorul 'scripts'
    csv_path = Path(__file__).parent / "addresses.csv"
    if not csv_path.exists():
        print(f"EROARE: Fișierul {csv_path} nu a fost găsit.")
        return

    print("Se conectează la baza de date...")
    # Folosim DATABASE_URL din settings, la fel ca restul aplicației
    engine = create_async_engine(settings.DATABASE_URL)
    AsyncSessionLocal = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

    print(f"Se citesc datele din {csv_path}...")
    try:
        with open(csv_path, mode='r', encoding='utf-8') as infile:
            reader = csv.DictReader(infile)
            addresses_to_insert = [
                {
                    "judet": row.get("judet"),
                    "localitate": row.get("localitate"),
                    "tip_artera": row.get("tip artera") or None,
                    "nume_strada": row.get("denumire artera") or None,
                    "cod_postal": row.get("codpostal"),
                    "sector": row.get("sector") or None,
                }
                for row in reader
            ]
    except Exception as e:
        print(f"EROARE la citirea fișierului CSV: {e}")
        return

    if not addresses_to_insert:
        print("Nu s-au găsit adrese de importat.")
        return

    print(f"S-au găsit {len(addresses_to_insert)} adrese. Se începe inserarea...")
    
    async with AsyncSessionLocal() as session:
        async with session.begin():
            print("Se șterg datele vechi din tabela 'romania_addresses'...")
            await session.execute(models.RomaniaAddress.__table__.delete())

            batch_size = 5000
            for i in range(0, len(addresses_to_insert), batch_size):
                batch = addresses_to_insert[i:i + batch_size]
                
                # === AICI ESTE CORECȚIA ===
                # Folosim funcția 'insert' pe care am importat-o corect.
                stmt = insert(models.RomaniaAddress).values(batch)
                await session.execute(stmt)
                
                print(f"S-au inserat {i + len(batch)} / {len(addresses_to_insert)} adrese...")
        
        # `session.begin()` face commit automat la ieșirea din bloc

    print("Importul a fost finalizat cu succes!")


if __name__ == "__main__":
    asyncio.run(main())
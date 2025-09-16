# worker.py

from arq.connections import RedisSettings
from database import AsyncSessionLocal
from services import sync_service

# =================================================================
# TASK-UL ASINCRON
# =================================================================
async def sync_orders_task(ctx, store_id: int):
    """Task-ul ARQ care rulează sincronizarea comenzilor (asincron)."""
    print(f"Începe sincronizarea pentru magazinul cu ID: {store_id}")
    
    # Creăm o sesiune de bază de date asincronă nouă pentru fiecare task
    db_session = AsyncSessionLocal()
    
    try:
        # =================================================================
        # AICI ESTE CORECȚIA PRINCIPALĂ: Apelăm funcția corectă
        # =================================================================
        # Presupunem că sincronizarea standard este pe ultimele 30 de zile
        result = await sync_service.run_full_sync(db=db_session, days=30)
        # =================================================================
        
        print(f"Sincronizare finalizată pentru magazinul {store_id}. Rezultat: {result}")
        return result
    except Exception as e:
        print(f"EROARE în timpul sincronizării pentru magazinul {store_id}: {e}")
    finally:
        await db_session.close()

# =================================================================
# CONFIGURAREA WORKER-ULUI
# =================================================================
async def startup(ctx):
    """Funcție de pornire (nu mai este necesară gestionarea sesiunii aici)."""
    pass

async def shutdown(ctx):
    """Funcție de oprire."""
    pass

class WorkerSettings:
    """Configurarea worker-ului ARQ."""
    functions = [sync_orders_task] # Lista de task-uri
    on_startup = startup
    on_shutdown = shutdown
    # Asigură-te că serverul Redis rulează pe această adresă
    redis_settings = RedisSettings(host='localhost', port=6379)
# main.py

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from routes import (
    store_categories, printing, logs, orders, sync, labels, 
    settings, validation, webhooks, couriers, background
)

app = FastAPI(
    title="AWB Hub",
    description="Aplicator pentru managementul comenzilor și generarea de AWB-uri.",
    version="1.0.0"
)

# Montarea fișierelor statice (CSS, JS)
app.mount("/static", StaticFiles(directory="static"), name="static")

# Includerea tuturor router-elor din aplicație
app.include_router(orders.router, tags=["Orders"])
app.include_router(sync.router, tags=["Sync"])
app.include_router(labels.router, tags=["Labels"])
app.include_router(settings.router, tags=["Settings"])
app.include_router(validation.router, tags=["Validation"])
app.include_router(webhooks.router, tags=["Webhooks"])
app.include_router(couriers.router, tags=["Couriers"])
app.include_router(printing.router, tags=["Printing"])
app.include_router(logs.router, tags=["Logs"])
app.include_router(store_categories.router, tags=["Store Categories"])
app.include_router(background.router, tags=["Background Tasks"])


@app.get("/", tags=["Root"])
def read_root():
    """Endpoint de bază pentru a verifica dacă aplicația rulează."""
    return {"message": "AWB Hub is running."}
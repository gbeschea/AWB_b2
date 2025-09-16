# routes/orders.py

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from sqlalchemy.ext.asyncio import AsyncSession

import crud.stores as store_crud
from database import get_db
from services import filter_service
from templating import templates # Importăm instanța unică de template-uri
from settings import settings # Importăm setările pentru a accesa harta de tracking
from datetime import datetime, timezone


router = APIRouter()

def enhance_orders_data(orders):
    """Adaugă câmpuri dinamice la obiectele Order pentru afișare."""
    for order in orders:
        latest_shipment = None
        if order.shipments:
            # Sortăm livrările după data creării, cele mai noi la sfârșit.
            # Folosim o valoare minimă pentru livrările care nu au încă o dată setată.
            sorted_shipments = sorted(
                order.shipments,
                key=lambda s: s.fulfillment_created_at or datetime.min.replace(tzinfo=timezone.utc)
            )
            latest_shipment = sorted_shipments[-1]

        setattr(order, 'latest_shipment', latest_shipment)
        setattr(order, 'line_items_str', ", ".join([f"{item.quantity}x {item.title}" for item in order.line_items]))
        setattr(order, 'mapped_courier_status', latest_shipment.last_status if latest_shipment and latest_shipment.last_status else "N/A")
    return orders

@router.get("/view", response_class=HTMLResponse, name="view_orders")
async def view(request: Request, db: AsyncSession = Depends(get_db)):
    """Afișează pagina principală cu comenzile filtrate și paginate (asincron)."""
    
    query_params = request.query_params
    
    # Apelăm funcțiile asincrone cu 'await'
    orders, total_orders, filter_counts = await filter_service.get_filtered_orders(db, query_params)
    all_stores = await store_crud.get_stores(db)
    
    # Iterăm prin comenzi pentru a adăuga atributele dinamice necesare template-ului.
    for order in orders:
        latest_shipment = None
        if order.shipments:
            # Sortăm livrările pentru a o găsi pe cea mai recentă
            sorted_shipments = sorted(
                order.shipments,
                key=lambda s: s.fulfillment_created_at or datetime.min.replace(tzinfo=timezone.utc)
            )
            latest_shipment = sorted_shipments[-1]
        
        # Adăugăm atributele de care template-ul are nevoie
        setattr(order, 'latest_shipment', latest_shipment)
        setattr(order, 'line_items_str', ", ".join([f"{item.quantity}x {item.title}" for item in order.line_items]))
        
        
        # AICI ESTE LINIA CHEIE PENTRU A REZOLVA PROBLEMA:
        # Citim valoarea 'mapped_courier_status' care vine din VIEW-ul bazei de date
        # și ne asigurăm că este setată pe obiectul pe care îl trimitem la template.
        # Dacă din orice motiv valoarea nu există (ex: comandă fără AWB), punem 'N/A'.
        setattr(order, 'mapped_courier_status', getattr(order, 'mapped_courier_status', 'N/A'))

    
    page = int(query_params.get("page", 1))
    page_size = 50
    total_pages = (total_orders + page_size - 1) // page_size
    
    # Logică îmbunătățită pentru afișarea numerelor de pagină
    page_numbers = []
    if total_pages > 1:
        if total_pages <= 7:
            page_numbers = list(range(1, total_pages + 1))
        else:
            visible_pages = {1, 2, total_pages - 1, total_pages, page, page - 1, page + 1}
            last_page = 0
            for p in sorted(list(visible_pages)):
                if 1 <= p <= total_pages:
                    if p - last_page > 1:
                        page_numbers.append('...')
                    page_numbers.append(p)
                    last_page = p

    # Construirea contextului pentru template
    context = {
        "request": request,
        "orders": orders,
        "total_orders": total_orders,
        "page": page,
        "total_pages": total_pages,
        "page_numbers": page_numbers,
        "query_params": query_params,
        "sort_by": query_params.get('sort_by', 'created_at_desc'),

        "filter_counts": filter_counts,
        "stores": all_stores,
        "all_stores": all_stores,
        "selected_stores": request.query_params.getlist("stores"),
        
        # Variabila care a cauzat ultima eroare, acum este trimisă corect
        "courier_tracking_map": settings.COURIER_TRACKING_MAP,
        
        # Opțiuni pentru dropdown-urile de filtre
        "categories": [], # Acestea pot fi populate dinamic dintr-o altă funcție crud
        "couriers": ["DPD", "Sameday", "Econt"],
        "derived_status_options": ["Nou", "În procesare", "Finalizat", "Problemă"],
        "courier_status_group_options": [("in_transit", "În Tranzit"), ("delivered", "Livrat"), ("refused", "Refuzat")],
        "address_status_options": ["Valid", "Invalid", "Neverificat"],
        "financial_status_options": ["paid", "pending", "refunded"],
        "fulfillment_status_options": ["fulfilled", "unfulfilled", "partial"]
    }

    return templates.TemplateResponse("index.html", context)
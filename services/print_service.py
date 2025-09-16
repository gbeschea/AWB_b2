# services/print_service.py (sau un alt fișier de servicii relevant)

from collections import defaultdict
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy import select
from typing import List, Dict


import models

async def get_aggregated_line_items_for_printing(db: AsyncSession, order_ids: List[int]) -> Dict:
    """
    Preia produsele din comenzile specificate, le grupează și le sortează
    conform logicii de business pentru picking.
    """
    
    # 1. Preluăm comenzile și produsele asociate
    stmt = (
        select(models.Order)
        .options(
            selectinload(models.Order.store),
            selectinload(models.Order.line_items)
        )
        .where(models.Order.id.in_(order_ids))
    )
    result = await db.execute(stmt)
    orders = result.scalars().unique().all()

    # 2. Agregăm datele
    # Structura: { 'Nume Magazin': { 'SKU Produs': { 'title': 'Titlu Produs', 'quantity': 5, 'courier': 'DPD' } } }
    aggregated_data = defaultdict(lambda: defaultdict(lambda: {'quantity': 0}))

    for order in orders:
        store_name = order.store.name
        courier = order.assigned_courier or 'N/A'
        
        for item in order.line_items:
            sku = item.sku or 'SKU_Necunoscut'
            aggregated_data[store_name][sku]['quantity'] += item.quantity
            aggregated_data[store_name][sku]['title'] = item.title
            # Presupunem că toate produsele dintr-un magazin merg cu același curier
            # O logică mai complexă ar putea fi necesară dacă un magazin are mai mulți curieri
            aggregated_data[store_name][sku]['courier'] = courier


    # 3. Sortăm rezultatele conform cerințelor
    # Structura finală: [ { 'store': 'Nume Magazin', 'courier': 'DPD', 'products': [ ... ] }, ... ]
    
    sorted_stores = []
    # Sortăm magazinele alfabetic
    for store_name in sorted(aggregated_data.keys()):
        store_data = aggregated_data[store_name]
        
        # Sortăm produsele din magazin după cantitatea totală, descrescător
        sorted_products = sorted(
            store_data.items(),
            key=lambda item: item[1]['quantity'],
            reverse=True
        )
        
        # Extragem curierul (va fi același pentru toate produsele din acest magazin)
        # și formatăm lista de produse
        courier = 'N/A'
        product_list = []
        if sorted_products:
            courier = sorted_products[0][1].get('courier', 'N/A')
            product_list = [
                {'sku': sku, 'title': details['title'], 'quantity': details['quantity']}
                for sku, details in sorted_products
            ]

        sorted_stores.append({
            'store': store_name,
            'courier': courier,
            'products': product_list
        })
        
    # La final, sortăm întreaga listă de magazine după curier
    final_sorted_list = sorted(sorted_stores, key=lambda s: s['courier'])
    
    return final_sorted_list
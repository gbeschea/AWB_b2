# services/filter_service.py

import logging
from typing import Dict, Any, Tuple, List
from sqlalchemy import select, func, or_, and_, text, Table, Column, Integer, String
from sqlalchemy.orm import joinedload, selectinload
from sqlalchemy.ext.asyncio import AsyncSession

# Importăm modelele și motorul bazei de date
import models
from database import engine

orders_view = Table(
    'orders_view',
    models.Base.metadata,
    Column('id', Integer, primary_key=True),
    Column('mapped_courier_status', String),
    extend_existing=True  # Previne erorile la reîncărcarea serverului (hot-reload)
)


async def get_filtered_orders(db: AsyncSession, query_params: Any) -> Tuple[List[models.Order], int, Dict[str, Any]]:
    """
    Preia comenzile filtrate, implementând toate filtrele din UI și îmbogățind
    rezultatele cu statusul mapat din orders_view.
    """


    query = select(
        models.Order,
        orders_view.c.mapped_courier_status
    ).options(
        joinedload(models.Order.store),
        selectinload(models.Order.line_items),
        joinedload(models.Order.shipments)
    ).outerjoin(orders_view, models.Order.id == orders_view.c.id)

    filters = []

    if query_search := query_params.get('order_q', '').strip():
        search_term = f"%{query_search.lower()}%"
        filters.append(or_(
            func.lower(models.Order.name).like(search_term),
            func.lower(models.Order.customer).like(search_term),
            func.lower(models.Order.shipping_phone).like(search_term),
            models.Order.shipments.any(func.lower(models.Shipment.awb).like(search_term))
        ))
        
    if (selected_stores := query_params.getlist("stores")) and "all" not in selected_stores:
        filters.append(models.Order.store_id.in_([int(s) for s in selected_stores]))

    if (category_id := query_params.get('category')) and category_id != 'all':
        query = query.join(models.Order.store).join(models.Store.categories)
        filters.append(models.StoreCategory.id == int(category_id))

    if (courier := query_params.get('courier')) and courier != 'all':
        filters.append(models.Order.assigned_courier == courier)

    if (derived_status := query_params.get('derived_status')) and derived_status != 'all':
        filters.append(models.Order.derived_status == derived_status)

    if (courier_status_group := query_params.get('courier_status_group')) and courier_status_group != 'all':
        filters.append(orders_view.c.mapped_courier_status == courier_status_group)

    if (address_status := query_params.get('address_status')) and address_status != 'all':
        filters.append(models.Order.address_status == address_status)
        
    if (financial_status := query_params.get('financial_status')) and financial_status != 'all':
        filters.append(models.Order.financial_status == financial_status)

    if (fulfillment_status := query_params.get('fulfillment_status')) and fulfillment_status != 'all':
        filters.append(models.Order.shopify_status == fulfillment_status)

    if (printed_status := query_params.get('printed_status')) and printed_status != 'all':
        if printed_status == 'printed':
            filters.append(models.Order.shipments.any(models.Shipment.printed_at.isnot(None)))
        elif printed_status == 'not_printed':
            filters.append(or_(
                ~models.Order.shipments.any(),
                models.Order.shipments.any(models.Shipment.printed_at.is_(None))
            ))

    if filters:
        query = query.where(and_(*filters))

    count_query = select(func.count()).select_from(query.order_by(None).subquery())
    total_orders = await db.scalar(count_query)

    sort_by = query_params.get('sort_by', 'created_at_desc')
    sort_map = {
        'created_at_desc': models.Order.created_at.desc(),
        'created_at_asc': models.Order.created_at.asc(),
        'order_name_desc': models.Order.name.desc(),
        'order_name_asc': models.Order.name.asc(),
    }
    if sort_by in sort_map:
        query = query.order_by(sort_map[sort_by])

    page = int(query_params.get("page", 1))
    page_size = 50
    query = query.limit(page_size).offset((page - 1) * page_size)

    result = await db.execute(query)
    unique_results = result.unique().mappings().all()


    
    orders = []
    for row in unique_results:
        # Extragem obiectul Order din fiecare rând/dicționar
        order = row['Order']
        # Setăm atributul custom pentru statusul curierului
        order.mapped_courier_status = row['mapped_courier_status']
        orders.append(order)
        # Acum, `order.line_items` va fi garantat populat și va ajunge la template
        
    filter_counts = {}

    return orders, total_orders, filter_counts
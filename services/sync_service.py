# /services/sync_service.py

import asyncio
import logging
import json
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, Tuple

from sqlalchemy.orm import joinedload
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert as pg_insert
from dateutil.parser import parse as parse_datetime

import models
from settings import settings
from services import shopify_service, address_service, courier_service
from services.utils import calculate_and_set_derived_status
from websocket_manager import manager
from database import AsyncSessionLocal


def _dt(v: Optional[str]) -> Optional[datetime]:
    if not v: return None
    try:
        return datetime.fromisoformat(v.replace('Z', '+00:00'))
    except (ValueError, TypeError):
        return None

def map_payment_method(gateways: List[str], financial_status: str) -> str:
    raw_gateways = gateways or []
    lower_gateways_set = {g.lower().strip() for g in raw_gateways}
    gateway_str_joined = ", ".join(raw_gateways).lower()

    if settings.PAYMENT_MAP:
        for standard_name, keywords in settings.PAYMENT_MAP.items():
            if not lower_gateways_set.isdisjoint(keywords):
                return standard_name
            if any(keyword in gateway_str_joined for keyword in keywords):
                return standard_name

    if not gateway_str_joined.strip():
        if financial_status == 'paid': return "Fara plata"
        if financial_status == 'pending': return "Ramburs"

    return ", ".join(raw_gateways)

async def courier_from_shopify(db: AsyncSession, tracking_company: str) -> Tuple[Optional[str], Optional[str]]:
    search_text = (tracking_company or '').strip()
    if not search_text:
        return None, None

    stmt = (
        select(models.CourierMapping)
        .options(joinedload(models.CourierMapping.account))
        .where(func.lower(models.CourierMapping.shopify_name) == search_text.lower())
    )
    result = await db.execute(stmt)
    mapping = result.scalars().first()

    if mapping and mapping.account and mapping.account.is_active:
        logging.info(f"Mapare găsită pentru '{search_text}'. Folosim contul: {mapping.account.account_key} ({mapping.account.courier_type})")
        return mapping.account.courier_type, mapping.account.account_key

    logging.warning(f"Nu s-a găsit nicio mapare exactă și activă pentru curierul: '{search_text}'")
    return None, None

def _get_mapped_address(order_data: Dict[str, Any], pii_source: str) -> Dict[str, Any]:
    address = {}
    if pii_source == 'shopify':
        source = order_data.get('shippingAddress')
        if not source: return address
        first_name, last_name = source.get('firstName', ''), source.get('lastName', '')
        address = {
            'name': f"{first_name} {last_name}".strip(), 'address1': source.get('address1'),
            'address2': source.get('address2'), 'phone': source.get('phone'),
            'city': source.get('city'), 'zip': source.get('zip'), 'province': source.get('province'),
            'country': source.get('country'), 'email': order_data.get('email')
        }
    elif pii_source == 'metafield':
        source_node = order_data.get('metafield')
        if not source_node or not source_node.get('value'): return address
        try:
            metafield_data = json.loads(source_node['value'])
            address = {
                'name': f"{metafield_data.get('first_name', '')} {metafield_data.get('last_name', '')}".strip(),
                'address1': metafield_data.get('address1'), 'address2': metafield_data.get('address2'),
                'phone': metafield_data.get('phone_number'), 'city': metafield_data.get('city'),
                'zip': metafield_data.get('postal_code'), 'province': metafield_data.get('county'),
                'country': metafield_data.get('country'), 'email': metafield_data.get('email')
            }
        except json.JSONDecodeError:
            logging.warning(f"Nu s-a putut decoda metafield-ul PII pentru comanda {order_data.get('name')}")
    return address


async def run_orders_sync(db: AsyncSession, days: int, full_sync: bool = False):
    start_ts = datetime.now(timezone.utc)
    sync_type = "TOTALĂ" if full_sync else "STANDARD"
    logging.warning(f"ORDER SYNC ({sync_type}) a pornit pentru ultimele {days} zile.")
    await manager.broadcast({"type": "sync_start", "message": f"Sincronizare comenzi ({sync_type})...", "sync_type": "orders"})

    stores_res = await db.execute(select(models.Store).where(models.Store.is_active == True))
    stores_to_sync = stores_res.scalars().all()

    if not stores_to_sync:
        logging.warning("ORDER SYNC: Nu există magazine active. Sincronizarea a fost oprită.")
        await manager.broadcast({"type": "sync_end", "message": "Nu sunt magazine active."})
        return
    
    fetch_tasks = [shopify_service.fetch_orders(s, since_days=days) for s in stores_to_sync]
    all_results = await asyncio.gather(*fetch_tasks, return_exceptions=True)

    total_orders_to_process = sum(len(res) for res in all_results if isinstance(res, list))
    await manager.broadcast({"type": "progress_update", "current": 0, "total": total_orders_to_process, "message": f"S-au găsit {total_orders_to_process} comenzi. Se procesează..."})

    processed_count = 0
    all_processed_order_ids = set()

    for store_rec, orders_or_exception in zip(stores_to_sync, all_results):
        if isinstance(orders_or_exception, Exception):
            logging.warning(f"Eroare la preluarea comenzilor pentru {store_rec.domain}: {orders_or_exception}")
            continue

        for o in orders_or_exception:
            sid = o['id']

            order_res = await db.execute(
                select(models.Order)
                .options(joinedload(models.Order.line_items), joinedload(models.Order.shipments), joinedload(models.Order.fulfillment_orders))
                .where(models.Order.shopify_order_id == sid)
            )
            order = order_res.unique().scalar_one_or_none()

            shipping_address = _get_mapped_address(o, store_rec.pii_source)
            if not shipping_address:
                 logging.warning(f"Nu s-au găsit date PII pentru comanda {o.get('name')} din sursa '{store_rec.pii_source}'")

            gateways, financial_status = o.get('paymentGatewayNames', []), o.get('displayFinancialStatus', 'unknown')
            mapped_payment = map_payment_method(gateways, financial_status)
            total_price_str = o.get('totalPriceSet', {}).get('shopMoney', {}).get('amount', '0.0')
            status_from_shopify = (o.get('displayFulfillmentStatus') or 'unfulfilled').strip().lower()
            if status_from_shopify == 'success': status_from_shopify = 'fulfilled'
            fulfillment_orders = o.get('fulfillmentOrders', {}).get('edges', [])
            has_active_hold = any(ff_edge.get('node', {}).get('fulfillmentHolds') for ff_edge in fulfillment_orders)

            order_data = {
                'name': o.get('name'), 'customer': shipping_address.get('name') or 'N/A',
                'created_at': _dt(o.get('createdAt')), 'cancelled_at': _dt(o.get('cancelledAt')),
                'is_on_hold_shopify': has_active_hold, 'financial_status': financial_status,
                'total_price': float(total_price_str) if total_price_str else 0.0,
                'payment_gateway_names': ", ".join(gateways), 'mapped_payment': mapped_payment,
                'tags': ",".join(o.get('tags', [])), 'note': o.get('note', ''),
                'shopify_status': status_from_shopify,
                'shipping_name': shipping_address.get('name'), 'shipping_address1': shipping_address.get('address1'),
                'shipping_address2': shipping_address.get('address2'), 'shipping_phone': shipping_address.get('phone'),
                'shipping_city': shipping_address.get('city'), 'shipping_zip': shipping_address.get('zip'),
                'shipping_province': shipping_address.get('province'), 'shipping_country': shipping_address.get('country'),
            }

            if not order:
                order = models.Order(store_id=store_rec.id, shopify_order_id=sid, **order_data)
                db.add(order)
            else:
                for key, value in order_data.items():
                    setattr(order, key, value)
            
            if order.line_items:
                for item in order.line_items:
                    await db.delete(item)
                await db.flush()
            
            line_items_data = o.get('lineItems', {}).get('edges', [])
            for item_edge in line_items_data:
                item_node = item_edge.get('node', {})
                new_line_item = models.LineItem(
                    sku=item_node.get('sku'),
                    title=item_node.get('title'),
                    quantity=item_node.get('quantity')
                )
                order.line_items.append(new_line_item)

            fulfillments_data = o.get('fulfillments', [])
            if fulfillments_data:
                existing_fulfillment_ids = {str(s.shopify_fulfillment_id) for s in order.shipments if s.shopify_fulfillment_id}
                for fulfillment in fulfillments_data:
                    fulfillment_id = str(fulfillment.get('id'))
                    if fulfillment_id and fulfillment_id not in existing_fulfillment_ids:
                        tracking_info = fulfillment.get('trackingInfo', [{}])[0]
                        courier_type, account_key = await courier_from_shopify(db, tracking_info.get('company'))
                        logging.info(f"Livrare nouă găsită pentru comanda {order.name}: AWB {tracking_info.get('number')}")
                        new_shipment = models.Shipment(
                            order_id=order.id,
                            shopify_fulfillment_id=fulfillment_id,
                            awb=tracking_info.get('number'),
                            courier=courier_type,
                            account_key=account_key,
                            fulfillment_created_at=_dt(fulfillment.get('createdAt'))
                        )
                        db.add(new_shipment)

            all_processed_order_ids.add(order.id)
            processed_count += 1
            if processed_count % 50 == 0:
                await manager.broadcast({"type": "progress_update", "current": processed_count, "total": total_orders_to_process, "message": f"Se procesează... ({processed_count}/{total_orders_to_process})"})

    await db.commit()

    if all_processed_order_ids:
        logging.warning(f"Validare adrese și recalculare statusuri pentru {len(all_processed_order_ids)} comenzi...")
        
        async with AsyncSessionLocal() as session:
            orders_to_recalc_res = await session.execute(
                select(models.Order).options(joinedload(models.Order.shipments)).where(models.Order.id.in_(all_processed_order_ids))
            )
            orders_to_process = orders_to_recalc_res.unique().scalars().all()
            
            total_to_validate = len(orders_to_process)
            validated_count = 0

            for order in orders_to_process:
                validated_count += 1
                if validated_count % 20 == 0:
                    logging.warning(f"   -> Validat {validated_count}/{total_to_validate}...")

                # === AICI ESTE OPTIMIZAREA ===
                # Rulăm validarea doar dacă statusul este cel implicit, "nevalidat",
                # pentru a nu re-valida la infinit.
                if order.address_status == 'nevalidat':
                    await address_service.validate_address_for_order(session, order)
                
                calculate_and_set_derived_status(order)
            
            logging.warning(f"   -> Validare finalizată pentru toate cele {total_to_validate} comenzi. Se salvează...")
            await session.commit()
    
    await manager.broadcast({"type": "sync_end", "message": f"Sincronizare finalizată! {processed_count} comenzi actualizate."})
    logging.warning(f"ORDER SYNC finalizat în {(datetime.now(timezone.utc) - start_ts).total_seconds():.1f}s.")


async def run_couriers_sync(db: AsyncSession, full_sync: bool = False):
    await courier_service.track_and_update_shipments(db, full_sync=full_sync)

async def run_full_sync(db: AsyncSession, days: int):
    await run_orders_sync(db, days, full_sync=True)
    await run_couriers_sync(db, full_sync=True)
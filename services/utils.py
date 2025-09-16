from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Any
import re
import json
import logging
import models
from settings import settings

# --- Funcții de Parsare și Mapare ---

def extract_gid(gid_string: Optional[str]) -> Optional[str]:
    """Extrage ID-ul numeric dintr-un GID Shopify (ex: 'gid://shopify/Order/12345')."""
    if not gid_string:
        return None
    match = re.search(r'/(\d+)$', gid_string)
    return match.group(1) if match else None

def parse_timestamp(timestamp_str: Optional[str]) -> Optional[datetime]:
    """Convertește un string timestamp ISO 8601 într-un obiect datetime conștient de fusul orar."""
    if not timestamp_str:
        return None
    try:
        return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
    except (ValueError, TypeError):
        return None

def get_payment_mapping(payment_gateways: List[str]) -> Optional[str]:
    """Găsește prima mapare de plată validă dintr-o listă de gateway-uri."""
    if not payment_gateways:
        return None
    for gateway in payment_gateways:
        if gateway in settings.PAYMENT_MAP:
            return settings.PAYMENT_MAP[gateway]
    return None

def get_courier_mapping(tags: List[str]) -> Optional[str]:
    """Găsește prima mapare de curier validă dintr-o listă de tag-uri."""
    if not tags:
        return None
    for tag in tags:
        normalized_tag = tag.strip().lower()
        if normalized_tag in settings.COURIER_MAP:
            return settings.COURIER_MAP[normalized_tag]
    return None

# --- Funcții de Actualizare a Relațiilor Comenzii ---

def update_line_items(order: models.Order, shopify_line_items: List[Dict]):
    """Actualizează sau adaugă articolele (line items) pentru o comandă."""
    existing_skus = {li.sku for li in order.line_items}
    new_line_items = []
    for item in shopify_line_items:
        node = item['node']
        sku = node.get('sku')
        if sku and sku not in existing_skus:
            new_line_items.append(
                models.LineItem(
                    order_id=order.id,
                    sku=sku,
                    title=node.get('title'),
                    quantity=node.get('quantity')
                )
            )
    if new_line_items:
        order.line_items.extend(new_line_items)

# Înlocuiește funcția `update_shipments` cu această versiune
def update_shipments(order: models.Order, shopify_fulfillments: List[Dict], courier_map: Dict[str, str]):
    """Actualizează sau adaugă expedierile (fulfillments/shipments) pentru o comandă."""
    existing_fulfillment_ids = {s.shopify_fulfillment_id for s in order.shipments}
    new_shipments = []
    
    for fulfillment in shopify_fulfillments:
        fulfillment_gid = extract_gid(fulfillment.get('id', ''))
        if fulfillment_gid and fulfillment_gid not in existing_fulfillment_ids:
            tracking_info = fulfillment.get('trackingInfo', [{}])[0]
            courier_name = (tracking_info.get('company') or '').lower().strip()
            
            # Caută account_key în dicționarul de mapări
            account_key = courier_map.get(courier_name)
            
            if not account_key:
                logging.warning(f"Nu s-a găsit o mapare de cont pentru curierul '{tracking_info.get('company')}' la comanda {order.name}. AWB-ul nu va fi urmărit.")

            new_shipments.append(
                models.Shipment(
                    # order_id este setat automat de SQLAlchemy la adăugarea în listă
                    shopify_fulfillment_id=fulfillment_gid,
                    fulfillment_created_at=parse_timestamp(fulfillment.get('createdAt')),
                    courier=tracking_info.get('company'),
                    awb=tracking_info.get('number'),
                    # Setează account_key, chiar dacă este None
                    account_key=account_key
                )
            )
    if new_shipments:
        order.shipments.extend(new_shipments)


def update_fulfillment_orders(order: models.Order, shopify_ff_orders: List[Dict]):
    """Actualizează sau adaugă fulfillment orders pentru o comandă."""
    existing_ff_order_ids = {fo.shopify_fulfillment_order_id for fo in order.fulfillment_orders}
    new_ff_orders = []

    for item in shopify_ff_orders:
        node = item['node']
        ff_order_gid = extract_gid(node.get('id', ''))
        if ff_order_gid and ff_order_gid not in existing_ff_order_ids:
            # --- MODIFICARE AICI ---
            # Corectăm logica pentru a trata 'fulfillmentHolds' ca pe o listă.
            fulfillment_holds = node.get('fulfillmentHolds', [])
            # --- FINAL MODIFICARE ---
            
            new_ff_orders.append(
                models.FulfillmentOrder(
                    order_id=order.id,
                    shopify_fulfillment_order_id=ff_order_gid,
                    status=node.get('status'),
                    # Luăm primul element din listă, dacă există
                    hold_details=fulfillment_holds[0] if fulfillment_holds else None
                )
            )
            
    if new_ff_orders:
        order.fulfillment_orders.extend(new_ff_orders)

def get_shipping_address_from_metafield(metafield: Dict[str, Any]) -> Dict[str, Any]:
    """Parsează adresa de livrare dintr-un metafield JSON."""
    if not metafield or not metafield.get('value'):
        return {}
    try:
        address_data = json.loads(metafield['value'])
        return {
            "shipping_name": f"{address_data.get('firstName', '')} {address_data.get('lastName', '')}".strip(),
            "shipping_address1": address_data.get('address1'),
            "shipping_address2": address_data.get('address2'),
            "shipping_city": address_data.get('city'),
            "shipping_province": address_data.get('province'),
            "shipping_zip": address_data.get('zip'),
            "shipping_country": address_data.get('country'),
            "shipping_phone": address_data.get('phone')
        }
    except (json.JSONDecodeError, TypeError):
        logging.warning("Nu s-a putut decoda JSON-ul din metafield-ul de adresă.")
        return {}

# --- Funcție de Calcul Status Derivat ---

def calculate_and_set_derived_status(order: models.Order):
    """Calculează și setează statusul derivat al comenzii."""
    now = datetime.now(timezone.utc)
    RAW_STATUS_TO_GROUP_KEY = {s.lower().strip(): group for group, (_, statuses) in settings.COURIER_STATUS_MAP.items() for s in statuses}
    
    def get_shipment_sort_key(shipment):
        return (shipment.fulfillment_created_at or datetime.min.replace(tzinfo=timezone.utc), shipment.id)

    latest_shipment = max(order.shipments, key=get_shipment_sort_key) if order.shipments else None

    # Logica pentru statusul de procesare
    order_tags = {tag.strip().lower() for tag in (order.tags or '').split(',')}
    if 'on-hold' in order_tags or 'hold' in order_tags:
        order.processing_status = "On Hold"
    elif order.address_status == 'invalid':
        order.processing_status = "Adresă Invalidă"
    elif order.address_status == 'nevalidat':
        order.processing_status = "Așteaptă Validare"
    elif not latest_shipment or not latest_shipment.awb:
        order.processing_status = "Neprocesată"
    else:
        order.processing_status = "Procesată"

    # Logica pentru statusul derivat
    raw_status = (latest_shipment.last_status or 'AWB Generat').strip() if latest_shipment else ''
    courier_status_key = RAW_STATUS_TO_GROUP_KEY.get(raw_status.lower())
    
    is_on_hold = order.is_on_hold_shopify or 'on-hold' in order_tags or 'hold' in order_tags
    is_canceled_event = (order.cancelled_at is not None) or (courier_status_key == 'canceled')
    has_left_warehouse = courier_status_key in ('shipped', 'in_transit', 'pickup_office', 'delivery_issues', 'delivered', 'refused')
    
    new_status = "N/A"

    if is_canceled_event:
        new_status = "❌ Refuzată" if has_left_warehouse else "❌ Anulată"
    elif is_on_hold:
        new_status = "🚦 On Hold"
    elif not latest_shipment or not latest_shipment.awb:
        new_status = "📦 Neprocesată"
    elif courier_status_key == 'delivered':
        new_status = "✅ Livrată"
    elif courier_status_key == 'refused':
        new_status = "❌ Refuzată"
    elif courier_status_key == 'processed':
        if order.fulfilled_at and order.fulfilled_at < (now - timedelta(days=3)):
            new_status = "⏰ Netrimisă (Alertă)"
        else:
            new_status = "✈️ Procesată"
    elif courier_status_key == 'shipped':
        new_status = "🚚 Expediată"
    elif courier_status_key in ('in_transit', 'pickup_office', 'delivery_issues'):
        new_status = "🚚 În curs de livrare"
    else:
        new_status = f"❔ {raw_status}" if raw_status and raw_status != 'AWB Generat' else "✈️ Procesată"
        
    order.derived_status = new_status


    
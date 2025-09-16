# services/shopify_service.py

import httpx
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Any
import models # <-- Am schimbat importul: folosim modelele din DB, nu setările

async def fetch_orders(store: models.Store, since_days: int) -> list:
    """
    Preia TOATE comenzile de la Shopify folosind paginarea,
    folosind un obiect 'Store' din baza de date.
    """
    since_date = datetime.now(timezone.utc) - timedelta(days=since_days)
    since_str = since_date.isoformat()
    
    all_orders = []
    has_next_page = True
    cursor = None

    # Accesăm atributele direct de pe modelul SQLAlchemy
    url = f"https://{store.domain}/admin/api/{store.api_version or '2025-07'}/graphql.json"
    headers = {
        "X-Shopify-Access-Token": store.access_token,
        "Content-Type": "application/json",
    }
    
    logging.warning(f"Se preiau datele PII din Shopify API pentru {store.domain}")
    shipping_address_query_part = ""
    if store.pii_source == 'shopify':
        shipping_address_query_part = """
            shippingAddress { firstName, lastName, address1, address2, city, province, zip, country, phone }
            email
        """
    
    async with httpx.AsyncClient(timeout=60.0) as client:
        while has_next_page:
            pagination_part = f', after: "{cursor}"' if cursor else ""
            query = f"""
            {{
                orders(first: 250, sortKey: CREATED_AT, reverse: true, query: "created_at:>{since_str}"{pagination_part}) {{
                    edges {{
                        cursor
                        node {{
                            id, name, createdAt, cancelledAt, displayFinancialStatus,
                            displayFulfillmentStatus, tags, note,
                            totalPriceSet {{ shopMoney {{ amount }} }},
                            paymentGatewayNames,
                            {shipping_address_query_part}
                            metafield(namespace: "custom", key: "adresa") {{ value }},
                            lineItems(first: 50) {{ edges {{ node {{ sku, title, quantity }} }} }},
                            fulfillments {{ createdAt, id, trackingInfo {{ company, number, url }} }},
                            fulfillmentOrders(first: 10) {{ edges {{ node {{ id, status, fulfillmentHolds {{ reason, reasonNotes }} }} }} }}
                        }}
                    }}
                    pageInfo {{
                        hasNextPage
                    }}
                }}
            }}
            """
            payload = {"query": query}
            
            try:
                response = await client.post(url, json=payload, headers=headers)
                response.raise_for_status()
                data = response.json()

                if "errors" in data:
                    logging.error(f"Eroare GraphQL pentru {store.domain}: {data['errors']}")
                    break

                orders_data = data.get("data", {}).get("orders", {})
                if not orders_data or not orders_data.get("edges"):
                    break

                edges = orders_data["edges"]
                all_orders.extend([edge["node"] for edge in edges])
                
                has_next_page = orders_data.get("pageInfo", {}).get("hasNextPage", False)
                if has_next_page and edges:
                    cursor = edges[-1]["cursor"]
                else:
                    has_next_page = False

            except httpx.HTTPStatusError as e:
                logging.error(f"Eroare HTTP la preluarea comenzilor pentru {store.domain}: {e.response.status_code} - {e.response.text}")
                break
            except Exception as e:
                logging.error(f"Eroare neașteptată la preluarea comenzilor pentru {store.domain}: {e}")
                break
            
    logging.warning(f"S-au preluat în total {len(all_orders)} comenzi de la {store.domain}.")
    return all_orders


async def notify_shopify_of_shipment(store_cfg: models.Store, order_gid: str, fulfillment_id: str, tracking_info: Dict[str, str]):
    """
    Actualizează o livrare (fulfillment) în Shopify cu informațiile de tracking.
    Folosește un obiect 'Store' din baza de date.
    """
    url = f"https://{store_cfg.domain}/admin/api/{store_cfg.api_version or '2024-07'}/graphql.json"
    headers = {
        "X-Shopify-Access-Token": store_cfg.access_token,
        "Content-Type": "application/json",
    }

    mutation = """
    mutation fulfillmentTrackingInfoUpdate($fulfillmentId: ID!, $trackingInfo: FulfillmentTrackingInput!) {
        fulfillmentTrackingInfoUpdate(fulfillmentId: $fulfillmentId, trackingInfo: $trackingInfo) {
            fulfillment {
                id
                trackingInfo {
                    company
                    number
                    url
                }
            }
            userErrors {
                field
                message
            }
        }
    }
    """
    
    variables = {
        "fulfillmentId": fulfillment_id,
        "trackingInfo": {
            "company": tracking_info.get("company"),
            "number": tracking_info.get("number"),
            "url": tracking_info.get("url")
        }
    }
    
    payload = {"query": mutation, "variables": variables}

    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(url, json=payload, headers=headers)
            response.raise_for_status()
            data = response.json()
            user_errors = data.get("data", {}).get("fulfillmentTrackingInfoUpdate", {}).get("userErrors", [])
            if user_errors:
                logging.error(f"Eroare la actualizarea tracking-ului pentru {order_gid} pe magazinul {store_cfg.domain}: {user_errors}")
            else:
                logging.info(f"Tracking actualizat cu succes pentru {order_gid} pe {store_cfg.domain}.")
        except httpx.HTTPStatusError as e:
            logging.error(f"Eroare HTTP la notificarea Shopify pentru {store_cfg.domain}: {e.response.status_code} - {e.response.text}")
        except Exception as e:
            logging.error(f"Eroare neașteptată la notificarea Shopify pentru {store_cfg.domain}: {e}")
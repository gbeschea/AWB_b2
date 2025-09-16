import sys
import os
import asyncio

# --- START FIX: Add project root to Python path ---
# This allows the script to find and import modules like 'database' and 'models'
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)
# --- END FIX ---

from sqlalchemy import select
from database import AsyncSessionLocal
import models
from services.shopify_service import ShopifyAdminAPI
from settings import settings

# --- Configuration ---
# Define the webhook topics you want to subscribe to.
WEBHOOK_TOPICS = [
    "orders/create",
    "orders/updated",
    "orders/edited",
    "orders/delete",
    "fulfillments/create",
    "fulfillments/update",
]

# The public URL where your app receives webhooks.
# For local testing, this will be your ngrok URL.
WEBHOOK_RECEIVER_URL = "https://6bc96baa55c8.ngrok-free.app/webhooks/shopify"

async def register_webhooks_for_store(store: models.Store):
    """
    Ensures all required webhooks are registered for a single store,
    and removes any unnecessary ones.
    """
    print(f"\n--- Processing webhooks for store: {store.name} ---")

    api = ShopifyAdminAPI(store)
    
    try:
        existing_webhooks = await api.get_existing_webhooks()
        print(f"Found {len(existing_webhooks)} existing webhooks.")
    except Exception as e:
        print(f"ERROR: Could not fetch webhooks for {store.name}. Skipping. Reason: {e}")
        return

    existing_topics = {wh['topic']: wh['id'] for wh in existing_webhooks if wh['address'] == WEBHOOK_RECEIVER_URL}

    for topic, webhook_id in existing_topics.items():
        if topic not in WEBHOOK_TOPICS:
            print(f"Deleting obsolete webhook: {topic}")
            await api.delete_webhook(webhook_id)

    for topic in WEBHOOK_TOPICS:
        if topic not in existing_topics:
            print(f"Creating missing webhook: {topic}")
            await api.create_webhook(topic, WEBHOOK_RECEIVER_URL)
        else:
            print(f"Webhook already exists: {topic}")
            
    print(f"--- Finished processing for {store.name} ---")


async def main():
    """Main function to run the registration for all stores."""
    if "your-public-url.com" in WEBHOOK_RECEIVER_URL:
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        print("!!! ERROR: Please update WEBHOOK_RECEIVER_URL in     !!!")
        print("!!! scripts/register_webhooks.py before running.     !!!")
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        return

    print("Starting webhook registration process...")
    db = AsyncSessionLocal()
    try:
        stores_res = await db.execute(select(models.Store).where(models.Store.shared_secret.isnot(None)))
        all_stores = stores_res.scalars().all()

        if not all_stores:
            print("No stores found in the database. Exiting.")
            return

        for store in all_stores:
            await register_webhooks_for_store(store)

    finally:
        await db.close()
    
    print("\nWebhook registration process complete.")


if __name__ == "__main__":
    asyncio.run(main())
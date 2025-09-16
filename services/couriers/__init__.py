# services/couriers/__init__.py
from typing import Optional
import httpx
from .base import BaseCourier
from .dpd import DPDCourier
from .sameday import SamedayCourier

_http_client = httpx.AsyncClient(timeout=45.0)

_courier_instances = {
    "dpd": DPDCourier(_http_client),
    "sameday": SamedayCourier(_http_client),
}

def get_courier_service(courier_key: str) -> Optional[BaseCourier]:
    if not courier_key:
        return None
    for key, instance in _courier_instances.items():
        if key in courier_key.lower():
            return instance
    return None
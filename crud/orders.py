# crud/orders.py

from sqlalchemy.orm import Session
import models
import schemas

def get_order(db: Session, order_id: int):
    """
    Preia o singură comandă din baza de date pe baza ID-ului.
    """
    return db.query(models.Order).filter(models.Order.id == order_id).first()

def get_orders(db: Session, skip: int = 0, limit: int = 100):
    """
    Preia o listă de comenzi din baza de date.
    """
    return db.query(models.Order).offset(skip).limit(limit).all()

def create_order(db: Session, order: schemas.OrderCreate): # Va trebui să creezi schema OrderCreate în schemas.py
    """
    Creează o nouă comandă în baza de date.
    """
    db_order = models.Order(**order.dict())
    db.add(db_order)
    db.commit()
    db.refresh(db_order)
    return db_order

def get_stores_by_ids(db: Session, store_ids: list[int]):
    return db.query(models.Store).filter(models.Store.id.in_(store_ids)).all()

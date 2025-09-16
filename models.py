# /models.py

from datetime import datetime
from sqlalchemy import Column, Integer, String, ForeignKey, Text, Float, Table, Index, Boolean
from sqlalchemy.dialects.postgresql import TIMESTAMP, JSONB
from sqlalchemy.orm import relationship
from sqlalchemy.types import JSON
from sqlalchemy.sql import func
from database import Base 

# --- Hărți pentru relații Many-to-Many ---
courier_category_map = Table('courier_category_map', Base.metadata,
    Column('category_id', Integer, ForeignKey('courier_categories.id'), primary_key=True),
    Column('courier_key', String(64), primary_key=True)
)
store_category_map = Table('store_category_map', Base.metadata,
    Column('category_id', Integer, ForeignKey('store_categories.id'), primary_key=True),
    Column('store_id', Integer, ForeignKey('stores.id'), primary_key=True)
)

# --- Modele de Bază ---
class CourierCategory(Base):
    __tablename__ = 'courier_categories'
    id = Column(Integer, primary_key=True)
    name = Column(String(255), unique=True, nullable=False)
    tracking_url_template = Column(String(512), nullable=True)

class StoreCategory(Base):
    __tablename__ = 'store_categories'
    id = Column(Integer, primary_key=True)
    name = Column(String(255), unique=True, nullable=False)
    stores = relationship("Store", secondary=store_category_map, back_populates="categories")
    default_courier = Column(String(64), nullable=True)
    default_courier_account = Column(String(64), nullable=True)
    dpd_pickup_location_id = Column(String(255), nullable=True)

class Store(Base):
  __tablename__ = 'stores'
  id = Column(Integer, primary_key=True)
  name = Column(String(255))
  domain = Column(String(255), unique=True)
  shared_secret = Column(String(255), nullable=True)
  access_token = Column(String(255), nullable=True)
  api_version = Column(String(32), default='2025-07', nullable=False)
  pii_source = Column(String(32), default='shopify', nullable=False)
  is_active = Column(Boolean, default=True, nullable=False)
  last_sync_at = Column(TIMESTAMP(timezone=True), nullable=True)
  orders = relationship('Order', back_populates='store')
  categories = relationship("StoreCategory", secondary=store_category_map, back_populates="stores")
  paper_size = Column(String(16), default='A6', nullable=False)
  dpd_client_id = Column(String(255), nullable=True)

class Order(Base):
  __tablename__ = 'orders'
  id = Column(Integer, primary_key=True)
  store_id = Column(Integer, ForeignKey('stores.id'))
  shopify_order_id = Column(String(50), unique=True, index=True)
  name = Column(String(64))
  customer = Column(String(255))
  created_at = Column(TIMESTAMP(timezone=True))
  updated_at = Column(TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now())
  cancelled_at = Column(TIMESTAMP(timezone=True), nullable=True)
  financial_status = Column(String(64), index=True)
  total_price = Column(Float, nullable=True)
  payment_gateway_names = Column(Text, nullable=True)
  mapped_payment = Column(String(64), nullable=True)
  tags = Column(Text, nullable=True)
  note = Column(Text, nullable=True)
  sync_status = Column(String(32), default='not_synced')
  last_sync_at = Column(TIMESTAMP(timezone=True), nullable=True)
  shopify_status = Column(String(64), nullable=True, index=True)
  fulfilled_at = Column(TIMESTAMP(timezone=True), nullable=True)
  shipping_name = Column(Text, nullable=True)
  shipping_address1 = Column(Text, nullable=True)
  shipping_address2 = Column(Text, nullable=True)
  shipping_phone = Column(String(64), nullable=True)
  shipping_city = Column(String(255), nullable=True)
  shipping_zip = Column(String(32), nullable=True)
  shipping_province = Column(String(255), nullable=True)
  shipping_country = Column(String(255), nullable=True)
  address_status = Column(String(32), default='nevalidat', index=True, nullable=False)
  address_score = Column(Integer, nullable=True)
  address_validation_errors = Column(JSON, nullable=True)
  processing_status = Column(String(32), default='pending_validation', index=True, nullable=False)
  assigned_courier = Column(String(64), nullable=True)
  is_on_hold_shopify = Column(Boolean, default=False, nullable=False, index=True)
  derived_status = Column(String(255), nullable=True, index=True)
  store = relationship('Store', back_populates='orders')
  line_items = relationship('LineItem', back_populates='order', cascade='all, delete-orphan')
  shipments = relationship('Shipment', back_populates='order', cascade='all, delete-orphan')
  fulfillment_orders = relationship('FulfillmentOrder', back_populates='order', cascade='all, delete-orphan')

class Shipment(Base):
  __tablename__ = 'shipments'
  id = Column(Integer, primary_key=True)
  order_id = Column(Integer, ForeignKey('orders.id'))
  fulfillment_created_at = Column(TIMESTAMP(timezone=True), nullable=True)
  shopify_fulfillment_id = Column(String(50), nullable=True, index=True)
  awb = Column(String(64), index=True)
  courier_specific_data = Column(JSON, nullable=True)
  courier = Column(String(64), index=True)
  account_key = Column(String(32))
  paper_size = Column(String(16))
  printed_at = Column(TIMESTAMP(timezone=True), nullable=True, index=True)
  last_status = Column(String(255), nullable=True, index=True)
  last_status_at = Column(TIMESTAMP(timezone=True), nullable=True)
  derived_status = Column(String(255), nullable=True)
  order = relationship('Order', back_populates='shipments')

class RomaniaAddress(Base):
    __tablename__ = 'romania_addresses'
    id = Column(Integer, primary_key=True)
    judet = Column(String(255), index=True)
    localitate = Column(String(255), index=True)
    sector = Column(String(32), nullable=True, index=True)
    tip_artera = Column(String(64), nullable=True, index=True)
    nume_strada = Column(String(512), nullable=True, index=True)
    cod_postal = Column(String(10), index=True)
    __table_args__ = (Index('ix_localitate_judet', 'localitate', 'judet'),)

class LineItem(Base):
  __tablename__ = 'line_items'
  id = Column(Integer, primary_key=True)
  order_id = Column(Integer, ForeignKey('orders.id'))
  sku = Column(String(128), index=True)
  title = Column(Text)
  quantity = Column(Integer)
  order = relationship('Order', back_populates='line_items')

class FulfillmentOrder(Base):
    __tablename__ = 'fulfillment_orders'
    id = Column(Integer, primary_key=True)
    order_id = Column(Integer, ForeignKey('orders.id'), index=True)
    shopify_fulfillment_order_id = Column(String(50), unique=True, index=True)
    status = Column(String(64))
    hold_details = Column(JSON, nullable=True)
    order = relationship('Order', back_populates='fulfillment_orders')

class PrintLog(Base):
    __tablename__ = 'print_logs'
    id = Column(Integer, primary_key=True)
    created_at = Column(TIMESTAMP(timezone=True), default=datetime.utcnow)
    category_name = Column(String(255))
    category_id = Column(Integer)
    awb_count = Column(Integer)
    user_ip = Column(String(45), nullable=True)
    pdf_path = Column(String(512), nullable=True)
    entries = relationship('PrintLogEntry', back_populates='log', cascade='all, delete-orphan')

class PrintLogEntry(Base):
    __tablename__ = 'print_log_entries'
    id = Column(Integer, primary_key=True)
    print_log_id = Column(Integer, ForeignKey('print_logs.id'))
    order_name = Column(String(64))
    awb = Column(String(64), index=True)
    log = relationship('PrintLog', back_populates='entries')

class CourierAccount(Base):
    __tablename__ = 'courier_accounts'
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    account_key = Column(String(64), unique=True, nullable=False, index=True)
    courier_type = Column(String(64), nullable=False, index=True)
    tracking_url = Column(String(512), nullable=True)
    credentials = Column(JSONB, nullable=True)
    is_active = Column(Boolean, default=True, nullable=False)
    mappings = relationship("CourierMapping", back_populates="account")

class CourierMapping(Base):
    __tablename__ = 'courier_mappings'
    id = Column(Integer, primary_key=True)
    shopify_name = Column(String(255), unique=True, nullable=False, index=True)
    account_key = Column(String(64), ForeignKey('courier_accounts.account_key'), nullable=False)
    account = relationship("CourierAccount", back_populates="mappings")
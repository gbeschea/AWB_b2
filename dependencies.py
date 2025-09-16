# dependencies.py
from fastapi.templating import Jinja2Templates
from datetime import datetime
from zoneinfo import ZoneInfo

ROMANIA_TZ = ZoneInfo("Europe/Bucharest")

def to_local_time(utc_dt: datetime):
    if utc_dt is None:
        return None
    return utc_dt.astimezone(ROMANIA_TZ)

templates = Jinja2Templates(directory="templates")
templates.env.filters['localtime'] = to_local_time

def get_templates():
    return templates

# MODIFICARE: Am adăugat funcția de paginare
def get_pagination_numbers(current_page: int, total_pages: int, context_size: int = 2) -> list:
    """Generează o listă de numere de pagină pentru controalele de paginare."""
    if total_pages <= 1:
        return []

    page_numbers = []
    start_page = max(1, current_page - context_size)
    end_page = min(total_pages, current_page + context_size)

    if start_page > 1:
        page_numbers.append(1)
        if start_page > 2:
            page_numbers.append('...')
    
    page_numbers.extend(range(start_page, end_page + 1))

    if end_page < total_pages:
        if end_page < total_pages - 1:
            page_numbers.append('...')
        page_numbers.append(total_pages)
        
    return page_numbers
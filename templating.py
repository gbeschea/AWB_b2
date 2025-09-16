from fastapi.templating import Jinja2Templates
from babel.dates import format_datetime
import pytz
from slugify import slugify

# --- Funcția NOUĂ pentru filtrul 'localtime' ---
def to_localtime(utc_dt, tz='Europe/Bucharest'):
    """Convertește un obiect datetime din UTC în fusul orar local."""
    if utc_dt is None:
        return None
    local_tz = pytz.timezone(tz)
    # Asigură-te că data primită este conștientă de fusul orar UTC
    utc_dt = utc_dt.replace(tzinfo=pytz.utc)
    return utc_dt.astimezone(local_tz)

# Funcție existentă pentru formatare directă
def format_datetime_local(utc_dt, format='medium', tz='Europe/Bucharest'):
    """Formatează direct un datetime UTC în string local."""
    local_dt = to_localtime(utc_dt, tz)
    if local_dt is None:
        return ""
    return format_datetime(local_dt, format=format, locale='ro_RO')

# Inițializează motorul de template-uri
templates = Jinja2Templates(directory="templates")

# --- Înregistrează ambele filtre ---
templates.env.filters['datetime_local'] = format_datetime_local
templates.env.filters['localtime'] = to_localtime # Adaugă noul filtru
templates.env.filters['slugify'] = slugify
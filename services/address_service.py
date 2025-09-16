# /services/address_service.py

import logging
import unicodedata
import re
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from Levenshtein import distance as levenshtein_distance
import models
from typing import Optional, List, Tuple, Set

LOCKER_KEYWORDS = {"easybox", "locker", "emag", "sameday", "dpd", "fan", "cargus", "gls", "box", "pachetomat"}
STREET_PREFIXES = [
    'strada', 'str', 'str.', 'bulevardul', 'bd', 'bdul', 'b-dul', 'calea', 'soseaua', 'sos', 'sos.', 
    'drumul', 'drm', 'aleea', 'alee', 'piata', 'p-ta', 'prelungirea'
]

def normalize(s: Optional[str]) -> str:
    """Funcția centrală de normalizare."""
    if not s: return ""
    s = s.lower().strip()
    # Înlocuim "ă" și "î" cu "a" și "i" înainte de a elimina complet diacriticele
    s = s.replace('ă', 'a').replace('î', 'i')
    s = "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")
    s = re.sub(r'[^a-z0-9\s-]', ' ', s)
    return re.sub(r'\s+', ' ', s).strip()

def extract_street_info(s: Optional[str]) -> Tuple[str, bool]:
    """Extrage numele de bază al străzii și verifică dacă adresa conține un număr."""
    if not s: return "", False
    
    has_number = any(char.isdigit() for char in s)
    s_norm = normalize(s)
    
    for prefix in STREET_PREFIXES:
        if s_norm.startswith(prefix + ' '):
            s_norm = s_norm[len(prefix):].strip()
            break
            
    # Eliminăm agresiv tot ce urmează după un indicator de număr/bloc sau un număr
    s_norm = re.split(r'\s+(nr|nr\.|numar|bl|bloc|sc|scara|ap|apartament|et|etaj|cam|camera|complex|parc|cladirea|sector|casa|complex|iride|business|park)\b|\s+\d', s_norm, 1)[0]
    
    common_cities = ['brasov', 'bucuresti', 'pitesti', 'sibiu', 'craiova', 'constanta', 'arad', 'timisoara', 'cluj', 'napoca', 'oradea', 'iasi', 'galati']
    for city in common_cities:
        s_norm = re.sub(r'\b' + city + r'\b', '', s_norm)
    
    return s_norm.strip(), has_number

async def validate_address_for_order(db: AsyncSession, order: models.Order):
    """
    Validează adresa folosind o logică avansată, multi-pas, conform feedback-ului.
    """
    order_name = order.name
    logging.info(f"--- Validare pentru Comanda: {order_name} ---")
    
    in_street_raw = f"{order.shipping_address1 or ''} {order.shipping_address2 or ''}".strip()
    
    if any(keyword in in_street_raw.lower() for keyword in LOCKER_KEYWORDS):
        order.address_status = 'valid'; order.address_score = 100
        order.address_validation_errors = ["Adresă de tip Locker/Pickup Point, marcată automat ca validă."]
        logging.info(f"OK {order_name}: Valid (Locker/Pickup Point).")
        return

    in_judet_raw = order.shipping_province
    in_city_raw = order.shipping_city
    in_zip_raw = order.shipping_zip

    core_street_name, has_number = extract_street_info(in_street_raw)
    in_judet_normalized = normalize(in_judet_raw)
    in_city_normalized = normalize(re.sub(r'\(.*\)', '', in_city_raw or "").split(',')[0].strip())
    
    logging.info(f"-> INTRARE: Judet='{in_judet_raw}', Oras='{in_city_raw}', Strada='{in_street_raw}'")
    logging.info(f"-> NORMALIZAT: Judet='{in_judet_normalized}', Oras='{in_city_normalized}', Strada='{core_street_name}', AreNumar={has_number}")
    
    errors = []
    
    # Căutăm TOATE localitățile din județul respectiv
    stmt = select(models.RomaniaAddress).where(func.unaccent(func.lower(models.RomaniaAddress.judet)) == in_judet_normalized)
    result = await db.execute(stmt)
    localities_in_judet = result.scalars().all()

    if not localities_in_judet:
        order.address_status = 'not_found'; order.address_validation_errors = [f"Județul '{in_judet_raw}' nu a fost găsit."]
        logging.warning(f"FAIL {order_name}: Not Found. MOTIV: Județul '{in_judet_normalized}' nu a returnat niciun rezultat.")
        return

    db_cities_normalized = {normalize(loc.localitate): loc.localitate for loc in localities_in_judet}
    
    best_city_match_name, min_dist = None, 100
    for norm_db_city, raw_db_city in db_cities_normalized.items():
        dist = levenshtein_distance(in_city_normalized, norm_db_city)
        if dist < min_dist:
            min_dist = dist
            best_city_match_name = raw_db_city
    
    if not best_city_match_name or min_dist > 2: # Prag strict pentru potrivirea orașului
        order.address_status = 'not_found'
        order.address_validation_errors = [f"Orașul '{in_city_raw}' nu a fost găsit în județul '{in_judet_raw}'."]
        logging.warning(f"FAIL {order_name}: Not Found. MOTIV: Cea mai bună potrivire pt '{in_city_normalized}' a fost '{best_city_match_name}' cu distanța {min_dist} (>2).")
        return

    logging.info(f"-> INFO: Găsit potrivire oraș: '{in_city_raw}' -> '{best_city_match_name}' (Distanța: {min_dist})")
    city_streets = [addr for addr in localities_in_judet if addr.localitate == best_city_match_name]
    
    has_streets_in_db = any(addr.nume_strada for addr in city_streets)
    if not has_streets_in_db:
        if not has_number:
            order.address_status = 'invalid'; order.address_score = 10
            errors.append(f"Localitate fără nomenclator stradal, dar adresa nu conține un număr.")
        else:
            order.address_status = 'valid'; order.address_score = 100
            errors.append(f"Localitate fără nomenclator stradal. Adresa are număr și este considerată validă.")
        order.address_validation_errors = errors
        logging.info(f"OK/FAIL {order_name}: {order.address_status} (localitate fără străzi în DB).")
        return

    # Validare avansată pentru străzi
    input_street_words = set(core_street_name.split())
    
    best_match_street_obj, best_jaccard_score = None, -1.0
    for street_obj in city_streets:
        db_street_name = normalize(f"{street_obj.tip_artera or ''} {street_obj.nume_strada or ''}")
        db_street_words = set(db_street_name.split())
        
        intersection = len(input_street_words.intersection(db_street_words))
        union = len(input_street_words.union(db_street_words))
        jaccard_score = intersection / union if union > 0 else 0
        
        if jaccard_score > best_jaccard_score:
            best_jaccard_score = jaccard_score
            best_match_street_obj = street_obj

    if not best_match_street_obj or best_jaccard_score < 0.5: # Prag minim de 50% potrivire pe cuvinte
        order.address_status = 'invalid'; final_status = "Invalid"
        errors.append(f"Numele străzii ('{in_street_raw}') nu se potrivește cu nicio intrare din '{best_city_match_name}'.")
        order.address_validation_errors = errors
        logging.warning(f"REZULTAT {order_name}: {final_status}. MOTIVE: {errors}")
        return

    best_match_street_name = normalize(f"{best_match_street_obj.tip_artera or ''} {best_match_street_obj.nume_strada or ''}")
    dist = levenshtein_distance(core_street_name, best_match_street_name)
    
    # Scorul este acum bazat pe potrivirea cuvintelor și finețea potrivirii Levenshtein
    score = (best_jaccard_score * 100) - (dist * 5)
    
    logging.info(f"-> STRADA: Intrare='{core_street_name}', Potrivire='{best_match_street_name}' (Jaccard: {best_jaccard_score:.2f}, Distanta: {dist})")
    
    if in_zip_raw and order.shipping_zip != best_match_street_obj.cod_postal:
        errors.append(f"Sugestie cod poștal: {best_match_street_obj.cod_postal}")
    
    order.address_score = score
    
    if score >= 90:
        order.address_status = 'valid'; final_status = "Valid"
    elif score >= 60:
        order.address_status = 'partial_match'; final_status = "Partial Match"
        if dist > 1: # Dăm sugestie doar dacă există o diferență
             errors.append(f"Sugestie stradă: '{best_match_street_obj.nume_strada}'")
    else:
        order.address_status = 'invalid'; final_status = "Invalid"
        errors.append(f"Numele străzii ('{in_street_raw}') nu se potrivește cu nicio intrare din '{best_city_match_name}'.")

    order.address_validation_errors = errors
    logging.warning(f"REZULTAT {order_name}: {final_status} (scor {score}). MOTIVE: {errors if errors else 'Niciunul'}")
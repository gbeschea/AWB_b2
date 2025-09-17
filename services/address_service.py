# address_service.py
# Validator adrese RO pentru comenzi Shopify (v2 - 2025-09-16)
# Îmbunătățiri față de v1:
# - Stradă + număr OBLIGATORIU pentru toate adresele (cu excepția locker/pick-up)
# - În localități fără nomenclator stradal: dacă există stradă+număr în input -> VALID (nu doar partial)
# - Normalizare ZIP: acceptă 5/6 cifre; pentru București face pad la 6 cu 0 în față
# - Potrivire tolerantă pe străzi cu Jaccard(cuvinte nucleu) + SequenceMatcher
# - Suport prefixe noi (b-ul, bdl, bld, blv etc.), ignoră titluri (dr., arh.), elimină tokens de 1 caracter
# - Parsare număr casă din text liber (ex: "nr3", "12 A1", "11-13", "35-T")
# - Verificare intervale multiple din nomenclator (ex: "nr. 1-25; 2-14A; 71-T")
# - Zgomot/rumoare în adresă ignorat (ex: CNAS, Iride, Bloc, Sc., Ap., Et., Ghișeul etc.)
# - Sugestii mai bune pentru stradă și ZIP; scoruri consistente
#
# Integrare:
#   async def validate_address_for_order(db: AsyncSession, order: models.Order) -> None
#     setează pe 'order': address_status, address_score, address_validation_errors (list[str])
#
# Așteptări modele:
#   models.RomaniaAddress(judet, localitate, tip_artera, nume_strada, numar, cod_postal)
#   models.Order: name, shipping_address1, shipping_address2, shipping_zip, shipping_province, shipping_city
#
# Notă: acest modul nu are dependențe externe în afară de SQLAlchemy async și python standard.

from __future__ import annotations

import logging
import re
import unicodedata
from difflib import SequenceMatcher
from typing import Optional, Dict, Set, Tuple, List

from sqlalchemy import select, or_
from sqlalchemy.ext.asyncio import AsyncSession

import models


# --------------------------- CONSTANTE ---------------------------

LOCKER_KEYWORDS = {
    "easybox", "locker", "parcel locker", "packeta", "fanbox", "pachetomat",
    "parcelshop", "inpost", "omniva", "pickup point", "pick-up point", "automatul", "automat colet"
}

# Alias-uri / prefixe artere
PREFIX_ALIASES = {
    "strada":      ["strada", "stradă", "str", "st", "str.", "st.", "străzii"],
    "sosea":       ["sosea", "șosea", "sos", "șos", "soseaua", "șoseaua", "șos.", "sos."],
    "intrare":     ["intrare", "intrarea", "intr", "int", "intr."],
    "bulevard":    ["bulevard", "bulevardul", "bd", "bdul", "b-dul", "bvd", "blvd", "bld", "bld.", "bdl", "bdl.", "b-ul", "b-ul."],
    "piata":       ["piata", "piața", "p-ta", "p-ța", "pta", "pța", "p-ta.", "p-ța."],
    "alee":        ["alee", "aleea", "al", "al.", "aleei"],
    "cale":        ["cale", "calea", "cal", "cal."],
    "piateta":     ["piateta", "piațetă", "ptet", "pțet", "ptt", "pțt"],
    "splai":       ["splai", "splaiul", "spl", "spl."],
    "drum":        ["drum", "drumul", "dr", "drm", "dr.", "drm."],
    "prelungire":  ["prelungire", "prelungirea", "prel", "prel.", "prelung"],
    "pasaj":       ["pasaj", "pasajul", "pas", "psj", "psj."],
    "ulita":       ["ulita", "ulița", "ul", "ul."],
    "fundatura":   ["fundatura", "fundătura", "fund", "fdt", "fund.", "fdt."],
    "cartier":     ["cartier", "cart", "cart."],
    "varianta":    ["varianta", "variantă", "var", "var."],
    "platou":      ["platou"],
    "parc":        ["parc", "parcul"],
    "pod":         ["pod", "podul"],
    "canal":       ["canal"],
    "scuar":       ["scuar"],
    "poteca":      ["poteca", "potecă"],
}

PREFIX_MAP = {alias: canon for canon, aliases in PREFIX_ALIASES.items() for alias in aliases}
CANONICAL_PREFIXES = set(PREFIX_ALIASES.keys())

TITLES_TO_IGNORE = {"dr", "arh", "arhitect", "mat", "prof", "ing", "dr.", "arh.", "prof.", "ing."}

NOISE_WORDS = {
    # generice
    "sector", "sectorul", "bucuresti", "bucurești", "judetul", "jud", "localitatea", "oras", "oraș",
    "comuna", "sat", "romania", "românia", "tara", "ţara",
    # clădiri / adiacente
    "cnas", "spclep", "spclep-ul", "casa", "salon", "business", "park", "iride", "cladirea", "clădirea",
    "ghiseul", "ghișeul", "birou", "biroul", "hala", "hala.", "corp", "corpul",
    # anexe bloc
    "bl", "bloc", "sc", "scara", "ap", "apartament", "et", "etaj", "cam", "cam.", "lot", "complex",
    # alte marcaje
    "po", "box", "po box"
}

DELIMITERS = [
    r"\b(nr|no|numar|nr\.|no\.|bl|bloc|sc|scara|ap|apart|apartament|et|etaj|complex|lot|cam|cam\.|po box)\b",
    r","
]

SECTOR_ROMAN = {"i": 1, "ii": 2, "iii": 3, "iv": 4, "v": 5, "vi": 6}


# --------------------------- HELPERI ---------------------------

def normalize(s: Optional[str]) -> str:
    """ lower + remove diacritics + collapse spaces """
    if not s:
        return ""
    s = unicodedata.normalize("NFD", s.lower().strip())
    s = s.encode("ascii", "ignore").decode("utf-8")
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def normalize_zip(zip_str: Optional[str]) -> str:
    """Păstrează doar cifrele și face pad la 6 cifre (București 01xxxx etc.)."""
    if not zip_str:
        return ""
    digits = re.sub(r"\D", "", zip_str)
    if not digits:
        return ""
    # pad la 6
    if len(digits) < 6:
        digits = digits.zfill(6)
    return digits


def parse_sector(text: str) -> Optional[int]:
    """Extrage Sectorul 1–6 din text (forme: 'sector 3', 'sec. iv', 's2')."""
    t = normalize(text)
    m = re.search(r"\bsec(?:tor)?\.?\s*([1-6]|i{1,3}|iv|v|vi)\b", t)
    if not m:
        return None
    token = m.group(1)
    if token.isdigit():
        return int(token)
    token = token.lower()
    return SECTOR_ROMAN.get(token)


def lemmatize_ro_token(w: str) -> str:
    # strip frequent Romanian suffixes (longest first)
    for suf in ("ilor", "ului", "iilor", "ilor", "urilor", "ul", "le", "ei", "ii", "lor", "a", "i"):
        if w.endswith(suf) and len(w) > len(suf) + 2:
            return w[: -len(suf)]
    return w


def get_core_words(s: str) -> Set[str]:
    """Cuvinte nucleu pentru matching (prefix canonic + nume), fără zgomot.
       Elimină token-urile de 1 caracter (ex. 'c a rosetti' -> 'rosetti')."""
    words = normalize(s).split()
    if not words:
        return set()
    first = PREFIX_MAP.get(words[0], words[0])
    core: Set[str] = set()
    for w in words:
        if w in TITLES_TO_IGNORE or w in CANONICAL_PREFIXES or w in PREFIX_MAP or w in NOISE_WORDS:
            continue
        # elimină inițiale scurte (ex: 'c', 'a', 'c.a')
        if len(w) <= 2 or re.fullmatch(r"[a-z]\.?[a-z]?\.?", w):
            continue
        core.add(lemmatize_ro_token(w))
    if first in CANONICAL_PREFIXES:
        core.add(first)
    return core


def _first_delim_pos(s: str) -> int:
    pos = len(s)
    for pattern in DELIMITERS:
        m = re.search(pattern, s)
        if m and m.start() < pos:
            pos = m.start()
    return pos


def _looks_building_token_start(s: str) -> bool:
    s = normalize(s)
    if not s:
        return False
    first = s.split()[0]
    return first in {"bl", "bloc", "sc", "scara", "ap", "et", "etaj"}

def extract_street_components(address: str) -> Dict:
    """
    Extrage numele străzii și numărul (dacă apare) dintr-o adresă brută.
    return: {'street': 'splaiul unirii', 'number': '12A'/'1-3', 'has_number': bool}
    """
    s = (address or "").lower().strip()
    components = {"street": None, "number": None, "has_number": False}

    cut = _first_delim_pos(s)
    street_part = s[:cut].strip()

    # număr oriunde în adresă (ex. 'nr 12A', '12-14', '12 a1', 'nr3')
    num_match = re.search(r"(\d+\s*[a-z]?\d*(?:\s*[-–—]\s*\d+\s*[a-z]?\d*)?)", s)
    if num_match:
        num = re.sub(r"\s+", "", num_match.group(1))
        components["number"] = num
        components["has_number"] = True

    # cazul cu numărul la început ('12 Splaiul Unirii')
    num_first = re.match(r"^(\d+\s*[a-z]?\d*)\s+(.*)", street_part)
    if num_first:
        street_name = num_first.group(2)
        if not components["number"]:
            components["number"] = re.sub(r"\s+", "", num_first.group(1))
            components["has_number"] = True
    else:
        street_name = street_part

    words = street_name.split()
    cleaned = [w for w in words if w not in NOISE_WORDS]
    components["street"] = " ".join(cleaned)
    return components


def _parse_house_number(num: str) -> Tuple[Optional[int], Optional[str]]:
    """'12A' -> (12, 'A'); '34' -> (34, None); '12A1' -> (12, 'A')"""
    if not num:
        return None, None
    m = re.match(r"^(\d+)([a-z])?", num.lower())
    if not m:
        return None, None
    return int(m.group(1)), m.group(2)


def _parse_db_range_one(fragment: str) -> Tuple[Optional[int], Optional[int], bool]:
    """
    Un fragment de tip 'nr. 1-33', '21-T', '7', '2-14A'.
    Returnează (low, high, open_high). high poate fi None pentru 'T'.
    Ignoră literele de la capete pt. comparația numerică.
    """
    if not fragment:
        return None, None, False
    t = normalize(fragment)
    t = t.replace("nr.", " ").replace("nr", " ").replace("no.", " ").replace("no", " ").strip()
    t = re.sub(r"\s+", "", t)
    t = re.sub(r"[–—]", "-", t)

    if "-" in t:
        a, b = t.split("-", 1)
        try:
            low = int(re.sub(r"\D", "", a))
        except ValueError:
            return None, None, False
        if re.fullmatch(r"[tT]", b):
            return low, None, True
        try:
            high = int(re.sub(r"\D", "", b))
        except ValueError:
            return None, None, False
        return low, high, False

    m = re.search(r"(\d+)", t)
    if m:
        v = int(m.group(1))
        return v, v, False
    return None, None, False


def _parse_db_ranges(db_numar: Optional[str]) -> List[Tuple[Optional[int], Optional[int], bool]]:
    """Acceptă multiple intervale despărțite de ';' sau ','. """
    if not db_numar:
        return []
    parts = re.split(r"[;,]+", db_numar)
    ranges = []
    for p in parts:
        r = _parse_db_range_one(p)
        if any(x is not None for x in r):
            ranges.append(r)
    return ranges


def number_in_range(order_num: Optional[str], db_numar: Optional[str]) -> Optional[bool]:
    """
    True -> în interval, False -> în afara intervalului, None -> nu se poate evalua.
    Acceptă multiple intervale din DB separate prin ';' / ','.
    """
    if not order_num or not db_numar:
        return None
    base, _ = _parse_house_number(order_num)
    if base is None:
        return None
    ranges = _parse_db_ranges(db_numar)
    if not ranges:
        return None
    for low, high, open_high in ranges:
        if low is None:
            continue
        if open_high and base >= low:
            return True
        if high is None:
            high = low
        if low <= base <= high:
            return True
    return False


def seq_similarity(a: str, b: str) -> float:
    """Similarity ratio [0..1] via SequenceMatcher (no external deps)."""
    return SequenceMatcher(None, normalize(a), normalize(b)).ratio()


# --------------------------- VALIDATOR ---------------------------

async def validate_address_for_order(db: AsyncSession, order: models.Order):
    """
    Setează pe obiectul 'order':
      - address_status: 'valid' | 'partial_match' | 'invalid' | 'not_found'
      - address_score: 0..100
      - address_validation_errors: [str, ...]
    Reguli:
      - STRADĂ + NUMĂR obligatoriu (exceptând locker/pick-up)
      - În localități fără nomenclator: dacă există stradă+număr în input -> VALID (nu doar partial)
    """
    order_name = getattr(order, "name", "N/A")
    logging.info(f"[VALIDARE] Comanda: {order_name}")

    # Concat adresă
    in_street_raw = f"{order.shipping_address1 or ''} {order.shipping_address2 or ''}".strip()

    # 0) Locker / Pick-up points
    if any(k in in_street_raw.lower() for k in LOCKER_KEYWORDS):
        order.address_status = "valid"
        order.address_score = 100
        order.address_validation_errors = ["Adresă de tip Locker/Pickup Point."]
        return

    in_zip_raw = (order.shipping_zip or "").strip()
    in_zip = normalize_zip(in_zip_raw)  # 6 cifre
    in_judet = order.shipping_province or ""
    in_city = order.shipping_city or ""

    errors: List[str] = []

    if not in_judet or not in_city:
        order.address_status = "not_found"
        order.address_score = 0
        order.address_validation_errors = ["Județul și/sau localitatea lipsesc."]
        return

    # 1) Parsare stradă/număr
    parsed = extract_street_components(in_street_raw)

    # dacă nu avem stradă+număr, încearcă address2 separat
    if (not parsed.get("street") or not parsed.get("has_number")) and (order.shipping_address2 and order.shipping_address2.strip() and order.shipping_address2.lower() != 'none'):
        parsed2 = extract_street_components(order.shipping_address2)
        # Preferă address2 dacă oferă stradă + număr, sau dacă prima parseare a scos ceva dubios (ex: începe cu 'bl', 'sc')
        bad_start = False
        if parsed.get("street"):
            sw = parsed["street"].split()[:1]
            bad_start = bool(sw and sw[0] in {"bl", "bloc", "sc", "scara"})
        if parsed2.get("street") and parsed2.get("has_number"):
            parsed = parsed2
        elif bad_start and parsed2.get("street"):
            parsed = parsed2

    sector_from_city = parse_sector(in_city) or parse_sector(in_street_raw)

    # 1.1) STRADĂ + NUMĂR obligatorii (în afară de locker)
    if not (parsed["street"] and parsed["street"].strip()):
        order.address_status = "invalid"
        order.address_score = 0
        order.address_validation_errors = ["Adresă incompletă: lipsește strada."]
        return
    if not parsed["has_number"]:
        order.address_status = "invalid"
        order.address_score = 0
        order.address_validation_errors = ["Adresă incompletă: lipsește numărul străzii."]
        return

    parsed_core = get_core_words(parsed["street"] or "")

    # 2) STRATEGIA 1: Căutare după ZIP (tolerant 5/6 cifre)
    if in_zip:
        zip_candidates = {in_zip, in_zip.lstrip("0")}
        # IMPORTANT: compara doar ca text (VARCHAR). Nu trimite INTEGER la Postgres.
        stmt_zip = select(models.RomaniaAddress).where(
            models.RomaniaAddress.cod_postal.in_(list(zip_candidates))
        )
        db_addrs_by_zip = (await db.execute(stmt_zip)).scalars().all()

        if db_addrs_by_zip:
            # alegem cea mai apropiată stradă pentru ZIP dat
            best = {"score": -1.0, "ratio": -1.0, "obj": None}
            for db_addr in db_addrs_by_zip:
                db_street_full = f"{db_addr.tip_artera or ''} {db_addr.nume_strada or ''}".strip()
                db_core = get_core_words(db_street_full)
                # scor Jaccard pe cuvinte nucleu
                union = len(parsed_core.union(db_core)) or 1
                jacc = len(parsed_core.intersection(db_core)) / union
                # fallback tie-breaker pe similaritate secvențială
                ratio = seq_similarity(parsed["street"] or "", db_street_full)
                score = jacc + 0.15 * ratio  # mic boost pentru ratio
                if score > best["score"]:
                    best = {"score": score, "ratio": ratio, "obj": db_addr}

            if best["obj"]:
                db_address = best["obj"]
                db_full = f"{db_address.tip_artera or ''} {db_address.nume_strada or ''}".strip()
                db_core = get_core_words(db_full)
                union = len(parsed_core.union(db_core)) or 1
                jacc = len(parsed_core.intersection(db_core)) / union

                # scor de bază din Jaccard
                score = int(round(jacc * 100))

                # verificăm intervalul pentru număr
                db_numar = getattr(db_address, 'numar', None)
                num_ok = number_in_range(parsed["number"], db_numar)
                if num_ok is True:
                    score = min(100, score + 20)
                elif num_ok is False:
                    if db_numar:
                        errors.append(f"Numărul {parsed['number']} este în afara intervalului {db_numar}.")
                    else:
                        errors.append(f"Numărul {parsed['number']} pare în afara intervalului din nomenclator.")
                    score = max(0, score - 30)

                # Dacă potrivirea pe ZIP e slabă, continuăm cu strategia pe localitate
                if jacc < 0.5:
                    pass  # nu returnăm, trecem la strategia 2
                else:
                    # decizie status (permite lipsa prefixului dacă ratio e mare și avem număr)
                    if ((jacc >= 0.6) or (ratio >= 0.7 and parsed.get('has_number'))) and (num_ok in (True, None)):
                        order.address_status = "valid"
                    else:
                        order.address_status = "partial_match"

                    # sugestii
                    if parsed_core != db_core:
                        errors.append(f"Sugestie stradă: '{db_full}'")

                    # sugestie ZIP canonic (6 cifre)
                    db_zip = str(db_address.cod_postal) if db_address.cod_postal is not None else ""
                    if db_zip:
                        errors.append(f"Sugestie cod poștal: {normalize_zip(db_zip)}")

                    order.address_score = max(0, min(100, score))
                    order.address_validation_errors = errors
                    return

        # dacă nu s-a găsit pe ZIP -> continuăm cu potrivire pe localitate + stradă
        errors.append("Codul poștal nu a putut fi validat pentru stradă; continui potrivirea după localitate și nume.")

    # 3) STRATEGIA 2: Potrivire pe județ + localitate (apoi stradă)
    judet_key = normalize(in_judet)
    stmt_city = select(models.RomaniaAddress).where(models.RomaniaAddress.judet.ilike(judet_key))
    all_in_judet = (await db.execute(stmt_city)).scalars().all()
    if not all_in_judet:
        # fallback la valoarea brută, în caz că DB păstrează diacritice
        stmt_city = select(models.RomaniaAddress).where(models.RomaniaAddress.judet.ilike(in_judet))
        all_in_judet = (await db.execute(stmt_city)).scalars().all()
    if not all_in_judet:
        order.address_status = "not_found"
        order.address_score = 0
        order.address_validation_errors = [f"Județul '{in_judet}' nu a fost găsit în nomenclator."]
        return

    norm_city = normalize(re.sub(r"\\(.*\\)", "", in_city).split("sector")[0].strip())
    # heuristic: extract locality from 'sat'/'comuna' when city seems to be the county name
    city_seems_county = normalize(in_city) == normalize(in_judet)
    extracted_loc = None
    addr_all = f"{order.shipping_address1 or ''} {order.shipping_address2 or ''}"
    m_loc = _re.search(r"\b(?:sat|comuna)\s+([a-zA-Z\-]+)\b", normalize(addr_all))
    if city_seems_county and m_loc:
        extracted_loc = m_loc.group(1)

    # alegem cea mai apropiată localitate din județ (preferă extracted_loc dacă există)
    city_counts = {}
    for x in all_in_judet:
        city_counts[normalize(x.localitate)] = x.localitate
    best_city, best_city_ratio = None, 0.0
    for ncity, raw in city_counts.items():
        if extracted_loc and normalize(extracted_loc) == ncity:
            best_city, best_city_ratio = raw, 1.0
            break
        r = seq_similarity(norm_city, ncity)
        if r > best_city_ratio:
            best_city_ratio, best_city = r, raw
    # alegem cea mai apropiată localitate din județ
    city_counts = {}
    for x in all_in_judet:
        city_counts[normalize(x.localitate)] = x.localitate
    best_city, best_city_ratio = None, 0.0
    for ncity, raw in city_counts.items():
        r = seq_similarity(norm_city, ncity)
        if r > best_city_ratio:
            best_city_ratio, best_city = r, raw

    if not best_city or best_city_ratio < 0.5:
        order.address_status = "not_found"
        order.address_score = 0
        order.address_validation_errors = [f"Localitatea '{in_city}' nu a fost găsită în județul '{in_judet}'."]
        return

    # străzile din localitatea selectată
    stmt_city_only = select(models.RomaniaAddress).where(
        models.RomaniaAddress.judet.ilike(in_judet),
        models.RomaniaAddress.localitate == best_city
    )
    city_rows = (await db.execute(stmt_city_only)).scalars().all()
    has_db_streets = any(r.nume_strada for r in city_rows)

    if not has_db_streets:
        # Localitate fără nomenclator: dacă avem stradă + număr -> VALID (după cerință)
        order.address_status = "valid"
        order.address_score = 70  # valid cu încredere medie: verificare doar pe prezență
        order.address_validation_errors = [
            "Localitate fără nomenclator stradal: validat pe baza prezenței stradă + număr."
        ]
        return

    # Caută cea mai bună stradă din localitate
    best = {"score": -1.0, "obj": None}
    for r in city_rows:
        if not r.nume_strada:
            continue
        db_full = f"{r.tip_artera or ''} {r.nume_strada or ''}".strip()
        db_core = get_core_words(db_full)
        union = len(parsed_core.union(db_core)) or 1
        jacc = len(parsed_core.intersection(db_core)) / union
        ratio = seq_similarity(parsed["street"] or "", db_full)
        score = jacc + 0.15 * ratio
        if score > best["score"]:
            best = {"score": score, "obj": r}

    best_obj = best["obj"]
    if not best_obj:
        order.address_status = "not_found"
        order.address_score = 0
        errors.append(f"Nu am reușit să potrivesc strada '{parsed['street']}' în {best_city}.")
        order.address_validation_errors = errors
        return

    db_full = f"{best_obj.tip_artera or ''} {best_obj.nume_strada or ''}".strip()
    db_core = get_core_words(db_full)
    union = len(parsed_core.union(db_core)) or 1
    jacc = len(parsed_core.intersection(db_core)) / union
    street_score = int(round(jacc * 100))

    # penalizare ușoară dacă ZIP a fost dat dar nu a ajutat
    if in_zip:
        street_score = int(street_score * 0.9)

    # verificare număr în interval
    best_numar = getattr(best_obj, 'numar', None)
    rng_ok = number_in_range(parsed["number"], best_numar)
    if rng_ok is True:
        street_score = min(100, street_score + 20)
    elif rng_ok is False:
        if best_numar:
            errors.append(f"Numărul {parsed['number']} este în afara intervalului {best_numar}.")
        else:
            errors.append(f"Numărul {parsed['number']} pare în afara intervalului din nomenclator.")
        street_score = max(0, street_score - 30)

    # decizie finală
    if ((jacc >= 0.6) or (seq_similarity(parsed.get('street') or '', db_full) >= 0.7 and parsed.get('has_number'))) and (rng_ok in (True, None)):
        status = "valid"
    else:
        status = "partial_match"

    # sugestii
    if parsed_core != db_core:
        errors.append(f"Sugestie stradă: '{db_full}'")
    db_zip = str(best_obj.cod_postal) if best_obj.cod_postal is not None else ""
    if db_zip:
        errors.append(f"Sugestie cod poștal: {normalize_zip(db_zip)}")

    order.address_status = status
    order.address_score = max(0, min(100, street_score))
    order.address_validation_errors = errors
    return

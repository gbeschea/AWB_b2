Directorul /routes - API Layer
Acest director conține logica de "controler". Fiecare fișier definește un set de pagini sau endpoint-uri API, primește cererile de la utilizator, apelează serviciile corespunzătoare pentru a procesa datele și apoi returnează un răspuns (de obicei un template HTML).

orders.py: Gestionează afișarea paginii principale. Preia parametrii de filtrare și paginare din URL, apelează filter_service pentru a obține comenzile, și trimite datele către template-ul index.html.

printing.py: Controlează pagina "Print Hub". Afișează loturile disponibile și gestionează cererea de printare, apelând print_service pentru a genera PDF-ul final.

sync.py: Expune endpoint-urile pe care le apelează butoanele de "Sync" din interfață. Pornește task-urile de sincronizare în fundal.

Directorul /services - Business Logic Layer
Aici se află logica de business a aplicației. Aceste fișiere nu știu nimic despre pagini web sau HTML; ele primesc date, le procesează și returnează un rezultat.

sync_service.py: Este inima procesului de sincronizare. Apelează shopify_service pentru a prelua comenzile, le procesează, salvează sau actualizează înregistrările în baza de date și apelează address_service și utils pentru a valida adresele și a calcula statusurile.

filter_service.py: Este un serviciu crucial care construiește dinamic interogări SQL complexe cu SQLAlchemy. Pe baza filtrelor active, adaugă JOIN-uri și clauze WHERE pentru a returna exact setul de date cerut, într-un mod eficient.

print_service.py: Conține logica avansată de sortare ierarhică pentru "Print Hub", gândită să optimizeze procesul de picking din depozit.

couriers/__init__.py: Implementează un design pattern de tip "Factory" (Fabrică). Oferă o singură funcție, get_courier_service(), care returnează instanța corectă a serviciului de curierat (DPD, Sameday etc.) pe baza unui text, asigurând că starea (ex: token-ul de la Sameday) este partajată corect.

Fișiere Principale
main.py: Punctul de intrare al aplicației. Inițializează obiectul FastAPI și înregistrează toate "routerele" din directorul /routes.

models.py: Definește structura bazei de date folosind clase Python. Fiecare clasă corespunde unui tabel (ex: Order, Shipment), iar atributele clasei corespund coloanelor. Aici sunt definite și relațiile dintre tabele (ex: o Comandă are mai multe Livrări).

settings.py: Un sistem de configurare centralizat și puternic. Folosește Pydantic pentru a încărca automat variabilele din fișierul .env și toate fișierele de configurare din directorul /config, validându-le și expunându-le într-un singur obiect settings disponibil în toată aplicația.








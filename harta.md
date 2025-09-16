├── awb_archive/             # Director unde se arhivează PDF-urile AWB-urilor printate
├── config/                  # Fișiere de configurare JSON (chei API, mapări, etc.)
│   ├── courier_map.json
│   ├── courier_status_map.json
│   ├── dpd.json
│   ├── sameday.json
│   └── ...
├── routes/                  # API Layer: Definește toate paginile și endpoint-urile web
│   ├── orders.py            # Pagina principală de comenzi, filtre, sortare
│   ├── labels.py            # Endpoint-uri pentru generare AWB-uri individuale
│   ├── printing.py          # Logica pentru pagina "Print Hub"
│   ├── validation.py        # Pagina "Validation Hub" pentru corectarea adreselor
│   ├── settings.py          # Pagina principală de setări
│   ├── logs.py              # Pagina cu istoricul printărilor
│   └── sync.py              # Endpoint-uri pentru a porni sincronizările
├── services/                # Business Logic Layer: Conține "creierul" aplicației
│   ├── couriers/            # Arhitectură modulară pentru fiecare curier
│   │   ├── base.py          # Definește "contractul" (interfața) comună
│   │   ├── dpd.py           # Implementarea specifică pentru DPD
│   │   ├── sameday.py       # Implementarea specifică pentru Sameday
│   │   └── __init__.py      # "Fabrica" ce livrează serviciul de curierat corect
│   ├── address_service.py   # Motorul de validare și auto-corectare a adreselor
│   ├── courier_service.py   # Logica pentru tracking-ul AWB-urilor în loturi
│   ├── filter_service.py    # Construiește interogările SQL pentru filtrare și sortare
│   ├── label_service.py     # Logica pentru generarea PDF-urilor cu etichete
│   ├── print_service.py     # Logica de sortare avansată pentru "Print Hub"
│   ├── shopify_service.py   # Comunicarea cu API-ul Shopify
│   ├── sync_service.py      # Orchestrarea proceselor de sincronizare
│   └── utils.py             # Funcții ajutătoare, ex: calcularea statusului derivat
├── static/                  # Fișiere statice (CSS, JS)
│   └── js/main.js
├── templates/               # Fișiere HTML (Jinja2)
│   ├── index.html           # Pagina principală cu tabelul de comenzi
│   └── ...                  # (print_view.html, validation.html, etc.)
├── .env                     # Fișier de configurare pentru secrete (ex: user/parolă DB)
├── main.py                  # Punctul de intrare al aplicației FastAPI
├── models.py                # Definirea tabelelor bazei de date (SQLAlchemy)
├── database.py              # Configurația conexiunii la baza de date
├── settings.py              # Logica de încărcare a configurărilor din .env și /config
└── websocket_manager.py     # Manager pentru conexiunile WebSocket (trimite progresul sync)

# config_loader.py
import json
from pathlib import Path

CONFIG_PATH = Path(__file__).parent / "config"

class ConfigLoader:
    def __init__(self):
        self._configs = {}
        self._load_all_configs()

    def _load_all_configs(self):
        """Încarcă toate fișierele .json din directorul de configurare."""
        for config_file in CONFIG_PATH.glob("*.json"):
            try:
                with open(config_file, "r") as f:
                    self._configs[config_file.stem] = json.load(f)
            except (IOError, json.JSONDecodeError) as e:
                print(f"Eroare la încărcarea {config_file.name}: {e}")

    def get_config(self, name: str) -> dict:
        """Returnează configurarea pentru un nume dat (ex: 'dpd', 'sameday')."""
        return self._configs.get(name, {})

# Creăm o singură instanță pe care o vom importa în restul aplicației
config_loader = ConfigLoader()

# Pentru a putea accesa usor setările
def get_courier_settings(courier_name: str) -> dict:
    return config_loader.get_config(courier_name.lower())
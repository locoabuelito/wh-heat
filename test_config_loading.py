import json
import os

def load_config():
    """Carga la configuración desde config.json o usa valores por defecto."""
    defaults = {
        "concurrency": {"max_racks": 8, "max_connections": 800},
        "circuit_breaker": {"max_failures": 3, "cooldown_seconds": 300, "max_store_size": 5000},
        "subnets_range": [1, 111],
        "warehouses": {}
    }
    try:
        if os.path.exists("config.json"):
            print(f"Found config.json at {os.path.abspath('config.json')}")
            with open("config.json", "r") as f:
                user_config = json.load(f)
                defaults.update(user_config)
                print("Loaded config.json content")
        else:
            print("config.json NOT FOUND")
    except Exception as e:
        print(f"Error loading config: {e}")
    return defaults

CONFIG = load_config()
AUTH_USER = CONFIG.get("miner_auth", {}).get("username", "root")
AUTH_PASS = CONFIG.get("miner_auth", {}).get("password", "root")

print(f"AUTH_USER: '{AUTH_USER}'")
print(f"AUTH_PASS: '{AUTH_PASS}'")
print(f"Full Miner Auth Config: {CONFIG.get('miner_auth')}")

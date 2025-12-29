import asyncio  # Librería base para manejar concurrencia y tareas asíncronas.
import aiohttp  # Cliente HTTP asíncrono de alto rendimiento.
import json     # Para codificar y decodificar datos en formato JSON.
import time     # Para medir tiempos, calcular latencias y obtener timestamps.
import logging  # Para generar registros (logs) ordenados en la consola.
import re       # Expresiones regulares, usadas para limpiar respuestas JSON sucias.
import os       # Para verificar si existe el archivo config.json.
from contextlib import asynccontextmanager # Para gestionar el ciclo de vida de la App.
from typing import Dict, List, Any, Optional # Tipos de datos para mejorar la legibilidad.
from collections import OrderedDict # Diccionario que recuerda el orden (para caché LRU).

# --- IMPORTACIONES DE FASTAPI ---
from fastapi import FastAPI, Response, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

# --- 1. CONFIGURACIÓN DEL SISTEMA DE LOGS ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("MiningMonitor")

# --- 2. GESTIÓN DE CONFIGURACIÓN DINÁMICA ---
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
            with open("config.json", "r") as f:
                user_config = json.load(f)
                defaults.update(user_config)
                logger.info("✅ Configuración cargada desde config.json")
        else:
            logger.warning("⚠️ config.json no encontrado. Usando defaults.")
    except Exception as e:
        logger.error(f"❌ Error cargando config: {e}")
    return defaults

CONFIG = load_config()

# Constantes Globales
MAX_CONCURRENT_RACKS = CONFIG["concurrency"]["max_racks"]
MAX_HTTP_CONNECTIONS = CONFIG["concurrency"]["max_connections"]
BATCH_SIZE = CONFIG.get("batch_processing", {}).get("batch_size", 60)
BATCH_DELAY_MS = CONFIG.get("batch_processing", {}).get("delay_ms", 50)
CB_CONFIG = CONFIG["circuit_breaker"]
IP_TEMPLATE = "10.140.{}.251"

# --- 3. CONSTRUCCIÓN DEL MAPA DE WAREHOUSES ---
WAREHOUSE_CONFIG = {}

if "warehouses" in CONFIG and CONFIG["warehouses"]:
    for wh_name, wh_conf in CONFIG["warehouses"].items():
        try:
            wh_type = wh_conf.get("type", "antminer")
            racks_count = wh_conf.get("racks_count", 16)
            ip_pattern = wh_conf.get("base_ip_pattern", "10.142.{}.1")
            offset = wh_conf.get("offset_start", 0)
            
            racks_dict = {}
            for i in range(racks_count):
                subnet_num = offset + i
                base_ip = ip_pattern.format(subnet_num)
                racks_dict[f"Rack{i+1}"] = {
                    "base_ip": base_ip, "range": 180, 
                    "type": wh_type, "rack_number": i + 1
                }
            
            WAREHOUSE_CONFIG[wh_name] = {"type": wh_type, "racks": racks_dict}
            logger.info(f"🏗️  {wh_name} configurado: {racks_count} Racks.")
        except Exception as e:
            logger.error(f"Error config {wh_name}: {e}")
else:
    logger.warning("⚠️ No se detectaron Warehouses en config.json")

# --- 4. ESTADO GLOBAL EN MEMORIA ---
circuit_breaker_store = OrderedDict()

performance_metrics = {
    "requests_total": 0, "requests_failed": 0, "avg_latency_ms": 0.0,
    "active_racks_processing": 0, "start_time": time.time()
}

system_health = {
    "containers_last_beat": 0, "warehouses_last_beat": 0, "gc_last_beat": 0
}

cache_data = {
    "containers": {}, "warehouses": {}, "racks": {}, "miners": {}, "wh_miners": {}
}

# --- 5. FUNCIONES AUXILIARES ---
def create_empty_container(subnet_index):
    """Estructura vacía para evitar errores en UI."""
    ip = IP_TEMPLATE.format(subnet_index)
    return {
        'ip_address': ip, 'ip_sortable': "".join(p.zfill(3) for p in ip.split('.')),
        'online': False, 'miner_num': 0, 'total_expected': 0, 'miner_info': {}, 
        'total_hashrate_ph': 0.0, 'pwr_box1_kw': 0.0, 'pwr_box2_kw': 0.0,
        'antbox_internal_humidity': 0, 'supply_liquid_temp': 0, 
        'supply_liquid_pressure': 0, 'return_liquid_pressure': 0,
        'leakage_fault': False, 'temp_fault': False, 'liquid_level_low': False,
        'supply_liquid_pressure_high': False, 'supply_liquid_flow_low': False,
        'return_liquid_pressure_low': False, 'cooling_tower_liquid_level_low': False,
        'last_seen': 0, 'last_seen_str': '--:--:--'
    }

def init_cache():
    """Inicializa la memoria con datos vacíos."""
    start, end = CONFIG["subnets_range"]
    for s in range(start, end):
        ip = IP_TEMPLATE.format(s)
        cache_data["containers"][ip] = create_empty_container(s)
    
    for wh_name, wh_config in WAREHOUSE_CONFIG.items():
        cache_data["warehouses"][wh_name] = {
            "name": wh_name, "type": wh_config["type"], "total_racks": len(wh_config["racks"]), 
            "online_racks": 0, "total_miners": 0, "online_miners": 0,
            "total_hashrate_th": 0.0, "total_power_w": 0, "avg_temp": 0, "last_updated": 0
        }
        for rack_name, rack_config in wh_config["racks"].items():
            base_ip = rack_config["base_ip"]
            rack_key = f"{wh_name}_{rack_name}"
            cache_data["racks"][rack_key] = {
                "warehouse": wh_name, "name": rack_name, "rack_number": rack_config["rack_number"], 
                "base_ip": base_ip, "type": rack_config["type"], "total_miners": rack_config["range"],
                "online_miners": 0, "offline_miners": 0, "total_hashrate_th": 0.0, "total_power_w": 0,
                "avg_temp": 0, "last_updated": 0
            }
            cache_data["wh_miners"][rack_key] = []

def update_metrics(latency: float, success: bool):
    """Actualiza métricas de latencia."""
    performance_metrics["requests_total"] += 1
    if not success: performance_metrics["requests_failed"] += 1
    alpha = 0.01 
    curr = performance_metrics["avg_latency_ms"]
    performance_metrics["avg_latency_ms"] = (alpha * (latency * 1000)) + ((1 - alpha) * curr)

def extract_safe_json(text: str) -> Optional[Dict]:
    """Limpia JSONs sucios."""
    try:
        # El ? después del asterisco hace que se detenga en el primer cierre de llave }
        match = re.search(r'(\{.*?\})', text, re.DOTALL)
        if match:
            return json.loads(match.group(1))
    except Exception as e:
        logger.debug(f"Error parsing specific JSON: {e}")
    return None

# --- 6. CIRCUIT BREAKER ---
def enforce_cb_limit():
    if len(circuit_breaker_store) > CB_CONFIG["max_store_size"]:
        circuit_breaker_store.popitem(last=False)

def record_failure(ip: str):
    now = time.time()
    if ip not in circuit_breaker_store:
        circuit_breaker_store[ip] = {"failures": 0, "retry_at": 0, "last_seen": now}
        enforce_cb_limit()
    else:
        circuit_breaker_store.move_to_end(ip)
        circuit_breaker_store[ip]["last_seen"] = now
    
    state = circuit_breaker_store[ip]
    state["failures"] += 1
    
    if state["failures"] == CB_CONFIG["max_failures"]:
        state["retry_at"] = now + CB_CONFIG["cooldown_seconds"]
        logger.warning(f"🔌 Circuit OPEN: {ip} bloqueado.")
    elif state["failures"] > CB_CONFIG["max_failures"]:
        state["retry_at"] = now + CB_CONFIG["cooldown_seconds"]

def record_success(ip: str):
    if ip in circuit_breaker_store: del circuit_breaker_store[ip]

def is_circuit_open(ip: str) -> bool:
    if ip not in circuit_breaker_store: return False
    state = circuit_breaker_store[ip]
    now = time.time()
    
    if state["failures"] >= CB_CONFIG["max_failures"]:
        if now < state["retry_at"]: return True
        state["retry_at"] = now + CB_CONFIG["cooldown_seconds"]
        return False
    return False
# --- 7. DRIVERS DE CONEXIÓN (FETCHERS) ---

async def fetch_container_data(session: aiohttp.ClientSession, ip: str, node: Dict) -> Dict:
    """Lee datos del PLC."""
    start = time.time()
    node = node.copy()
    success = False
    try:
        async with session.get(f"http://{ip}/cooler?operation=coolerState", timeout=2.0) as rc:
            if rc.status == 200:
                data = await rc.json()
                node.update(data.get('params', {}))
                node['online'] = True
                node['pwr_box1_kw'] = round(float(node.get('distribution_box1_power', 0)) / 1000, 1)
                node['pwr_box2_kw'] = round(float(node.get('distribution_box2_power', 0)) / 1000, 1)
                success = True
            else: node['online'] = False

        if node['online']:
            async with session.get(f"http://{ip}/cooler?operation=minerInfo", timeout=2.0) as rm:
                if rm.status == 200:
                    m = (await rm.json()).get('params', {})
                    node['miner_num'] = int(m.get('miner_num', 0))
                    clean = {}
                    for k, v in m.get('miner_info', {}).items():
                        clean[k] = {'hashrate': float(v.get('GHS_5s', 0)), 'elapsed': int(v.get('elapsed', 0)), 'temp': int(v.get('pcb_max_temp', 0))}
                    node['miner_info'] = clean
                    node['total_expected'] = len(clean)
                    node['total_hashrate_ph'] = round(float(m.get('total_hashrate', 0)) / 1000, 2)
        
        node['last_seen'] = time.time()
        node['last_seen_str'] = time.strftime('%H:%M:%S')
    except Exception: node['online'] = False
    
    update_metrics(time.time() - start, success)
    return node

async def fetch_avalon_miner(session: aiohttp.ClientSession, ip: str, wh_name: str, rack_name: str) -> Dict:
    if is_circuit_open(ip): 
        return {'ip': ip, 'warehouse': wh_name, 'rack': rack_name, 'online': False, 'hashrate_th': 0, 'updated': time.time(), 'last_updated': 0}
    
    start = time.time()
    success = False
    # Estructura base del resultado
    result = {
        'ip': ip, 'warehouse': wh_name, 'rack': rack_name, 
        'online': False, 'hashrate_th': 0, 'power_w': 0, 
        'temp_chip': 0, 'updated': time.time(), 'last_updated': time.time()
    }
    
    try:
        auth = aiohttp.BasicAuth('root', 'root')
        async with session.get(f"http://{ip}/get_home.cgi", auth=auth, timeout=3.0) as r:
            if r.status == 200:
                data = extract_safe_json(await r.text())
                if data and 'av' in data:
                    record_success(ip)
                    val_temp = int(data.get('temperature', 0)) # <--- Capturamos el valor
                    result.update({
                        'online': True, 
                        'hashrate_th': float(data.get('av', 0)), 
                        'power_w': int(data.get('wall_power', 0)), 
                        'temp_chip': val_temp,      # Para colores de rack y tablas
                        'temp_ambient': val_temp,   # <--- AHORA YA NO SERÁ NULL
                        'model': 'Avalon', 
                        'last_updated': time.time()
                    })
                else: record_failure(ip)
            else: record_failure(ip)
    except Exception:
        record_failure(ip)
    
    update_metrics(time.time() - start, success)
    return result

async def fetch_antminer_stats(session: aiohttp.ClientSession, ip: str, wh_name: str, rack_name: str) -> Dict:
    if is_circuit_open(ip): 
        return {'ip': ip, 'warehouse': wh_name, 'rack': rack_name, 'online': False, 'hashrate_th': 0, 'updated': time.time(), 'last_updated': 0}
    
    start = time.time()
    success = False
    result = {'ip': ip, 'warehouse': wh_name, 'rack': rack_name, 'online': False, 'hashrate_th': 0, 'power_w': 0, 'temp_chip': 0, 'temp_ambient': 0, 'updated': time.time(), 'last_updated': time.time()}
    
    try:
        auth = aiohttp.BasicAuth('root', 'root')
        async with session.get(f"http://{ip}/cgi-bin/stats.cgi", auth=auth, timeout=5.0) as r:
            if r.status == 200:
                data = extract_safe_json(await r.text())
                if data and 'STATS' in data and len(data['STATS']) > 0:
                    s_obj = data['STATS'][0]
                    ambient = float(s_obj.get('ambient_temp', 0)) # <--- Valor del S21+
                    
                    record_success(ip)
                    result.update({
                        'online': True, 
                        'hashrate_th': round(float(s_obj.get('rate_5s', 0)) / 1000, 2),
                        'temp_chip': ambient, 
                        'temp_ambient': ambient, # <--- SE ASEGURA EL VALOR AQUÍ
                        'model': data.get('INFO', {}).get('type', 'Antminer S21+'),
                        'last_updated': time.time()
                    })
                else: record_failure(ip)
            else: record_failure(ip)
    except Exception as e:
        # Silenciamos el error en consola para no ensuciar el log, o lo dejamos como debug
        record_failure(ip)
    
    update_metrics(time.time() - start, success)
    return result

# --- 8. ORQUESTADORES ASÍNCRONOS ---

async def update_containers():
    connector = aiohttp.TCPConnector(limit=100, force_close=True)
    async with aiohttp.ClientSession(connector=connector) as session:
        while True:
            system_health["containers_last_beat"] = time.time()
            start_ip, end_ip = CONFIG["subnets_range"]
            targets = list(range(start_ip, end_ip))
            
            tasks = []
            for s in targets:
                ip = IP_TEMPLATE.format(s)
                if ip not in cache_data["containers"]:
                    cache_data["containers"][ip] = create_empty_container(s)
                node = cache_data["containers"][ip].copy()
                tasks.append(fetch_container_data(session, ip, node))
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for i, result in enumerate(results):
                if not isinstance(result, Exception):
                    s = targets[i]
                    ip = IP_TEMPLATE.format(s)
                    cache_data["containers"][ip] = result
            await asyncio.sleep(5)

async def process_rack_data(session, wh_name, rack_name, rack_config, semaphore):
    """
    Procesa un Rack usando Batch Processing para evitar saturación de red.
    Divide los 180 mineros en batches de 60, con pausas de 50ms entre batches.
    Esto mejora la precisión de ~70% a ~99% con solo ~1s adicional.
    """
    async with semaphore:
        performance_metrics["active_racks_processing"] += 1
        try:
            rack_key = f"{wh_name}_{rack_name}"
            ip_parts = rack_config["base_ip"].split('.')
            base_num = int(ip_parts[-1])
            
            rack_miners = []
            r_stats = {"online": 0, "offline": 0, "hashrate": 0.0, "power": 0, "temps": [], "last_updated": 0}
            
            # Batch Processing: Procesar en grupos para no saturar el switch
            total_miners = rack_config["range"]
            for batch_start in range(0, total_miners, BATCH_SIZE):
                batch_end = min(batch_start + BATCH_SIZE, total_miners)
                
                # Crear batch de tareas (ej: 60 mineros)
                batch_tasks = []
                for i in range(batch_start, batch_end):
                    ip = f"{'.'.join(ip_parts[:-1])}.{base_num + i}"
                    if rack_config["type"] == "avalon":
                        batch_tasks.append(fetch_avalon_miner(session, ip, wh_name, rack_name))
                    else:
                        batch_tasks.append(fetch_antminer_stats(session, ip, wh_name, rack_name))
                
                # Ejecutar batch completo en paralelo
                batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
                
                # Procesar resultados del batch
                for res in batch_results:
                    if not isinstance(res, Exception):
                        cache_data["miners"][res['ip']] = res
                        rack_miners.append(res)
                        if res['online']:
                            r_stats["online"] += 1
                            r_stats["hashrate"] += res['hashrate_th']
                            r_stats["power"] += res['power_w']
                            if res['temp_chip'] > 0: r_stats["temps"].append(res['temp_chip'])
                        else:
                            r_stats["offline"] += 1
                        if res['last_updated'] > r_stats["last_updated"]:
                            r_stats["last_updated"] = res['last_updated']
                
                # Pausa entre batches para dar tiempo al switch (excepto en el último batch)
                if batch_end < total_miners:
                    await asyncio.sleep(BATCH_DELAY_MS / 1000)
            
            # Actualizar cache del rack
            cache_data["wh_miners"][rack_key] = rack_miners
            avg_temp = round(sum(r_stats["temps"])/len(r_stats["temps"]), 1) if r_stats["temps"] else 0
            
            cache_data["racks"][rack_key].update({
                "online_miners": r_stats["online"], "offline_miners": r_stats["offline"],
                "total_hashrate_th": round(r_stats["hashrate"], 2), "total_power_w": r_stats["power"],
                "avg_temp": avg_temp, "last_updated": r_stats["last_updated"] or time.time()
            })
            return wh_name, r_stats
        finally:
            performance_metrics["active_racks_processing"] -= 1

async def update_warehouses():
    connector = aiohttp.TCPConnector(limit=MAX_HTTP_CONNECTIONS, force_close=True, ttl_dns_cache=300)
    rack_sem = asyncio.Semaphore(MAX_CONCURRENT_RACKS)
    
    async with aiohttp.ClientSession(connector=connector) as session:
        while True:
            system_health["warehouses_last_beat"] = time.time()
            logger.info("--- INICIO ESCANEO MASIVO ---")
            start_ts = time.time()
            
            all_rack_tasks = []
            wh_accumulator = { name: {"miners":0, "on":0, "hash":0.0, "pwr":0, "temps":[], "racks_on":0} for name in WAREHOUSE_CONFIG }

            for wh_name, wh_config in WAREHOUSE_CONFIG.items():
                for rack_name, rack_config in wh_config["racks"].items():
                    task = process_rack_data(session, wh_name, rack_name, rack_config, rack_sem)
                    all_rack_tasks.append(task)
            
            results = await asyncio.gather(*all_rack_tasks, return_exceptions=True)
            
            for res in results:
                if isinstance(res, Exception) or not res: continue
                wh_name, r_stats = res
                acc = wh_accumulator[wh_name]
                acc["miners"] += (r_stats["online"] + r_stats["offline"])
                acc["on"] += r_stats["online"]
                acc["hash"] += r_stats["hashrate"]
                acc["pwr"] += r_stats["power"]
                acc["temps"].extend(r_stats["temps"])
                if r_stats["online"] > 0: acc["racks_on"] += 1

            for wh_name, acc in wh_accumulator.items():
                avg_temp = round(sum(acc["temps"])/len(acc["temps"]), 1) if acc["temps"] else 0
                cache_data["warehouses"][wh_name].update({
                    "total_miners": acc["miners"], "online_miners": acc["on"],
                    "total_hashrate_th": round(acc["hash"], 2),
                    "total_power_w": acc["pwr"], "avg_temp": avg_temp,
                    "online_racks": acc["racks_on"], "last_updated": time.time()
                })
            
            elapsed = time.time() - start_ts
            logger.info(f"--- FIN ESCANEO: {elapsed:.2f}s ---")
            await asyncio.sleep(10)

async def prune_circuit_breaker():
    """Garbage Collector: Limpia IPs viejas."""
    while True:
        system_health["gc_last_beat"] = time.time()
        try:
            await asyncio.sleep(3600)
            now = time.time()
            keys_to_remove = [ip for ip, state in circuit_breaker_store.items() if (now - state.get("last_seen", 0)) > 86400]
            for ip in keys_to_remove:
                if ip in circuit_breaker_store: del circuit_breaker_store[ip]
            if keys_to_remove: logger.info(f"GC: Limpiadas {len(keys_to_remove)} entradas.")
        except Exception as e: logger.error(f"GC Error: {e}")
# --- 9. API FASTAPI Y LIFESPAN ---

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_cache()
    c_task = asyncio.create_task(update_containers())
    w_task = asyncio.create_task(update_warehouses())
    gc_task = asyncio.create_task(prune_circuit_breaker())
    yield
    c_task.cancel(); w_task.cancel(); gc_task.cancel()
    try: await asyncio.gather(c_task, w_task, gc_task, return_exceptions=True)
    except asyncio.CancelledError: pass

app = FastAPI(title="Mining Monitor", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
async def root():
    try:
        with open("templates/index.html", "r", encoding="utf-8") as f: return HTMLResponse(content=f.read())
    except Exception: return Response("Template not found", status_code=500)

@app.get("/api/health")
async def health_check():
    """Endpoint de Salud para monitoreo externo."""
    now = time.time()
    return {
        "status": "ok",
        "uptime": int(now - performance_metrics["start_time"]),
        "backpressure": {
            "active_racks": performance_metrics["active_racks_processing"],
            "max_allowed": MAX_CONCURRENT_RACKS
        },
        "tasks_lag": {
            "containers": int(now - system_health["containers_last_beat"]),
            "warehouses": int(now - system_health["warehouses_last_beat"])
        },
        "metrics": {
            "total_reqs": performance_metrics["requests_total"],
            "failed_reqs": performance_metrics["requests_failed"],
            "avg_latency": round(performance_metrics["avg_latency_ms"], 2),
            "cb_size": len(circuit_breaker_store)
        }
    }

@app.get("/api/containers")
async def get_containers(): return list(cache_data["containers"].values())

@app.get("/api/air")
async def get_air():
    data = []
    for wh_name, wh_conf in WAREHOUSE_CONFIG.items():
        for r_name, r_conf in wh_conf["racks"].items():
            r_key = f"{wh_name}_{r_name}"
            miners = cache_data["wh_miners"].get(r_key, [])
            for m in miners:
                data.append({
                    "wh": wh_name, 
                    "rack": r_conf["rack_number"],
                    "ip": m.get('ip'), 
                    "online": m.get('online'),
                    "hashrate": m.get('hashrate_th', 0), 
                    "temp_chip": m.get('temp_chip', 0),
                    "temp_ambient": m.get('temp_ambient', 0), # <--- .get(key, default)
                    "updated": m.get('updated', 0)
                })
    return data

@app.get("/api/stats")
async def get_stats():
    """Estadísticas globales."""
    total_c = len(cache_data["containers"])
    on_c = sum(1 for c in cache_data["containers"].values() if c.get('online'))
    
    wh_miners = 0; wh_on = 0; wh_hash = 0.0; wh_pwr = 0.0; online_wh = 0
    for wh in cache_data["warehouses"].values():
        wh_miners += wh["total_miners"]; wh_on += wh["online_miners"]
        wh_hash += wh["total_hashrate_th"]; wh_pwr += wh["total_power_w"]
        if wh["online_racks"] > 0: online_wh += 1
    
    c_hash = sum(c.get('total_hashrate_ph', 0) * 1000 for c in cache_data["containers"].values() if c.get('online'))
    
    # Cálculos seguros
    c_pct = round((on_c/total_c*100) if total_c else 0, 1)
    w_pct = round((online_wh/len(WAREHOUSE_CONFIG)*100) if WAREHOUSE_CONFIG else 0, 1)
    m_pct = round((wh_on/wh_miners*100) if wh_miners else 0, 1)
    
    return {
        "summary": {
            "total_containers": total_c, "online_containers": on_c, "containers_percentage": c_pct,
            "total_warehouses": len(WAREHOUSE_CONFIG), "online_warehouses": online_wh, "warehouses_percentage": w_pct,
            "total_miners": wh_miners, "online_miners": wh_on, "miners_percentage": m_pct,
            "total_hashrate_ph": round((wh_hash + c_hash)/1000, 2),
            "total_power_mw": round(wh_pwr/1000000, 2),
            "system_health": round((c_pct + w_pct + m_pct)/3, 1),
            "last_updated": time.time()
        }
    }

@app.get("/api/monitor/blocked")
async def get_blocked():
    """Endpoint diagnóstico: Mineros bloqueados por Circuit Breaker."""
    blocked = []
    now = time.time()
    for ip, state in list(circuit_breaker_store.items()):
        if state["failures"] >= CB_CONFIG["max_failures"]:
            rem = int(state["retry_at"] - now)
            blocked.append({
                "ip": ip, "status": "OPEN" if rem > 0 else "SEMI-OPEN",
                "failures": state["failures"], "cooldown_sec": max(0, rem)
            })
    blocked.sort(key=lambda x: x["cooldown_sec"])
    return {"count": len(blocked), "blocked": blocked}

if __name__ == "__main__":
    import uvicorn
    # Iniciamos el servidor en 0.0.0.0 para que sea accesible desde la red local
    uvicorn.run(app, host="0.0.0.0", port=5000, log_level="info")
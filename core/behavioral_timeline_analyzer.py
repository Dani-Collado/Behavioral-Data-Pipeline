import sqlite3
import re
import time
import random
import os
import math
import statistics

# Configuración de rutas
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)
DB_PATH = os.path.join(PROJECT_ROOT, "data", "pipeline.db")

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"

def generate_behavioral_timeline(dataset_id, nombre, target_processing_hours, browser):
    """
    Recibe el BROWSER de Playwright, abriendo y cerrando un CONTEXTO por cada página
    para evitar que los scripts residuales ahoguen el networkidle.
    """
    print(f"\n======================================")
    print(f"[*] Processing Timeline Generation for: {nombre} ({dataset_id})")
    print(f"[*] Target Processing Hours: {target_processing_hours}h")

    # ---------------------------------------------------------
    # 1. SAMPLE SELECTION
    # ---------------------------------------------------------
    target_processing_minutes = target_processing_hours * 60
    rango_min = target_processing_minutes * 0.7 
    rango_max = target_processing_minutes * 1.3 

    # Note: URL retains original source parameters for portfolio demonstration purposes
    url_leaderboard = f"https://steamhunters.com/apps/{dataset_id}/users?state=perfect"
    print(f" -> Scanning for valid user samples at: {url_leaderboard}")

    user_samples = []
    
    # AISLAMIENTO DE CONTEXTO PARA LA LEADERBOARD
    context_leader = browser.new_context(user_agent=USER_AGENT)
    page_leader = context_leader.new_page()
    page_leader.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

    try:
        page_leader.goto(url_leaderboard, wait_until="networkidle", timeout=60000)
        
        script_content = page_leader.evaluate("""
            () => {
                const scripts = Array.from(document.querySelectorAll('script'));
                const target = scripts.find(s => s.innerText.includes('var sh ='));
                return target ? target.innerText : null;
            }
        """)

        if not script_content:
            print("   [-] No matching state found on the sample providers page.")
            return False

        tiempos = list(re.finditer(r'"playtime":(\d+)', script_content))
        urls = list(re.finditer(r'"url":"([^"]+/achievements[^"]*)"', script_content))

        for i in range(min(len(tiempos), len(urls))):
            minutos_jugador = int(tiempos[i].group(1))
            url_jugador_parcial = urls[i].group(1)

            if rango_min <= minutos_jugador <= rango_max:
                url_completa = f"https://steamhunters.com{url_jugador_parcial}"
                url_completa = url_completa.split('?')[0]
                
                if url_completa not in user_samples:
                    user_samples.append(url_completa)

                if len(user_samples) == 5:
                    break

        if not user_samples:
            print(f"   [-] No user sample matches the range of {int(rango_min)}m to {int(rango_max)}m. Skipping.")
            return False

        print(f"   [+] Successfully extracted {len(user_samples)} valid user samples.")

    except Exception as e:
        print(f"   [!] Error selecting user samples: {e}")
        return False
    finally:
        context_leader.close()

    # ---------------------------------------------------------
    # 2. EVENT TIMELINE EXTRACTION
    # ---------------------------------------------------------
    event_timestamps = {}
    
    for idx, url in enumerate(user_samples, 1):
        print(f"   -> [Sample {idx}/{len(user_samples)}] Extracting active event timestamps...")
        time.sleep(random.uniform(3, 6))

        # AISLAMIENTO DE CONTEXTO POR CADA REQUEST
        context_player = browser.new_context(user_agent=USER_AGENT)
        page_player = context_player.new_page()
        page_player.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        try:
            page_player.goto(url, wait_until="networkidle", timeout=60000)
            
            script_content = page_player.evaluate("""
                () => {
                    const scripts = Array.from(document.querySelectorAll('script'));
                    const target = scripts.find(s => s.innerText.includes('var sh ='));
                    return target ? target.innerText : null;
                }
            """)

            if script_content:
                matches = re.finditer(r'"unlockDate":new Date\((\d+)\)[^}]+?"apiName":"([^"]+)"', script_content)
                encontrados = 0
                for match in matches:
                    timestamp_ms = int(match.group(1))
                    event_id = match.group(2)
                    
                    if event_id not in event_timestamps:
                        event_timestamps[event_id] = []
                    event_timestamps[event_id].append(timestamp_ms)
                    encontrados += 1
                
                print(f"      [+] {encontrados} events extracted from timeline.")
            else:
                print("      [-] State descriptor not found in sample profile.")

        except Exception as e:
            print(f"      [!] Error parsing timeline data: {e}")
        finally:
            context_player.close()

    # ---------------------------------------------------------
    # 3. MATHEMATICAL EVENT CONSENSUS AND NORMALIZATION
    # ---------------------------------------------------------
    if not event_timestamps:
        print("   [-] Failed to extract enough timeline data. Aborting generation.")
        return False

    print(f"   [⚙️] Calculating dynamic natural occurrence models for {len(event_timestamps)} unique events...")
    
    target_processing_total_mins = target_processing_hours * 60.0
    
    # 3.1. Calculate absolute median for each event based on user samples
    event_medians = []
    for event_id, timestamps in event_timestamps.items():
        if len(timestamps) > 0:
            mediana_ts = statistics.median(timestamps)
            event_medians.append({'event_id': event_id, 'mediana_ts': mediana_ts})

    # 3.2. Sequential Temporal Ordering
    sorted_events = sorted(event_medians, key=lambda x: x['mediana_ts'])

    # 3.3. Damping logic using Square Root limits
    event_weights = []
    timestamp_anterior = None

    for indice, evento in enumerate(sorted_events):
        if indice == 0:
            peso = 0.0 # Handled in final allocation step
        else:
            diferencia_ms = evento['mediana_ts'] - timestamp_anterior
            delta_bruto_mins = diferencia_ms / (1000 * 60)
            
            if delta_bruto_mins < 0:
                delta_bruto_mins = 0.0
                
            peso = math.sqrt(delta_bruto_mins)
            
        event_weights.append({'event_id': evento['event_id'], 'peso': peso})
        timestamp_anterior = evento['mediana_ts']

    # 3.4. Calculate proportional scalar factor 
    suma_pesos = sum(item['peso'] for item in event_weights)
    if suma_pesos == 0: suma_pesos = 1.0
    
    tiempo_restante_para_repartir = target_processing_total_mins * 0.985
    factor_escala = tiempo_restante_para_repartir / suma_pesos

    # 3.5. Final array structure generation and SQLite persistence
    final_timeline = []
    
    for indice, item in enumerate(event_weights, 1):
        if indice == 1:
            wait_minutes = target_processing_total_mins * 0.015
        else:
            wait_minutes = item['peso'] * factor_escala

        final_timeline.append({
            'event_id': item['event_id'],
            'orden': indice,
            'wait_minutes': round(wait_minutes, 2)
        })

    if final_timeline:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            for l in final_timeline:
                cursor.execute('''
                    UPDATE event_details 
                    SET orden = ?, wait_minutes = ? 
                    WHERE dataset_id = ? AND event_id = ?
                ''', (l['orden'], l['wait_minutes'], dataset_id, l['event_id']))
            conn.commit()
            
        print(f"   [💾] Organic pipeline timeline configured: {len(final_timeline)} events mapped!")
        return True
        
    return False

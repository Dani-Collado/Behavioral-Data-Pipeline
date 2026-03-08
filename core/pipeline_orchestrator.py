import os
import sys
import sqlite3
import time

from playwright.sync_api import sync_playwright

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)
DB_PATH = os.path.join(PROJECT_ROOT, "data", "pipeline.db")

from .database import PipelineDB
from .behavioral_timeline_analyzer import generate_behavioral_timeline

db = PipelineDB()
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"

# Placeholder function to simulate data enrichment scraping
def extract_processing_target(page, dataset_id):
    """
    Extracción simulada para el portfolio. 
    En un entorno real, esto interactuaría con la red para obtener los targets de processing_hours.
    """
    try:
        url = f"https://steamhunters.com/apps/{dataset_id}/achievements"
        response = page.goto(url, wait_until="networkidle", timeout=30000)
        
        if response and response.status == 404:
            return -1.0
            
        script_content = page.evaluate("""
            () => {
                const scripts = Array.from(document.querySelectorAll('script'));
                const target = scripts.find(s => s.innerText.includes('var sh ='));
                return target ? target.innerText : null;
            }
        """)
        if script_content:
            import re
            match = re.search(r'"medianCompletionTime"\s*:\s*([0-9.]+)', script_content)
            if match:
                return float(match.group(1))
    except Exception:
        pass
    return None

def update_dataset_target_time(dataset_id, target_hours):
    """Auxiliar: Actualiza target_processing_hours de un dataset en la BD."""
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("UPDATE datasets SET target_processing_hours = ? WHERE dataset_id = ?", (target_hours, dataset_id))
        conn.commit()


def execute_pipeline(target_dataset_id=None):
    """
    Revisa la integridad de los datos de los datasets. 
    Ejecuta validaciones, sincronizaciones web y normalización cronológica SÓLO si faltan metadata.
    """
    print("\n[PIPELINE ORCHESTRATOR] Initializing Data Pipeline validation sequence...")
    
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        if target_dataset_id:
            cursor.execute("SELECT * FROM datasets WHERE dataset_id = ?", (target_dataset_id,))
        else:
            # We filter for datasets that actually have configured events
            cursor.execute("SELECT * FROM datasets WHERE total_events > 0")
            
        datasets_to_review = [dict(fila) for fila in cursor.fetchall()]

    if not datasets_to_review:
        print("[PIPELINE ORCHESTRATOR] No valid datasets available for orchestration.")
        return

    # Táctico lazy loading: Instanciamos Chromium en Headless solo si alguna fase lo requiere.
    playwright_cm = None
    p = None
    browser = None

    def get_browser():
        nonlocal playwright_cm, p, browser
        if not browser:
            print("  [⚙️] Initializing headless Playwright engine...")
            playwright_cm = sync_playwright()
            p = playwright_cm.__enter__()
            browser = p.chromium.launch(headless=True)
        return browser

    for indice, dataset in enumerate(datasets_to_review, 1):
        dataset_id = dataset['dataset_id']
        nombre = dataset['nombre']
        total_staged_events = dataset['total_events']
        current_target_time = dataset['target_processing_hours']
        
        print(f"\n========================================================")
        print(f"[{indice}/{len(datasets_to_review)}] Evaluating Integrity: {nombre} ({dataset_id})")
        
        # -----------------------------------------------------------------
        # STEP 1: Schema Validation (Event Models)
        # -----------------------------------------------------------------
        with sqlite3.connect(DB_PATH) as conn:
            c = conn.cursor()
            c.execute("SELECT count(*) FROM event_details WHERE dataset_id = ?", (dataset_id,))
            event_count = c.fetchone()[0]

        if event_count == 0 or (event_count < total_staged_events and total_staged_events > 0):
            print(f"  -> [Step 1 | SCHEMA] Metadata mapping required ({event_count}/{total_staged_events}). Syncing with upstream APIs...")
            # En el portfolio, esto requeriría sync_full_library si quisieras la integracion real
            print("      [+] Simulated schema ingestion successful for portfolio demonstration.")
        else:
            print(f"  -> [Step 1 | SCHEMA] Event models fully mapped ({event_count}/{total_staged_events}), skipping...")

        # -----------------------------------------------------------------
        # STEP 2: Enrichment (Target Process Validation)
        # -----------------------------------------------------------------
        if current_target_time is None or current_target_time <= 0.0:
            print(f"  -> [Step 2 | ENRICHMENT] Target processing metric absent. Executing dynamic scraping...")
            
            b = get_browser()
            context = b.new_context(user_agent=USER_AGENT)
            page = context.new_page()
            page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            new_target_metric = extract_processing_target(page, dataset_id)
            
            if new_target_metric is not None and new_target_metric > 0:
                update_dataset_target_time(dataset_id, new_target_metric)
                current_target_time = new_target_metric  
                
            context.close()
            time.sleep(2)
        else:
            print(f"  -> [Step 2 | ENRICHMENT] Target metric confirmed ({current_target_time}h), skipping...")

        # -----------------------------------------------------------------
        # STEP 3: Timeline Sequence Generation (Behavioral Normalization)
        # -----------------------------------------------------------------
        if current_target_time > 0:
            with sqlite3.connect(DB_PATH) as conn:
                c = conn.cursor()
                c.execute("SELECT count(*) FROM event_details WHERE dataset_id = ? AND orden = 0", (dataset_id,))
                unsequenced_events = c.fetchone()[0]
                
            if unsequenced_events > 0:
                print(f"  -> [Step 3 | TIMELINE] {unsequenced_events} unsequenced events detected. Launching timeline analyzer...")
                
                b = get_browser()
                # Pasamos la instancia de Playwright al sub-módulo para aprovechar su eficiencia
                success = generate_behavioral_timeline(dataset_id, nombre, current_target_time, b)
                
                if success:
                    time.sleep(3)
            else:
                print(f"  -> [Step 3 | TIMELINE] Chronological sequences generated successfully, skipping...")
        else:
            print(f"  -> [Step 3 | TIMELINE] Target estimation absent. Unable to normalize mathematical models. Skipped.")


    if browser:
        print("\n[🔌] Shutting down headless engines gracefully...")
        browser.close()
        
    if playwright_cm:
        playwright_cm.__exit__(None, None, None)

    print("\n[+] Data Pipeline orchestration cycle completed successfully.")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        try:
            target_id = int(sys.argv[1])
            execute_pipeline(target_id)
        except ValueError:
            print("Parameter must be a numeric dataset ID.")
    else:
        execute_pipeline()

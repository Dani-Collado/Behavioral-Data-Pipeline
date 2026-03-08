import os
import sys
import math
from tabulate import tabulate

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

from core.database import PipelineDB

db = PipelineDB()

def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

def print_header():
    print("="*60)
    print(f"{'BEHAVIORAL DATA PIPELINE DASHBOARD':^60}")
    print("="*60)

def menu_ver_resumen():
    clear_screen()
    print_header()
    resumen = db.obtener_resumen_pipeline()
    
    if resumen['total_datasets'] == 0:
        print("\n[!] No active datasets found in the pipeline.")
    else:
        porcentaje = 0
        if resumen['eventos_globales'] > 0:
            porcentaje = (resumen['eventos_procesados'] / resumen['eventos_globales']) * 100
        
        datos_tabla = [
            ["Total Datasets Tracked:", resumen['total_datasets']],
            ["Datasets with Valid Events:", resumen['datasets_con_eventos']],
            ["Datasets Fully Processed (100%):", resumen['fully_processed']],
            ["Datasets Pending Processing:", resumen['pendientes']],
            ["Total Global Events Processed:", f"{resumen['eventos_procesados']} / {resumen['eventos_globales']}"],
            ["Pipeline Completion Ratio:", f"{porcentaje:.2f}%"],
            ["Total Active Hours (Input):", f"{resumen.get('active_hours_totales', 0):.1f}h"],
            ["Target Processing Backlog:", f"{resumen.get('tiempo_pendiente', 0):.1f}h"]
        ]
        
        print("\n")
        print(tabulate(datos_tabla, tablefmt="fancy_grid"))

    input("\nPress Enter to return to main menu...")

def menu_ver_datasets_pendientes():
    clear_screen()
    print_header()
    pendientes = db.listar_datasets_para_procesar()
    
    if not pendientes:
        print("\n[+] All registered datasets are fully processed.")
    else:
        tabla = []
        pendientes.sort(key=lambda x: (x['processed_events'] / x['total_events']) if x['total_events'] else 0, reverse=True)
        
        for d in pendientes:
            dataset_id = d['dataset_id']
            nombre = d['nombre']
            proc = d['processed_events']
            tot = d['total_events']
            faltan = tot - proc
            porc = (proc / tot) * 100 if tot > 0 else 0
            
            tiempo = d.get('target_processing_hours', 0.0)
            if tiempo > 0:
                tiempo_str = f"{tiempo:.1f}h"
            elif tiempo == -1:
                tiempo_str = "N/A (No Data)"
            else:
                tiempo_str = "Pending Analysis"
                
            active_hours = d.get('active_hours', 0.0)
            suma_esperas_mins = d.get('suma_espera_mins', 0.0)
            
            if suma_esperas_mins > 0:
                horas_esp = suma_esperas_mins / 60.0
                str_suma_esperas = f"{horas_esp:.1f}h"
            else:
                str_suma_esperas = "-"
                
            tabla.append([dataset_id, nombre[:25], f"{proc}/{tot}", f"{porc:.1f}%", faltan, f"{active_hours:.1f}h", str_suma_esperas, tiempo_str])
            
        print("\n")
        print(tabulate(tabla, headers=["Dataset ID", "Project Name", "Progress", "%", "Pending", "Active Hrs.", "Event Sum", "Target Est."], tablefmt="presto"))
        
    input("\nPress Enter to return...")

def menu_ver_datasets_completados():
    clear_screen()
    print_header()
    completados = db.obtener_datasets_fully_processed()
    
    if not completados:
        print("\n[!] No datasets have reached 100% completion yet.")
    else:
        tabla = []
        for d in completados:
            active_hours = d.get('active_hours', 0.0)
            tabla.append([d['dataset_id'], d['nombre'], f"{active_hours:.1f}h", "✔️ FULLY PROCESSED"])
            
        print("\n")
        print(tabulate(tabla, headers=["Dataset ID", "Project Name", "Active Hours", "Pipeline Status"], tablefmt="simple"))
        
    input("\nPress Enter to return...")

def menu_detalles_dataset():
    clear_screen()
    print_header()
    
    dataset_str = input("\nEnter Dataset ID to inspect: ").strip()
    if not dataset_str:
        return
        
    try:
        dataset_id = int(dataset_str)
    except ValueError:
        print("\n[!] Error: Dataset ID must be strictly numeric.")
        input("Press Enter...")
        return
        
    dataset = db.obtener_dataset(dataset_id)
    if not dataset:
        print(f"\n[!] Error: Dataset {dataset_id} not found in database.")
        input("Press Enter...")
        return
        
    print(f"\n>> {dataset['nombre']} (ID: {dataset_id})")
    print(f">> Base Extraction Progress: {dataset['processed_events']} / {dataset['total_events']} Events")
    
    active_hours = dataset.get('active_hours', 0.0)
    tiempo_est = dataset.get('target_processing_hours', 0.0)
    str_tiempo_est = f"{tiempo_est:.1f}h" if tiempo_est > 0 else "N/A"
    print(f">> Accumulated Active Hours: {active_hours:.1f}h | Final Target: {str_tiempo_est}")
    
    estado_auto = "ENABLED" if dataset.get('automation', 1) else "DISABLED (Manual)"
    print(f">> Pipeline Automation: {estado_auto}")
    
    eventos_pendientes = db.obtener_eventos_pendientes(dataset_id)
    if not eventos_pendientes:
        print("\n[+] Zero pending events: All data mapped correctly.")
    else:
        print(f"\n=== PROCESSING TIMELINE: ({len(eventos_pendientes)}) PENDING EVENTS ===")
        
        eventos_pendientes.sort(key=lambda x: x.get('orden', 0) if x.get('orden', 0) > 0 else 9999)
        
        tabla = []
        for e in eventos_pendientes:
            id_tec = e.get('event_id', 'N/A')
            nombre_vis = e.get('display_name', 'Hidden')
            orden = e.get('orden', 0)
            espera = e.get('wait_minutes', 0.0)
            
            if len(nombre_vis) > 35:
                 nombre_vis = nombre_vis[:32] + "..."
                 
            str_orden = f"#{orden}" if orden > 0 else "N/A"
            str_espera = f"{espera:.1f} min" if orden > 0 else "-"
            
            tabla.append([str_orden, str_espera, id_tec, nombre_vis])
            
        print(tabulate(tabla, headers=["Timeline Order", "Processing Delay", "Event ID", "Display Name"], tablefmt="grid"))
        
    input("\nPress Enter to return to main menu...")

def bucle_principal():
    while True:
        clear_screen()
        print_header()
        print("Select an operation:\n")
        print("  [1] Pipeline Execution Summary (Metrics)")
        print("  [2] Datasets Pending Processing (WIP)")
        print("  [3] Fully Processed Datasets (100% Completed)")
        print("  [4] Specific Dataset Timeline Inspector")
        print("  [0] Exit CLI")
        
        opcion = input("\nSelection > ").strip()
        
        if opcion == '1':
            menu_ver_resumen()
        elif opcion == '2':
            menu_ver_datasets_pendientes()
        elif opcion == '3':
            menu_ver_datasets_completados()
        elif opcion == '4':
            menu_detalles_dataset()
        elif opcion == '0':
            print("\n[+] Exiting Dashboard. Processing terminated.\n")
            break
        else:
            print("\n[!] Invalid Option. Try again.")
            input("Press Enter to continue...")

if __name__ == "__main__":
    try:
        bucle_principal()
    except KeyboardInterrupt:
        print("\n\n[+] Forced exit signal caught. Shutting down...\n")
        sys.exit(0)

import os
import sqlite3
from datetime import datetime

# Rutas absolutas para un uso seguro en Docker
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)
DB_PATH = os.path.join(PROJECT_ROOT, "data", "pipeline.db")
SCHEMA_PATH = os.path.join(PROJECT_ROOT, "data", "schema.sql")

class PipelineDB:
    def __init__(self):
        """Inicializa la clase y asegura que la base de datos existe con el esquema actual."""
        self.inicializar_bd()
        
    def inicializar_bd(self):
        """Crea las tablas leyendo el archivo schema.sql."""
        if not os.path.exists(SCHEMA_PATH):
            print(f"Advertencia: No se encuentra el archivo de esquema {SCHEMA_PATH}. Asegúrate de crearlo.")
            return
            
        with open(SCHEMA_PATH, 'r', encoding='utf-8') as f:
            esquema_sql = f.read()
            
        with sqlite3.connect(DB_PATH) as conexion:
            cursor = conexion.cursor()
            cursor.executescript(esquema_sql)
            conexion.commit()

    def guardar_o_actualizar_dataset(self, datos):
        """
        Inserta o reemplaza la información del dataset en la base de datos principal.
        
        Args:
            datos (dict): Contiene 'dataset_id', 'nombre', 'origen', 'total_events' y 'processed_events'.
        """
        ahora = datetime.now().isoformat()
        with sqlite3.connect(DB_PATH) as conexion:
            cursor = conexion.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO datasets (dataset_id, nombre, origen, total_events, processed_events, active_hours, target_processing_hours, automation, last_session, last_update)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                datos['dataset_id'],
                datos['nombre'],
                datos['origen'],
                datos.get('total_events', 0),
                datos.get('processed_events', 0),
                datos.get('active_hours', 0.0),
                datos.get('target_processing_hours', 0.0),
                datos.get('automation', 1),
                datos.get('last_session', ahora),
                datos.get('last_update', ahora)
            ))
            conexion.commit()

    def guardar_event_detail(self, datos):
        """
        Inserta un evento específico en la tabla secundaria event_details utilizando
        INSERT OR IGNORE para no sobreescribir el campo de 'processed' si ya existe el registro.
        
        Args:
            datos (dict): Contiene 'event_id', 'dataset_id', 'display_name', 'descripcion', 'processed'.
        """
        with sqlite3.connect(DB_PATH) as conexion:
            cursor = conexion.cursor()
            cursor.execute('''
                INSERT OR IGNORE INTO event_details (event_id, dataset_id, display_name, descripcion, processed)
                VALUES (?, ?, ?, ?, ?)
            ''', (
                datos['event_id'],
                datos['dataset_id'],
                datos.get('display_name', ''),
                datos.get('descripcion', ''),
                datos.get('processed', False)
            ))
            conexion.commit()

    def obtener_dataset(self, dataset_id):
        """Consulta los datos de un dataset específico por su ID."""
        with sqlite3.connect(DB_PATH) as conexion:
            conexion.row_factory = sqlite3.Row 
            cursor = conexion.cursor()
            cursor.execute('SELECT * FROM datasets WHERE dataset_id = ?', (dataset_id,))
            fila = cursor.fetchone()
            
            if fila:
                return dict(fila)
            return None
            
    def obtener_event_id(self, dataset_id, display_name):
        """
        Busca el identificador técnico interno (event_id) de un evento basándose
        en el ID del dataset y el nombre en lenguaje humano.
        """
        with sqlite3.connect(DB_PATH) as conexion:
            cursor = conexion.cursor()
            cursor.execute('''
                SELECT event_id FROM event_details 
                WHERE dataset_id = ? AND display_name = ?
            ''', (dataset_id, display_name))
            resultado = cursor.fetchone()
            
            if resultado:
                return resultado[0]
            return None

    def actualizar_estado_evento(self, dataset_id, event_id, processed):
        """Actualiza el estado de procesamiento de un evento específico."""
        with sqlite3.connect(DB_PATH) as conexion:
            cursor = conexion.cursor()
            cursor.execute('''
                UPDATE event_details 
                SET processed = ? 
                WHERE dataset_id = ? AND event_id = ?
            ''', (processed, dataset_id, event_id))
            conexion.commit()

    def actualizar_progreso_dataset(self, dataset_id, processed_events_count):
        """Calcula el progreso e inserta en la base maestra."""
        ahora = datetime.now().isoformat()
        with sqlite3.connect(DB_PATH) as conexion:
            cursor = conexion.cursor()
            cursor.execute('''
                UPDATE datasets 
                SET processed_events = ?, last_session = ?
                WHERE dataset_id = ?
            ''', (processed_events_count, ahora, dataset_id))
            conexion.commit()

    def obtener_eventos_pendientes(self, dataset_id):
        """Devuelve una lista de eventos pendientes, incluyendo su orden matemático y tiempo de espera."""
        eventos = []
        with sqlite3.connect(DB_PATH) as conexion:
            conexion.row_factory = sqlite3.Row
            cursor = conexion.cursor()
            
            cursor.execute('''
                SELECT event_id, display_name, descripcion, orden, wait_minutes 
                FROM event_details 
                WHERE dataset_id = ? AND processed = 0
                ORDER BY 
                    CASE WHEN orden > 0 THEN 0 ELSE 1 END,
                    orden ASC
            ''', (dataset_id,))
            
            for fila in cursor.fetchall():
                eventos.append(dict(fila))
                
        return eventos

    def listar_datasets_para_procesar(self):
        """Retorna la lista de datasets donde aún quedan eventos por procesar, incluyendo la suma de wait_minutes."""
        datasets = []
        with sqlite3.connect(DB_PATH) as conexion:
            conexion.row_factory = sqlite3.Row
            cursor = conexion.cursor()
            cursor.execute('''
                SELECT d.*, IFNULL(SUM(ed.wait_minutes), 0) as suma_espera_mins
                FROM datasets d
                LEFT JOIN event_details ed ON d.dataset_id = ed.dataset_id AND ed.processed = 0
                WHERE d.processed_events < d.total_events AND d.total_events > 0
                GROUP BY d.dataset_id
            ''')
            
            for fila in cursor.fetchall():
                datasets.append(dict(fila))
                
        return datasets

    def obtener_datasets_fully_processed(self):
        """Retorna la lista de datasets que han sido completados al 100%."""
        datasets = []
        with sqlite3.connect(DB_PATH) as conexion:
            conexion.row_factory = sqlite3.Row
            cursor = conexion.cursor()
            cursor.execute('''
                SELECT * FROM datasets 
                WHERE processed_events >= total_events AND total_events > 0
                ORDER BY nombre ASC
            ''')
            
            for fila in cursor.fetchall():
                datasets.append(dict(fila))
                
        return datasets

    def obtener_resumen_pipeline(self):
        """Devuelve un diccionario con las métricas clave del pipeline."""
        with sqlite3.connect(DB_PATH) as conexion:
            cursor = conexion.cursor()
            
            cursor.execute('SELECT COUNT(*) FROM datasets')
            total_datasets = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(*) FROM datasets WHERE total_events > 0')
            con_eventos = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(*) FROM datasets WHERE processed_events >= total_events AND total_events > 0')
            completados = cursor.fetchone()[0]
            
            cursor.execute('SELECT SUM(total_events), SUM(processed_events) FROM datasets WHERE total_events > 0')
            sumas = cursor.fetchone()
            eventos_totales = sumas[0] if sumas[0] else 0
            eventos_procesados = sumas[1] if sumas[1] else 0
            
            cursor.execute('''SELECT SUM(target_processing_hours) FROM datasets 
                              WHERE processed_events < total_events 
                              AND total_events > 0 
                              AND target_processing_hours > 0''')
            suma_tiempo = cursor.fetchone()[0]
            tiempo_pendiente = suma_tiempo if suma_tiempo else 0.0

            cursor.execute('SELECT SUM(active_hours) FROM datasets')
            suma_horas = cursor.fetchone()[0]
            active_hours_totales = suma_horas if suma_horas else 0.0
            
            return {
                "total_datasets": total_datasets,
                "datasets_con_eventos": con_eventos,
                "fully_processed": completados,
                "pendientes": con_eventos - completados,
                "eventos_globales": eventos_totales,
                "eventos_procesados": eventos_procesados,
                "tiempo_pendiente": tiempo_pendiente,
                "active_hours_totales": active_hours_totales
            }

    def obtener_todos_los_dataset_ids(self):
        """Devuelve un set con todos los IDs almacenados en la base de datos."""
        with sqlite3.connect(DB_PATH) as conexion:
            cursor = conexion.cursor()
            cursor.execute('SELECT dataset_id FROM datasets')
            return {fila[0] for fila in cursor.fetchall()}

    def actualizar_orden_evento(self, dataset_id, event_id, nuevo_orden):
        """Hace UPDATE en la tabla de eventos para establecer la prioridad."""
        with sqlite3.connect(DB_PATH) as conexion:
            cursor = conexion.cursor()
            cursor.execute('''
                UPDATE event_details 
                SET orden = ? 
                WHERE dataset_id = ? AND event_id = ?
            ''', (nuevo_orden, dataset_id, event_id))
            conexion.commit()

    def actualizar_total_eventos_dataset(self, dataset_id, nuevo_total):
        """Hace un UPDATE ligero de total_events sin hacer REPLACE, para no sobreescribir campos extras."""
        with sqlite3.connect(DB_PATH) as conexion:
            cursor = conexion.cursor()
            cursor.execute('''
                UPDATE datasets 
                SET total_events = ? 
                WHERE dataset_id = ?
            ''', (nuevo_total, dataset_id))
            conexion.commit()

    def obtener_datasets_sin_timeline(self):
        """Retorna datasets que tienen target_processing_hours pero sus eventos no tienen orden asignado."""
        datasets = []
        with sqlite3.connect(DB_PATH) as conexion:
            conexion.row_factory = sqlite3.Row
            cursor = conexion.cursor()
            cursor.execute('''
                SELECT DISTINCT d.dataset_id, d.nombre, d.target_processing_hours 
                FROM datasets d
                JOIN event_details ed ON d.dataset_id = ed.dataset_id
                WHERE d.target_processing_hours > 0 AND ed.orden = 0
            ''')
            for fila in cursor.fetchall():
                datasets.append(dict(fila))
        return datasets

    def guardar_timeline_eventos(self, dataset_id, timeline_eventos):
        """
        Recibe una lista de diccionarios: [{'event_id': '...', 'orden': 1, 'wait_minutes': 15.5}, ...]
        y actualiza la BBDD.
        """
        with sqlite3.connect(DB_PATH) as conexion:
            cursor = conexion.cursor()
            for evento in timeline_eventos:
                cursor.execute('''
                    UPDATE event_details 
                    SET orden = ?, wait_minutes = ?
                    WHERE dataset_id = ? AND event_id = ?
                ''', (evento['orden'], evento['wait_minutes'], dataset_id, evento['event_id']))
            conexion.commit()

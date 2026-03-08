-- Tabla principal de Datasets (Proyectos/Embudos analizados)
CREATE TABLE IF NOT EXISTS datasets (
    dataset_id INTEGER PRIMARY KEY,
    nombre TEXT NOT NULL,
    origen TEXT NOT NULL,
    total_events INTEGER DEFAULT 0,
    processed_events INTEGER DEFAULT 0,
    active_hours REAL DEFAULT 0.0,           
    target_processing_hours REAL DEFAULT 0.0,  
    automation BOOLEAN DEFAULT 1,             
    last_session DATETIME,
    last_update DATETIME
);

-- Tabla de Eventos detallados (Hitos/Timelines)
CREATE TABLE IF NOT EXISTS event_details (
    event_id TEXT NOT NULL,
    dataset_id INTEGER NOT NULL,
    display_name TEXT,
    descripcion TEXT,
    processed BOOLEAN DEFAULT 0,
    orden INTEGER DEFAULT 0,                  
    wait_minutes REAL DEFAULT 0.0,          
    PRIMARY KEY (event_id, dataset_id),
    FOREIGN KEY (dataset_id) REFERENCES datasets(dataset_id) ON DELETE CASCADE
);
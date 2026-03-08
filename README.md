# Behavioral Data Orchestration Pipeline 🚀

Este proyecto implementa una arquitectura de microservicios diseñada para **extraer, limpiar y normalizar** grandes volúmenes de datos de comportamiento de usuarios (timelines/eventos) desde plataformas web protegidas y Single Page Applications (SPAs).

## 🧠 Retos Técnicos y Soluciones

### 1. Extracción en SPAs protegidas (Anti-Bot Evasion)
Las plataformas actuales bloquean el scraping tradicional o el abuso de automatización. 
* **Solución:** En lugar de parsear el DOM (frágil y lento), el motor utiliza **Playwright** instanciando *Contextos de Navegador Aislados* (`browser.new_context()`) por cada petición. A través de inyecciones JavaScript estratégicas, capturamos el estado interno de la aplicación (objetos JSON en memoria) evadiendo bloqueos de red tipo Cloudflare y evitando la contaminación cruzada de *cookies* o *caché*.

### 2. Data Science: Normalización de Anomalías (Square Root Dampening)
Al analizar timelines de usuarios reales, la varianza es extrema (ej. pausas de inactividad de meses entre dos eventos seguidos). Usar medias aritméticas destruía la estadística.
* **Solución:** El motor matemático (`behavioral_timeline_analyzer.py`) primero calcula la **Mediana Absoluta** de la base de usuarios para cada evento. Posteriormente, aplica un algoritmo de **Amortiguación por Raíz Cuadrada ($\sqrt{\Delta t}$)** a los deltas de tiempo. Esto permite comprimir los *outliers* extremos manteniendo intacta la varianza orgánica y distribuyendo el tiempo objetivo sin repeticiones artificiales (Capped Weighting natural).

### 3. Orquestación Perezosa (Lazy Evaluation)
* **Solución:** Un `pipeline_orchestrator` evalúa el estado de la base de datos transaccional SQLite en caliente. Aplica evaluación perezosa para levantar los microservicios y el motor Chromium **únicamente** si detecta que una fila específica necesita enriquecimiento de datos o carece de validación matemática, minimizando el uso de CPU y red.

## 🛠️ Stack Tecnológico
* **Python 3.10+** (Core Logic & Math)
* **Playwright** (Headless Browser & JS Injection)
* **SQLite3** (Persistencia y motor transaccional)
* **uv (Astral)** (Gestor de dependencias ultrarrápido en Rust)

## 🚀 Quickstart & Demo (Mock Data)

Para no comprometer datos reales de usuarios ni exponer credenciales de APIs en este repositorio público, se incluye un generador de **Mock Data**. Puedes probar el *CLI Dashboard* interactivo localmente.

1. **Clona el repositorio e inicializa el entorno con `uv`:**
   ```bash
   git clone [https://github.com/TU-USUARIO/Behavioral-Data-Pipeline.git](https://github.com/TU-USUARIO/Behavioral-Data-Pipeline.git)
   cd Behavioral-Data-Pipeline
   uv sync
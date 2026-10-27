# SismoWatch - Plataforma Global de Monitoreo Sísmico en Tiempo Real
> **Ruta de IA y Automatización &bull; SuperApp**  
> Orquestación con Apache Airflow, Transformación Medallón con Apache Spark, Calidad con Pandera, Gobernanza con OpenMetadata y Consumo Seguro en FastAPI + Web.

[![Architecture: Medallion](https://img.shields.io/badge/Architecture-Medallion%20(Bronze%20%7C%20Silver%20%7C%20Gold)-blue.svg)](#arquitectura-medallón)
[![Orchestration: Airflow](https://img.shields.io/badge/Orchestrator-Apache%20Airflow%202.8.2-017CEE.svg)](#orquestación-con-apache-airflow)
[![Engine: Spark](https://img.shields.io/badge/Distributed%20Engine-Apache%20Spark%203.5.0-E25A1C.svg)](#motor-distribuido-apache-spark)
[![Governance: OpenMetadata](https://img.shields.io/badge/Governance-OpenMetadata%20Ready-7C3AED.svg)](#gobernanza-con-openmetadata)
[![Security: RBAC + Least Privilege](https://img.shields.io/badge/Security-RBAC%20%7C%20Least%20Privilege-emerald.svg)](#seguridad-y-mínimo-privilegio)

---

## 1. Puesta en Marcha Rápida (Un Solo Comando)

Todo el ecosistema (PostgreSQL, Airflow Webserver, Airflow Scheduler, Spark Master, Spark Worker, Backend FastAPI, Web Frontend y Gobernanza) levanta automáticamente con un único comando:

### Paso 1: Configurar Variables de Entorno
Copie el archivo de plantilla a `.env` (las contraseñas y llaves ya vienen preconfiguradas y seguras para desarrollo):
```bash
cp .env.example .env
```

### Paso 2: Levantar el Proyecto con Docker Compose
```bash
docker compose up -d
```

### Paso 3: Acceder a los Servicios
Una vez levantados los contenedores, los servicios quedan inmediatamente disponibles en:

| Servicio | URL Local | Credenciales por Defecto |
| :--- | :--- | :--- |
| **Plataforma Web (Frontend)** | [http://localhost:3000](http://localhost:3000) | **Admin:** `admin` / `AdminSismo2026!`<br>**Viewer:** `viewer` / `ViewerSismo2026!` |
| **Backend API (Swagger Docs)** | [http://localhost:8000/docs](http://localhost:8000/docs) | Autenticación vía JWT (`POST /auth/login`) |
| **Apache Airflow UI** | [http://localhost:8080](http://localhost:8080) | `admin` / `SismoAdmin2026!` |
| **Apache Spark Master UI** | [http://localhost:8081](http://localhost:8081) | Monitoreo del Clúster Spark |
| **PostgreSQL Database** | `localhost:5432` | DB: `sismowatch`, User: `sismo_root` |

---

## 2. Propósito y Arquitectura Medallón

### 2.1 El Problema que Resuelve
Hasta hoy, la información sísmica del Servicio Geológico de Estados Unidos (USGS) solo existía como un feed público efímero en formato GeoJSON (`all_hour.geojson`) que se sobrescribe cada minuto sin guardar historial, sin validar su calidad y sin consolidar resúmenes para la toma de decisiones.

**SismoWatch** transforma este feed público en un producto de datos empresarial:
1. Extrae y persiste automáticamente cada evento sin duplicados (cargas idempotentes).
2. Limpia, normaliza y categoriza mediante arquitectura Medallón.
3. Detecta anomalías y enjambres sísmicos con algoritmos de Machine Learning.
4. Genera reportes ejecutivos diarios con IA generativa con **cero alucinación numérica**.
5. Dispone de un dashboard interactivo en tiempo casi real con streaming continuo (Server-Sent Events) y mapa global geolocalizado.

### 2.2 Arquitectura Medallón: Sin Landing y Sin Cuarentena
Conforme a la especificación del desafío, la arquitectura Medallón se implementa de manera ágil y directa:
- **Sin Carpeta de Landing:** El feed de USGS se ingesta directamente en la base de datos relacional dentro del esquema `bronze.earthquakes_raw`. Se conserva el JSON original íntegro junto con metadatos de extracción, evitando latencias de lectura/escritura en disco.
- **Sin Tablas de Cuarentena:** Las no conformidades son interceptadas por una compuerta de calidad bloqueante con **Pandera**. En lugar de acumular registros corruptos en tablas de cuarentena, las no conformidades se auditan detalladamente en la tabla `gold.quality_metrics` (reglas evaluadas, aprobadas, rechazadas, motivos y score porcentual). Si el score es inferior al 90%, la publicación a `Gold` es bloqueada.

```
USGS API (Pública)
       │
       ▼ [Cada 5 min - Airflow Ingest DAG - role_ingest]
┌──────────────────────────────────────────────┐
│  CAPA BRONZE (bronze.earthquakes_raw)         │
│  - Payload crudo JSONB                       │
│  - Metadatos de extracción y timestamp       │
└──────────────────────┬───────────────────────┘
                       │
                       ▼ [Airflow Transform DAG - Spark Engine]
┌──────────────────────────────────────────────┐
│  CAPA SILVER (silver.earthquakes_cleaned)    │
│  - Desanidado de latitud, longitud, prof.    │
│  - Conversión de epoch ms a Timestamp UTC    │
│  - Sanitización estricta anti Prompt-Inject  │
│  - Categorización de magnitud y profundidad  │
└──────────────────────┬───────────────────────┘
                       │
                       ▼ [Data Quality Gate - Pandera (Score >= 90%)]
                       ├──────────────────────────────────┐
                       │ (Registros válidos)              │ (Auditoría métricas)
                       ▼                                  ▼
┌──────────────────────────────────────────────┐ ┌──────────────────────────────────────────────┐
│  CAPA GOLD (gold.fact_earthquakes)           │ │  AUDITORÍA (gold.quality_metrics)            │
│  - Hechos listos para analítica              │ │  - Total evaluados, aprobados, rechazados    │
│  - Detección ML de anomalías y enjambres     │ │  - Score de calidad y lag de frescura        │
│  - gold.daily_metrics (Agregados diarios)    │ └──────────────────────────────────────────────┘
│  - gold.ai_reports (Resumen ejecutivo IA)    │
└──────────────────────┬───────────────────────┘
                       │
                       ▼ [platform_user - Mínimo Privilegio: Solo SELECT en Gold]
┌──────────────────────────────────────────────┐
│  FASTAPI BACKEND & WEB DASHBOARD             │
│  - KPIs, Gráficos, Mapa Leaflet, SSE En Vivo │
└──────────────────────────────────────────────┘
```

Para una explicación exhaustiva con diagramas Mermaid y trade-offs técnicos, consulte [docs/ARQUITECTURA.md](docs/ARQUITECTURA.md).

---

## 3. Orquestación con Apache Airflow

Se implementaron 4 DAGs modulares con buenas prácticas: uso de `TaskGroup`, `tags`, `doc_md`, control de concurrencia (`max_active_runs=1`), tiempos límite (`execution_timeout`), reintentos exponenciales y alertas mediante `on_failure_callback`.

| DAG ID | Frecuencia | Descripción y Responsabilidad |
| :--- | :--- | :--- |
| **`ingest_earthquakes_dag`** | `*/5 * * * *` (Cada 5 min) | Consulta el feed de USGS en tiempo real. Realiza un `upsert` idempotente en `bronze.earthquakes_raw` bajo el rol `role_ingest`. Al terminar, dispara automáticamente la transformación. |
| **`transform_quality_dag`** | Disparado por Ingesta | Ejecuta la transformación Bronze &rarr; Silver, corre la compuerta de calidad con Pandera y publica en `gold.fact_earthquakes` junto con la detección de anomalías y agregados diarios. |
| **`daily_ai_report_dag`** | `5 0 * * *` (Diario a las 00:05 UTC) | Extrae las cifras exactas del día anterior de Gold, ejecuta el generador de reportes con IA, audita tokens/costos y almacena el resumen en `gold.ai_reports`. |
| **`backfill_earthquakes_dag`** | Manual / Vía API Admin | Carga histórica parametrizada (por defecto 30 días, magnitud $\ge 2.5$) desde el endpoint de consulta de USGS (`/fdsnws/event/1/query`). Idempotente. |

---

## 4. Calidad de Datos (Data Quality Gate)

- **Herramienta Elegida:** **Pandera** combinada con verificaciones estadísticas avanzadas.
- **Justificación:** Pandera permite definir contratos de datos tipados sobre DataFrames con alto rendimiento, validaciones bloqueantes y documentación transparente de reglas.
- **Reglas Implementadas:**
  1. *Esquema y Tipos:* Tipos estrictos para ID (string), Magnitud (float), Coordenadas (float) y Fecha (datetime).
  2. *Campos Obligatorios:* No nulidad en `id`, `event_time`, `longitude`, `latitude`, `depth_km` y `place`.
  3. *Rangos Válidos:*
     - Latitud: $[-90.0, 90.0]$
     - Longitud: $[-180.0, 180.0]$
     - Magnitud: $[-2.0, 10.0]$
     - Profundidad: $[-5.0, 800.0]\text{ km}$
  4. *Unicidad de ID:* Detección y eliminación de duplicados.
  5. *Coherencia Temporal:* Validación de que la fecha del evento no sea futura.
  6. *Frescura de Datos:* Alerta si el desfase del evento más reciente supera el umbral configurado (120 min).
  7. *Compuerta Bloqueante:* Si el score es inferior al 90%, el flujo se interrumpe y los datos no se publican en `Gold`.

**Visualización de Resultados:**  
Los resultados de calidad se consultan en tiempo real en la pestaña **Pipeline & Calidad** del frontend y en el endpoint `GET /api/pipeline/status`.

---

## 5. Seguridad y Mínimo Privilegio

### 5.1 Roles de Base de Datos (Mínimo Privilegio)
Se diseñaron 3 roles exclusivos en PostgreSQL:
- **`role_ingest`:** Solo tiene permisos de `INSERT` y `UPDATE` sobre el esquema `bronze`. No tiene acceso alguno a `silver` ni `gold`.
- **`role_transform`:** Acceso de lectura en `bronze` y lectura/escritura en `silver` y `gold`. Utilizado por Spark y Airflow.
- **`platform_user`:** Acceso de **solo lectura (`SELECT`)** exclusivamente sobre el esquema `gold`. No puede ver ni consultar tablas de Bronze o Silver.

### 5.2 Seguridad de la Plataforma y RBAC
- **Autenticación con JWT:** Algoritmo HS256 con tiempo de expiración y firma criptográfica.
- **Control de Acceso por Roles:**
  - `viewer`: Solo puede ver el dashboard, consultar eventos, gráficos, mapa, exportar datos y ver el stream en vivo.
  - `admin`: Puede ejecutar backfills históricos, regenerar reportes de IA y ver la auditoría técnica de no conformidades. Si un `viewer` intenta ejecutar un endpoint de admin, recibe `403 Forbidden`. Sin token se recibe `401 Unauthorized`.
- **Sanitización de Datos Externos:** La descripción geográfica (`place`) se sanitiza removiendo etiquetas HTML/XSS y patrones de inyección a LLMs (ej: `ignore previous instructions`, `system prompt:`) antes de ser procesada o enviada a un modelo.

---

## 6. Automatización con Inteligencia Artificial

### 6.1 Reporte Ejecutivo con IA Generativa
- **Proveedor:** Google Gemini (compatible con OpenAI) y **motor de contingencia determinista (Fallback)** integrado.
- **Control Antialucinación:** Las cifras oficiales (total de eventos, magnitud máxima, ubicación del evento pico, promedio, sismos con tsunami y conteo de anomalías) se calculan formalmente en SQL/Python. El LLM solo redacta la narrativa.
- **Validación Matemática:** Una función inspectora audita el texto generado y verifica que ninguna cifra redactada contradiga las métricas oficiales calculadas.
- **Plan Alterno (Fallback):** Si no hay API key de LLM o el servicio externo excede el tiempo límite, se activa el motor analítico determinista garantizando que el pipeline nunca se detenga.
- **Auditoría:** Cada ejecución de IA registra modelo, tokens de entrada, tokens de salida, latencia en milisegundos y costo estimado en `gold.ai_execution_logs`.
- **Prompts Versionados:** Ubicados en `airflow/include/prompts/executive_report_v1.txt`.

### 6.2 Detección de Anomalías (Machine Learning)
Se implementaron dos técnicas analíticas complementarias:
1. **Isolation Forest:** Modelo multivariado sobre magnitud, profundidad y significancia para identificar eventos sísmicos con comportamiento atípico.
2. **Detección Espacio-Temporal de Enjambres Sísmicos (Swarms):** Algoritmo basado en distancias ortodrómicas (Haversine) que detecta agrupamientos inusuales de $\ge 3$ sismos en un radio $< 60\text{ km}$ y un intervalo $< 4\text{ horas}$. Cada anomalía genera una explicación clara registrada en `gold.anomaly_alerts`.

---

## 7. Plataforma Web y Tiempo Real (SSE)

- **Tecnología:** Frontend reactivo servido por Nginx y conectado al backend FastAPI.
- **Tiempo Real:** Implementado mediante **Server-Sent Events (SSE)** en el endpoint `/api/stream`. Se eligió SSE sobre WebSocket por su ligereza nativa sobre HTTP, reconexión automática incorporada en los navegadores y menor consumo de memoria.
- **Indicador "EN VIVO":** Pulsación visual continua en verde con hora de la última actualización y aviso si la conexión entra en reconexión.
- **Mapa Interactivo:** Mapa global con Leaflet con marcadores dinámicos codificados por color según magnitud.
- **Exportación:** Soporte para descarga en formato CSV y reporte imprimible en HTML/PDF.
- **Diseño Resiliente:** Manejo de estados de carga, vacío y error para evitar pantallas en blanco.

---

## 8. Gobernanza y Linaje con OpenMetadata

- **Catálogo de Metadatos:** Configuración formal de ingestión en `openmetadata/postgres_ingestion.yaml` para perfilado continuo de esquemas Medallón.
- **Linaje de Punta a Punta:** Script de registro `openmetadata/register_metadata.py` que traza el flujo completo:  
  `USGS API` &rarr; `Airflow Ingest DAG` &rarr; `bronze.earthquakes_raw` &rarr; `Airflow Transform DAG (Spark)` &rarr; `silver.earthquakes_cleaned` &rarr; `Data Quality Gate` &rarr; `gold.fact_earthquakes` &rarr; `FastAPI` &rarr; `Web Dashboard`.
- Para más información, consulte [docs/OPENMETADATA.md](docs/OPENMETADATA.md).

---

## 9. Pruebas Automatizadas

La suite de pruebas con **pytest** verifica los 4 pilares obligatorios de la solución:
1. Transformación de GeoJSON crudo a fila limpia de Silver.
2. Integridad de importación de todos los DAGs de Airflow.
3. Reglas de calidad de datos con Pandera (casos conformes y rechazos de rangos).
4. Seguridad de endpoints de la API, roles RBAC (401 sin token, 403 para Viewer en rutas de Admin).
5. Detección de anomalías, enjambres y validación numérica de reportes de IA.

Para correr las pruebas:
```bash
pytest airflow/tests/ -v
```

---

## 10. Evidencia del Escaneo de Seguridad

Se realizó el análisis de seguridad de código y dependencias con **Bandit** y verificación de secretos:
- Cero credenciales o secretos embebidos en el código fuente.
- Manejo estricto de `.env` ignorado en `.gitignore`.
- Sanitización de consultas SQL mediante parámetros tipados (`%s`) previniendo inyección SQL.
- Sanitización de texto libre contra ataques XSS y Prompt Injection.

---

## 11. Estructura del Repositorio

```
.
├── docker-compose.yml              # Orquestación de todos los servicios
├── .env.example                    # Plantilla de variables de entorno
├── .gitignore                      # Protección contra subida de secretos
├── README.md                       # Documentación principal
├── docs/
│   ├── ARQUITECTURA.md             # Justificación y decisiones técnicas
│   └── OPENMETADATA.md             # Guía de linaje y gobernanza
├── airflow/
│   ├── Dockerfile                  # Imagen optimizada de Airflow
│   ├── requirements.txt            # Dependencias del pipeline
│   ├── dags/                       # DAGs de Airflow
│   │   ├── ingest_earthquakes_dag.py
│   │   ├── transform_quality_dag.py
│   │   ├── daily_ai_report_dag.py
│   │   └── backfill_dag.py
│   ├── include/                    # Módulos reutilizables
│   │   ├── config.py
│   │   ├── db.py
│   │   ├── extractors.py
│   │   ├── spark_transformations.py
│   │   ├── quality.py
│   │   ├── anomaly_detection.py
│   │   ├── ai_client.py
│   │   └── prompts/
│   │       └── executive_report_v1.txt
│   └── tests/                      # Suite completa con pytest
├── db/
│   └── init/                       # Scripts de inicialización PostgreSQL
│       ├── 01_schemas_and_roles.sql
│       ├── 02_bronze_tables.sql
│       ├── 03_silver_tables.sql
│       └── 04_gold_tables.sql
├── spark/
│   └── jobs/
│       └── process_medallion.py    # Script PySpark para procesamiento distribuido
├── openmetadata/
│   ├── postgres_ingestion.yaml     # Perfilador de OpenMetadata
│   └── register_metadata.py        # Generador de catálogo y linaje
├── api/
│   ├── Dockerfile                  # Contenedor FastAPI
│   ├── requirements.txt
│   └── app/                        # Backend REST y SSE
└── web/
    ├── Dockerfile                  # Contenedor Nginx
    ├── nginx.conf
    └── src/                        # Frontend UI (Leaflet, Chart.js, Tailwind)
```

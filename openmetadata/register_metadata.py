"""
Script de Integración y Linaje para OpenMetadata - SismoWatch.
Genera el catálogo de metadatos, descripciones de columnas, tags de gobernanza
y define el linaje completo de datos de la arquitectura Medallón.
"""
import os
import json
import logging
from typing import Dict, Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("OpenMetadataRegistry")

# Definición del Linaje y Catálogo de Gobernanza
METADATA_CATALOG = {
    "service": "sismowatch_data_platform",
    "schemas": [
        {
            "name": "bronze",
            "description": "Capa cruda: Almacena eventos GeoJSON exactamente como se reciben de USGS.",
            "tables": [
                {
                    "name": "earthquakes_raw",
                    "description": "Payload y propiedades crudas del feed USGS sin transformaciones.",
                    "tags": ["Raw", "Bronze", "Ingestion"],
                    "owner": "DataOps Team"
                }
            ]
        },
        {
            "name": "silver",
            "description": "Capa intermedia: Datos limpios, tipados, coordenadas desanidadas y texto sanitizado.",
            "tables": [
                {
                    "name": "earthquakes_cleaned",
                    "description": "Eventos sísmicos tipados y sanitizados contra inyecciones y XSS.",
                    "tags": ["Cleaned", "Silver", "Sanitized"],
                    "owner": "Data Engineers"
                }
            ]
        },
        {
            "name": "gold",
            "description": "Capa curada: Marts de negocio, hechos analíticos, reportes ejecutivos de IA y anomalías.",
            "tables": [
                {
                    "name": "fact_earthquakes",
                    "description": "Tabla de hechos curada para consumo de la API, dashboard y analítica.",
                    "tags": ["Curated", "Gold", "BusinessReady"],
                    "owner": "BI & Analytics"
                },
                {
                    "name": "daily_metrics",
                    "description": "Métricas agregadas diarias por fecha y región.",
                    "tags": ["Gold", "Aggregated"],
                    "owner": "BI & Analytics"
                },
                {
                    "name": "quality_metrics",
                    "description": "Auditoría de ejecuciones de compuerta de calidad de datos.",
                    "tags": ["Governance", "Quality", "Audit"],
                    "owner": "Data Governance"
                },
                {
                    "name": "ai_reports",
                    "description": "Reportes diarios redactados por IA con validación matemática estricta.",
                    "tags": ["AI", "GenAI", "Executive"],
                    "owner": "Risk Operations"
                },
                {
                    "name": "anomaly_alerts",
                    "description": "Alertas de anomalías sísmicas y enjambres detectados por algoritmos ML.",
                    "tags": ["ML", "Alerts", "RealTime"],
                    "owner": "Risk Operations"
                }
            ]
        }
    ],
    "lineage_edges": [
        {
            "fromEntity": "source.usgs_earthquake_api",
            "toEntity": "pipeline.airflow.ingest_earthquakes_dag",
            "description": "Extracción periódica HTTP cada 5 minutos"
        },
        {
            "fromEntity": "pipeline.airflow.ingest_earthquakes_dag",
            "toEntity": "table.bronze.earthquakes_raw",
            "description": "Upsert idempotente con role_ingest"
        },
        {
            "fromEntity": "table.bronze.earthquakes_raw",
            "toEntity": "pipeline.airflow.transform_quality_dag",
            "description": "Lectura para limpieza y transformación"
        },
        {
            "fromEntity": "pipeline.airflow.transform_quality_dag",
            "toEntity": "table.silver.earthquakes_cleaned",
            "description": "Escritura de datos tipados y sanitizados"
        },
        {
            "fromEntity": "table.silver.earthquakes_cleaned",
            "toEntity": "gate.data_quality_pandera",
            "description": "Compuerta de calidad bloqueante (Score >= 90%)"
        },
        {
            "fromEntity": "gate.data_quality_pandera",
            "toEntity": "table.gold.quality_metrics",
            "description": "Auditoría de métricas de calidad"
        },
        {
            "fromEntity": "table.silver.earthquakes_cleaned",
            "toEntity": "model.ml_isolation_forest_swarms",
            "description": "Detección de anomalías y enjambres sísmicos"
        },
        {
            "fromEntity": "model.ml_isolation_forest_swarms",
            "toEntity": "table.gold.anomaly_alerts",
            "description": "Registro de alertas explicadas"
        },
        {
            "fromEntity": "model.ml_isolation_forest_swarms",
            "toEntity": "table.gold.fact_earthquakes",
            "description": "Publicación curada a hechos de negocio"
        },
        {
            "fromEntity": "table.gold.fact_earthquakes",
            "toEntity": "table.gold.daily_metrics",
            "description": "Agregación analítica diaria"
        },
        {
            "fromEntity": "table.gold.fact_earthquakes",
            "toEntity": "pipeline.airflow.daily_ai_report_dag",
            "description": "Extracción de cifras exactas para informe IA"
        },
        {
            "fromEntity": "pipeline.airflow.daily_ai_report_dag",
            "toEntity": "table.gold.ai_reports",
            "description": "Publicación de reporte ejecutivo validado"
        },
        {
            "fromEntity": "table.gold.fact_earthquakes",
            "toEntity": "api.fastapi_backend",
            "description": "Consumo exclusivo de capa Gold mediante platform_user"
        },
        {
            "fromEntity": "api.fastapi_backend",
            "toEntity": "ui.sismowatch_dashboard",
            "description": "Visualización en mapa, gráficos y streaming SSE"
        }
    ]
}

def export_governance_metadata(output_file: str = "openmetadata/lineage_definition.json"):
    """Exporta el catálogo y linaje de gobernanza en formato compatible con OpenMetadata."""
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(METADATA_CATALOG, f, indent=2, ensure_ascii=False)
    logger.info(f"Linaje y catálogo de metadatos exportados exitosamente a {output_file}")

if __name__ == "__main__":
    export_governance_metadata()

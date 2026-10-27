"""
Módulo de Detección de Anomalías Sísmicas (Machine Learning & Analítica Espacio-Temporal).
Combina Isolation Forest para valores multivariados atípicos y detección de enjambres sísmicos (clusters espaciales y temporales).
Registra alertas explicadas en gold.anomaly_alerts y etiqueta gold.fact_earthquakes.
"""
import logging
import math
from typing import List, Dict, Any, Tuple
import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from airflow.include.db import execute_batch_upsert

logger = logging.getLogger(__name__)

def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calcula la distancia ortodrómica en kilómetros entre dos coordenadas geográficas."""
    R = 6371.0 # Radio de la Tierra en km
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2.0)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2.0)**2
    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
    return R * c

def detect_anomalies_and_swarms(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Analiza un DataFrame de eventos sísmicos y detecta:
      1. Outliers estadísticos con Isolation Forest (magnitud, profundidad, significancia).
      2. Enjambres sísmicos (agrupamiento de >= 3 sismos en < 60 km y < 4 horas).
      3. Eventos mayores o con alerta de tsunami.
    Retorna el DataFrame enriquecido con columnas (is_anomaly, anomaly_score, anomaly_reason)
    y la lista de alertas generadas para insertar en gold.anomaly_alerts.
    """
    if df.empty:
        df["is_anomaly"] = False
        df["anomaly_score"] = 0.0
        df["anomaly_reason"] = ""
        return df, []

    enriched_df = df.copy()
    enriched_df["is_anomaly"] = False
    enriched_df["anomaly_score"] = 0.0
    enriched_df["anomaly_reason"] = ""

    alerts: List[Dict[str, Any]] = []

    # 1. Detección con Isolation Forest si hay suficientes muestras
    if len(enriched_df) >= 10:
        try:
            features = enriched_df[["magnitude", "depth_km", "significance"]].fillna(0)
            iso = IsolationForest(contamination=0.08, random_state=42)
            preds = iso.fit_predict(features)
            scores = -iso.score_samples(features) # mayor valor = mayor anomalía

            for idx, (pred, score) in enumerate(zip(preds, scores)):
                if pred == -1: # Anomalía detectada
                    row = enriched_df.iloc[idx]
                    mag = float(row["magnitude"])
                    depth = float(row["depth_km"])
                    reason = f"Anomalía estadística multivariada (Score: {score:.2f}): Magnitud {mag:.1f} a {depth:.1f} km de profundidad."
                    enriched_df.iloc[idx, enriched_df.columns.get_loc("is_anomaly")] = True
                    enriched_df.iloc[idx, enriched_df.columns.get_loc("anomaly_score")] = float(score)
                    enriched_df.iloc[idx, enriched_df.columns.get_loc("anomaly_reason")] = reason
                    
                    alerts.append({
                        "event_id": str(row["id"]),
                        "algorithm": "IsolationForest",
                        "anomaly_score": float(score),
                        "severity": "HIGH" if mag >= 5.5 else "MEDIUM",
                        "explanation": reason
                    })
        except Exception as exc:
            logger.warning(f"Error en IsolationForest: {exc}. Continuando con reglas heurísticas.")

    # 2. Regla analítica para eventos mayores y tsunamis
    for idx, row in enriched_df.iterrows():
        mag = float(row.get("magnitude", 0.0))
        tsunami = bool(row.get("tsunami_alert", False))
        event_id = str(row["id"])

        if mag >= 6.0 or tsunami:
            existing_reason = str(row.get("anomaly_reason") or "")
            tag = "ALERTA MAYOR: "
            if tsunami:
                tag += "Amenaza de Tsunami declarada. "
            if mag >= 6.0:
                tag += f"Sismo de magnitud destructiva {mag:.1f}."
            
            combined_reason = f"{tag} {existing_reason}".strip()
            enriched_df.at[idx, "is_anomaly"] = True
            enriched_df.at[idx, "anomaly_score"] = max(float(row.get("anomaly_score") or 0.0), 0.95)
            enriched_df.at[idx, "anomaly_reason"] = combined_reason

            alerts.append({
                "event_id": event_id,
                "algorithm": "HeuristicThreshold",
                "anomaly_score": 0.95,
                "severity": "CRITICAL" if mag >= 7.0 else "HIGH",
                "explanation": combined_reason
            })

    # 3. Detección de Enjambres Sísmicos (Swarms)
    # Ordenar por fecha para análisis temporal
    if "event_time" in enriched_df.columns and len(enriched_df) >= 3:
        sorted_df = enriched_df.sort_values(by="event_time").reset_index(drop=True)
        n = len(sorted_df)
        visited = set()

        for i in range(n):
            if i in visited:
                continue
            t_i = pd.to_datetime(sorted_df.iloc[i]["event_time"])
            lat_i = float(sorted_df.iloc[i]["latitude"])
            lon_i = float(sorted_df.iloc[i]["longitude"])

            cluster = [i]
            for j in range(i + 1, n):
                t_j = pd.to_datetime(sorted_df.iloc[j]["event_time"])
                # Ventana de tiempo menor a 4 horas
                diff_hours = (t_j - t_i).total_seconds() / 3600.0
                if diff_hours > 4.0:
                    break
                
                lat_j = float(sorted_df.iloc[j]["latitude"])
                lon_j = float(sorted_df.iloc[j]["longitude"])
                dist_km = haversine_distance(lat_i, lon_i, lat_j, lon_j)

                if dist_km <= 60.0:
                    cluster.append(j)

            if len(cluster) >= 3:
                visited.update(cluster)
                swarm_ids = [str(sorted_df.iloc[c]["id"]) for c in cluster]
                avg_mag = float(sorted_df.iloc[cluster]["magnitude"].mean())
                region_name = str(sorted_df.iloc[i].get("region", "Área no determinada"))
                swarm_explanation = (
                    f"Enjambre sísmico detectado en {region_name}: {len(cluster)} sismos "
                    f"en un radio de 60 km en menos de 4h (Magnitud prom: {avg_mag:.1f})."
                )

                for c in cluster:
                    eid = str(sorted_df.iloc[c]["id"])
                    # Actualizar en enriched_df
                    match_idx = enriched_df[enriched_df["id"] == eid].index
                    if not match_idx.empty:
                        enriched_df.loc[match_idx, "is_anomaly"] = True
                        enriched_df.loc[match_idx, "anomaly_score"] = 0.85
                        prev_reason = enriched_df.loc[match_idx, "anomaly_reason"].values[0]
                        enriched_df.loc[match_idx, "anomaly_reason"] = f"{swarm_explanation} {prev_reason}".strip()

                    alerts.append({
                        "event_id": eid,
                        "algorithm": "SpatioTemporalSwarmDBSCAN",
                        "anomaly_score": 0.85,
                        "severity": "MEDIUM",
                        "explanation": swarm_explanation
                    })

    logger.info(f"Detección de anomalías completada: {enriched_df['is_anomaly'].sum()} eventos anómalos detectados, {len(alerts)} alertas generadas.")
    return enriched_df, alerts

def save_anomaly_alerts(alerts: List[Dict[str, Any]]) -> int:
    """Registra las alertas de anomalías en gold.anomaly_alerts de forma idempotente."""
    if not alerts:
        return 0

    values = [
        (a["event_id"], a["algorithm"], a["anomaly_score"], a["severity"], a["explanation"])
        for a in alerts
    ]
    query = """
        INSERT INTO gold.anomaly_alerts (event_id, algorithm, anomaly_score, severity, explanation)
        VALUES %s
        ON CONFLICT DO NOTHING;
    """
    try:
        count = execute_batch_upsert(query, values, role="transform")
        logger.info(f"{count} alertas de anomalías registradas en gold.anomaly_alerts.")
        return count
    except Exception as exc:
        logger.error(f"Error al guardar alertas de anomalías: {exc}")
        return 0

"""
Cliente de Inteligencia Artificial para Generación de Reportes Narrativos.
Implementa:
  - Cómputo de métricas exactas en Python/SQL (el LLM no calcula cifras).
  - Validación numérica estricta entre el texto redactado y los datos calculados.
  - Esquema estructurado Pydantic.
  - Plan de contingencia automático (Fallback) determinista ante fallos o timeouts del LLM.
  - Auditoría de llamadas en gold.ai_execution_logs y publicación en gold.ai_reports.
"""
import os
import json
import time
import logging
import re
from datetime import date, datetime, timedelta, timezone
from typing import Dict, Any, List, Optional, Tuple
from pydantic import BaseModel, Field, ValidationError
from airflow.include.config import settings
from airflow.include.db import execute_query

logger = logging.getLogger(__name__)

class AIReportSchema(BaseModel):
    title: str = Field(..., description="Título del reporte ejecutivo")
    executive_summary: str = Field(..., description="Resumen narrativo en español")
    key_findings: List[str] = Field(..., description="Lista de hallazgos clave")
    risk_assessment: str = Field(..., description="Nivel de riesgo global evaluado")
    operational_recommendations: List[str] = Field(..., description="Recomendaciones para operaciones")

def get_calculated_metrics(target_date: Optional[date] = None) -> Dict[str, Any]:
    """
    Calcula mediante SQL en la capa Gold las cifras exactas y verificadas para el reporte.
    Garantiza que la verdad matemática provenga de la base de datos, no del LLM.
    """
    ref_date = target_date or (datetime.now(timezone.utc) - timedelta(days=1)).date()
    
    # Consulta de métricas del día en gold.fact_earthquakes
    query = """
        SELECT
            COUNT(*) AS total_events,
            COALESCE(MAX(magnitude), 0.0) AS max_magnitude,
            COALESCE(ROUND(AVG(magnitude), 2), 0.0) AS avg_magnitude,
            COALESCE(SUM(CASE WHEN tsunami_alert THEN 1 ELSE 0 END), 0) AS tsunami_events,
            COALESCE(SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END), 0) AS anomalous_events,
            COALESCE(ROUND(AVG(depth_km)::numeric, 2), 0.0) AS avg_depth_km,
            COALESCE(MODE() WITHIN GROUP (ORDER BY region), 'Sin actividad') AS top_active_region
        FROM gold.fact_earthquakes
        WHERE event_date = %s;
    """
    rows = execute_query(query, (ref_date,), role="transform", fetch=True)
    metrics = rows[0] if rows else {
        "total_events": 0, "max_magnitude": 0.0, "avg_magnitude": 0.0,
        "tsunami_events": 0, "anomalous_events": 0, "avg_depth_km": 0.0,
        "top_active_region": "Sin actividad"
    }

    # Obtener el evento mayor
    query_top = """
        SELECT place, magnitude, depth_km, region
        FROM gold.fact_earthquakes
        WHERE event_date = %s
        ORDER BY magnitude DESC
        LIMIT 1;
    """
    top_row = execute_query(query_top, (ref_date,), role="transform", fetch=True)
    if top_row:
        metrics["max_mag_location"] = f"{top_row[0]['place']} ({top_row[0]['region']})"
    else:
        metrics["max_mag_location"] = "No se registraron eventos significativos"

    # Obtener top 3 eventos destacados
    query_notable = """
        SELECT id, magnitude, place, region, depth_km, tsunami_alert, is_anomaly, anomaly_reason
        FROM gold.fact_earthquakes
        WHERE event_date = %s
        ORDER BY magnitude DESC
        LIMIT 3;
    """
    notables = execute_query(query_notable, (ref_date,), role="transform", fetch=True)
    metrics["notable_events"] = notables
    metrics["report_date"] = str(ref_date)
    return metrics

def validate_numbers_integrity(narrative_text: str, metrics: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """
    Verifica que las cifras numéricas mencionadas en el texto narrativo no contradigan
    los números calculados oficialmente.
    """
    discrepancies = []
    # Verificar si menciona magnitud máxima inventada
    max_mag = float(metrics.get("max_magnitude", 0.0))
    # Buscar patrones de magnitud en el texto como "magnitud X.Y" o "mag X.Y"
    mag_matches = re.findall(r"(?:magnitud|mag(?:\.)?)\s*([0-9]+(?:\.[0-9]+)?)", narrative_text, flags=re.IGNORECASE)
    for m in mag_matches:
        val = float(m)
        if val > (max_mag + 0.1): # Margen mínimo por redondeo
            discrepancies.append(f"El texto menciona una magnitud {val} que supera el máximo verificado de {max_mag}.")

    # Verificar mención de conteo de tsunamis
    tsunami_count = int(metrics.get("tsunami_events", 0))
    if tsunami_count == 0:
        if re.search(r"\b(generó|desencadenó|provocó|activó)\s+(un|varios|alerta de)?\s+tsunami\b", narrative_text, flags=re.IGNORECASE):
            discrepancies.append("El texto afirma que hubo alerta de tsunami, pero el conteo oficial verificado es 0.")

    is_valid = len(discrepancies) == 0
    return is_valid, discrepancies

def generate_fallback_report(metrics: Dict[str, Any]) -> Dict[str, Any]:
    """
    Plan de contingencia determinista: genera un reporte ejecutivo estructurado sin depender
    de LLM externo, garantizando que el pipeline nunca se rompa.
    """
    total = metrics.get("total_events", 0)
    max_m = metrics.get("max_magnitude", 0.0)
    top_loc = metrics.get("max_mag_location", "Ubicación desconocida")
    avg_m = metrics.get("avg_magnitude", 0.0)
    tsunamis = metrics.get("tsunami_events", 0)
    anomalies = metrics.get("anomalous_events", 0)
    top_reg = metrics.get("top_active_region", "N/A")
    r_date = metrics.get("report_date", str(date.today()))

    risk = "Bajo"
    if max_m >= 7.0 or tsunamis > 0:
        risk = "Crítico"
    elif max_m >= 6.0 or anomalies >= 3:
        risk = "Elevado"
    elif max_m >= 5.0 or total > 100:
        risk = "Moderado"

    summary = (
        f"Durante la jornada del {r_date}, la red global SismoWatch registró un total de {total} eventos sísmicos "
        f"analizados. La magnitud máxima observada fue de {max_m} localizada en {top_loc}. El promedio "
        f"global de magnitud se situó en {avg_m}. "
        f"La región con mayor concentración de sismicidad fue '{top_reg}'. Se detectaron {anomalies} eventos "
        f"con patrones de anomalía o agrupamiento enjambre y {tsunamis} eventos con alerta de tsunami."
    )

    findings = [
        f"Actividad sísmica consolidada: {total} eventos procesados globalmente.",
        f"Evento de mayor intensidad: Magnitud {max_m} en {top_loc}.",
        f"Foco regional predominante: {top_reg} presentó la mayor frecuencia de eventos.",
        f"Seguridad marítima: {tsunamis} alertas de tsunami generadas en las últimas 24 horas."
    ]

    recommendations = [
        f"Mantener monitoreo continuo en el clúster regional de {top_reg}.",
        "Supervisar el estado de las compuertas de calidad y validar la frescura de las estaciones sísmicas."
    ]

    return {
        "title": f"Reporte Ejecutivo Sísmico Diario - {r_date} [Modo de Contingencia]",
        "executive_summary": summary,
        "key_findings": findings,
        "risk_assessment": risk,
        "operational_recommendations": recommendations
    }

def generate_daily_ai_report(target_date: Optional[date] = None) -> Dict[str, Any]:
    """
    Ejecuta el flujo completo de generación de reporte:
      1. Obtiene métricas calculadas oficiales.
      2. Invoca el proveedor LLM (Gemini / OpenAI).
      3. Valida el formato estructurado Pydantic y la veracidad numérica.
      4. Si el LLM falla, excede tiempo o alucina, activa el Fallback automático.
      5. Registra auditoría en gold.ai_execution_logs y guarda el reporte en gold.ai_reports.
    """
    metrics = get_calculated_metrics(target_date)
    r_date_str = metrics.get("report_date", str(date.today()))
    prompt_path = os.path.join(os.path.dirname(__file__), "prompts", "executive_report_v1.txt")
    
    prompt_template = ""
    if os.path.exists(prompt_path):
        with open(prompt_path, "r", encoding="utf-8") as f:
            prompt_template = f.read()

    rendered_prompt = prompt_template.format(
        report_date=r_date_str,
        total_events=metrics.get("total_events", 0),
        max_magnitude=metrics.get("max_magnitude", 0.0),
        max_mag_location=metrics.get("max_mag_location", "N/A"),
        avg_magnitude=metrics.get("avg_magnitude", 0.0),
        tsunami_events=metrics.get("tsunami_events", 0),
        anomalous_events=metrics.get("anomalous_events", 0),
        top_active_region=metrics.get("top_active_region", "N/A"),
        notable_events_json=json.dumps(metrics.get("notable_events", []), indent=2, default=str)
    )

    start_time = time.time()
    model_name = settings.AI_MODEL_NAME
    tokens_prompt = len(rendered_prompt.split()) * 2 # Estimación de tokens
    tokens_completion = 0
    generation_status = "SUCCESS"
    error_msg = None
    report_data = None

    # Intentar llamada al LLM si hay API Key configurada
    if settings.GEMINI_API_KEY:
        try:
            import google.generativeai as genai
            genai.configure(api_key=settings.GEMINI_API_KEY)
            model = genai.GenerativeModel(model_name)
            response = model.generate_content(
                rendered_prompt,
                generation_config={"response_mime_type": "application/json", "temperature": 0.2}
            )
            raw_text = response.text
            parsed_json = json.loads(raw_text)
            validated = AIReportSchema(**parsed_json)
            
            # Validación de consistencia numérica
            is_valid_num, discrepancies = validate_numbers_integrity(validated.executive_summary, metrics)
            if not is_valid_num:
                logger.warning(f"Auditoría numérica falló: {discrepancies}. Activando fallback de protección.")
                report_data = generate_fallback_report(metrics)
                generation_status = "FALLBACK_DISCREPANCY"
            else:
                report_data = validated.model_dump()
                tokens_completion = len(raw_text.split()) * 2
        except Exception as exc:
            logger.warning(f"Error invocando Gemini API: {exc}. Activando fallback.")
            error_msg = str(exc)
            report_data = generate_fallback_report(metrics)
            generation_status = "FALLBACK"
    else:
        logger.info("Sin API Key de LLM configurada. Empleando motor analítico determinista de contingencia.")
        report_data = generate_fallback_report(metrics)
        generation_status = "FALLBACK"

    latency_ms = round((time.time() - start_time) * 1000, 2)
    total_tokens = tokens_prompt + tokens_completion
    # Costo aproximado estándar (\$0.00000015 por token en modelos ligeros)
    estimated_cost = round(total_tokens * 0.0000002, 6)

    # 1. Registrar auditoría en gold.ai_execution_logs
    audit_query = """
        INSERT INTO gold.ai_execution_logs (
            task_name, model_name, prompt_tokens, completion_tokens,
            total_tokens, latency_ms, estimated_cost_usd, status, error_message
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    audit_params = (
        "daily_executive_report",
        model_name if generation_status == "SUCCESS" else "deterministic-fallback",
        tokens_prompt,
        tokens_completion,
        total_tokens,
        latency_ms,
        estimated_cost,
        generation_status,
        error_msg
    )
    try:
        execute_query(audit_query, audit_params, role="transform")
    except Exception as exc:
        logger.error(f"Error auditando log de IA: {exc}")

    # 2. Guardar reporte en gold.ai_reports
    save_report_query = """
        INSERT INTO gold.ai_reports (
            report_date, title, executive_summary, key_findings,
            calculated_metrics, model_name, generation_status
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (report_date) DO UPDATE SET
            title = EXCLUDED.title,
            executive_summary = EXCLUDED.executive_summary,
            key_findings = EXCLUDED.key_findings,
            calculated_metrics = EXCLUDED.calculated_metrics,
            model_name = EXCLUDED.model_name,
            generation_status = EXCLUDED.generation_status,
            created_at = CURRENT_TIMESTAMP;
    """
    save_params = (
        datetime.strptime(r_date_str, "%Y-%m-%d").date(),
        report_data["title"],
        report_data["executive_summary"],
        json.dumps(report_data["key_findings"]),
        json.dumps(metrics, default=str),
        model_name if generation_status == "SUCCESS" else "fallback_engine",
        generation_status
    )
    execute_query(save_report_query, save_params, role="transform")

    logger.info(f"Reporte diario de IA generado para {r_date_str} (Estado: {generation_status}, Latencia: {latency_ms}ms).")
    return report_data

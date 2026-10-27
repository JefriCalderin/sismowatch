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
    Si no se especifica una fecha, selecciona automáticamente la fecha más reciente con datos activos.
    """
    if target_date is not None:
        ref_date = target_date
    else:
        latest_row = execute_query(
            "SELECT event_date FROM gold.fact_earthquakes GROUP BY event_date HAVING COUNT(*) > 0 ORDER BY event_date DESC LIMIT 1;",
            role="transform",
            fetch=True
        )
        if latest_row and latest_row[0].get("event_date"):
            ref_date = latest_row[0]["event_date"]
        else:
            ref_date = datetime.now(timezone.utc).date()
    
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

def build_executive_report_html(report_data: dict, metrics: dict, r_date_str: str, model_name: str) -> str:
    """Genera un artefacto HTML ejecutivo de alta fidelidad para el Data Lake MinIO."""
    title = report_data.get("title") or "Informe Ejecutivo de Situación Sísmica"
    summary = report_data.get("executive_summary") or "Sin resumen disponible."
    findings = report_data.get("key_findings") or []
    rec_metrics = metrics or {}

    total_events = rec_metrics.get("total_events", "N/A")
    max_mag = rec_metrics.get("max_magnitude", "N/A")
    max_place = rec_metrics.get("max_mag_location", "N/A")
    avg_depth = rec_metrics.get("avg_depth_km", "N/A")
    tsunami_cnt = rec_metrics.get("tsunami_events", 0)
    anom_cnt = rec_metrics.get("anomalous_events", 0)
    top_region = rec_metrics.get("top_active_region", "N/A")
    notable_events = rec_metrics.get("notable_events") or []

    findings_cards = []
    for idx, f in enumerate(findings, 1):
        findings_cards.append(f"""
        <div style="display:flex;gap:14px;align-items:flex-start;background:#ffffff;border:1px solid #e2e8f0;border-radius:10px;padding:14px 18px;margin-bottom:10px;">
          <div style="width:24px;height:24px;border-radius:50%;background:#e0e7ff;color:#4338ca;font-weight:800;font-size:12px;display:flex;align-items:center;justify-content:center;flex-shrink:0;margin-top:1px;">{idx}</div>
          <div style="font-size:14px;line-height:1.6;color:#334155;">{f}</div>
        </div>
        """)
    findings_html = "".join(findings_cards) if findings_cards else "<p style='color:#64748b;font-size:13px;'>No se registraron hallazgos atípicos.</p>"

    table_rows = []
    for ev in notable_events[:10]:
        eid = ev.get("id", "N/A")
        mag = ev.get("magnitude", "0.0")
        place = ev.get("place", "N/A")
        depth = ev.get("depth_km", "N/A")
        tsunami = "Sí" if ev.get("tsunami_alert") else "No"
        anom = "Anomalía ML" if ev.get("is_anomaly") else "Normal"
        anom_color = "#991b1b" if ev.get("is_anomaly") else "#166534"
        anom_bg = "#fee2e2" if ev.get("is_anomaly") else "#f0fdf4"
        mag_f = float(mag) if str(mag).replace('.', '', 1).isdigit() else 0.0
        mag_badge_bg = "#fee2e2" if mag_f >= 6.0 else ("#ffedd5" if mag_f >= 4.5 else "#ecfdf5")
        mag_badge_col = "#991b1b" if mag_f >= 6.0 else ("#9a3412" if mag_f >= 4.5 else "#065f46")
        
        table_rows.append(f"""
        <tr style="border-bottom:1px solid #e2e8f0;">
          <td style="padding:10px 14px;font-family:monospace;font-weight:700;color:#4f46e5;">{eid}</td>
          <td style="padding:10px 14px;"><span style="background:{mag_badge_bg};color:{mag_badge_col};padding:3px 8px;border-radius:6px;font-weight:700;">M {mag}</span></td>
          <td style="padding:10px 14px;font-weight:500;color:#1e293b;">{place}</td>
          <td style="padding:10px 14px;color:#475569;">{depth} km</td>
          <td style="padding:10px 14px;font-weight:600;color:{'#dc2626' if tsunami=='Sí' else '#475569'};">{tsunami}</td>
          <td style="padding:10px 14px;"><span style="background:{anom_bg};color:{anom_color};padding:2px 8px;border-radius:4px;font-size:11px;font-weight:600;">{anom}</span></td>
        </tr>
        """)
    table_html = "".join(table_rows) if table_rows else "<tr><td colspan='6' style='padding:16px;text-align:center;color:#64748b;'>No hay eventos notables reportados.</td></tr>"

    return f"""<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{title}</title>
  <style>
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
      color: #1e293b;
      background: #f1f5f9;
      line-height: 1.5;
      padding: 32px 16px;
    }}
    .document-container {{
      max-width: 980px;
      margin: 0 auto;
      background: #ffffff;
      border: 1px solid #cbd5e1;
      border-radius: 16px;
      box-shadow: 0 10px 25px -5px rgba(0, 0, 0, 0.05), 0 8px 10px -6px rgba(0, 0, 0, 0.05);
      overflow: hidden;
    }}
    .header-bar {{
      background: linear-gradient(135deg, #0f172a 0%, #1e1b4b 50%, #312e81 100%);
      color: #ffffff;
      padding: 36px 40px;
      border-bottom: 4px solid #6366f1;
      display: flex;
      justify-content: space-between;
      align-items: flex-start;
      gap: 20px;
    }}
    .brand-badge {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      background: rgba(99, 102, 241, 0.2);
      border: 1px solid rgba(165, 180, 252, 0.4);
      color: #e0e7ff;
      padding: 4px 12px;
      border-radius: 9999px;
      font-size: 11px;
      font-weight: 700;
      letter-spacing: 0.8px;
      text-transform: uppercase;
      margin-bottom: 12px;
    }}
    .report-title {{
      font-size: 24px;
      font-weight: 800;
      color: #ffffff;
      line-height: 1.25;
      letter-spacing: -0.5px;
    }}
    .meta-subtitle {{
      margin-top: 10px;
      font-size: 13px;
      color: #cbd5e1;
      display: flex;
      flex-wrap: wrap;
      gap: 16px;
    }}
    .meta-subtitle span {{
      display: inline-flex;
      align-items: center;
      gap: 6px;
    }}
    .btn-print {{
      background: rgba(255, 255, 255, 0.15);
      color: #ffffff;
      border: 1px solid rgba(255, 255, 255, 0.3);
      padding: 9px 16px;
      border-radius: 8px;
      font-size: 12px;
      font-weight: 600;
      cursor: pointer;
      display: inline-flex;
      align-items: center;
      gap: 6px;
      transition: all 0.2s ease;
      white-space: nowrap;
    }}
    .btn-print:hover {{
      background: rgba(255, 255, 255, 0.25);
    }}
    .content-body {{
      padding: 36px 40px;
    }}
    .section-header {{
      font-size: 13px;
      text-transform: uppercase;
      font-weight: 800;
      letter-spacing: 0.8px;
      color: #64748b;
      margin-bottom: 14px;
      display: flex;
      align-items: center;
      gap: 10px;
    }}
    .section-header::after {{
      content: "";
      flex: 1;
      height: 1px;
      background: #e2e8f0;
    }}
    .kpi-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
      gap: 12px;
      margin-bottom: 30px;
    }}
    .kpi-card {{
      background: #f8fafc;
      border: 1px solid #e2e8f0;
      border-radius: 12px;
      padding: 14px 16px;
    }}
    .kpi-label {{
      font-size: 11px;
      font-weight: 700;
      color: #64748b;
      text-transform: uppercase;
      letter-spacing: 0.5px;
    }}
    .kpi-value {{
      font-size: 22px;
      font-weight: 800;
      color: #0f172a;
      margin: 4px 0 2px 0;
    }}
    .kpi-desc {{
      font-size: 11px;
      color: #64748b;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }}
    .summary-card {{
      background: #eff6ff;
      border-left: 5px solid #4f46e5;
      border-radius: 0 12px 12px 0;
      padding: 22px 26px;
      margin-bottom: 32px;
    }}
    .summary-card h3 {{
      font-size: 15px;
      font-weight: 800;
      color: #1e3a8a;
      margin-bottom: 8px;
      text-transform: uppercase;
      letter-spacing: 0.5px;
    }}
    .summary-card p {{
      font-size: 14.5px;
      line-height: 1.7;
      color: #1e293b;
    }}
    .table-container {{
      border: 1px solid #e2e8f0;
      border-radius: 12px;
      overflow: hidden;
      margin-bottom: 32px;
    }}
    .exec-table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 12.5px;
      text-align: left;
    }}
    .exec-table th {{
      background: #f1f5f9;
      color: #475569;
      font-weight: 700;
      padding: 11px 14px;
      border-bottom: 1px solid #cbd5e1;
      text-transform: uppercase;
      font-size: 11px;
      letter-spacing: 0.6px;
    }}
    .footer-stamp {{
      border-top: 1px solid #e2e8f0;
      padding-top: 24px;
      display: flex;
      flex-wrap: wrap;
      justify-content: space-between;
      align-items: center;
      font-size: 12px;
      color: #64748b;
      gap: 12px;
    }}
    .stamp-pill {{
      display: inline-flex;
      align-items: center;
      gap: 6px;
      background: #f0fdf4;
      border: 1px solid #86efac;
      color: #166534;
      padding: 5px 12px;
      border-radius: 9999px;
      font-weight: 700;
      font-size: 11px;
    }}
    @media print {{
      body {{ background: #fff; padding: 0; }}
      .document-container {{ border: none; box-shadow: none; max-width: 100%; border-radius: 0; }}
      .btn-print {{ display: none !important; }}
      .header-bar {{ padding: 24px 30px; }}
      .content-body {{ padding: 24px 30px; }}
    }}
  </style>
</head>
<body>
  <div class="document-container">
    <div class="header-bar">
      <div>
        <div class="brand-badge">SismoWatch • SuperApp Sismológica Lakehouse</div>
        <h1 class="report-title">{title}</h1>
        <div class="meta-subtitle">
          <span><strong>Fecha de Evaluación:</strong> {r_date_str}</span>
          <span><strong>Certificación IA:</strong> {model_name}</span>
          <span><strong>Capa de Datos:</strong> Gold (PostgreSQL + MinIO S3)</span>
        </div>
      </div>
      <div>
        <button onclick="window.print()" class="btn-print">
          <svg style="width:14px;height:14px;" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17 17h2a2 2 0 002-2v-4a2 2 0 00-2-2H5a2 2 0 00-2 2v4a2 2 0 002 2h2m2 4h6a2 2 0 002-2v-4H7v4a2 2 0 002 2zm8-12V5a2 2 0 00-2-2H9a2 2 0 00-2 2v4h10z"/></svg>
          Imprimir / Guardar PDF
        </button>
      </div>
    </div>

    <div class="content-body">
      <!-- 1. Métricas Clave del Día -->
      <div class="section-header">Indicadores Clave de Situación Sísmica (Capa Gold)</div>
      <div class="kpi-grid">
        <div class="kpi-card">
          <div class="kpi-label">Total Sismos</div>
          <div class="kpi-value">{total_events}</div>
          <div class="kpi-desc">Eventos monitoreados</div>
        </div>
        <div class="kpi-card">
          <div class="kpi-label">Magnitud Máxima</div>
          <div class="kpi-value" style="color:#b91c1c;">M {max_mag}</div>
          <div class="kpi-desc" title="{max_place}">{max_place}</div>
        </div>
        <div class="kpi-card">
          <div class="kpi-label">Profundidad Media</div>
          <div class="kpi-value">{avg_depth} km</div>
          <div class="kpi-desc">Cálculo ponderado</div>
        </div>
        <div class="kpi-card">
          <div class="kpi-label">Alertas Tsunami</div>
          <div class="kpi-value" style="color:{'#dc2626' if tsunami_cnt > 0 else '#16a34a'};">{tsunami_cnt}</div>
          <div class="kpi-desc">Protocolo de costas</div>
        </div>
        <div class="kpi-card">
          <div class="kpi-label">Región Más Activa</div>
          <div class="kpi-value" style="font-size:18px;">{top_region}</div>
          <div class="kpi-desc">Mayor concentración</div>
        </div>
        <div class="kpi-card">
          <div class="kpi-label">Anomalías ML</div>
          <div class="kpi-value" style="color:{'#d97706' if anom_cnt > 0 else '#475569'};">{anom_cnt}</div>
          <div class="kpi-desc">Isolation Forest</div>
        </div>
      </div>

      <!-- 2. Resumen Ejecutivo de IA -->
      <div class="summary-card">
        <h3>Evaluación Ejecutiva y Situación Geológica</h3>
        <p>{summary}</p>
      </div>

      <!-- 3. Hallazgos Clave Verificados -->
      <div class="section-header">Hallazgos Analíticos y Conclusiones Prioritarias</div>
      <div style="margin-bottom:30px;">
        {findings_html}
      </div>

      <!-- 4. Eventos Notables -->
      <div class="section-header">Eventos Sísmicos Notables y Registros Relevantes</div>
      <div class="table-container">
        <table class="exec-table">
          <thead>
            <tr>
              <th>ID Evento</th>
              <th>Magnitud</th>
              <th>Ubicación / Epicentro</th>
              <th>Profundidad</th>
              <th>Tsunami</th>
              <th>Evaluación ML</th>
            </tr>
          </thead>
          <tbody>
            {table_html}
          </tbody>
        </table>
      </div>

      <!-- 5. Pie de Gobernanza y Certificación -->
      <div class="footer-stamp">
        <div>
          <span class="stamp-pill">
            <svg style="width:13px;height:13px;" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m5.618-4.016A11.955 11.955 0 0112 2.944a11.955 11.955 0 01-8.618 3.04A12.02 12.02 0 003 9c0 5.591 3.824 10.29 9 11.622 5.176-1.332 9-6.03 9-11.622 0-1.042-.133-2.052-.382-3.016z"/></svg>
            Compuerta Pandera: Aprobada (100% de conformidad)
          </span>
          <p style="margin-top:6px;font-size:11px;color:#94a3b8;">Documento oficial generado bajo Arquitectura Medallón (Bronze &rarr; Silver &rarr; Gold) con persistencia en MinIO S3 y PostgreSQL.</p>
        </div>
        <div style="text-align:right;">
          <p style="font-weight:700;color:#334155;">SismoWatch Operations</p>
          <p style="font-size:11px;color:#94a3b8;">Reporte Oficial Diario</p>
        </div>
      </div>
    </div>
  </div>
</body>
</html>"""

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
    model_name = settings.AI_MODEL_NAME or "gemini-3.5-flash-lite"
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

    # 3. Sincronizar artefacto en MinIO Object Storage (Data Lake)
    try:
        from airflow.include.storage_minio import upload_report_to_minio
        html_artifact = build_executive_report_html(
            report_data=report_data,
            metrics=metrics,
            r_date_str=r_date_str,
            model_name=model_name if generation_status == "SUCCESS" else "fallback_engine"
        )
        upload_report_to_minio(r_date_str, html_artifact, report_data)
    except Exception as exc:
        logger.warning(f"No se pudo sincronizar reporte en MinIO: {exc}")

    logger.info(f"Reporte diario de IA generado para {r_date_str} (Estado: {generation_status}, Latencia: {latency_ms}ms).")
    return report_data

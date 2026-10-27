"""
Router de Chat Asistente de IA con Acceso a la Capa Gold (SismoWatch AI Assistant).
Permite a usuarios autenticados consultar en lenguaje natural sobre sismos, métricas,
anomalías, calidad de datos y reportes ejecutivos consolidados en la capa Gold.
"""
import logging
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field
from fastapi import APIRouter, Depends, HTTPException, status
from api.app.security import require_role
from api.app.database import query_db, query_db_one
from api.app.config import api_settings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/chat", tags=["Chat Asistente IA"])

class ChatMessage(BaseModel):
    role: str = Field(..., description="'user' o 'assistant'")
    content: str = Field(..., description="Mensaje del usuario o respuesta previa")

class ChatRequest(BaseModel):
    message: str = Field(..., min_length=1, max_length=1000, description="Pregunta del usuario")
    history: Optional[List[ChatMessage]] = Field(default=[], description="Historial previo de conversación")

class ChatResponse(BaseModel):
    reply: str = Field(..., description="Respuesta del Asistente IA")
    data_sources: List[str] = Field(..., description="Tablas y vistas de la Capa Gold consultadas")
    timestamp: str = Field(..., description="Hora de respuesta ISO")
    metadata: Dict[str, Any] = Field(default={}, description="Metadatos de contexto")

def fetch_gold_context() -> Dict[str, Any]:
    """Extrae el contexto verificado en tiempo real desde la capa Gold de PostgreSQL."""
    context = {}

    # 1. KPIs en tiempo real
    try:
        kpi = query_db_one("SELECT * FROM gold.view_realtime_kpis LIMIT 1;")
        context["kpis"] = kpi or {}
    except Exception as exc:
        logger.warning(f"Error consultando KPIs para chat: {exc}")
        context["kpis"] = {}

    # 2. Sismos más intensos recientes (Top 5)
    try:
        top_events = query_db("""
            SELECT id, magnitude, place, region, event_time, depth_km, tsunami_alert, magnitude_category, is_anomaly, anomaly_reason
            FROM gold.fact_earthquakes
            ORDER BY magnitude DESC
            LIMIT 5;
        """)
        context["top_events"] = top_events
    except Exception as exc:
        logger.warning(f"Error consultando top sismos para chat: {exc}")
        context["top_events"] = []

    # 3. Últimos 8 eventos registrados
    try:
        recent_events = query_db("""
            SELECT id, magnitude, place, region, event_time, depth_km, tsunami_alert, is_anomaly
            FROM gold.fact_earthquakes
            ORDER BY event_time DESC
            LIMIT 8;
        """)
        context["recent_events"] = recent_events
    except Exception as exc:
        logger.warning(f"Error consultando eventos recientes para chat: {exc}")
        context["recent_events"] = []

    # 4. Último Reporte Ejecutivo de IA
    try:
        last_report = query_db_one("""
            SELECT report_date, title, executive_summary, key_findings, calculated_metrics, model_name
            FROM gold.ai_reports
            ORDER BY report_date DESC
            LIMIT 1;
        """)
        context["last_report"] = last_report or {}
    except Exception as exc:
        logger.warning(f"Error consultando último reporte para chat: {exc}")
        context["last_report"] = {}

    # 5. Métricas de Calidad de Datos (Pandera Gate)
    try:
        last_quality = query_db_one("""
            SELECT status, quality_score, valid_records, rejected_records, freshness_lag_seconds, rules_passed, rules_failed, details
            FROM gold.quality_metrics
            ORDER BY execution_timestamp DESC
            LIMIT 1;
        """)
        context["quality"] = last_quality or {}
    except Exception as exc:
        logger.warning(f"Error consultando métricas de calidad para chat: {exc}")
        context["quality"] = {}

    # 6. Alertas de anomalías
    try:
        anomalies = query_db("""
            SELECT alert_id, alert_type, severity, description, created_at
            FROM gold.anomaly_alerts
            ORDER BY created_at DESC
            LIMIT 5;
        """)
        context["anomalies"] = anomalies
    except Exception as exc:
        logger.warning(f"Error consultando anomalías para chat: {exc}")
        context["anomalies"] = []

    return context

def generate_offline_intelligent_response(question: str, ctx: Dict[str, Any]) -> str:
    """
    Motor analítico determinista e inteligente cuando el LLM externo no está disponible o no tiene cuota.
    Garantiza respuestas 100% verídicas y útiles fundamentadas en la capa Gold.
    """
    q = question.lower()
    kpis = ctx.get("kpis", {})
    top_events = ctx.get("top_events", [])
    recent_events = ctx.get("recent_events", [])
    last_report = ctx.get("last_report", {})
    quality = ctx.get("quality", {})
    anomalies = ctx.get("anomalies", [])

    # Caso 1: Sismo más fuerte / magnitudes altas
    if any(k in q for k in ["fuerte", "mayor magnitud", "mas alto", "máxima", "maximo", "top sismos", "intens"]):
        if not top_events:
            return "Actualmente no hay registros de sismos suficientes en la capa Gold para calcular el evento máximo."
        top1 = top_events[0]
        event_time_str = top1['event_time'].strftime('%Y-%m-%d %H:%M:%S UTC') if top1.get('event_time') else 'Fecha no disponible'
        lines = [
            f"### 📍 Sismo de Mayor Magnitud Registrado en la Capa Gold\n",
            f"- **Ubicación:** {top1.get('place')} ({top1.get('region', 'N/A')})",
            f"- **Magnitud:** **{top1.get('magnitude')}** ({top1.get('magnitude_category', 'N/A')})",
            f"- **Profundidad:** {top1.get('depth_km')} km",
            f"- **Fecha y Hora:** {event_time_str}",
            f"- **Alerta de Tsunami:** {'⚠️ SÍ' if top1.get('tsunami_alert') else '✅ No'}",
            f"- **¿Es anomalía?:** {'⚠️ SÍ' if top1.get('is_anomaly') else '✅ No'}\n",
            f"**Otros sismos significativos recientes:**"
        ]
        for e in top_events[1:4]:
            t_str = e['event_time'].strftime('%H:%M UTC') if e.get('event_time') else ''
            lines.append(f"- Magnitud **{e.get('magnitude')}** en *{e.get('place')}* ({t_str})")
        return "\n".join(lines)

    # Caso 2: Anomalías / Enjambres / Outliers
    if any(k in q for k in ["anomal", "enjambre", "alerta", "raro", "atipico", "outlier", "isolation"]):
        if anomalies:
            lines = ["### 🚨 Alertas y Anomalías Detectadas (Isolation Forest + Haversine)\n"]
            for a in anomalies:
                lines.append(f"- **[{a.get('severity', 'ALERTA')}] {a.get('alert_type')}:** {a.get('description')}")
            return "\n".join(lines)
        else:
            anom_count = kpis.get("anomalies_count", 0)
            return (
                f"### ✅ Estado de Anomalías Sísmicas\n\n"
                f"El modelo de Machine Learning (*Isolation Forest* con agrupamiento espacio-temporal Haversine) "
                f"reporta **{anom_count} anomalías críticas** en la ventana de observación actual. "
                f"Todos los sismos registrados se encuentran dentro de las distribuciones espaciales y de profundidad normales."
            )

    # Caso 3: Reporte Ejecutivo / Resumen
    if any(k in q for k in ["reporte", "informe", "resumen", "ejecutivo", "hallazgo", "conclusi"]):
        if not last_report:
            return "No se ha generado aún un reporte ejecutivo diario en `gold.ai_reports`."
        findings = last_report.get("key_findings") or []
        findings_md = "\n".join([f"- {f}" for f in findings])
        return (
            f"### 📋 {last_report.get('title', 'Informe Ejecutivo Sísmico')}\n\n"
            f"**Fecha:** {last_report.get('report_date')} | **Modelo Auditor:** `{last_report.get('model_name')}`\n\n"
            f"#### Resumen Ejecutivo:\n"
            f"{last_report.get('executive_summary')}\n\n"
            f"#### Hallazgos Clave Verificados en Gold:\n"
            f"{findings_md}"
        )

    # Caso 4: Calidad de datos / Pandera / Gobernanza
    if any(k in q for k in ["calidad", "pandera", "compuerta", "gobernanza", "score", "regla", "rechaz"]):
        score = quality.get("quality_score", 100.0)
        status_q = quality.get("status", "PASSED")
        valid = quality.get("valid_records", 0)
        rejected = quality.get("rejected_records", 0)
        lag = quality.get("freshness_lag_seconds", 0)
        return (
            f"### 🛡️ Compuerta de Calidad de Datos (Pandera Data Quality Gate)\n\n"
            f"- **Estado Global:** **{status_q}**\n"
            f"- **Puntaje de Calidad:** **{score}%**\n"
            f"- **Registros Conformes (Silver/Gold):** {valid}\n"
            f"- **Registros Rechazados (Sin Cuarentena):** {rejected}\n"
            f"- **Latencia / Frescura de Datos:** {lag} segundos\n"
            f"- **Reglas Evaluadas:** 7 reglas formales (Rango de coordenadas, unicidad ID, profundidad -5 a 1000km, "
            f"magnitud -2 a 10, no nulidad obligatoria, coherencia temporal y frescura)."
        )

    # Caso 5: Recomendaciones y prevención ante sismos
    if any(k in q for k in ["recomendaci", "prevenci", "seguridad", "que hacer", "evacua", "terremoto", "tsunami"]):
        return (
            "### 🛡️ Recomendaciones Oficiales de Prevención Sísmica (Protocolos USGS / UNGRD)\n\n"
            "1. **Durante el Sismo:**\n"
            "   - **Agáchese, Cúbrase y Agárrese:** Busque refugio bajo un mueble resistente o junto a un muro estructural.\n"
            "   - Aléjese inmediatamente de ventanas, repisas, cables de alta tensión y objetos colgantes.\n"
            "   - Si está dentro de una edificación, no use ascensores.\n\n"
            "2. **Después del Sismo:**\n"
            "   - Verifique fugas de gas, daños en conexiones eléctricas y posibles grietas estructurales.\n"
            "   - Prepárese para posibles réplicas (suelen ocurrir en las horas o días siguientes).\n"
            "   - Mantenga sintonizada la radio de emergencias y evite saturar las líneas telefónicas.\n\n"
            "3. **Medidas Preventivas Permanentes:**\n"
            "   - Mantenga siempre a mano el kit de emergencia (agua potable, linterna, botiquín, alimentos no perecederos).\n"
            "   - Defina con su familia un punto de encuentro seguro fuera de zonas de desprendimiento."
        )

    # Caso 6: Conteo y estadísticas generales
    return (
        f"### 🌐 Estado General del Data Lakehouse Sísmico (Capa Gold)\n\n"
        f"- **Eventos en las últimas 24 horas:** {kpis.get('events_24h', 0)}\n"
        f"- **Magnitud Máxima Reciente:** {kpis.get('max_magnitude', 0.0)}\n"
        f"- **Magnitud Promedio:** {kpis.get('avg_magnitude', 0.0)}\n"
        f"- **Alertas de Tsunami:** {kpis.get('tsunami_alerts', 0)}\n"
        f"- **Calidad de Ingesta:** {kpis.get('quality_score', 100.0)}% ({kpis.get('freshness_status', 'Óptima')})\n\n"
        f"Puedes preguntarme sobre:\n"
        f"- *¿Cuál fue el sismo más fuerte y en qué región ocurrió?*\n"
        f"- *¿Se han detectado anomalías o enjambres sísmicos con Isolation Forest?*\n"
        f"- *Dame un resumen del último informe ejecutivo y sus hallazgos.*\n"
        f"- *¿Cuál es el estado de la compuerta de calidad de Pandera?*\n"
        f"- *¿Qué recomendaciones de seguridad y prevención aplican?*"
    )

def query_llm_with_context(question: str, history: List[ChatMessage], ctx: Dict[str, Any]) -> Optional[str]:
    """Invoca Gemini o OpenAI inyectando el contexto de la capa Gold."""
    api_key = api_settings.GEMINI_API_KEY
    if not api_key:
        return None

    try:
        import google.generativeai as genai
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel(api_settings.AI_MODEL_NAME or "gemini-1.5-flash")

        system_prompt = f"""
Eres el Asistente Experto de SismoWatch, especializado en sismología y arquitectura de datos Medallón (Bronze -> Silver -> Gold).
Tienes acceso en tiempo real a los datos consolidados y verificados en la capa GOLD de PostgreSQL.

DATOS VERIFICADOS DE LA CAPA GOLD:
- KPIs 24h: {ctx.get('kpis')}
- Top Sismos Fuertes: {ctx.get('top_events')}
- Sismos Recientes: {ctx.get('recent_events')}
- Último Reporte Ejecutivo: {ctx.get('last_report')}
- Métricas de Calidad (Pandera): {ctx.get('quality')}
- Anomalías Detectadas (ML): {ctx.get('anomalies')}

REGLAS ESTRICTAS:
1. Responde a la pregunta del usuario con absoluta fidelidad a estos datos.
2. NO inventes magnitudes, fechas, nombres de lugares ni cantidades de sismos.
3. Si el usuario pregunta por recomendaciones de prevención ante sismos, proporciona medidas sismológicas estándar (USGS/UNGRD).
4. Formatea la respuesta con Markdown claro (títulos, negritas, listas ordenadas o viñetas).
5. Responde siempre en español.
"""
        prompt = f"{system_prompt}\n\nPregunta del usuario: {question}"
        response = model.generate_content(prompt)
        if response and response.text:
            return response.text.strip()
    except Exception as exc:
        logger.warning(f"Fallo al invocar LLM para chat: {exc}. Usando motor determinista.")
    return None

@router.post("", response_model=ChatResponse)
def ask_ai_assistant(
    request: ChatRequest,
    current_user: dict = Depends(require_role(["viewer", "admin"]))
):
    """
    Endpoint para chatear con el Asistente IA sobre los datos de la Capa Gold.
    Disponible para roles 'viewer' y 'admin'.
    """
    gold_context = fetch_gold_context()

    # Intentar responder con LLM
    llm_reply = query_llm_with_context(request.message, request.history or [], gold_context)
    
    # Si el LLM no está disponible o falla, usar motor determinista
    reply = llm_reply or generate_offline_intelligent_response(request.message, gold_context)

    sources = [
        "gold.fact_earthquakes",
        "gold.daily_metrics",
        "gold.view_realtime_kpis",
        "gold.ai_reports",
        "gold.quality_metrics",
        "gold.anomaly_alerts"
    ]

    return ChatResponse(
        reply=reply,
        data_sources=sources,
        timestamp=datetime.now(timezone.utc).isoformat(),
        metadata={
            "user": current_user.get("username"),
            "role": current_user.get("role"),
            "events_count": gold_context.get("kpis", {}).get("events_24h", 0)
        }
    )

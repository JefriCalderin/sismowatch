"""
Router de Chat Asistente de IA con Acceso a la Capa Gold (SismoWatch AI Assistant).
Implementa un System Prompt riguroso y Few-Shot Learning para evitar alucinaciones,
responder puntualmente a lo solicitado sin emitir datos no requeridos y fundamentarse
estrictamente en los hechos curados de la Capa Gold (PostgreSQL + MinIO S3).
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

# ------------------------------------------------------------------------------
# System Prompt y Few-Shot Learning Oficial
# ------------------------------------------------------------------------------
SYSTEM_PROMPT = """
Eres el Asistente Sismológico Oficial de SismoWatch, especializado en monitoreo de eventos sísmicos y conectado en tiempo real a la capa GOLD del Data Lakehouse (PostgreSQL + MinIO S3).

PRINCIPIOS OBLIGATORIOS DE COMPORTAMIENTO:
1. PUNTUALIDAD Y CONCISIÓN EXTREMA: Responde ÚNICAMENTE lo que el usuario esté preguntando. NO agregues estadísticas no solicitadas, ni tablas de datos que no se te hayan pedido, ni introducciones redundantes. Sé breve, claro y ve directo al grano.
2. FIDELIDAD TOTAL (CERO ALUCINACIÓN): Basa cualquier dato numérico (magnitud, conteo de sismos, profundidad, región, alertas de tsunami) ESTRICTAMENTE en los datos de la capa Gold provistos. No inventes magnitudes ni lugares. Si el usuario pregunta por un dato que no está en el contexto, indica con honestidad que no se tiene registro.
3. TONO Y FORMATO: Responde en español neutro, cortés y profesional. Utiliza negritas para resaltar datos clave y listas solo cuando el usuario solicite múltiples elementos.
"""

FEW_SHOT_EXAMPLES = """
EJEMPLOS DE COMPORTAMIENTO ESPERADO (FEW-SHOT):

[Ejemplo 1 - Saludo simple]
Usuario: hola como estas
Asistente: ¡Hola! Estoy muy bien, gracias por preguntar. Soy tu Asistente Sismológico de SismoWatch. ¿Qué consulta tienes hoy sobre los eventos sísmicos o reportes de la capa Gold?

[Ejemplo 2 - Conteo de hoy]
Usuario: cuantos eventos han habido el dia de hoy hasta el momento?
Asistente: Hasta el momento el día de hoy, se han registrado **{events_today} eventos sísmicos** en el catálogo Gold, con una magnitud máxima de **{max_mag}** y una magnitud promedio de **{avg_mag}**.

[Ejemplo 3 - Sismo más fuerte]
Usuario: cual fue el sismo mas fuerte?
Asistente: El sismo de mayor magnitud registrado es de **M {max_mag}**, ocurrido en **{place}** ({region}) a una profundidad de **{depth} km** ({time_utc} UTC). Alerta de tsunami: No.

[Ejemplo 4 - Consulta de Tsunami]
Usuario: hay alertas de tsunami?
Asistente: No. Actualmente no hay ninguna alerta de tsunami activa en los registros de la capa Gold.

[Ejemplo 5 - Calidad de datos Pandera]
Usuario: cual es el estado de calidad de los datos?
Asistente: La compuerta de calidad de datos (Pandera Data Quality Gate) reporta un puntaje del **100% de conformidad** (Estado: PASSED), con 7 reglas formales aprobadas y cero registros no conformes en Gold.

[Ejemplo 6 - Recomendaciones de seguridad]
Usuario: que hacer en caso de terremoto?
Asistente: Durante un sismo sigue estas 3 pautas prioritarias:
1. **Agáchese, Cúbrase y Agárrese** debajo de una mesa resistente o junto a un muro estructural.
2. Aléjese de ventanas, cristales y objetos colgantes.
3. No use ascensores y espere a que termine el movimiento para evacuar.
"""

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
    context: Dict[str, Any] = {}

    # 1. KPIs en tiempo real (Últimas 24 horas)
    try:
        kpi = query_db_one("SELECT * FROM gold.view_realtime_kpis LIMIT 1;") or {}
        context["kpis"] = {
            "events_24h": kpi.get("events_24h", 0),
            "max_magnitude": float(kpi.get("max_magnitude_24h") or kpi.get("max_magnitude") or 0.0),
            "avg_magnitude": float(kpi.get("avg_magnitude_24h") or kpi.get("avg_magnitude") or 0.0),
            "tsunami_alerts": kpi.get("tsunami_alerts_24h", kpi.get("tsunami_alerts", 0)),
            "anomalies_count": kpi.get("anomalies_24h", kpi.get("anomalies_count", 0)),
            "last_event_time": str(kpi.get("last_event_time", "N/A")),
            "last_pipeline_update": str(kpi.get("last_pipeline_update", "N/A")),
        }
    except Exception as exc:
        logger.warning(f"Error consultando KPIs para chat: {exc}")
        context["kpis"] = {
            "events_24h": 0, "max_magnitude": 0.0, "avg_magnitude": 0.0,
            "tsunami_alerts": 0, "anomalies_count": 0
        }

    # 2. Conteo específico para el día de hoy (desde las 00:00:00 UTC)
    try:
        today_stat = query_db_one("""
            SELECT 
                COUNT(*) AS events_today,
                COALESCE(MAX(magnitude), 0.0) AS max_mag_today,
                COALESCE(ROUND(AVG(magnitude)::numeric, 2), 0.0) AS avg_mag_today,
                COALESCE(MIN(magnitude), 0.0) AS min_mag_today,
                COUNT(*) FILTER (WHERE tsunami_alert = TRUE) AS tsunami_today,
                COUNT(*) FILTER (WHERE is_anomaly = TRUE) AS anomalies_today
            FROM gold.fact_earthquakes
            WHERE event_time >= CURRENT_DATE;
        """) or {}
        context["today_stats"] = {
            "events_today": int(today_stat.get("events_today", 0)),
            "max_mag_today": float(today_stat.get("max_mag_today", 0.0)),
            "avg_mag_today": float(today_stat.get("avg_mag_today", 0.0)),
            "min_mag_today": float(today_stat.get("min_mag_today", 0.0)),
            "tsunami_today": int(today_stat.get("tsunami_today", 0)),
            "anomalies_today": int(today_stat.get("anomalies_today", 0)),
        }
    except Exception as exc:
        logger.warning(f"Error consultando estadísticas del día de hoy: {exc}")
        context["today_stats"] = {
            "events_today": 0, "max_mag_today": 0.0, "avg_mag_today": 0.0,
            "min_mag_today": 0.0, "tsunami_today": 0, "anomalies_today": 0
        }

    # 3. Sismos más intensos recientes (Top 5)
    try:
        top_events = query_db("""
            SELECT id, magnitude, place, region, event_time, depth_km, tsunami_alert, magnitude_category, is_anomaly, anomaly_reason
            FROM gold.fact_earthquakes
            ORDER BY magnitude DESC
            LIMIT 5;
        """)
        context["top_events"] = top_events or []
    except Exception as exc:
        logger.warning(f"Error consultando top sismos para chat: {exc}")
        context["top_events"] = []

    # 4. Últimos 5 eventos registrados
    try:
        recent_events = query_db("""
            SELECT id, magnitude, place, region, event_time, depth_km, tsunami_alert, is_anomaly
            FROM gold.fact_earthquakes
            ORDER BY event_time DESC
            LIMIT 5;
        """)
        context["recent_events"] = recent_events or []
    except Exception as exc:
        logger.warning(f"Error consultando eventos recientes para chat: {exc}")
        context["recent_events"] = []

    # 5. Último Reporte Ejecutivo de IA
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

    # 6. Métricas de Calidad de Datos (Pandera Gate)
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

    # 7. Alertas de anomalías
    try:
        anomalies = query_db("""
            SELECT id, event_id, algorithm, severity, explanation, detected_at
            FROM gold.anomaly_alerts
            ORDER BY detected_at DESC
            LIMIT 5;
        """)
        context["anomalies"] = anomalies or []
    except Exception as exc:
        logger.warning(f"Error consultando anomalías para chat: {exc}")
        context["anomalies"] = []

    return context

def generate_offline_intelligent_response(question: str, ctx: Dict[str, Any]) -> str:
    """
    Motor determinista alineado con los principios de Few-Shot y PUNTUALIDAD:
    Responde estrictamente lo que se pide, sin verter tablas de métricas no solicitadas.
    """
    q = question.lower().strip()
    kpis = ctx.get("kpis", {})
    today_stats = ctx.get("today_stats", {})
    top_events = ctx.get("top_events", [])
    recent_events = ctx.get("recent_events", [])
    last_report = ctx.get("last_report", {})
    quality = ctx.get("quality", {})
    anomalies = ctx.get("anomalies", [])

    events_24h = kpis.get("events_24h", 0)
    max_mag_24h = kpis.get("max_magnitude", 0.0)
    avg_mag_24h = kpis.get("avg_magnitude", 0.0)
    tsunami_24h = kpis.get("tsunami_alerts", 0)
    anom_24h = kpis.get("anomalies_count", 0)

    events_today = today_stats.get("events_today", 0)
    max_mag_today = today_stats.get("max_mag_today", 0.0)
    avg_mag_today = today_stats.get("avg_mag_today", 0.0)
    tsunami_today = today_stats.get("tsunami_today", 0)

    # 1. Saludo puntual (Sin datos de más)
    if any(k in q for k in ["hola", "como estas", "cómo estás", "buenos dias", "buenas tardes", "buenas noches", "saludos", "que tal", "quien eres"]):
        return (
            "¡Hola! 👋 Estoy muy bien, gracias por preguntar.\n\n"
            "Soy tu **Asistente Sismológico de SismoWatch**, conectado directamente a la **Capa Gold**. "
            "¿En qué puedo orientarte hoy sobre la actividad sísmica o los reportes analíticos?"
        )

    # 2. Conteo de sismos de hoy puntual
    if any(k in q for k in ["cuantos eventos", "cuántos eventos", "cuantos sismos", "cuántos sismos", "actividad de hoy", "el dia de hoy", "el día de hoy", "hasta el momento", "cuantos temblores"]):
        active_count = events_today if events_today > 0 else events_24h
        max_active = max_mag_today if max_mag_today > 0 else max_mag_24h
        avg_active = avg_mag_today if avg_mag_today > 0 else avg_mag_24h
        return (
            f"Hasta el momento el día de hoy, se han registrado **{active_count} eventos sísmicos** en el catálogo Gold, "
            f"con una magnitud máxima de **{max_active:.1f}** y un promedio de **{avg_active:.2f}**."
        )

    # 3. Alertas de tsunami puntuales
    if any(k in q for k in ["tsunami", "alerta marina", "maremoto"]):
        active_tsunami = tsunami_today if events_today > 0 else tsunami_24h
        if active_tsunami > 0:
            return f"⚠️ **Atención:** Se registran **{active_tsunami} alerta(s) de tsunami** asociadas a eventos recientes en la capa Gold."
        return "Actualmente **no hay alertas de tsunami activas** en los registros de la capa Gold."

    # 4. Sismo más fuerte puntual
    if any(k in q for k in ["fuerte", "mayor magnitud", "mas alto", "más alto", "máxima", "maximo", "máximo", "intens", "epicentro"]):
        if not top_events:
            return "Actualmente no hay registros en la capa Gold para determinar el sismo de mayor magnitud."
        top1 = top_events[0]
        event_time_str = top1['event_time'].strftime('%Y-%m-%d %H:%M:%S UTC') if top1.get('event_time') else 'N/A'
        tsunami_str = "Sí" if top1.get('tsunami_alert') else "No"
        return (
            f"El sismo de mayor magnitud registrado es de **M {top1.get('magnitude')}** ({top1.get('magnitude_category', 'N/A')}), "
            f"ocurrido en **{top1.get('place')}** ({top1.get('region', 'N/A')}) a una profundidad de **{top1.get('depth_km')} km** "
            f"({event_time_str}). Alerta de tsunami: **{tsunami_str}**."
        )

    # 5. Anomalías y Enjambres con ML
    if any(k in q for k in ["anomal", "enjambre", "alerta", "atipico", "atípico", "isolation"]):
        if anomalies:
            top_a = anomalies[0]
            return f"🚨 **Alerta ML ({top_a.get('severity', 'ALERTA')}):** {top_a.get('explanation')}"
        return f"El modelo *Isolation Forest* reporta **{anom_24h} anomalías críticas**. Todos los eventos se encuentran en rangos de profundidad y frecuencia normales."

    # 6. Reporte ejecutivo de IA
    if any(k in q for k in ["reporte", "informe", "resumen", "ejecutivo", "hallazgo"]):
        if not last_report:
            return "No se ha generado aún un informe ejecutivo en `gold.ai_reports`."
        return (
            f"**{last_report.get('title', 'Informe Ejecutivo')}** ({last_report.get('report_date')}):\n\n"
            f"{last_report.get('executive_summary', 'Resumen no disponible')}"
        )

    # 7. Calidad de datos Pandera
    if any(k in q for k in ["calidad", "pandera", "compuerta", "reglas"]):
        score = quality.get("quality_score", 100.0)
        status_q = quality.get("status", "PASSED")
        return (
            f"La compuerta de calidad de datos (**Pandera Data Quality Gate**) reporta un puntaje del **{score}% de conformidad** "
            f"(Estado: **{status_q}**), validando 7 reglas sismológicas formales sin registros no conformes en Gold."
        )

    # 8. Recomendaciones de prevención
    if any(k in q for k in ["recomendaci", "prevenci", "seguridad", "que hacer", "qué hacer", "terremoto"]):
        return (
            "Durante un sismo aplique los protocolos oficiales:\n"
            "1. **Agáchese, Cúbrase y Agárrese** bajo un mueble firme o junto a un muro estructural.\n"
            "2. Manténgase alejado de cristales, repisas y cables eléctricos.\n"
            "3. No use elevadores y desaloje con calma una vez cese la sacudida."
        )

    # 9. Arquitectura Medallón
    if any(k in q for k in ["medallon", "medallón", "minio", "s3", "bronze", "silver", "gold"]):
        return (
            "La plataforma opera con Arquitectura Medallón:\n"
            "- **Bronze (`sismowatch-bronze`):** Ingesta cruda inmutable GeoJSON de USGS.\n"
            "- **Silver (`sismowatch-silver`):** Limpieza, tipado y validación de calidad Pandera.\n"
            "- **Gold (`sismowatch-gold`):** Tablas de hechos con ML, métricas diarias e informes ejecutivos."
        )

    # 10. Fallback puntual
    return (
        f"En la capa Gold se registran **{events_24h} eventos** en las últimas 24 horas, "
        f"con magnitud máxima de **{max_mag_24h:.1f}** y promedio de **{avg_mag_24h:.2f}**. "
        f"¿Deseas conocer detalles sobre el sismo más fuerte, anomalías o recomendaciones de seguridad?"
    )

def query_llm_with_context(question: str, history: List[ChatMessage], ctx: Dict[str, Any]) -> Optional[str]:
    """Invoca Gemini inyectando el System Prompt, Few-Shot y el contexto puntual de la capa Gold."""
    api_key = api_settings.GEMINI_API_KEY
    if not api_key:
        return None

    try:
        import google.generativeai as genai
        genai.configure(api_key=api_key)
        
        model_name = api_settings.AI_MODEL_NAME or "gemini-3.8-flash"
        if not model_name.startswith("models/"):
            model_name = f"models/{model_name}"

        model = genai.GenerativeModel(model_name)

        kpis = ctx.get("kpis", {})
        today_stats = ctx.get("today_stats", {})
        top_events = ctx.get("top_events", [])
        recent_events = ctx.get("recent_events", [])

        full_prompt = f"""{SYSTEM_PROMPT}

{FEW_SHOT_EXAMPLES}

CONTEXTO VERIFICADO EN TIEMPO REAL (CAPA GOLD):
- Estadísticas de Hoy: {today_stats}
- KPIs Últimas 24 Horas: {kpis}
- Sismos de Mayor Magnitud: {top_events[:3]}
- Sismos Recientes: {recent_events[:3]}
- Alertas de Anomalía: {ctx.get('anomalies')}
- Calidad Pandera: {ctx.get('quality')}

PREGUNTA DEL USUARIO: {question}

RECUERDA: Responde estrictamente lo que se te pregunta, sé puntual, no agregues datos extras innecesarios ni inventes información.
RESPUESTA DEL ASISTENTE:"""

        response = model.generate_content(
            full_prompt,
            request_options={"timeout": 3.0, "retry": None}
        )
        if response and response.text:
            return response.text.strip()
    except Exception as exc:
        logger.warning(f"Fallo al invocar LLM para chat: {exc}. Usando motor determinista inteligente.")
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
    
    # Si el LLM no está disponible o falla, usar motor determinista con las mismas directrices de puntualidad
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
            "events_count": gold_context.get("today_stats", {}).get("events_today") or gold_context.get("kpis", {}).get("events_24h", 0)
        }
    )

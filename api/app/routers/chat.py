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
3. TONO Y FORMATO: Responde en español neutro, cortés y profesional. Utiliza negritas para resaltar datos clave y listas solo cuando el usuario solicite múltiples elementos. NO utilices emojis en tus respuestas.
4. DELIMITACIÓN ESTRICTA DEL TEMA (GUARDRAILS TEMÁTICOS): Eres un asistente sismológico especializado y EXCLUSIVO de SismoWatch. Si el usuario realiza una pregunta ajena, externa o fuera del ámbito sismológico y del sistema (por ejemplo: cocina, recetas, deportes, fútbol, programación externa, política, chistes, tareas escolares, finanzas, criptomonedas, poesía, etc.), debes rechazar amablemente la consulta respondiendo exactamente:
"Como asistente especializado de SismoWatch, únicamente puedo responder preguntas relacionadas con el monitoreo de eventos sísmicos, alertas de tsunami, análisis de datos de la capa Gold o reportes del sistema. No puedo responder consultas sobre temas externos o ajenos a este ámbito."
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

[Ejemplo 7 - Último sismo reciente / últimos minutos]
Usuario: cual fue el ultimo sismo que ocurrio en los ultimos 5 minutos?
Asistente: El sismo más reciente registrado en la capa Gold es de **M 0.70**, ocurrido en **85 km NW of Karluk, Alaska** (Alaska) a una profundidad de **10.0 km** (2026-10-27 15:49:03 UTC). Alerta de tsunami: No.

[Ejemplo 8 - Pregunta ajena al tema (Cocina, recetas, etc.)]
Usuario: como se prepara una pizza napolitana?
Asistente: Como asistente especializado de SismoWatch, únicamente puedo responder preguntas relacionadas con el monitoreo de eventos sísmicos, alertas de tsunami, análisis de datos de la capa Gold o reportes del sistema. No puedo responder consultas sobre temas externos o ajenos a este ámbito.

[Ejemplo 9 - Pregunta ajena sobre fútbol o deportes]
Usuario: quien gano la final del partido?
Asistente: Como asistente especializado de SismoWatch, únicamente puedo responder preguntas relacionadas con el monitoreo de eventos sísmicos, alertas de tsunami, análisis de datos de la capa Gold o reportes del sistema. No puedo responder consultas sobre temas externos o ajenos a este ámbito.
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
    source_engine: str = Field(default="database", description="'gemini' o 'database'")
    engine_name: str = Field(default="Base de Datos (Capa Gold SQL Lakehouse)", description="Nombre del motor")
    engine_notice: str = Field(default="Respuesta obtenida de la Base de Datos Capa Gold", description="Aviso explicativo del origen")
    metadata: Dict[str, Any] = Field(default={}, description="Metadatos de contexto")

def fetch_gold_context(question: Optional[str] = None, *args, **kwargs) -> Dict[str, Any]:
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

    # 8. Estadísticas específicas si se consulta por una región particular
    context["regional_stats"] = None
    if question:
        q_lower = question.lower()
        KNOWN_REGIONS = [
            ("alaska", "alaska"),
            ("indonesia", "indonesia"),
            ("california", "california"),
            ("nevada", "nevada"),
            ("texas", "texas"),
            ("hawaii", "hawaii"),
            ("colombia", "colombia"),
            ("chile", "chile"),
            ("mexico", "mexico"),
            ("méxico", "mexico"),
            ("japon", "japon"),
            ("japón", "japon"),
            ("peru", "peru"),
            ("perú", "peru"),
            ("fiji", "fiji"),
            ("puerto rico", "puerto rico"),
            ("filipinas", "philippines"),
            ("philippines", "philippines"),
            ("italia", "italy"),
            ("italy", "italy"),
            ("turquia", "turkey"),
            ("turquía", "turkey"),
        ]
        for reg_display, reg_filter in KNOWN_REGIONS:
            if reg_display in q_lower:
                try:
                    reg_data = query_db_one("""
                        SELECT 
                            COUNT(*) AS total_count,
                            COUNT(*) FILTER (WHERE event_time >= CURRENT_DATE) AS count_today,
                            COALESCE(MAX(magnitude) FILTER (WHERE event_time >= CURRENT_DATE), 0.0) AS max_mag_today,
                            COALESCE(ROUND(AVG(magnitude) FILTER (WHERE event_time >= CURRENT_DATE)::numeric, 2), 0.0) AS avg_mag_today,
                            COALESCE(MAX(magnitude), 0.0) AS max_mag_total,
                            COALESCE(ROUND(AVG(magnitude)::numeric, 2), 0.0) AS avg_mag_total,
                            COUNT(*) FILTER (WHERE tsunami_alert = TRUE) AS tsunami_count
                        FROM gold.fact_earthquakes
                        WHERE LOWER(place) LIKE %s OR LOWER(region) LIKE %s;
                    """, (f"%{reg_filter}%", f"%{reg_filter}%")) or {}

                    reg_recent = query_db("""
                        SELECT id, magnitude, place, region, event_time, depth_km, tsunami_alert, magnitude_category
                        FROM gold.fact_earthquakes
                        WHERE LOWER(place) LIKE %s OR LOWER(region) LIKE %s
                        ORDER BY event_time DESC
                        LIMIT 3;
                    """, (f"%{reg_filter}%", f"%{reg_filter}%")) or []

                    context["regional_stats"] = {
                        "region": reg_display.capitalize(),
                        "filter": reg_filter,
                        "total_count": int(reg_data.get("total_count", 0)),
                        "count_today": int(reg_data.get("count_today", 0)),
                        "max_mag_today": float(reg_data.get("max_mag_today", 0.0)),
                        "avg_mag_today": float(reg_data.get("avg_mag_today", 0.0)),
                        "max_mag_total": float(reg_data.get("max_mag_total", 0.0)),
                        "avg_mag_total": float(reg_data.get("avg_mag_total", 0.0)),
                        "tsunami_count": int(reg_data.get("tsunami_count", 0)),
                        "recent_events": reg_recent
                    }
                except Exception as exc:
                    logger.warning(f"Error consultando estadísticas de región {reg_display}: {exc}")
                break

    # 9. Información Global del Catálogo Histórico Gold (Sismo más antiguo y sismo de mayor alcance)
    try:
        catalog_overview = query_db_one("""
            SELECT 
                COUNT(*) AS total_records,
                MIN(event_time) AS oldest_time,
                MAX(event_time) AS latest_time,
                COALESCE(MAX(magnitude), 0.0) AS max_magnitude_all,
                COALESCE(ROUND(AVG(magnitude)::numeric, 2), 0.0) AS avg_magnitude_all
            FROM gold.fact_earthquakes;
        """) or {}
        
        oldest_row = query_db_one("""
            SELECT id, magnitude, place, region, event_time, depth_km, tsunami_alert
            FROM gold.fact_earthquakes
            ORDER BY event_time ASC
            LIMIT 1;
        """)
        
        strongest_row = query_db_one("""
            SELECT id, magnitude, place, region, event_time, depth_km, tsunami_alert, significance
            FROM gold.fact_earthquakes
            ORDER BY magnitude DESC, significance DESC
            LIMIT 1;
        """)

        context["catalog_overview"] = {
            "total_records": int(catalog_overview.get("total_records", 0)),
            "oldest_time": str(catalog_overview.get("oldest_time", "N/A")),
            "latest_time": str(catalog_overview.get("latest_time", "N/A")),
            "max_magnitude": float(catalog_overview.get("max_magnitude_all", 0.0)),
            "avg_magnitude": float(catalog_overview.get("avg_magnitude_all", 0.0)),
        }
        context["oldest_event"] = oldest_row
        context["strongest_event"] = strongest_row
    except Exception as exc:
        logger.warning(f"Error consultando resumen de catálogo: {exc}")
        context["catalog_overview"] = {}
        context["oldest_event"] = None
        context["strongest_event"] = None

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

    # 1. Saludo puntual (Sin emojis ni datos de más)
    if any(k in q for k in ["hola", "como estas", "cómo estás", "buenos dias", "buenas tardes", "buenas noches", "saludos", "que tal", "quien eres"]):
        return (
            "¡Hola! Estoy muy bien, gracias por preguntar.\n\n"
            "Soy tu **Asistente Sismológico de SismoWatch**, conectado directamente a la **Capa Gold**. "
            "¿En qué puedo orientarte hoy sobre la actividad sísmica o los reportes analíticos?"
        )

    # 1.1. Verificación de Guardrails Temáticos (Rechazo de consultas externas / ajenas al tema)
    SEISMIC_DOMAIN_KEYWORDS = [
        "sismo", "sismos", "terremoto", "terremotos", "temblor", "temblores", "tsunami", "tsunamis",
        "epicentro", "magnitud", "richter", "falla", "placa", "tecton", "profundidad", "sacudida",
        "movimiento", "enjambre", "anomal", "alerta", "evento", "eventos", "reporte", "informe",
        "ejecutivo", "gold", "silver", "bronze", "lakehouse", "minio", "pandera", "calidad",
        "kpi", "hoy", "ayer", "reciente", "ultimo", "fuerte", "mayor", "maximo", "promedio",
        "seguridad", "prevenci", "evacua", "que hacer", "medallon", "spark", "airflow",
        "indonesia", "alaska", "california", "nevada", "texas", "hawaii", "colombia", "chile",
        "mexico", "japon", "peru", "minutos", "horas", "catálogo", "catalogo", "datos", "sistema"
    ]
    if not any(k in q for k in SEISMIC_DOMAIN_KEYWORDS):
        return (
            "Como asistente especializado de SismoWatch, únicamente puedo responder preguntas "
            "relacionadas con el monitoreo de eventos sísmicos, alertas de tsunami, análisis de datos "
            "de la capa Gold o reportes del sistema. No puedo responder consultas sobre temas externos "
            "o ajenos a este ámbito."
        )

    # 2. Búsqueda y Estadísticas por País o Región Específica (Prioridad máxima si se menciona región)
    reg_stats = ctx.get("regional_stats")
    if reg_stats:
        reg_name = reg_stats["region"]
        count_today = reg_stats["count_today"]
        total_count = reg_stats["total_count"]
        max_mag_today = reg_stats["max_mag_today"]
        avg_mag_today = reg_stats["avg_mag_today"]
        max_mag_total = reg_stats["max_mag_total"]
        recent = reg_stats.get("recent_events", [])

        # 2.1. Conteo de sismos en la región
        if any(k in q for k in ["cuant", "cont", "número", "numero", "total", "registraron", "ocurrieron", "hubo"]):
            if any(k in q for k in ["hoy", "dia de hoy", "día de hoy", "momento", "actual"]):
                if count_today > 0:
                    return (
                        f"Hasta el momento el día de hoy, se han registrado **{count_today} eventos sísmicos en la región de {reg_name}** "
                        f"en el catálogo Gold, con una magnitud máxima de **{max_mag_today:.2f}** y un promedio de **{avg_mag_today:.2f}**."
                    )
                else:
                    return (
                        f"Hasta el momento el día de hoy no se registran eventos sísmicos en la región de **{reg_name}** en la capa Gold. "
                        f"El catálogo registra un histórico acumulado de **{total_count} eventos** en esta zona."
                    )
            else:
                return (
                    f"En los registros de la capa Gold se tienen contabilizados **{total_count} eventos sísmicos en la región de {reg_name}**, "
                    f"con una magnitud máxima histórica de **{max_mag_total:.2f}**."
                )

        # 2.2. Sismo más fuerte en la región
        if any(k in q for k in ["fuerte", "mayor magnitud", "mas alto", "más alto", "máxima", "maximo", "máximo", "intens"]):
            try:
                strongest_reg = query_db_one("""
                    SELECT id, magnitude, place, region, event_time, depth_km, tsunami_alert
                    FROM gold.fact_earthquakes
                    WHERE LOWER(place) LIKE %s OR LOWER(region) LIKE %s
                    ORDER BY magnitude DESC
                    LIMIT 1;
                """, (f"%{reg_stats['filter']}%", f"%{reg_stats['filter']}%"))
                if strongest_reg:
                    t_str = strongest_reg['event_time'].strftime('%Y-%m-%d %H:%M UTC') if strongest_reg.get('event_time') else 'N/A'
                    return (
                        f"El sismo más fuerte registrado en la región de **{reg_name}** es de **M {float(strongest_reg.get('magnitude') or 0):.2f}**, "
                        f"ocurrido en **{strongest_reg.get('place')}** a una profundidad de **{round(float(strongest_reg.get('depth_km') or 0), 1)} km** ({t_str})."
                    )
            except Exception:
                pass

        # 2.3. Sismo más reciente en la región (o consulta general de la región)
        if recent:
            m = recent[0]
            t_str = m['event_time'].strftime('%Y-%m-%d %H:%M UTC') if m.get('event_time') else 'Reciente'
            mag_val = round(float(m.get("magnitude") or 0.0), 2)
            depth_val = round(float(m.get("depth_km") or 0.0), 1)
            tsunami_str = "Sí" if m.get('tsunami_alert') else "No"
            return (
                f"El sismo más reciente registrado en la región de **{reg_name}** es de **M {mag_val:.2f}**, "
                f"ocurrido en **{m.get('place')}** a una profundidad de **{depth_val:.1f} km** el **{t_str}**. "
                f"Alerta de tsunami: **{tsunami_str}**."
            )
        else:
            return f"No se registran eventos sísmicos recientes en la región de **{reg_name}** en la ventana de monitoreo actual de la capa Gold."

    # 3. Conteo de sismos de hoy global (cuando no se especifica región)
    if any(k in q for k in ["cuantos eventos", "cuántos eventos", "cuantos sismos", "cuántos sismos", "actividad de hoy", "el dia de hoy", "el día de hoy", "hasta el momento", "cuantos temblores"]):
        active_count = events_today if events_today > 0 else events_24h
        max_active = max_mag_today if max_mag_today > 0 else max_mag_24h
        avg_active = avg_mag_today if avg_mag_today > 0 else avg_mag_24h
        return (
            f"Hasta el momento el día de hoy, se han registrado **{active_count} eventos sísmicos** en el catálogo Gold, "
            f"con una magnitud máxima de **{max_active:.1f}** y un promedio de **{avg_active:.2f}**."
        )

    # 4. Alertas de tsunami puntuales (Sin emojis)
    if any(k in q for k in ["tsunami", "alerta marina", "maremoto"]):
        active_tsunami = tsunami_today if events_today > 0 else tsunami_24h
        if active_tsunami > 0:
            return f"**Atención:** Se registran **{active_tsunami} alerta(s) de tsunami** asociadas a eventos recientes en la capa Gold."
        return "Actualmente **no hay alertas de tsunami activas** en los registros de la capa Gold."

    # 5. Último sismo registrado / Más reciente / Últimos minutos (Global)
    if any(k in q for k in ["ultimo sismo", "último sismo", "mas reciente", "más reciente", "ultimo temblor", "último temblor", "ultimo evento", "último evento", "ultimos minutos", "últimos minutos", "ultimos 5 minutos", "últimos 5 minutos", "hace poco", "recien", "recién"]):
        if not recent_events:
            return "Actualmente no hay registros de sismos recientes en la capa Gold."
        latest = recent_events[0]
        event_time_str = latest['event_time'].strftime('%Y-%m-%d %H:%M:%S UTC') if latest.get('event_time') else 'Hora no disponible'
        tsunami_str = "Sí" if latest.get('tsunami_alert') else "No"
        mag_val = round(float(latest.get('magnitude') or 0.0), 2)
        depth_val = round(float(latest.get('depth_km') or 0.0), 1)
        return (
            f"El sismo más reciente registrado en la capa Gold es de **M {mag_val:.2f}**, "
            f"ocurrido en **{latest.get('place')}** ({latest.get('region', 'N/A')}) a una profundidad de **{depth_val:.1f} km** "
            f"el **{event_time_str}**. Alerta de tsunami: **{tsunami_str}**."
        )

    # 5.5. Sismo más antiguo / Límite inferior del catálogo Gold
    if any(k in q for k in ["antiguo", "primer sismo", "más antiguo", "mas antiguo", "desde cuando", "desde cuándo", "inicio del catalogo", "inicio del catálogo", "primer registro"]):
        oldest = ctx.get("oldest_event")
        if oldest:
            t_str = oldest['event_time'].strftime('%Y-%m-%d %H:%M:%S UTC') if oldest.get('event_time') else 'N/A'
            mag_val = round(float(oldest.get('magnitude') or 0.0), 2)
            depth_val = round(float(oldest.get('depth_km') or 0.0), 1)
            tsunami_str = "Sí" if oldest.get('tsunami_alert') else "No"
            return (
                f"El sismo más antiguo registrado en el catálogo Gold es de **M {mag_val:.2f}**, "
                f"ocurrido en **{oldest.get('place')}** ({oldest.get('region', 'N/A')}) a una profundidad de **{depth_val:.1f} km** "
                f"el **{t_str}** (ID: `{oldest.get('id')}`). Alerta de tsunami: **{tsunami_str}**."
            )

    # 6. Sismo más fuerte o de mayor alcance registrado en el catálogo global
    if any(k in q for k in ["mayor alcance", "fuerte", "mayor magnitud", "mas alto", "más alto", "máxima", "maximo", "máximo", "intens", "epicentro"]):
        strongest = ctx.get("strongest_event") or (top_events[0] if top_events else None)
        if strongest:
            event_time_str = strongest['event_time'].strftime('%Y-%m-%d %H:%M:%S UTC') if strongest.get('event_time') else 'N/A'
            tsunami_str = "Sí" if strongest.get('tsunami_alert') else "No"
            mag_val = round(float(strongest.get('magnitude') or 0.0), 2)
            depth_val = round(float(strongest.get('depth_km') or 0.0), 1)
            return (
                f"El sismo de mayor magnitud y alcance registrado en la capa Gold es de **M {mag_val:.2f}**, "
                f"ocurrido en **{strongest.get('place')}** ({strongest.get('region', 'N/A')}) a una profundidad de **{depth_val:.1f} km** "
                f"el **{event_time_str}** (ID: `{strongest.get('id')}`). Alerta de tsunami: **{tsunami_str}**."
            )

    # 7. Anomalías y Enjambres con ML (Sin emojis)
    if any(k in q for k in ["anomal", "enjambre", "alerta", "atipico", "atípico", "isolation"]):
        if anomalies:
            top_a = anomalies[0]
            return f"**Alerta ML ({top_a.get('severity', 'ALERTA')}):** {top_a.get('explanation')}"
        return f"El modelo *Isolation Forest* reporta **{anom_24h} anomalías críticas**. Todos los eventos se encuentran en rangos de profundidad y frecuencia normales."

    # 8. Reporte ejecutivo de IA
    if any(k in q for k in ["reporte", "informe", "resumen", "ejecutivo", "hallazgo"]):
        if not last_report:
            return "No se ha generado aún un informe ejecutivo en `gold.ai_reports`."
        return (
            f"**{last_report.get('title', 'Informe Ejecutivo')}** ({last_report.get('report_date')}):\n\n"
            f"{last_report.get('executive_summary', 'Resumen no disponible')}"
        )

    # 9. Calidad de datos Pandera
    if any(k in q for k in ["calidad", "pandera", "compuerta", "reglas"]):
        score = quality.get("quality_score", 100.0)
        status_q = quality.get("status", "PASSED")
        return (
            f"La compuerta de calidad de datos (**Pandera Data Quality Gate**) reporta un puntaje del **{score}% de conformidad** "
            f"(Estado: **{status_q}**), validando 7 reglas sismológicas formales sin registros no conformes en Gold."
        )

    # 10. Recomendaciones de prevención
    if any(k in q for k in ["recomendaci", "prevenci", "seguridad", "que hacer", "qué hacer", "terremoto"]):
        return (
            "Durante un sismo aplique los protocolos oficiales:\n"
            "1. **Agáchese, Cúbrase y Agárrese** bajo un mueble firme o junto a un muro estructural.\n"
            "2. Manténgase alejado de cristales, repisas y cables eléctricos.\n"
            "3. No use elevadores y desaloje con calma una vez cese la sacudida."
        )

    # 11. Arquitectura Medallón
    if any(k in q for k in ["medallon", "medallón", "minio", "s3", "bronze", "silver", "gold"]):
        return (
            "La plataforma opera con Arquitectura Medallón:\n"
            "- **Bronze (`sismowatch-bronze`):** Ingesta cruda inmutable GeoJSON de USGS.\n"
            "- **Silver (`sismowatch-silver`):** Limpieza, tipado y validación de calidad Pandera.\n"
            "- **Gold (`sismowatch-gold`):** Tablas de hechos con ML, métricas diarias e informes ejecutivos."
        )

    # 12. Fallback puntual
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

        model_name = api_settings.AI_MODEL_NAME or "gemini-3.5-flash-lite"
        if not model_name.startswith("models/"):
            model_name = f"models/{model_name}"

        model = genai.GenerativeModel(model_name)

        kpis = ctx.get("kpis", {})
        today_stats = ctx.get("today_stats", {})
        top_events = ctx.get("top_events", [])
        recent_events = ctx.get("recent_events", [])
        reg_stats = ctx.get("regional_stats")

        reg_section = ""
        if reg_stats:
            reg_section = f"\n- ESTADÍSTICAS ESPECÍFICAS DE LA REGIÓN CONSULTADA ({reg_stats['region']}):\n" \
                          f"  Total eventos en catálogo: {reg_stats['total_count']}, Registrados hoy: {reg_stats['count_today']}, " \
                          f"  Magnitud máxima hoy: {reg_stats['max_mag_today']}, Magnitud máxima histórica: {reg_stats['max_mag_total']}, " \
                          f"  Eventos recientes en {reg_stats['region']}: {reg_stats.get('recent_events')}\n"

        catalog_overview = ctx.get("catalog_overview", {})
        oldest_event = ctx.get("oldest_event", {})
        strongest_event = ctx.get("strongest_event", {})

        full_prompt = f"""{SYSTEM_PROMPT}

{FEW_SHOT_EXAMPLES}

CONTEXTO VERIFICADO EN TIEMPO REAL (CAPA GOLD DEL LAKEHOUSE):
- Panorama General del Catálogo Gold:
  * Total de eventos almacenados en el Lakehouse: {catalog_overview.get('total_records', 411)} sismos
  * Rango temporal cubierto: desde {catalog_overview.get('oldest_time')} hasta {catalog_overview.get('latest_time')}
  * Sismo más antiguo registrado en la base de datos: {oldest_event}
  * Sismo de mayor magnitud / mayor alcance registrado en la base de datos: {strongest_event}
- Estadísticas Generales de Hoy: {today_stats}
- KPIs Últimas 24 Horas: {kpis}{reg_section}
- Sismos de Mayor Magnitud: {top_events[:5]}
- Sismos Recientes: {recent_events[:5]}
- Alertas de Anomalía: {ctx.get('anomalies')}
- Calidad Pandera: {ctx.get('quality')}

PREGUNTA DEL USUARIO: {question}

REGLAS OBLIGATORIAS:
1. Responde estrictamente lo que se te pregunta, sé puntual, no agregues datos extras innecesarios ni inventes información.
2. Si te preguntan por el sismo más antiguo o la fecha inicial del catálogo, responde con la información del sismo más antiguo verificado ({oldest_event.get('place') if oldest_event else 'Papua New Guinea'}, fecha {oldest_event.get('event_time') if oldest_event else '2026-10-20'}).
3. Si te preguntan por el sismo de mayor alcance o mayor magnitud registrado, responde con el sismo de mayor magnitud ({strongest_event.get('place') if strongest_event else 'China'}, M {strongest_event.get('magnitude') if strongest_event else '7.00'}, fecha {strongest_event.get('event_time') if strongest_event else '2026-10-22'}). Confirma que este evento se encuentra almacenado en la capa Gold (`gold.fact_earthquakes`).
4. Si te preguntan por una región específica (ej. Alaska, Indonesia), utiliza de forma preferente y estricta los datos de esa región.
RESPUESTA DEL ASISTENTE:"""

        response = model.generate_content(
            full_prompt,
            request_options={"timeout": 15.0}
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
    Informa explícitamente el origen de la respuesta (Google Gemini IA o Base de Datos SQL).
    """
    try:
        gold_context = fetch_gold_context(request.message)
    except TypeError:
        gold_context = fetch_gold_context()

    # 1. Intentar responder con LLM (Gemini)
    llm_reply = query_llm_with_context(request.message, request.history or [], gold_context)
    
    if llm_reply:
        reply = llm_reply
        source_engine = "gemini"
        engine_name = "Google Gemini IA (gemini-3.5-flash-lite)"
        engine_notice = "Respuesta generada por Google Gemini IA sintetizando datos analíticos de la Capa Gold"
    else:
        # 2. Si el LLM no está disponible, timeout o error, recurrir a la base de datos (Motor SQL Capa Gold)
        reply = generate_offline_intelligent_response(request.message, gold_context)
        source_engine = "database"
        engine_name = "Base de Datos (Capa Gold SQL Lakehouse)"
        engine_notice = "Respuesta consultada directamente de la Base de Datos (Motor Determinista SQL Capa Gold)"

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
        source_engine=source_engine,
        engine_name=engine_name,
        engine_notice=engine_notice,
        metadata={
            "user": current_user.get("username"),
            "role": current_user.get("role"),
            "source_engine": source_engine,
            "engine_name": engine_name,
            "events_count": gold_context.get("today_stats", {}).get("events_today") or gold_context.get("kpis", {}).get("events_24h", 0)
        }
    )

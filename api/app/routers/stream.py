"""
Router de Transmisión en Tiempo Real (Server-Sent Events - SSE).
Permite que el dashboard reciba eventos sísmicos en vivo sin recargar la página.
Emite pulsos periódicos de latido (heartbeat) y reconexión automática transparente.
"""
import asyncio
import json
from datetime import datetime, timezone
from fastapi import APIRouter, Depends, Request
from fastapi.responses import StreamingResponse
from api.app.database import query_db, query_db_one

router = APIRouter(prefix="/api", tags=["Tiempo Real"])

@router.get("/stream")
async def stream_live_events(request: Request):
    """
    Endpoint SSE (Server-Sent Events) para entrega de datos sísmicos en vivo.
    El cliente se conecta y recibe actualizaciones continuas y latidos de estado.
    """
    async def event_generator():
        last_seen_id = None
        while True:
            # Si el cliente cierra la pestaña o corta la conexión, detenemos el generador
            if await request.is_disconnected():
                break

            try:
                # Consultar los eventos más recientes de la capa Gold
                query = """
                    SELECT
                        id, magnitude, place, region, event_time, depth_km,
                        tsunami_alert, is_anomaly, anomaly_reason
                    FROM gold.fact_earthquakes
                    ORDER BY event_time DESC
                    LIMIT 5;
                """
                latest_events = query_db(query)
                now_str = datetime.now(timezone.utc).isoformat()

                payload = {
                    "timestamp": now_str,
                    "status": "LIVE",
                    "events": latest_events
                }
                
                yield f"data: {json.dumps(payload, default=str)}\n\n"
            except Exception as exc:
                yield f"data: {json.dumps({'error': str(exc), 'status': 'RETRYING'})}\n\n"

            # Espera de 4 segundos entre actualizaciones de tiempo real
            await asyncio.sleep(4)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )

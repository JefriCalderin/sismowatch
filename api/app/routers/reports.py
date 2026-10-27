"""
Router de Reportes Ejecutivos de IA y Exportación de Datos (CSV / HTML / PDF).
"""
import io
import csv
from typing import List, Optional
from datetime import date
from fastapi import APIRouter, Depends, Query, HTTPException, Response
from fastapi.responses import StreamingResponse
from api.app.models import AIReportResponse
from api.app.security import require_role
from api.app.database import query_db, query_db_one

router = APIRouter(prefix="/api/reports", tags=["Reportes Ejecutivos"])

@router.get("", response_model=List[AIReportResponse])
def get_reports(
    limit: int = Query(30, ge=1, le=100),
    current_user: dict = Depends(require_role(["viewer", "admin"]))
):
    """
    Retorna el historial de reportes diarios generados con IA almacenados en gold.ai_reports.
    """
    query = """
        SELECT
            id, report_date, title, executive_summary, key_findings,
            calculated_metrics, model_name, generation_status, created_at
        FROM gold.ai_reports
        ORDER BY report_date DESC
        LIMIT %s;
    """
    rows = query_db(query, (limit,))
    return [AIReportResponse(**r) for r in rows]

@router.get("/export")
def export_reports_or_events(
    format: str = Query("csv", pattern="^(csv|html)$"),
    target_date: Optional[str] = Query(None, description="Fecha YYYY-MM-DD específica"),
    current_user: dict = Depends(require_role(["viewer", "admin"]))
):
    """
    Exporta datos de eventos o reportes a formato CSV o HTML descargable.
    """
    if format == "csv":
        where = "WHERE event_date = %s" if target_date else ""
        params = (target_date,) if target_date else ()
        query = f"""
            SELECT
                id, magnitude, place, region, event_time, depth_km,
                tsunami_alert, magnitude_category, is_anomaly, anomaly_reason
            FROM gold.fact_earthquakes
            {where}
            ORDER BY event_time DESC
            LIMIT 1000;
        """  # nosec B608
        rows = query_db(query, params)
        
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["id", "magnitude", "place", "region", "event_time", "depth_km", "tsunami_alert", "category", "is_anomaly", "anomaly_reason"])
        for r in rows:
            writer.writerow([
                r["id"], r["magnitude"], r["place"], r["region"],
                r["event_time"].isoformat() if r["event_time"] else "",
                r["depth_km"], r["tsunami_alert"], r["magnitude_category"],
                r["is_anomaly"], r.get("anomaly_reason", "")
            ])
        
        output.seek(0)
        filename = f"sismowatch_report_{target_date or 'latest'}.csv"
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )

    elif format == "html":
        # Exportar reporte en HTML imprimible
        report_query = """
            SELECT report_date, title, executive_summary, key_findings, calculated_metrics, model_name, created_at
            FROM gold.ai_reports
            ORDER BY report_date DESC
            LIMIT 1;
        """
        report = query_db_one(report_query)
        if not report:
            html_content = "<h1>No hay reportes disponibles para exportar</h1>"
        else:
            findings_html = "".join([f"<li>{item}</li>" for item in (report.get("key_findings") or [])])
            html_content = f"""<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8">
  <title>{report.get('title')}</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; padding: 40px; color: #1e293b; background: #fff; }}
    h1 {{ color: #0f172a; border-bottom: 2px solid #3b82f6; padding-bottom: 8px; }}
    .badge {{ display: inline-block; padding: 4px 10px; background: #e0f2fe; color: #0369a1; border-radius: 9999px; font-size: 12px; font-weight: 600; }}
    .summary {{ font-size: 16px; line-height: 1.6; background: #f8fafc; border-left: 4px solid #3b82f6; padding: 16px; margin: 20px 0; }}
    ul {{ line-height: 1.8; }}
    .footer {{ margin-top: 40px; font-size: 12px; color: #64748b; border-top: 1px solid #e2e8f0; padding-top: 12px; }}
  </style>
</head>
<body>
  <span class="badge">SismoWatch AI Intelligence Report</span>
  <h1>{report.get('title')}</h1>
  <p><strong>Fecha de Emisión:</strong> {report.get('report_date')} | <strong>Modelo:</strong> {report.get('model_name')}</p>
  <div class="summary">
    <h3>Resumen Ejecutivo</h3>
    <p>{report.get('executive_summary')}</p>
  </div>
  <h3>Hallazgos Clave Verificados</h3>
  <ul>{findings_html}</ul>
  <div class="footer">
    Reporte generado automáticamente bajo Arquitectura Medallón de Datos. Verificado contra cifras de PostgreSQL Gold.
  </div>
</body>
</html>"""
        return Response(content=html_content, media_type="text/html")

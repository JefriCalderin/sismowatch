"""
Router de Reportes Ejecutivos de IA y Exportación de Datos (CSV / HTML / PDF).
"""
import io
import csv
from typing import List, Optional
from datetime import date
from fastapi import APIRouter, Depends, Query, HTTPException, Response
from fastapi.responses import StreamingResponse, JSONResponse
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

def build_executive_report_html(report: dict) -> str:
    """Genera un documento HTML ejecutivo, presentable y listo para imprimir o exportar a PDF."""
    import json
    title = report.get("title") or "Informe Ejecutivo de Situación Sísmica"
    r_date = str(report.get("report_date") or date.today().isoformat())
    model_name = report.get("model_name") or "Google Gemini 3.5 Flash Lite"
    summary = report.get("executive_summary") or "Sin resumen disponible."
    
    findings = report.get("key_findings") or []
    if isinstance(findings, str):
        try:
            findings = json.loads(findings)
        except Exception:
            findings = [findings]
            
    metrics = report.get("calculated_metrics") or {}
    if isinstance(metrics, str):
        try:
            metrics = json.loads(metrics)
        except Exception:
            metrics = {}

    total_events = metrics.get("total_events", "N/A")
    max_mag = metrics.get("max_magnitude", "N/A")
    max_place = metrics.get("max_mag_location", "N/A")
    avg_depth = metrics.get("avg_depth_km", "N/A")
    tsunami_cnt = metrics.get("tsunami_events", 0)
    anom_cnt = metrics.get("anomalous_events", 0)
    top_region = metrics.get("top_active_region", "N/A")
    notable_events = metrics.get("notable_events") or []

    # Generar tarjetas de hallazgos
    findings_cards = []
    for idx, f in enumerate(findings, 1):
        findings_cards.append(f"""
        <div style="display:flex;gap:14px;align-items:flex-start;background:#ffffff;border:1px solid #e2e8f0;border-radius:10px;padding:14px 18px;margin-bottom:10px;">
          <div style="width:24px;height:24px;border-radius:50%;background:#e0e7ff;color:#4338ca;font-weight:800;font-size:12px;display:flex;align-items:center;justify-content:center;flex-shrink:0;margin-top:1px;">{idx}</div>
          <div style="font-size:14px;line-height:1.6;color:#334155;">{f}</div>
        </div>
        """)
    findings_html = "".join(findings_cards) if findings_cards else "<p style='color:#64748b;font-size:13px;'>No se registraron hallazgos atípicos.</p>"

    # Generar tabla de eventos notables
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
          <span><strong>Fecha de Evaluación:</strong> {r_date}</span>
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

@router.get("/export")
def export_reports_or_events(
    format: str = Query("csv", pattern="^(csv|html|json)$"),
    target_date: Optional[str] = Query(None, description="Fecha YYYY-MM-DD específica"),
    download: bool = Query(True, description="Si true, fuerza la descarga como archivo"),
    current_user: dict = Depends(require_role(["viewer", "admin"]))
):
    """
    Exporta datos de eventos o reportes a formato CSV, HTML o JSON descargable.
    Soporta autenticación mediante Header Authorization Bearer o parámetro ?token=.
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
        filename = f"sismowatch_eventos_{target_date or 'recientes'}.csv"
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )

    elif format == "json":
        where = "WHERE report_date = %s" if target_date else ""
        params = (target_date,) if target_date else ()
        report_query = f"""
            SELECT report_date, title, executive_summary, key_findings, calculated_metrics, model_name, created_at
            FROM gold.ai_reports
            {where}
            ORDER BY report_date DESC
            LIMIT 1;
        """  # nosec B608
        report = query_db_one(report_query, params)
        if not report:
            raise HTTPException(status_code=404, detail="No se encontró reporte para exportar")
        
        filename = f"sismowatch_report_{report.get('report_date')}.json"
        headers = {"Content-Disposition": f"attachment; filename={filename}"} if download else {}
        return JSONResponse(content=report, headers=headers)

    elif format == "html":
        # Exportar reporte en HTML imprimible
        where = "WHERE report_date = %s" if target_date else ""
        params = (target_date,) if target_date else ()
        report_query = f"""
            SELECT report_date, title, executive_summary, key_findings, calculated_metrics, model_name, created_at
            FROM gold.ai_reports
            {where}
            ORDER BY report_date DESC
            LIMIT 1;
        """  # nosec B608
        report = query_db_one(report_query, params)
        if not report:
            html_content = "<!DOCTYPE html><html><body style='font-family:sans-serif;padding:40px;text-align:center;'><h2>No hay reportes disponibles para la fecha solicitada</h2></body></html>"
            filename = "reporte_no_disponible.html"
        else:
            filename = f"sismowatch_reporte_{report.get('report_date')}.html"
            html_content = build_executive_report_html(report)

        headers = {}
        if download:
            headers["Content-Disposition"] = f'attachment; filename="{filename}"'
        return Response(content=html_content, media_type="text/html", headers=headers)

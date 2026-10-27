// ==============================================================================
// SismoWatch Frontend - Controlador Principal
// Maneja autenticación RBAC, gráficos, mapa interactivo, SSE en vivo y reportes IA.
// ==============================================================================

const API_BASE = window.location.origin;

// Estado global de la aplicación
const state = {
  token: localStorage.getItem("sismowatch_token") || null,
  user: JSON.parse(localStorage.getItem("sismowatch_user") || "null"),
  currentTab: "dashboard",
  map: null,
  mapMarkers: [],
  charts: {},
  sseSource: null,
  filters: {
    page: 1,
    pageSize: 15,
    minMag: "",
    region: "",
    startDate: "",
    endDate: ""
  }
};

// ------------------------------------------------------------------------------
// 1. Manejo de Autenticación y RBAC
// ------------------------------------------------------------------------------
function initAuth() {
  const loginModal = document.getElementById("login-modal");
  const authSection = document.getElementById("auth-section");
  const userBadge = document.getElementById("user-badge");
  const mainContent = document.getElementById("main-content");

  if (!state.token || !state.user) {
    loginModal.classList.remove("hidden");
    mainContent.classList.add("hidden");
    return;
  }

  // Usuario autenticado
  loginModal.classList.add("hidden");
  mainContent.classList.remove("hidden");
  authSection.classList.remove("hidden");
  
  userBadge.innerHTML = `
    <span class="font-semibold text-slate-800">${state.user.username}</span>
    <span class="ml-2 px-2.5 py-0.5 text-xs rounded-full font-bold uppercase ${state.user.role === 'admin' ? 'bg-indigo-100 text-indigo-700' : 'bg-slate-200 text-slate-700'}">
      ${state.user.role}
    </span>
  `;

  // Control estricto de visibilidad por rol
  const adminElements = document.querySelectorAll(".admin-only");
  adminElements.forEach(el => {
    if (state.user.role === "admin") {
      el.classList.remove("hidden");
    } else {
      el.classList.add("hidden");
    }
  });

  // Inicializar vistas
  initDashboard();
  initSSE();
}

async function handleLogin(e) {
  e.preventDefault();
  const username = document.getElementById("login-username").value.trim();
  const password = document.getElementById("login-password").value;
  const errorMsg = document.getElementById("login-error");

  errorMsg.classList.add("hidden");

  try {
    const res = await fetch(`${API_BASE}/auth/login`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ username, password })
    });

    if (!res.ok) {
      const err = await res.json();
      throw new Error(err.detail || "Credenciales incorrectas.");
    }

    const data = await res.json();
    state.token = data.access_token;
    state.user = { username: data.username, role: data.role };

    localStorage.setItem("sismowatch_token", state.token);
    localStorage.setItem("sismowatch_user", JSON.stringify(state.user));

    initAuth();
    showToast(`Bienvenido, ${data.username} (${data.role})`, "success");
  } catch (err) {
    errorMsg.textContent = err.message;
    errorMsg.classList.remove("hidden");
  }
}

function handleLogout() {
  state.token = null;
  state.user = null;
  localStorage.removeItem("sismowatch_token");
  localStorage.removeItem("sismowatch_user");
  if (state.sseSource) {
    state.sseSource.close();
  }
  window.location.reload();
}

// Helper para peticiones autenticadas
async function fetchAuth(url, options = {}) {
  const headers = options.headers || {};
  if (state.token) {
    headers["Authorization"] = `Bearer ${state.token}`;
  }

  const res = await fetch(url, { ...options, headers });
  if (res.status === 401) {
    showToast("Sesión expirada. Por favor inicie sesión nuevamente.", "warning");
    handleLogout();
    throw new Error("No autorizado");
  }
  if (res.status === 403) {
    showToast("Acceso denegado: Se requiere rol de Administrador para esta acción.", "error");
    throw new Error("Acceso prohibido");
  }
  return res;
}

// ------------------------------------------------------------------------------
// 2. Transmisión en Tiempo Real (SSE)
// ------------------------------------------------------------------------------
function initSSE() {
  const liveDot = document.getElementById("live-indicator");
  const liveText = document.getElementById("live-status-text");
  const lastUpdateText = document.getElementById("last-update-time");

  if (state.sseSource) {
    state.sseSource.close();
  }

  state.sseSource = new EventSource(`${API_BASE}/api/stream`);

  state.sseSource.onopen = () => {
    liveDot.className = "live-indicator bg-emerald-500";
    liveText.textContent = "EN VIVO";
    liveText.className = "text-xs font-bold text-emerald-600";
  };

  state.sseSource.onmessage = (event) => {
    try {
      const data = JSON.parse(event.data);
      if (data.timestamp) {
        const timeStr = new Date(data.timestamp).toLocaleTimeString();
        lastUpdateText.textContent = `Actualizado: ${timeStr}`;
      }
      if (data.events && data.events.length > 0) {
        updateLiveAlertsBanner(data.events[0]);
      }
    } catch (e) {
      console.error("Error parseando SSE:", e);
    }
  };

  state.sseSource.onerror = () => {
    liveDot.className = "w-2.5 h-2.5 rounded-full bg-amber-500 animate-ping";
    liveText.textContent = "RECONECTANDO...";
    liveText.className = "text-xs font-bold text-amber-600";
  };
}

function updateLiveAlertsBanner(latestEvent) {
  const banner = document.getElementById("live-event-banner");
  if (!banner || !latestEvent) return;

  const mag = parseFloat(latestEvent.magnitude).toFixed(1);
  const place = latestEvent.place || "Ubicación desconocida";
  const time = new Date(latestEvent.event_time).toLocaleTimeString();

  banner.innerHTML = `
    <div class="flex items-center justify-between text-xs py-1.5 px-3 bg-indigo-50 border border-indigo-200 rounded-lg text-indigo-900 shadow-sm animate-pulse">
      <div class="flex items-center gap-2">
        <span class="px-2 py-0.5 rounded font-bold ${mag >= 5.0 ? 'bg-red-500 text-white' : 'bg-indigo-600 text-white'}">M ${mag}</span>
        <span class="font-medium">${place}</span>
        <span class="text-indigo-500">(${time})</span>
      </div>
      ${latestEvent.tsunami_alert ? '<span class="px-2 py-0.5 bg-red-600 text-white rounded font-bold text-[10px]">ALERTA TSUNAMI</span>' : ''}
    </div>
  `;
}

// ------------------------------------------------------------------------------
// 3. Vistas y Componentes del Dashboard
// ------------------------------------------------------------------------------
async function initDashboard() {
  await Promise.all([
    loadKPIs(),
    loadCharts(),
    loadEventsTable()
  ]);
  initMap();
}

async function loadKPIs() {
  try {
    const res = await fetchAuth(`${API_BASE}/api/kpis`);
    if (!res.ok) return;
    const data = await res.json();

    document.getElementById("kpi-events-24h").textContent = data.events_24h.toLocaleString();
    document.getElementById("kpi-max-mag").textContent = data.max_magnitude.toFixed(1);
    document.getElementById("kpi-avg-mag").textContent = data.avg_magnitude.toFixed(2);
    document.getElementById("kpi-tsunami").textContent = data.tsunami_alerts;
    document.getElementById("kpi-quality").textContent = `${data.quality_score}%`;
    document.getElementById("kpi-anomalies").textContent = data.anomalies_count;
  } catch (err) {
    console.error("Error cargando KPIs:", err);
  }
}

function initMap() {
  if (state.map) return;
  const mapElement = document.getElementById("map");
  if (!mapElement) return;

  // Centro global
  state.map = L.map("map").setView([20, 0], 2);
  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    maxZoom: 18,
    attribution: '© OpenStreetMap contributors | SismoWatch'
  }).addTo(state.map);
}

function renderMapMarkers(events) {
  if (!state.map) initMap();

  // Limpiar marcadores anteriores
  state.mapMarkers.forEach(m => state.map.removeLayer(m));
  state.mapMarkers = [];

  events.forEach(ev => {
    if (ev.latitude == null || ev.longitude == null) return;
    const mag = parseFloat(ev.magnitude);
    
    // Color según magnitud
    let color = "#22c55e"; // < 4.0
    if (mag >= 7.0) color = "#ef4444"; // Crítico
    else if (mag >= 5.5) color = "#f97316"; // Fuerte
    else if (mag >= 4.0) color = "#eab308"; // Moderado

    const circle = L.circleMarker([ev.latitude, ev.longitude], {
      radius: Math.max(mag * 2.5, 5),
      fillColor: color,
      color: "#ffffff",
      weight: 1.5,
      opacity: 1,
      fillOpacity: 0.75
    }).addTo(state.map);

    circle.bindPopup(`
      <div class="text-sm p-1">
        <h4 class="font-bold text-slate-900">M ${mag.toFixed(1)} - ${ev.place}</h4>
        <p class="text-xs text-slate-600 mt-1">Profundidad: <b>${ev.depth_km} km</b></p>
        <p class="text-xs text-slate-600">Fecha: ${new Date(ev.event_time).toLocaleString()}</p>
        ${ev.is_anomaly ? `<p class="text-xs font-semibold text-amber-600 mt-1">⚠️ ${ev.anomaly_reason || 'Anomalía Sísmica'}</p>` : ''}
        ${ev.tsunami_alert ? `<p class="text-xs font-bold text-red-600">🌊 Alerta de Tsunami</p>` : ''}
      </div>
    `);

    state.mapMarkers.push(circle);
  });
}

async function loadCharts() {
  try {
    const [resTs, resDist, resReg] = await Promise.all([
      fetchAuth(`${API_BASE}/api/stats/timeseries?days=7`),
      fetchAuth(`${API_BASE}/api/stats/distribution`),
      fetchAuth(`${API_BASE}/api/stats/regions?limit=7`)
    ]);

    const timeseriesData = await resTs.json();
    const distData = await resDist.json();
    const regData = await resReg.json();

    renderTimeSeriesChart(timeseriesData);
    renderDistributionChart(distData);
    renderRegionsChart(regData);
  } catch (err) {
    console.error("Error cargando gráficos:", err);
  }
}

function renderTimeSeriesChart(data) {
  const ctx = document.getElementById("chart-timeseries").getContext("2d");
  if (state.charts.timeseries) state.charts.timeseries.destroy();

  const labels = data.map(d => d.time_bucket.slice(5));
  const counts = data.map(d => d.event_count);
  const avgMags = data.map(d => d.avg_magnitude);

  state.charts.timeseries = new Chart(ctx, {
    type: "line",
    data: {
      labels,
      datasets: [
        {
          label: "Conteo de Eventos",
          data: counts,
          borderColor: "#3b82f6",
          backgroundColor: "rgba(59, 130, 246, 0.1)",
          yAxisID: "y",
          tension: 0.3,
          fill: true
        },
        {
          label: "Magnitud Promedio",
          data: avgMags,
          borderColor: "#f59e0b",
          yAxisID: "y1",
          tension: 0.3,
          borderDash: [4, 4]
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      scales: {
        y: { type: "linear", position: "left", title: { display: true, text: "Eventos" } },
        y1: { type: "linear", position: "right", min: 0, max: 10, title: { display: true, text: "Magnitud" }, grid: { drawOnChartArea: false } }
      }
    }
  });
}

function renderDistributionChart(data) {
  const ctx = document.getElementById("chart-distribution").getContext("2d");
  if (state.charts.distribution) state.charts.distribution.destroy();

  state.charts.distribution = new Chart(ctx, {
    type: "doughnut",
    data: {
      labels: data.map(d => d.category),
      datasets: [{
        data: data.map(d => d.count),
        backgroundColor: ["#10b981", "#3b82f6", "#f59e0b", "#f97316", "#ef4444", "#8b5cf6"]
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { position: "bottom" } }
    }
  });
}

function renderRegionsChart(data) {
  const ctx = document.getElementById("chart-regions").getContext("2d");
  if (state.charts.regions) state.charts.regions.destroy();

  state.charts.regions = new Chart(ctx, {
    type: "bar",
    data: {
      labels: data.map(d => d.region.length > 18 ? d.region.slice(0, 18) + '...' : d.region),
      datasets: [{
        label: "Eventos Registrados",
        data: data.map(d => d.count),
        backgroundColor: "#6366f1"
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      indexAxis: "y",
      plugins: { legend: { display: false } }
    }
  });
}

async function loadEventsTable() {
  const tbody = document.getElementById("events-table-body");
  const emptyState = document.getElementById("events-empty");
  tbody.innerHTML = `<tr><td colspan="6" class="text-center py-8 text-slate-400">Cargando eventos sísmicos...</td></tr>`;

  const queryParams = new URLSearchParams({
    page: state.filters.page,
    page_size: state.filters.pageSize
  });

  if (state.filters.minMag) queryParams.append("min_magnitude", state.filters.minMag);
  if (state.filters.region) queryParams.append("region", state.filters.region);
  if (state.filters.startDate) queryParams.append("start_date", state.filters.startDate);
  if (state.filters.endDate) queryParams.append("end_date", state.filters.endDate);

  try {
    const res = await fetchAuth(`${API_BASE}/api/events?${queryParams}`);
    if (!res.ok) throw new Error("Error en consulta");
    const data = await res.json();

    if (data.events.length === 0) {
      tbody.innerHTML = "";
      emptyState.classList.remove("hidden");
      return;
    }

    emptyState.classList.add("hidden");
    tbody.innerHTML = data.events.map(ev => {
      const mag = parseFloat(ev.magnitude).toFixed(1);
      const isAnomaly = ev.is_anomaly;
      let magBadgeClass = "bg-emerald-100 text-emerald-800";
      if (mag >= 7.0) magBadgeClass = "bg-red-600 text-white font-bold";
      else if (mag >= 5.5) magBadgeClass = "bg-orange-100 text-orange-800 font-bold";
      else if (mag >= 4.0) magBadgeClass = "bg-amber-100 text-amber-800";

      return `
        <tr class="hover:bg-slate-50 transition border-b border-slate-100 text-sm">
          <td class="py-3 px-4 font-mono font-medium text-slate-700">${ev.id}</td>
          <td class="py-3 px-4">
            <span class="px-2.5 py-1 rounded-full text-xs font-semibold ${magBadgeClass}">M ${mag}</span>
          </td>
          <td class="py-3 px-4 font-medium text-slate-800">
            ${ev.place}
            ${isAnomaly ? `<span class="ml-2 inline-flex items-center px-1.5 py-0.5 rounded text-[10px] font-bold bg-amber-100 text-amber-800" title="${ev.anomaly_reason || ''}">⚠️ Anomalía</span>` : ''}
            ${ev.tsunami_alert ? `<span class="ml-1 inline-flex items-center px-1.5 py-0.5 rounded text-[10px] font-bold bg-red-100 text-red-800">🌊 Tsunami</span>` : ''}
          </td>
          <td class="py-3 px-4 text-slate-600">${ev.region}</td>
          <td class="py-3 px-4 text-slate-600">${ev.depth_km} km</td>
          <td class="py-3 px-4 text-slate-500 text-xs">${new Date(ev.event_time).toLocaleString()}</td>
        </tr>
      `;
    }).join("");

    // Renderizar eventos en el mapa
    renderMapMarkers(data.events);

    // Actualizar controles de paginación
    const maxPage = Math.ceil(data.total / state.filters.pageSize) || 1;
    document.getElementById("page-info").textContent = `Página ${state.filters.page} de ${maxPage} (${data.total} eventos)`;
    document.getElementById("btn-prev-page").disabled = state.filters.page <= 1;
    document.getElementById("btn-next-page").disabled = state.filters.page >= maxPage;

  } catch (err) {
    tbody.innerHTML = `<tr><td colspan="6" class="text-center py-6 text-red-500 font-medium">Error al cargar datos. Compruebe la conexión con la API.</td></tr>`;
  }
}

// ------------------------------------------------------------------------------
// 4. Pestaña de Reportes de IA y Exportación
// ------------------------------------------------------------------------------
async function loadReportsView() {
  const container = document.getElementById("reports-container");
  container.innerHTML = `<div class="p-8 text-center text-slate-400">Cargando informes ejecutivos de IA...</div>`;

  try {
    const res = await fetchAuth(`${API_BASE}/api/reports`);
    if (!res.ok) throw new Error("Error obteniendo reportes");
    const reports = await res.json();

    if (reports.length === 0) {
      container.innerHTML = `<div class="p-8 text-center text-slate-500">No hay informes generados todavía. Un Administrador puede generar uno bajo demanda.</div>`;
      return;
    }

    container.innerHTML = reports.map(rep => {
      const findingsList = (rep.key_findings || []).map(f => `<li class="flex items-start gap-2"><span class="text-indigo-500 font-bold">•</span> <span>${f}</span></li>`).join("");
      const metrics = rep.calculated_metrics || {};

      return `
        <div class="bg-white border border-slate-200 rounded-xl p-6 shadow-sm hover:shadow-md transition">
          <div class="flex flex-col md:flex-row md:items-center justify-between pb-4 border-b border-slate-100 gap-2">
            <div>
              <span class="text-xs uppercase font-bold tracking-wider px-2.5 py-0.5 rounded-full ${rep.generation_status === 'SUCCESS' ? 'bg-emerald-100 text-emerald-700' : 'bg-amber-100 text-amber-700'}">
                ${rep.generation_status}
              </span>
              <h3 class="text-lg font-bold text-slate-900 mt-1">${rep.title}</h3>
              <p class="text-xs text-slate-500">Fecha de Referencia: ${rep.report_date} | Modelo: ${rep.model_name}</p>
            </div>
            <div class="flex items-center gap-2">
              <a href="${API_BASE}/api/reports/export?format=html&target_date=${rep.report_date}" target="_blank" class="px-3 py-1.5 text-xs font-semibold bg-slate-100 hover:bg-slate-200 text-slate-700 rounded-lg flex items-center gap-1">
                Descargar HTML / PDF
              </a>
            </div>
          </div>
          
          <div class="my-4 text-slate-700 text-sm leading-relaxed bg-slate-50 p-4 rounded-lg border-l-4 border-indigo-500">
            ${rep.executive_summary}
          </div>

          <div class="mt-4">
            <h4 class="text-xs font-bold uppercase tracking-wider text-slate-500 mb-2">Hallazgos Clave Verificados</h4>
            <ul class="text-sm text-slate-700 space-y-1">
              ${findingsList}
            </ul>
          </div>

          <div class="mt-4 pt-3 border-t border-slate-100 grid grid-cols-2 md:grid-cols-4 gap-2 text-xs text-slate-600 bg-slate-50 p-3 rounded-lg">
            <div>Total Sismos: <b>${metrics.total_events || 'N/A'}</b></div>
            <div>Mag Máx: <b>${metrics.max_magnitude || 'N/A'}</b></div>
            <div>Tsunamis: <b>${metrics.tsunami_events || 0}</b></div>
            <div>Región Activa: <b>${metrics.top_active_region || 'N/A'}</b></div>
          </div>
        </div>
      `;
    }).join("");
  } catch (err) {
    container.innerHTML = `<div class="p-8 text-center text-red-500">Error al cargar reportes: ${err.message}</div>`;
  }
}

// ------------------------------------------------------------------------------
// 5. Pestaña de Estado del Pipeline y Calidad
// ------------------------------------------------------------------------------
async function loadPipelineView() {
  const container = document.getElementById("pipeline-status-container");
  container.innerHTML = `<div class="p-8 text-center text-slate-400">Verificando estado del orquestador y calidad de datos...</div>`;

  try {
    const res = await fetchAuth(`${API_BASE}/api/pipeline/status`);
    if (!res.ok) throw new Error("Error obteniendo estado del pipeline");
    const data = await res.json();

    const dagsHtml = (data.dags || []).map(d => `
      <div class="p-4 bg-white border border-slate-200 rounded-lg flex items-center justify-between">
        <div>
          <div class="font-bold text-slate-800 text-sm">${d.dag_id}</div>
          <div class="text-xs text-slate-500">${d.purpose}</div>
        </div>
        <div class="text-right">
          <span class="px-2 py-0.5 text-xs font-semibold bg-indigo-50 text-indigo-700 rounded-md font-mono">${d.schedule}</span>
        </div>
      </div>
    `).join("");

    container.innerHTML = `
      <div class="space-y-6">
        <div class="grid grid-cols-1 md:grid-cols-3 gap-4">
          <div class="bg-white p-5 border border-slate-200 rounded-xl">
            <span class="text-xs font-semibold text-slate-500">Estado General</span>
            <div class="text-xl font-bold text-emerald-600 mt-1 flex items-center gap-2">
              <span class="w-3 h-3 rounded-full bg-emerald-500 inline-block"></span>
              ${data.overall_status}
            </div>
          </div>
          <div class="bg-white p-5 border border-slate-200 rounded-xl">
            <span class="text-xs font-semibold text-slate-500">Puntaje de Calidad</span>
            <div class="text-xl font-bold text-indigo-600 mt-1">${data.quality_score}%</div>
          </div>
          <div class="bg-white p-5 border border-slate-200 rounded-xl">
            <span class="text-xs font-semibold text-slate-500">Registros Curados (Gold)</span>
            <div class="text-xl font-bold text-slate-800 mt-1">${data.medallion_layers.gold_curated_records.toLocaleString()}</div>
          </div>
        </div>

        <div class="bg-white p-6 border border-slate-200 rounded-xl">
          <h3 class="text-base font-bold text-slate-800 mb-4">DAGs Orquestados en Apache Airflow</h3>
          <div class="space-y-3">${dagsHtml}</div>
        </div>

        ${state.user.role === 'admin' ? `
          <div class="bg-white p-6 border border-slate-200 rounded-xl">
            <div class="flex items-center justify-between mb-4">
              <h3 class="text-base font-bold text-slate-800">Auditoría de Calidad y Rechazos</h3>
              <button onclick="inspectQuarantineAudit()" class="px-3 py-1.5 text-xs font-bold bg-slate-800 text-white rounded-lg hover:bg-slate-700">
                Consultar Auditoría de Registros
              </button>
            </div>
            <div id="quarantine-audit-results" class="text-xs text-slate-600">
              Presione el botón para ver el detalle de lotes y registros con anomalías o reglas no conformes.
            </div>
          </div>
        ` : ''}
      </div>
    `;
  } catch (err) {
    container.innerHTML = `<div class="p-8 text-center text-red-500">Error al consultar estado: ${err.message}</div>`;
  }
}

async function inspectQuarantineAudit() {
  const resultDiv = document.getElementById("quarantine-audit-results");
  resultDiv.innerHTML = "Consultando auditoría de calidad...";

  try {
    const res = await fetchAuth(`${API_BASE}/api/quality/quarantine`);
    const data = await res.json();
    resultDiv.innerHTML = `
      <div class="p-3 bg-slate-50 rounded border border-slate-200 mt-2">
        <p class="font-semibold text-slate-800 mb-2">${data.architecture_note}</p>
        <pre class="bg-slate-900 text-emerald-400 p-3 rounded overflow-x-auto text-[11px]">${JSON.stringify(data.rejection_audit_logs, null, 2)}</pre>
      </div>
    `;
  } catch (err) {
    resultDiv.innerHTML = `<span class="text-red-500">Error: ${err.message}</span>`;
  }
}

// ------------------------------------------------------------------------------
// 6. Acciones de Administrador (Backfill & Regeneración de IA)
// ------------------------------------------------------------------------------
async function triggerBackfill() {
  if (state.user.role !== "admin") return;
  const days = prompt("Ingrese la cantidad de días hacia atrás para el backfill histórico (ej: 30):", "30");
  if (!days) return;

  showToast("Lanzando backfill histórico en Airflow...", "info");
  try {
    const res = await fetchAuth(`${API_BASE}/api/admin/backfill?days=${parseInt(days)}`, { method: "POST" });
    const data = await res.json();
    showToast(data.message, "success");
    loadKPIs();
    loadEventsTable();
  } catch (err) {
    showToast(err.message, "error");
  }
}

async function triggerRegenerateReport() {
  if (state.user.role !== "admin") return;
  showToast("Solicitando regeneración de informe de IA...", "info");
  try {
    const res = await fetchAuth(`${API_BASE}/api/admin/reports/regenerate`, { method: "POST" });
    const data = await res.json();
    showToast(data.message, "success");
    setTimeout(loadReportsView, 3000);
  } catch (err) {
    showToast(err.message, "error");
  }
}

// ------------------------------------------------------------------------------
// 7. Notificaciones Toast y Helpers
// ------------------------------------------------------------------------------
function showToast(message, type = "info") {
  const toast = document.getElementById("toast");
  const toastMsg = document.getElementById("toast-message");

  let bg = "bg-slate-800 text-white";
  if (type === "success") bg = "bg-emerald-600 text-white";
  if (type === "warning") bg = "bg-amber-600 text-white";
  if (type === "error") bg = "bg-red-600 text-white";

  toast.className = `fixed bottom-5 right-5 z-50 px-4 py-3 rounded-lg shadow-lg text-sm font-medium transition-all duration-300 ${bg}`;
  toastMsg.textContent = message;
  toast.classList.remove("hidden");

  setTimeout(() => {
    toast.classList.add("hidden");
  }, 4000);
}

// Control de Pestañas
function switchTab(tabId) {
  state.currentTab = tabId;
  ["dashboard", "reports", "pipeline"].forEach(tab => {
    const section = document.getElementById(`tab-section-${tab}`);
    const btn = document.getElementById(`nav-btn-${tab}`);
    if (tab === tabId) {
      section.classList.remove("hidden");
      btn.className = "px-3 py-2 text-sm font-bold text-indigo-600 border-b-2 border-indigo-600";
    } else {
      section.classList.add("hidden");
      btn.className = "px-3 py-2 text-sm font-medium text-slate-600 hover:text-slate-900";
    }
  });

  if (tabId === "dashboard") {
    loadKPIs();
    setTimeout(() => { if (state.map) state.map.invalidateSize(); }, 200);
  } else if (tabId === "reports") {
    loadReportsView();
  } else if (tabId === "pipeline") {
    loadPipelineView();
  }
}

// ------------------------------------------------------------------------------
// Inicialización al cargar la página
// ------------------------------------------------------------------------------
document.addEventListener("DOMContentLoaded", () => {
  document.getElementById("login-form").addEventListener("submit", handleLogin);
  document.getElementById("btn-logout").addEventListener("click", handleLogout);

  // Filtros de tabla
  document.getElementById("filter-form").addEventListener("submit", (e) => {
    e.preventDefault();
    state.filters.minMag = document.getElementById("filter-min-mag").value;
    state.filters.region = document.getElementById("filter-region").value;
    state.filters.startDate = document.getElementById("filter-start-date").value;
    state.filters.endDate = document.getElementById("filter-end-date").value;
    state.filters.page = 1;
    loadEventsTable();
  });

  document.getElementById("btn-reset-filters").addEventListener("click", () => {
    document.getElementById("filter-form").reset();
    state.filters = { page: 1, pageSize: 15, minMag: "", region: "", startDate: "", endDate: "" };
    loadEventsTable();
  });

  // Paginación
  document.getElementById("btn-prev-page").addEventListener("click", () => {
    if (state.filters.page > 1) {
      state.filters.page--;
      loadEventsTable();
    }
  });

  document.getElementById("btn-next-page").addEventListener("click", () => {
    state.filters.page++;
    loadEventsTable();
  });

  initAuth();
});

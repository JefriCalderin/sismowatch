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
  queryMode: "usgs", // "usgs" (API Oficial USGS) o "gold" (PostgreSQL Gold)
  filters: {
    page: 1,
    pageSize: 15,
    searchType: "last_n_days",
    lastDays: 7,
    targetDate: "",
    startDate: "",
    endDate: "",
    month: new Date().getMonth() + 1,
    year: new Date().getFullYear(),
    minMag: 2.5,
    orderBy: "time"
  },
  ttsEnabled: true,
  speechRecognition: null,
  isListening: false
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
        loadTodayEvents();
        loadKPIs();
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
  initMap();
  await Promise.all([
    loadKPIs(),
    loadTodayEvents(),
    loadCharts(),
    loadEventsTable()
  ]);

  // Refresco automático de eventos de hoy cada 30 segundos
  if (!state.todayRefreshInterval) {
    state.todayRefreshInterval = setInterval(() => {
      loadTodayEvents();
      loadKPIs();
    }, 30000);
  }
}

async function loadTodayEvents() {
  const tbody = document.getElementById("today-events-table-body");
  const emptyState = document.getElementById("today-events-empty");
  const badgeCount = document.getElementById("badge-today-count");
  if (!tbody) return;

  try {
    const res = await fetchAuth(`${API_BASE}/api/events/today?limit=50`);
    if (!res.ok) throw new Error("Error obteniendo eventos de hoy");
    const data = await res.json();
    const events = data.events || [];

    if (badgeCount) {
      badgeCount.textContent = `${events.length} sismos registrados hoy`;
    }

    if (events.length === 0) {
      tbody.innerHTML = "";
      if (emptyState) emptyState.classList.remove("hidden");
      return;
    }

    if (emptyState) emptyState.classList.add("hidden");

    tbody.innerHTML = events.map(ev => {
      const magVal = parseFloat(ev.magnitude) || 0.0;
      let magBadgeClass = "bg-emerald-100 text-emerald-800";
      if (magVal >= 7.0) magBadgeClass = "bg-red-600 text-white font-bold";
      else if (magVal >= 5.5) magBadgeClass = "bg-orange-100 text-orange-800 font-bold";
      else if (magVal >= 4.0) magBadgeClass = "bg-amber-100 text-amber-800";
      else if (magVal >= 3.0) magBadgeClass = "bg-blue-100 text-blue-800";

      const coordsStr = (ev.latitude != null && ev.longitude != null) 
        ? `${ev.latitude}, ${ev.longitude}` 
        : 'N/A';
      const eventTimeStr = ev.event_time ? new Date(ev.event_time).toUTCString().slice(17, 25) + ' UTC' : 'N/A';

      return `
        <tr class="hover:bg-slate-50 transition border-b border-slate-100 text-xs">
          <td class="py-3 px-4 font-mono font-bold text-indigo-700">
            ${ev.url ? `<a href="${ev.url}" target="_blank" class="hover:underline flex items-center gap-1">${ev.id} <svg class="w-3 h-3 text-slate-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14"/></svg></a>` : ev.id}
          </td>
          <td class="py-3 px-4">
            <span class="px-2.5 py-0.5 rounded-full text-xs font-bold ${magBadgeClass}">M ${magVal.toFixed(1)}</span>
          </td>
          <td class="py-3 px-4 font-medium text-slate-800">
            ${ev.place}
            ${ev.is_anomaly ? `<span class="ml-1.5 inline-flex items-center px-1.5 py-0.5 rounded text-[10px] font-bold bg-amber-100 text-amber-800">⚠️ Anomalía</span>` : ''}
            ${ev.tsunami_alert ? `<span class="ml-1 inline-flex items-center px-1.5 py-0.5 rounded text-[10px] font-bold bg-red-100 text-red-800">🌊 Tsunami</span>` : ''}
          </td>
          <td class="py-3 px-4 text-slate-600">${ev.region || 'Global'}</td>
          <td class="py-3 px-4 text-slate-600 font-semibold">${ev.depth_km} km</td>
          <td class="py-3 px-4 font-mono text-[11px] text-slate-500">${coordsStr}</td>
          <td class="py-3 px-4 text-slate-500 text-[11px] whitespace-nowrap font-medium">${eventTimeStr}</td>
        </tr>
      `;
    }).join("");

    // Renderizar los sismos de hoy en el mapa en vivo inicialmente
    renderMapMarkers(events);

  } catch (err) {
    console.error("Error cargando eventos de hoy:", err);
  }
}
window.loadTodayEvents = loadTodayEvents;

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
  if (!state.map) return;

  // Limpiar marcadores anteriores
  state.mapMarkers.forEach(m => state.map.removeLayer(m));
  state.mapMarkers = [];

  const latLngs = [];

  events.forEach(ev => {
    if (ev.latitude == null || ev.longitude == null) return;
    const mag = parseFloat(ev.magnitude) || 0.0;
    
    // Color según magnitud sismológica
    let color = "#22c55e"; // < 3.0 Micro
    if (mag >= 7.0) color = "#ef4444"; // Mayor
    else if (mag >= 5.5) color = "#f97316"; // Fuerte
    else if (mag >= 4.0) color = "#eab308"; // Moderado
    else if (mag >= 3.0) color = "#3b82f6"; // Menor

    const circle = L.circleMarker([ev.latitude, ev.longitude], {
      radius: Math.max(mag * 2.8, 5),
      fillColor: color,
      color: "#ffffff",
      weight: 1.5,
      opacity: 1,
      fillOpacity: 0.75
    }).addTo(state.map);

    const eventTimeStr = ev.event_time ? new Date(ev.event_time).toUTCString() : (ev.time ? new Date(ev.time).toUTCString() : 'N/A');

    circle.bindPopup(`
      <div class="text-xs p-1">
        <h4 class="font-bold text-slate-900 text-sm">M ${mag.toFixed(1)} - ${ev.place}</h4>
        <p class="text-slate-600 mt-1">Región: <b>${ev.region || 'Global'}</b></p>
        <p class="text-slate-600">Profundidad: <b>${ev.depth_km} km</b></p>
        <p class="text-slate-600">Coordenadas: <b>${ev.latitude}, ${ev.longitude}</b></p>
        <p class="text-slate-500 mt-0.5">Fecha UTC: ${eventTimeStr}</p>
        ${ev.is_anomaly ? `<p class="font-semibold text-amber-600 mt-1">⚠️ ${ev.anomaly_reason || 'Anomalía Sísmica'}</p>` : ''}
        ${ev.tsunami_alert ? `<p class="font-bold text-red-600">🌊 Alerta de Tsunami</p>` : ''}
        ${ev.url ? `<p class="mt-2"><a href="${ev.url}" target="_blank" class="text-indigo-600 hover:underline font-semibold flex items-center gap-1">Ver ficha USGS oficial &rarr;</a></p>` : ''}
      </div>
    `);

    state.mapMarkers.push(circle);
    latLngs.push([ev.latitude, ev.longitude]);
  });

  // Ajustar la vista suavemente para visualizar los sismos consultados
  if (latLngs.length > 1) {
    try {
      state.map.fitBounds(L.latLngBounds(latLngs), { padding: [35, 35], maxZoom: 8 });
    } catch (e) {
      // Ignorar si los límites son planos
    }
  } else if (latLngs.length === 1) {
    state.map.setView(latLngs[0], 5);
  }
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

// ------------------------------------------------------------------------------
// Controladores del Módulo de Seguimiento e Historial Sísmico (USGS & Gold)
// ------------------------------------------------------------------------------
window.setQueryMode = function(mode) {
  state.queryMode = mode;
  state.filters.page = 1;
  const btnUsgs = document.getElementById("btn-mode-usgs");
  const btnGold = document.getElementById("btn-mode-gold");
  if (btnUsgs && btnGold) {
    if (mode === "usgs") {
      btnUsgs.className = "px-3 py-1.5 rounded-lg bg-white text-indigo-700 shadow-2xs font-bold transition cursor-pointer";
      btnGold.className = "px-3 py-1.5 rounded-lg text-slate-600 hover:text-slate-900 transition cursor-pointer";
    } else {
      btnGold.className = "px-3 py-1.5 rounded-lg bg-white text-indigo-700 shadow-2xs font-bold transition cursor-pointer";
      btnUsgs.className = "px-3 py-1.5 rounded-lg text-slate-600 hover:text-slate-900 transition cursor-pointer";
    }
  }
  showToast(`Modo cambiado: ${mode === 'usgs' ? 'Consulta Oficial USGS (En Vivo)' : 'Catálogo Local Capa Gold'}`, "info");
  loadEventsTable();
};

window.onSearchTypeChange = function() {
  const select = document.getElementById("filter-search-type");
  const type = select ? select.value : "last_n_days";
  state.filters.searchType = type;

  const containers = {
    last_n_days: document.getElementById("field-container-last-days"),
    specific_day: document.getElementById("field-container-specific-day"),
    custom_range: document.getElementById("field-container-custom-range"),
    full_month: document.getElementById("field-container-full-month"),
    full_year: document.getElementById("field-container-full-year")
  };

  Object.entries(containers).forEach(([key, el]) => {
    if (!el) return;
    if (key === type) el.classList.remove("hidden");
    else el.classList.add("hidden");
  });
};

window.setQuickDays = function(days) {
  const input = document.getElementById("filter-last-days");
  if (input) input.value = days;
  state.filters.lastDays = days;
  state.filters.page = 1;
  loadEventsTable();
};

window.resetSearchFilters = function() {
  const form = document.getElementById("filter-form");
  if (form) form.reset();
  state.filters.page = 1;
  state.filters.searchType = "last_n_days";
  state.filters.lastDays = 7;
  state.filters.minMag = 2.5;
  state.filters.orderBy = "time";
  
  const selectType = document.getElementById("filter-search-type");
  if (selectType) selectType.value = "last_n_days";
  const inputDays = document.getElementById("filter-last-days");
  if (inputDays) inputDays.value = 7;
  const inputMag = document.getElementById("filter-min-mag");
  if (inputMag) inputMag.value = 2.5;
  const selectOrder = document.getElementById("filter-orderby");
  if (selectOrder) selectOrder.value = "time";

  onSearchTypeChange();
  loadEventsTable();
};

window.handleFilterSubmit = function(e) {
  if (e && e.preventDefault) e.preventDefault();
  state.filters.page = 1;
  loadEventsTable();
};

async function loadEventsTable() {
  const tbody = document.getElementById("events-table-body");
  const emptyState = document.getElementById("events-empty");
  const errorState = document.getElementById("events-error");
  const loadingState = document.getElementById("events-loading");
  const countBadge = document.getElementById("badge-results-count");
  const periodBadge = document.getElementById("badge-period-info");

  if (loadingState) loadingState.classList.remove("hidden");
  if (emptyState) emptyState.classList.add("hidden");
  if (errorState) errorState.classList.add("hidden");
  tbody.innerHTML = "";

  // Extraer valores actuales de los inputs
  const searchType = document.getElementById("filter-search-type") ? document.getElementById("filter-search-type").value : "last_n_days";
  const lastDays = document.getElementById("filter-last-days") ? parseInt(document.getElementById("filter-last-days").value) || 7 : 7;
  const targetDate = document.getElementById("filter-target-date") ? document.getElementById("filter-target-date").value : "";
  const startDate = document.getElementById("filter-start-date") ? document.getElementById("filter-start-date").value : "";
  const endDate = document.getElementById("filter-end-date") ? document.getElementById("filter-end-date").value : "";
  const month = document.getElementById("filter-month") ? parseInt(document.getElementById("filter-month").value) || (new Date().getMonth() + 1) : (new Date().getMonth() + 1);
  const monthYear = document.getElementById("filter-month-year") ? parseInt(document.getElementById("filter-month-year").value) || new Date().getFullYear() : new Date().getFullYear();
  const fullYear = document.getElementById("filter-full-year") ? parseInt(document.getElementById("filter-full-year").value) || new Date().getFullYear() : new Date().getFullYear();
  const minMag = document.getElementById("filter-min-mag") ? parseFloat(document.getElementById("filter-min-mag").value) || 2.5 : 2.5;
  const orderby = document.getElementById("filter-orderby") ? document.getElementById("filter-orderby").value : "time";

  try {
    let url = "";
    let data = null;

    if (state.queryMode === "usgs") {
      const qParams = new URLSearchParams({
        search_type: searchType,
        min_magnitude: minMag,
        orderby: orderby,
        page: state.filters.page,
        page_size: state.filters.pageSize
      });
      if (searchType === "last_n_days") qParams.append("last_days", lastDays);
      else if (searchType === "specific_day" && targetDate) qParams.append("target_date", targetDate);
      else if (searchType === "custom_range") {
        if (startDate) qParams.append("start_date", startDate);
        if (endDate) qParams.append("end_date", endDate);
      } else if (searchType === "full_month") {
        qParams.append("month", month);
        qParams.append("year", monthYear);
      } else if (searchType === "full_year") {
        qParams.append("year", fullYear);
      }

      url = `${API_BASE}/api/events/usgs-query?${qParams.toString()}`;
      const res = await fetchAuth(url);
      if (!res.ok) {
        const err = await res.json().catch(() => ({ detail: "Error en servidor USGS" }));
        throw new Error(err.detail || "Error consultando API del USGS");
      }
      data = await res.json();

      if (countBadge) countBadge.textContent = `${data.total.toLocaleString()} sismos en USGS Oficial (M ≥ ${minMag})`;
      if (periodBadge && data.period) {
        periodBadge.textContent = `Período: ${data.period.starttime.replace('T', ' ')} a ${data.period.endtime.replace('T', ' ')} UTC`;
      }
    } else {
      // Modo Capa Gold local
      const qParams = new URLSearchParams({
        page: state.filters.page,
        page_size: state.filters.pageSize,
        min_magnitude: minMag
      });
      if (startDate) qParams.append("start_date", startDate);
      if (endDate) qParams.append("end_date", endDate);

      url = `${API_BASE}/api/events?${qParams.toString()}`;
      const res = await fetchAuth(url);
      if (!res.ok) throw new Error("Error consultando Capa Gold");
      data = await res.json();

      if (countBadge) countBadge.textContent = `${data.total} sismos en Catálogo Gold (M ≥ ${minMag})`;
      if (periodBadge) periodBadge.textContent = `Almacenados en PostgreSQL Gold`;
    }

    if (loadingState) loadingState.classList.add("hidden");

    if (!data || !data.events || data.events.length === 0) {
      if (emptyState) emptyState.classList.remove("hidden");
      renderMapMarkers([]);
      return;
    }

    // Renderizar filas de la tabla (7 columnas completas solicitadas)
    tbody.innerHTML = data.events.map(ev => {
      const magVal = parseFloat(ev.magnitude) || 0.0;
      let magBadgeClass = "bg-emerald-100 text-emerald-800";
      if (magVal >= 7.0) magBadgeClass = "bg-red-600 text-white font-bold";
      else if (magVal >= 5.5) magBadgeClass = "bg-orange-100 text-orange-800 font-bold";
      else if (magVal >= 4.0) magBadgeClass = "bg-amber-100 text-amber-800";
      else if (magVal >= 3.0) magBadgeClass = "bg-blue-100 text-blue-800";

      const coordsStr = (ev.latitude != null && ev.longitude != null) 
        ? `${ev.latitude}, ${ev.longitude}` 
        : 'N/A';
      const eventTimeStr = ev.event_time ? new Date(ev.event_time).toUTCString() : (ev.time ? new Date(ev.time).toUTCString() : 'N/A');

      return `
        <tr class="hover:bg-slate-50 transition border-b border-slate-100 text-xs">
          <td class="py-3 px-4 font-mono font-bold text-indigo-700">
            ${ev.url ? `<a href="${ev.url}" target="_blank" class="hover:underline flex items-center gap-1">${ev.id} <svg class="w-3 h-3 text-slate-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14"/></svg></a>` : ev.id}
          </td>
          <td class="py-3 px-4">
            <span class="px-2.5 py-0.5 rounded-full text-xs font-bold ${magBadgeClass}">M ${magVal.toFixed(1)}</span>
          </td>
          <td class="py-3 px-4 font-medium text-slate-800">
            ${ev.place}
            ${ev.is_anomaly ? `<span class="ml-1.5 inline-flex items-center px-1.5 py-0.5 rounded text-[10px] font-bold bg-amber-100 text-amber-800">⚠️ Anomalía</span>` : ''}
            ${ev.tsunami_alert ? `<span class="ml-1 inline-flex items-center px-1.5 py-0.5 rounded text-[10px] font-bold bg-red-100 text-red-800">🌊 Tsunami</span>` : ''}
          </td>
          <td class="py-3 px-4 text-slate-600">${ev.region || 'Global'}</td>
          <td class="py-3 px-4 text-slate-600 font-semibold">${ev.depth_km} km</td>
          <td class="py-3 px-4 font-mono text-[11px] text-slate-500">${coordsStr}</td>
          <td class="py-3 px-4 text-slate-500 text-[11px] whitespace-nowrap">${eventTimeStr}</td>
        </tr>
      `;
    }).join("");

    // Actualizar marcadores en el mapa Leaflet con los sismos consultados
    renderMapMarkers(data.map_markers || data.events);

    // Paginación
    const maxPage = Math.ceil(data.total / state.filters.pageSize) || 1;
    document.getElementById("page-info").textContent = `Página ${state.filters.page} de ${maxPage} (${data.total.toLocaleString()} sismos)`;
    document.getElementById("btn-prev-page").disabled = state.filters.page <= 1;
    document.getElementById("btn-next-page").disabled = state.filters.page >= maxPage;

  } catch (err) {
    if (loadingState) loadingState.classList.add("hidden");
    if (errorState) {
      errorState.classList.remove("hidden");
      document.getElementById("events-error-message").textContent = `Error: ${err.message}`;
    }
    showToast(`Fallo al consultar sismos: ${err.message}`, "error");
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
              <button onclick="downloadExport('html', '${rep.report_date}')" class="px-3 py-1.5 text-xs font-semibold bg-indigo-50 hover:bg-indigo-100 text-indigo-700 rounded-lg flex items-center gap-1 cursor-pointer transition">
                <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4"/></svg>
                Descargar HTML
              </button>
              <button onclick="downloadExport('json', '${rep.report_date}')" class="px-3 py-1.5 text-xs font-semibold bg-slate-100 hover:bg-slate-200 text-slate-700 rounded-lg flex items-center gap-1 cursor-pointer transition">
                JSON
              </button>
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
  ["dashboard", "chat", "reports", "pipeline"].forEach(tab => {
    const section = document.getElementById(`tab-section-${tab}`);
    const btn = document.getElementById(`nav-btn-${tab}`);
    if (section && btn) {
      if (tab === tabId) {
        section.classList.remove("hidden");
        btn.className = "px-3 py-2 text-sm font-bold text-indigo-600 border-b-2 border-indigo-600";
      } else {
        section.classList.add("hidden");
        btn.className = "px-3 py-2 text-sm font-medium text-slate-600 hover:text-slate-900";
      }
    }
  });

  if (tabId === "dashboard") {
    loadKPIs();
    setTimeout(() => { if (state.map) state.map.invalidateSize(); }, 200);
  } else if (tabId === "chat") {
    setTimeout(() => {
      const input = document.getElementById("chat-input");
      if (input) input.focus();
    }, 100);
  } else if (tabId === "reports") {
    loadReportsView();
  } else if (tabId === "pipeline") {
    loadPipelineView();
  }
}

// ------------------------------------------------------------------------------
// Función de Descarga Segura de Reportes y Datos (Resuelve Error 401 de Token)
// ------------------------------------------------------------------------------
window.downloadExport = async function(format = 'csv', targetDate = null) {
  try {
    const token = state.token;
    if (!token) {
      showToast("Debe iniciar sesión para descargar reportes.", "warning");
      return;
    }
    let url = `${API_BASE}/api/reports/export?format=${format}&download=true`;
    if (targetDate) url += `&target_date=${encodeURIComponent(targetDate)}`;
    url += `&token=${encodeURIComponent(token)}`;

    showToast(`Preparando descarga (${format.toUpperCase()})...`, "info");
    
    const res = await fetch(url, {
      headers: { "Authorization": `Bearer ${token}` }
    });

    if (!res.ok) {
      const err = await res.json().catch(() => ({ detail: "Error descargando archivo" }));
      throw new Error(err.detail || "Error en el servidor al exportar");
    }

    const blob = await res.blob();
    const disposition = res.headers.get("content-disposition");
    let ext = format === 'html' ? 'html' : (format === 'json' ? 'json' : 'csv');
    let filename = `sismowatch_${format}_${targetDate || 'export'}.${ext}`;
    if (disposition && disposition.indexOf("filename=") !== -1) {
      const matches = /filename[^;=\n]*=((['"]).*?\2|[^;\n]*)/.exec(disposition);
      if (matches != null && matches[1]) {
        filename = matches[1].replace(/['"]/g, '');
      }
    }

    const blobUrl = window.URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = blobUrl;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    window.URL.revokeObjectURL(blobUrl);
    showToast(`Archivo descargado con éxito: ${filename}`, "success");
  } catch (err) {
    showToast(`Error al descargar: ${err.message}`, "error");
  }
};

// ------------------------------------------------------------------------------
// 8. Chat Asistente de IA (Capa Gold)
// ------------------------------------------------------------------------------
let chatHistory = [];

window.sendQuickChatMessage = function(promptText) {
  const input = document.getElementById("chat-input");
  if (input) {
    input.value = promptText;
    handleChatSubmit(new Event("submit"));
  }
};

window.handleChatSubmit = async function(e) {
  if (e && e.preventDefault) e.preventDefault();
  const input = document.getElementById("chat-input");
  const sendBtn = document.getElementById("chat-send-btn");
  if (!input) return;
  const message = (input.value || "").trim();
  if (!message) return;

  // Renderizar mensaje de usuario
  appendChatMessage("user", message);
  input.value = "";
  input.disabled = true;
  if (sendBtn) {
    sendBtn.disabled = true;
    sendBtn.innerHTML = `<span>Consultando...</span>`;
  }

  // Renderizar indicador de carga asistente
  const loadingId = appendChatLoading();

  try {
    const res = await fetchAuth(`${API_BASE}/api/chat`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        message: message,
        history: chatHistory.slice(-6)
      })
    });

    const data = await res.json();
    removeChatLoading(loadingId);
    appendChatMessage("assistant", data.reply, data.data_sources);
    
    // Guardar en historial
    chatHistory.push({ role: "user", content: message });
    chatHistory.push({ role: "assistant", content: data.reply });
  } catch (err) {
    removeChatLoading(loadingId);
    appendChatMessage("assistant", `⚠️ Lo siento, ocurrió un error consultando los datos de la Capa Gold: ${err.message}`);
  } finally {
    input.disabled = false;
    if (sendBtn) {
      sendBtn.disabled = false;
      sendBtn.innerHTML = `<span>Enviar</span><svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M14 5l7 7m0 0l-7 7m7-7H3"/></svg>`;
    }
    input.focus();
  }
};

function formatMarkdownToHtml(text) {
  if (!text) return "";
  let html = text
    .replace(/^### (.*$)/gim, '<h4 class="font-bold text-indigo-950 mt-2 mb-1">$1</h4>')
    .replace(/^## (.*$)/gim, '<h3 class="font-bold text-indigo-950 mt-3 mb-1 text-base">$1</h3>')
    .replace(/^# (.*$)/gim, '<h2 class="font-extrabold text-indigo-950 mt-3 mb-2 text-lg">$1</h2>')
    .replace(/\*\*(.*?)\*\*/gim, '<b>$1</b>')
    .replace(/\*(.*?)\*/gim, '<i>$1</i>')
    .replace(/`(.*?)`/gim, '<code class="px-1.5 py-0.5 bg-slate-100 text-indigo-700 rounded font-mono text-xs">$1</code>')
    .replace(/^\- (.*$)/gim, '<li class="ml-4 list-disc">$1</li>')
    .replace(/\n\n/gim, '<br/><br/>')
    .replace(/\n/gim, '<br/>');
  return html;
}

function appendChatMessage(role, content, sources = []) {
  const container = document.getElementById("chat-messages");
  if (!container) return;

  const msgDiv = document.createElement("div");

  if (role === "user") {
    msgDiv.className = "flex items-start gap-3 justify-end";
    msgDiv.innerHTML = `
      <div class="bg-indigo-600 text-white rounded-2xl rounded-tr-none p-4 max-w-xl text-sm leading-relaxed shadow-sm">
        <p>${content}</p>
      </div>
      <div class="w-8 h-8 rounded-full bg-slate-800 text-white flex items-center justify-center font-bold text-xs shrink-0">
        ${state.user ? state.user.username.slice(0, 2).toUpperCase() : 'TU'}
      </div>
    `;
  } else {
    msgDiv.className = "flex items-start gap-3";
    const formatted = formatMarkdownToHtml(content);
    let sourcesBadge = "";
    if (sources && sources.length > 0) {
      sourcesBadge = `
        <div class="mt-3 pt-2 border-t border-slate-200/60 flex flex-wrap gap-1 items-center text-[11px] text-slate-500">
          <span class="font-semibold text-slate-400">Fuentes Gold:</span>
          ${sources.map(s => `<span class="px-1.5 py-0.5 bg-slate-200 text-slate-700 rounded font-mono text-[10px]">${s}</span>`).join(" ")}
        </div>
      `;
    }

    msgDiv.innerHTML = `
      <div class="w-8 h-8 rounded-full bg-indigo-600 text-white flex items-center justify-center font-bold text-xs shrink-0">IA</div>
      <div class="bg-slate-50 border border-slate-200 rounded-2xl rounded-tl-none p-4 max-w-2xl text-sm text-slate-800 leading-relaxed shadow-2xs">
        <div>${formatted}</div>
        ${sourcesBadge}
      </div>
    `;

    // Reproducir respuesta por voz si el sintetizador de voz está activo
    speakAssistantReply(content);
  }

  container.appendChild(msgDiv);
  container.scrollTop = container.scrollHeight;
}

// ------------------------------------------------------------------------------
// Selector y Controles de Voz para el Asistente IA (Web Speech API)
// ------------------------------------------------------------------------------
window.setChatInputMode = function(mode) {
  const btnText = document.getElementById("chat-mode-text");
  const btnVoice = document.getElementById("chat-mode-voice");
  if (mode === "voice") {
    if (btnVoice) btnVoice.className = "px-2.5 py-1 rounded-md bg-white text-indigo-700 font-bold shadow-2xs transition cursor-pointer";
    if (btnText) btnText.className = "px-2.5 py-1 rounded-md text-slate-600 hover:text-slate-900 font-semibold transition cursor-pointer";
    showToast("Modo de voz activado. Haz clic en el micrófono para dictar.", "info");
    toggleVoiceDictation();
  } else {
    if (btnText) btnText.className = "px-2.5 py-1 rounded-md bg-white text-indigo-700 font-bold shadow-2xs transition cursor-pointer";
    if (btnVoice) btnVoice.className = "px-2.5 py-1 rounded-md text-slate-600 hover:text-slate-900 font-semibold transition cursor-pointer";
  }
};

window.toggleTextToSpeech = function() {
  state.ttsEnabled = !state.ttsEnabled;
  const icon = document.getElementById("tts-icon");
  const label = document.getElementById("tts-label");
  if (state.ttsEnabled) {
    if (icon) icon.textContent = "🔊";
    if (label) label.textContent = "Voz: Activada";
    showToast("Lectura por voz activada", "success");
  } else {
    if (icon) icon.textContent = "🔇";
    if (label) label.textContent = "Voz: Desactivada";
    if (window.speechSynthesis) window.speechSynthesis.cancel();
    showToast("Lectura por voz desactivada", "info");
  }
};

window.speakAssistantReply = function(text) {
  if (!state.ttsEnabled || !window.speechSynthesis) return;
  try {
    window.speechSynthesis.cancel();
    const clean = text.replace(/[*#_`~>•]/g, "").replace(/\n+/g, ". ");
    const utterance = new SpeechSynthesisUtterance(clean);
    utterance.lang = "es-ES";
    utterance.rate = 1.05;
    window.speechSynthesis.speak(utterance);
  } catch (e) {
    // Silencioso si el navegador no permite audio automático
  }
};

window.toggleVoiceDictation = function() {
  const SpeechRec = window.SpeechRecognition || window.webkitSpeechRecognition;
  if (!SpeechRec) {
    showToast("Tu navegador no soporta reconocimiento de voz nativo (Web Speech API). Usa Google Chrome o Edge.", "warning");
    return;
  }

  const btnMic = document.getElementById("btn-mic");
  const input = document.getElementById("chat-input");

  if (state.isListening && state.speechRecognition) {
    state.speechRecognition.stop();
    state.isListening = false;
    if (btnMic) btnMic.className = "p-3 bg-slate-100 hover:bg-indigo-50 text-slate-600 hover:text-indigo-600 rounded-xl transition border border-slate-200 cursor-pointer";
    return;
  }

  const rec = new SpeechRec();
  rec.lang = "es-ES";
  rec.continuous = false;
  rec.interimResults = false;

  rec.onstart = function() {
    state.isListening = true;
    if (btnMic) btnMic.className = "p-3 bg-red-100 text-red-600 animate-pulse rounded-xl transition border border-red-300 cursor-pointer";
    showToast("Escuchando tu pregunta... habla ahora.", "info");
  };

  rec.onresult = function(e) {
    const transcript = e.results[0][0].transcript;
    if (input) input.value = transcript;
    showToast(`Pregunta escuchada: "${transcript}"`, "success");
    handleChatSubmit(new Event("submit"));
  };

  rec.onerror = function(e) {
    state.isListening = false;
    if (btnMic) btnMic.className = "p-3 bg-slate-100 hover:bg-indigo-50 text-slate-600 hover:text-indigo-600 rounded-xl transition border border-slate-200 cursor-pointer";
    showToast(`Error de micrófono: ${e.error}`, "warning");
  };

  rec.onend = function() {
    state.isListening = false;
    if (btnMic) btnMic.className = "p-3 bg-slate-100 hover:bg-indigo-50 text-slate-600 hover:text-indigo-600 rounded-xl transition border border-slate-200 cursor-pointer";
  };

  state.speechRecognition = rec;
  rec.start();
};

function appendChatLoading() {
  const container = document.getElementById("chat-messages");
  if (!container) return "";
  const id = "loading-" + Date.now();
  const div = document.createElement("div");
  div.id = id;
  div.className = "flex items-start gap-3";
  div.innerHTML = `
    <div class="w-8 h-8 rounded-full bg-indigo-600 text-white flex items-center justify-center font-bold text-xs shrink-0">IA</div>
    <div class="bg-slate-50 border border-slate-200 rounded-2xl rounded-tl-none p-4 text-xs text-slate-500 flex items-center gap-2 shadow-2xs">
      <span class="w-2 h-2 rounded-full bg-indigo-500 animate-bounce"></span>
      <span class="w-2 h-2 rounded-full bg-indigo-500 animate-bounce [animation-delay:0.2s]"></span>
      <span class="w-2 h-2 rounded-full bg-indigo-500 animate-bounce [animation-delay:0.4s]"></span>
      <span>Consultando tablas Gold y procesando sismicidad...</span>
    </div>
  `;
  container.appendChild(div);
  container.scrollTop = container.scrollHeight;
  return id;
}

function removeChatLoading(id) {
  const el = document.getElementById(id);
  if (el) el.remove();
}

// ------------------------------------------------------------------------------
// Modo Oscuro (Dark Mode)
// ------------------------------------------------------------------------------
function initTheme() {
  const savedTheme = localStorage.getItem("sismowatch_theme");
  const prefersDark = window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches;
  if (savedTheme === "dark" || (!savedTheme && prefersDark)) {
    document.documentElement.classList.add("dark");
    updateThemeIcon(true);
  } else {
    document.documentElement.classList.remove("dark");
    updateThemeIcon(false);
  }
}

function toggleDarkMode() {
  const isDark = document.documentElement.classList.toggle("dark");
  localStorage.setItem("sismowatch_theme", isDark ? "dark" : "light");
  updateThemeIcon(isDark);
  showToast(`Modo ${isDark ? "Oscuro" : "Claro"} activado`, "info");
}

function updateThemeIcon(isDark) {
  const icon = document.getElementById("theme-icon");
  if (icon) {
    icon.textContent = isDark ? "☀️" : "🌙";
  }
}
window.toggleDarkMode = toggleDarkMode;

// ------------------------------------------------------------------------------
// Inicialización al cargar la página
// ------------------------------------------------------------------------------
document.addEventListener("DOMContentLoaded", () => {
  initTheme();
  document.getElementById("login-form").addEventListener("submit", handleLogin);
  document.getElementById("btn-logout").addEventListener("click", handleLogout);

  // Inicializar fechas por defecto en los filtros
  const today = new Date();
  const past7 = new Date();
  past7.setDate(today.getDate() - 7);

  const targetDateInput = document.getElementById("filter-target-date");
  if (targetDateInput) targetDateInput.value = today.toISOString().split("T")[0];

  const startDateInput = document.getElementById("filter-start-date");
  if (startDateInput) startDateInput.value = past7.toISOString().split("T")[0];

  const endDateInput = document.getElementById("filter-end-date");
  if (endDateInput) endDateInput.value = today.toISOString().split("T")[0];

  const monthYearInput = document.getElementById("filter-month-year");
  if (monthYearInput) monthYearInput.value = today.getFullYear();

  const fullYearInput = document.getElementById("filter-full-year");
  if (fullYearInput) fullYearInput.value = today.getFullYear();

  const monthSelect = document.getElementById("filter-month");
  if (monthSelect) monthSelect.value = (today.getMonth() + 1).toString();

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

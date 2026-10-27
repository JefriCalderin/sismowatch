"""
Aplicación Principal FastAPI - Backend de Plataforma SismoWatch.
Provee endpoints de consumo analítico de datos de la capa Gold con seguridad RBAC y streaming en tiempo real.
"""
from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from api.app.config import api_settings
from api.app.routers import auth, kpis, events, stats, reports, stream, admin

app = FastAPI(
    title="SismoWatch Data Platform API",
    description="Backend de consumo analítico para monitoreo sísmico global casi en tiempo real bajo Arquitectura Medallón.",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Configuración de CORS restringido a los orígenes autorizados
app.add_middleware(
    CORSMiddleware,
    allow_origins=api_settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "OPTIONS"],
    allow_headers=["*"],
)

# Registro de Routers
app.include_router(auth.router)
app.include_router(kpis.router)
app.include_router(events.router)
app.include_router(stats.router)
app.include_router(reports.router)
app.include_router(stream.router)
app.include_router(admin.router)

@app.get("/health", tags=["Salud"])
def health_check():
    """Endpoint de comprobación de salud del contenedor API."""
    return {
        "status": "healthy",
        "service": "sismowatch-api",
        "architecture": "Medallion (Bronze -> Silver -> Gold)",
        "governance": "OpenMetadata Ready"
    }

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Manejador global de excepciones para evitar fugas de información interna en errores 500."""
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"detail": "Ha ocurrido un error interno en el servidor. Consulte los logs del sistema."}
    )

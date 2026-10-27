#!/bin/bash
set -e

echo "======================================================================"
echo "SismoWatch - Plataforma Global de Monitoreo Sísmico en Tiempo Real"
echo "======================================================================"

if [ ! -f .env ]; then
    echo ">>> Archivo .env no encontrado. Copiando desde .env.example..."
    cp .env.example .env
fi

echo ">>> Levantando todos los servicios con Docker Compose..."
docker compose up -d

echo ""
echo "======================================================================"
echo " Plataforma SismoWatch Desplegada con Éxito"
echo "======================================================================"
echo "  - Plataforma Web:       http://localhost:3000"
echo "  - FastAPI Documentación: http://localhost:8000/docs"
echo "  - Apache Airflow:       http://localhost:8080 (admin / SismoAdmin2026!)"
echo "  - Apache Spark UI:      http://localhost:8081"
echo "======================================================================"

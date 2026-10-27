# ==============================================================================
# Makefile para SismoWatch Data Platform
# ==============================================================================

.PHONY: start stop restart logs status test security lineage clean help

help:
	@echo "Comandos disponibles:"
	@echo "  make start      - Levanta todo el stack con Docker Compose (PostgreSQL, Airflow, Spark, API, Web, OpenMetadata)"
	@echo "  make stop       - Detiene todos los contenedores"
	@echo "  make restart    - Reinicia todos los servicios"
	@echo "  make logs       - Muestra logs en tiempo real"
	@echo "  make status     - Muestra el estado de los contenedores"
	@echo "  make test       - Ejecuta las pruebas unitarias y de integración con pytest"
	@echo "  make lineage    - Genera y registra el linaje y catálogo en OpenMetadata"
	@echo "  make security   - Ejecuta el escaneo de seguridad estático"

start:
	@echo ">>> Levantando plataforma SismoWatch con un solo comando..."
	docker compose up -d
	@echo ">>> Todos los servicios iniciados exitosamente."
	@echo "    - Web Dashboard:    http://localhost:3000"
	@echo "    - FastAPI Backend:  http://localhost:8000/docs"
	@echo "    - Airflow UI:       http://localhost:8080 (admin / SismoAdmin2026!)"
	@echo "    - Spark Master UI:  http://localhost:8081"

stop:
	@echo ">>> Deteniendo servicios..."
	docker compose down

restart: stop start

logs:
	docker compose logs -f

status:
	docker compose ps

test:
	@echo ">>> Ejecutando suite de pruebas con pytest..."
	pytest airflow/tests/ -v

lineage:
	@echo ">>> Sincronizando catálogo y linaje OpenMetadata..."
	python3 openmetadata/register_metadata.py

security:
	@echo ">>> Analizando seguridad de código..."
	bandit -r airflow/ api/ -ll || true

clean:
	@echo ">>> Limpiando caché y archivos temporales..."
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

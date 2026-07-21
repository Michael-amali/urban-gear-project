.PHONY: setup generate-data test run-etl run-dbt airflow-init up down clean

setup:
	pip install -r requirements.txt
	cp -n .env.example .env || true
	mkdir -p data/raw data/processed data/rejected data/metrics
	@echo "Setup complete. Edit .env if needed (ENV=local|local-s3|prod)"

generate-data:
	python scripts/generate_sample_data.py --days 7 --records-per-day 50000 --bad-rate 0.07 --env $${ENV:-local}

test:
	pytest tests/ -v

run-etl:
	python etl/etl_job.py --process_date $${PROCESS_DATE:-$$(date -d yesterday +%F)} --env $${ENV:-local}

run-dbt:
	cd dbt/urbangear && dbt deps && dbt run --profiles-dir . --target local && dbt test --profiles-dir . --target local

airflow-init:
	docker-compose up airflow-init

up:
	docker-compose up -d
	@echo "Airflow UI: http://localhost:8080 (airflow/airflow)"
	@echo "Warehouse Postgres: localhost:5433"

down:
	docker-compose down

clean:
	rm -rf data/processed/* data/rejected/* data/metrics/*
	find . -type d -name __pycache__ -exec rm -rf {} +

backfill:
	python scripts/backfill.py --start 2026-03-01 --end 2026-03-07

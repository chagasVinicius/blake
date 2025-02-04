.PHONY: up down

up:
	docker compose -f dagster-compose.yaml up --build -d 
	docker compose -f spark-compose.yaml up --build -d
	docker compose -f dagster-compose.yaml -f spark-compose.yaml logs -f

down:
	docker compose -f dagster-compose.yaml -f spark-compose.yaml down -v --remove-orphans

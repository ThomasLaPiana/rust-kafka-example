.PHONY: run
run:
	docker compose up --wait
	cargo run
	docker compose down --remove-orphans --volumes

up:
	docker compose up --wait

down:
	docker compose down --remove-orphans --volumes
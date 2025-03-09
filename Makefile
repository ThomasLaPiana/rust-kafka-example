run:
	docker compose up --wait
	cargo run
	docker compose down --remove-orphans --volumes
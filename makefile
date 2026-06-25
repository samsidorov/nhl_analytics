up:
	docker compose -f docker/docker-compose.yml up --build

down:
	docker compose -f docker/docker-compose.yml down

restart:
	make down && make up

logs:
	docker compose -f docker/docker-compose.yml logs -f
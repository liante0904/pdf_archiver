COMPOSE := docker compose

.PHONY: up down restart logs ps

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

restart:
	$(COMPOSE) restart

logs:
	$(COMPOSE) logs -f pdf-archiver

ps:
	$(COMPOSE) ps

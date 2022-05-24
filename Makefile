SHELL := /bin/bash
PWD := $(shell pwd)

all:

docker-image:
	docker build -f ./src/Dockerfile -t "7574_tp2:latest" .
.PHONY: docker-image

docker-compose-up: docker-image
	docker-compose -f docker-compose.yaml --env-file local.env up -d --build
.PHONY: docker-compose-up

docker-compose-down:
	docker-compose -f docker-compose.yaml --env-file local.env stop -t 10
	docker-compose -f docker-compose.yaml --env-file local.env down --remove-orphans
.PHONY: docker-compose-down

docker-compose-logs:
	docker-compose -f docker-compose.yaml logs -f
.PHONY: docker-compose-logs

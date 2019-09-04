SHELL := /bin/bash
PATH := $(PATH)
CLUSTER := webapps
SERVICE := tda-3

build:
	docker-compose build

dev:
	docker-compose -f docker-compose_DEV.yml up -d --build

reload:
	docker-compose down && make dev && docker-compose logs -f

test:
	echo "Testing not yet set up..."

aws-login:
	$(shell aws ecr get-login --no-include-email)

push:
	docker tag transit_data_access/parser:latest 517918230755.dkr.ecr.us-east-2.amazonaws.com/transit-data-access/parser:latest \
	&& docker tag transit_data_access/web_client:latest 517918230755.dkr.ecr.us-east-2.amazonaws.com/transit-data-access/web_client:latest \
	&& docker tag transit_data_access/web_server:latest 517918230755.dkr.ecr.us-east-2.amazonaws.com/transit-data-access/web_server:latest \
	&& docker push 517918230755.dkr.ecr.us-east-2.amazonaws.com/transit-data-access/parser:latest \
	&& docker push 517918230755.dkr.ecr.us-east-2.amazonaws.com/transit-data-access/web_server:latest \
	&& docker push 517918230755.dkr.ecr.us-east-2.amazonaws.com/transit-data-access/web_client:latest \

register-task:
	aws ecs register-task-definition --cli-input-json file://aws-deploy-conf/taskdef.json

create-service:
	aws ecs create-service --cluster $(CLUSTER) --service-name $(SERVICE) --cli-input-json file://aws-deploy-conf/servicedef.json

update-service:
	aws ecs update-service --cluster $(CLUSTER) --service $(SERVICE) --force-new-deployment


deploy: build test aws-login push update-service

create: build test aws-login push create-service

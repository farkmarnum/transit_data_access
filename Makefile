build:
	docker-compose build

dev:
	docker-compose -f docker-compose_DEV.yml up -d --build

push:
	docker push farkmarnum/transit_data_access:web_client && docker push farkmarnum/transit_data_access:web_server && docker push farkmarnum/transit_data_access:parser

pull:
	docker pull farkmarnum/transit_data_access:web_client && docker pull farkmarnum/transit_data_access:web_server && docker pull farkmarnum/transit_data_access:parser

run:
	docker stack deploy -c docker-compose.yml tda

build:
	docker-compose build

dev:
	docker-compose -f docker-compose_DEV.yml up -d --build

push-docker:
	docker push farkmarnum/transit_data_access:web_client \
	&& docker push farkmarnum/transit_data_access:web_server \
	&& docker push farkmarnum/transit_data_access:parser

pull-docker:
	docker pull farkmarnum/transit_data_access:web_client \
	&& docker pull farkmarnum/transit_data_access:web_server \
	&& docker pull farkmarnum/transit_data_access:parser

run-docker:
	docker stack deploy -c docker-compose.yml --prune --with-registry-auth tda

update-docker:
	docker service update tda_parser --image farkmarnum/transit_data_access:parser --with-registry-auth \
	&& docker service update tda_web_server --image farkmarnum/transit_data_access:web_server --with-registry-auth \
	&& docker service update tda_web_client --image farkmarnum/transit_data_access:web_client --with-registry-auth

aws:
	$(aws ecr get-login --no-include-email --region us-east-2) \
	&& docker-compose build \
	&& docker tag transit-data-access/parser:latest 517918230755.dkr.ecr.us-east-2.amazonaws.com/transit-data-access/parser:latest \
	&& docker tag transit-data-access/web_client:latest 517918230755.dkr.ecr.us-east-2.amazonaws.com/transit-data-access/web_client:latest \
	&& docker tag transit-data-access/web_server:latest 517918230755.dkr.ecr.us-east-2.amazonaws.com/transit-data-access/web_server:latest \
	&& docker push 517918230755.dkr.ecr.us-east-2.amazonaws.com/transit-data-access/parser:latest \
	&& docker push 517918230755.dkr.ecr.us-east-2.amazonaws.com/transit-data-access/web_server:latest \
	&& docker push 517918230755.dkr.ecr.us-east-2.amazonaws.com/transit-data-access/web_client:latest

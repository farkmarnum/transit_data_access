build:
	docker-compose build

dev:
	docker-compose -f docker-compose_DEV.yml up -d --build

reload:
	docker-compose down && make dev && docker-compose logs -f

test:
	echo "Testing not yet set up..."

push:
	docker tag transit_data_access/parser:latest 517918230755.dkr.ecr.us-east-2.amazonaws.com/transit-data-access/parser:latest \
	&& docker tag transit_data_access/web_client:latest 517918230755.dkr.ecr.us-east-2.amazonaws.com/transit-data-access/web_client:latest \
	&& docker tag transit_data_access/web_server:latest 517918230755.dkr.ecr.us-east-2.amazonaws.com/transit-data-access/web_server:latest \
	&& docker push 517918230755.dkr.ecr.us-east-2.amazonaws.com/transit-data-access/parser:latest \
	&& docker push 517918230755.dkr.ecr.us-east-2.amazonaws.com/transit-data-access/web_server:latest \
	&& docker push 517918230755.dkr.ecr.us-east-2.amazonaws.com/transit-data-access/web_client:latest \

register-task:
	aws ecs register-task-definition --cli-input-json file://task-transit_data_access.json

create-service:
	aws ecs create-service --cluster webapps --service-name tda --cli-input-json file://service-tda.json

deploy:
	aws ecs update-service --cluster webapps --service tda --force-new-deployment

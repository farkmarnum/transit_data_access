build:
	docker-compose build

dev:
	docker-compose -f docker-compose_DEV.yml up -d --build

test:
	echo "Testing not yet set up..."

push:
	docker-compose build \
	&& docker tag transit_data_access/parser:latest 517918230755.dkr.ecr.us-east-2.amazonaws.com/transit-data-access/parser:latest \
	&& docker tag transit_data_access/web_client:latest 517918230755.dkr.ecr.us-east-2.amazonaws.com/transit-data-access/web_client:latest \
	&& docker tag transit_data_access/web_server:latest 517918230755.dkr.ecr.us-east-2.amazonaws.com/transit-data-access/web_server:latest \
	&& docker push 517918230755.dkr.ecr.us-east-2.amazonaws.com/transit-data-access/parser:latest \
	&& docker push 517918230755.dkr.ecr.us-east-2.amazonaws.com/transit-data-access/web_server:latest \
	&& docker push 517918230755.dkr.ecr.us-east-2.amazonaws.com/transit-data-access/web_client:latest \

register-task:
	aws ecs register-task-definition --cli-input-json file://ecs-task.json

deploy:
	aws ecs update-service --cluster webapps --service transit-data-access --force-new-deployment

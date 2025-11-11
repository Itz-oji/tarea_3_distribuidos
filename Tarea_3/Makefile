DOCKER_COMPOSE=docker-compose

docker-MV1:
	$(DOCKER_COMPOSE) --profile mv1 up --build 

docker-MV2:
	$(DOCKER_COMPOSE) --profile mv2 up --build 

docker-MV3:
	$(DOCKER_COMPOSE) --profile mv3 up --build 

docker-MV4:
	$(DOCKER_COMPOSE) --profile mv4 up --build 

clean:
	$(DOCKER_COMPOSE) down -v --remove-orphans
# Kafka
kafka-elk:
	@docker-compose -f docker/docker-compose.yml up -d --remove-orphans --renew-anon-volumes --force-recreate

kafka-elk-down:
	@docker-compose -f docker/docker-compose.yml down --remove-orphans


# Faust
run-es-app:
	@python -m src.es_app -l info
	
run-es-app-debug:
	@python -m src.es_app -l debug

#
run-test-app:
	@python -m src.test_app -l info

run-test-app-debug:
	@python -m src.test_app -l debug

#
run-standalone-app:
	@python -m src.standalone -l info

run-standalone-app-debug:
	@python -m src.standalone -l debug

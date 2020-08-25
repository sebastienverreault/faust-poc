# Kafka
kafka-elk:
	@docker-compose -f docker/docker-compose.yml up -d --remove-orphans --renew-anon-volumes --force-recreate

kafka-elk-down:
	@docker-compose -f docker/docker-compose.yml down --remove-orphans

# Elasticsearch
manual-map-raw-es:
	@curl -XPUT -H 'Content-Type: application/json' 'localhost:9200/raw_weblogs'     --data-binary @elasticsearch/raw_es_mapping.json
manual-reduced-raw-es:
	@curl -XPUT -H 'Content-Type: application/json' 'localhost:9200/reduced_weblogs' --data-binary @elasticsearch/reduced_es_mapping.json

manual-load-raw-es:
	@curl -XPOST -H 'Content-Type: application/json' 'localhost:9200/raw_weblogs/_bulk'     --data-binary elasticsearch/es_raw_logs.json
manual-load-reduced-es:
	@curl -XPOST -H 'Content-Type: application/json' 'localhost:9200/reduced_weblogs/_bulk' --data-binary elasticsearch/es_reduced_logs.json


# Faust
run-es-app:
	@python -m src.es_app worker -l info
	
run-es-app-debug:
	@python -m src.es_app worker -l debug

#
run-test-app:
	@python -m src.test_app worker -l info

run-test-app-debug:
	@python -m src.test_app worker -l debug

#
run-standalone-app:
	@python -m src.standalone worker -l info

run-standalone-app-debug:
	@python -m src.standalone worker -l debug

import logging
import sys, getopt
import traceback

from elasticsearch import AsyncElasticsearch
from elasticsearch.exceptions import ElasticsearchException

#
# Starting

logging.info("Starting ClientRequest")


#
# Configures
FAUST_APP_NAME = "weblogs-stream-processor"
KAFKA_BROKERS = "kafka://127.0.0.1:9092"
ELASTICSEARCH_HOST = "127.0.0.1"	#os.environ["ELASTICSEARCH_HOST"]
ELASTICSEARCH_USER = ""				#os.environ["ELASTICSEARCH_USER"]
ELASTICSEARCH_PASS = ""				#os.environ["ELASTICSEARCH_PASS"]
ELASTICSEARCH_PORT = 9200			#os.environ["ELASTICSEARCH_PASS"]

#
# Elasticsearch config
ELASTIC_SEARCH_CONF = {
    'host': ELASTICSEARCH_HOST,
    # 'http_auth': (ELASTICSEARCH_USER, ELASTICSEARCH_PASS),
    # 'url_prefix': 'elasticsearch',
    'scheme': 'http',
    'port': ELASTICSEARCH_PORT
}

#
# Elasticsearch indexes we will use
RAW_ELASTICSEARCH_DOCUMENT_INDEX = "raw_weblogs"
REDUCED_ELASTICSEARCH_DOCUMENT_INDEX = "reduced_weblogs"

#
# Elasticsearch object init
es = AsyncElasticsearch([ELASTIC_SEARCH_CONF])


#
# convenience func for launching the app
def main(argv) -> None:
    try:
        ipAddress = ''
        userAgent = ''
        request = ''
        key = ''
        loByte = ''
        hiByte = ''

        try:
            opts, args = getopt.getopt(argv, "ki:u:r:l:h:", ["key=", "ip_address=", "user_agent=", "request=", "lo_byte=", "hi_byte="])
        except getopt.GetoptError:
            print(f'{argv[0]} ' + '-i <ip_address> -u <user_agent> -r <request> -l <lo_byte> -h <hi_byte>')
            print('or')
            print(f'{argv[0]} ' + '-k <key:ip_address_user_agent_request> -l <lo_byte> -h <hi_byte>')
            sys.exit(2)
        for opt, arg in opts:
            if opt in ('-k', '--key'):
                key = arg
            elif opt in ('-i', '--ip_address'):
                ipAddress = arg
            elif opt in ('-u', '--user_agent'):
                userAgent = arg
            elif opt in ('-r', '--request'):
                request = arg
            elif opt in ('-l', '--lo_byte'):
                loByte = arg
            elif opt in ('-h', '--hi_byte'):
                hiByte = arg

        if not key:
            key = "_".join((ipAddress, userAgent, request))

        # No range so we see what we got
        raw_search_body = {
            "query": {
                "bool": {
                  "must": [
                    { "match": { "ip_address": { "query": ipAddress } } },
                    { "match": { "user_agent": { "query": userAgent } } },
                    { "match": { "request": { "query": request } } },
                  ] } } }

        # No range so we see what we did
        reduced_search_body = {
            "query": {
                "bool": {
                  "must": [
                    { "match": { "ip_address": { "query": ipAddress } } },
                    { "match": { "user_agent": { "query": userAgent } } },
                    { "match": { "request": { "query": request } } },
                  ] } } }

        # No range so we see if we can answer the request right
        reduced_search_body_with_range = {
            "query": {
                "bool": {
                  "must": [
                    { "match": { "ip_address": { "query": ipAddress } } },
                    { "match": { "user_agent": { "query": userAgent } } },
                    { "match": { "request": { "query": request } } },
                    { "range": { "lo_byte": { "lte": loByte } } },
                    { "range": { "hi_byte": { "gte": hiByte } } }
                  ] } } }

        # Get raw data first
        response = es.search(index=RAW_ELASTICSEARCH_DOCUMENT_INDEX, body=raw_search_body)
        failed = response.get("_shards", {}).get("failed")
        if failed:
            logging.error("Elasticsearch request failed with the following error: " +
                          str(response) + "The parameters were, id/key: " + str(key) +
                          " body/value: " + str(request))
        else:
            print("Got %d Raw Hits:" % response['hits']['total']['value'])
            for hit in response['hits']['hits']:
                print(hit)
                # print("%(timestamp)s %(author)s: %(text)s" % hit["_source"])

        # Get reduced then
        response = es.search(index=REDUCED_ELASTICSEARCH_DOCUMENT_INDEX, body=reduced_search_body)
        failed = response.get("_shards", {}).get("failed")
        if failed:
            logging.error("Elasticsearch request failed with the following error: " +
                          str(response) + "The parameters were, id/key: " + str(key) +
                          " body/value: " + str(request))
        else:
            print("Got %d Reduced Hits:" % response['hits']['total']['value'])
            for hit in response['hits']['hits']:
                print(hit)
                # print("%(timestamp)s %(author)s: %(text)s" % hit["_source"])

        # Finally get a reduced with range answer
        response = es.search(index=REDUCED_ELASTICSEARCH_DOCUMENT_INDEX, body=reduced_search_body_with_range)
        failed = response.get("_shards", {}).get("failed")
        if failed:
            logging.error("Elasticsearch request failed with the following error: " +
                          str(response) + "The parameters were, id/key: " + str(key) +
                          " body/value: " + str(request))
        else:
            print("Got %d Reduced with range Hits:" % response['hits']['total']['value'])
            for hit in response['hits']['hits']:
                print(hit)
                # print("%(timestamp)s %(author)s: %(text)s" % hit["_source"])

    except ElasticsearchException as ex:
        logging.exception("An Elasticsearch exception has been caught :" + str(ex) +
                          "The parameters are: id/key - " + str(key) + request)

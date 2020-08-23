import faust
import logging

from collections import defaultdict
from elasticsearch import AsyncElasticsearch
from elasticsearch.exceptions import ElasticsearchException

from src.WebLogs.WebLogEntry import WebLogEntry
from src.WebLogs.WebLogReducer import WebLogReducer

import json
import os

#
# Starting
logging.info("Starting WebLogs stream processor worker")

#
# Controlling the streaming worker frequency
PUBLISH_DELAY = 0.1

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
ELASTICSEARCH_DOCUMENT_INDEX = "reduced_weblogs"
ELASTICSEARCH_DOCUMENT_TYPE  = "comment"

ELASTICSEARCH_DOCUMENT_INDEX = "raw_weblogs"
ELASTICSEARCH_DOCUMENT_TYPE  = "comment"

#
# Elasticsearch object init
es = AsyncElasticsearch([ELASTIC_SEARCH_CONF])

#
# Faust app config
app = faust.App(
        FAUST_APP_NAME,
        broker=KAFKA_BROKERS,
        key_serializer="json",
        value_serializer="json")


#
# convenience func for launching the app
def main() -> None:
    app.main()


#
# Create topics we will use in the pipeline
weblogs_topic = app.topic("weblogs_stream")
weblogs_tokenized_topic = app.topic("weblogs_tokenized_stream")
weblogs_persistence_topic = app.topic("weblogs_persistence_stream")
weblogs_stats_topic = app.topic("weblogs_stats_stream")

#
# Define a table to keep our state
logs = app.Table('logs', key_type=str, value_type=WebLogReducer, default=WebLogReducer)
stats = app.Table('stats', key_type=str, value_type=defaultdict, default=defaultdict(int))


#
# Read weblogs data to simulate sequential arrivals
filename = os.path.abspath(r"./src/data/sample.log")

#
# Using pandas would be easier but we will stream the line as is
# df_weblogs = pd.read_csv(filename, delimiter='\t')

#
# Reading straight from file
with open(filename) as f:
    content = f.read().splitlines()

#
# Set our state, skip headers
current_row = 1
max_row = len(content)


#
# Worker sending weblogs
@app.timer(PUBLISH_DELAY)
async def generate_weblogs():
    # Get the current row according to our preserved state
    if current_row < max_row:
        # weblogs_sample = df_weblogs[current_row]
        weblogs_data = content[current_row]
        current_row += 1
    # Or just a random sample from the
    # weblogs_sample = df_weblogs.sample(1)
    #
    # Publish it
    await weblogs_topic.send(value=weblogs_data)


#
#
@app.agent(weblogs_topic)
async def tokenize_weblogs(raw_line):
    #
    # Read line, split into tokens and map to a WebLogEntry
    tokens = raw_line.split('\t')
    entry = WebLogEntry.Map(tokens)
    if entry.Valid:
        await weblogs_tokenized_topic.send(value=entry)


#
# Listen on the "tokenized" topic to get filtered entries
# Package them and send to elasticsearch for indexing there
@app.agent(weblogs_tokenized_topic)
async def weblogs_elasticsearch_sink(entry):
    try:
        key = entry.Key
        json_str_entry = json.dumps(entry)
        response = await es.index(
            index=ELASTICSEARCH_DOCUMENT_INDEX,
            doc_type=ELASTICSEARCH_DOCUMENT_TYPE,
            id=key,
            body=json_str_entry)
        failed = response.get("_shards", {}).get("failed")
        if failed:
            logging.error("Elasticsearch request failed with the following error: " +
                          str(response) + "The parameters were, id/key: " + str(key) +
                          " body/value: " + str(json_str_entry))
    except ElasticsearchException as e:
        logging.exception("An Elasticsearch exception has been caught :" + str(e) +
                          "The parameters are: id/key - " + str(key) + json_str_entry)


#
# Listen on the "tokenized" topic to get filtered entries
# Reduce them to keep track of what bytes were delivered
# We could also stream the key and a list of byte ranges (WebLogReducer's internal list)
# to a persistence layer like Cassandra where it would be easy to query
@app.agent(weblogs_tokenized_topic)
async def reduce_weblogs(entry):
    # New Keys in the dictionary should already be a WebLogEntry, so just process it
    # if entry.Key not in logs:
    #    logs[entry.Key] = WebLogReducer()
    logs[entry.Key].ProcessNewEntry(entry)
    # Update our stats
    if logs[entry.Key].IsFileComplete():
        stats['complete'] = 1
        stats['incomplete'].pop(entry.Key, None)
        stats['incompleteNonZero'].pop(entry.Key, None)
        stats['incompleteNonSingle'].pop(entry.Key, None)
    elif logs[entry.Key].IsFileIncompleteNonZero():
        stats['complete'].pop(entry.Key, None)
        stats['incomplete'] = 1
        stats['incompleteNonZero'] = 1
        stats['incompleteNonSingle'].pop(entry.Key, None)
    elif logs[entry.Key].IsFileIncompleteNonSingle():
        stats['complete'].pop(entry.Key, None)
        stats['incomplete'] = 1
        stats['incompleteNonZero'].pop(entry.Key, None)
        stats['incompleteNonSingle'] = 1
    await weblogs_stats_topic.send(value=[
        len(stats['complete']),
        len(stats['incomplete']),
        len(stats['incompleteNonZero']),
        len(stats['incompleteNonSingle'])
    ])
    #
    # Publish for Cassandra type DB
    await weblogs_persistence_topic.send(id=entry.key, value=logs[entry.Key].ListOfByteRange())


#
# Send stats on that topic for easy monitoring
@app.agent(weblogs_stats_topic)
async def print_stats(stats):
    print(f"Completed # files: {stats[0]} Incomplete # files: {stats[1]} " +
          f"( Missing in Start {stats[2]}, Missing in Middle {stats[3]})")
    await weblogs_stats_topic.send(value=1)

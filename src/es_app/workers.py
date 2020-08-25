import faust
import logging
import traceback

from elasticsearch import AsyncElasticsearch
from elasticsearch.exceptions import ElasticsearchException

from src.cassandra.cassandra import CassandraDriver

from src.WebLogs.ReducedLog import ReducedLog
from src.WebLogs.ByteRange import ByteRange
from src.WebLogs.ByteRange import BRReduce
from src.WebLogs.WebLogEntry import WebLogEntry
from src.WebLogs.WebLogJson import WebLogJson

from typing import List

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
RAW_ELASTICSEARCH_DOCUMENT_INDEX = "raw_weblogs"
RAW_ELASTICSEARCH_DOCUMENT_TYPE  = "raw_logs"           # still relevant?

REDUCED_ELASTICSEARCH_DOCUMENT_INDEX = "reduced_weblogs"
REDUCED_ELASTICSEARCH_DOCUMENT_TYPE  = "reduced_logs"   # still relevant?

#
# Elasticsearch object init
es = AsyncElasticsearch([ELASTIC_SEARCH_CONF])


#
# Encapsulated Cassandra driver
drv = CassandraDriver()
drv.createsession()
drv.setlogger()
# rows = drv.session.execute('SELECT ip_address, user_agent, request, byte_ranges FROM logs')
# for user_row in rows:
#     print(f"{user_row.ip_address}, {user_row.user_agent}, {user_row.request}, {user_row.byte_ranges}")
# drv.createkeyspace('weblogs')
# drv.create_table()

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
weblogs_topic = app.topic("weblogs", value_type=str)
weblogs_token_topic = app.topic("weblogs_tokens", value_type=WebLogEntry)
weblogs_stats_topic = app.topic("weblogs_stats", value_type=ReducedLog)
weblogs_persistence_topic = app.topic("weblogs_persistence", value_type=List)

#
# Define a table to keep our state
weblogs_tokens = app.Table('weblogs_tokens', key_type=str, default=list)
# weblogs_stats = app.Table('weblogs_stats', key_type=str, default=set)


#
# Read weblogs data to simulate sequential arrivals
# filename = os.path.abspath(r"./src/data/sample.log")
filename = os.path.abspath(r"./src/data/test_sample.log")


#
# Reading straight from file
with open(filename) as f:
    content = f.read().splitlines()


#
# Set our state, skip headers
current_row = [0]
max_row = len(content)


#
# Worker sending weblogs
@app.timer(PUBLISH_DELAY)
async def generate_weblogs():
    # Get the current row according to our preserved state
    current_row[0] += 1
    if current_row[0] < max_row:
        # normal  order
        # weblogs_data = content[current_row[0]]
        # reverse order
        weblogs_data = content[max_row - current_row[0]]
        # Or just a random sample from the
        # weblogs_sample = df_weblogs.sample(1)

        # Publish it
        if len(weblogs_data) > 0:
            await weblogs_topic.send(value=weblogs_data)


#
# Tokenize, i.e. split string and construct a serializable object
@app.agent(weblogs_topic)
async def tokenize_weblogs(weblogs):
    #
    # Read line, split into tokens and map to a WebLogEntry
    async for weblog in weblogs:
        try:
            tokens = weblog.split('\t')
            entry = WebLogEntry.Map(tokens)
            print(f"Sent entry:<{entry}>")
            if entry.Valid:
                await weblogs_token_topic.send(key=entry.Key, value=entry)
        except Exception as ex:
            track = traceback.format_exc()
            print(track)


#
# Listen on the "tokenized" topic to get raw  entries
# Package them and send to elasticsearch for indexing there
@app.agent(weblogs_token_topic)
async def weblogs_elasticsearch_sink(tokens):
    async for entry in tokens.group_by(WebLogEntry.Key):
        try:
            key = entry.Key
            print(f"weblogs_elasticsearch_sink -> rx entry: <{entry}>")
            print(f"weblogs_elasticsearch_sink -> rx entry.__dict__: <{entry.__dict__}>")
            print(f"weblogs_elasticsearch_sink -> rx json.dumps(entry.__dict__): <{json.dumps(entry.__dict__)}>")
            json_str_entry = json.dumps(entry.__dict__)
            print(json_str_entry)
            print(f"weblogs_elasticsearch_sink -> json entry: <{json_str_entry}>")
            response = await es.index(
                index=RAW_ELASTICSEARCH_DOCUMENT_INDEX,
                #doc_type=RAW_ELASTICSEARCH_DOCUMENT_TYPE,
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


@app.agent(weblogs_token_topic)
async def reduce_weblogs_tokens(tokens):
    # New Keys in the dictionary should already be a WebLogEntry, so just process it
    async for entry in tokens.group_by(WebLogEntry.Key):
        try:
            # Print entry
            data = f"reduce_weblogs_tokens rx entry : <{entry.Key}, {entry.LoByte}, {entry.HiByte}>"
            print(data)

            # Process
            byte_range_list = weblogs_tokens[entry.Key]
            new_byte_range = ByteRange(LoByte=entry.LoByte, HiByte=entry.HiByte)
            BRReduce(byte_range_list, new_byte_range)
            weblogs_tokens[entry.Key] = byte_range_list

            # Print result
            state = f"<{entry.Key}, {[str(x) for x in byte_range_list]}>"
            print(state)
            #
            # Should really be putting the list of byte_range, but that going to be another iteration
            rl = ReducedLog(IpAddress=entry.IpAddress, UserAgent=entry.UserAgent, Request=entry.Request,
                            LoByte=entry.LoByte, HiByte=entry.HiByte)

            print(f"reduce_weblogs_tokens -> json entry: <{rl}>")
            # Send info on stats monitoring topic
            await weblogs_stats_topic.send(value=rl)
        except Exception as ex:
            track = traceback.format_exc()
            print(track)
            print(f"For entry = <{entry}>")


#
# Listen on the reduced topic to get filtered entries
# Package them and send to elasticsearch for indexing there
@app.agent(weblogs_stats_topic)
async def weblogs_stats_elasticsearch_sink(reduced_logs):
    async for rl in reduced_logs:
        try:
            print(f"weblogs_stats_elasticsearch_sink -> rx reduced_logs: <{rl}>")
            key = "_".join((rl.IpAddress, rl.UserAgent, rl.Request))
            json_str_rl = json.dumps(rl)
            print(f"weblogs_stats_elasticsearch_sink -> json reduced_logs: <{json_str_rl}>")
            response = await es.index(
                index=REDUCED_ELASTICSEARCH_DOCUMENT_INDEX,
                #doc_type=REDUCED_ELASTICSEARCH_DOCUMENT_TYPE,
                id=key,
                body=json_str_rl)
            failed = response.get("_shards", {}).get("failed")
            if failed:
                logging.error("Elasticsearch request failed with the following error: " +
                              str(response) + "The parameters were, id/key: " + str(key) +
                              " body/value: " + str(json_str_rl))
        except ElasticsearchException as e:
            logging.exception("An Elasticsearch exception has been caught :" + str(e) +
                              "The parameters are: id/key - " + str(key) + json_str_rl)

# #
# # Send stats on that topic for easy monitoring
# @app.agent(weblogs_stats_topic)
# async def weblogs_stats_cassandra_sink(reduced_logs):
#     async for reduced_log in reduced_logs:
#         try:
#             #
#             # Publish for Cassandra type DB, save in Cassandra?
#             await drv.insert_data(reduced_log)
#             # await drv.update_data()
#         except Exception as ex:
#             track = traceback.format_exc()
#             print(track)

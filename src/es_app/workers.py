import faust
import logging
import traceback

from collections import defaultdict
from elasticsearch import AsyncElasticsearch
from elasticsearch.exceptions import ElasticsearchException

from src.WebLogs.ByteRange import ByteRange
from src.WebLogs.WebLogEntry import WebLogEntry
from src.WebLogs.WebLogReducer import WebLogReducer
from src.WebLogs.WebLogReducer2 import WebLogReducer2

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
weblogs_topic = app.topic("weblogs", value_type=str)
weblogs_token_topic = app.topic("weblogs_tokens", value_type=WebLogEntry)
weblogs_stats_topic = app.topic("weblogs_stats", value_type=List)
weblogs_persistence_topic = app.topic("weblogs_persistence_stream", value_type=List)

#
# Define a table to keep our state
# weblogs_tokens = app.Table('weblogs_tokens', key_type=str, value_type=WebLogReducer, default=WebLogReducer)
# weblogs_tokens = app.Table('weblogs_tokens', key_type=str, value_type=WebLogReducer2, default=WebLogReducer2)
# weblogs_tokens = app.Table('weblogs_tokens', key_type=str, value_type=int, default=int)
weblogs_tokens = app.Table('weblogs_tokens', key_type=str, default=list)
weblogs_stats = app.Table('weblogs_stats', key_type=str, default=set)


#
# Read weblogs data to simulate sequential arrivals
# filename = os.path.abspath(r"./src/data/sample.log")
filename = os.path.abspath(r"./src/data/test_sample.log")

#
# Using pandas would be easier but we will stream the line as is
# df_weblogs = pd.read_csv(filename, delimiter='\t')

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
        # print(f"Sending row: {current_row[0]}")
        # weblogs_sample = df_weblogs[current_row]
        weblogs_data = content[current_row[0]]
        # Or just a random sample from the
        # weblogs_sample = df_weblogs.sample(1)
        #
        # Publish it
        if len(weblogs_data) > 0:
            await weblogs_topic.send(value=weblogs_data)


#
# Tokenize
@app.agent(weblogs_topic)
async def tokenize_weblogs(raw_line_stream):
    #
    # Read line, split into tokens and map to a WebLogEntry
    async for raw_line in raw_line_stream:
        try:
            # print(raw_line)
            tokens = raw_line.split('\t')
            entry = WebLogEntry.Map(tokens)
            if entry.Valid:
                # print(entry)
                await weblogs_token_topic.send(value=entry)
        except Exception as ex:
            track = traceback.format_exc()
            print(track)


# #
# # Listen on the "tokenized" topic to get filtered entries
# # Package them and send to elasticsearch for indexing there
# @app.agent(weblogs_tokenized_topic)
# async def weblogs_elasticsearch_sink(entry):
#     try:
#         key = entry.Key
#         json_str_entry = json.dumps(entry)
#         response = await es.index(
#             index=ELASTICSEARCH_DOCUMENT_INDEX,
#             doc_type=ELASTICSEARCH_DOCUMENT_TYPE,
#             id=key,
#             body=json_str_entry)
#         failed = response.get("_shards", {}).get("failed")
#         if failed:
#             logging.error("Elasticsearch request failed with the following error: " +
#                           str(response) + "The parameters were, id/key: " + str(key) +
#                           " body/value: " + str(json_str_entry))
#     except ElasticsearchException as e:
#         logging.exception("An Elasticsearch exception has been caught :" + str(e) +
#                           "The parameters are: id/key - " + str(key) + json_str_entry)


def SafeAdd(k: str, s: set):
    if k not in s:
        s.add(k)


def SafeRemove(k: str, s: set):
    if k in s:
        s.remove(k)


# @app.agent(weblogs_token_topic)
# async def reduce_weblogs_tokens(tokens):
#     # New Keys in the dictionary should already be a WebLogEntry, so just process it
#     async for entry in tokens.group_by(WebLogEntry.Key):
#         try:
#             print(f"stats before ={weblogs_tokens[entry.Key]}")
#             weblogs_tokens[entry.Key] += 1
#             print(f"Key='{entry.Key}'")
#             print(f"stats after ={weblogs_tokens[entry.Key]}")
#         except Exception as ex:
#             track = traceback.format_exc()
#             print(track)


@app.agent(weblogs_token_topic)
async def reduce_weblogs_tokens(tokens):
    # New Keys in the dictionary should already be a WebLogEntry, so just process it
    async for entry in tokens.group_by(WebLogEntry.Key):
        try:
            tuple_list = weblogs_tokens[entry.Key]
            print(f"tuples before '{entry.Key} ({entry.LoByte}, {entry.HiByte})'={len(tuple_list)}")
            for e in tuple_list:
                print(e)
            new_tuple = ByteRange(LoByte=entry.LoByte, HiByte=entry.HiByte)
            Reduce(tuple_list, new_tuple)
            # tuple_list.append()
            print(f"tuples after '{entry.Key}'={len(tuple_list)}")
            for e in tuple_list:
                print(e)
            # tuple_list.ProcessNewEntry(entry)
            weblogs_tokens[entry.Key] = tuple_list
        except Exception as ex:
            track = traceback.format_exc()
            print(track)


def Reduce(br_list: List[ByteRange], new_br: ByteRange):
    gotMerged = False

    print(f"Reduce got new tuple (stack size = {len(br_list)}):\n")
    print(f"{new_br}")
    # print(f"Reduce got new tuple: {new_tuple}")
    # work
    for a_br in br_list:
        print(f"In for loop")
        # Is new overlapping on the hi side of the other?
        if new_br.IsOverlappingOnHiOf(a_br):
            # overlapping!  update tuple
            a_br.SetLoHi(a_br.LoByte, new_br.HiByte)
            gotMerged = True
            print(f"overlapping! updated: {a_br}")
            break

        # Is new overlapping on the lo side of the other?
        if new_br.IsOverlappingOnLoOf(a_br):
            # overlapping!  update tuple
            a_br.SetLoHi(new_br.LoByte, a_br.HiByte)
            gotMerged = True
            print(f"overlapping! updated: {a_br}")
            break

        # Is new fully contained in the other?
        if new_br.IsContainedIn(a_br):
            # contained, drop new, we received no new information -> leave
            gotMerged = True
            print(f"contained! do nothing")
            break

        # Is new fully containing the other?
        if new_br.IsContaining(a_br):
            # fully containing, update tuple with new, we received no new information
            a_br.SetLoHi(new_br.LoByte, new_br.HiByte)
            gotMerged = True
            print(f"containing! updated: {a_br}")
            break

        # Is new totally dissociated (below or above) from the other?
        if new_br.IsDissociatedFrom(a_br):
            # new tuple, continue
            print(f"dissociated! current: {a_br}")
            print(f"dissociated! new: {new_br}")
            pass

    if gotMerged:
        print(f"got merged!")
        if len(br_list) > 1:
            print(f"diving into the stack!")
            Reduce(br_list.pop())
    else:
        print(f"not merged, adding to stack (stack size = {len(br_list)})")
        br_list.append(new_br)
        print(f"after adding to stack (stack size = {len(br_list)})")

    print(f"Reduce stack size is: {len(br_list)}")
    print(f"Reduce stack contains: ")
    for t in br_list:
        print(f"{t}\n")
    print(f"\n")


# @app.agent(weblogs_token_topic)
# async def reduce_weblogs_tokens(tokens):
#     # New Keys in the dictionary should already be a WebLogEntry, so just process it
#     async for entry in tokens.group_by(WebLogEntry.Key):
#         try:
#             entry_list = weblogs_tokens[entry.Key]
#             print(f"stats before ={len(entry_list)}")
#             for e in entry_list:
#                 print(e)
#             entry_list.append(entry)
#             print(f"Key='{entry.Key}'")
#             print(f"stats after ={len(entry_list)}")
#             for e in entry_list:
#                 print(e)
#             if len(entry_list) > 1:
#                 reducer = WebLogReducer()
#                 for e in entry_list:
#                     reducer.ProcessNewEntry(e)
#                 entry_list = reducer.GetList()
#                 print(f"stats after reduce ={len(entry_list)}")
#                 reducer = None
#                 for e in entry_list:
#                     print(e)
#             # entry_list.ProcessNewEntry(entry)
#             weblogs_tokens[entry.Key] = entry_list
#         except Exception as ex:
#             track = traceback.format_exc()
#             print(track)


# @app.agent(weblogs_token_topic)
# async def reduce_weblogs_tokens(tokens):
#     # New Keys in the dictionary should already be a WebLogEntry, so just process it
#     async for entry in tokens.group_by(WebLogEntry.Key):
#         try:
#             entry_list = weblogs_tokens[entry.Key]
#             print(f"stats before ={entry_list}")
#             entry_list.add(entry)
#             # entry_list.ProcessNewEntry(entry)
#             print(f"Key='{entry.Key}'")
#             print(f"stats after ={entry_list}")
#             weblogs_tokens[entry.Key] = entry_list
#         except Exception as ex:
#             track = traceback.format_exc()
#             print(track)


# #
# # Listen on the "tokenized" topic to get filtered entries
# # Reduce them to keep track of what bytes were delivered
# # We could also stream the key and a list of byte ranges (WebLogReducer's internal list)
# # to a persistence layer like Cassandra where it would be easy to query
# @app.agent(weblogs_token_topic)
# async def reduce_weblogs_tokens(tokens):
#     # New Keys in the dictionary should already be a WebLogEntry, so just process it
#     async for entry in tokens.group_by(WebLogEntry.Key):
#         try:
#             entry_list = weblogs_tokens[entry.Key]
#             print(f"Before processing => {entry_list}")
#             entry_list.ProcessNewEntry(entry)
#             print(f"After processing => {entry_list}")
#             weblogs_tokens[entry.Key] = entry_list
#             # Update our stats
#             completeSet = weblogs_stats['complete']
#             incompleteSet = weblogs_stats['incomplete']
#             incompleteNonZeroSet = weblogs_stats['incompleteNonZero']
#             incompleteNonSingleSet = weblogs_stats['incompleteNonSingle']
#             if entry_list.IsFileComplete():
#                 SafeAdd(entry.Key, completeSet)
#                 SafeRemove(entry.Key, incompleteSet)
#                 SafeRemove(entry.Key, incompleteNonZeroSet)
#                 SafeRemove(entry.Key, incompleteNonSingleSet)
#             else:
#                 if entry_list.IsFileIncompleteNonZero():
#                     SafeRemove(entry.Key, completeSet)
#                     SafeAdd(entry.Key, incompleteSet)
#                     SafeAdd(entry.Key, incompleteNonZeroSet)
#                     SafeRemove(entry.Key, incompleteNonSingleSet)
#                 elif entry_list.IsFileIncompleteNonSingle():
#                     SafeRemove(entry.Key, completeSet)
#                     SafeAdd(entry.Key, incompleteSet)
#                     SafeRemove(entry.Key, incompleteNonZeroSet)
#                     SafeAdd(entry.Key, incompleteNonSingleSet)
#
#             statsList = [
#                 len(completeSet),
#                 len(incompleteSet),
#                 len(incompleteNonZeroSet),
#                 len(incompleteNonSingleSet)
#             ]
#
#             print(f"Key='{entry.Key}'")
#             print(f"stats={statsList}")
#             print(f"bytes={entry_list.ListOfByteRange()}")
#
#             await weblogs_stats_topic.send(value=statsList)
#
#             #
#             # Publish for Cassandra type DB, save in Cassandra?
#             # await weblogs_persistence_topic.send(id=entry.key, value=entry_list.ListOfByteRange())
#         except Exception as ex:
#             track = traceback.format_exc()
#             print(track)


# #
# # Send stats on that topic for easy monitoring
# @app.agent(weblogs_stats_topic)
# async def print_stats(stats):
#     print(f"Completed # files: {stats[0]} Incomplete # files: {stats[1]} " +
#           f"( Missing in Start {stats[2]}, Missing in Middle {stats[3]})")
#     #await weblogs_stats_topic.send(value=1)

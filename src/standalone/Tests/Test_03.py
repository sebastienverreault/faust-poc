import traceback

import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import statsmodels as sm

from src.WebLogs.WebLogEntry import WebLogEntry
from src.WebLogs.WebLogProcessor import WebLogProcessor
from src.cassandra.cassandra import CassandraDriver


class Test_03(object):
    def __init__(self, filename):
        self.filename = filename

    def run(self):
        # load raw data
        df = raw_data = pd.read_csv(self.filename, delimiter='\t')

        webLogs = []
        for row in df.itertuples(index=False):
            entry = WebLogEntry.Map(row)
            if entry.Valid:
                webLogs.append(entry)

        # Simulate streaming the entries to our processor => In random order, but completely
        indexList = np.arange(len(webLogs))
        np.random.shuffle(indexList)
        processor = WebLogProcessor()
        for i in indexList:
            # process the entry
            processor.ProcessNewEntry(webLogs[i])
            # print some stats
            processor.PrintSomeStats()

        # print extra stats on incomplete
        processor.PrintSomeMoreStatsOnIncompletes()

        # Create a table in Cassandra for our results & save them
        # Will not be fully workable, but good enough for poc
        try:
            drv = CassandraDriver()
            drv.createsession()
            drv.createkeyspace('weblogs')
            drv.create_table('Test_04')
            for reducer_key in processor.stats:
                reducer = processor.stats[reducer_key]
                lgv2 = reducer.GetAReducedLogV2()
                drv.insert_data(lgv2)
        except Exception as ex:
            track = traceback.format_exc()
            print(track)

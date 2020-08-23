import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import statsmodels as sm

from src.WebLogs.WebLogEntry import WebLogEntry
from src.WebLogs.WebLogProcessor import WebLogProcessor

class Test_03(object):
    def __init__(self, filename):
        self.filename = filename

    def run(self):
        # load raw data
        df = raw_data = pd.read_csv(self.filename, delimiter='\t')

        # Now done inside WebLogEntry.Map() which makes more sense
        # remove non [200, 206] status code
        # is_status_200_206 = (raw_data['status'] == 200) | (raw_data['status'] == 206)
        # df = raw_data[is_status_200_206]

        # # re-index for fun
        # df.reset_index(inplace=True, drop=True)

        # split byte
        # byte_range_split = df['byte_range'].str.split('-', n = 1, expand = True)
        # df['from_by'] = byte_range_split[0]
        # df['to_byte'] = byte_range_split[1]

        # Now done inside WebLogEntry.Map() which makes more sense
        # df.loc[:, 'from_by'] = df['byte_range'].apply(lambda x: x.split('-')[0])
        # df.loc[:, 'to_byte'] = df['byte_range'].apply(lambda x: x.split('-')[1])

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

        # print extra stats on imcomplete
        processor.PrintSomeMoreStatsOnIncompletes()

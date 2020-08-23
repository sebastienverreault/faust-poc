from src.WebLogs.WebLogEntry import WebLogEntry
from src.WebLogs.WebLogReducer import WebLogReducer


class WebLogProcessor(object):
    def __init__(self):
        self.stats = {}

    def ProcessNewEntry(self, entry: WebLogEntry):
        if entry.Key not in self.stats:
            self.stats[entry.Key] = WebLogReducer()
        self.stats[entry.Key].ProcessNewEntry(entry)

    def PrintSomeStats(self):
        complete = 0
        incomplete = 0

        for key in self.stats:
            reducer = self.stats[key]
            if reducer.IsFileComplete():
                complete += 1
            else:
                incomplete += 1

        print(f"Completed # files: {complete} \t Incomplete # files: {incomplete}")

    def PrintSomeMoreStatsOnIncompletes(self):
        incompleteNonZero = 0
        incompleteNonSingle = 0

        for key in self.stats:
            reducer = self.stats[key]
            if reducer.IsFileIncompleteNonZero():
                incompleteNonZero += 1
            elif reducer.IsFileIncompleteNonSingle():
                incompleteNonSingle += 1

        if incompleteNonZero != 0 and incompleteNonSingle != 0:
            print(f"Incomplete due to missing start from zero: {incompleteNonZero} \t Incomplete due to missing middle chunk: {incompleteNonSingle}. Total={incompleteNonZero + incompleteNonSingle}")
        else:
            print("All complete")
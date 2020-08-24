from typing import List
from collections import deque

import faust
from src.WebLogs.WebLogEntry import WebLogEntry


class WebLogReducer2(faust.Record, serializer='json', validation=True):
    # stack: List[WebLogEntry] = []
    stack: list = []

    def add(self, newEntry: WebLogEntry):
        print(f"WebLogReducer2 append called")
        self.stack.append(newEntry)

    def __str__(self):
        ret = f"Stack Len={len(self.stack)}\n"
        ret += f"Stack contains: \n"
        for entry in self.stack:
            ret += f"{entry}\n"
        return ret

    def __repr__(self):
        return self.__str__()



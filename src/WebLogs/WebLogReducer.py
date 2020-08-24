from typing import List
from collections import deque

import faust
from src.WebLogs.WebLogEntry import WebLogEntry


class WebLogReducer(faust.Record, serializer='json', validation=True):
    stack: list = []

    # def __init__(self):
    #     self.stack = deque()

    def IsFileComplete(self) -> bool:
        return len(self.stack) == 1 and self.stack[-1].LoByte == 0

    def IsFileIncompleteNonZero(self) -> bool:
        return len(self.stack) == 1 and self.stack[-1].LoByte != 0

    def IsFileIncompleteNonSingle(self) -> bool:
        return len(self.stack) != 1

    def ProcessNewEntry(self, entry: WebLogEntry):
        self.__Reduce__(entry)

    def ListOfByteRange(self):
        mylist = []
        for entry in self.stack:
            mylist.append((entry.LoByte, entry.HiByte))
        return mylist

    def GetList(self):
        return self.stack

    def __Reduce__(self, newEntry: WebLogEntry):
        gotMerged = False

        print(f"Reduce got new entry (stack size = {len(self.stack)}):\n")
        print(f"{newEntry}")
        # print(f"Reduce got new entry: {newEntry}")
        # work
        for entry in self.stack:
            print(f"In for loop")
            # Is new overlapping on the hi side of the other?
            if newEntry.IsOverlappingOnHiOf(entry):
                # overlapping!  update entry
                entry.SetLoHi(entry.LoByte, newEntry.HiByte)
                gotMerged = True
                print(f"overlapping! updated entry: {entry}")
                break

            # Is new overlapping on the lo side of the other?
            if newEntry.IsOverlappingOnLoOf(entry):
                # overlapping!  update entry
                entry.SetLoHi(newEntry.LoByte, entry.HiByte)
                gotMerged = True
                print(f"overlapping! updated entry: {entry}")
                break

            # Is new fully contained in the other?
            if newEntry.IsContainedIn(entry):
                # contained, drop new, we received no new information -> leave
                gotMerged = True
                print(f"contained! do nothing")
                break

            # Is new fully containing the other?
            if newEntry.IsContaining(entry):
                # fully containing, update entry with new, we received no new information
                entry.SetLoHi(newEntry.LoByte, newEntry.HiByte)
                gotMerged = True
                print(f"containing! updated entry: {entry}")
                break

            # Is new totally dissociated (below or above) from the other?
            if newEntry.IsDissociatedFrom(entry):
                # new entry, continue
                print(f"dissociated! current entry: {entry}")
                print(f"dissociated! new entry: {newEntry}")
                pass

        if gotMerged:
            print(f"got merged!")
            if len(self.stack) > 1:
                print(f"diving into the stack!")
                self.__Reduce__(self.stack.pop())
        else:
            print(f"not merged, adding to stack (stack size = {len(self.stack)})")
            self.stack.append(newEntry)
            print(f"after adding to stack (stack size = {len(self.stack)})")

        print(f"Reduce stack size is: {len(self.stack)}")
        print(f"Reduce stack contains: ")
        for entry in self.stack:
            print(f"{entry}\n")
        print(f"\n")

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



from collections import deque

from src.WebLogs.ReducedLog import ReducedLogV2
from src.WebLogs.WebLogEntry import WebLogEntry


class WebLogReducer(object):
    def __init__(self):
        self.stack = deque()

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

    def GetAReducedLogV2(self):
        rlv2 = ReducedLogV2()
        for entry in self.stack:
            rlv2.IpAddress = entry.IpAddress
            rlv2.UserAgent = entry.UserAgent
            rlv2.Request = entry.Request
            rlv2.ByteRanges.append((entry.LoByte, entry.HiByte))
        return rlv2

    def __Reduce__(self, newEntry: WebLogEntry):
        gotMerged = False

        # work
        for entry in self.stack:
            # Is new overlapping on the hi side of the other?
            if newEntry.IsOverlappingOnHiOf(entry):
                # overlapping!  update entry
                entry.SetLoHi(entry.LoByte, newEntry.HiByte)
                gotMerged = True
                break

            # Is new overlapping on the lo side of the other?
            if newEntry.IsOverlappingOnLoOf(entry):
                # overlapping!  update entry
                entry.SetLoHi(newEntry.LoByte, entry.HiByte)
                gotMerged = True
                break

            # Is new fully contained in the other?
            if newEntry.IsContainedIn(entry):
                # contained, drop new, we received no new information -> leave
                gotMerged = True
                break

            # Is new fully containing the other?
            if newEntry.IsContaining(entry):
                # fully containing, update entry with new, we received no new information
                entry.SetLoHi(newEntry.LoByte, newEntry.HiByte)
                gotMerged = True
                break

            # Is new totally dissociated (below or above) from the other?
            if newEntry.IsDissociatedFrom(entry):
                # new entry, continue
                pass

        if gotMerged:
            if len(self.stack) > 1:
                self.__Reduce__(self.stack.pop())
        else:
            self.stack.append(newEntry)

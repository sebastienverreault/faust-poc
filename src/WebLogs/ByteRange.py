from __future__ import annotations

import faust
from typing import List


class ByteRange(faust.Record):
    LoByte: int
    HiByte: int

    def IsDissociatedFrom(self, entry: ByteRange) -> bool:
        # lo side
        loLoDiff = self.LoByte - entry.LoByte
        loHiDiff = self.LoByte - entry.HiByte
        # hi side
        hiLoDiff = entry.LoByte - self.HiByte
        hiHiDiff = entry.HiByte - self.HiByte
        return (loLoDiff > 1 and loHiDiff > 1) or (hiLoDiff > 1 and hiHiDiff > 1)

    def IsOverlappingOnHiOf(self, entry: ByteRange) -> bool:
        # lo side
        loLoDiff = self.LoByte - entry.LoByte
        loHiDiff = self.LoByte - entry.HiByte
        return loLoDiff >= 1 and loHiDiff <= 1

    def IsOverlappingOnLoOf(self, entry: ByteRange) -> bool:
        # hi side
        hiLoDiff = entry.LoByte - self.HiByte
        hiHiDiff = entry.HiByte - self.HiByte
        return hiLoDiff <= 1 and hiHiDiff >= 1

    def IsContainedIn(self, entry: ByteRange) -> bool:
        # lo side
        loLoDiff = self.LoByte - entry.LoByte
        # hi side
        hiHiDiff = entry.HiByte - self.HiByte
        return loLoDiff >= 0 and hiHiDiff >= 0

    def IsContaining(self, entry: ByteRange) -> bool:
        # lo side
        loLoDiff = entry.LoByte - self.LoByte
        # hi side
        hiHiDiff = self.HiByte - entry.HiByte
        return loLoDiff >= 0 and hiHiDiff >= 0

    def SetLoHi(self, lo: int, hi: int):
        self.LoByte = lo
        self.HiByte = hi

    def __str__(self):
        return f"({str(self.LoByte)}, {str(self.HiByte)})"

    def __repr__(self):
        return self.__str__()


def BRReduce(br_list: List[ByteRange], new_br: ByteRange):
    gotMerged = False
    for a_br in br_list:
        # Is new overlapping on the hi side of the other?
        if new_br.IsOverlappingOnHiOf(a_br):
            # overlapping!  update byte_range
            a_br.SetLoHi(a_br.LoByte, new_br.HiByte)
            gotMerged = True
            break

        # Is new overlapping on the lo side of the other?
        if new_br.IsOverlappingOnLoOf(a_br):
            # overlapping!  update byte_range
            a_br.SetLoHi(new_br.LoByte, a_br.HiByte)
            gotMerged = True
            break

        # Is new fully contained in the other?
        if new_br.IsContainedIn(a_br):
            # contained, drop new, we received no new information -> leave
            gotMerged = True
            break

        # Is new fully containing the other?
        if new_br.IsContaining(a_br):
            # fully containing, update byte_range with new, we received no new information
            a_br.SetLoHi(new_br.LoByte, new_br.HiByte)
            gotMerged = True
            break

        # Is new totally dissociated (below or above) from the other?
        if new_br.IsDissociatedFrom(a_br):
            # new byte_range, continue
            pass

    if gotMerged:
        if len(br_list) > 1:
            BRReduce(br_list.pop())
    else:
        br_list.append(new_br)

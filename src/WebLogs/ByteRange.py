from __future__ import annotations

import faust


class ByteRange(faust.Record, serializer='json'):
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



from __future__ import annotations

import faust
from datetime import datetime


class WebLogEntry(faust.Record, serializer='json'):
    TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
    #
    DATE_OFFSET = 0
    TIME_OFFSET = 1
    IP_ADDRESS_OFFSET = 2
    USER_AGENT_OFFSET = 3
    REQUEST_OFFSET = 4
    STATUS_OFFSET = 5
    BYTE_RANGE_OFFSET = 6

    SUPPORTED_STATUS = [200, 206]

    Key: str
    Timestamp: datetime
    IpAddress: str
    UserAgent: str
    Request: str
    Status: int
    OriginalByteRange: str
    LoByte: int
    HiByte: int
    # ByteRange: str
    Valid: bool

    def Map(row) -> WebLogEntry:
        ret = WebLogEntry('', datetime.now(), '', '', '', 0, '', 0, 0, False)
        try:
            ret.Valid = False
            timestamp = f'{row[WebLogEntry.DATE_OFFSET]} {row[WebLogEntry.TIME_OFFSET]}'
            ret.Timestamp = datetime.strptime(timestamp, WebLogEntry.TIME_FORMAT)
            ret.IpAddress = row[WebLogEntry.IP_ADDRESS_OFFSET]
            ret.UserAgent = row[WebLogEntry.USER_AGENT_OFFSET]
            ret.Request = row[WebLogEntry.REQUEST_OFFSET]
            ret.Status = int(row[WebLogEntry.STATUS_OFFSET])

            ret.Key = "_".join((ret.IpAddress, ret.UserAgent, ret.Request))

            if ret.Status not in WebLogEntry.SUPPORTED_STATUS:
                raise Exception('Unsupported Status: ', f'{ret.Status}')

            ret.OriginalByteRange = row[WebLogEntry.BYTE_RANGE_OFFSET]

            ret.LoByte = int(ret.OriginalByteRange.split('-')[0])
            ret.HiByte = int(ret.OriginalByteRange.split('-')[1])

            # ret.ByteRange = "-".join((ret.LoByte, ret.HiByte))

            if ret.LoByte < 0 or ret.HiByte < ret.LoByte:
                raise Exception('Irregular byte range: ', f'{ret.OriginalByteRange}')
               
            ret.Valid = True
        except Exception as ex:
            print(ex)

        return ret

    # def _get_ByteRange(self) -> str:
    #     return "-".join((self.LoByte, self.HiByte))

    # def _get_Key(self) -> str:
    #     return "_".join((self.IpAddress, self.UserAgent, self.Request))
    
    # ByteRange = property(_get_ByteRange)
    # Key = property(_get_Key)

    def IsDissociatedFrom(self, entry: WebLogEntry) -> bool:
        # lo side
        loLoDiff = self.LoByte - entry.LoByte
        loHiDiff = self.LoByte - entry.HiByte
        # hi side
        hiLoDiff = entry.LoByte - self.HiByte
        hiHiDiff = entry.HiByte - self.HiByte

        return (loLoDiff > 1 and loHiDiff > 1) or (hiLoDiff > 1 and hiHiDiff > 1)

    def IsOverlappingOnHiOf(self, entry: WebLogEntry) -> bool:
        # lo side
        loLoDiff = self.LoByte - entry.LoByte
        loHiDiff = self.LoByte - entry.HiByte

        return (loLoDiff >= 1 and loHiDiff <= 1)

    def IsOverlappingOnLoOf(self, entry: WebLogEntry) -> bool:
        # hi side
        hiLoDiff = entry.LoByte - self.HiByte
        hiHiDiff = entry.HiByte - self.HiByte

        return (hiLoDiff <= 1 and hiHiDiff >= 1)

    def IsContainedIn(self, entry: WebLogEntry) -> bool:
        # lo side
        loLoDiff = self.LoByte - entry.LoByte
        # hi side
        hiHiDiff = entry.HiByte - self.HiByte

        return (loLoDiff >= 0 and hiHiDiff >= 0)

    def IsContaining(self, entry: WebLogEntry) -> bool:
        # lo side
        loLoDiff = entry.LoByte - self.LoByte
        # hi side
        hiHiDiff = self.HiByte - entry.HiByte

        return (loLoDiff >= 0 and hiHiDiff >= 0)


    def SetLoHi(self, lo: int, hi: int):
        self.LoByte = lo
        self.HiByte = hi


    def __str__(self):
        return "\t".join((str(self.Timestamp), self.IpAddress, self.UserAgent, self.Request, str(self.Status), str(self.LoByte), str(self.HiByte)))

    def __repr__(self):
        return self.__str__()



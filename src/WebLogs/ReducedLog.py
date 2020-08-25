import faust
from typing import List, Tuple


class ReducedLog(faust.Record):
    IpAddress: str
    UserAgent: str
    Request: str
    LoByte: int
    HiByte: int


class ReducedLogV2(faust.Record):
    IpAddress: str = ''
    UserAgent: str = ''
    Request: str = ''
    ByteRanges: List[Tuple] = []


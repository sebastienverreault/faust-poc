from __future__ import annotations

from typing import List, Tuple

import faust


class ReducedLog(faust.Record, serializer='json'):
    IpAddress: str
    UserAgent: str
    Request: str
    LoByte: int
    HiByte: int


class ReducedLogV2(faust.Record, serializer='json'):
    IpAddress: str = ''
    UserAgent: str = ''
    Request: str = ''
    ByteRanges: List[Tuple] = []


from __future__ import annotations

import faust


class ReducedLog(faust.Record, serializer='json'):
    IpAddress: str
    UserAgent: str
    Request: str
    LoByte: int
    HiByte: int


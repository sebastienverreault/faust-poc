import faust

from src.WebLogs.WebLogEntry import WebLogEntry


class WebLogJson(faust.Record):
    Timestamp: str
    IpAddress: str
    UserAgent: str
    Request: str
    Status: int
    LoByte: int
    HiByte: int

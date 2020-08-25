import faust

from src.WebLogs.WebLogEntry import WebLogEntry


class WebLogJson(faust.Record, serializer='json'):
    Timestamp: str
    IpAddress: str
    UserAgent: str
    Request: str
    Status: int
    LoByte: int
    HiByte: int


def GetJson(obj: WebLogEntry):
    return WebLogJson(
        Timestamp=str(obj.Timestamp),
        IpAddress=obj.IpAddress,
        UserAgent=obj.UserAgent,
        Request=obj.Request,
        Status=obj.Status,
        LoByte=obj.LoByte,
        HiByte=obj.HiByte
    )

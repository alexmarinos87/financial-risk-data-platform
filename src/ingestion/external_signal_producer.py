from datetime import datetime
from pydantic import BaseModel


class ExternalSignal(BaseModel):
    signal_id: str
    name: str
    value: float
    ts_event: datetime
    source: str

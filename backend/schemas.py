from pydantic import BaseModel
from typing import Optional, Any


class DefaultResponse(BaseModel):
    """Стандартный ответ от API."""
    error: bool
    message: Optional[str]
    payload: Optional[Any]
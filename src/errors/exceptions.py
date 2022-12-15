import traceback
from typing import Callable


class ExceptionsTemplate(Exception):
    
    def __init__(self, message: str) -> None:
        super().__init__(self, message)


class AbstractMethodError(ExceptionsTemplate):
    def __init__(self, function: Callable[[any], any]) -> None:
        self.function = function
        self.message = f"Function {function.__name__} must be implemented."


class ExtractError(ExceptionsTemplate):

    def __init__(self, message) -> None:
        self.message = f"[ERROR] Extract data failed: {message}"
        super().__init__(self.message)
        
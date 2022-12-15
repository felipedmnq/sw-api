from typing import Callable


class ExceptionsTemplate(Exception):
    
    def __init__(self, message: str) -> None:
        self.__message = message
        super().__init__(self)


class AbstractMethodError(ExceptionsTemplate):
    def __init__(self, function: Callable[[any], any]) -> None:
        self.function = function
        self.__message = f"Function {function.__name__} must be implemented."
        
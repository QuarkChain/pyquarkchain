class BaseCancelTokenException(Exception):
    """
    Base exception class for the `asyncio-cancel-token` library.
    """

    pass


class EventLoopMismatch(BaseCancelTokenException):
    """
    Raised when two different asyncio event loops are referenced, but must be equal
    """

    pass


class OperationCancelled(BaseCancelTokenException):
    """
    Raised when an operation was cancelled.
    """

    pass

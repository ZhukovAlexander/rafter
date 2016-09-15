class NotLeaderException(Exception):
    """Raised when server is not a leader"""


class UnboundExposedCommand(Exception):
    """Raised when the command is not bound to a service instance"""


class UnknownCommand(Exception):
    """Raised when someont tries to invoka an unknown command"""

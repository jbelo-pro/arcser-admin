
class ServerException(Exception):
    """  Exception to catch errors during server administration """
    def __init__(self, message, errors):
        super().__init__(message)
        self.errors = errors
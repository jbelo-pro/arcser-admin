
class ServerException(Exception):
    """  Exception to catch errors during server administration """
    def __init__(self, message, errors):
        super().__init__(message)
        self.errors = errors


def slicer(list_, n):
    for i in range(0, len(list_), n):
        yield list_[i:i + n]




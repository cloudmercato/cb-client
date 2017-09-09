class CbClientError(Exception):
    pass


class WringerNotFound(CbClientError):
    pass


class ServerError(CbClientError):
    pass


class ParseError(CbClientError):
    pass

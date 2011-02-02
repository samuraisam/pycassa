__all__ = ['NotFoundException', 'InvalidRequestException',
           'AuthenticationException', 'AuthorizationException',
           'UnavailableException', 'TimedOutException']

class WhyException(Exception):

    def __init__(self, why=None):
        self.why = why

class NotFoundException(WhyException): pass
class InvalidRequestException(WhyException): pass
class UnavailableException(WhyException): pass
class TimedOutException(WhyException): pass
class AuthenticationException(WhyException): pass
class AuthorizationException(WhyException): pass
class IncompatibleAPIException(Exception): pass

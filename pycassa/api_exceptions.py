__all__ = ['NotFoundException', 'InvalidRequestException',
           'AuthenticationException', 'AuthorizationException',
           'UnavailableException', 'TimedOutException']

class NotFoundException(Exception): pass
class InvalidRequestException(Exception): pass
class UnavailableException(Exception): pass
class TimedOutException(Exception): pass
class AuthenticationException(Exception): pass
class AuthorizationException(Exception): pass

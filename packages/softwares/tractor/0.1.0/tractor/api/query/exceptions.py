class TractorQueryError(Exception):
    """Base class for query module exceptions."""
    pass

class PasswordRequired(TractorQueryError):
    """Raised when a password must be specified in the EngineClient,
    If the default client, ModuleEngineClient, is used, this can
    be specified using setEngineClientParam(password=some_password).
    """
    pass

class InvalidValue(TractorQueryError):
    """Raised when a parameter value is not valid or of the correct type."""
    pass

class MissingSearchClause(TractorQueryError):
    """Raised when a search clause has not been specified."""
    pass

class MissingTargetKey(TractorQueryError):
    """Raised when a dictionary or EngineDB.Row object is used to
    guide a search and at least one required attribute is missing.
    """
    pass

class MissingParameter(TractorQueryError):
    """Raised when a required parameter has not been specified."""
    pass

class SortNotAllowed(TractorQueryError):
    """Raised when sorting is requested on items
    guide a search and at least one required attribute is missing.
    """
    pass


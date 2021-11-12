class AuthorError(Exception):
    """Base class for author module exceptions."""
    pass


class RequiredValueError(AuthorError):
    """Raised if an attribute must be defined when emitting an element."""
    pass


class ParentExistsError(AuthorError):
    """Raised when attempting to add a task to multiple parent tasks."""
    pass


class SpoolError(AuthorError):
    """Raised when there is a problem spooling a job."""
    pass

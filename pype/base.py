"""
Collection module for all exceptions and 'constants'
"""
class Default(object):
    """
    A dummy class that can be instanced for default values that
    should never be equal.

    equality checks will fail for `Default() == Default()`.
    """
    pass


class Generic(object):
    """
    A dummy class to represent generic types in pipelines.

    Use `pype.generic` as shorthand.
    """
    pass


class Error(Exception):
    """
    Base exception class of exceptions raised by `pype`.
    """
    def __init__(self, string, *args, **kwargs):
        super(Error, self).__init__(string.format(*args, **kwargs))


class ConfigurationError(Error):
    """
    Exception raised when there was a problem applying configuration
    to a pype pipe.
    """
    pass


class PipeError(Error):
    """
    Exception raised when there is a problem in the pipeline verifying.
    """
    pass


# All the attributes (and their defaults) used by `pype` on functions.
default_pipe_variables = {
    'output_name': Default(),
    'input_name' : Default(),
    'output_type': None,
    'input_type' : None,
    'pass_state' : False,
    'buffered'   : False,
}


def copy_pipe_variables(original, new):
    for attribute, default in default_pipe_variables.items():
        setattr(new, attribute, getattr(original, attribute, default))
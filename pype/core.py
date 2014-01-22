from __future__ import absolute_import

from . import base
from . import util

import functools
import collections
import inspect


def input(name, type=None):
    """
    Specifies the input type expected by the generator. This is used
    to check compatiblity of pipes at pipeline creation.
    """
    def input(function):
        function.input_type = type
        function.input_name = name
        return function
    return input


def output(name, type=None):
    """
    Specifies the output type of the generator. This is used
    to check compatibility of pipes at pipeline creation.
    """
    def output(function):
        function.output_type = type
        function.output_name = name
        return function
    return output


def state(function):
    """
    Specifies that the decorated generator wants a state
    variable to be passed along with the result from the
    previous generator in the pipeline.

    This changes the `pipe` iterator to return (state, data)
    tuples.

    `state` can be None if no state was previously set for
    this piece of data.
    """
    function.pass_state = True
    return function


def consume(generator):
    """
    Consumes a generator fully. Returns no result.

    Any exceptions are propagated.
    """
    d = collections.deque(maxlen=0)
    d.extend(generator)


class State(dict):
    """
    An immutable dictionary, keys can only be set at creation.
    """
    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            return super(State, self).__getattr__(key)

    def mutate(self, **kwargs):
        """
        Updates a copy of self and returns it.

        This is about equal to the following code:
            copied = self.copy()
            copied.update(kwargs)
            return copied
        """
        c = self.copy()
        c.update(kwargs)
        return type(self)(c)

    def raise_type_error(self, *args, **kwargs):
        raise TypeError("State object can't be mutated")

    __setattr__ = raise_type_error
    __delattr__ = raise_type_error
    __setitem__ = raise_type_error
    __delitem__ = raise_type_error
    update      = raise_type_error
    pop         = raise_type_error
    popitem     = raise_type_error



@util.call()
class NoDefaultValue(object):
    def __eq__(self, other):
        return isinstance(other, self.__class__)

    def __repr__(self):
        return "NoDefaultValue"


class config(object):
    """
    Decorator that allows your pipe to be configured according to
    the argument spec of the function. This is best explained
    with an example.

        Take the following function:

        @config()
        def test(pipe, a, b, c=10):
            print (a + b) * c

        Normal `pipeline` construction would only supply the `pipe` argument,
        this would clearly create an error of sorts. However passing a config
        dict to the pipeline constructor or to your `test` function locally
        will fix this issue.

        Take for example `{'a': 2, 'b': 3}` as your config of choice, now to apply
        these to the `test` pipe, we can do `test = test.apply(our_dict)`. This
        will return a new `config` instance wrapping the original function, except
        now it will have arguments to supply when called.

        Thus when we do `test(None)` (the `None` as pipe replacement) both
        `a` and `b` will be filled in for us.

        This will print `50` since it will be called as `test(None, a=2, b=3, c=10)`.

    There are also various options that can be passed to the decorator to adjust behaviour:

        `name`: A name to prefix options with, take `name="test"` and the example above
                your config would need to look like `{'test.a': 2, 'test.b': 3}` and it
                will be resolved for you. This is to avoid argument name collision.

                You can omit the prefix if doing local direct `test.apply` calls. But
                they're required when passing a global configuration.


        `redirect`: A different function to use for argument spec reading. This makes the
                    decorator skip over the original function and read the argument spec
                    from the function pointed to by `redirect`.

                    This is helpful if we use a different generator inside the function.

        `only_with_defaults`: Only arguments that have a default value will be filled.
                              This plain **ignores** any arguments that do not have a default
                              value associated with them.

        `without`: An iterator of argument names to skip over and not export as configurable.
    """
    def __init__(self, name=None, redirect=None, only_with_defaults=False, without=None):
        super(config, self).__init__()

        self.prefix = name
        self.redirect = redirect
        self.only_with_defaults = only_with_defaults
        self.without  = without

        self.function = None
        self.config   = {}

    def apply(self, local=False, **config):
        c = self.copy()
        c.config.update(config)
        return c

    def copy(self):
        c = type(self)(
                          name=self.prefix,
                      redirect=self.redirect,
            only_with_defaults=self.only_with_defaults,
                       without=self.without,
        )
        c.function           = self.function
        c.config             = self.config.copy()
        c.possible_arguments = self.possible_arguments
        return c

    def __call__(self, *args, **kwargs):
        if self.function:
            return self._call_wrapped(*args, **kwargs)

        self._setup_wrapped(*args, **kwargs)
        return self

    def _setup_wrapped(self, function):
        """
        Reads out arguments and filters them depending on options
        passed to the decorator.
        """
        self.function = function

        function.redirected_configuration = self.redirect

        to_read_config = function

        # Resolve the redirection chain.
        while getattr(to_read_config, "redirected_configuration", None):
            to_read_config = getattr(to_read_config, "redirected_configuration")

        possible_arguments = self._extract_arguments(to_read_config)

        if not self.redirect:
            # Remove the first argument since it `should` be the previous pipe always
            possible_arguments = possible_arguments[1:]

        if self.only_with_defaults:
            # Remove any arguments that don't have a default value
            possible_arguments = [(arg, default) for arg, default in
                                  possible_arguments if default is not NoDefaultValue]

        if self.without:
            # Remove any arguments that were explicitely marked as to be
            # not used by the `without` argument.
            possible_arguments = [(arg, default) for arg, default in
                                  possible_arguments if (arg not in self.without)]

        if self.prefix:
            # Prepend all the arguments with our configuration name to avoid
            # name collision, and make it clearer to the configuration writer
            # where each option goes.
            possible_arguments = [('.'.join((self.prefix, arg)), default) for arg, default
                                  in possible_arguments]

        self.possible_arguments = possible_arguments

    def _call_wrapped(self, pipe, **config):
        # Update the bottom-most with the rest
        config.update(self.config)

        # Get the arguments we can use and any default values
        options = self.get_arguments(config)

        # Now call our wrapped function
        return self.function(pipe, **options)

    @staticmethod
    def _clean(name):
        """
        Removes any prefixes (separated by a period) from a configuration
        options name.

        'test.name' -> 'name'
        'name'      -> 'name'
        """
        if '.' not in name:
            return name
        _, cont = name.split('.', 1)
        return cont

    @staticmethod
    def _extract_arguments(function):
        """
        Extracts arguments from self.function, returns a list of tuples
        in the format of (argument, default) where default can be
        `NoDefaultValue` indicating there was no default value.

        This function currently ignores *args and **kwargs type
        arguments.
        """
        argspec = inspect.getargspec(function)

        options = []

        defaults = argspec.defaults or []
        args     = argspec.args     or []

        if defaults:
            # First we put in the arguments we know have no default value
            other_args = args[:- len(defaults)]

            for name in other_args:
                options.append((name, NoDefaultValue))

            # We need to align the rest of args with default values
            args_with_defaults = args[- len(defaults):]

            for name, default in zip(args_with_defaults, argspec.defaults):
                options.append((name, default))
        else:
            options = [(name, NoDefaultValue) for name in args]

        return options

    def get_arguments(self, config):
        """
        Extracts arguments for the wrapped function and returns
        a dictionary of (parameter, value).

        If any of the required parameters is missing a value in
        the passed `config`, this method will raise ConfigurationError
        """
        arguments = {}
        for name, default in self.possible_arguments:
            if name in config:
                arguments[self._clean(name)] = config[name]
            elif default is not NoDefaultValue:
                arguments[self._clean(name)] = default
            else:
                raise base.ConfigurationError("Missing required configuration {:s}", name)

        return arguments

    def __repr__(self):
        if self.function:
            return repr(self.function)
        return super(config, self).__repr__()

    def __str__(self):
        if self.function:
            return str(self.function)
        return super(config, self).__str__()

    def __getattr__(self, attribute):
        if attribute in base.default_pipe_variables:
            return getattr(self.function, attribute)
        return super(config, self).__getattr__(attribute)

    def __setattr__(self, attribute, value):
        if attribute in base.default_pipe_variables:
            return setattr(self.function, attribute, value)
        return super(config, self).__setattr__(attribute, value)

    def __delattr__(self, attribute):
        if attribute in base.default_pipe_variables:
            return delattr(self.function, attribute)
        return super(config, self).__delattr__(attribute)
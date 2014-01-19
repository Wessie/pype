from __future__ import absolute_import

import pype

import pytest


@pytest.fixture
def simple_function():
    def function(pipe, x=10):
        return x
    return function


@pytest.fixture
def larger_function():
    def function(pipe, u, x=10, y=20, z=30):
        return u + x + y + z
    return function


@pytest.fixture
def use_larger_function():
    def function(pipe, **config):
        return pipe(None, 40, **config)
    return function


def test_config_copies(simple_function):
    c = pype.config()(simple_function)

    # Make sure we return actual copies
    assert c is not c.copy()
    assert c is not c.apply()
    # Check if we can call a copy
    # and it returns the same thing
    assert c(None) == c.copy()(None)


def test_config_passthrough(simple_function):
    c = pype.config()(simple_function)

    assert c(None) == 10


def test_config_repr(simple_function):
    c = pype.config()(simple_function)

    assert repr(c) == repr(simple_function)
    assert str(c)  == str(simple_function)


def test_config_apply(simple_function):
    c = pype.config()(simple_function)

    # Control assert
    assert c(None) == 10
    # Check the apply
    assert c.apply(x=20)(None) == 20
    # Another control to make sure we had a copy
    assert c(None) == 10


def test_config_prefix_apply(simple_function):
    c = pype.config(name='test')(simple_function)

    options = {'test.x': 50}

    assert c.apply(**options)(None) == 50


def test_config_without(simple_function):
    c = pype.config(without=['x'])(simple_function)

    # Config should be ignoring our 'x'
    assert c.apply(x=20)(None) == 10


def test_config_redirect(use_larger_function, larger_function):
    # We're not removing the 'u' argument here, so we get
    # an exception when we call it later
    c = pype.config(redirect=larger_function)(use_larger_function)

    with pytest.raises(pype.ConfigurationError):
        assert c(larger_function) == 100

    with pytest.raises(pype.ConfigurationError):
        assert c.apply(x=60)(larger_function) == 150

    # Here we correctly remove 'u' from possible configuration passed
    # arguments and we can safely call it afterwards
    c = pype.config(redirect=larger_function, only_with_defaults=True)
    c = c(use_larger_function)

    assert c(larger_function) == 100
    assert c.apply(x=60)(larger_function) == 150


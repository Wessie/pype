from __future__ import absolute_import

import pype

import pytest


@pytest.fixture
def simple_function():
    def function(pipe, x=10):
        return x
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
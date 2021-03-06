from __future__ import absolute_import, division

from pype import core, base, engine

import pytest


def consume_with_counter(generator):
    i = 0
    for _ in generator:
        i += 1
    return i

def test_verify_pipe_types_with_invalid(invalid_pipeline):
    with pytest.raises(engine.PipeError):
        engine.verify_pipe_types(invalid_pipeline)


def test_verify_pipe_types_with_valid(valid_pipeline):
    engine.verify_pipe_types(valid_pipeline)


def test_initialize_pipe_variables_partial(string_generator):
    engine.initialize_pipe_variables(string_generator)

    g = string_generator

    assert g.output_name  == "string"
    assert g.input_name   == "string"
    assert g.output_type  is None
    assert g.input_type   is None
    assert g.pass_state   == False


def test_initialize_pipe_variables_full(plain_generator):
    engine.initialize_pipe_variables(plain_generator)

    g = plain_generator

    assert isinstance(g.output_name, base.Default)
    assert isinstance(g.input_name, base.Default)

    assert g.input_type  is None
    assert g.output_type is None
    # Default values for input and output should never be equal
    assert g.input_name  is not g.output_name
    # Passing state should always default to False for convenience
    assert g.pass_state  == False


def test_simple_pipeline(valid_pipeline):
    assert consume_with_counter(engine.pipeline(*valid_pipeline)) == 4950


def test_invalid_pipeline(invalid_pipeline):
    with pytest.raises(engine.PipeError):
        engine.pipeline(*invalid_pipeline)


def test_simple_state_pipeline(state_pipeline):
    assert consume_with_counter(engine.pipeline(*state_pipeline)) == 3921225
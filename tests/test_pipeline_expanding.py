from __future__ import unicode_literals
import pytest
from future.builtins import str, bytes, isinstance

import pype
import pype.engine


@pype.state
@pype.config()
def gen_with_state(pipe):
    yield pipe


@pype.config()
def gen_without_state(pipe):
    yield pipe


def test_pipeline_without_state():
    """
    pipeline version that should optimize out all state
    related things because there is no state using
    pipes in the pipeline.
    """
    pipes = create_pipes(
        gen_without_state,
        gen_without_state,
        gen_without_state,
    )

    result = pype.engine.initialize_pipeline_state_handling(pipes)

    compare_pipelines(result, pipes)


def test_pipeline_with_scattered_state():
    """
    Scattered state pipes though the pipeline.
    """
    pipes = create_pipes(
        gen_without_state,
        gen_with_state,
        gen_with_state,
        gen_without_state,
        gen_without_state,
    )

    result = pype.engine.initialize_pipeline_state_handling(pipes)

    compare_pipelines(
        result,
        [
         'gen_without_state',
         '_create_state',
         'gen_with_state',
         'gen_with_state',
         'remove_state',
         'gen_without_state',
         'gen_without_state',
        ],
    )


def test_pipeline_with_full_state():
    """
    Pipeline that has pipes with state fully. Initializer should
    only create state at the front of the pipe and optimize out
    any others.
    """
    pipes = create_pipes(
        gen_with_state,
        gen_with_state,
        gen_with_state,
        gen_with_state,
    )

    result = pype.engine.initialize_pipeline_state_handling(pipes)

    compare_pipelines(
        result,
        [
         '_create_state',
         'gen_with_state',
         'gen_with_state',
         'gen_with_state',
         'gen_with_state',
         ],
    )


def compare_pipelines(pipeline, expected):
    """
    Asserts `pipeline` and `expected` are equal after
    passing each to `convert_to_names`.
    """
    pipeline = convert_to_names(pipeline)
    expected = convert_to_names(expected)

    assert pipeline == expected


def convert_to_names(objs):
    """
    Returns __name__ of each object unless
    object is already a string. In which case
    the string is returned as is.
    """
    res = []

    for obj in objs:
        if isinstance(obj, (str, bytes)):
            res.append(obj)
        else:
            res.append(obj.__name__)

    return res


def create_pipes(*pipes):
    """
    Run all the given pipes through initialization equal
    to the initialization done in the engine.
    """
    for pipe in pipes:
        pype.engine.initialize_pipe_variables(pipe)

    return pipes

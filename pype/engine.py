from __future__ import absolute_import

import functools

from . import core
from .base import default_pipe_variables, PipeError, Generic


def pipeline(*pipeline, **config):
    for pipe in pipeline:
        initialize_pipe_variables(pipe)

    verify_pipe_types(pipeline)

    pipeline = initialize_pipeline_state_handling(pipeline)

    last = None
    for pipe in pipeline:
        if isinstance(pipe, core.config):
            last = pipe(last, **config)
        else:
            last = pipe(last)

    return last


def verify_pipe_types(pipes):
    """
    Verifies that all pipes have the correct input/output type according
    to their neighbours in the pipeline.

    raises `PipeError` if an incompatiblity is found.
    """
    attributes_to_check = (
        ("output_type", "input_type"),
        ("output_name", "input_name"),
    )

    previous_pipe = pipes[0]
    for pipe in pipes[1:]:
        for out_attr, in_attr in attributes_to_check:

            out_attr_value = getattr(previous_pipe, out_attr)
            in_attr_value = getattr(pipe, in_attr)

            if (out_attr_value != in_attr_value and
                    not isinstance(out_attr_value, Generic) and
                    not isinstance(in_attr_value, Generic)):
                raise PipeError(
                    "Incompatible output/input found: "
                    "(output: {:s} from {:s}) (input: {:s} to {:s})",
                    out_attr_value, previous_pipe.__name__,
                    in_attr_value, pipe.__name__)

        previous_pipe = pipe


def initialize_pipe_variables(pipe):
    """
    Sets all possible attributes on a pipe function to their
    default value.

    Does not touch attributes already set
    """
    for attribute, default in default_pipe_variables.items():
        setattr(pipe, attribute, getattr(pipe, attribute, default))


def initialize_pipeline_state_handling(pipes):
    first_pipe = pipes[0]

    stated_pipes = []
    # Shortcut for readability
    append = stated_pipes.append

    # Indicates if we've any state at all in the pipeline.
    first = True
    # Our first pair of state wrappers
    remove, add = _create_state_pair()

    pipe_iter = iter(pipes)
    # Do a first pass to insert our first state
    for pipe in pipe_iter:
        if pipe.pass_state:
            append(_create_state)
            append(pipe)

            break
        else:
            append(pipe)
    else:
        # We exited without any state, so just return
        # the original
        return pipes

    no_state = False
    # finish the rest of the pipes normally.
    for pipe in pipe_iter:
        if (pipe.pass_state or pipe.buffered) and no_state:
            # There is no state, but we want state.
            append(add)
            append(pipe)

            no_state = False

            # Create new pair because we used the last one.
            remove, add = _create_state_pair()
        elif not (pipe.pass_state or pipe.buffered) and not no_state:
            # There is state, and we don't want any.
            append(remove)
            append(pipe)

            no_state = True
        else:
            append(pipe)

    return stated_pipes


def _create_state_pair():
    """
    Creates a pair of functions that respectively remove a
    state instance and add a state instance from/to a generator.
    """
    last_state = [None]
    def remove_state(pipe):
        for state, data in pipe:
            last_state[0] = state
            yield data

    def add_state(pipe):
        for data in pipe:
            yield last_state[0], data

    return remove_state, add_state


def _create_state(pipe):
    # If we're the front we don't get any data, so just
    # pass None and a new state. Otherwise passthrough
    # any data we get from the previous pipe with a
    # new state.
    if pipe is None:
        while True:
            yield core.State(), None
    else:
        for data in pipe:
            yield core.State(), data
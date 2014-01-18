from __future__ import absolute_import

import functools

from . import core

class DefaultName(object):
    pass


default_pipe_variables = {
    'output_name': DefaultName(),
    'input_name' : DefaultName(),
    'output_type': None,
    'input_type' : None,
    'pass_state' : False,
    'buffered'   : False,
}


class PipeError(core.Error):
    pass


def pipeline(*pipeline):
    for pipe in pipeline:
        initialize_pipe_variables(pipe)

    verify_pipe_types(pipeline)

    pipeline = initialize_pipeline_state_handling(pipeline)

    last = None
    for pipe in pipeline:
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

            if out_attr_value != in_attr_value:
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
    # Indicates if we're in a stateless block
    in_block = False

    for pipe in pipes:
        if first:
            if pipe.pass_state:
                append(_create_state)
                append(pipe)
                first = False
            else:
                append(pipe)
        else:
            if pipe.pass_state or pipe.buffered:
                append(add)
                append(pipe)
                in_block = False

                # Used up our remove, add pair
                remove, add = _create_state_pair()
            elif in_block:
                append(pipe)
            else:
                append(remove)
                append(pipe)
                in_block = True

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
    for data in pipe:
        yield core.State(), data
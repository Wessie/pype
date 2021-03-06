from __future__ import absolute_import
import functools
import threading
try:
    import queue
except ImportError:
    import Queue as queue

from . import base


def buffered(buffersize, chunksize):
    """
    Buffers the output of the decorated generator.

    This is done with a new thread that executes the generator.

    :param buffersize: The maximum amount of chunks in the queue used between threads
    :param chunksize: The size of chunks used to move between threads

    To get the total amount of 'yields' that can be put in the buffer you can multiply
    `buffersize` and `chunksize`. It is suggested to fiddle around with the sizes in
    tests to determine the best size to use.
    """
    def buffered(function, *args, **kwargs):
        @functools.wraps(function)
        def buffered(*args, **kwargs):
            queue_buffer = queue.Queue(maxsize=buffersize)

            # Create ourself a sentinal to use as StopIteration indicator.
            exit = object()

            def threaded_generator(function, *args, **kwargs):
                # Preallocating is slightly faster than resizing
                chunk = [exit] * chunksize
                index = 0

                for x in function(*args, **kwargs):
                    chunk[index] = x

                    index += 1

                    if index >= chunksize:
                        queue_buffer.put(chunk[:])
                        index = 0

                queue_buffer.put(chunk[:index])
                queue_buffer.put(exit)

            run(threaded_generator, function, *args, **kwargs)

            cont = True
            while True:
                chunk = queue_buffer.get()

                # The method to detect the sentinal below is faster
                # than putting an 'if' statement in the for loop
                # below.
                if chunk is exit:
                    break

                for x in chunk:
                    yield x

        buffered.buffered = True
        base.copy_pipe_variables(function, buffered)

        return buffered
    return buffered


def run(function, *args, **kwargs):
    thread = threading.Thread(target=function, args=args, kwargs=kwargs)
    thread.daemon = True
    thread.start()


def call(*args, **kwargs):
    """
    Calls the decorated function or class with the arguments given and
    returns the result.
    """
    def call(func):
        return func(*args, **kwargs)
    return call


class Err(object):
    def __init__(self, name):
        super(Err, self).__init__()
        self.name = name

    def __repr__(self):
        return self.name
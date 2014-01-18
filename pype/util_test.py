from __future__ import absolute_import
import time

from . import util

import pytest


def test_call_decorator():
    function = lambda x: x*10

    res = util.call(5)(function)

    assert res == 50


def test_run_utility():
    res = {}

    def tester(x, y=10):
        time.sleep(1)

        res['x'] = x
        res['y'] = y

    util.run(tester, 60, y=50)

    time.sleep(2)

    assert res == dict(x=60, y=50)


@pytest.mark.parametrize("number_of_numbers",
        [10, 100, 1000, 10000])
def test_buffered_decorator(number_of_numbers):
    buffered_range = util.buffered(10, 10)(range)

    res = list(buffered_range(1000))

    assert len(res) == 1000

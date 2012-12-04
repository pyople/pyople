# -*- coding: utf-8 -*-

from pyople.tasks.models import multiprocess, list_mp


@multiprocess
def sum_values(a, b):
    return a + b


@multiprocess
def itersum(a, b):
    return list_mp([sum_values(i, b) for i in a])

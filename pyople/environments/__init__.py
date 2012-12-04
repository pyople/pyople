"""Module containing the different environments on which
multiprocess tasks will be executed.
The possibles environments are:
    - simple: tasks are executed as usual.
    - multiprocessor: tasks are executed using different instances.
    - development: emulate the multiprocess environment with a unique instance.
        Used for debugging.
"""

from development import WaitingTask, DevResult, DevelopmentEnvironment
from multiprocessor import MultiprocessEnvironment
from simple import simple_environment, SimpleCall

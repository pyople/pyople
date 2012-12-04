"""All mutiprocess tasks are imported from this module."""

__all__ = []
from pyople.tasks.models import *
from pyople.tasks.test_tasks import *
from pyople.tasks.task import *


for name, x in globals().items():
    try:
        if isinstance(x, MultiprocessTask):
            __all__.append(name)
    except:
        pass

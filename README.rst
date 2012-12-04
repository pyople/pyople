====================================
pyople: simple clustering in python  
====================================

What is it
==========

**Pyople** provides simple yet flexible way to create an distribute tasks and sub-tasks in a cluster using ZeroMQ and Python. 
Only adding a decorator above a function the task will be executed in parallel. 

Clustering shouldn't be too much complicated. Not in Python. 

In the file:
::

    @multiprocess
    def sum(a, b):
        return a + b
    
    @multiprocess
    def itersum(a, b):
        return list_mp([sum(i, b) for i in a])
  
In the terminal:
::

    >>> a = [1, 1, 2 ,3 ,5 ,8]
    >>> b = 1
    >>> itersum(a, b)

Management of tasks distribution, creation of sub-tasks in parallel and results recollection is transparent to the user. 
Each task follows a classic pipeline scheme: one fan starts the task, the worker runs it and a collector obtains the result.  

Where to get it
===============

The source code is hosted on GitHub at: http://github.com/pyople

Binary installers for the latest released version are available at the Python
package index::

    http://pypi.python.org/pypi/pyople/

And via ``easy_install`` or ``pip``::

    easy_install pyople
    pip install pyople

Licence
=======

BSD
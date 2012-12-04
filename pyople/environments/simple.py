import pyople.tasks


def simple_environment():
    """Set the environment to the simple environment."""
    s = SimpleCall()
    from tasks.models import MultiprocessTask
    MultiprocessTask.set_environment(s)


class SimpleCall():
    """Environment without multiprocess. The tasks are executed as usual."""
    def add_call(self, procedure, num_output, args, kwargs, code_type):
        """
        Call the funtion as a usual call.

        Arguments:
            - procedure: name of the function to be executed.
            - num_output: number of outputs of the function.(unused)
            - args: arguments used in the call of the function.
            - kwargs: key arguments used in the call of the function.
            - code_type: Type of the MultiCode used.(unused)
        """
        return getattr(tasks, procedure).func(*args, **kwargs)

from pyople.pipeline.worker import Worker, Result, BasicMultiCode
from pyople.pipeline.ventilator import TaskVentilatorData
from zmq.utils.jsonapi import loads as zmq_loads
from zmq.utils.jsonapi import dumps as zmq_dumps
from pyople.tasks.models import decode_output, split_output
import warnings
import pyople.tasks as tasks


class WaitingTask(TaskVentilatorData):
    """Override TaskVentilatorData class allowing DevelopmentEnvironment
    to emulate Ventilator tasks.

    Substitutes references to zmq sockets and database accesses
    using DevelopmentEnvironment attributes."""
    def _update_depend(self):
        """Overridden function. Substitutes the redis_manager reference
        calling _update_dependencies of the attribute ventilator."""
        if self.task_depend:
            for code in self.task_depend:
                self.ventilator._update_dependencies(code)

    def _send_sink_data(self):
        """Overridden function, add the task to the done tasks and update
        the attributes of the ventilator."""
        self.ventilator.global_done_task.add(self.code)
        self.ventilator._update_to_do_tasks(self.code)

    def send_worker(self):
        """Overridden function, set the task to the ready tasks."""
        res = self.ventilator.global_ready_task.setdefault(
                self.code, [self.procedure.name])
        data = self.wait_exists()
        res.extend(data)
        if not self._has_kwargs():
            res.append(zmq_dumps([]))
        if self.ventilator.display:
            print('Send: ' + str([self.code, self.procedure.name, data]))
        if self.self_needed:
            self.db_delete_needed()

    def wait_exists(self):
        """Overridden function, get the data from the ventilator
        global_data attribute."""
        data_dict = self.ventilator.global_data
        return [data_dict[task][int(num)] for [task, num]
         in map(split_output, self.needed)]

    def db_set(self, data):
        """Overridden function, set the data to the ventilator
        global_data attribute."""
        self.ventilator.global_data[self.code] = data

    def db_delete_needed(self):
        """Overridden function, delete the intern data of the ventilator
        global_data attribute."""
        if self.code in self.ventilator.global_data:
            del self.ventilator.global_data[self.code]


class DevResult(Result):
    """Override Result class allowing DevelopmentEnvironment
    to emulate Worker tasks.

    Substitutes references to zmq sockets using DevelopmentEnvironment
    attributes."""
    def _send_task(self, code):
        """
        Overridden function, the task is added to the ventilator
        global_wait_task attribute, emulating the ventilator task registration.
        """
        if self.worker.display:
            print('Send: ' + str([code, self.procedure, self.needed]))
        task_depend = set([decode_output(str(n)) for n in self.needed])
        task = WaitingTask(code, self.needed, self.procedure, self.worker)
        self_needed = code in task_depend
        if self_needed:
            task_depend.remove(code)
        task.set_left(task_depend, self_needed)
        if task_depend:
            for n in task_depend:
                list_task = self.worker.global_wait_task.setdefault(n, [])
                list_task.append(task)
        else:
            task.update_data(0)

    def _send_task_data(self, code):
        """Overridden function, add data to the ventilator global_data
        attribute. If the task is a zero task also update the done tasks."""
        if self.worker.display:
            print('Send data: ' + str([code, self.data]))
        res = self._development_dump_data(code)
        self.worker.global_data[code] = res
        if not self.send_task:
            self.worker.global_done_task.add(code)
            self.worker._update_to_do_tasks(code)

    def _send_task_worker(self, code):
        """
        Overridden function, the task is added to the ventilator
        global_ready_task attribute.
        """
        if self.worker.display:
            print('Send to worker: ' +
                  str([code, self.procedure, self.data]))
        res = self.worker.global_ready_task.setdefault(
                code, [self.procedure])
        res.extend(self._development_dump_data(code))
        if not self.has_kwargs:
            res.append(zmq_dumps([]))

    def _development_dump_data(self, code):
        """Auxiliary function for dumping data."""
        try:
            res = [zmq_dumps(d) for d in self.data]
        except TypeError:
            raise TypeError("Data of task %s with procedure %s not "
                            "serializable" % (code, self.procedure))
        return res


class DevelopmentEnvironment(Worker):
    """Environment emulating the multiprocess environment using a unique
    process. The flow of task, including creation and deletion, is the same as
    the multiprocess environment.
    Although the class extends the Worker class, Ventilator, Sink and
    RedisManager are also simulated in the environment.

    Principal uses of this environment are:
        - Check the correct execution of a task in the
            multiprocess environment.
        - Debugging the execution using multiprocess environment.

    Differences with the multiprocess environment:
        - No zmq sockets are used. (Task messages are encoded as usual)
        - No database is used for storing message data. (Task data is encoded
            as usual)
        - No processes or thread are opened.

    Arguments:
        - display: Define if the class prints information during execution.
        - code: Name of the code of the first task executed.
            By default is as the first server code. Can be defined if it is
            desired to emulate a task with a specified code name.
        - mongodb_name: Name of the mongodb database.
    """
    def __init__(self, display=False, code='0', mongodb_name=''):
        zmq_transport = 'inproc'
        io_threads = 1
        tornado_sockets = True
        group_id = '0'
        self.running = False
        self.init_code = code
        db_num = 1
        context = None
        Worker.__init__(self, zmq_transport, tornado_sockets,
                                    display, context, io_threads, db_num,
                                    group_id)
        self.result_class = DevResult
        from pyople.tasks.models import MultiprocessTask
        self.mongodb_name = mongodb_name
        self.connect_mongodb(mongodb_name)
        MultiprocessTask.set_environment(self)

    def _start_environment(self):
        """Set the intern attributes to default when execution starts."""
        # Emulate the poll of from_socket of the Worker.
        # Tasks to be executed are added to the dictionary
        # with the codes as keys and the message as values.
        self.global_ready_task = {}
        # Emulate the database for data. Used as a dict like the database.
        self.global_data = {}
        # Emulate the Ventilator attribute tasks.
        self.global_wait_task = {}
        # Emulates the RedisManager dependencies.
        self.global_dependencies = {}
        # Tasks already done. A task is deleted if it hasn't dependencies.
        self.global_done_task = set()
        self.running = True

    def send_dependencies(self):
        """Override Worker function. Dependencies are saved to the attribute
        global_done_task, emulating the RedisManager dependencies."""
        if self.task_dependencies:
            if self.display:
                print('Send depend: ' + str(self.task_dependencies))
            send_depend = self.task_dependencies.items()
            for code, [_, dep] in send_depend:
                len_dep = len(dep)
                if code in self.global_dependencies:
                    num_dep = self.global_dependencies[code]
                    if len_dep == -num_dep:
                        del self.global_dependencies[code]
                        del self.global_data[code]
                        if code in self.global_done_task:
                            self.global_done_task.remove(code)
                    else:
                        self.global_dependencies[code] = num_dep + len_dep
                else:
                    self.global_dependencies[code] = len_dep

    def _update_dependencies(self, code):
        """Emulate the RedisManager dependencies update."""
        if code in self.global_dependencies:
            dep = self.global_dependencies[code]
            if dep == 1:
                del self.global_dependencies[code]
                del self.global_data[code]
                if code in self.global_done_task:
                    self.global_done_task.remove(code)
            else:
                self.global_dependencies[code] = dep - 1
        else:
            self.global_dependencies[code] = -1

    def _update_wait_tasks(self):
        """
        Update the global_done_task and global_wait_task after the execution
        of a task. Emulate the sending of message between Ventilator and Sink
        tasks. Ready tasks are moved from global_wait_task to global_done_task.
        """
        [self._update_to_do_tasks(key) for key in self.global_done_task
         if key in self.global_wait_task]
        self.global_done_task.intersection_update(self.global_wait_task)

    def _update_to_do_tasks(self, code):
        """
        Update the task of the code. Emulate the Ventilator _update_task
        for one code. Also simulate the deletion of a task of the RedisManager.
        """
        if code in self.global_wait_task:
            for task in self.global_wait_task.pop(code):
                task.update_data()
        if code in self.global_dependencies and\
                self.global_dependencies[code] == 0:
            del self.global_dependencies[code]
            del self.global_data[code]

    def start(self, procedure, data, data_kwargs={}):
        """Set all attributes for the execution of a new execution.

        Arguments:
            - procedure: name of the function to be executed.
            - data: arguments of the function.
            - data_kwargs: key arguments of the function.
        """
        self._start_environment()
        self.num_out = getattr(tasks, procedure).num_output
        self.add_call(procedure, 0, data, data_kwargs, BasicMultiCode,
                      self.init_code)

    def has_task(self, codes=None):
        """
        Check if any task with code in codes is ready.
        If codes is None then it checks if there is any ready tasks.

        Arguments:
            - codes: iterable of tasks codes. If codes is a string,
                it is considered as a list of one element.
        """
        if codes is None:
            return bool(self.global_ready_task)
        if isinstance(codes, str):
            codes = [codes]
        for code in codes:
            if code in self.global_ready_task:
                return True
        return False

    def do_task(self, code=None):
        """Execute a ready task and update the attributes.
        If code is set, execute the task of the code."""
        if code is None:
            try:
                code, task = self.global_ready_task.popitem()
            except KeyError:
                raise KeyError("No ready tasks left")
        else:
            if self.has_task(code):
                task = self.global_ready_task.pop(code)
            else:
                raise KeyError("Task with code %s isn't ready" % code)
        #print(code + ' ; ' + task[0])
        self._work_tornado([code] + task)
        self._update_wait_tasks()

    def has_data(self, code):
        """Check if the data of the code task exists."""
        return code in self.global_data

    def get_data(self, code=None):
        """Get the data of the code task. If code is not set then return
        the data of the initial task."""
        if code is None:
            code = self.init_code
        if self.has_data(code):
            if code == self.init_code:
                if not self.is_clean():
                    warnings.warn("Environment is not clean", UserWarning)
                self.running = False
            data = [zmq_loads(d) for d in self.global_data[code]]
            if self.num_out == 1:
                return data[0]
            else:
                return data
        else:
            raise KeyError(code)

    def add_call(self, procedure, num_output, args, kwargs, code_type,
                 new_code=None):
        """Override function of Worker class. If it is the first execution set
        all intern attributes and run the execution. Else call the Worker
        function.

        Arguments:
            - procedure: name of the function to be executed.
            - num_output: number of outputs of the function.
            - args: arguments used in the call of the function.
            - kwargs: key arguments used in the call of the function.
            - code_type: Type of the MultiCode used.
            - new_code: Code of the created task.
                If is None code will be computed.
        """
        if not self.running:
            return self.run(procedure, args, kwargs)
        else:
            try:
                return Worker.add_call(self, procedure, num_output, args,
                    kwargs, code_type, new_code)
            except TypeError:
                raise TypeError("Error in %s adding call to function %s"
                                % (self.procedure.name, procedure))

    def run(self, procedure, data, data_kwargs={}):
        """Execute the task until getting the output.

        Arguments:
            - procedure: name of the function to be executed.
            - data: arguments of the function.
            - data_kwargs: key arguments of the function.
        """
        self.start(procedure, data, data_kwargs)
        self.finish_tasks()
        return self.get_data()

    def wait_for_tasks(self, codes):
        """Execute tasks until a code of codes is ready or execution finishes.

        Arguments:
            - codes: iterable of tasks codes. If codes is a string,
                it is considered as a list of one element.

        Output:
            - boolean set to True if any code of codes is ready,
                and False if execution has finished.
        """
        while not self.has_task(codes):
            if self.has_task():
                self.do_task()
            else:
                return False
        return True

    def finish_tasks(self):
        """Execute tasks until there isn't ready tasks."""
        while self.has_task():
            self.do_task()

    def is_clean(self):
        """Overrided function, check if all tasks are done,
        and all data from done tasks has been deleted."""
        return not any([bool(self.global_dependencies),
                       bool(self.global_ready_task),
                       bool(self.global_wait_task),
                       bool(self.global_done_task),
                       self.global_data.keys() != [self.init_code]])

    def print_wait_task(self, show=['code', 'left', 'needed']):
        """Print the non ready tasks. The attributes printed are
        defined in show, and must be WaitingTask attributes.

        Attributes:
            - show: list of attributes that will be printed for each task.
                The default attributes printed are code, left and needed."""
        sep = max(map(len, show))
        for k, list_tasks in self.global_wait_task.items():
            print(k)
            for task in list_tasks:
                for s in show:
                    print('    ' + s.ljust(sep) + ' : ' +
                          str(getattr(task, s)))

    def set_display(self, display):
        """Change the display attribute."""
        self.display = display

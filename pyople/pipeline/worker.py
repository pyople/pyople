from pyople.pipeline.models import SocketMultiprocess
from pyople.tasks.models import BasicMultiCode, DataBaseMultiCode,\
        code_new_task, has_kwargs, code_kwargs, VALUE_KWARGS
from zmq.utils.jsonapi import loads as zmq_json_loads
from zmq import SNDMORE, RCVMORE
import pyople.tasks


class Result(object):
    """
    Class used for tasks created in Worker.

    Arguments:
        - procedure: name of the function to be executed.
        - needed: list of needed codes.
        - data: list of arguments of the task.
        - send_task: bool defining it the task must be send to the ventilator.
            If it is False the task is a zero task.
        - worker: Worker instance that create the instances.
    """
    def __init__(self, procedure, needed, data, send_task, worker):
        self.procedure = procedure
        self.needed = needed
        self.has_kwargs = has_kwargs(needed) if needed else False
        self.data = data
        self.worker = worker
        self.send_task = send_task

    def is_direct(self):
        """Check if the task doesn't need data of other tasks.
        If it is True it can be executed with the existing data"""
        return len(self.data) == len(self.needed)

    def send(self, code):
        """Generic send function.

        Arguments:
            - code: code of the task."""
        if self.send_task:
            if self.is_direct():
                self._send_task_worker(code)
            else:
                self._send_task(code)
                if self.data:
                    self._send_task_data(code)
        else:
            self._send_task_data(code)

    def _send_task(self, code):
        """Send the task to the Ventilator."""
        if self.worker.display:
            print('Send: ' + str([code, self.procedure, self.needed]))
        self.worker.to_socket.send(code, SNDMORE)
        self.worker.to_socket.send(self.procedure, SNDMORE)
        self.worker.to_socket.send_json(map(str, self.needed))

    def _send_task_data(self, code):
        """Send the data to the Sink."""
        # send_mode meaning: (Register in sink, has kwargs)
        send_mode = ''.join(['0' if self.send_task else '1',
                     '1' if self.has_kwargs else '0'])
        if self.worker.display:
            print('Send data: ' + str(send_mode) + ' ; ' +
                  str([code, self.data]))
        self.worker.data_socket.send(code, SNDMORE)
        if self.data:
            self.worker.data_socket.send(send_mode, SNDMORE)
            for d in self.data[:-1]:
                self.worker.data_socket.send_json(d, SNDMORE)
            self.worker.data_socket.send_json(self.data[-1])
        else:
            self.worker.data_socket.send(send_mode)

    def _send_task_worker(self, code):
        """Send a ready task to a Worker."""
        if self.worker.display:
            print('Send to worker: ' + str([code, self.procedure,
                                            self.data]))
        self.worker.direct_task.send(code, SNDMORE)
        if self.data:
            self.worker.direct_task.send(self.procedure, SNDMORE)
            for d in self.data[:-1]:
                self.worker.direct_task.send_json(d, SNDMORE)
            if self.has_kwargs:
                self.worker.direct_task.send_json(self.data[-1])
            else:
                self.worker.direct_task.send_json(self.data[-1], SNDMORE)
                self.worker.direct_task.send_json([])
        else:
            self.worker.direct_task.send(self.procedure)


class Worker(SocketMultiprocess):
    """Execute the received tasks. Receive the tasks to be executed and
    send the new tasks, the needed data, output data and the dependencies.

    Connections between instances:
        Send:
        - Ventilator: new tasks created during execution.
            Zero tasks aren't send.
        - Worker: tasks without needed data that can be executed directly.
        - Sink: data of created tasks.
        - RedisManager: new dependencies for tasks created during execution.

        Receive:
        - Ventilator: tasks to be executed.
        - Worker: tasks to be executed.
    """
    def __init__(self, zmq_transport=None, tornado_sockets=None,
                 display=None, context=None, io_threads=None, db_num=0,
                 group_id='', connection_info={}, mongodb_name=''):
        SocketMultiprocess.__init__(self, zmq_transport, tornado_sockets,
                                    display, context, io_threads, db_num,
                                    group_id, connection_info)
        from pyople.tasks.models import MultiprocessTask
        self.mongodb_name = mongodb_name
        self.connect_mongodb(mongodb_name)
        MultiprocessTask.set_environment(self)
        self.result_class = Result

    def _sockets_path(self):
        new_dict = SocketMultiprocess._sockets_path(self)
        #from_socket: receive tasks to be executed in the instance.
        #to_socket: send created tasks.
        #data_socket: send data of created tasks.
        #direct_task: send tasks that can be executed without needed data.
        #redis_manager: send dependencies of the created tasks.
        new_dict.update({'from_socket': ['from_new_task', 'PULL', '_work'],
                         'to_socket': ['to_done_task', 'PUSH', ''],
                         'data_socket': ['to_data', 'PUSH', ''],
                         'direct_task': ['to_new_task', 'PUSH', ''],
                         'redis_manager': ['redis_manager', 'PUSH', '']})
        return new_dict

    def connect_mongodb(self, mongodb_name):
        """Connect with the mongodb database."""
        if mongodb_name:
            try:
                from pyople.settings.sockets import mongodb
            except ImportError:
                raise ImportError("mongodb direction not defined in settings")
            from mongoengine import connect
            connect(mongodb_name, port=mongodb.port, host=mongodb.host)

    def _list_subscribe(self):
        subscribe = SocketMultiprocess._list_subscribe(self)
        subscribe.extend(['Node', 'Worker', self.group_id])
        return subscribe

    def set_task(self, code, procedure):
        """Set the code and procedure of a new task.
        Attributes are set to the default value."""
        self.list_result = {}
        self.total_code = 0
        self.task_dependencies = {}
        self.code = code
        self.procedure = getattr(tasks, procedure)

    def run_task(self, *args, **kwargs):
        """Execute the internal procedure using the args and kwargs."""
        output = self.procedure.func(*args, **kwargs)
        if self.procedure.num_output == 1:
            output = [output]
        code_type = BasicMultiCode if self.total_code else DataBaseMultiCode
        self.add_call('final_mp', 0, output, {}, code_type, self.code)

    def _new_code(self):
        """Create the code for a new task.
        The new code can't be an existing code."""
        new = code_new_task(self.code, self.total_code)
        self.total_code += 1
        return new

    def separate_needed_data(self, code, arguments, needed, data):
        """Add the arguments to the needed and data.
        If an argument is a BasicMultiCode it is added to the needed.
        Else it is coded as a BasicMultiCode and added to the data and needed.
        Dependencies are updated.

        Arguments:
            - code: code of the task.
            - arguments: list of arguments to be added.
            - needed: list of needed codes of the task.
            - data: list with the data of the task.
        """
        task_codes = set()
        for c in arguments:
            if isinstance(c, BasicMultiCode):
                task_codes.add(c.code_task)
                needed.append(c)
            else:
                register_code = BasicMultiCode(code, len(data))
                data.append(c)
                needed.append(register_code)
        for task_code in task_codes:
            if task_code in self.task_dependencies:
                self.task_dependencies[task_code][1].add(code)

    def _register_kwargs(self, code, kwargs, needed, data):
        """Add the kwargs to the data and needed of the task."""
        self.separate_needed_data(code, kwargs.values(), needed, data)
        data.append(kwargs.keys())
        needed.append(BasicMultiCode(code, VALUE_KWARGS))

    def _substitute_kwargs(self, data, data_kwargs):
        """Create the dictionary of kwargs.

        Arguments:
            - data: list of data. First elements are from the arguments
                and the last from the kwargs.
            - data_kwargs: list with the keys of the kwargs.
        """
        if data_kwargs:
            len_kwargs = len(data_kwargs)
            data_kwargs = dict(zip(data_kwargs, data[-len_kwargs:]))
            data = data[:-len_kwargs]
        else:
            data_kwargs = {}
        return data, data_kwargs

    def add_call(self, procedure, num_output, args, kwargs, code_type,
                 new_code=None):
        """Manage the call of a multiprocess function. Add to the result list
        if it is a BasicMultiCode else send the task. Dependencies are updated.

        Arguments:
            - procedure: name of the function to be executed.
            - num_output: number of outputs of the function.
            - args: arguments used in the call of the function.
            - kwargs: key arguments used in the call of the function.
            - code_type: Type of the MultiCode used.
            - new_code: Code of the created task.
                If is None code will be computed.
        """
        if new_code is None:
            new_code = self._new_code()
        needed = []
        data = []
        self.separate_needed_data(new_code, args, needed, data)
        if kwargs:
            self._register_kwargs(new_code, kwargs, needed, data)
        # DataBaseMultiCode are zero tasks and aren't send to the ventilator
        send_task = code_type != DataBaseMultiCode
        res = self.result_class(procedure, needed, data, send_task, self)
        # if False the data and needed attributes of the task may be changed,
        #so it can't be sent here.
        if code_type == BasicMultiCode:
            res.send(new_code)
        else:
            self.list_result[new_code] = res
        if num_output:
            self.task_dependencies[new_code] = [num_output, set()]
            if num_output == 1:
                return code_type(new_code, 0)
            else:
                return tuple(code_type(new_code, i)
                             for i in range(num_output))
        else:
            return ()

    def send_dependencies(self):
        """Send the created dependencies."""
        if self.task_dependencies:
            if self.display:
                print('Send depend: ' + str(self.task_dependencies))
            self.redis_manager.send('', SNDMORE)
            send_depend = self.task_dependencies.items()
            for code, [output, dep] in send_depend[:-1]:
                self.redis_manager.send(code, SNDMORE)
                self.redis_manager.send(str(output), SNDMORE)
                self.redis_manager.send(str(len(dep)), SNDMORE)
            code, [output, dep] = send_depend[-1]
            self.redis_manager.send(code, SNDMORE)
            self.redis_manager.send(str(output), SNDMORE)
            self.redis_manager.send(str(len(dep)))

    def _work_zmq(self):
        """Auxiliary function of _work using zmq sockets."""
        task_code = self.from_socket.recv(RCVMORE)
        procedure = self.from_socket.recv(RCVMORE)
        data = []
        if self.from_socket.RCVMORE:
            while True:
                new_data = self.from_socket.recv_json(RCVMORE)
                if self.from_socket.RCVMORE:
                    data.append(new_data)
                else:
                    data_kwargs = new_data
                    break
        else:
            data_kwargs = []
        self._work(task_code, procedure, data, data_kwargs)

    def _work_tornado(self, msg):
        """Auxiliary function of _work using tornado sockets."""
        task_code = msg[0]
        procedure = msg[1]
        data = []
        [data.append(zmq_json_loads(d)) for d in msg[2:-1]]
        if data:
            data_kwargs = zmq_json_loads(msg[-1])
        else:
            data_kwargs = []
        self._work(task_code, procedure, data, data_kwargs)

    def _work(self, task_code, procedure, data, data_kwargs):
        """Execute the task and send the results and dependencies created."""
        if self.display:
            print('Recv: ' + str([task_code, procedure, data, data_kwargs]))
        self.set_task(task_code, procedure)
        data, data_kwargs = self._substitute_kwargs(data, data_kwargs)
        self.run_task(*data, **data_kwargs)
        [result.send(code) for code, result in self.list_result.items()]
        self.send_dependencies()

    def _work_clean_exit(self, task_code, procedure, data, data_kwargs):
        """Resend the received task to another instance."""
        data.append(data_kwargs)
        if self.worker.display:
            print('Resend to worker: ' + str([task_code, procedure,
                                              data]))
        self.result_class(procedure, [code_kwargs(task_code)], data,
                          True, self)._send_task_worker(task_code)

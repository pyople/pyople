from pyople.pipeline.models import SocketMultiprocess,\
        code_done, code_resend, connect_redis, uncode
from pyople.tasks.models import decode_output, code_output, has_kwargs
from zmq import SNDMORE, RCVMORE, SUBSCRIBE, UNSUBSCRIBE
from zmq.utils.jsonapi import loads as json_loads
import pyople.tasks
from redis import WatchError


class TaskVentilatorData(object):
    """Class associated to a new task created during execution.

    Arguments:
        code: code of the task.
        needed: needed codes of the task.
        procedure: function to be executed by the task.
        ventilator: Ventilator instance."""
    def __init__(self, code, needed, procedure, ventilator):
        self.code = code
        self.needed = needed
        self.procedure = getattr(tasks, procedure)
        self.ventilator = ventilator
        self.left = 0

    def update_data(self, num=1):
        """Update the instance and finish the task if is ready."""
        self.left -= num
        if not self.left:
            self.finish_task()

    def set_left(self, task_depend, self_needed):
        """Set the dependencies of the task.

        Arguments:
            task_depend: set of dependencies of the class.
            self_needed: boolean defining if the task has intern data,
                i.e, data assigned directly when the task was called."""
        self.left = len(task_depend)
        self.self_needed = self_needed
        self.task_depend = list(task_depend)

    def finish_task(self):
        """If the task can be executed with a 'direct_call',
        the task is executed. Else the data is send to be executed.

        In both cases the dependencies are send to the redis_manager
        to be updated."""
        if hasattr(self.procedure.code_type, 'direct_call'):
            self.send_sink()
        else:
            self.send_worker()
        self._update_depend()

    def _update_depend(self):
        """Send the dependencies of the instance to be updated
        by the redis_manager."""
        if self.task_depend:
            for code in self.task_depend[:-1]:
                self.ventilator.redis_manager.send(code, SNDMORE)
            self.ventilator.redis_manager.send(self.task_depend[-1])

    def send_sink(self):
        """Execute the task with a 'direct_call', save the output
        to the database and send the code to the sink."""
        data_redis = self.wait_exists()
        data = self.procedure.code_type.direct_call(data_redis)
        if self.self_needed:
            self.db_delete_needed()
        if self.ventilator.display:
            print('direct call: ' + self.procedure.name)
        self.db_set(data)
        if self.ventilator.display:
            print('Send data: ' + str(self.code))
        self._send_sink_data()

    def _send_sink_data(self):
        """Send the task data to the sink."""
        self.ventilator.data_socket.send(self.code, SNDMORE)
        # Register sink, kwargs
        self.ventilator.data_socket.send(
                ''.join(['1', '1' if self._has_kwargs() else '0']))

    def _has_kwargs(self):
        """Check if the arguments defined by the attribute needed has kwargs"""
        return has_kwargs(self.needed)

    def send_worker(self):
        """Send the task to be executed to the worker.
        The intern data of the task is deleted."""
        self.ventilator.to_socket.send(self.code, SNDMORE)
        data = self.wait_exists()
        if self.self_needed:
            self.db_delete_needed()
        if data:
            self.ventilator.to_socket.send(self.procedure.name, SNDMORE)
            for d in data[:-1]:
                    self.ventilator.to_socket.send(d, SNDMORE)
            if self._has_kwargs():
                self.ventilator.to_socket.send(data[-1])
            else:
                self.ventilator.to_socket.send(data[-1], SNDMORE)
                self.ventilator.to_socket.send_json([])
        else:
            self.ventilator.to_socket.send(self.procedure.name)
        if self.ventilator.display:
            print('Send: ' + str([self.code, self.procedure.name, data]))

    def send_restore_task(self, list_code):
        """Send the task to another instance."""
        self.ventilator.from_clean.send('_restore_task_clean_data', SNDMORE)
        self.ventilator.from_clean.send_json(list_code, SNDMORE)
        self.ventilator.from_clean.send(self.code, SNDMORE)
        self.ventilator.from_clean.send(self.procedure.name, SNDMORE)
        self.ventilator.from_clean.send_json(self.needed)

    def db_set(self, data):
        """Save the data to the database."""
        if data:
            data_keys = [code_output(self.code, i) for i in range(len(data))]
            with self.ventilator.redis.pipeline() as pipe:
                while True:
                    try:
                        pipe.watch(data_keys)
                        pipe.multi()
                        [pipe.set(k, d) for k, d in zip(data_keys, data)]
                        res = pipe.execute()
                        if self.ventilator.display:
                            print('Redis vent: saved data' +
                                  str(data_keys) + str(data))
                            print('     ' + str(res))
                        break
                    except WatchError:
                        continue
                    finally:
                        pipe.reset()

    def wait_exists(self):
        """Wait until all needed data is in the database."""
        if self.needed:
            left = self.needed
            if self.ventilator.display:
                print('Waiting for :' + str(left))
            with self.ventilator.redis.pipeline() as pipe:
                #left is updated until all needed codes are in the database
                while left:
                    try:
                        pipe.watch(left)
                        pipe.multi()
                        [pipe.exists(c) for c in left]
                        res = pipe.execute()
                        left = [c for c, b in zip(left, res) if not b]
                    except WatchError:
                        continue
                    finally:
                        pipe.reset()
                #get the needed codes
                while True:
                    try:
                        pipe.watch(self.needed)
                        pipe.multi()
                        [pipe.get(code) for code in self.needed]
                        res = pipe.execute()
                        if self.ventilator.display:
                            print('Redis get:  ; ' + str(self.needed))
                            print('       ' + str(res))
                        return res
                    except WatchError:
                        continue
                    finally:
                        pipe.reset()
        else:
            return []

    def db_delete_needed(self):
        """Delete from the database the intern data of the database, i.e.,
        codes in self.needed with task equal to the code attribute."""
        code_list = filter(lambda(x): decode_output(x) == self.code,
                           self.needed)
        with self.ventilator.redis.pipeline() as pipe:
            while True:
                try:
                    pipe.watch(code_list)
                    pipe.multi()
                    [pipe.delete(code) for code in code_list]
                    res = pipe.execute()
                    if self.ventilator.display:
                        print('Redis delete:  ; ' + str(code_list))
                        print('       ' + str(res))
                    return res
                except WatchError:
                    continue
                finally:
                    pipe.reset()


class TaskServerData(TaskVentilatorData):
    """Extension of the class TaskVentilatorData associated
    to a new task created from the server.

    Arguments:
        code: code of the task.
        address: zmq address of the server that sent the task.
        num_output: number of outputs of the function.
        ventilator: Ventilator instance."""
    def __init__(self, code, address, num_output, ventilator):
        self.code = code
        self.address = address
        self.num_output = num_output
        self.ventilator = ventilator
        self.left = 1
        self.self_needed = False

    def finish_task(self):
        """Get from the database and send to the server the output
        of the function.

        Dependencies are send to the redis_manager to be updated."""
        self.needed = [code_output(self.code, num)
                       for num in range(self.num_output)]
        self.ventilator.server.send(self.address, SNDMORE)
        data = self.wait_exists()
        if data:
            self.ventilator.server.send('', SNDMORE)
            for d in data[:-1]:
                self.ventilator.server.send(d, SNDMORE)
            self.ventilator.server.send(data[-1])
        else:
            self.ventilator.server.send('')
        if self.ventilator.display:
            print('Send server: ' + str(self.address) + ' ; ' + str(data))
            print(str([self.address, data]))
        self.ventilator.redis_manager.send(self.code)

    def send_restore_task(self, list_code):
        """Send the task to another instance."""
        self.ventilator.from_clean.send('_restore_task_clean_server', SNDMORE)
        self.ventilator.from_clean.send_json(list_code, SNDMORE)
        self.ventilator.from_clean.send(self.code, SNDMORE)
        self.ventilator.from_clean.send(self.address, SNDMORE)
        self.ventilator.from_clean.send(str(self.num_output))


class Ventilator(SocketMultiprocess):
    """Manage the creation of tasks in the environments. This include the tasks
    from the server and new subtasks created during execution.

    Connections between instances:
        Send:
        - Server: the output of received tasks.
        - Worker: ready tasks to be executed.
        - Sink: new task executed with a direct_call.
            message of resend done message if created.
        - RedisManager: dependencies of server tasks and dependencies updates.

        Receive:
        - Server: tasks to be executed.
        - Worker: new tasks created during execution.
        - Sink: message of done task.

    DataBase:
        Create:
        - data of server tasks.
        - data of direct calls.

        Read:
        - data of done tasks.

        Delete:
        - internal data of tasks when it's ready for execution.
    """
    def __init__(self, zmq_transport=None, tornado_sockets=None,
                 display=None, context=None, io_threads=None, db_num=0,
                 group_id='', connection_info={}):
        SocketMultiprocess.__init__(self, zmq_transport, tornado_sockets,
                                    display, context, io_threads, db_num,
                                    group_id, connection_info)
        self._connect_db(db_num)
        self.tasks = {}

    def _connect_db(self, db_num):
        """Connect the instance to the database."""
        self.redis = connect_redis(db_num, self.connection_info)

    def _sockets_path(self):
        new_dict = SocketMultiprocess._sockets_path(self)
        #tasks_pub: used for sending the status of the tasks.
        #tasks_sub: used for receiving the status of the tasks.
        #from_socket: receive new tasks created during execution.
        #to_socket: send ready tasks to be executed.
        #server: receive new tasks from the server.
        #redis_manager: send dependencies of the server tasks.
        #data_socket: send direct tasks data.
        new_dict.update({'tasks_pub': ['to_tasks_publish', 'PUB', ''],
                         'tasks_sub': ['from_tasks_publish',
                                       'SUB', '_update_task'],
                         'from_socket': ['from_done_task', 'PULL',
                                          '_create_task'],
                         'to_socket': ['to_new_task', 'PUSH', ''],
                         'server': ['to_server', 'DEALER',
                                     '_receive_task_server'],
                         'redis_manager': ['redis_manager', 'PUSH', ''],
                         'data_socket': ['to_data', 'PUSH', '']})
        return new_dict

    def _sockets_path_clean(self, direction):
        if direction == 'to':
            return {'to_clean': ['to_ventilator_clean', 'REP',
                                    '_create_new_clean']}
        elif direction == 'from':
            return {'from_clean': ['from_ventilator_clean', 'REQ', '']}

    def _list_subscribe(self):
        subscribe = SocketMultiprocess._list_subscribe(self)
        subscribe.extend(['Node', 'Ventilator',
                          'TaskManager' + self.group_id, self.group_id])
        return subscribe

    def _register_needed(self, needed_codes, actual_data):
        """Register the actual_data in the attribute 'tasks' the codes in
        needed_codes. Send the message to resend the message for done tasks.

        Arguments:
            needed_codes: list of needed codes to be registered.
            actual_data: instance with type TaskVentilatorData"""
        for code in needed_codes:
            code_d = code_done(code)
            if self.display:
                print('added task: ' + code_d)
            try:
                self.tasks[code_d].append(actual_data)
            except KeyError:
                if self.display:
                    print('subscribe: ' + code_d)
                self.tasks_sub.setsockopt(SUBSCRIBE, str(code_d))
                self.tasks_pub.send(code_resend(code))
                self.tasks[code_d] = [actual_data]

    def _create_task_zmq(self):
        """Auxiliary function of _create_task using zmq sockets."""
        code = self.from_socket.recv(RCVMORE)
        procedure = self.from_socket.recv(RCVMORE)
        needed = self.from_socket.recv_json()
        self._create_task(code, procedure, needed)

    def _create_task_tornado(self, msg):
        """Auxiliary function of _create_task using tornado sockets."""
        code, procedure, needed = msg
        needed = json_loads(needed)
        self._create_task(code, procedure, needed)

    def _create_task(self, code, procedure, needed):
        """Manage the new task. If there isn't needed codes for the task,
        it is directly processed. Else the needed codes are registered and
        send the message to resend the message for done tasks."""
        needed = map(str, needed)  # unicode to str for zmq compatibility
        if self.display:
            print('creating task: ' + str(code))
        actual_data = TaskVentilatorData(code, needed, procedure, self)
        task_needed = set(map(decode_output, needed))
        #equivalent to task_needed.discard(code) but self_needed is used.
        self_needed = code in task_needed
        if self_needed:
            task_needed.remove(code)
        actual_data.set_left(task_needed, self_needed)
        if task_needed:
            self._register_needed(task_needed, actual_data)
        else:
            #task will be send for execution
            actual_data.update_data(0)

    def _create_task_clean_exit(self, code, procedure, needed):
        """When instance is clean mode the new tasks are
        resend to other instances."""
        self.from_clean.send('_create_task', SNDMORE)
        self.from_clean.send(code, SNDMORE)
        self.from_clean.send(procedure, SNDMORE)
        self.from_clean.send_json(needed)
        self.from_clean.recv_multipart()

    def _update_task_zmq(self):
        """Auxiliary function of _update_task using zmq sockets.
        Get all incoming messages while the socket poll isn't empty.
        """
        set_update = set()
        while self.tasks_sub.poll(0):
            set_update.add(self.tasks_sub.recv())
        self._update_task(set_update)

    def _update_task_tornado(self, msg):
        """Auxiliary function of _update_task using tornado sockets."""
        self._update_task([msg[0]])

    def _update_task(self, recv_codes):
        """Update the tasks with needed codes in recv_codes"""
        if self.display:
            print('deleted and unsubs:' + str(recv_codes))
        list_tasks = {}
        for recv_code in recv_codes:
            self.tasks_sub.setsockopt(UNSUBSCRIBE, recv_code)
            for task in self.tasks.pop(recv_code):
                if task in list_tasks:
                    list_tasks[task] += 1
                else:
                    list_tasks[task] = 1
        [actual_data.update_data(value) for actual_data,
         value in list_tasks.items()]

    def _update_task_clean_exit(self, recv_codes):
        """Update the tasks with needed codes in recv_codes"""
        self._update_task_old(recv_codes)

    def _receive_task_server_zmq(self):
        """Auxiliary function of _receive_task_server using zmq sockets."""
        address = self.server.recv(RCVMORE)
        self.server.recv(RCVMORE)
        procedure = self.server.recv(RCVMORE)
        if self.server.RCVMORE:
            data = self.server.recv_multipart()
        else:
            data = []
        self._receive_task_server(address, procedure, data)

    def _receive_task_server_tornado(self, msg):
        """Auxiliary function of _receive_task_server using tornado sockets."""
        address = msg[0]
        procedure = msg[2]
        data = msg[3:]
        self._receive_task_server(address, procedure, data)

    def _receive_task_server(self, address, procedure, data):
        """Create and send a new task from the server.
        Dependencies are send to redis_manager."""
        code = self.db_new_server_task()
        if self.display:
            print('Recv server from : ' + str(address))
            print('New server task: ' + str(code))
        num_output = getattr(tasks, procedure).num_output
        actual_data = TaskServerData(code, address, num_output, self)
        self._register_needed([code], actual_data)
        self.to_socket.send(code, SNDMORE)
        if data:
            self.to_socket.send(procedure, SNDMORE)
            for d in data[:-1]:
                self.to_socket.send(d, SNDMORE)
            self.to_socket.send(data[-1])
        else:
            self.to_socket.send(procedure)
        if self.display:
            print('Send depend: ' + str({code: 1}))
        self.redis_manager.send('', SNDMORE)
        self.redis_manager.send(code, SNDMORE)
        self.redis_manager.send(str(num_output), SNDMORE)
        self.redis_manager.send('1')

    def _receive_task_server_clean_exit(self, address, procedure, data):
        """When instance is clean mode the new server tasks are
        resend to other instances."""
        if self.display:
            print('clean recv server from :' + address)
        self.from_clean.send('receive_task_server', SNDMORE)
        self.from_clean.send(address, SNDMORE)
        if data:
            self.from_clean.send(procedure, SNDMORE)
            for d in data[:-1]:
                self.from_clean.send(d, SNDMORE)
            self.from_clean.send(data[-1])
        else:
            self.from_clean.send(procedure)
        self.from_clean.recv_multipart()

    def _distribute_self_tasks(self):
        """Send the attribute 'tasks' to other instances."""
        if self.display:
            print('ventilator cleaning self tasks')
        self.subscribed_tasks = []
        tasks_distribute = {}
        for code, list_data in self.tasks.items():
            for actual_data in list_data:
                try:
                    tasks_distribute[actual_data].append(uncode(code))
                except KeyError:
                    tasks_distribute[actual_data] = [uncode(code)]
        for actual_data, list_code in tasks_distribute.items():
            actual_data.send_restore_task(list_code)
            self.from_clean.recv_multipart()
        self.tasks = {}
        if self.display:
            print('ventilator cleaned self tasks')

    def _create_new_clean_zmq(self):
        """Auxiliary function of _create_new_clean using zmq sockets."""
        func_name = self.to_clean.recv()
        msg = self.to_clean.recv_multipart()
        self._create_new_clean(func_name, msg)

    def _create_new_clean_tornado(self, msg):
        """Auxiliary function of _create_new_clean using tornado sockets."""
        func_name = msg[0]
        msg = msg[1:]
        self._create_new_clean(func_name, msg)

    def _create_new_clean(self, func_name, msg):
        """Call the inter funcion 'func_name' with 'msg' as arguments."""
        getattr(self, func_name + '_tornado')(msg)
        self.to_clean.send('')

    def _restore_task_clean_data_tornado(self, msg):
        """Create and register a task received during clean mode."""
        list_code, code, procedure, needed = msg
        list_code = json_loads(list_code)
        needed = json_loads(needed)
        actual_data = TaskVentilatorData(code, needed, procedure, self)
        task_needed = set(map(decode_output, needed))
        self_needed = code in task_needed
        if self_needed:
            task_needed.remove(code)
        actual_data.set_left(task_needed, self_needed)
        actual_data.update_data(len(task_needed) - len(list_code))
        self._register_needed(list_code, actual_data)
        [self.tasks_pub.send(code_resend(c)) for c in list_code]

    def _restore_task_clean_server_tornado(self, msg):
        """Create and register a server task received during clean mode."""
        list_code, code, address, num_output = msg
        list_code = json_loads(list_code)
        num_output = int(num_output)
        actual_data = TaskServerData(code, address, num_output, self)
        self._register_needed(list_code, actual_data)
        self.tasks_pub.send(code_resend(code))

    def _info_ask_clean(self):
        SocketMultiprocess._info_ask_clean(self)
        if self.tasks:
            print('tasks_data is not clean of keys. Keys left: ')
            for code, list_data in self.tasks.items():
                print(code)
                if not isinstance(list_data, list):
                    list_data = [list_data]
                for data in list_data:
                    if hasattr(data, 'code'):
                        print('    ' + data.code + ' ; ')
                    if hasattr(data, 'address'):
                        print('        ' + data.address + ' ; ')
                    if hasattr(data, 'left'):
                        print('        ' + 'left   : ' + str(data.left))
        else:
            print('tasks_data is clean')

    def _is_clean(self):
        self_clean = not bool(self.tasks)
        return self_clean and SocketMultiprocess._is_clean(self)

    def db_new_server_task(self):
        """Create a code for a new server task."""
        with self.redis.pipeline() as pipe:
            while True:
                try:
                    pipe.watch('tasks')
                    pipe.multi()
                    pipe.exists('tasks')
                    new = pipe.execute()[0]
                    if not new:
                        new_code = 0
                    else:
                        pipe.watch('tasks')
                        pipe.multi()
                        pipe.sort('tasks', start=0, num=1, desc=1)
                        sorted_keys = pipe.execute()[0]
                        if sorted_keys:
                            new_code = int(sorted_keys[0]) + 1
                        else:
                            new_code = 0
                    ok = self.db_add_server_task(new_code)
                    if not ok:
                        continue
                    return str(new_code)
                except WatchError:
                    continue
                finally:
                    pipe.reset()

    def db_add_server_task(self, new_code):
        """Add to the server the code of a new server task."""
        with self.redis.pipeline() as pipe:
            while True:
                try:
                    pipe.watch('tasks')
                    pipe.multi()
                    pipe.sadd('tasks', new_code)
                    return pipe.execute()[0]
                except WatchError:
                    continue
                finally:
                    pipe.reset()

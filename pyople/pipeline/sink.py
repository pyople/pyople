from pyople.pipeline.models import SocketMultiprocess, uncode, code_finish,\
        code_resend, code_done, connect_redis
from pyople.tasks.models import code_output
from zmq import RCVMORE, UNSUBSCRIBE, SUBSCRIBE
from redis import WatchError


class Sink(SocketMultiprocess):
    """
    Save the data to the database and sends a message when a task is done.

    Connections between instances:
        Send:
        - Ventilator: message of done task.
        - RedisManager: message of done task for the zero tasks.

        Receive:
        - Ventilator: new task executed with a direct_call.
            message of resend done message if created.
        - Worker: data of created tasks.
        - RedisManager: message finish. Also receive the message resend
            for the zero tasks.

    DataBase:
        Create:
        - data of received tasks.
    """
    def __init__(self, zmq_transport=None, tornado_sockets=None,
                 display=None, context=None, io_threads=None, db_num=0,
                 group_id='', connection_info={}):
        SocketMultiprocess.__init__(self, zmq_transport, tornado_sockets,
                                    display, context, io_threads, db_num,
                                    group_id, connection_info)
        self._connect_db(db_num)
        self.subscribed_tasks = []

    def _connect_db(self, db_num):
        """Connect the instance to the database."""
        self.redis = connect_redis(db_num, self.connection_info)

    def _sockets_path(self):
        new_dict = SocketMultiprocess._sockets_path(self)
        #tasks_pub: send the message of done tasks.
        #tasks_sub: receive the message to resend a task.
        #tasks_del: receive the message to finish a task.
        #data_socket: receive the data of a task.
        new_dict.update({'tasks_pub': ['to_tasks_publish', 'PUB', ''],
                         'tasks_sub': ['from_tasks_publish',
                                       'SUB', '_actualize_task'],
                         'tasks_del': ['from_tasks_publish',
                                          'SUB', '_del_task'],
                         'data_socket': ['from_data',
                                         'PULL', '_create_data']})
        return new_dict

    def _sockets_path_clean(self, direction):
        if direction == 'to':
            return {'to_clean': ['to_sink_clean', 'REP',
                                    '_create_new_clean']}
        elif direction == 'from':
            return {'from_clean': ['from_sink_clean', 'REQ', '']}

    def _list_subscribe(self):
        subscribe = SocketMultiprocess._list_subscribe(self)
        subscribe.extend(['Node', 'Sink', 'TaskManager' + self.group_id,
                          self.group_id])
        return subscribe

    def _register_needed(self, code):
        """Subscribe to the task of the code. Will receive the finish message
        and resend message."""
        if self.display:
            print('subscribe: ' + code)
        self.tasks_del.setsockopt(SUBSCRIBE, code_finish(code))
        self.tasks_sub.setsockopt(SUBSCRIBE, code_resend(code))
        self.subscribed_tasks.append(code)

    def _create_data_zmq(self):
        """Auxiliary function of _create_data using zmq sockets."""
        code = self.data_socket.recv(RCVMORE)
        mode = self.data_socket.recv(RCVMORE)
        data = self.data_socket.recv_multipart() if\
                self.data_socket.RCVMORE else []
        self._create_data(code, mode, data)

    def _create_data_tornado(self, msg):
        """Auxiliary function of _create_data using tornado sockets."""
        code = msg[0]
        mode = msg[1]
        data = msg[2:]
        self._create_data(code, mode, data)

    def _create_data(self, code, mode, data):
        """Receive the data of a new task.

        Arguments:
            - code: code of the task.
            - mode: list with two values, ['register', 'has_kwargs'].
                If 'register' the task is subscribed.
                If 'has_kwargs' the data has kwargs.
            - data: list of the data of the task.
        """
        register, has_kwargs = map(int, mode)
        self.db_set(code, data, has_kwargs)
        if register:
            self._register_needed(code)
            if self.display:
                print('publish: ' + str(code_done(code)))
            self.tasks_pub.send(code_done(code))

    def _create_data_clean_exit(self, code, mode, data):
        """Data is saved and the task is send to another Sink."""
        register, has_kwargs = map(int, mode)
        self.db_set(code, data, has_kwargs)
        if self.display:
            print('sink clean: ' + code)
        if register:
            self.from_clean.send(code)
            self.from_clean.recv_multipart()

    def _del_task_zmq(self):
        """Auxiliary function of _del_task using zmq sockets."""
        code = self.tasks_del.recv()
        self._del_task(code)

    def _del_task_tornado(self, msg):
        """Auxiliary function of _del_task using tornado sockets."""
        self._del_task(msg[0])

    def _del_task(self, code):
        """Unsubscribe to the task of the code."""
        if self.display:
            print('recv del: ' + code)
        self.tasks_del.setsockopt(UNSUBSCRIBE, code)
        un_code = uncode(code)
        self.tasks_sub.setsockopt(UNSUBSCRIBE, code_resend(un_code))
        try:
            self.subscribed_tasks.remove(un_code)
        except ValueError:
            pass

    def _del_task_clean_exit(self, code):
        """Unsubscribe to the task of the code."""
        self._del_task_old(code)

    def _create_new_clean_zmq(self):
        """Auxiliary function of _create_new_clean using zmq sockets."""
        code = self.to_clean.recv()
        self._create_new_clean(code)
        self.to_clean.send('')

    def _create_new_clean_tornado(self, msg):
        """Auxiliary function of _create_new_clean using tornado sockets."""
        self._create_new_clean(msg[0])
        self.to_clean.send('')

    def _create_new_clean(self, code):
        """Register to the received code from a instance in clean."""
        if self.display:
            print('sink recv clean : ' + code)
        self._create_data(code, '00', [])

    def _actualize_task_zmq(self):
        """Auxiliary function of _actualize_task using zmq sockets.
        Get all incoming messages while the socket poll isn't empty.
        """
        set_resend = set()
        while self.tasks_sub.poll(0):
            set_resend.add(self.tasks_sub.recv())
        for code in map(uncode, set_resend):
            self._actualize_task(code)

    def _actualize_task_tornado(self, msg):
        """Auxiliary function of _actualize_task using tornado sockets."""
        self._actualize_task(uncode(msg[0]))

    def _actualize_task(self, code):
        """Send the message of done code for the task."""
        if self.display:
            print('recv resend: ' + code)
        self.tasks_pub.send(code_done(code))

    def _actualize_task_clean_exit(self, code):
        """Nothing is done."""
        pass

    def _distribute_self_tasks(self):
        """Send the list of registered tasks to another Sink."""
        if self.display:
            print('sink cleaning self tasks')
        for code in set(self.subscribed_tasks):
            self.from_clean.send(code)
            self.from_clean.recv_multipart()
        if self.display:
            print('sink cleaned self tasks')
        self.subscribed_tasks = []

    def _info_ask_clean(self):
        SocketMultiprocess._info_ask_clean(self)
        if self.subscribed_tasks:
            print('subscribed_tasks is not clean: ')
            print('    ' + str(sorted(self.subscribed_tasks)))
        else:
            print('subscribed_tasks is clean')

    def _is_clean(self):
        self_clean = not bool(self.subscribed_tasks)
        return self_clean and SocketMultiprocess._is_clean(self)

    def db_set(self, code, data, with_kwargs):
        """Save the data of a task to the database.

        Arguments:
            - code: code of the task.
            - data: data of the task.
            - with_kwargs: bool defining it the data has kwargs.
                If True, the last element of the data is a list
                with the keys of the kwargs.
        """
        if data:
            if with_kwargs:
                names = range(len(data) - 1)
                names.append(-1)
            else:
                names = range(len(data))
            data_keys = map(lambda(x): code_output(code, x),
                            names)
            with self.redis.pipeline() as pipe:
                while True:
                    try:
                        pipe.watch(data_keys)
                        pipe.multi()
                        [pipe.set(c, d) for c, d in zip(data_keys, data)]
                        res = pipe.execute()
                        if self.display:
                            print('Redis sink: saved data' + str(data_keys) +
                                  ' ; ' + str(data))
                            print('     ' + str(res))
                        break
                    except WatchError:
                        continue
                    finally:
                        pipe.reset()

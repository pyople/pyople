from pyople.pipeline.brokers import broker
from pyople.environments.make_nodes import make_node
from multiprocessing import Process
from threading import Thread
from zmq import Context
from redis import WatchError


class MultiprocessEnvironment(object):
    """Environment that creates, manages and closes the different process
    needed for a multiprocess execution. This includes the Brokers, Admin,
    Server, RedisManager, Ventilators, Workers and Sinks.

    Arguments:
        - zmq_transport: Define the type of sockets of the zmq connections.
        - tornado_sockets: Define if the sockets must be used with tornado.
        - io_threads: Number of threads of new zmq Contexts.
        - num_ventilators: Number of ventilators created.
        - num_workers: Number of workers created.
        - num_sinks: Number of sinks created.
        - is_dispatcher: Boolean defining if the instance is the dispatcher
            group. A dispatcher group creates the brokers and the RedisManager.
            Just one dispatcher is allowed to be running, and is needed if
            non dispatcher instances will be created.
        - db_num: Define the db_num of the redis connection.
        - connection_info: Define the connections of the sockets.
        - mongodb_name: Name of the mongodb database.
        - mongodb_name: Name of the mongodb database.
        - display: Define if the class prints information during execution.
    """
    def __init__(self, zmq_transport='tcp', tornado_sockets=None,
                 io_threads=None, num_ventilators=0, num_workers=0,
                 num_sinks=0, is_dispatcher=True, db_num=0, connection_info={},
                 mongodb_name='', display=False):
        self.db_num = db_num
        self.is_dispatcher = is_dispatcher
        self.mongodb_name = mongodb_name
        self.connection_info = connection_info
        self.group_id = self._get_group_id()
        self.running_process = []
        if zmq_transport == 'inproc':
            self.context = Context()
        else:
            self.context = None
        self.display = display
        self.io_threads = io_threads
        self.tornado_sockets = tornado_sockets
        self.zmq_transport = zmq_transport
        self._connect_info_sockets()
        if is_dispatcher:
            self._make_redis_manager()
            self._make_server()
            self._make_broker()
        self._make_admin()
        self.finished = False
        self.num_ventilators = 0
        self.num_workers = 0
        self.num_sinks = 0
        self.new_nodes(num_ventilators, num_workers, num_sinks)

    def _connect_info_sockets(self):
        """
        Connect the sockets needed for communicate with the created instances.
        """
        from settings.settings import BROKER_IO_THREADS
        from settings.sockets import from_info, to_info
        import zmq
        if self.zmq_transport == 'inproc':
            self.self_context = self.context
        else:
            self.self_context =\
                    zmq.Context(self.io_threads or BROKER_IO_THREADS)
        self.to_info = self.self_context.socket(zmq.PUB)
        self.to_info.connect(getattr(to_info, self.zmq_transport)())
        self.from_info = self.self_context.socket(zmq.SUB)
        self.from_info.connect(getattr(from_info, self.zmq_transport)())
        self.from_info.setsockopt(zmq.SUBSCRIBE, 'Group' + self.group_id)

    def _new_process(self):
        """Return the class for opening a new process.
        Can be a thread or a process, in function of the zmq transport."""
        if self.zmq_transport == 'inproc':
            new = Thread
        else:
            new = Process
        return new

    def _make_broker(self):
        """Create all brokers, defined in settings."""
        from settings.settings import BROKER_LIST
        self.broker_list = []
        for device, name in BROKER_LIST:
            p = self._new_process()(target=broker, args=(device, name,
                    self.context, self.io_threads, self.zmq_transport,
                    self.connection_info))
            self.broker_list.append(p)
            p.start()

    def _get_node_args(self, name):
        """Return the list of arguments for creating a new multiprocess
        instance.

        Arguments:
            - name: name of the new multiprocess instance.
                Names are defined in lower case with underscores.
                Possibles values; admin, redis_manager, server, sink,
                        ventilator, worker.
        """
        args = [name, self.zmq_transport, self.tornado_sockets,
                self.display, self.context, self.io_threads, self.db_num,
                self.group_id, self.connection_info]
        if name == 'worker':
            args.append(self.mongodb_name)
        return args

    def _new_node(self, name):
        """Create a new instance defined by the name.

        Arguments:
            - name: name of the new multiprocess instance.
                Names are defined in lower case with underscores.
                Possibles values; admin, redis_manager, server, sink,
                        ventilator, worker.
        """
        p = self._new_process()(target=make_node,
                    args=(self._get_node_args(name)))
        self.running_process.append(p)
        p.start()

    def _make_redis_manager(self):
        """Create a RedisManager."""
        self._new_node('redis_manager')

    def _make_admin(self):
        """Create a Admin and wait until connection is done."""
        self._new_node('admin')
        self.to_info.send_multipart(['Admin' + self.group_id,
                'ready'])
        from zmq import NOBLOCK, ZMQError
        import time
        while True:
            try:
                msg = self.from_info.recv_multipart(NOBLOCK)
                if msg[1] == 'rep_ready':
                    print("Connection established as group %s%s"
                          % (self.group_id, "(dispatcher)"
                             if self.is_dispatcher else ''))
                    break
            except ZMQError:
                time.sleep(0.1)
                self.to_info.send_multipart(
                        ['Admin' + self.group_id, 'ready'])

    def _make_server(self):
        """Create a Server."""
        self.server = make_node(*self._get_node_args('server'))

    def _make_ventilator(self):
        """Create a Ventilator, and notify to the Admin."""
        self._new_node('ventilator')
        self.num_ventilators += 1
        self.to_info.send_multipart(
                ['Admin' + self.group_id, 'new_node', 'ventilator'])

    def _make_worker(self):
        """Create a Worker, and notify to the Admin."""
        self._new_node('worker')
        self.num_workers += 1
        self.to_info.send_multipart(
                ['Admin' + self.group_id, 'new_node', 'worker'])

    def _make_sink(self):
        """Create a Sink, and notify to the Admin."""
        self._new_node('sink')
        self.num_sinks += 1
        self.to_info.send_multipart(
                ['Admin' + self.group_id, 'new_node', 'sink'])

    def new_nodes(self, num_ventilators, num_workers, num_sinks):
        """Create new ventilators, workers and/or sinks.

        Arguments:
            - num_ventilators: number of new ventilators.
            - num_workers: number of new workers.
            - num_sinks: number of new sinks.
        """
        [self._make_ventilator() for _ in range(num_ventilators)]
        [self._make_worker() for _ in range(num_workers)]
        [self._make_sink() for _ in range(num_sinks)]

    def run(self, procedure, data, data_kwargs={}):
        """Execute the task under the multiprocess environment.

        Arguments:
            - procedure: name of the function to be executed.
            - data: arguments of the function.
            - data_kwargs: key arguments of the function.
        """
        if self.is_dispatcher:
            return self.server._work(procedure, data, data_kwargs)
        else:
            raise (AttributeError,
                    "'%s' is not dispatcher" % self.__class__.__name__)

    def set_display(self, display):
        """
        Change the display option of the instance and all created instances.
        """
        self.display = display
        self.to_info.send_multipart([self.group_id, 'display',
                                     str(self.display)])

    def ask_clean(self):
        """Ask to all created instances if are clean."""
        self.to_info.send_multipart([self.group_id, 'ask_clean'])

    def is_clean(self):
        """Check if the admin of the group is clean."""
        self.to_info.send_multipart(['Admin' + self.group_id, 'node_is_clean',
                                     'Group' + self.group_id])
        msg = self.from_info.recv_multipart()
        while msg[1] != 'rep_node_is_clean':
            msg = self.from_info.recv_multipart()
        ok = msg[2] == 'True'
        return ok

    def is_paused(self):
        """Check if the admin of the group is paused."""
        self.to_info.send_multipart(['Admin' + self.group_id, 'node_is_paused',
                                     'Group' + self.group_id])
        msg = self.from_info.recv_multipart()
        while msg[1] != 'rep_node_is_paused':
            msg = self.from_info.recv_multipart()
        ok = msg[2] == 'True'
        return ok

    def wait_finish(self):
        """Wait until the admin of the group is finished."""
        self.to_info.send_multipart(['Admin' + self.group_id, 'wait_finish'])
        msg = self.from_info.recv_multipart()
        while msg[1] != 'rep_wait_finish':
            msg = self.from_info.recv_multipart()

    def finish(self):
        """Finish the execution, closing all created instances and contexts
        and deleting the group is from the database. If the instance
        is dispatcher waits until the execution is finished.
        """
        if not self.self_context.closed:
            if self.is_dispatcher:
                self.wait_finish()
                self.to_info.send_multipart(['All', 'close', '0'])
            else:
                self.to_info.send_multipart([self.group_id, 'close', '0'])
                self.to_info.send_multipart(['Admin' + self.group_id,
                                             'close', '0'])
        [p.join(1) for p in self.running_process]
        self.self_context.destroy(0)
        if not self.context is None:
            self.context.destroy(0)
        if self.is_dispatcher:
            self.server.close(0)
            [p.terminate() for p in self.broker_list if p.is_alive()]
        [p.terminate() for p in self.running_process if p.is_alive()]
        self.db_del_group_id()

    def clean_exit(self, loop_time=1):
        """Finish the execution of the group, doing a clean exit.
        All instances of the group enters in clean mode and other
        groups waits until the group is closed."""
        if not self.is_closed():
            if not self.is_dispatcher:
                self.to_info.send_multipart(['Admin' + self.group_id,
                                             'clean_exit', str(loop_time)])
                msg = self.from_info.recv_multipart()
                while msg[1] != 'rep_clean_exit':
                    msg = self.from_info.recv_multipart()
        self.finish()
        return self.is_closed()

    def is_closed(self, linger=0):
        """Check if all contexts are closed and all process, or threads,
        aren't alive.

        Arguments:
            - linger: sleep time before checking if is closed."""
        if linger:
            import time
            time.sleep(linger)
        closed = self.self_context.closed
        process_list = self.running_process
        if self.is_dispatcher:
            closed = closed and self.server.context.closed
            process_list += self.broker_list
        closed = closed and not any([p.is_alive()
                for p in process_list])
        return closed

    def _get_group_id(self):
        """Get the name of the group for this instance. Group must be unique
        between different instances of the class."""
        from pipeline.models import connect_redis, code_done
        self.redis = connect_redis(self.db_num, self.connection_info)
        with self.redis.pipeline() as pipe:
            while True:
                try:
                    pipe.watch('group')
                    pipe.multi()
                    pipe.exists('group')
                    new = pipe.execute()[0]
                    if self.is_dispatcher:
                        new_code = -1
                    else:
                        if not new:
                            new_code = 0
                        else:
                            pipe.watch('group')
                            pipe.multi()
                            pipe.sort('group', start=0, num=1, desc=1)
                            sorted_keys = pipe.execute()[0]
                            if sorted_keys:
                                new_code = int(sorted_keys[0]) + 1
                            else:
                                new_code = 0
                    saved = self.db_add_group_id(new_code)
                    if saved == str(new_code):
                        continue
                    return code_done(str(new_code))
                except WatchError:
                    continue
                finally:
                    pipe.reset()

    def db_add_group_id(self, new_group):
        """Save the new group name into the database.

        Arguments:
            - new_group: name of the new group.

        Output:
            - boolean validating the save.
        """
        with self.redis.pipeline() as pipe:
            while True:
                try:
                    pipe.watch('group')
                    pipe.multi()
                    pipe.sadd('group', new_group)
                    return pipe.execute()[0]
                except WatchError:
                    continue
                finally:
                    pipe.reset()

    def db_del_group_id(self):
        """Delete the group name of the instance from the database."""
        from pipeline.models import uncode
        with self.redis.pipeline() as pipe:
            while True:
                try:
                    pipe.watch('group')
                    pipe.multi()
                    pipe.srem('group', uncode(self.group_id))
                    pipe.execute()
                    return
                except WatchError:
                    continue
                finally:
                    pipe.reset()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.finish()
        return False

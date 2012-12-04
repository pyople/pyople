from pyople.pipeline.models import SocketMultiprocess, connect_redis, code_finish, code_done,\
        code_resend, uncode
from pyople.tasks.models import code_output, split_new_task, code_new_task
from zmq import RCVMORE, SUBSCRIBE, ZMQError, UNSUBSCRIBE
from redis import WatchError

#milliseconds defining when can be assumed that environment
#is paused if poll is empty
TIME_SERVER_TASKS = 1000  # milliseconds


def get_subtask(code):
    """Separate the server task code and the subtasks codes."""
    split_code = split_new_task(code)
    return split_code[0], split_code[2]


class GarbageData():
    """Manage the new tasks received in RedisManager."""
    def __init__(self, redis_manager):
        #initialized because update_depend can be called
        #before the initialization in add_depend.
        self.left = 0
        self.redis_manager = redis_manager

    def add_depend(self, num_output, depend):
        """Set the attributes to the class.

        Arguments:
            num_output: Number of outputs of the task.
            depend: Number of dependencies of the task."""
        self.num_output = num_output
        self.left += depend

    def is_finished(self):
        """Check if the task is done and can be deleted."""
        return not bool(self.left)

    def update_depend(self):
        """Update the dependencies. Called when one task uses the data
        of the task of the instance."""
        self.left -= 1

    def delete(self, server_task, subtask):
        """Delete the data in the database associated with
        the server_task and subtask. Publish the message to finish the task."""
        if subtask:
            name_task = code_new_task(server_task, subtask)
        else:
            name_task = server_task
        self.redis_manager.tasks_pub.send(code_finish(name_task))
        self.redis_manager.db_delete(map(lambda(x): code_output(name_task, x),
                      range(self.num_output)))


class RedisManager(SocketMultiprocess):
    """Manage the dependencies between tasks. When a task is finished, delete
    the task from the database and send the finish message to other instances.
    Used as a garbage collector for the database.

    Only one instance of RedisManager is allowed in execution.
    The only dependency of the other classes to this class is the delete
    of the internal tasks, not affecting the flow to execute the tasks.

    Connections between instances:
        Send:
        - Sink: message finish. Also send the message resend
            for the zero tasks.

        Receive:
        - Ventilator: new dependencies for server tasks and
            dependencies updates.
        - Worker: new dependencies for tasks created during execution.
        - Sink: message of done task for the zero tasks.

    DataBase:
        Delete:
        - data for finished tasks.
        - server codes when finished.
    """
    def __init__(self, zmq_transport=None, tornado_sockets=None,
                 display=None, context=None, io_threads=None, db_num=0,
                 group_id='', connection_info={}):
        SocketMultiprocess.__init__(self, zmq_transport,
                                    tornado_sockets=False, display=display,
                                    context=context, io_threads=io_threads,
                                    db_num=db_num, group_id=group_id,
                                    connection_info=connection_info)
        self._connect_db(db_num)
        self.time_server_tasks = TIME_SERVER_TASKS
        self.finish = False
        #dictionary with server codes as keys and  dictionary as values
        #with the code of the subtasks as keys and GarbageData as values.
        self.tasks = {}
        # set of server tasks that are finished and can be deleted
        self.server_delete = set()

    def _connect_db(self, db_num):
        """Connect the instance to the database."""
        self.redis = connect_redis(db_num, self.connection_info)

    def _sockets_path(self):
        new_dict = SocketMultiprocess._sockets_path(self)
        #tasks_pub: used for sending the status of the tasks.
        #tasks_sub: used for receiving the status of the tasks.
        #redis_manager: receive the dependencies from other instances.
        new_dict.update({'tasks_pub': ['to_tasks_publish', 'PUB', ''],
                         'redis_manager': ['redis_manager',
                                           'PULL', '_recv_garbage'],
                         'tasks_sub': ['from_tasks_publish', 'SUB',
                                       '_update_zero_depend']})
        return new_dict

    def _list_subscribe(self):
        subscribe = SocketMultiprocess._list_subscribe(self)
        subscribe.extend(['Node', 'RedisManager',
                          'GroupManager', self.group_id])
        return subscribe

    def _connect_socket(self, sock, name, conn):
        if name == 'redis_manager':
            conn = 'bind'
        SocketMultiprocess._connect_socket(self, sock, name, conn)

    def run(self):
        """Connect the instance and activates the callbacks.
        Return if the admin of the group sends the close signal
        or a keyboard interruption.
        Delete the server tasks from the database if poll is empty
        during 'self.time_server_tasks'."""
        self._connect()
        try:
            while not self.context.closed:
                if self.server_delete and not self.making_clean_exit:
                    poll_recv = self.poller.poll(self.time_server_tasks)
                    if poll_recv:
                        self._manage_poll(poll_recv)
                    else:
                        self.delete_server_tasks()
                else:
                    self._manage_poll(self.poller.poll())
        except KeyboardInterrupt:
            pass
        except ZMQError as zerr:
            if str(zerr) == 'Context was terminated':
                pass
            else:
                raise zerr
        finally:
            if self.zmq_transport != 'inproc':
                self._close(0)

    def _recv_garbage_zmq(self):
        """Receive data from tornado. If the first element is empty
        then dependencies are updated else new dependencies are added."""
        new_code = self.redis_manager.recv(RCVMORE)
        if new_code:
            codes = [new_code]
            while self.redis_manager.RCVMORE:
                codes.append(self.redis_manager.recv(RCVMORE))
            self._update_depend(codes)
        else:
            depend = {}
            while self.redis_manager.RCVMORE:
                key = self.redis_manager.recv(RCVMORE)
                out = int(self.redis_manager.recv(RCVMORE))
                dep = int(self.redis_manager.recv(RCVMORE))
                depend[key] = [out, dep]
            self._new_depend(depend)

    def _recv_garbage_tornado(self, msg):
        """Receive data from tornado. If the first element is empty
        then dependencies are updated else new dependencies are added."""
        new_code = msg[0]
        if new_code:
            self._update_depend(msg)
        else:
            depend = {}
            try:
                iter_depend = iter(msg[1:])
            except StopIteration:
                depend[iter_depend.next()] = [int(iter_depend.next()),
                                             int(iter_depend.next())]
            self._new_depend(depend)

    def _new_depend(self, dependencies):
        """Manage the new dependencies received, updating the 'tasks'.

        Arguments:
            dependencies: dictionary with the task codes as keys.
                The values are lists formatted as arguments of the
                intern function 'add_depend' of the class 'GarbageData'.
        """
        if self.display:
            print('new depend: ' + str(dependencies))
        recv_server = set()
        for task_code, depend in dependencies.items():
            if not depend[1]:
                depend[1] = 1
                self._new_zero_task(task_code)
            server_task, subtask = get_subtask(task_code)
            recv_server.add(server_task)
            dict_tasks = self.tasks.setdefault(server_task, {})
            if subtask in dict_tasks:
                actual_data = dict_tasks[subtask]
                actual_data.add_depend(*depend)
                if actual_data.is_finished():
                    actual_data.delete(server_task, subtask)
                    del dict_tasks[subtask]
            else:
                actual_data = GarbageData(self)
                dict_tasks[subtask] = actual_data
                actual_data.add_depend(*depend)
        self._manage_server_tasks(recv_server)

    def _new_zero_task(self, code):
        """Subscribes to a task without procedure.
        For exemple, for tasks with a 'direct_call'."""
        self.tasks_sub.setsockopt(SUBSCRIBE, code_done(code))
        self.tasks_pub.send(code_resend(code))

    def _update_zero_depend_zmq(self):
        """Auxiliary function of _update_zero_depend using zmq sockets."""
        code = self.tasks_sub.recv()
        self._update_zero_depend(code)

    def _update_zero_depend_tornado(self, msg):
        """Auxiliary function of _update_zero_depend using tornado sockets."""
        self._update_zero_depend(msg[0])

    def _update_zero_depend(self, code):
        """Update a task created with _new_zero_task."""
        self.tasks_sub.setsockopt(UNSUBSCRIBE, code)
        self._update_depend([uncode(code)])

    def _update_depend(self, task_codes):
        """Update the 'tasks' with the receive list of codes.

        Arguments:
            task_codes: list of codes of done tasks
        """
        if self.display:
            print('update depend: ' + str(task_codes))
        recv_server = set()
        for task_code in task_codes:
            server_task, subtask = get_subtask(task_code)
            recv_server.add(server_task)
            dict_tasks = self.tasks.setdefault(server_task, {})
            if subtask in dict_tasks:
                actual_data = dict_tasks[subtask]
                actual_data.update_depend()
                if actual_data.is_finished():
                    actual_data.delete(server_task, subtask)
                    del dict_tasks[subtask]
            else:
                actual_data = GarbageData(self)
                dict_tasks[subtask] = actual_data
                actual_data.update_depend()
        self._manage_server_tasks(recv_server)

    def _manage_server_tasks(self, recv_server):
        """Manage the list of server codes. """
        active = filter(lambda(x): self.tasks[x], recv_server)
        self.server_delete.difference_update(active)
        # discard active tasks. The remaining are tasks to be deleted.
        recv_server.difference_update(active)
        self.server_delete.update(recv_server)

    def delete_server_tasks(self):
        """Delete the keys 'server_delete' from the database and 'tasks'.
        'server_delete' is cleared. If 'finish' is True inform
        the dispatcher admin to finish the execution."""
        self.db_del_server_task(list(self.server_delete))
        for server_task in self.server_delete:
            del self.tasks[server_task]
        self.server_delete.clear()
        if self.finish and not self.tasks:
            self._inform_finish()

    def _inform_finish(self):
        """Send the signal to finish to the dispatcher admin."""
        if self.display:
            print('End')
        self.to_info.send_multipart(['AdminGeneric', 'redis_finish'])

    def _is_clean(self):
        self_clean = not bool(self.tasks) and not bool(self.server_delete)
        return self_clean and SocketMultiprocess._is_clean(self)

    def _info_ask_clean(self):
        print(' ')
        if not self.server_delete:
            print('server delete is clean')
        else:
            print('server delete is not clean')
            print(str(sorted(self.server_delete)))
        print(' ')
        if not self.tasks:
            print('garbage tasks is clean')
        else:
            print('garbage tasks is not clean of keys')
            for server_code, dict_tasks in self.tasks.items():
                print(server_code)
                for subtask, data in dict_tasks.items():
                    print('    ' + subtask + ' ; ' + str(data.left))

    def _info_wait_finish(self):
        """Set finish attribute to True. When the poll of sockets and
        tasks attribute are empty inform the dispatcher admin that
        execution can be finished."""
        if self.tasks or self.server_delete:
            self.finish = True
        else:
            self._inform_finish()

    def db_del_server_task(self, codes):
        """Delete from the database the server codes."""
        if self.display:
            print('del server task: ' + str(codes))
        with self.redis.pipeline() as pipe:
            while True:
                try:
                    pipe.watch('tasks')
                    pipe.multi()
                    [pipe.srem('tasks', code) for code in codes]
                    pipe.execute()
                    break
                except WatchError:
                    continue
                finally:
                    pipe.reset()

    def db_delete(self, code_list):
        """Delete form the database the tasks with the codes in code_list"""
        if code_list:
            with self.redis.pipeline() as pipe:
                while True:
                    try:
                        pipe.watch(code_list)
                        pipe.multi()
                        [pipe.delete(code) for code in code_list]
                        res = pipe.execute()
                        if self.display:
                            print('Redis delete:  ; ' + str(code_list))
                            print('       ' + str(res))
                        return res
                    except WatchError:
                        continue
                    finally:
                        pipe.reset()
        else:
            return []

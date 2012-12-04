from models import SocketMultiprocess, connect_redis
from redis import WatchError
from zmq import UNSUBSCRIBE


class Admin(SocketMultiprocess):
    """Manage the instances in the same group. Used as a link between
    the user and instances.

    It isn't involved in the flow of tasks.

    Admin hasn't any direct connection to other instances.
    It use the to_info and from_info sockets to send and receive messages.
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
        self.ventilator = 0
        self.worker = 0
        self.sink = 0
        #if is_dispatcher this admin is the admin over all admins.
        self.is_dispatcher = group_id == '-1.'

    def _info_ready(self):
        """Send ready signal when connecting"""
        self.send_info(['Group' + self.group_id, 'rep_ready'])

    def send_info(self, msg):
        """Send info using to_info socket."""
        if self.display:
            print('Send info: ' + str(msg))
        if self.tornado_sockets:
            self.to_info.socket.send_multipart(msg)
        else:
            self.to_info.send_multipart(msg)

    def _list_subscribe(self):
        subscribe = SocketMultiprocess._list_subscribe(self)
        subscribe.extend(['AdminGeneric', 'Admin' + self.group_id, 'All'])
        return subscribe

    def _connect_db(self, db_num):
        """Connect the instance to the database."""
        self.redis = connect_redis(db_num, self.connection_info)

    def _info_new_node(self, node):
        """Receive info that a new node is in the group."""
        setattr(self, node, getattr(self, node) + 1)

    def is_clean(self):
        """Ask to managers in the group if they are clean."""
        self.send_info(['TaskManager' + self.group_id, 'is_clean'])
        self.send_info(['RedisManager', 'is_clean'])
        self.print_tasks()

    def wait_finish(self):
        """Waits for all tasks to be finished then reply."""
        self._info_wait_finish()

    def _info_wait_finish(self):
        """Waits for all tasks to be finished then reply."""
        self.send_info(['RedisManager', 'wait_finish'])
        msg = self.from_info.recv_multipart()
        while msg[1] != 'redis_finish':
            msg = self.from_info.recv_multipart()
        self.send_info(['Group' + self.group_id, 'rep_wait_finish'])

    def _info_redis_finish(self):
        """Received signal that all tasks are finished.
        Nothing is done."""
        pass

    def ask(self, ask_msg, num_nodes, nodes_name, args=[]):
        """Ask to to some instances and wait the reply.

        Arguments:
            'ask_msg': name of the function.
            'num_nodes': number of nodes that will reply.
            'nodes_name': name of the receivers.
            'args': arguments of the function 'ask_msg'.
        """
        self.send_info([nodes_name, ask_msg] + args)
        if self.display:
            print('asking ' + str(ask_msg))
        res = []
        wait_for = 'rep_' + ask_msg
        #wait for all nodes to reply
        while num_nodes:
            msg = self.from_info.recv_multipart()
            if msg[1] == wait_for:
                num_nodes -= 1
                res.append(msg[2] == 'True')
        if self.display:
            print('answer: ' + str(res))
        return res

    def _info_node_is_clean(self, rep_name):
        """Check if node_is_clean and reply."""
        ok = self.node_is_clean()
        self.send_info([rep_name, 'rep_node_is_clean', str(ok)])

    def _info_node_is_paused(self, rep_name):
        """Check if node_is_paused and reply."""
        ok = self.node_is_paused()
        print('Asked is_clean from ' + rep_name + ' Is clean group ' +
              self.group_id + ' : ' + str(ok))
        self.send_info([rep_name, 'rep_node_is_paused', str(ok)])

    def _info_finish_clean_exit(self, rep_name):
        """Receive the clean_exit finish signal.
        Nothing is done."""
        print('Clean exit of group ' + rep_name)

    def node_is_clean(self):
        """Check if all nodes in the group of the admin are clean."""
        num_nodes = self.ventilator + self.sink +\
        self.worker + self.is_dispatcher
        return all(self.ask('is_clean', num_nodes, self.group_id))

    def node_is_paused(self):
        """Check if all nodes in the group of the admin are paused."""
        num_nodes = self.ventilator + self.sink +\
        self.worker + self.is_dispatcher
        return not any(self.ask('is_working', num_nodes, self.group_id))

    def admin_is_paused(self, num_groups_id):
        """Check if all admins are paused."""
        return all(self.ask('node_is_paused', num_groups_id, 'AdminGeneric',
                            ['Admin' + self.group_id]))

    def _get_groups_id(self):
        """Get the current groups in the database."""
        return self.redis.smembers('group')

    def _info_clean_exit(self, loop_sleep):
        """Manages the clean_exit signal."""
        loop_sleep = int(loop_sleep)
        self.init_clean_exit()
        num_groups_id = len(self._get_groups_id()) - 1
        print('Waiting exit of group ' + self.group_id)
        self.wait_pause_admin(num_groups_id, loop_sleep)
        print('All nodes paused')
        print('Making clean exit')
        self.make_clean_exit(loop_sleep)
        print('Nodes clean of ' + self.group_id)
        self.send_info(['Group' + self.group_id, 'rep_clean_exit'])

    def wait_pause_admin(self, num_group_id, loop_sleep=1):
        """Wait for all admins to be paused."""
        import time
        self.from_info.setsockopt(UNSUBSCRIBE, 'AdminGeneric')
        while not (self.admin_is_paused(num_group_id)
                   and self.node_is_paused()):
            time.sleep(loop_sleep)

    def init_clean_exit(self):
        """Send the clean_exit begin signal to the group
        of the admin."""
        self.send_info(['Node', 'clean_exit', self.group_id])

    def make_clean_exit(self, loop_sleep=1):
        """Send the clean_exit signal."""
        self.send_info([self.group_id, 'make_clean_exit'])
        import time
        while not self.node_is_clean():
            time.sleep(loop_sleep)
        self.send_info(['All', 'finish_clean_exit',
                                     self.group_id])

    def print_tasks(self):
        """Print data in the database."""
        print('Server Tasks:')
        self.print_server_tasks()
        print(' ')
        print('Pipeline Tasks:')
        self.print_pipeline_tasks()

    def print_server_tasks(self):
        """Print server tasks in the database."""
        print(list(self.redis.smembers('tasks')))

    def print_pipeline_tasks(self):
        """Print data in the database."""
        with self.redis.pipeline() as pipe:
            while True:
                try:
                    keys = [key for key in self.redis.keys() if key != 'tasks']
                    [pipe.watch(code) for code in keys]
                    pipe.multi()
                    [pipe.get(key) for key in keys]
                    res = pipe.execute()
                    break
                except WatchError:
                    continue
                finally:
                    pipe.reset()
        for k, r in sorted(zip(keys, res), key=lambda(x): x[0]):
            print(k.ljust(20) + '    ' + r)

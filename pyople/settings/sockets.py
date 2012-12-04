# -*- coding: utf-8 -*-

__all__ = ['redis', 'mongodb'
           'to_data', 'from_data'
           'from_server', 'to_server',
           'from_info', 'to_info',
           'from_tasks_publish', 'to_tasks_publish',
           'from_new_task', 'to_new_task',
           'from_done_task', 'to_done_task',
           'from_ventilator_clean', 'to_ventilator_clean'
           'from_sink_clean', 'to_sink_clean'
           'redis_manager',
           'TORNADO_SOCKETS',
           'BROKER_LIST']

import settings as s


class _SocketConfiguration():
    """Defines the address of the sockets."""
    def __init__(self, name, ip, port, ignore_global_ip=False):
        self.name = name
        if not ignore_global_ip and s.global_ip is not None:
            ip = s.global_ip
        self.ip = ip
        self.port = port

    def tcp(self, config_info={}):
        """Return the address of the socket using tcp transport."""
        ip = port = None
        if 'global_ip' in config_info:
            ip = config_info['global_ip']
        if self.name in config_info:
            dict_conn = config_info[self.name]
            if 'port' in dict_conn:
                port = dict_conn['port']
            if 'ip' in dict_conn:
                ip = dict_conn['ip']
        if ip is None:
            ip = self.ip
        if port is None:
            port = self.port
        return 'tcp://%s:%s' % (ip, port)

    def inproc(self, config_info={}):
        """Return the address of the socket using inproc transport."""
        return 'inproc://' + self.name

    def ipc(self, config_info={}):
        """Return the address of the socket using ipc transport."""
        import os
        return 'ipc://' + os.path.join(s.DIR_IPC, self.name)

#defines the ip for all connections if global_ip is not None.
#(i.e, all ip defined bellow are ignored)

redis = _SocketConfiguration('redis', s.ip_redis, s.port_redis,
                             ignore_global_ip=True)

# MONGODB connections

#mongodb = _SocketConfiguration('mongodb', ip_mongodb, port_mongodb,
#                             ignore_global_ip=True)

# ZMQ connections

#info: send messages between instances.
from_info = _SocketConfiguration('from_info', s.ip_from_info, s.port_from_info)
to_info = _SocketConfiguration('to_info', s.ip_to_info, s.port_to_info)

#new_task: send the tasks to be executed.
to_new_task = _SocketConfiguration('to_new_task',
        s.ip_to_new_task, s.port_to_new_task)
from_new_task = _SocketConfiguration('from_new_task',
        s.ip_from_new_task, s.port_from_new_task)

#done_task: send the created tasks during execution.
to_done_task = _SocketConfiguration('to_done_task',
        s.ip_to_done_task, s.port_to_done_task)
from_done_task = _SocketConfiguration('from_done_task',
        s.ip_from_done_task, s.port_from_done_task)

#data: send the data to be saved to the database.
to_data = _SocketConfiguration('to_data', s.ip_to_data, s.port_to_data)
from_data = _SocketConfiguration('from_data', s.ip_from_data, s.port_from_data)

#server: send the tasks requested by the server. (User interface)
to_server = _SocketConfiguration('to_server', s.ip_to_server, s.port_to_server)
from_server = _SocketConfiguration('from_server',
        s.ip_from_server, s.port_from_server)

#tasks_publish: send the status of the tasks. (done, resend and finish)
to_tasks_publish = _SocketConfiguration('to_tasks_publish',
        s.ip_to_tasks_publish, s.port_to_tasks_publish)

from_tasks_publish = _SocketConfiguration('from_tasks_publish',
        s.ip_from_tasks_publish, s.port_from_tasks_publish)

#redis_manager: send the dependencies. (create and update)
redis_manager = _SocketConfiguration('redis_manager', s.ip_redis_manager,
        s.port_redis_manager)

#ventilator_clean: send the tasks of Ventilators during clean mode.
from_ventilator_clean = _SocketConfiguration('from_ventilator_clean',
        s.ip_from_ventilator_clean, s.port_from_ventilator_clean)

to_ventilator_clean = _SocketConfiguration('to_ventilator_clean',
        s.ip_to_ventilator_clean, s.port_to_ventilator_clean)

#sink_clean: send the tasks of Sinks during clean mode.
from_sink_clean = _SocketConfiguration('from_sink_clean',
        s.ip_from_sink_clean, s.port_from_sink_clean)

to_sink_clean = _SocketConfiguration('to_sink_clean',
        s.ip_to_sink_clean, s.port_to_sink_clean)

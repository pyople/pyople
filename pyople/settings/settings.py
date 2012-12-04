# -*- coding: utf-8 -*-

#Defines if tornado sockets will be used.
TORNADO_SOCKETS = False
#Defines the number of io threads of the zmq Contexts.
IO_THREADS = 1
#Defines the number of io threads of the zmq Contexts of the brokers.
BROKER_IO_THREADS = 1
#Defines transport used in the zmq connections.
ZMQ_TRANSPORT = 'tcp'
#Directory used in the ipc connection.
DIR_IPC = './temp'

BROKER_LIST = [['FORWARDER', 'tasks_publish'],
                ['STREAMER', 'new_task'],
                ['STREAMER', 'done_task'],
                ['STREAMER', 'data'],
                ['FORWARDER', 'info'],
                ['QUEUE', 'server'],
                ['QUEUE', 'sink_clean'],
                ['QUEUE', 'ventilator_clean']]


#defines the ip for all connections if global_ip is not None.
#(i.e, all ip defined bellow are ignored)

global_ip = '192.168.1.58'

#REDIS connections

ip_redis = '192.168.1.58'
port_redis = 6379


#MONGODB connections

#ip_mongodb = '192.168.1.58'
#port_mongodb = 27017
#mongodb = _SocketConfiguration('mongodb', ip_mongodb, port_mongodb,
#                             ignore_global_ip=True)

#ZMQ connections

ip_from_info = '192.168.1.58'
port_from_info = 60101

ip_to_info = '192.168.1.58'
port_to_info = 60102

ip_to_new_task = '192.168.1.58'
port_to_new_task = 60103

ip_from_new_task = '192.168.1.58'
port_from_new_task = 60104

ip_to_done_task = '192.168.1.58'
port_to_done_task = 60105

ip_from_done_task = '192.168.1.58'
port_from_done_task = 60106

ip_to_data = '192.168.1.58'
port_to_data = 60107

ip_from_data = '192.168.1.58'
port_from_data = 60108

ip_from_server = '192.168.1.58'
port_from_server = 60100

ip_to_server = '192.168.1.58'
port_to_server = 60110

ip_from_tasks_publish = '192.168.1.58'
port_from_tasks_publish = 60118

ip_to_tasks_publish = '192.168.1.58'
port_to_tasks_publish = 60119

ip_redis_manager = '192.168.1.58'
port_redis_manager = 60113

ip_from_ventilator_clean = '192.168.1.58'
port_from_ventilator_clean = 60114

ip_to_ventilator_clean = '192.168.1.58'
port_to_ventilator_clean = 60115

ip_from_sink_clean = '192.168.1.58'
port_from_sink_clean = 60116

ip_to_sink_clean = '192.168.1.58'
port_to_sink_clean = 60117

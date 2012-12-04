import sys
sys.path.append('.')

from environments import MultiprocessEnvironment
from pipeline.models import connect_redis
from zmq import SNDMORE, RCVMORE


def test_interrupt_zmq_tcp():
    io_threads = 1
    zmq_transport = 'tcp'
    tornado_sockets = False
    num_ventilators = 2
    num_workers = 4
    num_sinks = 2
    is_dispatcher = True
    db_num = 1
    connection_info = {}
    r = connect_redis(db_num, connection_info)
    r.flushdb()
    with MultiprocessEnvironment(zmq_transport, tornado_sockets, io_threads,
                   num_ventilators, num_workers, num_sinks,
                   is_dispatcher, db_num, connection_info) as g_dispatcher:
        is_dispatcher = False
        with MultiprocessEnvironment(zmq_transport, tornado_sockets,
                    io_threads, num_ventilators, num_workers, num_sinks,
                    is_dispatcher, db_num, connection_info) as g_node:
            import time
            procedure, data = 'test', [0.01, 1000, []]
            g_dispatcher.server.to_socket.send(procedure, SNDMORE)
            for d in data[:-1]:
                g_dispatcher.server.to_socket.send_json(d, SNDMORE)
            g_dispatcher.server.to_socket.send_json(data[-1])
            time.sleep(4)
            g_node.clean_exit()
            g_dispatcher.server.to_socket.poll()
            res = [g_dispatcher.server.to_socket.recv_json(RCVMORE)]
            while g_dispatcher.server.to_socket.RCVMORE:
                res.append(g_dispatcher.server.to_socket.recv_json(RCVMORE))
            print res
        g_dispatcher.wait_finish()
        assert g_dispatcher.is_clean()
    assert g_dispatcher.is_closed(linger=0.1)
    assert g_node.is_closed(linger=0.1)
    assert not r.keys()
    r.flushdb()

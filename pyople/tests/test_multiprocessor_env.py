import sys
sys.path.append('.')

from environments import MultiprocessEnvironment
from pipeline.models import connect_redis


def test_zmq_tcp():
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
                   is_dispatcher, db_num, connection_info) as g:
        procedure, data, result = 'sleep', [-1], 1
        for i in range(10):
            assert g.run(procedure, data) == result
        for i in range(3):
            procedure, data, result = 'test', [-1, i], i
            assert g.run(procedure, data) == result
        list_iter = [10, 10]
        num_iter = reduce(lambda x, y: (x + 1) * y, reversed(list_iter)) + 1
        procedure, data, result = 'test_deep', [-1, list_iter], num_iter
        assert g.run(procedure, data) == result
        g.wait_finish()
        assert g.is_clean()
    assert g.is_closed(linger=0.1)
    assert not r.keys()
    r.flushdb()


def test_tornado_tcp():
    io_threads = 1
    zmq_transport = 'tcp'
    tornado_sockets = True
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
                   is_dispatcher, db_num) as g:
        procedure, data, result = 'sleep', [-1], 1
        for i in range(10):
            assert g.run(procedure, data) == result
        for i in range(3):
            procedure, data, result = 'test', [-1, i], i
            assert g.run(procedure, data) == result
        list_iter = [10, 10]
        num_iter = reduce(lambda x, y: (x + 1) * y, reversed(list_iter)) + 1
        procedure, data, result = 'test_deep', [-1, list_iter], num_iter
        assert g.run(procedure, data) == result
        g.wait_finish()
        assert g.is_clean()
    assert g.is_closed(linger=0.1)
    assert not r.keys()
    r.flushdb()


def test_zmq_ipc():
    io_threads = 1
    zmq_transport = 'ipc'
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
                   is_dispatcher, db_num) as g:
        procedure, data, result = 'sleep', [-1], 1
        for i in range(10):
            assert g.run(procedure, data) == result
        for i in range(3):
            procedure, data, result = 'test', [-1, i], i
        assert g.run(procedure, data) == result
        list_iter = [10, 10]
        num_iter = reduce(lambda x, y: (x + 1) * y, reversed(list_iter)) + 1
        procedure, data, result = 'test_deep', [-1, list_iter], num_iter
        assert g.run(procedure, data) == result
        g.wait_finish()
        assert g.is_clean()
    assert g.is_closed(linger=0.1)
    assert not r.keys()
    r.flushdb()
    import shutil
    from settings.settings import DIR_IPC
    shutil.rmtree(DIR_IPC)


def test_tornado_ipc():
    io_threads = 1
    zmq_transport = 'ipc'
    tornado_sockets = True
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
                   is_dispatcher, db_num) as g:
        procedure, data, result = 'sleep', [-1], 1
        for i in range(10):
            assert g.run(procedure, data) == result
        for i in range(3):
            procedure, data, result = 'test', [-1, i], i
        assert g.run(procedure, data) == result
        list_iter = [10, 10]
        num_iter = reduce(lambda x, y: (x + 1) * y, reversed(list_iter)) + 1
        procedure, data, result = 'test_deep', [-1, list_iter], num_iter
        assert g.run(procedure, data) == result
        g.wait_finish()
        assert g.is_clean()
    assert g.is_closed(linger=0.1)
    assert not r.keys()
    r.flushdb()
    import shutil
    from settings.settings import DIR_IPC
    shutil.rmtree(DIR_IPC)

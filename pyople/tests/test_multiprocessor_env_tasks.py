import sys
sys.path.append('.')

from pyople.environments import MultiprocessEnvironment
from pyople.pipeline.models import connect_redis
import time


def create_data(len_data, tag):
    import random
    if tag == 1:
        return [random.random() + 1 for _ in range(len_data)]
    elif tag == 0:
        return [random.random() - 2 for _ in range(len_data)]


def make_data_tags(len_data, num_data, init=0):
    data = []
    for key, num in enumerate(num_data):
        [data.append(create_data(len_data, key)) for _ in range(num)]
    keys = [str(init + i) for i in range(sum(num_data))]
    data = dict(zip(keys, data))
    tags = []
    [tags.extend([key] * num) for key, num in enumerate(num_data)]
    tags = dict(zip(keys, tags))
    return data, tags


def test_tasks_zmq_tcp():
    len_data, num_data, num_diag = [10, [10, 10], [2, 2]]
    data_compare, tag_compare = make_data_tags(len_data, num_data, 0)
    data_diag, tag_diag = make_data_tags(len_data, num_diag,
                                         len(data_compare))
    procedure, data, result = 'classify_mse',\
            [data_compare, tag_compare, data_diag], tag_diag
    io_threads = 4
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
        assert g.run(procedure, data) == result
        g.wait_finish()
        assert g.is_clean()
    assert g.is_closed(linger=0.1)
    assert not r.keys()
    r.flushdb()
    time.sleep(0.2)


def test_tasks_tornado_tcp():
    len_data, num_data, num_diag = [10, [10, 10], [2, 2]]
    data_compare, tag_compare = make_data_tags(len_data, num_data, 0)
    data_diag, tag_diag = make_data_tags(len_data, num_diag,
                                         len(data_compare))
    procedure, data, result = 'classify_mse',\
            [data_compare, tag_compare, data_diag], tag_diag
    io_threads = 4
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
                   is_dispatcher, db_num, connection_info) as g:
        assert g.run(procedure, data) == result
        g.wait_finish()
        assert g.is_clean()
    assert g.is_closed(linger=0.1)
    assert not r.keys()
    r.flushdb()
    time.sleep(0.2)


def test_tasks_zmq_ipc():
    len_data, num_data, num_diag = [10, [10, 10], [2, 2]]
    data_compare, tag_compare = make_data_tags(len_data, num_data, 0)
    data_diag, tag_diag = make_data_tags(len_data, num_diag,
                                         len(data_compare))
    procedure, data, result = 'classify_mse',\
            [data_compare, tag_compare, data_diag], tag_diag
    io_threads = 4
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
                   is_dispatcher, db_num, connection_info) as g:
        assert g.run(procedure, data) == result
        g.wait_finish()
        assert g.is_clean()
    assert g.is_closed(linger=0.1)
    assert not r.keys()
    r.flushdb()
    import shutil
    from settings.settings import DIR_IPC
    shutil.rmtree(DIR_IPC)
    time.sleep(0.2)


def test_tasks_tornado_ipc():
    len_data, num_data, num_diag = [10, [10, 10], [2, 2]]
    data_compare, tag_compare = make_data_tags(len_data, num_data, 0)
    data_diag, tag_diag = make_data_tags(len_data, num_diag,
                                         len(data_compare))
    procedure, data, result = 'classify_mse',\
            [data_compare, tag_compare, data_diag], tag_diag
    io_threads = 4
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
                   is_dispatcher, db_num, connection_info) as g:
        assert g.run(procedure, data) == result
        g.wait_finish()
        assert g.is_clean()
    assert g.is_closed(linger=0.1)
    assert not r.keys()
    r.flushdb()
    import shutil
    from settings.settings import DIR_IPC
    shutil.rmtree(DIR_IPC)
    time.sleep(0.2)

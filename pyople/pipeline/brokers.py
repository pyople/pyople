import zmq
import pyople.settings.sockets as sockets
import os


def broker(device, name, context, io_threads, zmq_transport, connection_info):
    """Create a broker.

    Arguments:
        - device: type of zmq broker. Can be STREAMER, FORWARDER or QUEUE.
        - name: name of sockets of settings to be connected.
        - context: Can be a zmq Context for some zmq_transports.
        - io_threads: Number of threads of new zmq Contexts.
        - zmq_transport: Define the type of sockets of the zmq connections.
        - connection_info: Define the connections of the sockets.
    """
    if context is None:
        from settings.settings import BROKER_IO_THREADS
        context = zmq.Context(io_threads or BROKER_IO_THREADS)
    brokers_dict = {'STREAMER': [zmq.PULL, zmq.PUSH],
                    'FORWARDER': [zmq.SUB, zmq.PUB],
                    'QUEUE': [zmq.DEALER, zmq.ROUTER]}
    if zmq_transport == 'ipc':
        from settings.settings import DIR_IPC
        if not os.path.isdir(DIR_IPC):
            os.makedirs(DIR_IPC)
    frontend = context.socket(brokers_dict[device][0])
    frontend.bind(getattr(getattr(sockets, 'to_%s' % name),
                          zmq_transport)(connection_info))
    if device == 'FORWARDER':
        frontend.setsockopt(zmq.SUBSCRIBE, '')
    backend = context.socket(brokers_dict[device][1])
    backend.bind(getattr(getattr(sockets, 'from_%s' % name),
                         zmq_transport)(connection_info))
    try:
        zmq.device(getattr(zmq, device), frontend, backend)
    except zmq.ZMQError as zerr:
        if str(zerr) == 'Context was terminated':
            pass
        else:
            raise zerr


def run_all_brokers(zmq_transport='tcp', context=None, io_threads=None,
                    connection_info={}):
    """Create all brokers defined in settings.

    Arguments:
        - zmq_transport: Define the type of sockets of the zmq connections.
        - context: Can be a zmq Context for some zmq_transports.
        - io_threads: Number of threads of new zmq Contexts.
        - connection_info: Define the connections of the sockets.
    """
    from settings.settings import BROKER_IO_THREADS, BROKER_LIST
    if context is None:
        context = zmq.Context(io_threads or BROKER_IO_THREADS)
    try:
        from threading import Thread
        new_process = Thread
        running = []
        for device, name in BROKER_LIST:
            new = new_process(target=broker, args=(device, name, context,
                                                   io_threads, zmq_transport,
                                                   connection_info))
            new.start()
            running.append(new)
        import signal
        signal.pause()
    except KeyboardInterrupt:
        print('Finished by user')
    except zmq.ZMQError as zerr:
        if str(zerr) == 'Context was terminated':
            pass
        else:
            raise zerr
    finally:
        if zmq_transport == 'inproc' and not context.closed:
            context.term()

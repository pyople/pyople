

def make_node(name, zmq_transport=None, tornado_sockets=None,
                 display=None, context=None, io_threads=1, db_num=0,
                 group_id='', connection_info={}, mongodb_name=''):
    """Create a instance with the type SocketMultiprocess.

    Arguments:
        - name: name of the instance to be created.
        - zmq_transport: Define the type of sockets of the zmq connections.
        - tornado_sockets: Define if the sockets must be used with tornado.
        - display: Define if the class prints information.
        - context: Can be a zmq Context for some zmq_transports.
        - io_threads: Number of threads of new zmq Contexts.
        - db_num: Define the db_num of the redis connection.
        - group_id: Group of the instance. Used for sending information
                 between instances.
        - connection_info: Define the connections of the sockets.
        - mongodb_name: Name of the mongodb database.
    """
    try:
        module = __import__('pipeline.' + name, fromlist=[name])
        name_class = ''.join(map(lambda(x): x.capitalize(), name.split('_')))
        if name_class != 'Worker':
            #worker doesn't have connection to the database
            node = getattr(module, name_class)(zmq_transport, tornado_sockets,
                                               display, context, io_threads,
                                               db_num, group_id,
                                               connection_info)
        else:
            node = getattr(module, name_class)(zmq_transport, tornado_sockets,
                                               display, context, io_threads,
                                               db_num, group_id,
                                               connection_info, mongodb_name)
        ok = True
    except ImportError:
        if display:
            print('Cannot import ' + name)
        ok = False
    if ok:
        if display:
            print('Running ' + name)
        node.connect()
        if name != 'server':
            node.run()
        else:
            return node

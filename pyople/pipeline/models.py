import zmq
import pyople.settings.sockets as sockets
import redis


def uncode(code):
    """Get the original code"""
    return code[:-1]


def code_done(code):
    """Return the string to be send when a code is done."""
    return code + '.'


def code_finish(code):
    """Return the string to be send when a code has been used."""
    return code + 'f'


def code_resend(code):
    """Return the string to be send for asking if a code is done."""
    return code + 's'


def connect_redis(db_num=0, config_info={}):
    """Connect to the redis data base. If config_info is empty
    the configuration in settings is used."""
    ip = port = None
    if 'global_ip' in config_info:
        ip = config_info['global_ip']
    if 'redis' in config_info:
        dict_conn = config_info['redis']
        if 'port' in dict_conn:
            port = dict_conn['port']
        if 'ip' in dict_conn:
            ip = dict_conn['ip']
    redis_info = getattr(sockets, 'redis')
    if ip is None:
        ip = redis_info.ip
    if port is None:
        port = redis_info.port
    r = redis.Redis(host=ip, port=port, db=db_num)
    r.ping()
    return r


class SocketMultiprocess(object):
    """Main class for all classes managing sockets in the environments.
    By default the configuration in settings is used.

     Arguments:
         - zmq_transport: Define the type of sockets of the zmq connections.
         - tornado_sockets: Define if the sockets must be used with tornado.
         - display: Define if the class prints information during execution.
         - context: Can be a zmq Context for some zmq_transports.
         - io_threads: Number of threads of new zmq Contexts.
         - db_num: Define the db_num of the redis connection.
         - group_id: Group of the instance. Used for sending information
                 between instances.
         - connection_info: Define the connections of the sockets.
         - mongodb_name: Name of the mongodb database.
     """
    def __init__(self, zmq_transport=None, tornado_sockets=None,
                 display=False, context=None, io_threads=None, db_num=0,
                 group_id='', connection_info={}, mongodb_name=''):
        import pyople.settings.settings as settings
        self.zmq_transport = settings.ZMQ_TRANSPORT\
                if zmq_transport is None else zmq_transport
        self.tornado_sockets = settings.TORNADO_SOCKETS\
                if tornado_sockets is None else tornado_sockets
        self.display = display
        self.io_threads = settings.IO_THREADS\
                if io_threads is None else io_threads
        if context is not None:
            self.context = context
            self._make_connect(self._sockets_path())
        self.group_id = group_id
        self.wait_continue = []
        self.making_clean_exit = False
        self.connection_info = connection_info

    def _connect(self):
        """Connect and subscribe all sockets."""
        if hasattr(self, 'context') and self.context.closed:
            self.context.destroy(linger=0)
            del self.context
        if not hasattr(self, 'context'):
            if not self.tornado_sockets:
                self.callbacks = {}
                self.poller = zmq.Poller()
            self.context = zmq.Context(self.io_threads)
            self._make_connect(self._sockets_path())
            [self.from_info.setsockopt(zmq.SUBSCRIBE, key_sub)
             for key_sub in self._list_subscribe()]

    def connect(self):
        """Connect and subscribe all sockets."""
        self._connect()

    def _sockets_path(self):
        """Define the connections of sockets.
        The key is the name of the attribute.
        The values are the name of the socket in settings,
        the type of connection and the name of the callback if needed."""
        # to_info: used for sending messages between instances.
        # from_info: used for receiving info from other instances.
        return {'from_info': ['from_info', 'SUB', '_manage_info'],
                'to_info': ['to_info', 'PUB', '']}

    def _sockets_path_clean(self, direction):
        """Sockets to be connected when cleaning the instance.
        The format is the same defined in _sockets_path."""
        return {}

    def _list_subscribe(self):
        """List of subscribes for from_info socket."""
        return ['All']

    def _make_connect(self, dict_sockets):
        """Create and connect the sockets in dict_sockets.
        The key is the name of the attribute.
        The values are the name of the socket in settings,
        the type of connection and the name of the callback if needed."""
        type_sockets = ['zmq', 'tornado'][self.tornado_sockets]
        if self.display:
            print('Connecting using %s sockets with %s' % (type_sockets,
                                                          self.zmq_transport))
        if self.tornado_sockets:
            import zmq.eventloop.zmqstream as zmqstream
        if self.zmq_transport == 'ipc':
            import os
            from settings.settings import DIR_IPC
            if not os.path.isdir(DIR_IPC):
                os.makedirs(DIR_IPC)
        for name, values in dict_sockets.items():
            new_sock = self.context.socket(
                    getattr(zmq, values[1]))
            if self.tornado_sockets:
                new_stream = zmqstream.ZMQStream(new_sock)
            else:
                new_stream = new_sock
            self._connect_socket(new_stream, values[0], 'connect')
            #values[2] is the name of the callback to register if isn't empty
            if values[2]:
                callback_name = values[2] + '_' + type_sockets
                if self.tornado_sockets:
                    new_stream.on_recv(getattr(self, callback_name))
                else:
                    self.poller.register(new_stream, zmq.POLLIN)
                    self.callbacks[new_stream] = getattr(self, callback_name)
            setattr(self, name, new_stream)

    def _connect_socket(self, socket, name, conn):
        '''Connect one socket with the defined arguments.

        sock: Socked to be connected.
        name: Name of the socket in settings defining the address.
        conn: Type of connection. Can be connection or bind.
        '''
        getattr(socket, conn)(getattr(getattr(sockets, name),
                                    self.zmq_transport)(self.connection_info))

    def run(self):
        """Connect the instance and activates the callbacks.
        Return if the admin of the group sends the close signal
        or a keyboard interruption."""
        self._connect()
        try:
            if self.tornado_sockets:
                import zmq.eventloop.zmqstream as zmqstream
                zmqstream.IOLoop.instance().start()
            else:
                while not self.context.closed:
                    self._manage_poll(self.poller.poll())
        except KeyboardInterrupt:
            #Silent exit if the user cancel the execution.
            pass
        except zmq.ZMQError as zerr:
            if str(zerr) == 'Context was terminated':
                #Silent exit if the context is closed. (Close signal)
                pass
            else:
                raise zerr
        finally:
            #prevent that sockets remains opened.
            if self.zmq_transport != 'inproc':
                self._close(0)

    def _manage_poll(self, poll):
        """Manage the poll selection the callbacks.
        Used if not using tornado sockets."""
        if self.display:
            print(self.__class__.__name__ + ' Poll recv:')
        for socket in poll:
            if (socket[0] in self.callbacks) and (not socket[0].closed):
                self.callbacks[socket[0]]()
        if self.display:
            print(' ')

    def _close(self, linger=None):
        """Close the context and sockets."""
        if self.display:
            print('Closing')
        if self.tornado_sockets:
            import zmq.eventloop.zmqstream as zmqstream
            zmqstream.IOLoop.instance().stop()
        else:
            if not self.context.closed:
                if linger is None:
                    self.context.term()
                else:
                    self.context.destroy(linger=int(linger))

    def close(self, linger=None):
        """Close the context and sockets."""
        self._close(linger)

    def _manage_info_zmq(self):
        """Auxiliary function for zmq"""
        msg = self.from_info.recv_multipart()
        self._manage_info(msg[1], *msg[2:])

    def _manage_info_tornado(self, msg):
        """Auxiliary function for tornado"""
        self._manage_info(msg[1], *msg[2:])

    def _manage_info(self, procedure, *data):
        """Calls the function '_info_%s' %procedure
        using data as the arguments. Used for sending signals
        between instances with pub sub."""
        if self.display:
            print('Recv info: ' + procedure + str(data))
        if data:
            if len(data) == 1:
                data = data[0]
            getattr(self, '_info_' + procedure)(data)
        else:
            getattr(self, '_info_' + procedure)()

    def _info_clean_exit(self, group_id):
        """If group_id is the instance group close the current execution,
        cleaning all sockets and data. Else enter in clean mode, waiting
        the group_id to be closed before continuing."""
        self.making_clean_exit = True
        if self.group_id == group_id:
            self._make_connect(self._sockets_path_clean('from'))
            self._clean_exit()
        else:
            self._make_connect(self._sockets_path_clean('to'))
            self.wait_continue.append(group_id)
            self._disable_callbacks(self._sockets_path(), False)

    def _info_finish_clean_exit(self, group_id):
        """Signal from group_id that cleaning was done.
        If the instance group_id is the instance group the instance is closed.
        Else the execution continues."""
        if self.group_id == group_id:
            self.close(0)
        else:
            self.wait_continue.remove(group_id)
            if not self.wait_continue:
                self.making_clean_exit = False
                self._disable_callbacks(self._sockets_path_clean('to'), True)
                self._enable_callbacks(self._sockets_path())

    def _info_make_clean_exit(self):
        """Disable the sockets and send all intern data and
        poll of sockets to other instances."""
        self._disable_callbacks(self._sockets_path(), True)
        if hasattr(self, '_distribute_self_tasks'):
            self._distribute_self_tasks()

    def _clean_exit(self):
        """Replace the callbacks to the callbacks of clean mode.
        Recieved data is send to other instances."""
        for name, values in self._sockets_path().items():
            if values[2] and name != 'from_info':
                callback_name = values[2]
                setattr(self, callback_name + '_old',
                        getattr(self, callback_name))
                new_func = getattr(self, callback_name + '_clean_exit')
                setattr(self, callback_name, new_func)

    def _disable_callbacks(self, dict_sockets, to_close=False):
        """Disable the callbacks defined in 'dict_sockets'.
        If 'to_close' the sockets are closed."""
        for name, values in dict_sockets.items():
            new_stream = getattr(self, name)
            if name not in ['from_info', 'to_info']:
                if values[2]:
                    if self.tornado_sockets:
                        new_stream.on_recv()
                    else:
                        if new_stream in self.callbacks:
                            self.poller.unregister(new_stream)
                            del self.callbacks[new_stream]
                if to_close:
                    new_stream.close(linger=0)

    def _enable_callbacks(self, dict_sockets):
        """Register the callbacks in 'dict_sockets'."""
        type_sockets = ['zmq', 'tornado'][self.tornado_sockets]
        for name, values in dict_sockets.items():
            if values[2] and name != 'from_info':
                callback_name = values[2] + '_' + type_sockets
                new_stream = getattr(self, name)
                if self.tornado_sockets:
                    new_stream.on_recv(getattr(self, callback_name))
                else:
                    self.poller.register(new_stream, zmq.POLLIN)
                    self.callbacks[new_stream] = getattr(self, callback_name)

    def _has_poll(self, dict_sockets):
        """Check if it is any poll of the sockets in 'dict_sockets'."""
        for name, values in dict_sockets.items():
            #if name not in ['from_info', 'to_info']:
            if values[2] and name != 'from_info':
                new_stream = getattr(self, name)
                if self.tornado_sockets:
                    socket = new_stream.socket
                else:
                    socket = new_stream
                poll = socket.poll(1000, zmq.POLLIN)
                if self.display:
                    print('    ' + name + ' has poll ' + str(poll))
                if poll:
                    return True
        return False

    def is_working(self):
        """Check if the instance has any socket with not empty poll"""
        if self.wait_continue:
            bool_working = self._has_poll(self._sockets_path_clean('to'))
        else:
            #if self.making_clean_exit:
            #    bool_working = self._has_poll(
            #self._sockets_path_clean('from'))
            #else:
            bool_working = self._has_poll(self._sockets_path())
        return bool_working

    def _info_is_working(self):
        """Check if instance is working and reply to the admin"""
        bool_working = self.is_working()
        if self.display:
            print(self.__class__.__name__ + ' is working : ' +
                  str(bool_working))
        self.to_info.send_multipart(['Admin' + self.group_id,
                'rep_is_working', str(bool_working)])

    def _info_is_paused(self):
        """Check if instance is paused and reply to the admin"""
        bool_working = self.is_working()
        if self.display:
            print(self.__class__.__name__ + ' is paused : ' +
                  str(not bool_working))
        self.to_info.send_multipart(['Admin' + self.group_id,
                'rep_is_paused', str(not bool_working)])

    def _info_ask_clean(self):
        """Check if all sockets polls are empty and intern data is empty."""
        print(' ')
        is_clean = SocketMultiprocess._is_clean(self)
        print(self.__class__.__name__ + ' has poll clean: ' + str(is_clean))
        print(' ')

    def _info_is_clean(self):
        """Check if instance is clean and reply to the admin"""
        is_clean = self._is_clean()
        if self.display:
            print(self.__class__.__name__ + ' is clean: ' + str(is_clean))
        self.to_info.send_multipart(['Admin' + self.group_id, 'rep_is_clean',
                                str(is_clean)])

    def _is_clean(self):
        """Check if all sockets polls are empty and intern data is empty."""
        if not self.wait_continue:
            if self.making_clean_exit:
                return True
            else:
                dict_sockets = self._sockets_path()
        else:
            dict_sockets = self._sockets_path_clean('to')
        return not self._has_poll(dict_sockets)

    def _info_close(self, linger=-1):
        """Close the instance receiving the close signal."""
        self._close(linger)

    def _info_display(self, b):
        """Change the display attribute."""
        if self.display:
            print('Changed display option')
        if b[0] == 'True':
            self.display = True
        elif b[0] == 'False':
            self.display = False
        else:
            self.display = not self.display
        if self.display:
            print('Enabled display option')

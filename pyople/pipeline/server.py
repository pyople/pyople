from pyople.pipeline.models import SocketMultiprocess
from zmq import SNDMORE, RCVMORE
import pyople.tasks


class Server(SocketMultiprocess):
    """User interface for running tasks into the environments.

    Connections between instances:
        Send:
        - Ventilator: the new tasks created to be executed.

        Receive:
        - Ventilator: the output of sent tasks.
        """

    def __init__(self, zmq_transport=None, tornado_sockets=None,
                 display=None, context=None, io_threads=None, db_num=0,
                 group_id='', connection_info={}):
        SocketMultiprocess.__init__(self, zmq_transport,
                                    tornado_sockets=False, display=display,
                                    context=context, io_threads=io_threads,
                                    db_num=db_num, group_id=group_id,
                                    connection_info=connection_info)
        from tasks.models import MultiprocessTask
        MultiprocessTask.set_environment(self)

    def add_call(self, procedure, num_output, args, kwargs, code_type):
        """Send the call of a multiprocess task to the enviroment
        and block until the reply is received.

        Arguments:
            - procedure: name of the function to be executed.
            - num_output: number of outputs of the function. (unused)
            - args: arguments used in the call of the function.
            - kwargs: key arguments used in the call of the function.
            - code_type: Type of the MultiCode used.(unused)
        """
        return self.run(procedure, args, kwargs)

    def _list_subscribe(self):
        return []

    def _sockets_path(self):
        #to_socket: used for sending the request of running a new task.
        return {'to_socket': ['from_server', 'REQ', '']}

    def set_identity(self, identity):
        """Set the identity as a zmq socket."""
        self.identity = identity

    def run(self, procedure, data, kwargs_data={}):
        """Send the call of a multiprocess task to the enviroment
        and block until the reply is received."""
        try:
            return self._work(procedure, data, send_kwargs={})
        except KeyboardInterrupt:
            pass

    def _work(self, procedure, send_data, send_kwargs):
        """Send the call of a multiprocess task to the enviroment
        and block until the reply is received."""
        if self.display:
            print('Send: ' + str([procedure, send_data]))
        if send_data or send_kwargs:
            self.to_socket.send(procedure, SNDMORE)
            for d in send_data:
                self.to_socket.send_json(d, SNDMORE)
            for k_d in send_kwargs.values():
                self.to_socket.send_json(k_d, SNDMORE)
            self.to_socket.send_json(send_kwargs.keys())
        else:
            self.to_socket.send(procedure)
        self.to_socket.poll()
        recv_data = [self.to_socket.recv_json(RCVMORE)]
        while self.to_socket.RCVMORE:
            recv_data.append(self.to_socket.recv_json(RCVMORE))
        if getattr(tasks, procedure).num_output == 1:
            recv_data = recv_data[0]
        if self.display:
            print('Recv: ' + str(recv_data))
        return recv_data

    def wait_connection(self):
        """Wait until the environment is ready."""
        self.run('wait_connection', [])

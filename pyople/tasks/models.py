def code_new_task(code, num):
    """Define the new_code name"""
    return code + '_' + str(num)


def split_new_task(code):
    """Split the code into the previous codes"""
    return code.partition('_')


def code_output(code, num):
    """Define the name of the outputs"""
    return code + ':' + str(num)


def split_output(code):
    """Get the name of the task code and number of output"""
    return code.split(':', 1)


def decode_output(code):
    """Get the number of output"""
    return split_output(code)[0]

#Defines the name used for encoding the kwargs of a task
VALUE_KWARGS = '-1'


def has_kwargs(needed):
    """Check if the list of needed has kwargs"""
    return split_output(needed[-1])[1] == VALUE_KWARGS


def code_kwargs(code):
    """Encode the kwargs for the current code"""
    return code_output(code, VALUE_KWARGS)


class MultiprocessTask():
    """
    This class should be interpreted as a decorated function of 'self.func'.
    The attributes 'code' and 'num_output' defines the type and length
    of the expected output, which cannot be done with introspection.
    """
    def __init__(self, func, num_output, code_type):
        self.func = func
        self.num_output = num_output
        self.code_type = code_type
        self.name = func.func_name

    def __call__(self, *args, **kwargs):
        # Check if the args must be transformed before the call
        if args and issubclass(self.code_type, ExtendMultiCode):
            if issubclass(self.code_type, DictMultiCode):
                dict_data = args[0]
                if isinstance(dict_data, dict):
                    dict_data = dict_data.items()
                if isinstance(dict_data, (list, tuple)):
                    args = []
                    [args.extend(d) for d in dict_data]
                else:
                    args = [dict_data]
            else:
                args = args[0]
        return self.environment.add_call(self.name, self.num_output,
                              args, kwargs, self.code_type)

    @classmethod
    def set_environment(cls, environment):
        """Set the environment that will trigger the future calls
        of all MultiprocessTask instances"""
        cls.environment = environment

    @classmethod
    def get_environment(cls):
        """Get the environment.
        Used when any MultiprocessTask instances is called."""
        return cls.environment


class BasicMultiCode():
    """Used as outputs when calling MultiprocessTask."""
    def __init__(self, code_task, code_num_output):
        self.code = code_output(code_task, code_num_output)
        self.code_task = code_task
        self.code_num_output = code_num_output

    def split(self, c=None, num=None):
        if num is None:
            return self.code.split(c)
        else:
            return self.code.split(c, num)

    def __repr__(self):
        return self.code


class FinalMultiCode(BasicMultiCode):
    """Extension of BasicMultiCode used as output of the main call.
    The direct_call function defines the call, and is done when all
    inputs are ready. This is useful if the call function is trivial."""
    @staticmethod
    def direct_call(data):
        if len(data) == 1:
            return [data[0]]
        else:
            return data


class ExtendMultiCode(BasicMultiCode):
    """Extension of BasicMultiCode used as output of function
    with variable output length"""
    def __init__(self, code_task, num_output):
        BasicMultiCode.__init__(self, code_task, num_output)
        self.environment = MultiprocessTask.get_environment()
        result = self.environment.list_result[self.code_task]
        self.needed = result.needed
        self.data = result.data

    def __len__(self):
        return len(self.needed)

    def __iter__(self):
        return iter(self.needed)


class ListMultiCode(ExtendMultiCode):
    """Extension of ExtendMultiCode used for list type outputs."""
    def append(self, args):
        self.environment.separate_needed_data(self.code_task, [args],
                                       self.needed, self.data)

    def extend(self, args):
        self.environment.separate_needed_data(self.code_task, args,
                                       self.needed, self.data)

    def __add__(self, args):
        self.extend(args)

    def __getitem__(self, ind):
        res = self.needed[ind]
        return res

    @staticmethod
    def direct_call(data):
        return ['[' + ', '.join(data) + ']']


class DataBaseMultiCode(ExtendMultiCode):
    """Extension of ExtendMultiCode used for creating a task
    without a procedure. Useful for creating a list of elements
    which will be used as input of different functions."""
    def __init__(self, code_task, num_output):
        ExtendMultiCode.__init__(self, code_task, num_output)
        self.list_depend = self.environment.task_dependencies[self.code_task]

    def append(self, *args):
        self.environment.separate_needed_data(self.code_task, args,
                                       self.needed, self.data)
        self.list_depend[0] += len(args)

    def __getitem__(self, ind):
        res = self.needed[ind]
        return res

    @staticmethod
    def direct_call(data):
        return data


class DictMultiCode(ExtendMultiCode):
    """Extension of ExtendMultiCode used for dict type outputs."""
    def __init__(self, code_task, num_output):
        ExtendMultiCode.__init__(self, code_task, num_output)
        #Index of self.needed that are keys.
        self._index_keys = []
        self._add_index_keys(self.needed, [])

    def __setitem__(self, *args):
        self._add_index_keys(args, self.needed)
        self.environment.separate_needed_data(self.code_task, args,
                                       self.needed, self.data)

    def _add_index_keys(self, args, needed):
        len_args = len(args)
        # if len_args is 1 the call is from and update.
        if not len_args % 2:
            self._index_keys.extend(range(len(needed),
                                           len(needed) + len_args, 2))
        else:
            if len_args != 1:
                raise TypeError("cannot convert arguments "\
                                "to dictionary update sequence")

    def __len__(self):
        return len(self._index_keys)

    def keys(self):
        return self._get_items(0)

    def values(self):
        return self._get_items(1)

    def items(self):
        return zip(self.keys(), self.values())

    def __iter__(self):
        return iter(self.keys())

    def _get_items(self, parity):
        return [self.needed[n + parity] for n in self._index_keys]

    def __getitem__(self, key):
        if not isinstance(key, BasicMultiCode):
            try:
                ind = self.data.index(key)
                key = code_output(self.code_task, ind)
            except ValueError:
                raise (KeyError, key)
        try:
            for k in self._index_keys:
                need = self.needed[k]
                if isinstance(need, BasicMultiCode) and\
                        str(need) == str(key):
                    return self.needed[k + 1]
        except ValueError:
            raise (KeyError, key)

    def update(self, args):
        self.__setitem__(*[args])

    @staticmethod
    def direct_call(data):
        #Pop the elements of data added in a update call.
        def separate_to_update(data):
            is_key = True
            to_update = []
            for i, key in enumerate(data):
                if is_key:
                    if not key.startswith(("'", '"')):
                        to_update.append(i)
                        continue
                is_key = not is_key
            to_update_data = [data.pop(i) for i in to_update]
            to_update_data = [d for d in to_update_data if d[1:-1]]
            return to_update_data
        to_update = separate_to_update(data)
        iter_data = iter(data)
        dict_res = ','.join([iter_data.next() + ':' + iter_data.next()
                for _ in range(len(data) / 2)])
        if to_update:
            dict_res += ',' + ','.join([d[1:-1] for d in to_update])
        return ['{' + dict_res + '}']


def multiprocess(arg):
    """Decorator of a function returning a MultiprocessTask
    with BasicMultiCode output_type.
    Future calls of the function will be triggered by the
    environment of MultiprocessTask."""
    if isinstance(arg, int):
        def add_num(func):
            return MultiprocessTask(func, arg, BasicMultiCode)
        return add_num
    else:
        return MultiprocessTask(arg, 1, BasicMultiCode)


def multifinal(func):
    """Decorator of a function returning a MultiprocessTask
    with FinalMultiCode output_type.
    Future calls of the function will be triggered by the
    environment of MultiprocessTask."""
    return MultiprocessTask(func, 1, FinalMultiCode)


def multilist(func):
    """Decorator of a function returning a MultiprocessTask
    with ListMultiCode output_type.
    Future calls of the function will be triggered by the
    environment of MultiprocessTask."""
    return MultiprocessTask(func, 1, ListMultiCode)


def multidict(func):
    """Decorator of a function returning a MultiprocessTask
    with DictMultiCode output_type.
    Future calls of the function will be triggered by the
    environment of MultiprocessTask."""
    return MultiprocessTask(func, 1, DictMultiCode)


def multilistdb(func):
    """Decorator of a function returning a MultiprocessTask
    with DataBaseMultiCode output_type.
    Future calls of the function will be triggered by the
    environment of MultiprocessTask."""
    return MultiprocessTask(func, 1, DataBaseMultiCode)


@multifinal
def final_mp(*args):
    """Auxiliary function used for the output of functions in
    some environments"""
    return args


@multilist
def list_mp(*args):
    """Used as a list compatible with multiprocess environment"""
    return list(args)


@multidict
def dict_mp(*args):
    """Used as a dict compatible with multiprocess environment"""
    return dict(zip(*[iter(args)] * 2))


@multilistdb
def list_db_mp(*args):
    """Used in multiprocess environment for create an auxiliary list.
    Useful for creating a list of elements which will be used as input of
    different functions"""
    return list(args)

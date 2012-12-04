from models import multiprocess, list_mp, dict_mp


@multiprocess
def wait_connection():
    return 'ok'


def wait(w):
    if w < 0:
        return 1
    else:
        import time
        t = time.time()
        time.sleep(w)
        res = time.time() - t
        return res


@multiprocess
def test(time_sleep, num_tasks, extra_memory=''):
    res = list_mp()
    for _ in range(num_tasks):
        res.append(sleep(time_sleep, extra_memory))
    return cumul(res)


@multiprocess
def test_dict(num_task):
    res = dict_mp()
    for i in range(num_task):
        res[i] = sleep(0)
    return res


@multiprocess
def test_deep(time_sleep, list_nodes, extra_memory=''):
    if list_nodes:
        res = list_mp()
        res.append(wait(time_sleep))
        num_nodes, new_list_nodes = list_nodes[0], list_nodes[1:]
        [res.append(test_deep(time_sleep, new_list_nodes, extra_memory))
         for _ in range(num_nodes)]
        return cumul(res)
    else:
        return wait(time_sleep)


@multiprocess
def sleep(sleep, extra_memory=''):
    return wait(sleep)


@multiprocess
def cumul(res_times):
    return sum(res_times)


@multiprocess
def mse(a, b):
    return sum([abs(i - j) for i, j in zip(a, b)])


@multiprocess
def classify_by_min(data, tags):
    from numpy import argmin
    return tags[data.keys()[argmin(data.values())]]


@multiprocess
def classify_mse(data_compare, tags_compare, data_diag):
    res = dict_mp()
    tag_res = dict_mp()
    for key_diag, d_diag in data_diag.items():
        res[key_diag] = dict_mp()
        for key_compare, d_compare in data_compare.items():
            res[key_diag][key_compare] = mse(d_diag, d_compare)
        tag_res[key_diag] = classify_by_min(res[key_diag], tags_compare)
    return tag_res

import sys
sys.path.append('.')

from environments import simple_environment
from test_multiprocessor_env_tasks import make_data_tags
from tasks.test_tasks import test, classify_mse


def test_simple_env():
    simple_environment()
    test(0, 10, 0)


def test_simple_env_tasks():
    len_data, num_data, num_diag = [10, [10, 10], [2, 2]]
    data_compare, tag_compare = make_data_tags(len_data, num_data, 0)
    data_diag, tag_diag = make_data_tags(len_data, num_diag,
                                         len(data_compare))
    data = [data_compare, tag_compare, data_diag]
    result = tag_diag
    simple_environment()
    res = classify_mse(*data)
    assert res == result

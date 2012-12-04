import sys
sys.path.append('.')

from pyople.environments import DevelopmentEnvironment
from test_multiprocessor_env_tasks import make_data_tags


def test_development_env():
    d = DevelopmentEnvironment(display=False)
    d.start('test', [0, 10, 0])
    assert d.wait_for_tasks('0_11')
    d.finish_tasks()
    d.get_data()
    assert d.is_clean()


def test_development_env_tasks():
    len_data, num_data, num_diag = [10, [10, 10], [2, 2]]
    data_compare, tag_compare = make_data_tags(len_data, num_data, 0)
    data_diag, tag_diag = make_data_tags(len_data, num_diag,
                                         len(data_compare))
    procedure, data, result = 'classify_mse',\
            [data_compare, tag_compare, data_diag], tag_diag
    d = DevelopmentEnvironment(display=False)
    d.start(procedure, data)
    d.finish_tasks()
    res = d.get_data()
    assert res == result
    assert d.is_clean()

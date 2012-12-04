import sys
sys.path.append('.')


def test_connection_redis():
    from pipeline.models import connect_redis
    connect_redis()

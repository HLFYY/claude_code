import os

current_dir = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(current_dir, 'data')


REDIS_SET_URL = dict(
    host='db.selfmediaai.cn',
    port=26379,
    db=0,
    password='123.456.',
)
import os
import platform
import socket

CODE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.expanduser('~/datas/mcd')
os.makedirs(DATA_DIR, exist_ok=True)
LOG_DIR = os.path.expanduser('~/logs')
os.makedirs(LOG_DIR, exist_ok=True)

REDIS_SET_URL = dict(
    host='db.selfmediaai.cn',
    port=26379,
    db=0,
    password='123.456.',
)
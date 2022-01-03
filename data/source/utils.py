import time
import os
import requests
import zipfile
import pandas as pd
import traceback
from enum import Enum
from utils.logging import log_print


class FreqEnum(Enum):
    M1 = 1
    M5 = 5
    M15 = 15
    M30 = 30
    H1 = 60
    H4 = 240
    D1 = 1440


class Timer:

    def __init__(self, msg=None):
        self.msg = msg if isinstance(msg, str) else ''

    def __enter__(self):
        self.start = time.time()

    def __exit__(self, *args):
        self.end = time.time()
        print('Running {msg} for {time:.2f} seconds'.format(
            msg=self.msg,
            time=self.end - self.start
        ))


def download_as_df(url, suffix='', colnames=None, usecols=None, chunksize=None):
    df = None
    file_name = './data/source/tmp{}.zip'.format(suffix)
    try:
        log_print('Downloading: {}'.format(url))

        r = requests.get(url, stream=True)
        with open(file_name, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024 * 100):
                if chunk:
                    f.write(chunk)
                    f.flush()
        r.close()
        df = pd.read_csv(
            file_name,
            header=None,
            chunksize=chunksize,
            names=colnames,
            usecols=usecols
        )
    except:
        log_print('File not found')

    return df, file_name
